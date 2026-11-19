package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/go-livepeer/ai/runner"
	"github.com/livepeer/go-livepeer/core"
	"github.com/stretchr/testify/require"
)

// The registry checks the idle deadline whenever a request arrives, so these
// tests only wait past the timeout and never depend on the background sweep.
const liveRunnerTestSessionIdleTimeout = 100 * time.Millisecond

func newLiveRunnerIdleHTTP(t *testing.T, liveRunnerAddr string) *lphttp {
	t.Helper()
	node, err := core.NewLivepeerNode(nil, t.TempDir(), nil)
	require.NoError(t, err)
	if liveRunnerAddr != "" {
		node.LiveRunnerAddr, err = url.Parse(liveRunnerAddr)
		require.NoError(t, err)
	}
	orch := newStubOrchestrator()
	orch.secret = "live-runner-secret"
	lp := &lphttp{
		orchestrator: orch,
		node:         node,
		transRPC:     http.NewServeMux(),
	}
	manager := runner.NewLiveRunnerRegistry(runner.LiveRunnerRegistryConfig{
		Host:               testLiveRunnerHost{orchestrator: lp.orchestrator},
		SessionIdleTimeout: liveRunnerTestSessionIdleTimeout,
	})
	t.Cleanup(manager.Stop)
	node.LiveRunnerManager = manager
	require.NoError(t, startAIServer(lp))
	return lp
}

func liveRunnerSessionExists(t *testing.T, lp *lphttp, sessionID string) bool {
	t.Helper()
	manager, ok := lp.liveRunnerManager()
	require.True(t, ok)
	_, err := manager.RunnerEndpointForSession("runner-1", sessionID)
	return err == nil
}

func requireLiveRunnerCapacityUsed(t *testing.T, lp *lphttp, used int) {
	t.Helper()
	w := httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/discovery", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var resp []liveRunnerDiscoveryEntry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp, 1)
	require.Len(t, resp[0].Runners, 1)
	require.Equal(t, used, resp[0].Runners[0].CapacityUsed)
}

// serveInBackground runs a request that stays in flight until ctx is canceled
// or the handler returns on its own.
func serveInBackground(lp *lphttp, req *http.Request) (*httptest.ResponseRecorder, chan struct{}) {
	w := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		lp.ServeHTTP(w, req)
	}()
	return w, finished
}

func TestLiveRunnerSessionIdleTimeoutProxiedStreamKeepsSessionAlive(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/base/stream" {
			close(arrived)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	lp := newLiveRunnerIdleHTTP(t, "")
	registerLiveRunnerForSession(t, lp, &liveRunnerRegistrationOptions{RunnerURL: upstream.URL + "/base"})
	sessionID := reserveLiveRunnerSession(t, lp, "runner-1")
	requireLiveRunnerCapacityUsed(t, lp, 1)

	w, finished := serveInBackground(lp, httptest.NewRequest(http.MethodGet, "/apps/runner-1/session/"+sessionID+"/app/stream", nil))
	<-arrived
	// A long-lived client request outlasting the timeout keeps the session.
	time.Sleep(3 * liveRunnerTestSessionIdleTimeout)
	require.True(t, liveRunnerSessionExists(t, lp, sessionID))
	close(release)
	<-finished
	require.Equal(t, http.StatusOK, w.Code)

	// Once the last request finishes, the idle clock restarts; a request after
	// the deadline cannot revive the session, which frees its capacity.
	time.Sleep(2 * liveRunnerTestSessionIdleTimeout)
	w = httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/apps/runner-1/session/"+sessionID+"/app/ping", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, liveRunnerSessionExists(t, lp, sessionID))
	requireLiveRunnerCapacityUsed(t, lp, 0)
	reserveLiveRunnerSession(t, lp, "runner-1")
}

func TestLiveRunnerSessionIdleTimeoutGeneratedProxyCountsAsActivity(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	lp := newLiveRunnerIdleHTTP(t, "")
	registerLiveRunnerForSession(t, lp, &liveRunnerRegistrationOptions{RunnerURL: upstream.URL})
	sessionID := reserveLiveRunnerSession(t, lp, "runner-1")
	manager, _ := lp.liveRunnerManager()
	proxy, err := manager.CreateSessionProxy("runner-1", sessionID, "")
	require.NoError(t, err)

	_, finished := serveInBackground(lp, httptest.NewRequest(http.MethodGet, proxy.URL+"/stream", nil))
	<-arrived
	time.Sleep(3 * liveRunnerTestSessionIdleTimeout)
	require.True(t, liveRunnerSessionExists(t, lp, sessionID))
	close(release)
	<-finished

	time.Sleep(2 * liveRunnerTestSessionIdleTimeout)
	w := httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, proxy.URL+"/ping", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, liveRunnerSessionExists(t, lp, sessionID))
}

func TestLiveRunnerSessionIdleTimeoutClientTrickleKeepsSessionAlive(t *testing.T) {
	lp := newLiveRunnerIdleHTTP(t, "http://go-livepeer:8935")
	registerLiveRunnerForSession(t, lp, nil)
	sessionID := reserveLiveRunnerSession(t, lp, "runner-1")
	manager, _ := lp.liveRunnerManager()
	channel, err := manager.CreateTrickleChannel("runner-1", sessionID, "out", "video/MP2T")
	require.NoError(t, err)

	// A client subscriber waiting on the next segment stays in flight.
	w, finished := serveInBackground(lp, httptest.NewRequest(http.MethodGet, channel.URL+"/0", nil))
	time.Sleep(3 * liveRunnerTestSessionIdleTimeout)
	require.True(t, liveRunnerSessionExists(t, lp, sessionID), "an in-flight client trickle request keeps the session alive")
	// The runner publishes the segment on its own origin, which does not count,
	// and the subscriber's request completes.
	publish := httptest.NewRecorder()
	lp.ServeHTTP(publish, httptest.NewRequest(http.MethodPost, channel.InternalURL+"/0", strings.NewReader("segment")))
	require.Equal(t, http.StatusOK, publish.Code)
	<-finished
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "segment", w.Body.String())

	time.Sleep(2 * liveRunnerTestSessionIdleTimeout)
	w = httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, channel.URL+"/next", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, liveRunnerSessionExists(t, lp, sessionID))
	requireLiveRunnerCapacityUsed(t, lp, 0)
}

func TestLiveRunnerSessionIdleTimeoutIgnoresRunnerOriginTrickle(t *testing.T) {
	lp := newLiveRunnerIdleHTTP(t, "http://go-livepeer:8935")
	registerLiveRunnerForSession(t, lp, nil)
	sessionID := reserveLiveRunnerSession(t, lp, "runner-1")
	manager, _ := lp.liveRunnerManager()
	channel, err := manager.CreateTrickleChannel("runner-1", sessionID, "out", "video/MP2T")
	require.NoError(t, err)
	require.Contains(t, channel.InternalURL, "go-livepeer:8935")

	// Runner traffic on the separate -liveRunnerAddr origin does not count.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, channel.InternalURL+"/0", nil).WithContext(ctx)
	_, finished := serveInBackground(lp, req)
	time.Sleep(2 * liveRunnerTestSessionIdleTimeout)

	w := httptest.NewRecorder()
	lp.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/apps/runner-1/session/"+sessionID+"/app/ping", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.False(t, liveRunnerSessionExists(t, lp, sessionID))
	cancel()
	<-finished
}
