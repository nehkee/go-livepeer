package runner

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

const liveRunnerTestIdleTimeout = time.Minute

// newLiveRunnerIdleTestRegistry must be called inside the synctest bubble so
// the registry's background loops run on the fake clock. The heartbeat TTL is
// raised so only the session idle timeout is under test.
func newLiveRunnerIdleTestRegistry(t *testing.T, idleTimeout time.Duration, mode string) *LiveRunnerRegistry {
	t.Helper()
	registry := newLiveRunnerTestRegistryWithConfig(LiveRunnerRegistryConfig{
		Host:               liveRunnerTestHost{},
		O2RInterval:        liveRunnerTestQuietO2RInterval(),
		SessionIdleTimeout: idleTimeout,
	})
	t.Cleanup(registry.Stop)
	registry.mu.Lock()
	registry.heartbeatTTL = 1000 * time.Hour
	registry.mu.Unlock()
	req := liveRunnerTestHeartbeat("runner-1")
	req.Capacity = 2
	req.Mode = mode
	liveRunnerTestRegister(t, registry, req)
	return registry
}

func liveRunnerTestSessionExists(registry *LiveRunnerRegistry, sessionID string) bool {
	_, err := registry.RunnerEndpointForSession("runner-1", sessionID)
	return err == nil
}

func liveRunnerTestBeginSessionRequest(t *testing.T, registry *LiveRunnerRegistry, sessionID string) func() {
	t.Helper()
	done, err := registry.BeginSessionRequest("runner-1", sessionID)
	if err != nil {
		t.Fatalf("expected session request to start, got %v", err)
	}
	return done
}

func TestLiveRunnerRegistry_SessionIdleTimeoutDisabledByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, 0, LiveRunnerModePersistent)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(24 * time.Hour)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected session to survive without an idle timeout")
		}
		if _, matched, err := registry.BeginTrickleRequest(sessionID + "-events"); matched || err != nil {
			t.Fatalf("expected trickle accounting to be skipped when disabled, matched=%v err=%v", matched, err)
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutUnusedReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, liveRunnerTestIdleTimeout, LiveRunnerModePersistent)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(liveRunnerTestIdleTimeout - time.Nanosecond)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected session to survive just before the idle deadline")
		}
		// The background expiry loop reaps the session on its own once the
		// deadline passes.
		time.Sleep(defaultLiveRunnerHeartbeatInterval)
		synctest.Wait()
		if liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected the expiry loop to release an unused reservation")
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutRequestsAroundDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, liveRunnerTestIdleTimeout, LiveRunnerModePersistent)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}

		// Just before the deadline, a request keeps the session alive.
		time.Sleep(liveRunnerTestIdleTimeout - time.Nanosecond)
		done := liveRunnerTestBeginSessionRequest(t, registry, sessionID)
		// A request or stream that outlives the timeout keeps the session.
		time.Sleep(3 * liveRunnerTestIdleTimeout)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected an in-flight request to keep the session alive")
		}
		done()
		done() // Completion is idempotent and never double-counts.

		// Inactivity is measured from when the request finished.
		time.Sleep(liveRunnerTestIdleTimeout - time.Nanosecond)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected the idle clock to restart when the request finished")
		}
		time.Sleep(time.Nanosecond)
		// A request at the deadline cannot revive the session, even before the
		// periodic sweep runs.
		if _, err := registry.BeginSessionRequest("runner-1", sessionID); !isRunnerErrorStatus(err, http.StatusNotFound) {
			t.Fatalf("expected request at the deadline to get 404, got %v", err)
		}
		if liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected session to be released at the deadline")
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutOverlappingRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, liveRunnerTestIdleTimeout, LiveRunnerModePersistent)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}
		first := liveRunnerTestBeginSessionRequest(t, registry, sessionID)
		time.Sleep(liveRunnerTestIdleTimeout / 2)
		second := liveRunnerTestBeginSessionRequest(t, registry, sessionID)
		time.Sleep(liveRunnerTestIdleTimeout)
		first()
		// The second request is still in flight.
		time.Sleep(2 * liveRunnerTestIdleTimeout)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected the remaining in-flight request to keep the session alive")
		}
		second()
		time.Sleep(liveRunnerTestIdleTimeout - time.Nanosecond)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected the idle clock to start when the last request finished")
		}
		time.Sleep(time.Nanosecond)
		registry.removeIdleSessions(time.Now())
		if liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected session to be released after the last request went idle")
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutTrickleRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, liveRunnerTestIdleTimeout, LiveRunnerModePersistent)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}
		channel, err := registry.CreateTrickleChannel("runner-1", sessionID, "events", "application/json")
		if err != nil {
			t.Fatal(err)
		}
		if _, matched, err := registry.BeginTrickleRequest(sessionID + "-unknown"); matched || err != nil {
			t.Fatalf("expected unknown channel to be unmatched, matched=%v err=%v", matched, err)
		}

		done, matched, err := registry.BeginTrickleRequest(channel.ChannelName)
		if !matched || err != nil {
			t.Fatalf("expected session channel to match, matched=%v err=%v", matched, err)
		}
		time.Sleep(3 * liveRunnerTestIdleTimeout)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected an in-flight trickle request to keep the session alive")
		}
		done()

		time.Sleep(liveRunnerTestIdleTimeout)
		// A trickle request at the deadline cannot revive the session. The
		// background sweep fires at the same instant; if it wins, the channel is
		// already closed and no longer matches, so either way the request fails.
		if _, matched, err := registry.BeginTrickleRequest(channel.ChannelName); matched && !isRunnerErrorStatus(err, http.StatusNotFound) {
			t.Fatalf("expected trickle request at the deadline to get 404, matched=%v err=%v", matched, err)
		}
		if liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected session to be released at the deadline")
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutSkipsSingleShot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := newLiveRunnerIdleTestRegistry(t, liveRunnerTestIdleTimeout, LiveRunnerModeSingleShot)
		sessionID, _, err := registry.ReserveSession("runner-1")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * liveRunnerTestIdleTimeout)
		registry.removeIdleSessions(time.Now())
		if !liveRunnerTestSessionExists(registry, sessionID) {
			t.Fatal("expected single-shot sessions to be released only by their request")
		}
	})
}

func TestLiveRunnerRegistry_SessionIdleTimeoutReleasesThroughReleasePath(t *testing.T) {
	trickleSrv, channelBaseURL, channelStatus := newLiveRunnerTestTrickleServer(t)
	// A long timeout keeps the background loop out of the way; the sweep is
	// driven directly with a future time instead.
	const idleTimeout = time.Hour
	registry := newLiveRunnerTestRegistryWithConfig(LiveRunnerRegistryConfig{
		Host:               liveRunnerTestHost{},
		O2RInterval:        liveRunnerTestQuietO2RInterval(),
		SessionIdleTimeout: idleTimeout,
	})
	t.Cleanup(registry.Stop)
	registry.SetTrickleServer(trickleSrv, channelBaseURL, channelBaseURL)
	resp := liveRunnerTestRegister(t, registry, liveRunnerTestHeartbeat("runner-1"))

	sessionID, _, err := registry.ReserveSession("runner-1")
	if err != nil {
		t.Fatal(err)
	}
	liveRunnerTestReadSessionEvent(t, trickleSrv, resp.O2R.ChannelName, "reserved", sessionID)
	if _, err := registry.CreateTrickleChannel("runner-1", sessionID, "events", ""); err != nil {
		t.Fatal(err)
	}
	proxy, err := registry.CreateSessionProxy("runner-1", sessionID, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.ReserveSession("runner-1"); !isRunnerErrorStatus(err, http.StatusServiceUnavailable) {
		t.Fatalf("expected the runner to be at capacity, got %v", err)
	}

	registry.removeIdleSessions(time.Now().Add(idleTimeout))

	liveRunnerTestReadSessionEvent(t, trickleSrv, resp.O2R.ChannelName, "released", sessionID)
	if liveRunnerTestSessionExists(registry, sessionID) {
		t.Fatal("expected idle session to be released")
	}
	if status := channelStatus(sessionID + "-events"); status != http.StatusNotFound {
		t.Fatalf("expected idle release to close session channels, got status=%d", status)
	}
	if _, matched, err := registry.ResolveSessionProxy("localhost:1234", "/run/"+proxy.ProxyID); !matched || !isRunnerErrorStatus(err, http.StatusNotFound) {
		t.Fatalf("expected idle release to remove session proxy, matched=%v err=%v", matched, err)
	}
	runners := registry.Runners()
	if len(runners) != 1 || runners[0].CapacityUsed != 0 || runners[0].CapacityAvailable != 1 {
		t.Fatalf("expected idle release to free capacity, got %+v", runners)
	}
	if _, _, err := registry.ReserveSession("runner-1"); err != nil {
		t.Fatalf("expected freed capacity to be reservable, got %v", err)
	}
}
