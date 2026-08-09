package web

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleTrackerFiresAfterTimeout(t *testing.T) {
	var fired atomic.Bool
	tr := NewIdleTracker(30*time.Millisecond, nil, func() { fired.Store(true) })
	defer tr.Stop()
	require.Eventually(t, fired.Load, time.Second, 5*time.Millisecond,
		"onIdle should run once the window elapses with no activity")
}

func TestIdleTrackerResetOnRequest(t *testing.T) {
	var fired atomic.Bool
	tr := NewIdleTracker(60*time.Millisecond, nil, func() { fired.Store(true) })
	defer tr.Stop()
	handler := tr.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	// Keep it busy across more than one window; it must not fire while
	// requests arrive.
	for range 5 {
		time.Sleep(20 * time.Millisecond)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/workers/list", nil))
	}
	require.False(t, fired.Load(), "activity within the window keeps it alive")

	// Once requests stop, it eventually fires.
	require.Eventually(t, fired.Load, time.Second, 5*time.Millisecond)
}

// TestIdleTrackerHeldByInFlightRequest guards the long-call case (the GUI's
// control channel stays open for the window's whole life; a benchmark RPC can
// run for minutes): the countdown must not fire while a request is in flight,
// and restarts once the last one ends.
func TestIdleTrackerHeldByInFlightRequest(t *testing.T) {
	var fired atomic.Bool
	tr := NewIdleTracker(30*time.Millisecond, nil, func() { fired.Store(true) })
	defer tr.Stop()
	release := make(chan struct{})
	handler := tr.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/internal/gui/channel", nil))
	}()

	// Hold the request across several idle windows.
	time.Sleep(120 * time.Millisecond)
	require.False(t, fired.Load(), "an in-flight request must hold the daemon alive")

	close(release)
	<-done
	require.Eventually(t, fired.Load, time.Second, 5*time.Millisecond,
		"the countdown restarts when the last request ends")
}

// TestIdleTrackerExemptPaths: worker hub streams and ops probes are not client
// activity — a held-open worker stream must not pin the daemon, and a poller
// must not keep resetting the countdown.
func TestIdleTrackerExemptPaths(t *testing.T) {
	var fired atomic.Bool
	tr := NewIdleTracker(40*time.Millisecond, nil, func() { fired.Store(true) })
	defer tr.Stop()
	release := make(chan struct{})
	defer close(release)
	blocking := tr.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	instant := tr.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	// A worker stream held open for the daemon's whole life.
	go blocking.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/mass.v1.worker.WorkerHub/Session", nil))

	// Probes polling faster than the idle window.
	deadline := time.After(150 * time.Millisecond)
	for !fired.Load() {
		instant.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
		instant.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/internal/daemon/ping", nil))
		select {
		case <-deadline:
			t.Fatal("exempt traffic kept the daemon alive past the idle window")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestIdleTrackerBusyDefers: out-of-request work (an in-flight download)
// defers the shutdown until it clears.
func TestIdleTrackerBusyDefers(t *testing.T) {
	var busy atomic.Bool
	busy.Store(true)
	var fired atomic.Bool
	tr := NewIdleTracker(20*time.Millisecond, busy.Load, func() { fired.Store(true) })
	defer tr.Stop()

	time.Sleep(70 * time.Millisecond)
	require.False(t, fired.Load(), "busy work must defer the idle shutdown")

	busy.Store(false)
	require.Eventually(t, fired.Load, time.Second, 5*time.Millisecond,
		"once the work clears, the next window fires")
}

func TestIdleTrackerStopPreventsFiring(t *testing.T) {
	var fired atomic.Bool
	tr := NewIdleTracker(30*time.Millisecond, nil, func() { fired.Store(true) })
	tr.Stop()
	time.Sleep(60 * time.Millisecond)
	require.False(t, fired.Load(), "Stop must prevent a later firing")
}
