package web

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// IdleTracker shuts the daemon down after a period with no client traffic, so
// an instance spawned on demand (by the GUI or a CLI verb) doesn't linger once
// its clients are gone. It is only installed when a positive idle timeout is
// set (`mass serve --idle-timeout`); a plain `mass serve` never has one.
//
// A request in flight counts as activity for its whole duration, not just its
// arrival: the countdown is suspended while any request runs and restarts when
// the last one ends. That is what keeps the daemon up under an attached GUI
// window (its control channel is one long-lived request) and under an open
// dashboard tab (its SSE stream likewise).
//
// Internal streams must not count: a connected worker holds its hub stream
// open indefinitely, and ops probes poll forever — either would pin the
// daemon alive with no one using it. idleExempt filters them out.
//
// Work that outlives the request that started it (an in-flight model
// download) has no request to hold the countdown, so the tracker asks busy.
type IdleTracker struct {
	timeout time.Duration
	busy    func() bool // work in progress outside any request; nil = never busy.
	onIdle  func()      // called once, when the idle window elapses with no requests.

	mu       sync.Mutex
	timer    *time.Timer
	fired    bool
	inFlight int // requests currently being served; >0 suppresses firing.
}

// NewIdleTracker starts the idle countdown immediately (a daemon nobody calls
// should still expire) and returns the tracker. busy reports work the request
// count can't see (pass nil when there is none); onIdle runs at most once.
func NewIdleTracker(timeout time.Duration, busy func() bool, onIdle func()) *IdleTracker {
	t := &IdleTracker{timeout: timeout, busy: busy, onIdle: onIdle}
	// Under the lock: a zero timeout can fire before AfterFunc returns, and
	// fire reads t.timer.
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timer = time.AfterFunc(timeout, t.fire)
	return t
}

// idleExempt reports requests that are not client activity: worker hub
// streams (a connected worker would otherwise hold the daemon up forever) and
// ops/liveness probes (pollers would keep resetting the countdown).
func idleExempt(path string) bool {
	return strings.HasPrefix(path, "/mass.v1.worker.WorkerHub/") ||
		path == "/health" || path == "/ready" || path == "/metrics" ||
		path == "/internal/daemon/ping"
}

// Wrap returns next with each non-exempt request suspending the idle
// countdown for as long as it runs.
func (t *IdleTracker) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if idleExempt(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		t.begin()
		defer t.end()
		next.ServeHTTP(w, r)
	})
}

// begin marks a request in flight, holding the countdown. It's a no-op once
// the timer has already fired, so a request racing the shutdown can't revive
// a daemon that's stopping.
func (t *IdleTracker) begin() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fired {
		return
	}
	t.inFlight++
	t.timer.Stop()
}

// end retires an in-flight request, restarting the countdown when it was the
// last one. Skipped once fired, mirroring begin.
func (t *IdleTracker) end() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fired {
		return
	}
	if t.inFlight--; t.inFlight == 0 {
		t.timer.Reset(t.timeout)
	}
}

// fire runs onIdle once, the first time the window elapses with nothing in
// flight and nothing busy. With a request in flight it declines without
// rescheduling — end restarts the countdown when the last request finishes.
// Busy work has no such completion hook, so that case reschedules itself and
// checks again a window later.
func (t *IdleTracker) fire() {
	t.mu.Lock()
	if t.fired || t.inFlight > 0 {
		t.mu.Unlock()
		return
	}
	if t.busy != nil && t.busy() {
		t.timer.Reset(t.timeout)
		t.mu.Unlock()
		return
	}
	t.fired = true
	// onIdle shuts the server down, which waits for in-flight handlers — and
	// those call end(), which takes this lock. Release it first.
	t.mu.Unlock()
	t.onIdle()
}

// Stop halts the timer (on normal shutdown) so it can't fire afterward.
func (t *IdleTracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fired = true
	t.timer.Stop()
}
