package downloads

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	st, err := store.Open(store.DialectSQLite, filepath.Join(t.TempDir(), "downloads.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return NewManager(st, t.TempDir(), zerolog.Nop())
}

// No more than maxConcurrent transfers hit the network at once; queued
// jobs run as slots free up and every job still completes.
func TestManager_ConcurrencyCap(t *testing.T) {
	var (
		gateMu   sync.Mutex
		inflight int
		peak     int
	)
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateMu.Lock()
		inflight++
		if inflight > peak {
			peak = inflight
		}
		gateMu.Unlock()
		defer func() {
			gateMu.Lock()
			inflight--
			gateMu.Unlock()
		}()
		<-gate
		_, _ = w.Write([]byte("payload"))
	}))
	defer srv.Close()

	m := newTestManager(t)
	const jobs = 5
	for i := range jobs {
		require.NoError(t, m.Start(Job{
			RelPath: fmt.Sprintf("gguf/file-%d.gguf", i),
			URL:     fmt.Sprintf("%s/file-%d", srv.URL, i),
		}))
	}

	// Wait until the cap is saturated, then hold briefly to catch any
	// over-admission before releasing the gate.
	require.Eventually(t, func() bool {
		gateMu.Lock()
		defer gateMu.Unlock()
		return inflight == maxConcurrent
	}, 5*time.Second, 10*time.Millisecond, "expected %d transfers in flight", maxConcurrent)
	time.Sleep(200 * time.Millisecond)
	gateMu.Lock()
	require.Equal(t, maxConcurrent, peak, "no more than maxConcurrent transfers may run at once")
	gateMu.Unlock()

	close(gate)

	// Every job completes (done jobs are removed from the manager).
	require.Eventually(t, func() bool {
		return len(m.List()) == 0
	}, 10*time.Second, 25*time.Millisecond, "all downloads should finish after the gate opens")

	gateMu.Lock()
	require.Equal(t, maxConcurrent, peak, "peak concurrency must stay at the cap")
	gateMu.Unlock()
}

// A job still queued behind the semaphore must react to Cancel
// immediately: the runner goroutine exits without ever taking a slot,
// so Cancel returns well inside its 5s runner-exit grace period.
func TestManager_CancelWhileQueuedBehindSemaphore(t *testing.T) {
	m := newTestManager(t)

	// Occupy every transfer slot so the job queues on the semaphore.
	for range maxConcurrent {
		m.sem <- struct{}{}
	}
	defer func() {
		for range maxConcurrent {
			<-m.sem
		}
	}()

	const relPath = "gguf/queued.gguf"
	require.NoError(t, m.Start(Job{
		RelPath: relPath,
		// Never contacted: the job sits on the semaphore the whole time.
		URL: "http://127.0.0.1:1/unreachable",
	}))

	// Wait for the runner goroutine to register its cancel func + done
	// channel, so Cancel is guaranteed to see (and wait on) them.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		rj, ok := m.jobs[relPath]
		m.mu.Unlock()
		if !ok {
			return false
		}
		rj.mu.Lock()
		defer rj.mu.Unlock()
		return rj.done != nil
	}, 5*time.Second, 10*time.Millisecond)

	start := time.Now()
	require.NoError(t, m.Cancel(relPath))
	require.Less(t, time.Since(start), 4*time.Second,
		"Cancel must not hit the 5s runner-exit timeout: the queued runner should exit on ctx cancellation")

	require.Empty(t, m.List(), "cancelled job must be gone")
}
