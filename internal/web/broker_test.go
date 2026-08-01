package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBroker_BroadcastReachesSubscribers(t *testing.T) {
	b := NewBroker[int](zerolog.Nop(), "test")
	a := b.Subscribe()
	c := b.Subscribe()
	defer b.Unsubscribe(a)
	defer b.Unsubscribe(c)

	b.Broadcast(42)

	require.Equal(t, 42, <-a)
	require.Equal(t, 42, <-c)
}

func TestBroker_UnsubscribeClosesChannel(t *testing.T) {
	b := NewBroker[int](zerolog.Nop(), "test")
	ch := b.Subscribe()
	b.Unsubscribe(ch)

	_, ok := <-ch
	require.False(t, ok, "channel must be closed after Unsubscribe")
}

func TestBroker_UnsubscribedReceivesNothing(t *testing.T) {
	b := NewBroker[int](zerolog.Nop(), "test")
	ch := b.Subscribe()
	b.Unsubscribe(ch)

	// Broadcasting after unsubscribe must not panic on the closed channel.
	require.NotPanics(t, func() { b.Broadcast(1) })
}

func TestBroker_SlowSubscriberDropsNotBlocks(t *testing.T) {
	b := NewBroker[int](zerolog.Nop(), "test")
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	// Channel buffer is 16. Broadcasting more than that without draining must
	// drop the overflow rather than block the broadcaster.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			b.Broadcast(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked on a full subscriber")
	}
}

func TestBroker_ConcurrentSubscribeBroadcast(t *testing.T) {
	b := NewBroker[int](zerolog.Nop(), "test")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := b.Subscribe()
			b.Broadcast(1)
			b.Unsubscribe(ch)
		}()
	}
	require.NotPanics(t, func() { wg.Wait() })
}

func TestStreamChangeEvents_NilBrokerReturns503(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/scheduler/events", nil)
	w := httptest.NewRecorder()
	streamChangeEvents(w, r, nil, "scheduler events unavailable")

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "scheduler events unavailable")
}

// syncRecorder is a minimal concurrency-safe ResponseWriter for streaming
// tests: the handler goroutine writes while the test goroutine inspects what
// has been flushed, so the buffer and status need a mutex (httptest's recorder
// has none). It signals each flush over flushed so the test can synchronise
// without polling.
type syncRecorder struct {
	mu      sync.Mutex
	hdr     http.Header
	buf     []byte
	status  int
	flushed chan struct{}
}

func newSyncRecorder() *syncRecorder {
	return &syncRecorder{hdr: http.Header{}, status: http.StatusOK, flushed: make(chan struct{}, 16)}
}
func (s *syncRecorder) Header() http.Header { return s.hdr }
func (s *syncRecorder) WriteHeader(c int)   { s.mu.Lock(); s.status = c; s.mu.Unlock() }
func (s *syncRecorder) Write(b []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, b...)
	s.mu.Unlock()
	return len(b), nil
}
func (s *syncRecorder) Flush() {
	select {
	case s.flushed <- struct{}{}:
	default:
	}
}
func (s *syncRecorder) body() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.buf)
}

func TestStreamChangeEvents_EmitsChangeFrame(t *testing.T) {
	b := NewBroker[changeEvent](zerolog.Nop(), "test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/api/scheduler/events", nil).WithContext(ctx)
	w := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		streamChangeEvents(w, r, b, "unavailable")
		close(done)
	}()

	// Wait for the handler to subscribe before broadcasting (else the event
	// is dropped before anyone is listening).
	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.clients) == 1
	}, time.Second, 5*time.Millisecond)

	b.Broadcast(changeEvent{})

	// The handler flushes after writing the change frame.
	select {
	case <-w.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not flush a change frame")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamChangeEvents did not return after context cancel")
	}

	require.Contains(t, w.body(), "event: change")
}
