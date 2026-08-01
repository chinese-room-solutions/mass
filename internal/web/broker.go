package web

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/jitter"
	"github.com/rs/zerolog"
)

// Broker fans values of type T to connected SSE clients. Each tab that needs
// live updates owns one. Slow subscribers drop events rather than block the
// others.
type Broker[T any] struct {
	mu      sync.Mutex
	clients map[chan T]struct{}
	logger  zerolog.Logger
}

// NewBroker builds an empty broker. component names it in dropped-event logs.
func NewBroker[T any](logger zerolog.Logger, component string) *Broker[T] {
	return &Broker[T]{
		clients: make(map[chan T]struct{}),
		logger:  logger.With().Str("component", component).Logger(),
	}
}

// Subscribe returns a buffered channel that receives every event broadcast
// after the call. The caller MUST eventually call [Broker.Unsubscribe].
func (b *Broker[T]) Subscribe() chan T {
	ch := make(chan T, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes ch and closes it.
func (b *Broker[T]) Unsubscribe(ch chan T) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Broadcast non-blockingly sends evt to every subscriber.
func (b *Broker[T]) Broadcast(evt T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- evt:
		default:
			b.logger.Debug().Msg("dropping event for slow subscriber")
		}
	}
}

// sseHeartbeat is the nominal gap between the comment frames that keep an
// idle SSE connection (and any proxy in front of it) from being reaped.
const sseHeartbeat = 15 * time.Second

// newHeartbeatTicker returns one connection's heartbeat ticker, jittered by
// ±20% so a fleet of open dashboards doesn't wake the server in lockstep.
func newHeartbeatTicker() *time.Ticker {
	return time.NewTicker(jitter.Duration(sseHeartbeat, 0.2))
}

// streamChangeEvents serves the change-only SSE pattern shared by the
// Scheduler and Queue tabs: every broadcast becomes a fieldless
// "event: change" frame the browser reacts to by re-fetching its list, with
// a comment heartbeat to keep the connection alive. unavailableMsg is
// returned (503) when the broker is nil.
func streamChangeEvents(w http.ResponseWriter, r *http.Request, broker *Broker[changeEvent], unavailableMsg string) {
	if broker == nil {
		http.Error(w, unavailableMsg, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := broker.Subscribe()
	defer broker.Unsubscribe(ch)

	heartbeat := newHeartbeatTicker()
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			if _, err := io.WriteString(w, "event: change\ndata: \n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
