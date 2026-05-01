package web

import (
	"sync"

	"github.com/rs/zerolog"
)

// WorkersEventKind distinguishes the two kinds of update the Workers tab
// listens for. Stats fires periodically (gauge refresh, in-place); Change
// fires when the fleet shape itself changes (connect/disconnect/heartbeat
// snapshot, or after a benchmark run lands new persisted numbers).
type WorkersEventKind int

const (
	// WorkersEventStats — periodic stats tick. JS updates gauges in place.
	WorkersEventStats WorkersEventKind = iota
	// WorkersEventChange — fleet/list changed. JS re-fetches /api/workers/list.
	WorkersEventChange
)

// WorkersEvent is one notification fanned out to every active Workers-tab
// SSE subscriber.
type WorkersEvent struct {
	Kind WorkersEventKind
}

// WorkersBroker fans WorkersEvent values to connected SSE clients. Lives in
// the web package because it has no other consumer; if a second tab needs
// SSE later, generalize then.
type WorkersBroker struct {
	mu      sync.Mutex
	clients map[chan WorkersEvent]struct{}
	logger  zerolog.Logger
}

// NewWorkersBroker builds an empty broker.
func NewWorkersBroker(logger zerolog.Logger) *WorkersBroker {
	return &WorkersBroker{
		clients: make(map[chan WorkersEvent]struct{}),
		logger:  logger.With().Str("component", "workers-broker").Logger(),
	}
}

// Subscribe returns a buffered channel that receives every event broadcast
// after the call. The caller MUST eventually call [WorkersBroker.Unsubscribe].
func (b *WorkersBroker) Subscribe() chan WorkersEvent {
	ch := make(chan WorkersEvent, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes ch and closes it.
func (b *WorkersBroker) Unsubscribe(ch chan WorkersEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Broadcast non-blockingly sends evt to every subscriber. Slow subscribers
// drop events rather than block other tabs.
func (b *WorkersBroker) Broadcast(evt WorkersEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- evt:
		default:
			b.logger.Debug().Int("kind", int(evt.Kind)).Msg("dropping event for slow subscriber")
		}
	}
}
