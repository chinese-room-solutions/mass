package web

import (
	"sync"

	"github.com/rs/zerolog"
)

// SchedulerEvent is one notification fanned out to every active Scheduler-tab
// SSE subscriber. Only one kind for now ("change"); JS reacts by re-fetching
// /api/scheduler/list. Mirrors WorkersBroker's shape for consistency.
type SchedulerEvent struct{}

// SchedulerBroker fans SchedulerEvent values to connected SSE clients. Lives
// in the web package because it has no other consumer; if a second consumer
// appears, generalize then.
type SchedulerBroker struct {
	mu      sync.Mutex
	clients map[chan SchedulerEvent]struct{}
	logger  zerolog.Logger
}

func NewSchedulerBroker(logger zerolog.Logger) *SchedulerBroker {
	return &SchedulerBroker{
		clients: make(map[chan SchedulerEvent]struct{}),
		logger:  logger.With().Str("component", "scheduler-broker").Logger(),
	}
}

// Subscribe returns a buffered channel that receives every event broadcast
// after the call. The caller MUST eventually call [SchedulerBroker.Unsubscribe].
func (b *SchedulerBroker) Subscribe() chan SchedulerEvent {
	ch := make(chan SchedulerEvent, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes ch and closes it.
func (b *SchedulerBroker) Unsubscribe(ch chan SchedulerEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Broadcast non-blockingly sends evt to every subscriber. Slow subscribers
// drop events rather than block other tabs.
func (b *SchedulerBroker) Broadcast(evt SchedulerEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- evt:
		default:
			b.logger.Debug().Msg("dropping scheduler event for slow subscriber")
		}
	}
}
