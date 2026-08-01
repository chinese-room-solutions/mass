package web

import (
	"context"
	"errors"
	"fmt"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

// Queue operations shared by the dashboard's Queue tab and the public
// mass.v1.Mass Connect API. The mutating ops own their audit-log calls; the
// scheduler's sentinel errors (ErrRowInFlight, ErrUnknownQueue, ErrNotInflight,
// ErrWorkerGone, ErrEvictGlobalRow) pass through unwrapped so both transports
// keep their errors.Is switches.

// queueSnapshot returns the rows on the global queue and every active worker
// queue, or ErrOpUnavailable when the scheduler isn't wired.
func (h *Handler) queueSnapshot(ctx context.Context) ([]scheduler.QueueSection, error) {
	if h.orch == nil {
		return nil, fmt.Errorf("%w: scheduler", ErrOpUnavailable)
	}
	return h.orch.QueueSnapshot(ctx)
}

// cancelQueuedJob removes an unleased row from a worker or global queue.
func (h *Handler) cancelQueuedJob(ctx context.Context, queue, msgID, actor string) error {
	if h.orch == nil {
		return fmt.Errorf("%w: scheduler", ErrOpUnavailable)
	}
	if queue == "" || msgID == "" {
		return fmt.Errorf("%w: queue and msgID are required", ErrOpInvalid)
	}
	err := h.orch.CancelQueuedRow(ctx, queue, msgID)
	switch {
	case err == nil:
		audit.Log(h.logger, "queue.cancelled", msgID, audit.OutcomeOK).
			Str("actor", actor).Str("queue", queue).Msg("")
	case errors.Is(err, scheduler.ErrRowInFlight):
		audit.Log(h.logger, "queue.cancelled", msgID, audit.OutcomeDenied).
			Str("actor", actor).Str("queue", queue).Str("reason", "in flight").Msg("")
	}
	return err
}

// cancelRunningJob fires a cancel at the worker currently running requestID.
// The terminal frame arrives async; success means the cancel was accepted.
func (h *Handler) cancelRunningJob(ctx context.Context, requestID, actor string) error {
	if h.orch == nil {
		return fmt.Errorf("%w: scheduler", ErrOpUnavailable)
	}
	if requestID == "" {
		return fmt.Errorf("%w: requestID is required", ErrOpInvalid)
	}
	err := h.orch.CancelRunningJob(ctx, requestID)
	if err == nil {
		audit.Log(h.logger, "queue.running_cancelled", requestID, audit.OutcomeOK).
			Str("actor", actor).Msg("")
	}
	return err
}

// evictQueuedJob moves a worker-queue row back to the global queue for
// re-placement.
func (h *Handler) evictQueuedJob(ctx context.Context, queue, msgID, actor string) error {
	if h.orch == nil {
		return fmt.Errorf("%w: scheduler", ErrOpUnavailable)
	}
	if queue == "" || msgID == "" {
		return fmt.Errorf("%w: queue and msgID are required", ErrOpInvalid)
	}
	err := h.orch.EvictQueuedRowToGlobal(ctx, queue, msgID)
	switch {
	case err == nil:
		audit.Log(h.logger, "queue.evicted", msgID, audit.OutcomeOK).
			Str("actor", actor).Str("queue", queue).Msg("")
	case errors.Is(err, scheduler.ErrRowInFlight):
		audit.Log(h.logger, "queue.evicted", msgID, audit.OutcomeDenied).
			Str("actor", actor).Str("queue", queue).Str("reason", "in flight").Msg("")
	}
	return err
}
