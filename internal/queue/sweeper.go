package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

// ReapAbandoned writes a failure result for, and deletes, every message
// past its retry budget with an expired lease. One-shot at startup.
//
// At [MaxReceive]=1, every in-process failure already writes a result
// inline. The surviving case is **MASS crashed mid-flight** — a row was
// received but the result-writing goroutine died with the process. Reap
// cleans those up so callers don't hang on stale rows.
//
// globalQ is the durability-anchor queue; workerQueues holds one entry per
// persisted device queue (even ones whose worker hasn't reconnected). Both
// may carry prior-lifetime messages, and reaping a worker row also drops
// the global anchor it names — otherwise the anchor outlives its job under
// a lease no live dispatch is renewing.
//
// Returns total rows reaped. Per-row errors are logged; only DB-level
// errors abort.
func ReapAbandoned(ctx context.Context, globalQ QueueInterface, workerQueues []QueueInterface, results ResultStoreInterface, logger zerolog.Logger) (int, error) {
	total, err := reapQueue(ctx, globalQ, nil, results, logger)
	if err != nil {
		return total, err
	}
	for _, q := range workerQueues {
		n, err := reapQueue(ctx, q, globalQ, results, logger)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// reapQueue reaps one queue. anchorQ is the global queue when q is a
// worker queue (so anchors get dropped along with their rows), nil when q
// IS the global queue.
func reapQueue(ctx context.Context, q, anchorQ QueueInterface, results ResultStoreInterface, logger zerolog.Logger) (int, error) {
	abandoned, err := q.ListAbandoned(ctx)
	if err != nil {
		return 0, err
	}
	reaped := 0
	for _, msg := range abandoned {
		if err := reapOne(ctx, q, anchorQ, results, logger, msg); err != nil {
			logger.Warn().Err(err).Str("message_id", string(msg.ID)).Msg("reaping abandoned message")
			continue
		}
		reaped++
	}
	return reaped, nil
}

// reapOne writes a failure result for one abandoned message and deletes
// it, together with its global anchor when it has one. Terminal results
// (Done/Error) are NOT overwritten — guards the race where a worker
// reported success and the result landed but the queue Delete didn't
// before the process died.
func reapOne(ctx context.Context, q, anchorQ QueueInterface, results ResultStoreInterface, logger zerolog.Logger, msg *Message) error {
	requestID := string(msg.ID)
	anchorID := ""
	if env, err := UnmarshalEnvelope(msg.Body); err == nil {
		if env.RequestID != "" {
			requestID = env.RequestID
		}
		anchorID = env.GlobalMsgID
	}

	existing, err := results.Get(requestID)
	if err != nil {
		return fmt.Errorf("checking result state for %s: %w", requestID, err)
	}
	terminal := existing != nil && (existing.Status == ResultStatusDone || existing.Status == ResultStatusError)
	if !terminal {
		errMsg := fmt.Sprintf("task abandoned: delivered %d of %d times, no result reported", MaxReceive, MaxReceive)
		if err := results.Fail(requestID, errMsg); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("recording failure for %s: %w", requestID, err)
		}
	} else {
		logger.Debug().Str("request_id", requestID).Str("status", string(existing.Status)).Msg("abandoned row had a terminal result already; preserving it")
	}

	if anchorQ != nil && anchorID != "" {
		if err := q.DeleteBoth(ctx, msg.ID, anchorQ, MessageID(anchorID)); err != nil {
			return fmt.Errorf("deleting abandoned message %s and its anchor %s: %w", msg.ID, anchorID, err)
		}
		return nil
	}
	if err := q.Delete(ctx, msg.ID); err != nil {
		return fmt.Errorf("deleting abandoned message %s: %w", msg.ID, err)
	}
	return nil
}
