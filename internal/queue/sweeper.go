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
// Pass every queue that may hold prior-lifetime messages: global plus one
// per persisted device queue (even ones whose worker hasn't reconnected).
//
// Returns total rows reaped. Per-row errors are logged; only DB-level
// errors abort.
func ReapAbandoned(ctx context.Context, queues []QueueInterface, results ResultStoreInterface, logger zerolog.Logger) (int, error) {
	total := 0
	for _, q := range queues {
		n, err := reapQueue(ctx, q, results, logger)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func reapQueue(ctx context.Context, q QueueInterface, results ResultStoreInterface, logger zerolog.Logger) (int, error) {
	abandoned, err := q.ListAbandoned(ctx)
	if err != nil {
		return 0, err
	}
	reaped := 0
	for _, msg := range abandoned {
		if err := reapOne(ctx, q, results, logger, msg); err != nil {
			logger.Warn().Err(err).Str("message_id", string(msg.ID)).Msg("reaping abandoned message")
			continue
		}
		reaped++
	}
	return reaped, nil
}

// reapOne writes a failure result for one abandoned message and deletes
// it. Terminal results (Done/Error) are NOT overwritten — guards the race
// where a worker reported success and the result landed but the queue
// Delete didn't before the process died.
func reapOne(ctx context.Context, q QueueInterface, results ResultStoreInterface, logger zerolog.Logger, msg *Message) error {
	requestID := string(msg.ID)
	if env, err := UnmarshalEnvelope(msg.Body); err == nil && env.RequestID != "" {
		requestID = env.RequestID
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

	if err := q.Delete(ctx, msg.ID); err != nil {
		return fmt.Errorf("deleting abandoned message %s: %w", msg.ID, err)
	}
	return nil
}
