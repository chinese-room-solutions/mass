package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// WorkerQueueState persists the scheduler's per-worker-queue running totals.
// One row per worker queue.
type WorkerQueueState struct {
	QueueName string
	WorkerID  string
	DeviceIDs []string
	// TailSeconds is the expected wall-clock time to drain everything
	// currently queued (not-yet-dispatched) on this queue, summed across
	// envelopes as Difficulty/power + load_latency at enqueue time.
	// Subtracted by Envelope.QueuedSeconds on dispatch pop.
	TailSeconds float64
	// TailModelID is the model_id of the *last queued task* on this queue.
	// Read at scoring time to decide whether a new envelope pays a model-
	// switch load cost. Updated on enqueue; unchanged on dispatch pop
	// (popping the head doesn't move the tail).
	TailModelID string
	UpdatedAt   time.Time
}

// WorkerQueueStateStoreInterface abstracts worker queue state persistence.
type WorkerQueueStateStoreInterface interface {
	// UpsertWorkerQueueState creates a worker queue state entry or, when
	// the row already exists, refreshes its identity columns (worker_id,
	// device_ids) while preserving the tail columns — see the
	// implementation for why.
	UpsertWorkerQueueState(state WorkerQueueState) error
	// GetWorkerQueueState returns the state for a queue, or sql.ErrNoRows if not found.
	GetWorkerQueueState(queueName string) (WorkerQueueState, error)
	// ListWorkerQueueStates returns all worker queue states.
	ListWorkerQueueStates() ([]WorkerQueueState, error)
	// DeleteWorkerQueueState removes a worker queue state entry.
	DeleteWorkerQueueState(queueName string) error
	// AddTailSeconds atomically adjusts tail_seconds by delta. Used on
	// dispatch pop (delta = -env.QueuedSeconds) and on work-stealing
	// transfers (delta on both source and destination). Concurrent
	// updaters compose because the math runs in SQL.
	AddTailSeconds(queueName string, delta float64) error
	// AddTailSecondsAndSetModel atomically adjusts tail_seconds by delta
	// and sets tail_model_id to newModelID. Used on enqueue: the new
	// envelope's queued seconds extend the queue and the envelope
	// becomes the new tail-of-queue model.
	AddTailSecondsAndSetModel(queueName string, delta float64, newModelID string) error
	// SetTailSeconds atomically replaces tail_seconds with value and sets
	// tail_model_id to tailModelID. Used by re-estimation after the
	// operator toggles a device on/off — the per-envelope sum needs to be
	// reconciled with the new device set in one write, not incrementally.
	// Negative values clamp at 0 (same shape as AddTailSeconds).
	SetTailSeconds(queueName string, value float64, tailModelID string) error
}

// Compile-time check.
var _ WorkerQueueStateStoreInterface = (*Store)(nil)

// UpsertWorkerQueueState creates a worker queue state entry or refreshes
// an existing one's identity columns (worker_id, device_ids). The tail
// columns are deliberately NOT overwritten on conflict: the scheduler
// re-upserts on every reconnect and bench landing, and clobbering
// tail_seconds/tail_model_id there would silently zero a live queue's
// running totals. A fresh insert takes the struct's tail values (0 for a
// brand-new queue); tail mutations flow through the Add*/SetTailSeconds
// methods.
func (s *Store) UpsertWorkerQueueState(state WorkerQueueState) error {
	deviceIDsJSON, err := json.Marshal(state.DeviceIDs)
	if err != nil {
		return fmt.Errorf("marshalling device IDs: %w", err)
	}
	now := nowStamp()
	_, err = s.db.Exec(s.rebind(`
		INSERT INTO worker_queue_state (queue_name, worker_id, device_ids, tail_seconds, tail_model_id, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(queue_name) DO UPDATE SET
			worker_id      = excluded.worker_id,
			device_ids     = excluded.device_ids,
			updated_at     = excluded.updated_at`),
		state.QueueName, state.WorkerID, string(deviceIDsJSON),
		state.TailSeconds, state.TailModelID, now,
	)
	if err != nil {
		return fmt.Errorf("upserting worker queue state %s: %w", state.QueueName, err)
	}
	return nil
}

// GetWorkerQueueState returns the state for a queue, or sql.ErrNoRows if not found.
func (s *Store) GetWorkerQueueState(queueName string) (WorkerQueueState, error) {
	var st WorkerQueueState
	var deviceIDsJSON, ts string
	err := s.db.QueryRow(s.rebind(`
		SELECT queue_name, worker_id, device_ids, tail_seconds, tail_model_id, updated_at
		FROM worker_queue_state WHERE queue_name = ?`), queueName).
		Scan(&st.QueueName, &st.WorkerID, &deviceIDsJSON, &st.TailSeconds, &st.TailModelID, &ts)
	if err != nil {
		return WorkerQueueState{}, err
	}
	if err := json.Unmarshal([]byte(deviceIDsJSON), &st.DeviceIDs); err != nil {
		return WorkerQueueState{}, fmt.Errorf("unmarshalling device IDs: %w", err)
	}
	st.UpdatedAt, err = parseStamp(ts)
	if err != nil {
		return WorkerQueueState{}, err
	}
	return st, nil
}

// ListWorkerQueueStates returns all worker queue states.
func (s *Store) ListWorkerQueueStates() ([]WorkerQueueState, error) {
	rows, err := s.db.Query(`
		SELECT queue_name, worker_id, device_ids, tail_seconds, tail_model_id, updated_at
		FROM worker_queue_state ORDER BY queue_name`)
	if err != nil {
		return nil, fmt.Errorf("listing worker queue states: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []WorkerQueueState
	for rows.Next() {
		var st WorkerQueueState
		var deviceIDsJSON, ts string
		if err := rows.Scan(&st.QueueName, &st.WorkerID, &deviceIDsJSON, &st.TailSeconds, &st.TailModelID, &ts); err != nil {
			return nil, fmt.Errorf("scanning worker queue state: %w", err)
		}
		if err := json.Unmarshal([]byte(deviceIDsJSON), &st.DeviceIDs); err != nil {
			return nil, fmt.Errorf("unmarshalling device IDs: %w", err)
		}
		st.UpdatedAt, err = parseStamp(ts)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// DeleteWorkerQueueState removes a worker queue state entry.
func (s *Store) DeleteWorkerQueueState(queueName string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM worker_queue_state WHERE queue_name = ?`), queueName)
	if err != nil {
		return fmt.Errorf("deleting worker queue state %s: %w", queueName, err)
	}
	return nil
}

// AddTailSeconds atomically adjusts tail_seconds by delta. The clamp at
// zero protects against drift from out-of-order updates (a dispatch pop
// arriving before its enqueue side has been recorded). Without it the
// running sum could go slightly negative and warp later score calculations.
//
// The clamp uses the dialect's two-argument max: SQLite has scalar MAX(a,b),
// Postgres has GREATEST(a,b). [Store.maxFn] returns the right one.
func (s *Store) AddTailSeconds(queueName string, delta float64) error {
	_, err := s.db.Exec(s.rebind(`
		UPDATE worker_queue_state
		SET tail_seconds = `+s.maxFn()+`(0, tail_seconds + ?),
		    updated_at   = ?
		WHERE queue_name = ?`), delta, nowStamp(), queueName)
	if err != nil {
		return fmt.Errorf("adjusting tail_seconds for %s: %w", queueName, err)
	}
	return nil
}

// AddTailSecondsAndSetModel atomically adjusts tail_seconds by delta and
// sets tail_model_id to newModelID. Used on enqueue: the new envelope's
// queued seconds extend the queue and the envelope becomes the new
// tail-of-queue model.
func (s *Store) AddTailSecondsAndSetModel(queueName string, delta float64, newModelID string) error {
	_, err := s.db.Exec(s.rebind(`
		UPDATE worker_queue_state
		SET tail_seconds  = `+s.maxFn()+`(0, tail_seconds + ?),
		    tail_model_id = ?,
		    updated_at    = ?
		WHERE queue_name = ?`), delta, newModelID, nowStamp(), queueName)
	if err != nil {
		return fmt.Errorf("adjusting tail seconds + model for %s: %w", queueName, err)
	}
	return nil
}

// SetTailSeconds atomically replaces tail_seconds with value and sets
// tail_model_id to tailModelID. Negative values clamp at 0 to mirror
// AddTailSeconds's invariant.
func (s *Store) SetTailSeconds(queueName string, value float64, tailModelID string) error {
	if value < 0 {
		value = 0
	}
	_, err := s.db.Exec(s.rebind(`
		UPDATE worker_queue_state
		SET tail_seconds  = ?,
		    tail_model_id = ?,
		    updated_at    = ?
		WHERE queue_name = ?`), value, tailModelID, nowStamp(), queueName)
	if err != nil {
		return fmt.Errorf("setting tail_seconds for %s: %w", queueName, err)
	}
	return nil
}
