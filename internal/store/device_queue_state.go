package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// DeviceQueueState tracks the tail hash and loaded model for a device queue.
type DeviceQueueState struct {
	QueueName      string
	WorkerID       string
	DeviceIDs      []string
	TailHash       string
	TailDifficulty float64 // running sum of queued task difficulty; maintained on enqueue/dequeue/steal/drain
	LoadedHash     string
	Enabled        bool
	UpdatedAt      time.Time
}

// DeviceQueueStateStoreInterface abstracts device queue state persistence.
type DeviceQueueStateStoreInterface interface {
	// UpsertDeviceQueueState creates or replaces a device queue state entry.
	UpsertDeviceQueueState(state DeviceQueueState) error
	// GetDeviceQueueState returns the state for a queue, or sql.ErrNoRows if not found.
	GetDeviceQueueState(queueName string) (DeviceQueueState, error)
	// ListDeviceQueueStates returns all device queue states.
	ListDeviceQueueStates() ([]DeviceQueueState, error)
	// DeleteDeviceQueueState removes a device queue state entry.
	DeleteDeviceQueueState(queueName string) error
	// UpdateTail sets the tail fingerprint and difficulty sum for a queue.
	// Used when the head of the tail changes (e.g. a different-fp task is appended).
	UpdateTail(queueName, tailHash string, tailDifficulty float64) error
	// AddTailDifficulty atomically adjusts tail_difficulty by delta. Used for
	// per-message increments on enqueue and decrements on dequeue/steal so
	// concurrent updaters don't clobber each other. Pass a positive delta to
	// add, negative to subtract.
	AddTailDifficulty(queueName string, delta float64) error
	// UpdateLoadedHash updates the currently loaded model fingerprint for a queue.
	UpdateLoadedHash(queueName, loadedHash string) error
	// SetEnabled enables or disables a device queue for scheduling.
	SetEnabled(queueName string, enabled bool) error
}

// Compile-time check.
var _ DeviceQueueStateStoreInterface = (*Store)(nil)

// UpsertDeviceQueueState creates or replaces a device queue state entry.
// On first insert, the caller-supplied [DeviceQueueState.Enabled] is honored.
// On conflict (e.g. a worker reconnecting), `enabled` is intentionally NOT in
// the UPDATE clause — the user's prior choice for this queue is preserved.
func (s *Store) UpsertDeviceQueueState(state DeviceQueueState) error {
	deviceIDsJSON, err := json.Marshal(state.DeviceIDs)
	if err != nil {
		return fmt.Errorf("marshalling device IDs: %w", err)
	}
	enabled := 0
	if state.Enabled {
		enabled = 1
	}
	_, err = s.db.Exec(`
		INSERT INTO device_queue_state (queue_name, worker_id, device_ids, tail_hash, tail_difficulty, loaded_hash, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(queue_name) DO UPDATE SET
			worker_id       = excluded.worker_id,
			device_ids      = excluded.device_ids,
			tail_hash       = excluded.tail_hash,
			tail_difficulty = excluded.tail_difficulty,
			loaded_hash     = excluded.loaded_hash,
			updated_at      = strftime('%Y-%m-%dT%H:%M:%fZ')`,
		state.QueueName, state.WorkerID, string(deviceIDsJSON),
		state.TailHash, state.TailDifficulty, state.LoadedHash, enabled,
	)
	if err != nil {
		return fmt.Errorf("upserting device queue state %s: %w", state.QueueName, err)
	}
	return nil
}

// GetDeviceQueueState returns the state for a queue, or sql.ErrNoRows if not found.
func (s *Store) GetDeviceQueueState(queueName string) (DeviceQueueState, error) {
	var st DeviceQueueState
	var deviceIDsJSON, ts string
	err := s.db.QueryRow(`
		SELECT queue_name, worker_id, device_ids, tail_hash, tail_difficulty, loaded_hash, enabled, updated_at
		FROM device_queue_state WHERE queue_name = ?`, queueName).
		Scan(&st.QueueName, &st.WorkerID, &deviceIDsJSON, &st.TailHash, &st.TailDifficulty, &st.LoadedHash, &st.Enabled, &ts)
	if err != nil {
		return DeviceQueueState{}, err
	}
	if err := json.Unmarshal([]byte(deviceIDsJSON), &st.DeviceIDs); err != nil {
		return DeviceQueueState{}, fmt.Errorf("unmarshalling device IDs: %w", err)
	}
	st.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return st, nil
}

// ListDeviceQueueStates returns all device queue states.
func (s *Store) ListDeviceQueueStates() ([]DeviceQueueState, error) {
	rows, err := s.db.Query(`
		SELECT queue_name, worker_id, device_ids, tail_hash, tail_difficulty, loaded_hash, enabled, updated_at
		FROM device_queue_state ORDER BY queue_name`)
	if err != nil {
		return nil, fmt.Errorf("listing device queue states: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []DeviceQueueState
	for rows.Next() {
		var st DeviceQueueState
		var deviceIDsJSON, ts string
		if err := rows.Scan(&st.QueueName, &st.WorkerID, &deviceIDsJSON, &st.TailHash, &st.TailDifficulty, &st.LoadedHash, &st.Enabled, &ts); err != nil {
			return nil, fmt.Errorf("scanning device queue state: %w", err)
		}
		if err := json.Unmarshal([]byte(deviceIDsJSON), &st.DeviceIDs); err != nil {
			return nil, fmt.Errorf("unmarshalling device IDs: %w", err)
		}
		st.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, st)
	}
	return out, rows.Err()
}

// DeleteDeviceQueueState removes a device queue state entry.
func (s *Store) DeleteDeviceQueueState(queueName string) error {
	_, err := s.db.Exec(`DELETE FROM device_queue_state WHERE queue_name = ?`, queueName)
	if err != nil {
		return fmt.Errorf("deleting device queue state %s: %w", queueName, err)
	}
	return nil
}

// UpdateTail sets the tail fingerprint and difficulty sum for a queue.
func (s *Store) UpdateTail(queueName, tailHash string, tailDifficulty float64) error {
	_, err := s.db.Exec(`
		UPDATE device_queue_state
		SET tail_hash = ?, tail_difficulty = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE queue_name = ?`, tailHash, tailDifficulty, queueName)
	if err != nil {
		return fmt.Errorf("updating tail for %s: %w", queueName, err)
	}
	return nil
}

// AddTailDifficulty atomically adjusts tail_difficulty by delta. The clamp
// at zero protects against drift from out-of-order updates (a dequeue
// arriving before its enqueue side has been recorded). Without it the
// running sum could go slightly negative and warp later score calculations.
func (s *Store) AddTailDifficulty(queueName string, delta float64) error {
	_, err := s.db.Exec(`
		UPDATE device_queue_state
		SET tail_difficulty = MAX(0, tail_difficulty + ?),
		    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE queue_name = ?`, delta, queueName)
	if err != nil {
		return fmt.Errorf("adjusting tail_difficulty for %s: %w", queueName, err)
	}
	return nil
}

// SetEnabled enables or disables a device queue for scheduling.
func (s *Store) SetEnabled(queueName string, enabled bool) error {
	v := 1
	if !enabled {
		v = 0
	}
	_, err := s.db.Exec(`
		UPDATE device_queue_state
		SET enabled = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE queue_name = ?`, v, queueName)
	if err != nil {
		return fmt.Errorf("setting enabled for %s: %w", queueName, err)
	}
	return nil
}

// UpdateLoadedHash updates the currently loaded model fingerprint for a queue.
func (s *Store) UpdateLoadedHash(queueName, loadedHash string) error {
	_, err := s.db.Exec(`
		UPDATE device_queue_state
		SET loaded_hash = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE queue_name = ?`, loadedHash, queueName)
	if err != nil {
		return fmt.Errorf("updating loaded hash for %s: %w", queueName, err)
	}
	return nil
}
