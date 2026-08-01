package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
)

// WorkerDeviceEnabledRow is the operator-controlled enable flag for one
// (worker_id, device_id) pair. Absent row is treated as enabled (sane
// default for newly-connected workers).
type WorkerDeviceEnabledRow struct {
	WorkerID  string
	DeviceID  string
	Enabled   bool
	UpdatedAt time.Time
}

// SetWorkerDeviceEnabled upserts the enable flag for (workerID, deviceID).
func (s *Store) SetWorkerDeviceEnabled(workerID, deviceID string, enabled bool) error {
	_, err := s.db.Exec(s.rebind(`
		INSERT INTO worker_device_enabled (worker_id, device_id, enabled, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(worker_id, device_id) DO UPDATE SET
			enabled    = excluded.enabled,
			updated_at = excluded.updated_at`),
		workerID, deviceID, enabled, nowStamp(),
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("setting worker device enabled: %w", err),
			map[string]any{"worker_id": workerID, "device_id": deviceID})
	}
	return nil
}

// GetWorkerDeviceEnabled returns the row for (workerID, deviceID), or
// [sql.ErrNoRows] when absent.
func (s *Store) GetWorkerDeviceEnabled(workerID, deviceID string) (WorkerDeviceEnabledRow, error) {
	var row WorkerDeviceEnabledRow
	var ts string
	err := s.db.QueryRow(s.rebind(`
		SELECT worker_id, device_id, enabled, updated_at
		FROM worker_device_enabled WHERE worker_id = ? AND device_id = ?`),
		workerID, deviceID).Scan(&row.WorkerID, &row.DeviceID, &row.Enabled, &ts)
	if err != nil {
		return WorkerDeviceEnabledRow{}, err
	}
	row.UpdatedAt, err = parseStamp(ts)
	if err != nil {
		return WorkerDeviceEnabledRow{}, err
	}
	return row, nil
}

// ListWorkerDevicesEnabled returns every persisted row for workerID.
// Devices with no row are absent from the result; callers treat them as
// enabled by default.
func (s *Store) ListWorkerDevicesEnabled(workerID string) ([]WorkerDeviceEnabledRow, error) {
	rows, err := s.db.Query(s.rebind(`
		SELECT worker_id, device_id, enabled, updated_at
		FROM worker_device_enabled WHERE worker_id = ? ORDER BY device_id`),
		workerID)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing worker devices enabled: %w", err),
			map[string]any{"worker_id": workerID})
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()
	var out []WorkerDeviceEnabledRow
	for rows.Next() {
		var r WorkerDeviceEnabledRow
		var ts string
		if err := rows.Scan(&r.WorkerID, &r.DeviceID, &r.Enabled, &ts); err != nil {
			return nil, fmt.Errorf("scanning worker device enabled row: %w", err)
		}
		r.UpdatedAt, err = parseStamp(ts)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetWorkerDevicesEnabledBulk upserts the enable flag for every device in
// deviceIDs under workerID. Used by per-worker toggle handlers.
func (s *Store) SetWorkerDevicesEnabledBulk(workerID string, deviceIDs []string, enabled bool) error {
	if len(deviceIDs) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return ctxerr.With(fmt.Errorf("begin tx: %w", err), map[string]any{"worker_id": workerID})
	}
	commit := false
	defer func() {
		if !commit {
			if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
				panic(fmt.Errorf("rollback: %w", rerr))
			}
		}
	}()
	now := nowStamp()
	for _, devID := range deviceIDs {
		if _, err := tx.Exec(s.rebind(`
			INSERT INTO worker_device_enabled (worker_id, device_id, enabled, updated_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(worker_id, device_id) DO UPDATE SET
				enabled    = excluded.enabled,
				updated_at = excluded.updated_at`),
			workerID, devID, enabled, now,
		); err != nil {
			return ctxerr.With(fmt.Errorf("bulk-setting worker device enabled: %w", err),
				map[string]any{"worker_id": workerID, "device_id": devID})
		}
	}
	if err := tx.Commit(); err != nil {
		return ctxerr.With(fmt.Errorf("commit: %w", err), map[string]any{"worker_id": workerID})
	}
	commit = true
	return nil
}
