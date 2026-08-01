package store

import (
	"fmt"

	"github.com/KernelPryanic/ctxerr"
)

// WorkerRow is an enrolled worker's persistent identity and per-worker
// credential. SecretHash is the bcrypt hash of the secret handed back once at
// enrollment; the plaintext is never stored. CreatedAt is unix seconds.
// Deleting the row revokes the worker.
type WorkerRow struct {
	WorkerID   string
	Name       string
	SecretHash string
	CreatedAt  int64
}

// InsertWorker persists a newly enrolled worker.
func (s *Store) InsertWorker(row WorkerRow) error {
	_, err := s.db.Exec(s.rebind(`
		INSERT INTO workers (worker_id, name, secret_hash, created_at)
		VALUES (?, ?, ?, ?)`),
		row.WorkerID, row.Name, row.SecretHash, row.CreatedAt,
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("inserting worker: %w", err), map[string]any{"worker_id": row.WorkerID})
	}
	return nil
}

// GetWorker returns the enrolled worker by id, or [sql.ErrNoRows] when absent
// (unknown or revoked).
func (s *Store) GetWorker(workerID string) (WorkerRow, error) {
	var row WorkerRow
	err := s.db.QueryRow(s.rebind(`
		SELECT worker_id, name, secret_hash, created_at
		FROM workers WHERE worker_id = ?`), workerID).
		Scan(&row.WorkerID, &row.Name, &row.SecretHash, &row.CreatedAt)
	if err != nil {
		return WorkerRow{}, err
	}
	return row, nil
}

// ListWorkers returns every enrolled worker, newest first. Used by the
// revocation UI to show credentials that exist independent of a live stream.
func (s *Store) ListWorkers() ([]WorkerRow, error) {
	rows, err := s.db.Query(s.rebind(`
		SELECT worker_id, name, secret_hash, created_at
		FROM workers ORDER BY created_at DESC`))
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing workers: %w", err), nil)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()
	var out []WorkerRow
	for rows.Next() {
		var r WorkerRow
		if err := rows.Scan(&r.WorkerID, &r.Name, &r.SecretHash, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning worker row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteWorker removes an enrolled worker, revoking its per-worker credential.
// Returns whether a row was deleted so callers can distinguish revoke from
// no-op (unknown id).
func (s *Store) DeleteWorker(workerID string) (bool, error) {
	res, err := s.db.Exec(s.rebind(`DELETE FROM workers WHERE worker_id = ?`), workerID)
	if err != nil {
		return false, ctxerr.With(fmt.Errorf("deleting worker: %w", err), map[string]any{"worker_id": workerID})
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, ctxerr.With(fmt.Errorf("rows affected: %w", err), map[string]any{"worker_id": workerID})
	}
	return n > 0, nil
}
