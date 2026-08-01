package store

import (
	"fmt"
	"time"
)

// ThroughputCorrection is one persisted entry of the scheduler's live
// throughput-correction EWMA: the learned multiplier on WorkerID's benched
// throughput along one cost axis, plus how many completed jobs back it.
// Persisting the pair lets calibration survive gateway restarts instead of
// re-warming from the bench prior on every run.
type ThroughputCorrection struct {
	WorkerID  string
	Axis      string
	Factor    float64
	Samples   int
	UpdatedAt time.Time
}

// ThroughputCorrectionStoreInterface abstracts throughput-correction
// persistence.
type ThroughputCorrectionStoreInterface interface {
	// UpsertThroughputCorrection inserts or replaces the row for
	// (WorkerID, Axis). UpdatedAt is stamped by the store.
	UpsertThroughputCorrection(c ThroughputCorrection) error
	// ListThroughputCorrections returns every persisted correction,
	// ordered by (worker_id, axis).
	ListThroughputCorrections() ([]ThroughputCorrection, error)
	// DeleteThroughputCorrections removes every axis row for workerID.
	// Called when the worker's bench prior or device set changes — the
	// factors are relative to both, so the evidence doesn't transfer.
	DeleteThroughputCorrections(workerID string) error
}

// Compile-time check.
var _ ThroughputCorrectionStoreInterface = (*Store)(nil)

// UpsertThroughputCorrection inserts or replaces the row for (WorkerID, Axis).
func (s *Store) UpsertThroughputCorrection(c ThroughputCorrection) error {
	_, err := s.db.Exec(s.rebind(`
		INSERT INTO throughput_corrections (worker_id, axis, factor, samples, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(worker_id, axis) DO UPDATE SET
			factor     = excluded.factor,
			samples    = excluded.samples,
			updated_at = excluded.updated_at`),
		c.WorkerID, c.Axis, c.Factor, c.Samples, nowStamp(),
	)
	if err != nil {
		return fmt.Errorf("upserting throughput correction %s|%s: %w", c.WorkerID, c.Axis, err)
	}
	return nil
}

// ListThroughputCorrections returns every persisted correction.
func (s *Store) ListThroughputCorrections() ([]ThroughputCorrection, error) {
	rows, err := s.db.Query(`
		SELECT worker_id, axis, factor, samples, updated_at
		FROM throughput_corrections ORDER BY worker_id, axis`)
	if err != nil {
		return nil, fmt.Errorf("listing throughput corrections: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []ThroughputCorrection
	for rows.Next() {
		var c ThroughputCorrection
		var ts string
		if err := rows.Scan(&c.WorkerID, &c.Axis, &c.Factor, &c.Samples, &ts); err != nil {
			return nil, fmt.Errorf("scanning throughput correction: %w", err)
		}
		c.UpdatedAt, err = parseStamp(ts)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteThroughputCorrections removes every axis row for workerID.
func (s *Store) DeleteThroughputCorrections(workerID string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM throughput_corrections WHERE worker_id = ?`), workerID)
	if err != nil {
		return fmt.Errorf("deleting throughput corrections for %s: %w", workerID, err)
	}
	return nil
}
