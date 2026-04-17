package store

import (
	"database/sql"
	"fmt"
	"time"
)

// BenchmarkRow represents a stored device benchmark result.
type BenchmarkRow struct {
	WorkerID      string
	DeviceID      string
	DeviceName    string
	MemoryGBs     float64
	ComputeGFlops float64 // Q4_K matmul GFLOPS (comparable across CPU/GPU)
	BenchedAt     time.Time
}

// SaveBenchmark upserts a benchmark result for the given agent/device pair.
func (s *Store) SaveBenchmark(row BenchmarkRow) error {
	_, err := s.db.Exec(`
		INSERT INTO device_benchmarks (worker_id, device_id, device_name, memory_gbs, compute_gflops, benched_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id, device_id) DO UPDATE SET
			device_name    = excluded.device_name,
			memory_gbs  = excluded.memory_gbs,
			compute_gflops = excluded.compute_gflops,
			benched_at     = excluded.benched_at`,
		row.WorkerID,
		row.DeviceID,
		row.DeviceName,
		row.MemoryGBs,
		row.ComputeGFlops,
		row.BenchedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("saving benchmark for %s/%s: %w", row.WorkerID, row.DeviceID, err)
	}
	return nil
}

// GetBenchmark returns the stored benchmark for an agent/device pair, or sql.ErrNoRows.
func (s *Store) GetBenchmark(workerID, deviceID string) (BenchmarkRow, error) {
	var row BenchmarkRow
	var ts string
	err := s.db.QueryRow(`
		SELECT worker_id, device_id, device_name, memory_gbs, compute_gflops, benched_at
		FROM device_benchmarks WHERE worker_id = ? AND device_id = ?`, workerID, deviceID).
		Scan(&row.WorkerID, &row.DeviceID, &row.DeviceName, &row.MemoryGBs, &row.ComputeGFlops, &ts)
	if err != nil {
		return BenchmarkRow{}, err
	}
	row.BenchedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return row, nil
}

// ListBenchmarks returns all stored benchmark results.
func (s *Store) ListBenchmarks() ([]BenchmarkRow, error) {
	rows, err := s.db.Query(`
		SELECT worker_id, device_id, device_name, memory_gbs, compute_gflops, benched_at
		FROM device_benchmarks ORDER BY worker_id, device_id`)
	if err != nil {
		return nil, fmt.Errorf("listing benchmarks: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []BenchmarkRow
	for rows.Next() {
		var r BenchmarkRow
		var ts string
		if err := rows.Scan(&r.WorkerID, &r.DeviceID, &r.DeviceName, &r.MemoryGBs, &r.ComputeGFlops, &ts); err != nil {
			return nil, fmt.Errorf("scanning benchmark row: %w", err)
		}
		r.BenchedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteBenchmark removes a stored benchmark result for an agent/device pair.
func (s *Store) DeleteBenchmark(workerID, deviceID string) error {
	_, err := s.db.Exec(`DELETE FROM device_benchmarks WHERE worker_id = ? AND device_id = ?`, workerID, deviceID)
	if err != nil {
		return fmt.Errorf("deleting benchmark for %s/%s: %w", workerID, deviceID, err)
	}
	return nil
}

// HasBenchmark returns true if a benchmark exists for the given agent/device pair.
func (s *Store) HasBenchmark(workerID, deviceID string) (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM device_benchmarks WHERE worker_id = ? AND device_id = ?`, workerID, deviceID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Compile-time check that sql.ErrNoRows is importable (used by callers).
var _ = sql.ErrNoRows
