package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
)

// ModelBenchmarkRow is one measured (worker, device set, model) benchmark.
//
// DeviceSet is the canonical sorted comma-joined device-id list one load
// occupies (e.g. "gpu:0"): throughput of a split load isn't decomposable per
// device, so the set is the measurable unit.
//
// Error carries the row's verdict. Empty (stored NULL) means the measurements
// are usable and the model is schedulable on that device set. Non-empty means
// the set is incapable of this model — the measurements are zero and the
// pair is never re-benched automatically. An absent row means the bench
// hasn't concluded.
//
// ModelSize and ModelMTime record the file the row was measured against; a
// mismatch against the current file invalidates every row of that model.
// CreatedAt and UpdatedAt are unix seconds, stamped by the store.
type ModelBenchmarkRow struct {
	WorkerID     string
	DeviceSet    string
	ModelID      string
	UnitsPerSec  float64
	GraphSecs    float64
	BaseBytes    int64
	PerSlotBytes int64
	ModelSize    int64
	ModelMTime   int64
	Error        string
	CreatedAt    int64
	UpdatedAt    int64
}

// ModelBenchmarkStoreInterface abstracts per-model benchmark persistence.
type ModelBenchmarkStoreInterface interface {
	// SaveModelBenchmark records a successful measurement, clearing any
	// previous incapable verdict. row.Error is ignored.
	SaveModelBenchmark(row ModelBenchmarkRow) error
	// SaveModelBenchmarkError records an incapable verdict, zeroing any
	// previous measurements. Only the identity fields, the validity fields
	// and row.Error are read.
	SaveModelBenchmarkError(row ModelBenchmarkRow) error
	// GetModelBenchmark returns the row for the triple, or [sql.ErrNoRows].
	GetModelBenchmark(workerID, deviceSet, modelID string) (ModelBenchmarkRow, error)
	// ListModelBenchmarksByModel returns every concluded row for modelID.
	ListModelBenchmarksByModel(modelID string) ([]ModelBenchmarkRow, error)
	// DeleteModelBenchmarksByModel drops every row for modelID.
	DeleteModelBenchmarksByModel(modelID string) error
	// DeleteModelBenchmarksByWorker drops every row for workerID.
	DeleteModelBenchmarksByWorker(workerID string) error
}

// Compile-time check.
var _ ModelBenchmarkStoreInterface = (*Store)(nil)

const modelBenchmarkColumns = `worker_id, device_set, model_id, units_per_sec, graph_secs,
	base_bytes, per_slot_bytes, model_size, model_mtime, error, created_at, updated_at`

// SaveModelBenchmark records a successful measurement for the triple.
func (s *Store) SaveModelBenchmark(row ModelBenchmarkRow) error {
	row.Error = ""
	return s.upsertModelBenchmark(row)
}

// SaveModelBenchmarkError records an incapable verdict for the triple: this
// device set can't run this model until a model-file change or a manual
// re-bench wipes the row.
func (s *Store) SaveModelBenchmarkError(row ModelBenchmarkRow) error {
	if row.Error == "" {
		return ctxerr.With(fmt.Errorf("empty error for incapable model benchmark"), modelBenchmarkCtx(row.WorkerID, row.DeviceSet, row.ModelID))
	}
	row.UnitsPerSec, row.GraphSecs, row.BaseBytes, row.PerSlotBytes = 0, 0, 0, 0
	return s.upsertModelBenchmark(row)
}

// upsertModelBenchmark writes row atomically, leaving created_at untouched on
// an existing row.
func (s *Store) upsertModelBenchmark(row ModelBenchmarkRow) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(s.rebind(`
		INSERT INTO model_benchmarks (`+modelBenchmarkColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id, device_set, model_id) DO UPDATE SET
			units_per_sec  = excluded.units_per_sec,
			graph_secs     = excluded.graph_secs,
			base_bytes     = excluded.base_bytes,
			per_slot_bytes = excluded.per_slot_bytes,
			model_size     = excluded.model_size,
			model_mtime    = excluded.model_mtime,
			error          = excluded.error,
			updated_at     = excluded.updated_at`),
		row.WorkerID, row.DeviceSet, row.ModelID, row.UnitsPerSec, row.GraphSecs,
		row.BaseBytes, row.PerSlotBytes, row.ModelSize, row.ModelMTime,
		nullableError(row.Error), now, now,
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("saving model benchmark: %w", err), modelBenchmarkCtx(row.WorkerID, row.DeviceSet, row.ModelID))
	}
	return nil
}

// GetModelBenchmark returns the row for the triple, or [sql.ErrNoRows] when the
// bench hasn't concluded there.
func (s *Store) GetModelBenchmark(workerID, deviceSet, modelID string) (ModelBenchmarkRow, error) {
	r := s.db.QueryRow(s.rebind(`
		SELECT `+modelBenchmarkColumns+`
		FROM model_benchmarks
		WHERE worker_id = ? AND device_set = ? AND model_id = ?`), workerID, deviceSet, modelID)
	return scanModelBenchmark(r)
}

// ListModelBenchmarksByModel returns every concluded row for modelID, ordered
// by (worker_id, device_set). Callers use it to decide whether every eligible
// worker has concluded.
func (s *Store) ListModelBenchmarksByModel(modelID string) ([]ModelBenchmarkRow, error) {
	rows, err := s.db.Query(s.rebind(`
		SELECT `+modelBenchmarkColumns+`
		FROM model_benchmarks WHERE model_id = ? ORDER BY worker_id, device_set`), modelID)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing model benchmarks: %w", err), map[string]any{"model_id": modelID})
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []ModelBenchmarkRow
	for rows.Next() {
		row, err := scanModelBenchmark(rows)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("scanning model benchmark row: %w", err), map[string]any{"model_id": modelID})
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// DeleteModelBenchmarksByModel drops every row for modelID. Called when the
// model is removed or its file changed.
func (s *Store) DeleteModelBenchmarksByModel(modelID string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM model_benchmarks WHERE model_id = ?`), modelID)
	if err != nil {
		return ctxerr.With(fmt.Errorf("deleting model benchmarks: %w", err), map[string]any{"model_id": modelID})
	}
	return nil
}

// DeleteModelBenchmarksByWorker drops every row for workerID. Called when the
// worker is removed; its stale device-set rows die with it.
func (s *Store) DeleteModelBenchmarksByWorker(workerID string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM model_benchmarks WHERE worker_id = ?`), workerID)
	if err != nil {
		return ctxerr.With(fmt.Errorf("deleting model benchmarks: %w", err), map[string]any{"worker_id": workerID})
	}
	return nil
}

// rowScanner is the shared shape of [sql.Row] and [sql.Rows].
type rowScanner interface {
	Scan(dest ...any) error
}

func scanModelBenchmark(sc rowScanner) (ModelBenchmarkRow, error) {
	var (
		row      ModelBenchmarkRow
		benchErr sql.NullString
	)
	if err := sc.Scan(
		&row.WorkerID, &row.DeviceSet, &row.ModelID, &row.UnitsPerSec, &row.GraphSecs,
		&row.BaseBytes, &row.PerSlotBytes, &row.ModelSize, &row.ModelMTime,
		&benchErr, &row.CreatedAt, &row.UpdatedAt,
	); err != nil {
		return ModelBenchmarkRow{}, err
	}
	row.Error = benchErr.String
	return row, nil
}

// nullableError maps the empty verdict to SQL NULL, keeping "usable" a single
// representation in the column.
func nullableError(benchErr string) any {
	if benchErr == "" {
		return nil
	}
	return benchErr
}

func modelBenchmarkCtx(workerID, deviceSet, modelID string) map[string]any {
	return map[string]any{"worker_id": workerID, "device_set": deviceSet, "model_id": modelID}
}
