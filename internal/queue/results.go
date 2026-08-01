package queue

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/store"
)

// ResultStatus tracks the lifecycle of a queued job.
type ResultStatus string

const (
	ResultStatusPending    ResultStatus = "pending"
	ResultStatusProcessing ResultStatus = "processing"
	ResultStatusDone       ResultStatus = "done"
	ResultStatusError      ResultStatus = "error"
)

// Result represents a stored job result.
type Result struct {
	ID          string
	Status      ResultStatus
	Body        []byte // gateway-defined opaque response bytes
	Error       string
	CreatedAt   string
	CompletedAt *time.Time
}

// ResultStore manages the queue_results table for async retrieval and
// crash-recovery. The body is gateway-defined opaque bytes — MASS does not
// inspect it. Identity dedup belongs to the gateway, not MASS.
type ResultStore struct {
	db      *sql.DB
	dialect Dialect
}

// NewResultStore creates a new ResultStore using the given database.
func NewResultStore(db *sql.DB, dialect Dialect) *ResultStore {
	return &ResultStore{db: db, dialect: dialect}
}

// rebind rewrites `?` to `$N` placeholders for the store's dialect.
func (s *ResultStore) rebind(query string) string { return store.Rebind(s.dialect, query) }

// Create inserts a new pending result entry. Called when a request is enqueued.
func (s *ResultStore) Create(id string) error {
	_, err := s.db.Exec(
		s.rebind(`INSERT INTO queue_results (id, status, created_at) VALUES (?, ?, ?)`),
		id, ResultStatusPending, time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("creating result entry: %w", err), map[string]any{"id": id})
	}
	return nil
}

// Processing marks a pending result as processing — the job has actually
// started running on a worker. Guarded on the current status so a terminal
// write racing this call is never regressed: only a pending row flips.
func (s *ResultStore) Processing(id string) error {
	_, err := s.db.Exec(
		s.rebind(`UPDATE queue_results SET status = ? WHERE id = ? AND status = ?`),
		ResultStatusProcessing, id, ResultStatusPending,
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("marking result processing: %w", err), map[string]any{"id": id})
	}
	return nil
}

// Pending reverts a processing result back to pending — the job lost its
// worker and awaits redistribution. Guarded the same way as
// [ResultStore.Processing]: only a processing row flips, so a terminal
// status can never be regressed by a racing revert.
func (s *ResultStore) Pending(id string) error {
	_, err := s.db.Exec(
		s.rebind(`UPDATE queue_results SET status = ? WHERE id = ? AND status = ?`),
		ResultStatusPending, id, ResultStatusProcessing,
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("reverting result to pending: %w", err), map[string]any{"id": id})
	}
	return nil
}

// Complete stores the response body and marks the result as done.
func (s *ResultStore) Complete(id string, body []byte) error {
	_, err := s.db.Exec(
		s.rebind(`UPDATE queue_results SET status = ?, body = ?, completed_at = ? WHERE id = ?`),
		ResultStatusDone, body, time.Now().UTC().Format(time.RFC3339Nano), id,
	)
	return err
}

// Fail marks a result as failed with an error message.
func (s *ResultStore) Fail(id string, errMsg string) error {
	_, err := s.db.Exec(
		s.rebind(`UPDATE queue_results SET status = ?, error = ?, completed_at = ? WHERE id = ?`),
		ResultStatusError, errMsg, time.Now().UTC().Format(time.RFC3339Nano), id,
	)
	return err
}

// Get retrieves a result by ID.
func (s *ResultStore) Get(id string) (*Result, error) {
	r := &Result{}
	var completedAt, errMsg sql.NullString
	err := s.db.QueryRow(
		s.rebind(`SELECT id, status, body, error, created_at, completed_at FROM queue_results WHERE id = ?`),
		id,
	).Scan(&r.ID, &r.Status, &r.Body, &errMsg, &r.CreatedAt, &completedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("getting result: %w", err), map[string]any{"id": id})
	}
	r.Error = errMsg.String
	if completedAt.Valid {
		t, err := time.Parse(time.RFC3339Nano, completedAt.String)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("parsing completed_at: %w", err), map[string]any{"id": id})
		}
		r.CompletedAt = &t
	}
	return r, nil
}

// Cleanup removes terminal (done/error) results whose completion is older
// than the given TTL. Live rows (pending/processing) are never TTL-pruned —
// a job queued or running longer than the TTL must keep its result row so
// the eventual Complete/Fail lands somewhere. Never-terminal leftovers are
// the job of the buffer reaper and the startup sweeper, not this cleanup.
func (s *ResultStore) Cleanup(ttl time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-ttl).Format(time.RFC3339Nano)
	res, err := s.db.Exec(
		s.rebind(`DELETE FROM queue_results WHERE status IN (?, ?) AND completed_at < ?`),
		ResultStatusDone, ResultStatusError, cutoff,
	)
	if err != nil {
		return 0, ctxerr.With(fmt.Errorf("cleaning up results: %w", err), map[string]any{"ttl": ttl.String()})
	}
	return res.RowsAffected()
}
