package queue

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
)

// ResultStatus tracks the lifecycle of a queued request.
type ResultStatus string

const (
	ResultStatusPending    ResultStatus = "pending"
	ResultStatusProcessing ResultStatus = "processing"
	ResultStatusDone       ResultStatus = "done"
	ResultStatusError      ResultStatus = "error"
)

// Result represents a stored inference result.
type Result struct {
	ID          string
	RequestHash string
	Status      ResultStatus
	Body        []byte // serialized proto response
	Error       string
	CreatedAt   string
	CompletedAt *time.Time
}

// ResultStore manages the queue_results table for caching and async retrieval.
type ResultStore struct {
	db *sql.DB
}

// NewResultStore creates a new ResultStore using the given database.
func NewResultStore(db *sql.DB) *ResultStore {
	return &ResultStore{db: db}
}

// Create inserts a new pending result entry. Called when a request is enqueued.
func (s *ResultStore) Create(id, requestHash string) error {
	_, err := s.db.Exec(
		`INSERT INTO queue_results (id, request_hash, status) VALUES (?, ?, ?)`,
		id, requestHash, ResultStatusPending,
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("creating result entry: %w", err), map[string]any{"id": id, "request_hash": requestHash})
	}
	return nil
}

// MarkProcessing transitions a result to processing status.
func (s *ResultStore) MarkProcessing(id string) error {
	_, err := s.db.Exec(
		`UPDATE queue_results SET status = ? WHERE id = ?`,
		ResultStatusProcessing, id,
	)
	return err
}

// Complete stores the response body and marks the result as done.
func (s *ResultStore) Complete(id string, body []byte) error {
	_, err := s.db.Exec(
		`UPDATE queue_results SET status = ?, body = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ') WHERE id = ?`,
		ResultStatusDone, body, id,
	)
	return err
}

// Fail marks a result as failed with an error message.
func (s *ResultStore) Fail(id string, errMsg string) error {
	_, err := s.db.Exec(
		`UPDATE queue_results SET status = ?, error = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ') WHERE id = ?`,
		ResultStatusError, errMsg, id,
	)
	return err
}

// Get retrieves a result by ID.
func (s *ResultStore) Get(id string) (*Result, error) {
	r := &Result{}
	var completedAt, errMsg sql.NullString
	err := s.db.QueryRow(
		`SELECT id, request_hash, status, body, error, created_at, completed_at FROM queue_results WHERE id = ?`,
		id,
	).Scan(&r.ID, &r.RequestHash, &r.Status, &r.Body, &errMsg, &r.CreatedAt, &completedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("getting result: %w", err), map[string]any{"id": id})
	}
	r.Error = errMsg.String
	if completedAt.Valid {
		t, _ := time.Parse("2006-01-02T15:04:05.000Z", completedAt.String)
		r.CompletedAt = &t
	}
	return r, nil
}

// FindByHash looks up a cached result by request hash.
// Returns the most recent completed result within the TTL, or nil if none found.
func (s *ResultStore) FindByHash(requestHash string, ttl time.Duration) (*Result, error) {
	cutoff := time.Now().UTC().Add(-ttl).Format("2006-01-02T15:04:05.000Z")
	r := &Result{}
	var completedAt, errMsg sql.NullString
	err := s.db.QueryRow(
		`SELECT id, request_hash, status, body, error, created_at, completed_at
		 FROM queue_results
		 WHERE request_hash = ? AND status = ? AND completed_at >= ?
		 ORDER BY completed_at DESC
		 LIMIT 1`,
		requestHash, ResultStatusDone, cutoff,
	).Scan(&r.ID, &r.RequestHash, &r.Status, &r.Body, &errMsg, &r.CreatedAt, &completedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("finding cached result: %w", err), map[string]any{"request_hash": requestHash})
	}
	r.Error = errMsg.String
	if completedAt.Valid {
		t, _ := time.Parse("2006-01-02T15:04:05.000Z", completedAt.String)
		r.CompletedAt = &t
	}
	return r, nil
}

// Cleanup removes results older than the given TTL.
func (s *ResultStore) Cleanup(ttl time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-ttl).Format("2006-01-02T15:04:05.000Z")
	res, err := s.db.Exec(
		`DELETE FROM queue_results WHERE created_at < ?`,
		cutoff,
	)
	if err != nil {
		return 0, ctxerr.With(fmt.Errorf("cleaning up results: %w", err), map[string]any{"ttl": ttl.String()})
	}
	return res.RowsAffected()
}

// WaitForResult polls the result store until the given result is completed or the context is cancelled.
// Returns the completed result or an error.
func (s *ResultStore) WaitForResult(id string, pollInterval time.Duration, done <-chan struct{}) (*Result, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		r, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		if r != nil && (r.Status == ResultStatusDone || r.Status == ResultStatusError) {
			return r, nil
		}

		select {
		case <-done:
			return nil, fmt.Errorf("context cancelled while waiting for result %s", id)
		case <-ticker.C:
		}
	}
}
