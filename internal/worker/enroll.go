package worker

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/oklog/ulid/v2"

	"github.com/chinese-room-solutions/mass/internal/store"
)

// EnrollStoreInterface is the store surface the enrollment path needs: minting
// and validating join tokens, and creating/authenticating per-worker
// credentials. Narrowed so the worker package depends only on what it uses.
type EnrollStoreInterface interface {
	InsertJoinToken(row store.JoinTokenRow, now int64) error
	ListValidJoinTokens(now int64) ([]store.JoinTokenRow, error)
	InsertWorker(row store.WorkerRow) error
	GetWorker(workerID string) (store.WorkerRow, error)
}

// ErrInvalidJoinToken means the presented join token matched no live row.
var ErrInvalidJoinToken = errors.New("invalid or expired join token")

// ErrUnknownWorker means the presented worker id has no enrolled row (never
// enrolled, or revoked).
var ErrUnknownWorker = errors.New("unknown or revoked worker")

// ErrBadWorkerSecret means the worker id exists but the presented secret does
// not match its stored hash.
var ErrBadWorkerSecret = errors.New("worker secret does not match")

// Enroller mints and validates the credentials in the join-token enrollment
// flow. It is the single owner of the credential lifecycle, shared by the hub
// (enroll on connect, authenticate steady-state) and the control plane (mint a
// join token for an operator).
type Enroller struct {
	store EnrollStoreInterface
}

// NewEnroller builds an Enroller over the given store.
func NewEnroller(st EnrollStoreInterface) *Enroller {
	return &Enroller{store: st}
}

// MintJoinToken creates a join token valid for ttl. A ttl <= 0 selects
// [DefaultJoinTokenTTLSeconds]. Returns the plaintext (shown once) and the unix
// expiry.
func (e *Enroller) MintJoinToken(ttl time.Duration) (token string, expiresAt int64, err error) {
	if ttl <= 0 {
		ttl = DefaultJoinTokenTTLSeconds * time.Second
	}
	token, err = NewJoinToken()
	if err != nil {
		return "", 0, err
	}
	hash, err := HashCredential(token)
	if err != nil {
		return "", 0, err
	}
	now := time.Now()
	expiresAt = now.Add(ttl).Unix()
	row := store.JoinTokenRow{
		ID:        ulid.Make().String(),
		TokenHash: hash,
		ExpiresAt: expiresAt,
		CreatedAt: now.Unix(),
	}
	if err := e.store.InsertJoinToken(row, now.Unix()); err != nil {
		return "", 0, fmt.Errorf("persisting join token: %w", err)
	}
	return token, expiresAt, nil
}

// validateJoinToken reports whether token matches a live join token. bcrypt is
// one-way, so it scans live rows and compares each. Returns [ErrInvalidJoinToken]
// on no match.
func (e *Enroller) validateJoinToken(token string) error {
	rows, err := e.store.ListValidJoinTokens(time.Now().Unix())
	if err != nil {
		return fmt.Errorf("reading join tokens: %w", err)
	}
	for _, row := range rows {
		if credentialMatches(row.TokenHash, token) {
			return nil
		}
	}
	return ErrInvalidJoinToken
}

// Enroll mints a new worker identity and secret, persists the bcrypt hash, and
// returns the server-assigned id plus the plaintext secret (shown once). name
// is the worker's advertised display name.
func (e *Enroller) Enroll(name string) (workerID, secret string, err error) {
	workerID = ulid.Make().String()
	secret, err = NewWorkerSecret()
	if err != nil {
		return "", "", err
	}
	hash, err := HashCredential(secret)
	if err != nil {
		return "", "", err
	}
	row := store.WorkerRow{
		WorkerID:   workerID,
		Name:       name,
		SecretHash: hash,
		CreatedAt:  time.Now().Unix(),
	}
	if err := e.store.InsertWorker(row); err != nil {
		return "", "", fmt.Errorf("persisting worker: %w", err)
	}
	return workerID, secret, nil
}

// authenticateWorker verifies a steady-state (worker_id, secret) pair against
// the stored hash. Returns [ErrUnknownWorker] when the id has no row (unknown or
// revoked) and [ErrBadWorkerSecret] when the secret doesn't match.
func (e *Enroller) authenticateWorker(workerID, secret string) error {
	row, err := e.store.GetWorker(workerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ctxerr.With(ErrUnknownWorker, map[string]any{"worker_id": workerID})
		}
		return fmt.Errorf("reading worker %s: %w", workerID, err)
	}
	if !credentialMatches(row.SecretHash, secret) {
		return ctxerr.With(ErrBadWorkerSecret, map[string]any{"worker_id": workerID})
	}
	return nil
}
