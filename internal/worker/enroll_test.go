package worker

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/chinese-room-solutions/mass/internal/store"
)

// fakeEnrollStore is an in-memory EnrollStoreInterface for the credential-flow
// tests. It prunes on list/insert like the real store so expiry behavior is
// exercised without a database.
type fakeEnrollStore struct {
	tokens  map[string]store.JoinTokenRow
	workers map[string]store.WorkerRow
}

func newFakeEnrollStore() *fakeEnrollStore {
	return &fakeEnrollStore{
		tokens:  map[string]store.JoinTokenRow{},
		workers: map[string]store.WorkerRow{},
	}
}

func (f *fakeEnrollStore) InsertJoinToken(row store.JoinTokenRow, now int64) error {
	for id, r := range f.tokens {
		if r.ExpiresAt <= now {
			delete(f.tokens, id)
		}
	}
	f.tokens[row.ID] = row
	return nil
}

func (f *fakeEnrollStore) ListValidJoinTokens(now int64) ([]store.JoinTokenRow, error) {
	var out []store.JoinTokenRow
	for id, r := range f.tokens {
		if r.ExpiresAt <= now {
			delete(f.tokens, id)
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeEnrollStore) InsertWorker(row store.WorkerRow) error {
	f.workers[row.WorkerID] = row
	return nil
}

func (f *fakeEnrollStore) GetWorker(workerID string) (store.WorkerRow, error) {
	row, ok := f.workers[workerID]
	if !ok {
		return store.WorkerRow{}, sql.ErrNoRows
	}
	return row, nil
}

func TestEnrollerJoinToken(t *testing.T) {
	t.Run("mint returns plaintext and default ttl", func(t *testing.T) {
		st := newFakeEnrollStore()
		e := NewEnroller(st)

		token, expiresAt, err := e.MintJoinToken(0)
		require.NoError(t, err)
		require.True(t, looksLikeJoinToken(token), "token carries mjt_ prefix: %q", token)
		require.Len(t, st.tokens, 1)
		// 0 selects the default TTL.
		wantMin := time.Now().Add(DefaultJoinTokenTTLSeconds*time.Second - 5*time.Second).Unix()
		require.GreaterOrEqual(t, expiresAt, wantMin)
	})

	t.Run("valid token authenticates, wrong one does not", func(t *testing.T) {
		st := newFakeEnrollStore()
		e := NewEnroller(st)

		token, _, err := e.MintJoinToken(time.Hour)
		require.NoError(t, err)

		require.NoError(t, e.validateJoinToken(token))
		require.ErrorIs(t, e.validateJoinToken("mjt_wrong"), ErrInvalidJoinToken)
	})

	t.Run("expired token is pruned and rejected", func(t *testing.T) {
		st := newFakeEnrollStore()
		e := NewEnroller(st)

		// Insert a token that expired one second ago.
		hash, err := HashCredential("mjt_expired")
		require.NoError(t, err)
		require.NoError(t, st.InsertJoinToken(store.JoinTokenRow{
			ID: "x", TokenHash: hash, ExpiresAt: time.Now().Add(-time.Second).Unix(),
			CreatedAt: time.Now().Add(-time.Hour).Unix(),
		}, time.Now().Add(-time.Hour).Unix()))

		require.ErrorIs(t, e.validateJoinToken("mjt_expired"), ErrInvalidJoinToken)
		require.Empty(t, st.tokens, "expired token pruned on validate")
	})
}

func TestEnrollerWorkerCredentials(t *testing.T) {
	t.Run("enroll then authenticate round-trips", func(t *testing.T) {
		st := newFakeEnrollStore()
		e := NewEnroller(st)

		id, secret, err := e.Enroll("alpha")
		require.NoError(t, err)
		require.NotEmpty(t, id)
		require.True(t, len(secret) > len(workerSecretPrefix) && secret[:len(workerSecretPrefix)] == workerSecretPrefix)
		require.Equal(t, "alpha", st.workers[id].Name)

		require.NoError(t, e.authenticateWorker(id, secret))
	})

	t.Run("unknown worker is rejected", func(t *testing.T) {
		e := NewEnroller(newFakeEnrollStore())
		require.ErrorIs(t, e.authenticateWorker("ghost", "mws_x"), ErrUnknownWorker)
	})

	t.Run("wrong secret is rejected", func(t *testing.T) {
		st := newFakeEnrollStore()
		e := NewEnroller(st)

		id, _, err := e.Enroll("beta")
		require.NoError(t, err)
		require.ErrorIs(t, e.authenticateWorker(id, "mws_wrong"), ErrBadWorkerSecret)
	})
}

func TestNewCredentialFormat(t *testing.T) {
	jt, err := NewJoinToken()
	require.NoError(t, err)
	// mjt_ + 43 base64url chars (32 bytes, unpadded).
	require.Len(t, jt, len(joinTokenPrefix)+43)

	ws, err := NewWorkerSecret()
	require.NoError(t, err)
	require.Len(t, ws, len(workerSecretPrefix)+43)
}
