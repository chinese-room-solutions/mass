package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResultStore_CreateAndGet(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-abc")
	require.NoError(t, err)

	r, err := store.Get("req-1")
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, "req-1", r.ID)
	require.Equal(t, "hash-abc", r.RequestHash)
	require.Equal(t, ResultStatusPending, r.Status)
	require.Nil(t, r.Body)
	require.Nil(t, r.CompletedAt)
}

func TestResultStore_GetNotFound(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	r, err := store.Get("nonexistent")
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestResultStore_Lifecycle(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-abc")
	require.NoError(t, err)

	// Mark processing.
	err = store.MarkProcessing("req-1")
	require.NoError(t, err)

	r, _ := store.Get("req-1")
	require.Equal(t, ResultStatusProcessing, r.Status)

	// Complete.
	body := []byte("result-data")
	err = store.Complete("req-1", body)
	require.NoError(t, err)

	r, _ = store.Get("req-1")
	require.Equal(t, ResultStatusDone, r.Status)
	require.Equal(t, body, r.Body)
	require.NotNil(t, r.CompletedAt)
}

func TestResultStore_Fail(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-abc")
	require.NoError(t, err)

	err = store.Fail("req-1", "something went wrong")
	require.NoError(t, err)

	r, _ := store.Get("req-1")
	require.Equal(t, ResultStatusError, r.Status)
	require.Equal(t, "something went wrong", r.Error)
	require.NotNil(t, r.CompletedAt)
}

func TestResultStore_FindByHash_CacheHit(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-xyz")
	require.NoError(t, err)

	body := []byte("cached-response")
	err = store.Complete("req-1", body)
	require.NoError(t, err)

	// Should find the cached result.
	r, err := store.FindByHash("hash-xyz", 24*time.Hour)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, "req-1", r.ID)
	require.Equal(t, body, r.Body)
}

func TestResultStore_FindByHash_CacheMiss(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	// No results at all.
	r, err := store.FindByHash("hash-xyz", 24*time.Hour)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestResultStore_FindByHash_PendingNotReturned(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-xyz")
	require.NoError(t, err)

	// Pending results should not be returned as cache hits.
	r, err := store.FindByHash("hash-xyz", 24*time.Hour)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestResultStore_Cleanup(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-1")
	require.NoError(t, err)
	err = store.Complete("req-1", []byte("data"))
	require.NoError(t, err)

	// Wait briefly so the created_at timestamp is in the past relative to the cutoff.
	time.Sleep(10 * time.Millisecond)

	// Cleanup with a 1ms TTL should remove the result created > 1ms ago.
	n, err := store.Cleanup(time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	r, _ := store.Get("req-1")
	require.Nil(t, r)
}

func TestResultStore_WaitForResult(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-abc")
	require.NoError(t, err)

	// Complete the result in a goroutine after a small delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = store.Complete("req-1", []byte("done"))
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
	}()
	// Use a channel that won't close until we're done waiting.
	neverClose := make(chan struct{})
	r, err := store.WaitForResult("req-1", 10*time.Millisecond, neverClose)
	require.NoError(t, err)
	require.Equal(t, ResultStatusDone, r.Status)
	require.Equal(t, []byte("done"), r.Body)
}

func TestResultStore_WaitForResult_Cancelled(t *testing.T) {
	db := setupTestDB(t)
	store := NewResultStore(db)

	err := store.Create("req-1", "hash-abc")
	require.NoError(t, err)

	// Cancel immediately.
	cancelled := make(chan struct{})
	close(cancelled)

	r, err := store.WaitForResult("req-1", 10*time.Millisecond, cancelled)
	require.Error(t, err)
	require.Nil(t, r)
}
