package queue

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// makeAbandoned writes a queue row with received >= MaxReceive and an
// already-expired timeout, simulating a message whose only delivery
// attempt failed (consumer never called Delete) and whose lease has now
// lapsed. Returns the message ID written.
//
// Reaches into the underlying SQL store directly because there is no
// QueueInterface method to fast-forward retry/lease state — that's a
// test-only concern.
func makeAbandoned(t *testing.T, q *Queue, env Envelope) string {
	t.Helper()
	res, err := q.SubmitEnvelope(context.Background(), env, PriorityMedium)
	require.NoError(t, err)
	expired := time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02T15:04:05.000Z")
	_, err = q.db.Exec(`UPDATE goqite SET received = ?, timeout = ? WHERE id = ?`,
		MaxReceive, expired, res.ID)
	require.NoError(t, err)
	return res.ID
}

func TestReapAbandoned_AbandonedMessageBecomesError(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)
	ctx := context.Background()

	env := Envelope{
		Type:      RequestTypeChatCompletion,
		RequestID: "req-1",
		Payload:   []byte("body"),
	}
	gqID := makeAbandoned(t, q, env)
	require.NoError(t, results.Create(env.RequestID, "hash-1"))

	n, err := ReapAbandoned(ctx, []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	r, err := results.Get(env.RequestID)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, ResultStatusError, r.Status)
	require.Contains(t, r.Error, "abandoned")

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM goqite WHERE id = ?`, gqID).Scan(&count))
	require.Zero(t, count)
}

func TestReapAbandoned_NotAbandoned_LeaseStillHeld(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)

	env := Envelope{Type: RequestTypeChatCompletion, RequestID: "req-2", Payload: []byte("p")}
	res, err := q.SubmitEnvelope(context.Background(), env, PriorityMedium)
	require.NoError(t, err)

	// Simulate "currently being processed": received budget exhausted but
	// the lease is still in the future.
	future := time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05.000Z")
	_, err = db.Exec(`UPDATE goqite SET received = ?, timeout = ? WHERE id = ?`,
		MaxReceive, future, res.ID)
	require.NoError(t, err)
	require.NoError(t, results.Create(env.RequestID, "hash-2"))

	n, err := ReapAbandoned(context.Background(), []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Zero(t, n, "lease still held → reaper must skip")

	r, err := results.Get(env.RequestID)
	require.NoError(t, err)
	require.Equal(t, ResultStatusPending, r.Status)
}

func TestReapAbandoned_FallsBackToMessageIDWhenEnvelopeUnreadable(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)
	ctx := context.Background()

	// Insert a row with garbage body directly so UnmarshalEnvelope fails.
	expired := time.Now().UTC().Add(-time.Hour).Format("2006-01-02T15:04:05.000Z")
	gqID := "m_garbage"
	_, err := db.Exec(`
		INSERT INTO goqite (id, queue, body, timeout, received)
		VALUES (?, 'global', ?, ?, ?)`,
		gqID, []byte{0xff}, expired, MaxReceive,
	)
	require.NoError(t, err)
	require.NoError(t, results.Create(gqID, "hash-x"))

	n, err := ReapAbandoned(ctx, []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	r, err := results.Get(gqID)
	require.NoError(t, err)
	require.Equal(t, ResultStatusError, r.Status)
}

func TestReapAbandoned_NoQueuesIsClean(t *testing.T) {
	db := setupTestDB(t)
	n, err := ReapAbandoned(context.Background(), nil, NewResultStore(db), zerolog.Nop())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestReapAbandoned_NoRowsIsClean(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	n, err := ReapAbandoned(context.Background(), []QueueInterface{q}, NewResultStore(db), zerolog.Nop())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestReapAbandoned_DeletesRowEvenWhenResultRowMissing(t *testing.T) {
	// If results.Create was never called (caller crashed between Submit
	// and Create), Fail is a no-op UPDATE — the reaper still deletes the
	// row so it doesn't accumulate.
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)

	env := Envelope{Type: RequestTypeChatCompletion, RequestID: "req-orphan", Payload: []byte("p")}
	gqID := makeAbandoned(t, q, env)

	n, err := ReapAbandoned(context.Background(), []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM goqite WHERE id = ?`, gqID).Scan(&count))
	require.Zero(t, count, "abandoned row must be deleted regardless of result-row presence")
}

func TestReapAbandoned_PreservesDoneResult(t *testing.T) {
	// Race scenario: worker reported success and results.Complete ran,
	// but the queue Delete didn't land before the process died. The
	// reaper finds the row but must NOT overwrite the Done result.
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)

	env := Envelope{Type: RequestTypeChatCompletion, RequestID: "req-done", Payload: []byte("p")}
	gqID := makeAbandoned(t, q, env)
	require.NoError(t, results.Create(env.RequestID, "hash-done"))
	require.NoError(t, results.Complete(env.RequestID, []byte("real-response")))

	n, err := ReapAbandoned(context.Background(), []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 1, n, "row still gets deleted")

	r, err := results.Get(env.RequestID)
	require.NoError(t, err)
	require.Equal(t, ResultStatusDone, r.Status, "Done result must survive")
	require.Equal(t, []byte("real-response"), r.Body)
	require.Empty(t, r.Error)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM goqite WHERE id = ?`, gqID).Scan(&count))
	require.Zero(t, count)
}

func TestReapAbandoned_PreservesEarlierError(t *testing.T) {
	// If a real error was already recorded (e.g. worker reported failure
	// before the process died), don't clobber it with the generic
	// "abandoned" message — the original error is more useful.
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)

	env := Envelope{Type: RequestTypeChatCompletion, RequestID: "req-real-err", Payload: []byte("p")}
	makeAbandoned(t, q, env)
	require.NoError(t, results.Create(env.RequestID, "hash-err"))
	require.NoError(t, results.Fail(env.RequestID, "model OOM"))

	n, err := ReapAbandoned(context.Background(), []QueueInterface{q}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	r, err := results.Get(env.RequestID)
	require.NoError(t, err)
	require.Equal(t, ResultStatusError, r.Status)
	require.Equal(t, "model OOM", r.Error, "original error must be preserved")
}

func TestReapAbandoned_AcrossMultipleQueues(t *testing.T) {
	// Multiple queues each carry their own abandoned messages — the reaper
	// reaps every one and the count is summed.
	db := setupTestDB(t)
	results := NewResultStore(db)
	g := New(db)
	d1 := NewNamed(db, "device:local:gpu:0", MaxReceive, 30*time.Second)
	d2 := NewNamed(db, "device:local:gpu:1", MaxReceive, 30*time.Second)

	mk := func(q *Queue, reqID string) {
		env := Envelope{Type: RequestTypeChatCompletion, RequestID: reqID, Payload: []byte("p")}
		makeAbandoned(t, q, env)
		require.NoError(t, results.Create(reqID, "h-"+reqID))
	}
	mk(g, "g-1")
	mk(d1, "d1-1")
	mk(d1, "d1-2")
	mk(d2, "d2-1")

	n, err := ReapAbandoned(context.Background(), []QueueInterface{g, d1, d2}, results, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, 4, n)

	for _, id := range []string{"g-1", "d1-1", "d1-2", "d2-1"} {
		r, err := results.Get(id)
		require.NoError(t, err)
		require.Equal(t, ResultStatusError, r.Status, "result for %s should be Error", id)
	}
}
