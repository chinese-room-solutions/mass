package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPool_OpenSharesDB(t *testing.T) {
	db := setupTestDB(t)
	pool := NewPool(db)
	a := pool.Open("a")
	b := pool.Open("b")
	require.Same(t, db, a.db, "pool queues must share the pool's DB handle")
	require.Same(t, db, b.db, "pool queues must share the pool's DB handle")

	// MoveTo across two pool-opened queues works (same DB).
	ctx := context.Background()
	res, err := a.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "x", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	moved, err := a.MoveTo(ctx, b, MessageID(res.ID), PriorityMedium)
	require.NoError(t, err)
	require.True(t, moved)
}

// fakeIncompatibleQueue satisfies QueueInterface but is not a *Queue, so any
// atomic cross-queue operation against it must panic — it's a programmer
// error (wiring queues from different backends into one atomic op).
// All other methods are nil-embedded — they're not exercised in these tests.
type fakeIncompatibleQueue struct{ QueueInterface }

func TestMoveTo_AcrossQueuesPreservesEnvelope(t *testing.T) {
	db := setupTestDB(t)
	src := NewNamed(db, "src", MaxReceive, 30_000_000_000)
	dst := NewNamed(db, "dst", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	env := Envelope{
		Type:        RequestTypeChatCompletion,
		Priority:    PriorityHigh,
		Source:      "test",
		Fingerprint: "fp-123",
		RequestID:   "req-1",
		Payload:     []byte("payload"),
	}
	res, err := src.SubmitEnvelope(ctx, env, env.Priority)
	require.NoError(t, err)

	moved, err := src.MoveTo(ctx, dst, MessageID(res.ID), env.Priority)
	require.NoError(t, err)
	require.True(t, moved)

	// Source is empty.
	srcMsg, err := src.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, srcMsg, "source must be empty after move")

	// Destination has it, with the same envelope.
	dstMsg, err := dst.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, dstMsg)
	gotEnv, err := UnmarshalEnvelope(dstMsg.Body)
	require.NoError(t, err)
	require.Equal(t, env.RequestID, gotEnv.RequestID)
	require.Equal(t, env.Fingerprint, gotEnv.Fingerprint)
	require.Equal(t, env.Payload, gotEnv.Payload)
}

func TestMoveTo_AlreadyConsumedReturnsFalse(t *testing.T) {
	db := setupTestDB(t)
	src := NewNamed(db, "src", MaxReceive, 30_000_000_000)
	dst := NewNamed(db, "dst", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	env := Envelope{Type: RequestTypeChatCompletion, RequestID: "req-x", Payload: []byte("p")}
	res, err := src.SubmitEnvelope(ctx, env, PriorityMedium)
	require.NoError(t, err)

	// First move succeeds.
	ok, err := src.MoveTo(ctx, dst, MessageID(res.ID), PriorityMedium)
	require.NoError(t, err)
	require.True(t, ok)

	// Second move on the same ID is a race-loser, not an error.
	ok, err = src.MoveTo(ctx, dst, MessageID(res.ID), PriorityMedium)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMoveTo_PanicsOnIncompatibleQueue(t *testing.T) {
	db := setupTestDB(t)
	src := NewNamed(db, "src", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	res, err := src.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "req-y", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	require.PanicsWithValue(t,
		"queue: expected *Queue, got queue.fakeIncompatibleQueue — atomic cross-queue operations require both queues from the same SQL backend",
		func() { _, _ = src.MoveTo(ctx, fakeIncompatibleQueue{}, MessageID(res.ID), PriorityMedium) },
	)
}

func TestMoveTo_PanicsOnDifferentDB(t *testing.T) {
	db1 := setupTestDB(t)
	db2 := setupTestDB(t)
	src := NewNamed(db1, "src", MaxReceive, 30_000_000_000)
	dst := NewNamed(db2, "dst", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	res, err := src.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "req-z", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	require.PanicsWithValue(t,
		`queue: "src" and "dst" are backed by different databases — atomic cross-queue operations require both queues to share one *sql.DB`,
		func() { _, _ = src.MoveTo(ctx, dst, MessageID(res.ID), PriorityMedium) },
	)
}

func TestReleaseLeaseAndDelete_ReleasesOtherAndDeletesSelf(t *testing.T) {
	db := setupTestDB(t)
	device := NewNamed(db, "device", MaxReceive, 30_000_000_000)
	global := NewNamed(db, "global", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	// Submit to global, lease it (so it's not visible), then submit a
	// device-side row that references it.
	gRes, err := global.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "g-1", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)
	gMsg, err := global.Receive(ctx) // holds the lease
	require.NoError(t, err)
	require.NotNil(t, gMsg)

	// Confirm global is invisible right now.
	again, err := global.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, again, "global row must be leased and not re-receivable")

	dRes, err := device.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "g-1", GlobalMsgID: gRes.ID, Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	// Atomic release of global lease + delete of device row.
	require.NoError(t, device.ReleaseLeaseAndDelete(ctx, MessageID(dRes.ID), global, MessageID(gRes.ID)))

	// Device row gone.
	dCheck, err := device.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, dCheck)

	// Global row visible again.
	gReceived, err := global.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, gReceived, "global row must be re-receivable after lease release")
}

func TestReleaseLeaseAndDelete_PanicsOnIncompatibleQueue(t *testing.T) {
	db := setupTestDB(t)
	device := NewNamed(db, "device", MaxReceive, 30_000_000_000)
	ctx := context.Background()

	res, err := device.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "d-x", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	require.Panics(t, func() {
		_ = device.ReleaseLeaseAndDelete(ctx, MessageID(res.ID), fakeIncompatibleQueue{}, "g-x")
	})
}

// TestLeaseByID_LeasesWithoutDeleting confirms the lease holds the row
// invisible for the lease duration but leaves it in the table — Extend,
// Delete, and ReleaseLeaseAndDelete all rely on the row still existing.
func TestLeaseByID_LeasesWithoutDeleting(t *testing.T) {
	db := setupTestDB(t)
	q := NewNamed(db, "lease-test", MaxReceive, 30*time.Second)
	ctx := context.Background()

	res, err := q.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "r-1", Payload: []byte("data"),
	}, PriorityMedium)
	require.NoError(t, err)

	leased, err := q.LeaseByID(ctx, MessageID(res.ID), 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, leased)
	gotEnv, err := UnmarshalEnvelope(leased.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), gotEnv.Payload)

	// While leased: invisible to Receive and to a second LeaseByID.
	got, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "leased row must not be re-receivable")

	again, err := q.LeaseByID(ctx, MessageID(res.ID), 30*time.Second)
	require.NoError(t, err)
	require.Nil(t, again, "leased row must not be re-leasable")

	// Row still exists (so Extend / Delete / ReleaseLeaseAndDelete will work).
	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "Depth counts visible rows; leased row hides")

	require.NoError(t, q.Delete(ctx, MessageID(res.ID)))
}

// TestLeaseByID_BlocksAfterMaxReceive verifies the same delivery-budget
// guard goqite's Receive uses: a row that's already had its allowed
// deliveries cannot be leased again. This is what stops a
// crash-recovered-then-reaped row from being re-dispatched.
func TestLeaseByID_BlocksAfterMaxReceive(t *testing.T) {
	db := setupTestDB(t)
	q := NewNamed(db, "budget-test", MaxReceive, 50*time.Millisecond)
	ctx := context.Background()

	res, err := q.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "r-1", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	// First lease succeeds and bumps received to 1 (== MaxReceive).
	first, err := q.LeaseByID(ctx, MessageID(res.ID), 1*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, first)

	// Wait past the lease so visibility wouldn't block us.
	time.Sleep(20 * time.Millisecond)

	// Second lease must fail: budget exhausted.
	again, err := q.LeaseByID(ctx, MessageID(res.ID), 30*time.Second)
	require.NoError(t, err)
	require.Nil(t, again, "row past MaxReceive must not be re-leased")
}

// TestLeaseAndSubmit_AtomicHandoff verifies the dispatcher's intended
// invariant: the global row is leased AND the device row is inserted in
// one transaction. After commit, Extend on the global ID still works
// (proving the row survives), and Receive on the device queue returns the
// inserted envelope.
func TestLeaseAndSubmit_AtomicHandoff(t *testing.T) {
	db := setupTestDB(t)
	pool := NewPool(db)
	global := pool.Open("global")
	device := pool.Open("device:gpu:0")
	ctx := context.Background()

	gRes, err := global.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "r-1", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	env := Envelope{
		Type: RequestTypeChatCompletion, RequestID: "r-1",
		GlobalMsgID: gRes.ID, Payload: []byte("p"),
	}
	dRes, leased, err := global.LeaseAndSubmit(ctx, MessageID(gRes.ID), 30*time.Second, device, env, PriorityMedium)
	require.NoError(t, err)
	require.True(t, leased)
	require.NotEmpty(t, dRes.ID)

	// Global row is leased but still alive — Extend must succeed.
	require.NoError(t, global.Extend(ctx, MessageID(gRes.ID), 30*time.Second))

	// Device row exists and carries the envelope.
	dMsg, err := device.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, dMsg)
	gotEnv, err := UnmarshalEnvelope(dMsg.Body)
	require.NoError(t, err)
	require.Equal(t, gRes.ID, gotEnv.GlobalMsgID)

	// Cleanup.
	require.NoError(t, global.Delete(ctx, MessageID(gRes.ID)))
	require.NoError(t, device.Delete(ctx, dMsg.ID))
}

// TestLeaseAndSubmit_RaceLoserDoesNothing — when the source row is already
// leased, LeaseAndSubmit must report leased=false and leave both queues
// untouched (no orphan device row).
func TestLeaseAndSubmit_RaceLoserDoesNothing(t *testing.T) {
	db := setupTestDB(t)
	pool := NewPool(db)
	global := pool.Open("global")
	device := pool.Open("device:gpu:0")
	ctx := context.Background()

	gRes, err := global.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, RequestID: "r-1", Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	// First lease wins.
	first, err := global.LeaseByID(ctx, MessageID(gRes.ID), 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, first)

	// Second LeaseAndSubmit must fail to lease and not insert anything.
	_, leased, err := global.LeaseAndSubmit(ctx, MessageID(gRes.ID), 30*time.Second, device, Envelope{
		Type: RequestTypeChatCompletion, Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)
	require.False(t, leased, "second LeaseAndSubmit must report race-loser")

	// Device queue is empty.
	depth, err := device.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "no orphan device row on race-loser")
}

// TestLeaseAndSubmit_PanicsOnIncompatibleQueue mirrors the MoveTo /
// ReleaseLeaseAndDelete protection: cross-DB wiring is a programmer error,
// not a runtime condition.
func TestLeaseAndSubmit_PanicsOnIncompatibleQueue(t *testing.T) {
	db := setupTestDB(t)
	src := NewNamed(db, "src", MaxReceive, 30*time.Second)
	ctx := context.Background()

	res, err := src.SubmitEnvelope(ctx, Envelope{
		Type: RequestTypeChatCompletion, Payload: []byte("p"),
	}, PriorityMedium)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _, _ = src.LeaseAndSubmit(ctx, MessageID(res.ID), 30*time.Second, fakeIncompatibleQueue{}, Envelope{}, PriorityMedium)
	})
}
