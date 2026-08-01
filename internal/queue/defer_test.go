package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
)

// Defer is the dispatcher's bounce: hide the row for a beat instead of
// handing it straight back, so a consumer that can't take it yet doesn't
// spin. The row must come back, must still be leasable (MaxReceive=1 makes
// a charged bounce unreachable forever), and must not look in-flight while
// it waits.
func TestQueue_Defer_HidesRowThenBringsItBack(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	res, err := q.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid", Payload: []byte("p"),
	})
	require.NoError(t, err)
	leased, err := q.LeaseByID(ctx, queue.MessageID(res.ID), time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)

	require.NoError(t, q.Defer(ctx, queue.MessageID(res.ID), 150*time.Millisecond))

	msgs, err := q.Peek(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, msgs, "a deferred row must not be immediately visible")

	rows, err := q.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.False(t, rows[0].Leased, "a deferred row is waiting for retry, not running")

	require.Eventually(t, func() bool {
		msgs, err := q.Peek(ctx, 10)
		require.NoError(t, err)
		return len(msgs) == 1
	}, 5*time.Second, 10*time.Millisecond, "a deferred row must come back")

	again, err := q.LeaseByID(ctx, queue.MessageID(res.ID), time.Minute)
	require.NoError(t, err)
	require.NotNil(t, again, "a bounce must not consume the delivery budget")
}

func TestQueue_Defer_MissingRowIsNoOp(t *testing.T) {
	q := newTestQueue(t)
	require.NoError(t, q.Defer(context.Background(), "no-such-id", time.Second))
}
