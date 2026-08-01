package queue_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestQueue(t *testing.T) *queue.Queue {
	t.Helper()
	st, err := store.Open(store.DialectSQLite, filepath.Join(t.TempDir(), "q.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return queue.NewPool(st.DB(), st.Dialect()).Open("test")
}

// Receive must drain highest-priority-first, and FIFO within a priority
// level (created-ASC tiebreak). This is the ordering the scheduler relies
// on for gateway-set job priority to mean anything once a job is on a
// worker queue.
func TestQueue_ReceiveOrdersByPriorityThenFIFO(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	// Submit deliberately out of priority order, with two MEDIUM jobs to
	// pin the within-level FIFO tiebreak.
	submit := func(id string, p queue.Priority) {
		_, err := q.Submit(ctx, queue.Envelope{RequestID: id, Priority: p, RuntimeName: "rt"})
		require.NoError(t, err)
	}
	submit("low", queue.PriorityLow)
	submit("med-1", queue.PriorityMedium)
	submit("crit", queue.PriorityCritical)
	submit("med-2", queue.PriorityMedium)
	submit("high", queue.PriorityHigh)

	want := []string{"crit", "high", "med-1", "med-2", "low"}
	var got []string
	for range want {
		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg, "queue drained early")
		env, err := queue.UnmarshalEnvelope(msg.Body)
		require.NoError(t, err)
		got = append(got, env.RequestID)
	}
	require.Equal(t, want, got,
		"dequeue order must be priority DESC, then FIFO within a level")

	empty, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, empty, "queue fully drained")
}

// Peek reports the same ordering as Receive without consuming, so the
// dispatcher scores rows in the order it would dispatch them.
func TestQueue_PeekMatchesReceiveOrder(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	_, err := q.Submit(ctx, queue.Envelope{RequestID: "lo", Priority: queue.PriorityLow, RuntimeName: "rt"})
	require.NoError(t, err)
	_, err = q.Submit(ctx, queue.Envelope{RequestID: "hi", Priority: queue.PriorityHigh, RuntimeName: "rt"})
	require.NoError(t, err)

	msgs, err := q.Peek(ctx, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	first, err := queue.UnmarshalEnvelope(msgs[0].Body)
	require.NoError(t, err)
	require.Equal(t, "hi", first.RequestID, "Peek must surface the higher-priority row first")
}
