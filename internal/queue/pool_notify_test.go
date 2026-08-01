package queue_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

// The pool-level notify channel is the dispatcher's single wake-up source:
// a Submit on ANY queue opened from the pool must signal it, and multiple
// signals with no reader must coalesce into exactly one buffered wake-up
// (capacity 1).
func TestPool_NotifyChSignalsOnAnyQueueSubmit(t *testing.T) {
	st, err := store.Open(store.DialectSQLite, filepath.Join(t.TempDir(), "q.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	pool := queue.NewPool(st.DB(), st.Dialect())
	ctx := context.Background()

	qGlobal := pool.Open("global")
	qWorker := pool.Open("worker|w1")

	drained := func() bool {
		select {
		case <-pool.NotifyCh():
			return false
		default:
			return true
		}
	}
	// Signals are sent synchronously inside Submit, so a non-blocking
	// receive right after is deterministic.
	signalled := func() bool { return !drained() }

	tests := []struct {
		name        string
		act         func(t *testing.T)
		wantSignals int // buffered wake-ups expected after act
	}{
		{
			name: "submit on one pool queue signals",
			act: func(t *testing.T) {
				t.Helper()
				_, err := qGlobal.Submit(ctx, queue.Envelope{RequestID: "r1", RuntimeName: "rt"})
				require.NoError(t, err)
			},
			wantSignals: 1,
		},
		{
			name: "submit on a sibling pool queue signals the same channel",
			act: func(t *testing.T) {
				t.Helper()
				_, err := qWorker.Submit(ctx, queue.Envelope{RequestID: "r2", RuntimeName: "rt"})
				require.NoError(t, err)
			},
			wantSignals: 1,
		},
		{
			name: "submits across queues with no reader coalesce into one wake-up",
			act: func(t *testing.T) {
				t.Helper()
				for i, q := range []*queue.Queue{qGlobal, qWorker, qGlobal} {
					_, err := q.Submit(ctx, queue.Envelope{RequestID: "rc" + string(rune('0'+i)), RuntimeName: "rt"})
					require.NoError(t, err)
				}
			},
			wantSignals: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, drained(), "channel must start empty")
			tt.act(t)
			for i := 0; i < tt.wantSignals; i++ {
				require.True(t, signalled(), "expected a buffered wake-up")
			}
			require.True(t, drained(), "no extra wake-ups may be buffered")
		})
	}
}
