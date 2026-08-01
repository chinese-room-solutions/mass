package queue_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T) *queue.Pool {
	t.Helper()
	st, err := store.Open(store.DialectSQLite, filepath.Join(t.TempDir(), "q.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return queue.NewPool(st.DB(), st.Dialect())
}

// MoveTo must only move pending rows. A row a dispatcher holds under
// lease is mid-execution — moving it would run the job twice — and a
// past-budget row belongs to the abandoned-row sweeper. Both refuse
// with (false, nil), same as race-loss.
func TestQueue_MoveTo_OnlyMovesPendingRows(t *testing.T) {
	tests := []struct {
		name string
		// stage prepares the row's lease/budget state after submit.
		stage     func(t *testing.T, src *queue.Queue, id queue.MessageID)
		wantMoved bool
	}{
		{
			name:      "pending row moves",
			stage:     func(*testing.T, *queue.Queue, queue.MessageID) {},
			wantMoved: true,
		},
		{
			name: "leased row refused",
			stage: func(t *testing.T, src *queue.Queue, id queue.MessageID) {
				t.Helper()
				leased, err := src.LeaseByID(context.Background(), id, time.Minute)
				require.NoError(t, err)
				require.NotNil(t, leased)
			},
			wantMoved: false,
		},
		{
			name: "past-budget row refused",
			stage: func(t *testing.T, src *queue.Queue, id queue.MessageID) {
				t.Helper()
				// A negative lease leaves the row visible-looking (timeout in
				// the past) but with its delivery budget spent — the shape a
				// crash leaves behind for the sweeper.
				leased, err := src.LeaseByID(context.Background(), id, -time.Second)
				require.NoError(t, err)
				require.NotNil(t, leased)
			},
			wantMoved: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newTestPool(t)
			src := pool.Open("src")
			dst := pool.Open("dst")
			ctx := context.Background()

			res, err := src.Submit(ctx, queue.Envelope{
				Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid", Payload: []byte("p"),
			})
			require.NoError(t, err)
			tt.stage(t, src, queue.MessageID(res.ID))

			moved, err := src.MoveTo(ctx, dst, queue.MessageID(res.ID), queue.PriorityMedium)
			require.NoError(t, err)
			require.Equal(t, tt.wantMoved, moved)

			srcRows, err := src.PeekAll(ctx, 10)
			require.NoError(t, err)
			dstDepth, err := dst.Depth(ctx)
			require.NoError(t, err)
			if tt.wantMoved {
				require.Empty(t, srcRows, "moved row must be gone from src")
				require.Equal(t, 1, dstDepth, "moved row must land on dst")
			} else {
				require.Len(t, srcRows, 1, "refused row must stay on src")
				require.Zero(t, dstDepth, "refused row must not appear on dst")
			}
		})
	}
}
