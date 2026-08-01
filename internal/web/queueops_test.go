package web

import (
	"context"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/stretchr/testify/require"
)

// Queue ops on the shared core. The bare scheduler in newTestHandler has no
// queues materialised, so these cover the validation branches and the
// scheduler sentinels that pass through unwrapped for the handlers' and Connect
// adapters' errors.Is switches.
func TestQueueOps_Sentinels(t *testing.T) {
	h := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
		want error
	}{
		{
			name: "cancelQueuedJob missing fields -> ErrOpInvalid",
			call: func() error { return h.cancelQueuedJob(ctx, "", "", "test") },
			want: ErrOpInvalid,
		},
		{
			name: "cancelQueuedJob unknown queue -> ErrUnknownQueue (unwrapped)",
			call: func() error { return h.cancelQueuedJob(ctx, "worker|ghost", "m1", "test") },
			want: scheduler.ErrUnknownQueue,
		},
		{
			name: "cancelRunningJob missing id -> ErrOpInvalid",
			call: func() error { return h.cancelRunningJob(ctx, "", "test") },
			want: ErrOpInvalid,
		},
		{
			name: "evictQueuedJob missing fields -> ErrOpInvalid",
			call: func() error { return h.evictQueuedJob(ctx, "", "", "test") },
			want: ErrOpInvalid,
		},
		{
			name: "evictQueuedJob unknown queue -> ErrUnknownQueue (unwrapped)",
			call: func() error { return h.evictQueuedJob(ctx, "worker|ghost", "m1", "test") },
			want: scheduler.ErrUnknownQueue,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.call(), tt.want)
		})
	}
}

func TestQueueOps_SnapshotEmptyOnBareScheduler(t *testing.T) {
	h := newTestHandler(t)
	sections, err := h.queueSnapshot(context.Background())
	require.NoError(t, err)
	require.Empty(t, sections)
}
