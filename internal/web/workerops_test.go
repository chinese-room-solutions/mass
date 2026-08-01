package web

import (
	"context"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/stretchr/testify/require"
)

// Worker ops on the shared core, exercised without a live fleet: the input
// validation and not-found branches every transport maps from. The deep happy
// path (real StreamWorker toggle + scheduler drain) is covered by the
// scheduler tests and the browser E2E flow.
func TestWorkerOps_Sentinels(t *testing.T) {
	h := newTestHandler(t) // empty fleet, real store + bare scheduler
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
		want error
	}{
		{
			name: "setWorkerDeviceEnabled missing ids -> ErrOpInvalid",
			call: func() error { return h.setWorkerDeviceEnabled(ctx, "", "", true, "test") },
			want: ErrOpInvalid,
		},
		{
			name: "setWorkerDeviceEnabled unknown worker -> ErrOpNotFound",
			call: func() error { return h.setWorkerDeviceEnabled(ctx, "ghost", "gpu0", true, "test") },
			want: ErrOpNotFound,
		},
		{
			name: "setWorkerEnabled missing id -> ErrOpInvalid",
			call: func() error { return h.setWorkerEnabled(ctx, "", true, "test") },
			want: ErrOpInvalid,
		},
		{
			name: "setWorkerEnabled unknown worker -> ErrOpNotFound",
			call: func() error { return h.setWorkerEnabled(ctx, "ghost", true, "test") },
			want: ErrOpNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.call(), tt.want)
		})
	}
}

func TestWorkerOps_ReadsAreEmpty(t *testing.T) {
	h := newTestHandler(t)

	t.Run("workerInfos empty with no fleet", func(t *testing.T) {
		require.Empty(t, h.workerInfos())
	})

	t.Run("benchmarkWorkers no-op with no online workers", func(t *testing.T) {
		require.Empty(t, h.benchmarkWorkers(nil, nil))
	})

	t.Run("deviceEnabled defaults to enabled for an absent row", func(t *testing.T) {
		require.True(t, h.deviceEnabled("ghost", "gpu0"))
	})
}

// workerInfos must surface the registration-reported version + compatible range
// so the dashboard can show worker versions and flag pre-upgrade incompatibility.
func TestWorkerInfos_SurfacesVersionCompatible(t *testing.T) {
	h := newTestHandler(t)

	w := worker.NewFakeStreamWorker("w1", "llama-cpp", nil, time.Now())
	w.SetFakeVersionCompat("0.1.0", ">=0.1 <0.2")
	require.NoError(t, h.workers.Register(w))

	infos := h.workerInfos()
	require.Len(t, infos, 1)
	require.Equal(t, "0.1.0", infos[0].Version)
	require.Equal(t, ">=0.1 <0.2", infos[0].Compatible)
	require.Equal(t, "llama-cpp", infos[0].RuntimeName)
}
