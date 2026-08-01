package scheduler

import (
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// foldSlowSamples pushes n completed jobs through the live correction
// loop for (workerID, axis), each predicted at 1s but measured at 2s —
// a worker running consistently slower than benched.
func foldSlowSamples(t *testing.T, s *Scheduler, workerID, axis string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		s.startInflight(workerQueueName(workerID), "r", "m", "rt", axis, 1.0, 0)
		observeWithActual(t, s, "r", 2*time.Second)
		s.finishInflight("r")
	}
}

// restartScheduler builds a second scheduler over the same store, exactly
// as a gateway restart would.
func restartScheduler(t *testing.T, st *store.Store) *Scheduler {
	t.Helper()
	s := New(&config.Config{}, zerolog.Nop(), worker.NewFleet())
	s.InitQueue(queue.NewPool(st.DB(), st.Dialect()), queue.NewResultStore(st.DB(), st.Dialect()), st)
	return s
}

func TestCorrectionPersistence_SurvivesRestart(t *testing.T) {
	s, st := newTestScheduler(t)
	// Consistently slower than benched: factor converges below 1 once
	// the sample gate opens.
	foldSlowSamples(t, s, "w1", "axis", correctionMinSamples+1)
	warmed := s.correctionFactor("w1", "axis")
	require.Less(t, warmed, 1.0)

	s2 := restartScheduler(t, st)
	require.InDelta(t, warmed, s2.correctionFactor("w1", "axis"), 1e-9,
		"restart restores the learned factor instead of re-warming from the bench prior")
}

func TestCorrectionPersistence_GateStaysClosedBelowMinSamples(t *testing.T) {
	s, st := newTestScheduler(t)
	foldSlowSamples(t, s, "w1", "axis", correctionMinSamples-1)
	require.Equal(t, 1.0, s.correctionFactor("w1", "axis"))

	s2 := restartScheduler(t, st)
	require.Equal(t, 1.0, s2.correctionFactor("w1", "axis"),
		"persisted sample counts must not reopen a gate that never opened")

	// The restored samples still count: gate opens after the remaining one.
	foldSlowSamples(t, s2, "w1", "axis", 1)
	require.Less(t, s2.correctionFactor("w1", "axis"), 1.0,
		"restored evidence plus one fresh sample crosses the threshold")
}

func TestCorrectionPersistence_SkipsStaleRows(t *testing.T) {
	s, st := newTestScheduler(t)
	foldSlowSamples(t, s, "w1", "axis", correctionMinSamples)
	require.Less(t, s.correctionFactor("w1", "axis"), 1.0)

	// Backdate the persisted row past correctionMaxAge.
	stale := time.Now().Add(-correctionMaxAge - time.Hour).UTC().Format(time.RFC3339Nano)
	_, err := st.DB().Exec(`UPDATE throughput_corrections SET updated_at = ?`, stale)
	require.NoError(t, err)

	s2 := restartScheduler(t, st)
	require.Equal(t, 1.0, s2.correctionFactor("w1", "axis"),
		"month-old evidence must not seed the EWMA")
}

func TestResetCorrections_ClearsWorkerScopedState(t *testing.T) {
	s, st := newTestScheduler(t)
	foldSlowSamples(t, s, "w1", "axis", correctionMinSamples)
	foldSlowSamples(t, s, "w1", "other", correctionMinSamples)
	foldSlowSamples(t, s, "w2", "axis", correctionMinSamples)

	s.ResetCorrections("w1")

	require.Equal(t, 1.0, s.correctionFactor("w1", "axis"))
	require.Equal(t, 1.0, s.correctionFactor("w1", "other"))
	require.Less(t, s.correctionFactor("w2", "axis"), 1.0,
		"other workers' evidence is untouched")

	rows, err := st.ListThroughputCorrections()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "w2", rows[0].WorkerID)
}

// Both baseline-change events reset the learned corrections: a fresh
// bench replaces the throughput prior, a device toggle changes the
// device set the prior sums across.
func TestCorrectionResetHooks(t *testing.T) {
	tests := []struct {
		name string
		hook func(s *Scheduler)
	}{
		{name: "fresh bench result", hook: func(s *Scheduler) { s.InvalidateBench("w1", "gpu:0") }},
		{name: "device toggle", hook: func(s *Scheduler) { s.OnWorkerDevicesChanged("w1") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			foldSlowSamples(t, s, "w1", "axis", correctionMinSamples)
			require.Less(t, s.correctionFactor("w1", "axis"), 1.0)

			tt.hook(s)

			require.Equal(t, 1.0, s.correctionFactor("w1", "axis"))
			rows, err := st.ListThroughputCorrections()
			require.NoError(t, err)
			require.Empty(t, rows, "persisted rows are gone too")
		})
	}
}
