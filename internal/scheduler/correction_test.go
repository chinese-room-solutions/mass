package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// observeWithActual sets a deterministic dispatchedAt so observeThroughput
// sees a known "actual" wall-clock, then folds the sample in. Returns after
// the record is observed (caller may still finishInflight).
func observeWithActual(t *testing.T, s *Scheduler, requestID string, actual time.Duration) {
	t.Helper()
	s.inflightMu.Lock()
	rec, ok := s.inflightByRequest[requestID]
	require.True(t, ok, "inflight record must exist")
	rec.dispatchedAt = time.Now().Add(-actual)
	s.inflightByRequest[requestID] = rec
	s.inflightMu.Unlock()
	s.observeThroughput(requestID)
}

func TestCorrectionFactor_WarmupGate(t *testing.T) {
	s, _ := newTestScheduler(t)
	// Predicted 1s, actual 2s → ratio 0.5 (worker slower than benched).
	for i := 0; i < correctionMinSamples-1; i++ {
		id := "req"
		s.startInflight(workerQueueName("w1"), id, "m", "rt", "axis", 1.0, 0)
		observeWithActual(t, s, id, 2*time.Second)
		s.finishInflight(id)
	}
	require.Equal(t, 1.0, s.correctionFactor("w1", "axis"),
		"below min samples the bench prior stands alone (factor 1.0)")

	// One more sample crosses the threshold.
	s.startInflight(workerQueueName("w1"), "last", "m", "rt", "axis", 1.0, 0)
	observeWithActual(t, s, "last", 2*time.Second)
	s.finishInflight("last")
	require.Less(t, s.correctionFactor("w1", "axis"), 1.0,
		"once warmed, a consistently-slower worker has factor < 1")
}

// The production loop is self-referential: once the factor applies,
// predictions divide by corrected throughput, so each new sample
// measures only the residual error. The observe side multiplies the
// dispatch-time factor back in so the EWMA still converges to the true
// bench-relative ratio — folding the residual directly would settle at
// sqrt(true ratio) and leave a 4x-faster worker 2x mispredicted forever.
func TestCorrectionFactor_ConvergesTowardRatio(t *testing.T) {
	s, _ := newTestScheduler(t)
	// Bench alone predicts 2s; the worker really takes 1s → true ratio 2.
	// Mirror dispatchEnvelope: the live factor shrinks each prediction.
	for i := 0; i < 50; i++ {
		predicted := 2.0 / s.correctionFactor("w1", "axis")
		s.startInflight(workerQueueName("w1"), "r", "m", "rt", "axis", predicted, 0)
		observeWithActual(t, s, "r", 1*time.Second)
		s.finishInflight("r")
	}
	require.InDelta(t, 2.0, s.correctionFactor("w1", "axis"), 0.05,
		"EWMA converges to the bench-relative ratio, not its square root")
}

func TestCorrectionFactor_Clamped(t *testing.T) {
	s, _ := newTestScheduler(t)
	// Predicted 100s, actual 0.5s → raw ratio 200, clamped to correctionClamp.
	for i := 0; i < 50; i++ {
		s.startInflight(workerQueueName("w1"), "r", "m", "rt", "axis", 100.0, 0)
		observeWithActual(t, s, "r", 500*time.Millisecond)
		s.finishInflight("r")
	}
	require.InDelta(t, correctionClamp, s.correctionFactor("w1", "axis"), 1e-6,
		"factor never exceeds the clamp regardless of outlier ratio")
}

func TestObserveThroughput_IgnoresSubThresholdAndMissingFields(t *testing.T) {
	s, _ := newTestScheduler(t)

	// Sub-threshold actual: dominated by overhead, not a throughput signal.
	s.startInflight(workerQueueName("w1"), "fast", "m", "rt", "axis", 1.0, 0)
	observeWithActual(t, s, "fast", 10*time.Millisecond)
	s.finishInflight("fast")

	// Missing axis: nothing to key on.
	s.startInflight(workerQueueName("w1"), "noaxis", "m", "rt", "", 1.0, 0)
	observeWithActual(t, s, "noaxis", 2*time.Second)
	s.finishInflight("noaxis")

	// Non-positive predicted seconds: ratio undefined, must be skipped.
	s.startInflight(workerQueueName("w1"), "zero", "m", "rt", "axis", 0, 0)
	observeWithActual(t, s, "zero", 2*time.Second)
	s.finishInflight("zero")

	// Malformed queue name → empty workerID: nothing to key on.
	s.startInflight("not-a-worker-queue", "badq", "m", "rt", "axis", 1.0, 0)
	observeWithActual(t, s, "badq", 2*time.Second)
	s.finishInflight("badq")

	s.correctionMu.Lock()
	n := len(s.throughputCorrection)
	s.correctionMu.Unlock()
	require.Zero(t, n, "sub-threshold, axis-less, zero-predicted, and keyless jobs never populate the map")
	require.Equal(t, 1.0, s.correctionFactor("w1", "axis"))
}

// observeThroughput on an unknown requestID is a no-op (the record was
// already swept), and correctionFactor with empty args returns the neutral
// 1.0 without touching the map.
func TestCorrection_NoOpEdges(t *testing.T) {
	s, _ := newTestScheduler(t)
	s.observeThroughput("never-existed") // must not panic
	require.Equal(t, 1.0, s.correctionFactor("", "axis"))
	require.Equal(t, 1.0, s.correctionFactor("w1", ""))
	s.correctionMu.Lock()
	require.Empty(t, s.throughputCorrection)
	s.correctionMu.Unlock()
}

// The low clamp floors the factor: a worker far slower than predicted
// (huge actual vs. tiny predicted) can't drive the multiplier below
// 1/correctionClamp.
func TestCorrectionFactor_ClampedLow(t *testing.T) {
	s, _ := newTestScheduler(t)
	// Predicted 0.2s, actual 100s → raw ratio 0.002, floored at 1/clamp.
	for i := 0; i < 50; i++ {
		s.startInflight(workerQueueName("w1"), "r", "m", "rt", "axis", 0.2, 0)
		observeWithActual(t, s, "r", 100*time.Second)
		s.finishInflight("r")
	}
	require.InDelta(t, 1/correctionClamp, s.correctionFactor("w1", "axis"), 1e-6,
		"factor never drops below the low clamp regardless of how slow the worker is")
}
