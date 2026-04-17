package scheduler

import (
	"math"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

const (
	testTaskDifficulty = 1_000_000.0
	// 4 GB; the swap term in ScoreCost is M*M, so this is intentionally
	// large enough to dominate any realistic queue backlog.
	testModelSize = 4_000_000_000
)

// TestScoreCost_AffinityIsEmergent — matching effective fingerprint must
// beat any mismatched candidate, because the swap term goes to zero. With
// taskDifficulty=0 the score reduces to TailDifficulty/GFlops + (swap if
// mismatch), so a matched empty queue scores 0, which is the floor.
func TestScoreCost_AffinityIsEmergent(t *testing.T) {
	gflops := 1000.0
	tests := []struct {
		name string
		cost float64
	}{
		{"matching tail", ScoreCost(Candidate{GFlops: gflops, TailHash: "abc"}, "abc", 0, testModelSize)},
		{"matching loaded, empty tail", ScoreCost(Candidate{GFlops: gflops, LoadedHash: "abc"}, "abc", 0, testModelSize)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.InDelta(t, 0.0, tt.cost, 1e-9, "matched candidate with empty queue should cost 0")
		})
	}

	mismatch := ScoreCost(Candidate{GFlops: gflops, TailHash: "other"}, "abc", 0, testModelSize)
	require.Greater(t, mismatch, 0.0, "mismatched candidate must cost at least the swap")
}

// TestScoreCost_BusyMatchedBeatsIdleMismatch — even a queue with prior work
// queued under the same fingerprint can beat an idle but mismatched queue,
// as long as queue wait < swap cost. This is exactly the "no false
// affinity" property: don't switch models when a same-model queue is short.
func TestScoreCost_BusyMatchedBeatsIdleMismatch(t *testing.T) {
	gflops := 1000.0
	// Matched queue with some pending work (small).
	busyMatched := Candidate{GFlops: gflops, TailHash: "abc", TailDifficulty: 1_000}
	// Idle but wrong model loaded → swap = 4GB / 1000 GFlops.
	idleMismatch := Candidate{GFlops: gflops, LoadedHash: "other"}
	require.Less(t,
		ScoreCost(busyMatched, "abc", testTaskDifficulty, testModelSize),
		ScoreCost(idleMismatch, "abc", testTaskDifficulty, testModelSize),
		"a small same-model wait should beat a fresh model swap",
	)
}

// TestScoreCost_DeepQueueLosesToSwap — if the matched queue is deep enough
// that its accumulated work exceeds a model swap (M*M with the current
// formula), the formula correctly prefers the idle-but-mismatched candidate.
// Crossover: TailDifficulty > M*M.
func TestScoreCost_DeepQueueLosesToSwap(t *testing.T) {
	gflops := 1000.0
	deepMatched := Candidate{
		GFlops:         gflops,
		TailHash:       "abc",
		TailDifficulty: 2 * float64(testModelSize) * float64(testModelSize), // > one swap's worth
	}
	idleMismatch := Candidate{GFlops: gflops, LoadedHash: "other"}
	require.Greater(t,
		ScoreCost(deepMatched, "abc", testTaskDifficulty, testModelSize),
		ScoreCost(idleMismatch, "abc", testTaskDifficulty, testModelSize),
		"a long matched queue should lose to a fresh swap",
	)
}

// TestScoreCost_TailHashOverridesLoadedHash — when the tail is non-empty,
// only its fingerprint matters. A queue with the right model loaded but a
// different-fp tail will swap before our task runs, so it incurs the swap.
func TestScoreCost_TailHashOverridesLoadedHash(t *testing.T) {
	gflops := 1000.0
	// Loaded model matches, but tail says next batch is a different model.
	c := Candidate{GFlops: gflops, LoadedHash: "abc", TailHash: "other", TailDifficulty: 100}
	cost := ScoreCost(c, "abc", 0, testModelSize)
	require.Greater(t, cost, float64(testModelSize)/gflops*0.99,
		"tail mismatch must trigger swap cost even if loaded matches")
}

// TestScoreCost_ZeroGFlopsIsUnschedulable — never benchmarked devices
// must lose to every measured candidate. Returning +Inf is the cleanest
// way to encode that.
func TestScoreCost_ZeroGFlopsIsUnschedulable(t *testing.T) {
	c := Candidate{GFlops: 0, TailHash: "abc"}
	require.True(t, math.IsInf(ScoreCost(c, "abc", 0, testModelSize), 1))
}

// TestSelectMinCost_AffinityWins — the basic "prefer same model" check
// with full state on candidates.
func TestSelectMinCost_AffinityWins(t *testing.T) {
	candidates := []Candidate{
		{QueueName: "gpu0", TailHash: "other", GFlops: 1000},
		{QueueName: "gpu1", TailHash: "abc", GFlops: 500},
	}
	best := SelectMinCost(candidates, "abc", testTaskDifficulty, testModelSize)
	require.Equal(t, "gpu1", best.QueueName)
}

// TestSelectMinCost_TiebreakByGFlops — equal cost should prefer the
// stronger device (frees its capacity sooner for the next task).
func TestSelectMinCost_TiebreakByGFlops(t *testing.T) {
	candidates := []Candidate{
		{QueueName: "gpu0", GFlops: 500},
		{QueueName: "gpu1", GFlops: 1000},
	}
	best := SelectMinCost(candidates, "abc", 0, 0)
	require.Equal(t, "gpu1", best.QueueName)
}

// TestSelectMinCost_EmptyReturnsNil — empty candidate list is a valid
// input; SelectMinCost must not panic.
func TestSelectMinCost_EmptyReturnsNil(t *testing.T) {
	require.Nil(t, SelectMinCost(nil, "abc", 0, 0))
}

// TestSelectMinCost_TiebreakPrefersLoadedMatch — when costs tie, the
// candidate that already has the requested model loaded wins over an
// unloaded one. This guards selectAvailablePlacement: with no real task
// (taskDifficulty=0, modelSizeBytes=0), the cost formula collapses to 0
// for everyone, and we must still prefer the device that holds the model.
func TestSelectMinCost_TiebreakPrefersLoadedMatch(t *testing.T) {
	candidates := []Candidate{
		{QueueName: "gpu1-empty", GFlops: 1000},                     // cost = 0, no match
		{QueueName: "gpu0-loaded", GFlops: 1000, LoadedHash: "abc"}, // cost = 0, match
	}
	best := SelectMinCost(candidates, "abc", 0, 0)
	require.Equal(t, "gpu0-loaded", best.QueueName)

	// Order-independence: same outcome regardless of slice order.
	candidates = []Candidate{
		{QueueName: "gpu0-loaded", GFlops: 1000, LoadedHash: "abc"},
		{QueueName: "gpu1-empty", GFlops: 1000},
	}
	best = SelectMinCost(candidates, "abc", 0, 0)
	require.Equal(t, "gpu0-loaded", best.QueueName)
}

// TestSelectMinCost_TiebreakLoadedBeforeGFlops — the loaded-match tiebreak
// fires before the GFlops tiebreak. A weaker device with the model loaded
// beats a stronger empty device when costs are otherwise equal: we save the
// swap unconditionally.
func TestSelectMinCost_TiebreakLoadedBeforeGFlops(t *testing.T) {
	candidates := []Candidate{
		{QueueName: "weak-loaded", GFlops: 500, LoadedHash: "abc"},
		{QueueName: "strong-empty", GFlops: 5000},
	}
	best := SelectMinCost(candidates, "abc", 0, 0)
	require.Equal(t, "weak-loaded", best.QueueName)
}

func TestFindCandidates_SingleDevice(t *testing.T) {
	devices := []DeviceInfo{
		{WorkerID: "local", DeviceID: "gpu:0", TotalMemoryMB: 8000, GFlops: 1800},
		{WorkerID: "local", DeviceID: "gpu:1", TotalMemoryMB: 4000, GFlops: 900},
	}
	states := []store.DeviceQueueState{
		{QueueName: "device:local:gpu:0", TailHash: "abc", TailDifficulty: 1234.5},
	}

	// Model fits on gpu:0.
	candidates := FindCandidates(devices, states, 6000)
	require.Len(t, candidates, 1)
	require.Equal(t, "device:local:gpu:0", candidates[0].QueueName)
	require.Equal(t, "abc", candidates[0].TailHash)
}

func TestFindCandidates_MultiDevice(t *testing.T) {
	devices := []DeviceInfo{
		{WorkerID: "local", DeviceID: "gpu:0", TotalMemoryMB: 4000, GFlops: 1800},
		{WorkerID: "local", DeviceID: "gpu:1", TotalMemoryMB: 4000, GFlops: 900},
	}

	// Model doesn't fit on any single device, but fits on both combined.
	candidates := FindCandidates(devices, nil, 6000)
	require.Len(t, candidates, 1)
	require.Len(t, candidates[0].DeviceIDs, 2)
	require.Equal(t, 8000, candidates[0].TotalMemoryMB)
}

func TestFindCandidates_NoFit(t *testing.T) {
	devices := []DeviceInfo{
		{WorkerID: "local", DeviceID: "gpu:0", TotalMemoryMB: 2000, GFlops: 500},
	}
	candidates := FindCandidates(devices, nil, 10000)
	require.Empty(t, candidates)
}

func TestCalcTensorSplit(t *testing.T) {
	t.Run("single device returns empty", func(t *testing.T) {
		require.Equal(t, "", CalcTensorSplit([]DeviceInfo{{TotalMemoryMB: 8000}}))
	})

	t.Run("two equal devices", func(t *testing.T) {
		devices := []DeviceInfo{
			{TotalMemoryMB: 4000},
			{TotalMemoryMB: 4000},
		}
		require.Equal(t, "0.50,0.50", CalcTensorSplit(devices))
	})

	t.Run("unequal devices", func(t *testing.T) {
		devices := []DeviceInfo{
			{TotalMemoryMB: 6000},
			{TotalMemoryMB: 2000},
		}
		require.Equal(t, "0.75,0.25", CalcTensorSplit(devices))
	})
}

func TestCalcGpuLayers(t *testing.T) {
	tests := []struct {
		name        string
		modelBytes  int64
		availableMB int64
		wantLayers  int32
	}{
		{"full GPU", 2 * 1024 * 1024 * 1024, 4000, 0}, // model < VRAM
		{"no GPU", 2 * 1024 * 1024 * 1024, 0, 0},      // CPU only
		{"partial", 8 * 1024 * 1024 * 1024, 4000, 48}, // ~half fits (rounding)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalcGpuLayers(tt.modelBytes, tt.availableMB)
			require.Equal(t, tt.wantLayers, got)
		})
	}
}

func TestCandidate_IsCPU(t *testing.T) {
	tests := []struct {
		name      string
		deviceIDs []string
		want      bool
	}{
		{"single CPU", []string{"cpu:0"}, true},
		{"single GPU", []string{"gpu:0"}, false},
		{"mixed CPU+GPU", []string{"cpu:0", "gpu:0"}, false},
		{"multiple GPUs", []string{"gpu:0", "gpu:1"}, false},
		{"multiple CPUs", []string{"cpu:0", "cpu:1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Candidate{DeviceIDs: tt.deviceIDs}
			require.Equal(t, tt.want, c.IsCPU())
		})
	}
}

func TestDeviceQueueName(t *testing.T) {
	require.Equal(t, "device:local:gpu:0", DeviceQueueName("local", "gpu:0"))
	require.Equal(t, "device:remote1:cpu:0", DeviceQueueName("remote1", "cpu:0"))
}

func TestDeviceGroupQueueName(t *testing.T) {
	require.Equal(t, "device:local:gpu:0+gpu:1", DeviceGroupQueueName("local", []string{"gpu:0", "gpu:1"}))
}
