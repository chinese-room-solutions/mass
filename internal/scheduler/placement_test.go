package scheduler

import (
	"testing"

	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

func TestScorePlacement_Affinity(t *testing.T) {
	tests := []struct {
		name      string
		candidate Candidate
		fp        string
		wantSign  int // 1 = positive, -1 = negative
	}{
		{"matching tail", Candidate{TailHash: "abc", TailLength: 5}, "abc", 1},
		{"matching tail zero length", Candidate{TailHash: "abc", TailLength: 0}, "abc", 1},
		{"different tail", Candidate{TailHash: "abc", TailLength: 5}, "def", -1},
		{"different tail zero length", Candidate{TailHash: "abc", TailLength: 0}, "def", -1},
		{"empty tail and loaded", Candidate{}, "abc", -1},
		{"loaded model matches, empty queue", Candidate{LoadedHash: "abc"}, "abc", 1},
		{"loaded model matches, different tail queued", Candidate{LoadedHash: "abc", TailHash: "other", TailLength: 2}, "abc", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := ScorePlacement(tt.candidate, tt.fp)
			switch tt.wantSign {
			case 1:
				require.Greater(t, score, 0.0)
			case -1:
				require.Less(t, score, 0.0)
			}
		})
	}
}

func TestScorePlacement_LongerTailWins_Matching(t *testing.T) {
	// When both candidates match the fingerprint, longer tail must score higher.
	short := Candidate{TailHash: "abc", TailLength: 2}
	long := Candidate{TailHash: "abc", TailLength: 10}
	require.Greater(t, ScorePlacement(long, "abc"), ScorePlacement(short, "abc"))
}

func TestScorePlacement_LongerTailWins_NonMatching(t *testing.T) {
	// When neither matches, prefer LONGEST tail — context switch deferred furthest.
	short := Candidate{TailHash: "other", TailLength: 2}
	long := Candidate{TailHash: "other", TailLength: 10}
	require.Greater(t, ScorePlacement(long, "abc"), ScorePlacement(short, "abc"),
		"longer non-matching tail should score higher (context switch deferred)")
}

func TestScorePlacement_LoadedHashBeatsEmpty(t *testing.T) {
	// A device with the model loaded (but empty queue) should beat a cold device.
	loaded := Candidate{LoadedHash: "abc"}
	cold := Candidate{}
	require.Greater(t, ScorePlacement(loaded, "abc"), ScorePlacement(cold, "abc"))
}

func TestScorePlacement_TailMatchBeatsLoadedMatch(t *testing.T) {
	// Tail match (active sequence) should beat loaded-only match (no queue).
	tail := Candidate{TailHash: "abc", TailLength: 1}
	loaded := Candidate{LoadedHash: "abc"}
	require.Greater(t, ScorePlacement(tail, "abc"), ScorePlacement(loaded, "abc"))
}

func TestScorePlacement_MatchAlwaysBeatsNonMatch(t *testing.T) {
	// Any matching candidate must always beat any non-matching candidate,
	// regardless of tail lengths.
	match := Candidate{TailHash: "abc", TailLength: 0}       // weakest match
	noMatch := Candidate{TailHash: "other", TailLength: 100} // strongest non-match
	require.Greater(t, ScorePlacement(match, "abc"), ScorePlacement(noMatch, "abc"),
		"any tail match must beat any non-match")
}

func TestSelectBestCandidate(t *testing.T) {
	t.Run("prefers affinity", func(t *testing.T) {
		candidates := []Candidate{
			{QueueName: "gpu0", TailHash: "other", TailLength: 0, GFlops: 1000},
			{QueueName: "gpu1", TailHash: "abc", TailLength: 3, GFlops: 500},
		}
		best := SelectBestCandidate(candidates, "abc")
		require.Equal(t, "gpu1", best.QueueName)
	})

	t.Run("tiebreak by GFlops", func(t *testing.T) {
		candidates := []Candidate{
			{QueueName: "gpu0", TailHash: "", TailLength: 0, GFlops: 500},
			{QueueName: "gpu1", TailHash: "", TailLength: 0, GFlops: 1000},
		}
		best := SelectBestCandidate(candidates, "abc")
		require.Equal(t, "gpu1", best.QueueName)
	})

	t.Run("prefers longest non-matching tail", func(t *testing.T) {
		candidates := []Candidate{
			{QueueName: "gpu0", TailHash: "other", TailLength: 10, GFlops: 1000},
			{QueueName: "gpu1", TailHash: "other2", TailLength: 2, GFlops: 500},
		}
		best := SelectBestCandidate(candidates, "abc")
		require.Equal(t, "gpu0", best.QueueName) // longest tail = context switch deferred furthest
	})

	t.Run("empty returns nil", func(t *testing.T) {
		require.Nil(t, SelectBestCandidate(nil, "abc"))
	})
}

func TestFindCandidates_SingleDevice(t *testing.T) {
	devices := []DeviceInfo{
		{AgentID: "local", DeviceID: "gpu:0", TotalMemoryMB: 8000, GFlops: 1800},
		{AgentID: "local", DeviceID: "gpu:1", TotalMemoryMB: 4000, GFlops: 900},
	}
	states := []store.DeviceQueueState{
		{QueueName: "device:local:gpu:0", TailHash: "abc", TailLength: 5},
	}

	// Model fits on gpu:0.
	candidates := FindCandidates(devices, states, 6000)
	require.Len(t, candidates, 1)
	require.Equal(t, "device:local:gpu:0", candidates[0].QueueName)
	require.Equal(t, "abc", candidates[0].TailHash)
}

func TestFindCandidates_MultiDevice(t *testing.T) {
	devices := []DeviceInfo{
		{AgentID: "local", DeviceID: "gpu:0", TotalMemoryMB: 4000, GFlops: 1800},
		{AgentID: "local", DeviceID: "gpu:1", TotalMemoryMB: 4000, GFlops: 900},
	}

	// Model doesn't fit on any single device, but fits on both combined.
	candidates := FindCandidates(devices, nil, 6000)
	require.Len(t, candidates, 1)
	require.Len(t, candidates[0].DeviceIDs, 2)
	require.Equal(t, 8000, candidates[0].TotalMemoryMB)
}

func TestFindCandidates_NoFit(t *testing.T) {
	devices := []DeviceInfo{
		{AgentID: "local", DeviceID: "gpu:0", TotalMemoryMB: 2000, GFlops: 500},
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

func TestCalcMaxConcurrent(t *testing.T) {
	tests := []struct {
		name      string
		gflops    float64
		modelGB   float64
		totalMB   int
		modelMB   int
		kvCacheMB int
		want      int32
	}{
		{"3070 Ti 4B Q4 large KV", 1800, 2.5, 8000, 2500, 1800, 3}, // min(floor(5.14^0.75)=3, floor(5500/1800)=3) = 3
		{"3070 Ti 4B Q4 30K ctx", 1800, 2.5, 8000, 2500, 1091, 3},  // min(3, floor(5500/1091)=5) = 3
		{"3070 Ti 4B Q4 4K ctx", 1800, 2.5, 8000, 2500, 149, 3},    // min(3, floor(5500/149)=36) = 3
		{"A100 70B Q4 8K ctx", 20000, 40.0, 80000, 40000, 7450, 2}, // min(floor(3.57^0.75)=2, floor(40000/7450)=5) = 2
		{"zero gflops", 0, 2.5, 8000, 2500, 1800, 1},               // clamped to 1
		{"no kvCache info", 1800, 2.5, 8000, 2500, 0, 3},           // compute-only: floor(5.14^0.75)=3
		{"tiny model huge gpu", 10000, 0.5, 32000, 500, 100, 41},   // floor(142.86^0.75)=41, vramCap=315
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalcMaxConcurrent(tt.gflops, tt.modelGB, tt.totalMB, tt.modelMB, tt.kvCacheMB)
			require.Equal(t, tt.want, got)
		})
	}
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
