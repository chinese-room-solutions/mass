package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/store"
)

// DeviceInfo describes a compute device for placement decisions.
type DeviceInfo struct {
	WorkerID      string
	DeviceID      string
	TotalMemoryMB int
	GFlops        float64
}

// Candidate represents a possible placement for a task.
//
// `Manager` is resolved at scoring time inside [Dispatcher.collectCandidates]
// while the dispatcher lock is held, eliminating the second lookup that
// could race against worker disconnect. May be nil for candidates produced
// outside the dispatcher (tests, helper paths) — callers that need the
// manager must check before using it.
type Candidate struct {
	WorkerID       string
	DeviceIDs      []string // single device or multi-device for tensor split
	QueueName      string
	GFlops         float64 // min of group for tensor split
	TotalMemoryMB  int     // sum of group
	TailHash       string
	TailDifficulty float64 // running sum of difficulty for queued tasks
	LoadedHash     string
	Manager        *DeviceQueueManager
}

// IsCPU returns true if all devices in this candidate are CPU (no GPU offload possible).
func (c *Candidate) IsCPU() bool {
	for _, id := range c.DeviceIDs {
		if !strings.HasPrefix(id, "cpu:") {
			return false
		}
	}
	return true
}

// ScoreCost estimates how long the candidate would take to complete a fresh
// task — smaller is better.
//
//	cost = (queueWait + execTime + swapCost) / GFlops
//	queueWait = TailDifficulty                  (sum of queued M*I)
//	execTime  = taskDifficulty = M * I          (this task's heaviness)
//	swapCost  = M * M, only on a model swap
//
// The M*M swap term keeps swap and wait/exec dimensionally consistent —
// all three are bytes × bytes before normalization. Conceptually it treats
// a model load as a fictitious task that runs the whole model through
// itself; rough, but in the same units, so the weighting is honest.
//
// Swap is decided by the **effective fingerprint**: with a non-empty tail
// it's the tail's fingerprint (what the device will be running by the time
// our task reaches the front), otherwise the currently loaded model.
//
// Affinity is emergent: matching effective fingerprint → swap cost = 0.
// No magic constants, no separate "match" / "no match" tiers.
//
// GFlops <= 0 (never benchmarked) returns +Inf so it loses to every
// measured candidate.
func ScoreCost(c Candidate, taskFingerprint string, taskDifficulty float64, modelSizeBytes uint64) float64 {
	if c.GFlops <= 0 {
		return math.Inf(1)
	}

	effectiveFP := c.TailHash
	if effectiveFP == "" {
		effectiveFP = c.LoadedHash
	}

	cost := c.TailDifficulty + taskDifficulty
	if effectiveFP != taskFingerprint {
		m := float64(modelSizeBytes)
		cost += m * m
	}
	return cost / c.GFlops
}

// SelectMinCost picks the candidate with the lowest [ScoreCost]. Ties
// break by:
//  1. LoadedHash already matches taskFingerprint — reusing a loaded model
//     beats triggering a swap even when the cost number is the same
//     (common when modelSizeBytes is unknown or wait dominates swap).
//  2. Higher GFlops — frees the stronger device sooner for the next task.
//
// Returns nil if candidates is empty.
func SelectMinCost(candidates []Candidate, taskFingerprint string, taskDifficulty float64, modelSizeBytes uint64) *Candidate {
	if len(candidates) == 0 {
		return nil
	}
	best := 0
	bestCost := ScoreCost(candidates[0], taskFingerprint, taskDifficulty, modelSizeBytes)
	for i := 1; i < len(candidates); i++ {
		cost := ScoreCost(candidates[i], taskFingerprint, taskDifficulty, modelSizeBytes)
		if cost < bestCost {
			best = i
			bestCost = cost
			continue
		}
		if cost > bestCost {
			continue
		}
		// Equal cost: prefer already-loaded match, then higher GFlops.
		bestLoaded := candidates[best].LoadedHash == taskFingerprint
		thisLoaded := candidates[i].LoadedHash == taskFingerprint
		if thisLoaded && !bestLoaded {
			best = i
			continue
		}
		if !thisLoaded && bestLoaded {
			continue
		}
		if candidates[i].GFlops > candidates[best].GFlops {
			best = i
		}
	}
	return &candidates[best]
}

// FindCandidates returns possible placements for a model with the given VRAM requirement.
// It tries single-device first, then multi-device tensor splits on the same agent.
func FindCandidates(
	devices []DeviceInfo,
	queueStates []store.DeviceQueueState,
	modelVRAMMB int64,
) []Candidate {
	// Build lookup: queueName → state.
	stateByQueue := make(map[string]store.DeviceQueueState, len(queueStates))
	for _, s := range queueStates {
		stateByQueue[s.QueueName] = s
	}

	// Build lookup: workerID → devices.
	workerDevices := make(map[string][]DeviceInfo)
	for _, d := range devices {
		workerDevices[d.WorkerID] = append(workerDevices[d.WorkerID], d)
	}

	var candidates []Candidate

	// Single-device candidates.
	for _, d := range devices {
		if int64(d.TotalMemoryMB) >= modelVRAMMB {
			qn := DeviceQueueName(d.WorkerID, d.DeviceID)
			st := stateByQueue[qn]
			candidates = append(candidates, Candidate{
				WorkerID:       d.WorkerID,
				DeviceIDs:      []string{d.DeviceID},
				QueueName:      qn,
				GFlops:         d.GFlops,
				TotalMemoryMB:  d.TotalMemoryMB,
				TailHash:       st.TailHash,
				TailDifficulty: st.TailDifficulty,
				LoadedHash:     st.LoadedHash,
			})
		}
	}

	if len(candidates) > 0 {
		return candidates
	}

	// Multi-device candidates (tensor split) — only if no single device fits.
	for workerID, devs := range workerDevices {
		// Sort devices by VRAM descending for greedy grouping.
		sorted := make([]DeviceInfo, len(devs))
		copy(sorted, devs)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].TotalMemoryMB > sorted[j].TotalMemoryMB
		})

		// Try groups of increasing size (2, 3, ...).
		for groupSize := 2; groupSize <= len(sorted); groupSize++ {
			group := sorted[:groupSize]
			totalMB := 0
			minGFlops := math.MaxFloat64
			var deviceIDs []string
			for _, d := range group {
				totalMB += d.TotalMemoryMB
				if d.GFlops < minGFlops {
					minGFlops = d.GFlops
				}
				deviceIDs = append(deviceIDs, d.DeviceID)
			}
			if int64(totalMB) >= modelVRAMMB {
				sort.Strings(deviceIDs)
				qn := DeviceGroupQueueName(workerID, deviceIDs)
				st := stateByQueue[qn]
				candidates = append(candidates, Candidate{
					WorkerID:       workerID,
					DeviceIDs:      deviceIDs,
					QueueName:      qn,
					GFlops:         minGFlops,
					TotalMemoryMB:  totalMB,
					TailHash:       st.TailHash,
					TailDifficulty: st.TailDifficulty,
					LoadedHash:     st.LoadedHash,
				})
				break // smallest sufficient group for this agent
			}
		}
	}

	return candidates
}

// CalcTensorSplit computes the tensor split ratios proportional to each device's memory.
// Returns a comma-separated string like "0.70,0.30".
func CalcTensorSplit(devices []DeviceInfo) string {
	if len(devices) <= 1 {
		return ""
	}
	total := 0
	for _, d := range devices {
		total += d.TotalMemoryMB
	}
	if total == 0 {
		return ""
	}
	parts := make([]string, len(devices))
	for i, d := range devices {
		ratio := float64(d.TotalMemoryMB) / float64(total)
		parts[i] = fmt.Sprintf("%.2f", ratio)
	}
	return strings.Join(parts, ",")
}

// CalcGpuLayers estimates how many GPU layers fit in available VRAM.
// Returns 0 for full CPU, -1 for full GPU (all layers).
func CalcGpuLayers(modelSizeBytes int64, availableVRAMMB int64) int32 {
	if availableVRAMMB <= 0 {
		return 0 // CPU only
	}
	modelMB := modelSizeBytes / (1024 * 1024)
	if modelMB <= 0 {
		return -1 // unknown size, try all layers
	}
	if availableVRAMMB >= modelMB {
		return 0 // 0 = auto (all layers), our convention
	}
	// Partial offload: estimate fraction of layers that fit.
	// Assume ~100 layers for a typical model as a rough estimate.
	// The actual layer count varies, but this gives a reasonable starting point.
	const estimatedTotalLayers = 100
	layers := int32(float64(estimatedTotalLayers) * float64(availableVRAMMB) / float64(modelMB))
	if layers < 1 {
		layers = 1
	}
	return layers
}

// FallbackPlacement builds a PlacementConfig for RAM offload when a model
// doesn't fit fully on any GPU. It calculates partial GPU offload layers
// based on available VRAM.
func FallbackPlacement(modelSizeBytes int64, device DeviceInfo) config.PlacementConfig {
	gpuLayers := CalcGpuLayers(modelSizeBytes, int64(device.TotalMemoryMB))
	return config.PlacementConfig{
		GpuLayers: gpuLayers,
	}
}

// DeviceQueueName returns the canonical queue name for a single device.
func DeviceQueueName(workerID, deviceID string) string {
	return fmt.Sprintf("device:%s:%s", workerID, deviceID)
}

// DeviceGroupQueueName returns the canonical queue name for a multi-device group.
func DeviceGroupQueueName(workerID string, deviceIDs []string) string {
	return fmt.Sprintf("device:%s:%s", workerID, strings.Join(deviceIDs, "+"))
}
