package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/store"
)

const (
	// affinityWeight scales the tail length bonus when a device queue
	// already has the same model fingerprint loaded.
	affinityWeight = 10.0

	// computeBase is the denominator base in the max concurrent formula.
	// Used with a power law to model sublinear scaling of concurrent slots:
	//   max_concurrent = floor((GFlops / (modelSizeGB * computeBase)) ^ computeExponent)
	// Calibrated against known-good concurrency targets:
	//   RTX 3070 Ti (~1800 GFLOPS, 3GB model) → 3
	//   RTX 4090   (~10000 GFLOPS, 3GB model) → 10
	computeBase     = 140.0
	computeExponent = 0.75

	// maxConcurrentCap is the upper bound for auto-calculated max_concurrent.
	maxConcurrentCap int32 = 64
)

// DeviceInfo describes a compute device for placement decisions.
type DeviceInfo struct {
	AgentID       string
	DeviceID      string
	TotalMemoryMB int
	GFlops        float64
}

// Candidate represents a possible placement for a task.
type Candidate struct {
	AgentID       string
	DeviceIDs     []string // single device or multi-device for tensor split
	QueueName     string
	GFlops        float64 // min of group for tensor split
	TotalMemoryMB int     // sum of group
	TailHash      string
	TailLength    int
	LoadedHash    string
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

// ScorePlacement computes a placement score for a candidate.
// Higher score = better placement.
//
// Three tiers:
//  1. Matching tail: strongly preferred, longer tails score higher (build longest sequence).
//  2. Loaded model, empty queue: bonus for avoiding model load cost.
//  3. No match: prefer the LONGEST non-matching tail — the context switch is deferred
//     furthest into the future (after all those queued same-model tasks finish),
//     which means fewer context switches per unit time overall. Shorter tails are
//     protected because they'd switch sooner, causing more frequent switching.
//
// Device power (GFlops) is used as a tiebreaker when scores are equal.
func ScorePlacement(c Candidate, taskFingerprint string) float64 {
	if c.TailHash == taskFingerprint {
		// Build the longest uninterrupted sequence — prefer longer tails.
		return affinityWeight * float64(c.TailLength+1)
	}
	if c.LoadedHash == taskFingerprint && c.TailLength == 0 {
		// Model is loaded and queue is empty — avoids model load.
		return affinityWeight * 0.5
	}
	// No match — prefer longest tail: context switch happens later (after more
	// same-model tasks drain), avoiding frequent switching on short-tail devices.
	// Score is always negative (worse than any match) but less negative for longer tails.
	// The -affinityWeight base ensures non-matches never outscore matches.
	return -affinityWeight - 1.0/(float64(c.TailLength)+1)
}

// SelectBestCandidate picks the highest-scoring candidate.
// On tie, the candidate with higher GFlops wins (stronger device handles tasks faster).
func SelectBestCandidate(candidates []Candidate, taskFingerprint string) *Candidate {
	if len(candidates) == 0 {
		return nil
	}
	best := 0
	bestScore := ScorePlacement(candidates[0], taskFingerprint)
	for i := 1; i < len(candidates); i++ {
		score := ScorePlacement(candidates[i], taskFingerprint)
		if score > bestScore || (score == bestScore && candidates[i].GFlops > candidates[best].GFlops) {
			best = i
			bestScore = score
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

	// Build lookup: agentID → devices.
	agentDevices := make(map[string][]DeviceInfo)
	for _, d := range devices {
		agentDevices[d.AgentID] = append(agentDevices[d.AgentID], d)
	}

	var candidates []Candidate

	// Single-device candidates.
	for _, d := range devices {
		if int64(d.TotalMemoryMB) >= modelVRAMMB {
			qn := DeviceQueueName(d.AgentID, d.DeviceID)
			st := stateByQueue[qn]
			candidates = append(candidates, Candidate{
				AgentID:       d.AgentID,
				DeviceIDs:     []string{d.DeviceID},
				QueueName:     qn,
				GFlops:        d.GFlops,
				TotalMemoryMB: d.TotalMemoryMB,
				TailHash:      st.TailHash,
				TailLength:    st.TailLength,
				LoadedHash:    st.LoadedHash,
			})
		}
	}

	if len(candidates) > 0 {
		return candidates
	}

	// Multi-device candidates (tensor split) — only if no single device fits.
	for agentID, devs := range agentDevices {
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
				qn := DeviceGroupQueueName(agentID, deviceIDs)
				st := stateByQueue[qn]
				candidates = append(candidates, Candidate{
					AgentID:       agentID,
					DeviceIDs:     deviceIDs,
					QueueName:     qn,
					GFlops:        minGFlops,
					TotalMemoryMB: totalMB,
					TailHash:      st.TailHash,
					TailLength:    st.TailLength,
					LoadedHash:    st.LoadedHash,
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

// CalcMaxConcurrent computes the optimal max_concurrent value for a model on a device.
// Compute cap uses a power law to model sublinear scaling:
//
//	computeCap = floor((GFlops / (modelSizeGB * computeBase)) ^ computeExponent)
//
// VRAM cap: floor((TotalMB - modelSizeMB) / kvCacheMB)
// Result: min(computeCap, vramCap), clamped to [1, maxConcurrentCap].
func CalcMaxConcurrent(gflops float64, modelSizeGB float64, totalMB, modelSizeMB, kvCacheMB int) int32 {
	if gflops <= 0 || modelSizeGB <= 0 {
		return 1
	}

	ratio := gflops / (modelSizeGB * computeBase)
	computeCap := int32(math.Floor(math.Pow(ratio, computeExponent)))

	vramCap := maxConcurrentCap
	if kvCacheMB > 0 {
		freeMB := totalMB - modelSizeMB
		if freeMB <= 0 {
			return 1
		}
		vramCap = int32(freeMB / kvCacheMB)
	}

	result := min(computeCap, vramCap)
	if result < 1 {
		result = 1
	}
	if result > maxConcurrentCap {
		result = maxConcurrentCap
	}
	return result
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
func DeviceQueueName(agentID, deviceID string) string {
	return fmt.Sprintf("device:%s:%s", agentID, deviceID)
}

// DeviceGroupQueueName returns the canonical queue name for a multi-device group.
func DeviceGroupQueueName(agentID string, deviceIDs []string) string {
	return fmt.Sprintf("device:%s:%s", agentID, strings.Join(deviceIDs, "+"))
}
