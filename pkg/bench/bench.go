// Package bench provides lightweight benchmarks for estimating compute device
// processing power. Results are used by the scheduler to predict request cost
// and make informed routing/eviction decisions.
//
// Two metrics are measured per device:
//   - Memory bandwidth (GB/s): read throughput (CPU: sequential buffer scan,
//     GPU: host↔device transfer via ggml).
//   - Compute (GFLOPS): arithmetic throughput (CPU: FP32 dot-product,
//     GPU: FP32 matmul via ggml).
package bench

import (
	"fmt"
	"runtime"
	"time"
)

// Device describes a detected compute device (CPU or GPU).
type Device struct {
	ID            string // canonical ID, e.g. "cpu:0", "gpu:0"
	Name          string // human-readable, e.g. "12-core amd64/linux", "NVIDIA RTX 4090"
	Type          string // "CPU" or "GPU"
	TotalMemoryMB int    // total RAM/VRAM in MB (0 = unknown)
}

// DeviceStats holds live utilization metrics for a device.
type DeviceStats struct {
	DeviceID       string
	UsedMemoryMB   int     // currently used RAM/VRAM in MB
	TotalMemoryMB  int     // total RAM/VRAM in MB
	UtilizationPct float64 // 0-100, compute utilization (0 if unavailable)
}

// CPUStats returns live utilization stats for the host CPU.
func CPUStats() DeviceStats {
	return DeviceStats{
		DeviceID:       "cpu:0",
		UsedMemoryMB:   usedRAMMB(),
		TotalMemoryMB:  totalRAMMB(),
		UtilizationPct: CPUUtilization(),
	}
}

// StatsProviderInterface reads live utilization from compute devices.
type StatsProviderInterface interface {
	Stats() []DeviceStats
}

// Result holds the outcome of a single benchmark run.
type Result struct {
	DeviceID      string    `json:"device_id"`
	DeviceName    string    `json:"device_name"`
	MemoryGBs     float64   `json:"memory_gbs"`
	ComputeGFlops float64   `json:"compute_gflops"` // Q4_K matmul GFLOPS (comparable across CPU/GPU)
	BenchedAt     time.Time `json:"benched_at"`
}

// BencherInterface abstracts compute device discovery and benchmarking.
type BencherInterface interface {
	// Devices returns all available compute devices.
	Devices() []Device
	// Bench runs a benchmark on the device with the given ID.
	Bench(deviceID string) (Result, error)
}

// RunAll benchmarks every device returned by the bencher.
func RunAll(b BencherInterface) ([]Result, error) {
	var results []Result
	for _, dev := range b.Devices() {
		res, err := b.Bench(dev.ID)
		if err != nil {
			return nil, fmt.Errorf("benchmark %s: %w", dev.ID, err)
		}
		results = append(results, res)
	}
	return results, nil
}

// RunCPU returns a base CPU result with device info and timestamp.
// Bandwidth and compute are populated by the Bencher via ggml benchmarks.
func RunCPU() Result {
	return Result{
		DeviceID:   "cpu:0",
		DeviceName: CPUName(),
		BenchedAt:  time.Now(),
	}
}

// CPUInfo returns the Device descriptor for the host CPU.
func CPUInfo() Device {
	return Device{
		ID:            "cpu:0",
		Name:          CPUName(),
		Type:          "CPU",
		TotalMemoryMB: totalRAMMB(),
	}
}

// CPUName returns a human-readable CPU description.
func CPUName() string {
	return fmt.Sprintf("%d-core %s/%s", runtime.NumCPU(), runtime.GOARCH, runtime.GOOS)
}
