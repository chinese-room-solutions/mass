// Package bench measures per-device compute power for scheduler scoring.
//
// Two metrics per device:
//   - Memory bandwidth (GB/s): CPU sequential buffer scan, GPU host↔device
//     transfer via ggml.
//   - Compute (GFLOPS): CPU FP32 dot-product, GPU FP32 matmul via ggml.
//
// Device discovery and live utilization live in pkg/stats.
package bench

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/chinese-room-solutions/mass/pkg/stats"
)

// Result holds the outcome of a single benchmark run.
type Result struct {
	DeviceID      string    `json:"device_id"`
	DeviceName    string    `json:"device_name"`
	MemoryGBs     float64   `json:"memory_gbs"`
	ComputeGFlops float64   `json:"compute_gflops"` // matmul GFLOPS (comparable across CPU/GPU)
	BenchedAt     time.Time `json:"benched_at"`
}

// BencherInterface runs benchmarks on a known device.
// Discovery of which devices exist is the responsibility of [stats.HardwareInterface].
type BencherInterface interface {
	// Bench runs a benchmark on the device with the given ID.
	Bench(deviceID string) (Result, error)
}

// KernelProviderInterface measures memory bandwidth and matmul GFLOPS
// on a specific device. Implementations live in the runtime that owns the
// device (e.g. mass-worker-llama's internal/llama package).
type KernelProviderInterface interface {
	BandwidthCPU(threads int) (float64, bool)
	ComputeCPU(threads int) (float64, bool)
	BandwidthGPU(idx int) (float64, bool)
	ComputeGPU(idx int) (float64, bool)
}

// Bencher composes a stats provider (for device descriptors) and a kernel
// provider (for the actual measurements) to implement BencherInterface
// without depending on any specific runtime library.
type Bencher struct {
	stats   stats.HardwareInterface
	kernels KernelProviderInterface
}

// NewBencher constructs a Bencher from a stats provider and a kernel provider.
// Pass stats.NewHardware(stats.NoGPU{}) for CPU-only builds.
func NewBencher(s stats.HardwareInterface, kernels KernelProviderInterface) *Bencher {
	return &Bencher{stats: s, kernels: kernels}
}

// Bench runs bandwidth + compute kernels on the device with the given ID.
func (b *Bencher) Bench(deviceID string) (Result, error) {
	if deviceID == stats.CPUDeviceID {
		res := RunCPU()
		nThreads := runtime.NumCPU()
		if bw, ok := b.kernels.BandwidthCPU(nThreads); ok {
			res.MemoryGBs = bw
		}
		if score, ok := b.kernels.ComputeCPU(nThreads); ok {
			res.ComputeGFlops = score
		}
		return res, nil
	}
	if strings.HasPrefix(deviceID, "gpu:") {
		var idx int
		if _, err := fmt.Sscanf(deviceID, "gpu:%d", &idx); err != nil {
			return Result{}, fmt.Errorf("invalid device ID %q: %w", deviceID, err)
		}
		var info *stats.Device
		for _, d := range b.stats.Devices() {
			if d.ID == deviceID {
				info = &d
				break
			}
		}
		if info == nil {
			return Result{}, fmt.Errorf("GPU %d not found", idx)
		}
		res := Result{
			DeviceID:   deviceID,
			DeviceName: info.Name,
			BenchedAt:  time.Now(),
		}
		if bw, ok := b.kernels.BandwidthGPU(idx); ok {
			res.MemoryGBs = bw
		}
		if score, ok := b.kernels.ComputeGPU(idx); ok {
			res.ComputeGFlops = score
		}
		return res, nil
	}
	return Result{}, fmt.Errorf("unknown device %q", deviceID)
}

// RunAll benchmarks every device reported by the given stats provider using
// the given bencher.
func RunAll(s stats.HardwareInterface, b BencherInterface) ([]Result, error) {
	var results []Result
	for _, dev := range s.Devices() {
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
		DeviceID:   stats.CPUDeviceID,
		DeviceName: stats.CPUName(),
		BenchedAt:  time.Now(),
	}
}
