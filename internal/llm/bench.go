package llm

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/chinese-room-solutions/mass/internal/bench"
	llama "github.com/tcpipuk/llama-go"
)

// Compile-time checks.
var (
	_ bench.BencherInterface       = (*Bencher)(nil)
	_ bench.StatsProviderInterface = (*Bencher)(nil)
)

// Bencher implements bench.BencherInterface and bench.StatsProviderInterface
// for CPU and CUDA GPUs.
type Bencher struct{}

// Devices returns all available compute devices (CPU + GPUs).
func (b *Bencher) Devices() []bench.Device {
	devices := []bench.Device{bench.CPUInfo()}
	count := llama.GPUCount()
	for i := range count {
		info, ok := llama.GetGPUInfo(i)
		if !ok {
			continue
		}
		devices = append(devices, bench.Device{
			ID:            fmt.Sprintf("gpu:%d", info.DeviceID),
			Name:          info.DeviceName,
			Type:          "GPU",
			TotalMemoryMB: info.TotalMemoryMB,
		})
	}
	return devices
}

// Bench runs a benchmark on the device with the given ID.
// Both CPU and GPU use the same ggml-based benchmarks for consistency.
func (b *Bencher) Bench(deviceID string) (bench.Result, error) {
	if deviceID == "cpu:0" {
		res := bench.RunCPU()
		nThreads := runtime.NumCPU()
		if bw, ok := llama.BenchBandwidthCPU(nThreads); ok {
			res.MemoryGBs = bw
		}
		if score, ok := llama.BenchQ4KMatVecCPU(nThreads); ok {
			res.ComputeGFlops = score
		}
		return res, nil
	}
	if strings.HasPrefix(deviceID, "gpu:") {
		var idx int
		if _, err := fmt.Sscanf(deviceID, "gpu:%d", &idx); err != nil {
			return bench.Result{}, fmt.Errorf("invalid device ID %q: %w", deviceID, err)
		}
		info, ok := llama.GetGPUInfo(idx)
		if !ok {
			return bench.Result{}, fmt.Errorf("GPU %d not found", idx)
		}
		result := bench.Result{
			DeviceID:   deviceID,
			DeviceName: info.DeviceName,
			BenchedAt:  time.Now(),
		}
		if bw, ok := llama.BenchBandwidthGPU(idx); ok {
			result.MemoryGBs = bw
		}
		if score, ok := llama.BenchQ4KMatVecGPU(idx); ok {
			result.ComputeGFlops = score
		}
		return result, nil
	}
	return bench.Result{}, fmt.Errorf("unknown device %q", deviceID)
}

// Stats returns live utilization stats for all devices (CPU + GPUs).
func (b *Bencher) Stats() []bench.DeviceStats {
	stats := []bench.DeviceStats{bench.CPUStats()}
	count := llama.GPUCount()
	for i := range count {
		info, ok := llama.GetGPUInfo(i)
		if !ok {
			continue
		}
		usedMB := info.TotalMemoryMB - info.FreeMemoryMB
		if usedMB < 0 {
			usedMB = 0
		}
		ds := bench.DeviceStats{
			DeviceID:      fmt.Sprintf("gpu:%d", info.DeviceID),
			UsedMemoryMB:  usedMB,
			TotalMemoryMB: info.TotalMemoryMB,
		}
		if info.UtilizationPct >= 0 {
			ds.UtilizationPct = float64(info.UtilizationPct)
		}
		stats = append(stats, ds)
	}
	return stats
}
