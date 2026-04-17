// Package stats describes compute devices and reports live utilization
// for them (CPU + GPUs). It contains no benchmarking logic — see pkg/bench.
package stats

import (
	"fmt"
	"runtime"
)

// DeviceType identifies the kind of a compute device.
type DeviceType string

const (
	DeviceTypeCPU DeviceType = "CPU"
	DeviceTypeGPU DeviceType = "GPU"
)

// Canonical device IDs. The host has exactly one CPU compute pool — Go and
// the OS abstract sockets/cores away — so a singleton "cpu:0" is honest.
// (NUMA-aware multi-CPU placement would expose cpu:0 / cpu:1 per socket.)
const CPUDeviceID = "cpu:0"

// GPUDeviceID returns the canonical ID for the Nth GPU on a host.
func GPUDeviceID(index int) string {
	return fmt.Sprintf("gpu:%d", index)
}

// Device describes a detected compute device (CPU or GPU).
type Device struct {
	ID            string     // canonical ID — see CPUDeviceID / GPUDeviceID
	Name          string     // human-readable, e.g. "12-core amd64/linux", "NVIDIA RTX 4090"
	Type          DeviceType // CPU or GPU
	TotalMemoryMB int        // total RAM/VRAM in MB (0 = unknown)
}

// DeviceStats holds live utilization metrics for a device.
type DeviceStats struct {
	DeviceID       string
	UsedMemoryMB   int     // currently used RAM/VRAM in MB
	TotalMemoryMB  int     // total RAM/VRAM in MB
	UtilizationPct float64 // 0-100, compute utilization (0 if unavailable)
}

// HardwareInterface enumerates the host's compute devices and reports their
// live utilization. Implementations cover all detected devices (CPU + GPUs).
type HardwareInterface interface {
	// Devices returns every detected compute device on this host.
	Devices() []Device
	// Stats returns live utilization for every detected device.
	Stats() []DeviceStats
}

// CPUInfo returns the Device descriptor for the host CPU.
func CPUInfo() Device {
	return Device{
		ID:            CPUDeviceID,
		Name:          CPUName(),
		Type:          DeviceTypeCPU,
		TotalMemoryMB: totalRAMMB(),
	}
}

// CPUStats returns live utilization stats for the host CPU.
func CPUStats() DeviceStats {
	return DeviceStats{
		DeviceID:       CPUDeviceID,
		UsedMemoryMB:   usedRAMMB(),
		TotalMemoryMB:  totalRAMMB(),
		UtilizationPct: CPUUtilization(),
	}
}

// CPUName returns a human-readable CPU description.
func CPUName() string {
	return fmt.Sprintf("%d-core %s/%s", runtime.NumCPU(), runtime.GOARCH, runtime.GOOS)
}
