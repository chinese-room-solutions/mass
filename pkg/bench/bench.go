// Package bench carries the transport types MASS uses to receive device
// benchmark results from workers. These numbers describe hardware, not
// models: a generic matmul FLOPS figure for display and the two memory
// bandwidths. Per-model throughput is measured separately and lives in
// the model_benchmarks store.
//
// MASS itself does no local benchmarking — it is coordinator-only.
package bench

import (
	"time"
)

// Result holds the outcome of a single device benchmark on a worker.
// Flops is the device's generic matmul throughput, display only — the
// scheduler never divides a job cost by it.
//
// MemoryGBs is in-device memory bandwidth (STREAM-style) — the rate at
// which a kernel can read+write its own device buffers. LoadGBs is the
// host→device upload rate, which dominates wall-clock when MASS evicts
// a resident model and loads a different one. On a discrete GPU the two
// numbers differ by ~100× (PCIe vs GDDR), so MASS keeps them separate
// and only divides file sizes by LoadGBs in the switch-cost predictor.
type Result struct {
	DeviceID   string    `json:"device_id"`
	DeviceName string    `json:"device_name"`
	MemoryGBs  float64   `json:"memory_gbs"`
	LoadGBs    float64   `json:"load_gbs"`
	Flops      float64   `json:"flops"`
	BenchedAt  time.Time `json:"benched_at"`
}

// BencherInterface is implemented by workers that can run benchmarks
// on a known device. Discovery of which devices exist is the
// responsibility of [stats.HardwareInterface].
type BencherInterface interface {
	// Bench runs a benchmark on the device with the given ID.
	Bench(deviceID string) (Result, error)
}
