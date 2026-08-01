// Package bench carries the transport types MASS uses to receive
// benchmark results from workers. Workers measure throughput in
// runtime-private units (e.g. "q4k_matvec" GFLOPS for llama-cpp) and
// report them as a map keyed by axis name. Memory bandwidth (B/s) is
// a separate, universally-meaningful number used by MASS for load-
// latency scoring.
//
// MASS itself does no local benchmarking — it is coordinator-only.
package bench

import (
	"time"
)

// Result holds the outcome of a single device benchmark on a worker.
// Throughput is the worker's realised throughput on its runtime's
// calibration workloads, keyed by axis name. Schema is runtime-private
// and MASS does not interpret axis names.
//
// MemoryGBs is in-device memory bandwidth (STREAM-style) — the rate at
// which a kernel can read+write its own device buffers. LoadGBs is the
// host→device upload rate, which dominates wall-clock when MASS evicts
// a resident model and loads a different one. On a discrete GPU the two
// numbers differ by ~100× (PCIe vs GDDR), so MASS keeps them separate
// and only divides file sizes by LoadGBs in the switch-cost predictor.
type Result struct {
	DeviceID   string             `json:"device_id"`
	DeviceName string             `json:"device_name"`
	MemoryGBs  float64            `json:"memory_gbs"`
	LoadGBs    float64            `json:"load_gbs"`
	Throughput map[string]float64 `json:"throughput"`
	BenchedAt  time.Time          `json:"benched_at"`
}

// BencherInterface is implemented by workers that can run benchmarks
// on a known device. Discovery of which devices exist is the
// responsibility of [stats.HardwareInterface].
type BencherInterface interface {
	// Bench runs a benchmark on the device with the given ID.
	Bench(deviceID string) (Result, error)
}
