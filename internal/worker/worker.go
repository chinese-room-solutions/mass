// Package worker defines the worker abstraction for compute resource management.
// MASS delegates hardware discovery, benchmarking, and model loading to workers.
// The local worker runs in-process; remote workers connect over ConnectRPC.
package worker

import (
	"time"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/stats"
)

// WorkerInterface represents a compute worker that can discover hardware,
// run benchmarks, and load/run inference models.
// It embeds [stats.HardwareInterface] (Devices + Stats),
// [bench.BencherInterface] (Bench), and [llm.ModelLoaderInterface].
type WorkerInterface interface {
	llm.ModelLoaderInterface
	stats.HardwareInterface
	bench.BencherInterface

	// ID returns the unique identifier of this worker.
	ID() string

	// Name returns a human-readable name for this worker.
	Name() string

	// Status returns the current status of this worker.
	Status() WorkerStatus
}

// WorkerStatus describes the current state of an worker.
type WorkerStatus struct {
	Online   bool
	LastSeen time.Time
}
