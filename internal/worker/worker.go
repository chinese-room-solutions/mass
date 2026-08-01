// Package worker defines the worker abstraction. Workers connect to MASS via
// the WorkerHub gRPC stream, advertise their runtime_name + hardware, and
// execute jobs whose payload bytes are gateway-defined and opaque to MASS.
package worker

import (
	"time"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/stats"
)

// WorkerInterface represents a compute worker that can discover hardware,
// run benchmarks, and execute opaque-payload jobs assigned by MASS.
type WorkerInterface interface {
	stats.HardwareInterface
	bench.BencherInterface

	// ID returns the unique identifier of this worker.
	ID() string

	// Name returns a human-readable name for this worker.
	Name() string

	// RuntimeName reports which runtime gateway this worker pairs with
	// (e.g. "llama-cpp"). Set at register time.
	RuntimeName() string

	// Version reports the worker's own semver (e.g. "0.1.0"), set at register
	// time. Empty when the worker predates the handshake version field.
	Version() string

	// Compatible reports the semver range of runtime versions this worker
	// decodes (e.g. ">=0.1 <0.2"), set at register time. Empty when the worker
	// predates the handshake field. Used to flag workers a runtime upgrade
	// would strand.
	Compatible() string

	// Status returns the current status of this worker.
	Status() WorkerStatus

	// LoadedModels returns the worker's currently-loaded model summaries
	// from the most recent heartbeat. The slice is a snapshot — safe for
	// the caller to retain.
	LoadedModels() []LoadedModelStatus

	// AvailableCapacity reports how many additional jobs the worker would
	// accept right now without queueing internally. Updated each heartbeat.
	AvailableCapacity() int

	// ActiveJobs reports the number of jobs currently executing on this
	// worker. Updated each heartbeat.
	ActiveJobs() int
}

// WorkerStatus describes the current state of a worker.
type WorkerStatus struct {
	Online   bool
	LastSeen time.Time
}

// LoadedModelStatus mirrors the worker's per-loaded-model heartbeat report,
// plus the MASS-side Source attribution and idle tracking the worker
// doesn't track.
//
// PoolSize / Active come from worker heartbeats; Source is stamped once at
// load time by the gateway-supplied EnsureModelLoaded request and
// preserved across heartbeat refreshes (heartbeats don't carry it).
//
// IdleSince is the timestamp at which Active first dropped to 0 in MASS's
// view; zero when the model is currently busy. The scheduler uses it to
// auto-evict instances that have been idle longer than the configured TTL.
// Like Source, it survives heartbeat refreshes via merge.
type LoadedModelStatus struct {
	ModelID   string
	PoolSize  int
	Active    int
	Source    string    // gateway-attributed caller, e.g. "app: playground" / "direct"
	IdleSince time.Time // zero when Active>0; stamped when Active first hits 0
	// DeviceIDs lists the canonical IDs of every device this model
	// actually occupies after load. Single GPU for a small model, multiple
	// GPUs for a tensor split, a CPU when llama.cpp spills layers. Honest
	// scoring in the scheduler uses min(GFLOPS across this set) — split
	// models are bounded by their slowest device.
	DeviceIDs []string
	// Files are the store-relative cache keys backing this loaded model
	// (primary plus companions), echoed verbatim from the worker. An entry
	// may denote a single file OR a directory subtree; consumers protect and
	// match by exact string OR path prefix (entry + "/"). Opaque to MASS —
	// this is byte-level file identity for cache reconciliation, never parsed
	// for model semantics.
	Files []string
}
