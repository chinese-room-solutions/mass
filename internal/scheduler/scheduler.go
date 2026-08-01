// Package scheduler is MASS's runtime-agnostic job dispatcher.
//
// Architecture: two-level durable queue.
//
//   - One global queue ("global") receives every gateway-submitted job.
//   - One device queue per (worker, enabled-device) pair receives jobs the
//     dispatcher has bound to a specific compute target.
//   - A single dispatcher goroutine leases global rows: it picks a device
//     queue (filtered by runtime_name, preferring model-loaded targets,
//     breaking ties by available worker capacity) and atomically moves
//     each row to the chosen device queue. Device queues drain on their
//     own per-queue goroutines — the handoff may block for minutes on a
//     cold [worker.StreamWorker.LoadModel], and one worker's load must
//     not stall dispatch to the rest of the fleet. Each drainer leases,
//     calls [worker.StreamWorker.AssignJob], and appends worker chunks
//     into a per-job ring buffer that gateways attach to via
//     [Scheduler.StreamChunks] (resumable across reconnect). Per-worker
//     FIFO order is preserved: at most one drainer runs per queue.
//   - Work stealing rebalances: when a worker's device queues are empty but
//     it has capacity, the dispatcher peeks peer device queues (same
//     runtime, different worker) and moves a row over when the stealing
//     worker already has the target model loaded.
//
// Persistence: device queue identities are tracked in [store.WorkerQueueState]
// so a restart can reattach to the right SQL rows. Job payloads survive
// in goqite; on crash recovery the queue sweeper ([queue.ReapAbandoned])
// fails any rows past their delivery budget so callers don't hang.
//
// Worker handoff is the seam: this package does not know what a llama.cpp
// payload looks like, and the worker hub does not know what a device queue
// is. Placement decisions live here; transport lives there.
package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/jitter"
	"github.com/chinese-room-solutions/mass/internal/metrics"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// ErrNoWorker is returned when no online worker matches the requested
// runtime_name.
var ErrNoWorker = errors.New("no worker available for runtime kind")

// ErrNoDeviceQueue is returned by the dispatcher when a runtime has online
// workers but none of them have any enabled device queues yet (race with
// connect, or every device disabled). Surfaces as Unavailable to gateways.
var ErrNoDeviceQueue = errors.New("no enabled device queue available for runtime kind")

// ErrInvalidCost is returned by Submit when the gateway passed a
// non-positive Cost. Cost must be > 0; zero is the unset signal.
// Surfaces as InvalidArgument to gateways.
var ErrInvalidCost = errors.New("submit: cost must be > 0")

// ErrInvalidCostAxis is returned by Submit when the gateway passed an
// empty CostAxis. The axis must be non-empty so MASS can look up the
// worker's throughput. Surfaces as InvalidArgument to gateways.
var ErrInvalidCostAxis = errors.New("submit: cost_axis must be non-empty")

// ErrFieldTooLong is returned by Submit when a gateway-supplied identity
// field exceeds the envelope wire format's 255-byte cap. Truncating would
// silently corrupt identity (a truncated ModelID breaks residency
// matching and cancellation), so the submit is rejected instead.
// Surfaces as InvalidArgument to gateways.
var ErrFieldTooLong = errors.New("submit: field exceeds 255 bytes")

// ErrNoMemoryFit is returned by Submit when no online worker has
// enough total hardware memory across its default device set to host
// the requested load. Distinguishes "fleet fundamentally too small"
// from "fleet busy right now" — the former is operator-actionable
// (add a bigger worker, shrink the model); the latter would resolve
// by waiting. Surfaces as FailedPrecondition to gateways.
var ErrNoMemoryFit = errors.New("submit: no worker has enough memory to host this model")

// ErrWorkerReestimating is returned by toggle entry points when a
// previous enable/disable change on the same worker is still recomputing
// queued-job estimates. The handler maps this to HTTP 409 so the
// operator gets a clear "busy, retry" signal instead of racing the
// re-estimation pass.
var ErrWorkerReestimating = errors.New("worker is re-estimating after a recent device toggle")

// WorkerEnabledFn reports whether the operator-controlled toggle has the
// worker enabled. Returns true (admit) when no callback is wired or when
// the worker has no persisted disable state — sane default for newly-
// connected workers.
type WorkerEnabledFn func(workerID string) bool

// DeviceEnabledFn reports whether a single (worker, device) pair is enabled.
// A worker without any enabled devices has no device queues and is skipped
// entirely. Returns true when no callback is wired.
type DeviceEnabledFn func(workerID, deviceID string) bool

// RuntimeDefaultAxisFn returns the throughput axis a runtime's gateway
// declared as required (via InitResponse.default_cost_axis). The
// scheduler uses it as the fallback when an envelope's CostAxis names
// an axis a worker hasn't benched. Returns "" when no gateway is
// running for runtimeName — placement then disregards fallback and only
// admits workers that bench the exact requested axis.
type RuntimeDefaultAxisFn func(runtimeName string) string

// dispatchLeaseDuration is how long a dispatcher-leased row stays invisible
// to other consumers per extension. A dispatch routinely outlives one
// window (a cold LoadModel takes minutes; prompt processing can gap
// longer than this before the first chunk), so the invariant is NOT
// "the dispatcher finishes within the window" — it's the keep-alive
// started in dispatchEnvelope, which re-extends both queue rows every
// dispatchLeaseDuration/3 until the dispatch reaches a terminal path.
const dispatchLeaseDuration = 60 * time.Second

// disconnectRequeueBudget caps how many times an in-flight job may be
// re-placed after losing its worker before its result fails terminally.
// A single worker death is routine (restart, upgrade) and its jobs must
// redistribute to peers — but a job that kills every worker it lands on
// would otherwise cycle requeue → redispatch → crash forever, wedging
// every caller waiting on it. Charged against Envelope.Attempts, shared
// with the load-failure retry counter: both count "dispatches that ended
// badly", and a mix of the two failure modes should still converge.
const disconnectRequeueBudget = 3

// stealThreshold is the device-queue depth gap that triggers a steal
// attempt. A worker with an empty queue will look at peer queues only when
// peer.Depth() > stealThreshold; below that the imbalance isn't worth the
// cross-queue churn.
const stealThreshold = 2

// Scheduler is the central dispatcher. Construct with [New], then attach
// the persistent queue subsystem with [Scheduler.InitQueue] and start the
// dispatcher with [Scheduler.Start].
type Scheduler struct {
	cfg     *config.Config
	logger  zerolog.Logger
	workers *worker.Fleet
	store   StateStoreInterface

	queueMu   sync.RWMutex
	queuePool *queue.Pool
	globalQ   queue.QueueInterface
	results   queue.ResultStoreInterface
	devQueues map[string]queue.QueueInterface // keyed on device-queue name

	// tailMu guards tails — the write-through mirror of every worker
	// queue's (tail_seconds, tail_model_id), keyed by queue name. This
	// scheduler process is the only writer of those columns, so scoring
	// reads the mirror instead of issuing one store read per candidate
	// per envelope (the 200ms pending-retry loop re-scores constantly
	// while an unplaceable row sits on global). The store row stays the
	// durable copy: every mutation updates the mirror first, then
	// persists best-effort; recoverPersistedQueues rehydrates the mirror
	// after a restart. Entry lifecycle matches the store row's — created
	// on connect/recover, dropped on disconnect — and add/set against a
	// missing entry no-ops, exactly like the store's UPDATE statements.
	tailMu sync.RWMutex
	tails  map[string]tailState

	workerEnabledMu sync.RWMutex
	workerEnabled   WorkerEnabledFn
	deviceEnabled   DeviceEnabledFn
	runtimeAxis     RuntimeDefaultAxisFn

	jobsMu     sync.Mutex
	jobBuffers map[string]*jobBuffer // RequestID → in-memory replay buffer

	inflightMu sync.Mutex
	// inflightSeconds tracks the running wall-clock-seconds estimate of
	// jobs MASS has dispatched to a worker but hasn't observed a terminal
	// frame for yet. Keyed by worker-queue name so scoring in
	// pickWorkerQueue can read it independently per worker. Process-local
	// — recovery from a crash flows through the durable queue, not this
	// map.
	inflightSeconds map[string]float64
	// inflightByRequest maps RequestID → worker queue name → seconds, so
	// the terminal-frame decrement knows which key to subtract from.
	inflightByRequest map[string]inflightRecord
	// dispatchingByRequest covers the window between a worker-queue row
	// being leased and its [Scheduler.startInflight] call: the job is being
	// actively placed (model load, AssignJob) but isn't yet inflight, so a
	// concurrent cancel would otherwise find neither a pending global row
	// nor an inflight record and be lost. The entry's bool is the recorded
	// cancel intent; dispatchEnvelope reads it after load and aborts before
	// AssignJob. Guarded by inflightMu.
	dispatchingByRequest map[string]bool
	// memoryReservations tracks per-worker bytes-reserved for in-flight
	// cold loads: keyed by worker ID, summed across each cold dispatch.
	// Bridges the window between "MASS picks worker A for load" and the
	// next heartbeat showing worker A's used_memory reflect the load.
	// Released on terminal frame (alongside inflight seconds).
	memoryReservations map[string]int64

	// reestimateMu serialises per-worker re-estimation passes triggered by
	// device enable/disable toggles. Each worker gets its own mutex on
	// first toggle; concurrent toggles for the same worker fail fast with
	// [ErrWorkerReestimating] rather than queuing or racing. Cross-worker
	// toggles are independent — different workers, different locks.
	reestimateLockMu sync.Mutex
	reestimateLocks  map[string]*sync.Mutex

	benchMu sync.RWMutex
	// benchCache fronts [StateStoreInterface.GetBenchmark] so the hot
	// scoring path doesn't hit SQLite once per (worker, device, envelope).
	// Lazy-populated on first lookup; invalidated by [Scheduler.InvalidateBench]
	// after a fresh bench result lands. A negative cache (the inner map
	// containing an empty BenchmarkRow) records the absence of a row so
	// repeat misses stay cheap.
	benchCache map[string]map[string]store.BenchmarkRow

	// throughputCorrection holds a live EWMA multiplier on each worker's
	// benched throughput, keyed "workerID|axis". It learns from completed
	// jobs: ratio = predicted_seconds / actual_seconds (>1 → worker beat
	// the bench, <1 → slower than benched). The benchmark is the prior;
	// real jobs are the evidence. Closes the open loop between one-time
	// benching and live reality (thermal throttle, contention, a
	// systematically optimistic gateway Cost). The map is authoritative;
	// each folded sample is also persisted and restored at startup (see
	// [Scheduler.restoreCorrections]) so calibration survives restarts.
	// Reset when the baseline the factor is relative to changes — fresh
	// bench or device toggle (see [Scheduler.ResetCorrections]).
	correctionMu         sync.Mutex
	throughputCorrection map[string]correctionState

	// draining marks worker queues that currently have a drain goroutine
	// running (see drainDeviceQueues). The entry's bool records whether a
	// later pass wanted to drain the queue while it was busy — the drainer
	// consumes it on exit to decide whether to kick a follow-up pass.
	drainMu  sync.Mutex
	draining map[string]bool

	// gaugeMu guards the previously-reported gauge label sets below.
	// refreshGauges runs on the single metrics-sweep goroutine, but tests
	// call it directly; the lock keeps that safe and matches the struct's
	// guarded-field convention.
	gaugeMu sync.Mutex
	// prevWorkerRuntimes / prevInflightRuntimes are the runtime labels the
	// last sweep wrote to mass_workers_registered / mass_jobs_inflight. A
	// runtime present last sweep but absent this one gets an explicit
	// Set(rt, 0) — without it the labeled series freezes at its old
	// non-zero value forever once the last worker (or job) of a runtime
	// disappears. Zeroing rather than deleting the series is deliberate:
	// a 0 sample is what operators alert on.
	prevWorkerRuntimes   map[string]struct{}
	prevInflightRuntimes map[string]struct{}

	wake chan struct{} // capacity-1 dispatcher wake-up

	// onQueueChange is fanned out to the Queue tab's SSE broker every time
	// rows enter or leave a queue. Set via [Scheduler.SetQueueChangeCallback].
	// Optional — nil callback means no fan-out (e.g. headless tests).
	queueChangeMu sync.RWMutex
	onQueueChange func()
}

// SetQueueChangeCallback registers fn for queue-change notifications. Pass
// nil to clear. Called from the web layer to wire the Queue-tab SSE broker.
func (s *Scheduler) SetQueueChangeCallback(fn func()) {
	s.queueChangeMu.Lock()
	s.onQueueChange = fn
	s.queueChangeMu.Unlock()
}

// broadcastQueueChange notifies the registered callback (if any) that the
// queue snapshot has changed. Safe to call from any goroutine; non-blocking
// because the callback should itself be non-blocking (the broker fans out
// to subscribers without waiting).
func (s *Scheduler) broadcastQueueChange() {
	s.queueChangeMu.RLock()
	fn := s.onQueueChange
	s.queueChangeMu.RUnlock()
	if fn != nil {
		fn()
	}
}

type inflightRecord struct {
	queueName string
	// seconds is the compute-only wall-clock prediction (Cost divided by
	// the worker's effective throughput), re-priced at dispatch time. It
	// deliberately EXCLUDES the load-switch latency that the envelope's
	// QueuedSeconds carries: any model load has already completed before
	// this record is created, so the queue's remaining busy-time and the
	// throughput-correction baseline are both compute-only.
	seconds float64
	// modelID is the envelope's ModelID, captured so the device-set gate
	// can detect "MASS has already assigned a job for this model on this
	// worker" without waiting for the next worker heartbeat to bump
	// LoadedModelStatus.Active. Without this, two overlapping-device
	// jobs can dispatch in the same drain pass and silently co-locate
	// because Active still reads 0 between dispatch and first heartbeat.
	modelID string
	// workerJobID is the worker-side opaque ID returned by [worker.StreamWorker.AssignJob].
	// Captured at dispatch so [Scheduler.CancelRunningJob] can address the
	// in-flight job on the right worker (via HubCancelJob).
	workerJobID string
	// workerID is parsed from queueName once at dispatch so the cancel path
	// doesn't have to re-parse on every lookup. Empty when the inflight was
	// created against a non-worker queue (shouldn't happen today; defensive).
	workerID string
	// cancelledByOperator is set by [Scheduler.CancelRunningJob] just before
	// it fires HubCancelJob. The pump checks it when a terminal error frame
	// arrives so the result message reads "cancelled by operator" rather
	// than the worker's raw error text ("Chat: cancelled by operator", etc.).
	cancelledByOperator bool
	// reservedBytes is the cold-load memory reservation MASS held on the
	// worker for this job. 0 when the model was already resident at
	// dispatch (no load → nothing to reserve). Released to the worker's
	// memoryReservations ledger on finishInflight.
	reservedBytes int64
	// runtimeName is captured so per-runtime metric counters (jobs
	// dispatched) can be emitted on terminal frames without re-resolving
	// the envelope.
	runtimeName string
	// axis + dispatchedAt feed the throughput correction loop: on an ok
	// terminal we compare actual wall-clock (now - dispatchedAt) against
	// the predicted seconds (this record's `seconds`) for workerID|axis.
	// axis is the throughput axis the prediction actually divided by
	// (the runtime default when the envelope's CostAxis wasn't benched),
	// so correction samples land on the key scoring reads.
	axis         string
	dispatchedAt time.Time
	// correction is the EWMA factor that was already baked into this
	// record's predicted seconds at dispatch (effectiveThroughput
	// multiplies benched throughput by it). observeThroughput multiplies
	// the predicted/actual ratio back by this value so every sample is
	// measured against the UNCORRECTED bench prior. Folding the
	// corrected-prediction ratio directly would make the EWMA
	// self-referential: its fixed point lands at sqrt(true ratio), so a
	// worker running 4x its bench would stabilise at factor 2 and stay
	// 2x mispredicted forever.
	correction float64
}

// tailState is one entry of the in-memory tail mirror: the queued
// (not-yet-dispatched) wall-clock-seconds sum and the tail-of-queue
// model for one worker queue. See the Scheduler.tails field docs.
type tailState struct {
	seconds float64
	modelID string
}

// correctionState is the per-(worker,axis) EWMA of predicted/actual
// wall-clock ratio. factor multiplies benched throughput at scoring time;
// samples gates application until enough evidence accrues.
type correctionState struct {
	factor  float64
	samples int
}

const (
	// correctionAlpha is the EWMA weight on each new sample. 0.2 ≈ a ~10-
	// job memory: responsive to a real regime change (throttling kicking
	// in) without chasing single-job noise.
	correctionAlpha = 0.2
	// correctionMinSamples is how many ok-terminals must accrue before the
	// factor is applied — one or two jobs can't move placement.
	correctionMinSamples = 5
	// correctionClamp bounds the factor to [1/clamp, clamp] so a pathological
	// job (cold cache, a 30s thinking burst predicted at 1s) can't swing
	// placement wildly. The bench prior dominates outside this band.
	correctionClamp = 4.0
	// correctionMinActualSec drops sub-threshold jobs from the EWMA: their
	// wall-clock is dominated by fixed dispatch/RPC overhead, not compute,
	// so the ratio is meaningless as a throughput signal.
	correctionMinActualSec = 0.1
	// correctionMaxAge bounds how old a persisted correction may be and
	// still seed the EWMA at startup: month-old evidence says little about
	// today's thermals or drivers. Older rows are simply not loaded — the
	// next sample or reset overwrites them.
	correctionMaxAge = 30 * 24 * time.Hour
)

// StateStoreInterface is the slice of [store.Store] the scheduler needs for
// device-queue lifecycle persistence. Tightening the dependency to this
// subset keeps tests honest.
type StateStoreInterface interface {
	UpsertWorkerQueueState(state store.WorkerQueueState) error
	ListWorkerQueueStates() ([]store.WorkerQueueState, error)
	DeleteWorkerQueueState(queueName string) error
	// AddTailSeconds adjusts tail_seconds by delta. Used by dispatch pop
	// (delta = -env.QueuedSeconds) and by work-stealing transfers.
	AddTailSeconds(queueName string, delta float64) error
	// AddTailSecondsAndSetModel adjusts tail_seconds by delta AND sets
	// tail_model_id atomically. Used on enqueue: the new envelope's
	// QueuedSeconds extend the queue and its ModelID becomes the new
	// tail-of-queue model so the next score sees the right switch cost.
	AddTailSecondsAndSetModel(queueName string, delta float64, newModelID string) error
	// SetTailSeconds replaces tail_seconds with value AND sets tail_model_id
	// atomically. Used by re-estimation after device toggles — the
	// per-envelope sum needs to be reconciled with a new device set in
	// one write, not incrementally.
	SetTailSeconds(queueName string, value float64, tailModelID string) error
	// GetBenchmark returns the most recent per-device benchmark row.
	// Scoring requires it (no benchmark = device not schedulable).
	GetBenchmark(workerID, deviceID string) (store.BenchmarkRow, error)
	// UpsertThroughputCorrection persists one (worker, axis) entry of the
	// correction EWMA after a completed job folds in.
	UpsertThroughputCorrection(c store.ThroughputCorrection) error
	// ListThroughputCorrections returns every persisted correction entry;
	// [Scheduler.restoreCorrections] seeds the in-memory EWMA from it.
	ListThroughputCorrections() ([]store.ThroughputCorrection, error)
	// DeleteThroughputCorrections drops workerID's persisted corrections
	// when the baseline they're relative to changes.
	DeleteThroughputCorrections(workerID string) error
}

// New builds a Scheduler. Call [Scheduler.InitQueue] once a database is
// available and [Scheduler.Start] to launch the dispatcher.
func New(cfg *config.Config, logger zerolog.Logger, workers *worker.Fleet) *Scheduler {
	return &Scheduler{
		cfg:                  cfg,
		logger:               logger.With().Str("component", "scheduler").Logger(),
		workers:              workers,
		jobBuffers:           make(map[string]*jobBuffer),
		devQueues:            make(map[string]queue.QueueInterface),
		tails:                make(map[string]tailState),
		inflightSeconds:      make(map[string]float64),
		inflightByRequest:    make(map[string]inflightRecord),
		dispatchingByRequest: make(map[string]bool),
		memoryReservations:   make(map[string]int64),
		reestimateLocks:      make(map[string]*sync.Mutex),
		benchCache:           make(map[string]map[string]store.BenchmarkRow),
		draining:             make(map[string]bool),
		throughputCorrection: make(map[string]correctionState),
		wake:                 make(chan struct{}, 1),
	}
}

// InitQueue wires the durable queue subsystem and the device-queue state
// store. Must be called before [Scheduler.Start].
func (s *Scheduler) InitQueue(pool *queue.Pool, results queue.ResultStoreInterface, st StateStoreInterface) {
	s.queueMu.Lock()
	s.queuePool = pool
	s.globalQ = pool.Open("global")
	s.results = results
	s.store = st
	s.queueMu.Unlock()
	s.restoreCorrections(st)
}

// SetWorkerEnabledFn registers the per-worker enable check.
func (s *Scheduler) SetWorkerEnabledFn(fn WorkerEnabledFn) {
	s.workerEnabledMu.Lock()
	s.workerEnabled = fn
	s.workerEnabledMu.Unlock()
}

// SetDeviceEnabledFn registers the per-device enable check used to decide
// which device queues to create for each worker.
func (s *Scheduler) SetDeviceEnabledFn(fn DeviceEnabledFn) {
	s.workerEnabledMu.Lock()
	s.deviceEnabled = fn
	s.workerEnabledMu.Unlock()
}

// SetRuntimeDefaultAxisFn registers the per-runtime default-axis lookup
// used by the scheduler's throughput-fallback path. When a Submit names
// a cost_axis a candidate worker hasn't benched, the scheduler falls
// back to this axis instead. Without the callback wired, only exact-
// axis matches are eligible.
func (s *Scheduler) SetRuntimeDefaultAxisFn(fn RuntimeDefaultAxisFn) {
	s.workerEnabledMu.Lock()
	s.runtimeAxis = fn
	s.workerEnabledMu.Unlock()
}

func (s *Scheduler) runtimeDefaultAxis(runtimeName string) string {
	s.workerEnabledMu.RLock()
	fn := s.runtimeAxis
	s.workerEnabledMu.RUnlock()
	if fn == nil {
		return ""
	}
	return fn(runtimeName)
}

func (s *Scheduler) isWorkerEnabled(id string) bool {
	s.workerEnabledMu.RLock()
	fn := s.workerEnabled
	s.workerEnabledMu.RUnlock()
	if fn == nil {
		return true
	}
	return fn(id)
}

func (s *Scheduler) isDeviceEnabled(workerID, deviceID string) bool {
	s.workerEnabledMu.RLock()
	fn := s.deviceEnabled
	s.workerEnabledMu.RUnlock()
	if fn == nil {
		return true
	}
	return fn(workerID, deviceID)
}

// --- Worker queries ---

// WorkersForRuntime returns every online, operator-enabled worker
// advertising runtimeName.
func (s *Scheduler) WorkersForRuntime(runtimeName string) []*worker.StreamWorker {
	all := s.workers.All()
	out := make([]*worker.StreamWorker, 0, len(all))
	for _, w := range all {
		sw, ok := w.(*worker.StreamWorker)
		if !ok || !sw.Status().Online {
			continue
		}
		if sw.RuntimeName() != runtimeName {
			continue
		}
		if !s.isWorkerEnabled(sw.ID()) {
			continue
		}
		out = append(out, sw)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// WorkersWithModel returns workers of runtimeName that report modelID as
// loaded. A nil/empty modelID returns all workers of the kind.
func (s *Scheduler) WorkersWithModel(runtimeName, modelID string) []*worker.StreamWorker {
	all := s.WorkersForRuntime(runtimeName)
	if modelID == "" {
		return all
	}
	out := make([]*worker.StreamWorker, 0, len(all))
	for _, w := range all {
		for _, lm := range w.LoadedModels() {
			if lm.ModelID == modelID {
				out = append(out, w)
				break
			}
		}
	}
	return out
}

// --- Submit / StreamChunks ---

// SubmitRequest captures the gateway's job submission. The envelope's
// Files + LoadHints travel with every Submit so MASS can load-on-demand
// at dispatch when the chosen worker doesn't have ModelID resident.
type SubmitRequest struct {
	RuntimeName string
	ModelID     string
	Payload     []byte
	// Cost is the gateway's prediction of how expensive this job is in
	// the runtime's reference cost units. MASS never interprets the
	// units — all prediction physics lives runtime-side — the only
	// quantity MASS derives is time: Cost divided by the chosen worker's
	// throughput on CostAxis is the predicted wall-clock seconds every
	// scoring, tail, and calibration decision operates on. Required (> 0).
	Cost float64
	// CostAxis names the throughput dimension Cost divides by. The
	// runtime's gateway declares a default axis on Init that MASS uses
	// as fallback when CostAxis names something a worker hasn't
	// benched. Required (non-empty).
	CostAxis string
	// Files are the load artifacts MASS may need to ship to a worker that
	// doesn't already have ModelID loaded. Forwarded verbatim as
	// HubLoadModel.files.
	Files []*workerpb.ModelFile
	// LoadHints is the gateway-defined load configuration blob.
	// Forwarded verbatim as HubLoadModel.load_hints.
	LoadHints []byte
	// BaseLoadBytes is the gateway's prediction of the fixed device
	// memory cost the load pays regardless of concurrency. MASS uses
	// it to reject submits when no fleet member's hardware could fit
	// it (Submit-time) and to filter workers whose free memory is too
	// small at dispatch time. 0 = unknown — both checks pass-through.
	BaseLoadBytes int64
	// PerSlotBytes is the gateway's prediction of the incremental
	// memory cost per concurrent slot. MASS combines it with the
	// chosen worker's free memory and HeadroomPct to project the
	// post-grow pool size used for wall-clock load latency. 0 = no
	// concurrency dimension (projection collapses to pool=1).
	PerSlotBytes int64
	// HeadroomPct is the operator's explicit per-load device-memory
	// watermark override (1-100), set only when the load hints carry
	// one. The worker gives a per-load hint precedence over its own
	// --vram-headroom-pct flag, so when present this value wins the
	// projection too — see effectiveHeadroomPct. 0 = no override; MASS
	// uses the worker's registration-reported flag, then
	// defaultHeadroomPct.
	HeadroomPct int32
	// Source identifies the caller. Surfaced in MASS's Scheduler tab.
	// Empty defaults to "gateway:<runtime_name>".
	Source string
	// Priority orders the job within its worker queue (higher dequeues
	// first). The RPC boundary maps the proto enum here, defaulting an
	// unspecified value to PriorityMedium. Placement is unaffected —
	// priority governs dequeue order only.
	Priority queue.Priority
}

// Submit persists the job on the global queue (durability anchor) and,
// when an online worker is available, hands it off to that worker's queue
// in the same call. If no schedulable worker exists yet, the row stays
// unleased on global and drainGlobal re-scores it every tick.
//
// The global row remains leased after a successful handoff — it is the
// recovery anchor for the in-flight job. It's released + deleted on the
// terminal frame (dispatchEnvelope), or released-only on a worker
// disconnect (OnWorkerDisconnected) so drainGlobal can re-place it.
//
// Returns ErrNoWorker when no online worker matches the runtime.
// Returns ErrInvalidCost / ErrInvalidCostAxis when the gateway omitted
// the throughput contract fields.
func (s *Scheduler) Submit(ctx context.Context, req SubmitRequest) (string, error) {
	if req.RuntimeName == "" {
		return "", fmt.Errorf("submit: runtime_name required")
	}
	if req.Cost <= 0 {
		return "", ctxerr.With(ErrInvalidCost, map[string]any{"runtime_name": req.RuntimeName})
	}
	if req.CostAxis == "" {
		return "", ctxerr.With(ErrInvalidCostAxis, map[string]any{"runtime_name": req.RuntimeName})
	}
	source := req.Source
	if source == "" {
		source = "gateway:" + req.RuntimeName
	}
	// The envelope wire format length-prefixes identity fields with one
	// byte; Marshal panics on oversize, so reject bad input here at the
	// boundary. The resolved source is checked (not req.Source) because
	// the "gateway:" prefix counts against the cap too.
	for _, f := range []struct{ name, value string }{
		{"runtime_name", req.RuntimeName},
		{"model_id", req.ModelID},
		{"cost_axis", req.CostAxis},
		{"source", source},
	} {
		if len(f.value) > 255 {
			return "", ctxerr.With(fmt.Errorf("%w: %s", ErrFieldTooLong, f.name), map[string]any{"field": f.name, "length": len(f.value)})
		}
	}
	if err := s.preflight(req); err != nil {
		return "", err
	}

	s.queueMu.RLock()
	globalQ := s.globalQ
	results := s.results
	s.queueMu.RUnlock()
	if globalQ == nil {
		return "", fmt.Errorf("submit: queue not initialised")
	}

	requestID := uuid.NewString()
	s.jobsMu.Lock()
	s.jobBuffers[requestID] = newJobBuffer()
	s.jobsMu.Unlock()

	if results != nil {
		if err := results.Create(requestID); err != nil {
			s.dropJob(requestID)
			return "", ctxerr.With(fmt.Errorf("creating result entry: %w", err), map[string]any{"request_id": requestID})
		}
	}

	env := queue.Envelope{
		Priority:      req.Priority,
		Cost:          req.Cost,
		CostAxis:      req.CostAxis,
		RuntimeName:   req.RuntimeName,
		ModelID:       req.ModelID,
		Source:        source,
		RequestID:     requestID,
		Files:         req.Files,
		LoadHints:     req.LoadHints,
		BaseLoadBytes: req.BaseLoadBytes,
		PerSlotBytes:  req.PerSlotBytes,
		HeadroomPct:   req.HeadroomPct,
		Payload:       req.Payload,
	}
	res, err := globalQ.Submit(ctx, env)
	if err != nil {
		s.dropJob(requestID)
		return "", ctxerr.With(fmt.Errorf("submitting envelope: %w", err), map[string]any{"runtime_name": req.RuntimeName, "model_id": req.ModelID, "request_id": requestID})
	}
	env.GlobalMsgID = res.ID
	s.broadcastQueueChange()

	// Try to place inline. Skipping the dispatcher tick saves a small amount
	// of latency on the happy path. If no candidate exists, the row sits
	// unleased on global and drainGlobal will re-try on the next pass.
	s.placeOnWorkerQueue(ctx, env)
	s.kick()
	return requestID, nil
}

// placeOnWorkerQueue scores env against online workers, picks the cheapest,
// and hands env off via LeaseAndSubmit. The global row is left leased
// (not deleted) — it remains the durability anchor until the terminal
// frame.
//
// Returns silently when no candidate exists (caller relies on drainGlobal
// to retry) or when LeaseAndSubmit races a concurrent placer (the other
// side already moved the row).
func (s *Scheduler) placeOnWorkerQueue(ctx context.Context, env queue.Envelope) {
	target, queuedSeconds := s.pickWorkerQueue(env)
	if target == nil {
		return
	}
	// Stamp the per-placement bookkeeping fields before the handoff —
	// these flow with the envelope onto the worker queue and back out at
	// dispatch pop time.
	env.QueuedSeconds = queuedSeconds

	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	if globalQ == nil {
		return
	}
	_, leased, err := globalQ.LeaseAndSubmit(ctx, queue.MessageID(env.GlobalMsgID), dispatchLeaseDuration, target.q, env)
	if err != nil {
		s.logger.Warn().Err(err).Str("message_id", env.GlobalMsgID).Str("target", target.name).Msg("lease-and-submit to worker queue")
		return
	}
	if !leased {
		return // race-loser: another placer (Submit or drainGlobal) already moved it.
	}
	s.creditTail(target.name, queuedSeconds, env.ModelID)
	s.broadcastQueueChange()
}

// creditTail bumps tail_seconds + sets tail_model_id for the destination
// worker queue, reflecting the just-placed envelope. The mirror is updated
// first (it is what scoring reads), then the store row is persisted
// best-effort — errors log but do not abort dispatch, since the row is
// already on the worker queue and scoring tolerates a stale durable copy.
func (s *Scheduler) creditTail(queueName string, delta float64, modelID string) {
	s.tailMu.Lock()
	if t, ok := s.tails[queueName]; ok {
		t.seconds = max(0, t.seconds+delta)
		t.modelID = modelID
		s.tails[queueName] = t
	}
	s.tailMu.Unlock()

	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return
	}
	if err := st.AddTailSecondsAndSetModel(queueName, delta, modelID); err != nil {
		s.logger.Warn().Err(err).Str("queue", queueName).Float64("delta_seconds", delta).Msg("crediting tail")
	}
}

// StreamChunks attaches a consumer to an inflight (or recently completed)
// job's per-job ring buffer. Buffered chunks with seq >= resumeSeq are
// replayed first, then live chunks are pumped until the terminal frame is
// observed or ctx is cancelled. The channel is closed when the stream is
// done.
//
// Returns an error when requestID is unknown: either Submit was never
// called for it, or the post-terminal retention window
// ([config.Config.EffectiveStreamReplayTTL]) has already lapsed.
func (s *Scheduler) StreamChunks(ctx context.Context, requestID string, resumeSeq uint64) (<-chan SequencedChunk, error) {
	s.jobsMu.Lock()
	buf, ok := s.jobBuffers[requestID]
	s.jobsMu.Unlock()
	if !ok {
		return nil, ctxerr.With(fmt.Errorf("stream_chunks: unknown request_id %q", requestID), map[string]any{"request_id": requestID})
	}
	return buf.Attach(ctx, resumeSeq), nil
}

// preflight enforces the admission rules synchronously so a gateway sees
// no-worker errors on the RPC return rather than letting a row sit in the
// global queue with no chance of being placed. Model residency is *not*
// preflight-checked: the dispatcher loads the model on demand at pop time
// using the inline files + load_hints carried by every envelope.
func (s *Scheduler) preflight(req SubmitRequest) error {
	candidates := s.WorkersForRuntime(req.RuntimeName)
	if len(candidates) == 0 {
		return ctxerr.With(fmt.Errorf("%w: %s", ErrNoWorker, req.RuntimeName), map[string]any{"runtime_name": req.RuntimeName})
	}
	if !s.feasibleByAnyWorker(queue.Envelope{
		RuntimeName:   req.RuntimeName,
		ModelID:       req.ModelID,
		BaseLoadBytes: req.BaseLoadBytes,
	}) {
		return ctxerr.With(fmt.Errorf("%w: %d bytes", ErrNoMemoryFit, req.BaseLoadBytes), map[string]any{"runtime_name": req.RuntimeName, "base_load_bytes": req.BaseLoadBytes})
	}
	return nil
}

// dropJob removes the in-memory replay buffer for requestID. Used on
// submission failure paths and by the post-terminal sweep.
func (s *Scheduler) dropJob(requestID string) {
	s.jobsMu.Lock()
	delete(s.jobBuffers, requestID)
	s.jobsMu.Unlock()
}

// kick wakes the dispatcher non-blockingly.
func (s *Scheduler) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// EvictModel asks every worker (or one specific worker, when workerID != "")
// of runtimeName that has modelID loaded to drop it. Returns the count of
// successful unloads.
func (s *Scheduler) EvictModel(runtimeName, modelID, workerID string) (int, error) {
	candidates := s.WorkersWithModel(runtimeName, modelID)
	evicted := 0
	for _, w := range candidates {
		if workerID != "" && w.ID() != workerID {
			continue
		}
		if err := w.UnloadModel(modelID); err != nil {
			s.logger.Warn().Err(err).Str("worker_id", w.ID()).Str("model_id", modelID).Msg("evict model")
			continue
		}
		evicted++
		s.workers.NotifyLoadedChanged(w.ID())
	}
	return evicted, nil
}

// --- Worker-queue lifecycle ---

// workerQueueName builds the canonical queue name for a worker. The
// scheduling unit is the worker (not the device): llama.cpp may split a
// single model across multiple GPUs in lockstep, and CPU spill is
// another flavour of the same thing. MASS treats the worker as the
// atomic execution unit and lets the worker decide internally which of
// its enabled devices each job touches.
func workerQueueName(workerID string) string {
	return "worker|" + workerID
}

// parseWorkerQueueName reverses [workerQueueName]. Returns ok=false
// when name is not in the expected shape.
func parseWorkerQueueName(name string) (workerID string, ok bool) {
	if !strings.HasPrefix(name, "worker|") {
		return "", false
	}
	rest := name[len("worker|"):]
	if rest == "" {
		return "", false
	}
	return rest, true
}

// OnWorkerConnected materialises the single per-worker queue for w when
// at least one of its enabled devices has a benchmark on file. Workers
// without any usable (enabled + benched) device get no queue —
// scheduling skips them until a benchmark lands, at which point a
// subsequent OnWorkerConnected (or the bench-result hook) materialises
// the queue. Idempotent: safe to call on reconnect.
func (s *Scheduler) OnWorkerConnected(w *worker.StreamWorker) {
	s.queueMu.Lock()
	pool := s.queuePool
	st := s.store
	s.queueMu.Unlock()
	if pool == nil {
		return // queue not yet initialised; reconnect will retry
	}
	if !s.workerIsSchedulable(w) {
		return
	}

	wID := w.ID()
	name := workerQueueName(wID)
	enabledDevs := s.enabledDeviceIDs(w)

	s.queueMu.Lock()
	if _, exists := s.devQueues[name]; !exists {
		s.devQueues[name] = pool.Open(name)
	}
	s.queueMu.Unlock()

	// Initialize the tail mirror entry if absent. Reconnects (and the
	// bench-result hook re-calling this) must not zero a live tail —
	// the store Upsert below preserves tail columns on conflict for the
	// same reason.
	s.tailMu.Lock()
	if _, exists := s.tails[name]; !exists {
		s.tails[name] = tailState{}
	}
	s.tailMu.Unlock()

	if st != nil {
		if err := st.UpsertWorkerQueueState(store.WorkerQueueState{
			QueueName: name,
			WorkerID:  wID,
			DeviceIDs: enabledDevs,
		}); err != nil {
			s.logger.Warn().Err(err).Str("queue", name).Msg("persisting worker queue state")
		}
	}
	s.kick()
}

// enabledDeviceIDs returns the canonical IDs of every device on w that
// the operator has not disabled. Order matches w.Devices(), so callers
// that snapshot it for later comparison see deterministic ordering.
func (s *Scheduler) enabledDeviceIDs(w *worker.StreamWorker) []string {
	devs := w.Devices()
	out := make([]string, 0, len(devs))
	for _, dev := range devs {
		if !s.isDeviceEnabled(w.ID(), dev.ID) {
			continue
		}
		out = append(out, dev.ID)
	}
	return out
}

// workerIsSchedulable reports whether w has at least one enabled device
// with a recorded benchmark — the minimum bar for placing jobs on it.
// Returns true on the first eligible (enabled + benched + non-zero
// GFLOPS) device. A worker with all devices disabled, or all enabled
// devices unbenched, is skipped: MASS cannot honestly score it and the
// scheduler treats it as if it were absent.
func (s *Scheduler) workerIsSchedulable(w *worker.StreamWorker) bool {
	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		// No store wired yet — admit conservatively so a freshly-built
		// Scheduler (e.g. in tests) doesn't drop every worker. The real
		// store is wired before Start.
		return true
	}
	wID := w.ID()
	for _, dev := range w.Devices() {
		if !s.isDeviceEnabled(wID, dev.ID) {
			continue
		}
		if _, ok := s.getBenchmark(wID, dev.ID); ok {
			return true
		}
	}
	return false
}

// TryReestimateLock claims the per-worker re-estimation lock so a toggle
// handler can perform "update enable mask + recompute queued estimates"
// without racing a concurrent toggle for the same worker. Returns a
// release func when the lock was acquired, or (nil, false) when the
// worker is already mid-re-estimation. Different workers use different
// locks; cross-worker toggles never contend.
//
// Callers MUST invoke release() exactly once (typically via defer) and
// only after every store mutation + re-estimation step is done — the
// lock guards the *whole* transaction, not just the toggle write.
func (s *Scheduler) TryReestimateLock(workerID string) (release func(), ok bool) {
	s.reestimateLockMu.Lock()
	m, exists := s.reestimateLocks[workerID]
	if !exists {
		m = &sync.Mutex{}
		s.reestimateLocks[workerID] = m
	}
	s.reestimateLockMu.Unlock()
	if !m.TryLock() {
		return nil, false
	}
	return m.Unlock, true
}

// ReestimateWorkerQueue rewrites every PENDING envelope on workerID's
// queue so its QueuedSeconds reflects the worker's current enabled
// device set, and replaces tail_seconds with the new sum. Called by
// toggle handlers after the enable mask is persisted and pushed to the
// worker. Caller MUST hold the [Scheduler.TryReestimateLock] for the
// same workerID.
//
// LEASED (in-flight) rows are NOT recomputed: a running job is already
// executing on the device set it was scheduled against, the worker
// won't reroute it mid-stream, so its predicted wall-clock cost
// doesn't change with a toggle. The leased row's existing QueuedSeconds
// still contributes to the tail sum unchanged — the prediction is
// still valid for that specific in-flight job.
//
// Envelopes whose ModelID is empty, or whose CostAxis is unbenched on
// the worker's new device set, contribute 0 to the sum — the
// dispatcher's eligibility gate will surface those rows as "no fit"
// when they reach the head. Best-effort: per-row decode/encode errors
// are logged and the row is skipped — an undecodable row's on-disk
// QueuedSeconds is unreadable, so it is excluded from the new tail sum;
// an encode-back failure keeps the row's old on-disk value while the
// sum carries the recomputed one.
func (s *Scheduler) ReestimateWorkerQueue(ctx context.Context, workerID string) {
	wIface := s.workers.Get(workerID)
	sw, ok := wIface.(*worker.StreamWorker)
	if !ok || sw == nil {
		return
	}

	queueName := workerQueueName(workerID)
	s.queueMu.RLock()
	q, qOK := s.devQueues[queueName]
	st := s.store
	s.queueMu.RUnlock()
	if !qOK || st == nil {
		return
	}

	rows, err := q.PeekAll(ctx, peekAllLimit)
	if err != nil {
		s.logger.Warn().Err(err).Str("worker", workerID).Msg("re-estimate: peeking queue")
		return
	}

	defaultAxis := s.runtimeDefaultAxis(sw.RuntimeName())
	loadBytesSec := s.effectiveLoadThroughput(sw)

	var (
		tailSum    float64
		tailModel  string
		residentID string
	)
	if loaded := sw.LoadedModels(); len(loaded) > 0 {
		residentID = loaded[0].ModelID
	}

	for _, row := range rows {
		if row == nil {
			continue
		}
		env, decErr := queue.UnmarshalEnvelope(row.Body)
		if decErr != nil {
			// Undecodable row: its on-disk QueuedSeconds is unreadable, so
			// there is nothing to preserve — exclude it from the new tail
			// sum entirely.
			s.logger.Warn().Err(decErr).Str("worker", workerID).Str("msg_id", string(row.ID)).
				Msg("re-estimate: decoding envelope")
			continue
		}
		if row.Leased {
			// In-flight on the pre-toggle device set; its prediction
			// is still the right one for that specific running job.
			// Keep its existing QueuedSeconds and tail contribution
			// untouched; advance residency as if it completed (so
			// pending peers behind it preload the right model).
			tailSum += env.QueuedSeconds
			if env.ModelID != "" {
				tailModel = env.ModelID
				residentID = env.ModelID
			}
			continue
		}
		newQueued := s.predictedQueuedSeconds(sw, env, defaultAxis, loadBytesSec, residentID)
		env.QueuedSeconds = newQueued
		bodyBytes := env.Marshal()
		if err := q.UpdateBody(ctx, row.ID, bodyBytes); err != nil {
			s.logger.Warn().Err(err).Str("worker", workerID).Str("msg_id", string(row.ID)).
				Msg("re-estimate: writing envelope back")
		}
		tailSum += newQueued
		if env.ModelID != "" {
			tailModel = env.ModelID
			residentID = env.ModelID // walked envelopes "preload" the next
		}
	}

	s.tailMu.Lock()
	if _, ok := s.tails[queueName]; ok {
		s.tails[queueName] = tailState{seconds: max(0, tailSum), modelID: tailModel}
	}
	s.tailMu.Unlock()
	if err := st.SetTailSeconds(queueName, tailSum, tailModel); err != nil {
		s.logger.Warn().Err(err).Str("worker", workerID).Msg("re-estimate: persisting tail_seconds")
	}
	s.broadcastQueueChange()
}

// predictedQueuedSeconds returns the wall-clock seconds env will keep
// the worker busy under the current device set: compute time +
// load-switch cost (zero when env's model will already be resident by
// the time we get there — same rule as [loadLatencyForCand]).
func (s *Scheduler) predictedQueuedSeconds(w *worker.StreamWorker, env queue.Envelope, defaultAxis string, loadBytesSec float64, residentID string) float64 {
	tput, _, ok := s.effectiveThroughput(w, env.CostAxis, defaultAxis)
	if !ok || tput <= 0 {
		return 0
	}
	taskSec := env.Cost / tput
	switchSec := 0.0
	if env.ModelID != "" && env.ModelID != residentID {
		bytes := s.projectedLoadBytes(w, env)
		if bytes <= 0 {
			bytes = totalLoadBytes(env.Files)
		}
		if bytes > 0 && loadBytesSec > 0 {
			switchSec = float64(bytes) / loadBytesSec
		}
	}
	return taskSec + switchSec
}

// peekAllLimit caps full-queue PeekAll walks (re-estimation, disconnect
// drain) at a sane upper bound. Worker queues realistically hold dozens
// of rows, not thousands; sharing one cap keeps the walks consistent so
// no path strands rows another can still see.
const peekAllLimit = 256

// OnWorkerDevicesChanged is called when the operator toggles a worker's
// device enable flags. When every device is disabled the worker can no
// longer host jobs — drain its pending queue back to global so peers
// can pick the rows up. When at least one device remains enabled,
// just kick the dispatcher: newly-eligible peers can pull from global,
// and jobs already on the worker queue still place under default
// placement on the surviving enabled set.
//
// The worker_queue_state row stays in either case; re-enabling a
// device lets the queue receive again without a reconnect cycle.
func (s *Scheduler) OnWorkerDevicesChanged(workerID string) {
	// The enabled-device set is part of the correction factor's identity —
	// the bench prior sums across the set (see [Scheduler.throughputForAxis]),
	// so evidence learned on the old set doesn't transfer to the new one.
	s.ResetCorrections(workerID)

	wIface := s.workers.Get(workerID)
	sw, ok := wIface.(*worker.StreamWorker)
	if !ok || sw == nil {
		return
	}

	s.queueMu.RLock()
	globalQ := s.globalQ
	q, qOK := s.devQueues[workerQueueName(workerID)]
	s.queueMu.RUnlock()
	if globalQ == nil || !qOK {
		return
	}

	if len(s.deviceSet(sw)) == 0 {
		s.drainWorkerQueue(context.Background(), q, globalQ)
		s.broadcastQueueChange()
		return
	}
	s.kick()
	s.broadcastQueueChange()
}

// OnWorkerDisconnected drains the worker's queue back to the global
// queue so pending jobs can be re-placed on a peer. The
// worker_queue_state row is removed: workers re-register on reconnect.
func (s *Scheduler) OnWorkerDisconnected(workerID string) {
	// Best-effort runtime label for the disconnect counter — Get() returns
	// nil when the fleet has already deregistered the worker.
	runtimeName := ""
	if s.workers != nil {
		if w := s.workers.Get(workerID); w != nil {
			runtimeName = w.RuntimeName()
		}
	}
	metrics.WorkerDisconnect(runtimeName)

	// Bench cache rows for a disconnected worker are stale — drop them
	// so a reconnect with new hardware (re-benched devices, removed
	// devices) doesn't read against the previous topology. Cheap and
	// always-safe regardless of whether the worker had a queue.
	s.InvalidateWorkerBench(workerID)

	name := workerQueueName(workerID)

	s.queueMu.Lock()
	pool := s.queuePool
	globalQ := s.globalQ
	st := s.store
	q, ok := s.devQueues[name]
	s.queueMu.Unlock()
	if pool == nil || globalQ == nil || !ok {
		return
	}

	ctx := context.Background()
	s.drainWorkerQueue(ctx, q, globalQ)

	s.queueMu.Lock()
	delete(s.devQueues, name)
	s.queueMu.Unlock()

	s.tailMu.Lock()
	delete(s.tails, name)
	s.tailMu.Unlock()
	s.broadcastQueueChange()

	if st == nil {
		return
	}
	if err := st.DeleteWorkerQueueState(name); err != nil {
		s.logger.Warn().Err(err).Str("queue", name).Msg("deleting worker queue state")
	}
}

// drainWorkerQueue reaps every visible row from src and releases the
// global durability anchor for each so drainGlobal can re-place the
// envelope on a surviving worker.
//
// In-flight rows are reaped here too: the worker's gRPC stream closed on
// disconnect, so pumpWorkerChunks's workerCh will end with no terminal
// frame. Under the new model, pump detects offline + leaves the global
// anchor alone; this drain is what actually releases it. An in-flight
// row's re-placement is charged against disconnectRequeueBudget —
// without the cap, a job that crashes its worker cycles forever
// (requeue → redispatch → crash) and wedges every caller waiting on it.
// Past the budget the result fails terminally. Queued-but-never-
// dispatched rows carry no blame and re-place without consuming an
// attempt.
//
// Uses PeekAll (not Peek) so leased rows — which is exactly what an
// in-flight row looks like on disk — are included. Peek-only would
// strand every job that was actually running on the worker, since
// those rows are leased through the pump goroutine's lifetime.
//
// Best-effort: races (the global row already gone, a row already deleted
// by pump) and per-row errors are logged and skipped.
func (s *Scheduler) drainWorkerQueue(ctx context.Context, src, globalQ queue.QueueInterface) {
	msgs, err := src.PeekAll(ctx, peekAllLimit)
	if err != nil {
		s.logger.Warn().Err(err).Msg("peeking device queue for drain")
		return
	}
	for _, msg := range msgs {
		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Msg("unmarshal envelope on drain")
			continue
		}
		// A cancelled job must not be re-placed: drop both rows instead of
		// releasing the anchor back to global. Races the pump's own cancel
		// finalize; DeleteBoth is idempotent enough that whichever runs second
		// just no-ops on the already-gone rows.
		if s.isInflightCancelled(env.RequestID) {
			s.failResult(env.RequestID, "cancelled by operator")
			if err := src.DeleteBoth(ctx, msg.ID, globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
				s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("global_msg_id", env.GlobalMsgID).Msg("dropping cancelled row on worker drain")
			}
			continue
		}
		// A job whose terminal result already landed must not be re-placed
		// either. This isn't a rare race: a worker that detects device loss
		// ships the terminal error frame and only then exits for its clean
		// restart, so this drain always runs moments after that result was
		// stored. Requeueing would re-run work whose outcome the caller
		// already has — and for device loss, re-run the exact job that
		// killed the worker, cycling it through another crash.
		if s.resultIsTerminal(env.RequestID) {
			s.logger.Info().Str("request_id", env.RequestID).Msg("job already has a terminal result; dropping instead of requeueing on worker drain")
			if err := src.DeleteBoth(ctx, msg.ID, globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
				s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("global_msg_id", env.GlobalMsgID).Msg("dropping completed row on worker drain")
			}
			continue
		}
		if !msg.Leased {
			if err := src.DeleteAndReleaseLease(ctx, msg.ID, globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
				s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("global_msg_id", env.GlobalMsgID).Msg("releasing global anchor on worker drain")
			}
			continue
		}
		if int(env.Attempts)+1 >= disconnectRequeueBudget {
			s.failResult(env.RequestID, fmt.Sprintf(
				"worker disconnected while the job was running (attempt %d/%d)",
				env.Attempts+1, disconnectRequeueBudget))
			if err := src.DeleteBoth(ctx, msg.ID, globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
				s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("global_msg_id", env.GlobalMsgID).Msg("dropping exhausted row on worker drain")
			}
			continue
		}
		// Same re-placement shape as retryAfterLoadFailure: drop both rows
		// and submit a fresh global envelope with the attempt recorded (a
		// released anchor would keep the on-disk Attempts at its old value).
		oldGlobalID := env.GlobalMsgID
		env.Attempts++
		env.GlobalMsgID = ""
		env.QueuedSeconds = 0
		if err := src.DeleteBoth(ctx, msg.ID, globalQ, queue.MessageID(oldGlobalID)); err != nil {
			s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("global_msg_id", oldGlobalID).Msg("deleting in-flight rows on worker drain")
			continue
		}
		if _, err := globalQ.Submit(ctx, env); err != nil {
			// Both rows are gone; without the resubmit the job would vanish
			// silently — fail it so the caller sees the loss.
			s.logger.Warn().Err(err).Str("request_id", env.RequestID).Msg("resubmitting in-flight job on worker drain")
			s.failResult(env.RequestID, "requeue after worker disconnect failed: "+err.Error())
			continue
		}
		s.logger.Info().Str("request_id", env.RequestID).Uint8("attempt", env.Attempts).Int("budget", disconnectRequeueBudget).Msg("worker died mid-job; requeued for retry on a different worker")
		// The in-flight result reads processing; this drain owns the
		// processing→pending revert (pumpWorkerChunks' worker-offline path
		// deliberately leaves it alone; the store-side guard keeps a racing
		// terminal write from being regressed).
		s.pendingResult(env.RequestID)
	}
}

// recoverPersistedQueues reattaches to worker_queue_state rows that
// survived a restart and seeds the tail mirror from them, so queued-time
// estimates survive a scheduler restart. Queues whose worker isn't
// currently connected stay reattached — drain or steal them when
// capacity arrives.
func (s *Scheduler) recoverPersistedQueues() {
	s.queueMu.Lock()
	pool := s.queuePool
	st := s.store
	s.queueMu.Unlock()
	if pool == nil || st == nil {
		return
	}
	rows, err := st.ListWorkerQueueStates()
	if err != nil {
		s.logger.Warn().Err(err).Msg("listing persisted device queue states")
		return
	}
	s.queueMu.Lock()
	for _, row := range rows {
		if _, exists := s.devQueues[row.QueueName]; !exists {
			s.devQueues[row.QueueName] = pool.Open(row.QueueName)
		}
	}
	s.queueMu.Unlock()

	s.tailMu.Lock()
	for _, row := range rows {
		if _, exists := s.tails[row.QueueName]; !exists {
			// The store clamps tail_seconds at 0 on every write; the
			// max here just keeps a hand-edited row from poisoning
			// scoring, mirroring the old read-side clamp.
			s.tails[row.QueueName] = tailState{seconds: max(0, row.TailSeconds), modelID: row.TailModelID}
		}
	}
	s.tailMu.Unlock()
}

// --- Dispatcher ---

// Start launches the dispatcher goroutine. Cancel ctx to stop. Safe to call
// once after [Scheduler.InitQueue]; subsequent calls are no-ops.
//
// The abandoned-row reap runs synchronously before the dispatcher spins
// up: rows a prior process crashed on must be failed-and-deleted before
// any placement or steal pass can observe them, and running it here
// (not concurrently) means it can never race a live dispatch.
func (s *Scheduler) Start(ctx context.Context) {
	s.reapAbandonedAtStartup(ctx)
	s.recoverPersistedQueues()
	go s.dispatchLoop(ctx)
	go s.replayBufferSweepLoop(ctx)
	go s.metricsSweepLoop(ctx)
}

// reapAbandonedAtStartup runs [queue.ReapAbandoned] over the global
// queue plus every persisted worker queue — including queues whose
// worker hasn't reconnected — so rows stranded past their delivery
// budget by a MASS crash get a failure result instead of hanging their
// callers forever. Best-effort: errors log and startup proceeds (the
// rows stay dormant; goqite never redelivers past-budget rows).
func (s *Scheduler) reapAbandonedAtStartup(ctx context.Context) {
	s.queueMu.RLock()
	pool := s.queuePool
	globalQ := s.globalQ
	results := s.results
	st := s.store
	s.queueMu.RUnlock()
	if pool == nil || globalQ == nil || results == nil || st == nil {
		return
	}

	queues := []queue.QueueInterface{globalQ}
	rows, err := st.ListWorkerQueueStates()
	if err != nil {
		s.logger.Warn().Err(err).Msg("reap: listing persisted worker queues")
	} else {
		for _, row := range rows {
			queues = append(queues, pool.Open(row.QueueName))
		}
	}

	reaped, err := queue.ReapAbandoned(ctx, queues, results, s.logger)
	if err != nil {
		s.logger.Warn().Err(err).Msg("reaping abandoned queue rows at startup")
	}
	if reaped > 0 {
		s.logger.Info().Int("reaped", reaped).Msg("failed abandoned queue rows from a prior run")
	}
}

// metricsSweepLoop refreshes the gauge metrics (queue depth, workers
// registered, jobs in flight) every 5 seconds. Counters and histograms
// are emitted inline at their event sites; gauges live here so the
// reconciliation logic stays in one place and scrapes see a recent value
// without per-event sprawl.
func (s *Scheduler) metricsSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.refreshGauges()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// refreshGauges snapshots queue depth, worker counts, and in-flight
// counts and writes them to the metrics package. Cheap; safe to call
// from the sweep ticker.
func (s *Scheduler) refreshGauges() {
	// Workers registered, by runtime.
	byRuntime := map[string]int{}
	if s.workers != nil {
		for _, w := range s.workers.All() {
			byRuntime[w.RuntimeName()]++
		}
	}
	for rt, n := range byRuntime {
		metrics.WorkersRegistered(rt, n)
	}

	// Queue depth — pending rows only. Global is unleased global rows;
	// worker is the sum of pending (unleased) rows across every worker
	// queue. Leased global rows mean "placed on a worker", which would
	// double-count. Depth shares Peek's visibility predicate (timeout <=
	// now) but is a bare COUNT(*) — Peek would drag every pending body
	// (payloads can carry multi-MB blobs) through the driver just to be
	// counted, every sweep.
	globalPending, workerPending := 0, 0
	s.queueMu.RLock()
	globalQ := s.globalQ
	devQueues := make(map[string]queue.QueueInterface, len(s.devQueues))
	for k, v := range s.devQueues {
		devQueues[k] = v
	}
	s.queueMu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if globalQ != nil {
		if n, err := globalQ.Depth(ctx); err == nil {
			globalPending = n
		}
	}
	for _, q := range devQueues {
		if n, err := q.Depth(ctx); err == nil {
			workerPending += n
		}
	}
	metrics.QueueDepth("global", globalPending)
	metrics.QueueDepth("worker", workerPending)

	// Jobs in flight, by runtime — read straight from the inflight
	// tracker (where runtime was captured at startInflight).
	inflightByRuntime := map[string]int{}
	s.inflightMu.Lock()
	for _, rec := range s.inflightByRequest {
		inflightByRuntime[rec.runtimeName]++
	}
	s.inflightMu.Unlock()
	for rt, n := range inflightByRuntime {
		metrics.JobsInflight(rt, n)
	}

	// Zero the series of runtimes reported last sweep but absent now, then
	// remember this sweep's label sets — see the field docs for why zeroing
	// (not deletion) is the contract.
	workerSet := make(map[string]struct{}, len(byRuntime))
	for rt := range byRuntime {
		workerSet[rt] = struct{}{}
	}
	inflightSet := make(map[string]struct{}, len(inflightByRuntime))
	for rt := range inflightByRuntime {
		inflightSet[rt] = struct{}{}
	}
	s.gaugeMu.Lock()
	for rt := range s.prevWorkerRuntimes {
		if _, ok := workerSet[rt]; !ok {
			metrics.WorkersRegistered(rt, 0)
		}
	}
	for rt := range s.prevInflightRuntimes {
		if _, ok := inflightSet[rt]; !ok {
			metrics.JobsInflight(rt, 0)
		}
	}
	s.prevWorkerRuntimes = workerSet
	s.prevInflightRuntimes = inflightSet
	s.gaugeMu.Unlock()
}

// replayBufferSweepLoop drops per-job ring buffers whose terminal frame
// is older than the configured replay TTL. Runs at half the TTL so any
// buffer lives at least one tick past its terminal frame.
func (s *Scheduler) replayBufferSweepLoop(ctx context.Context) {
	ttl := s.cfg.EffectiveStreamReplayTTL()
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepReplayBuffers(s.cfg.EffectiveStreamReplayTTL())
		}
	}
}

// jobBufferMaxAge is how long a job buffer may live without reaching a
// terminal frame before the sweep reaps it. A buffer for a job that never
// becomes dispatchable (its worker never returns, its runtime is gone)
// would otherwise leak forever, along with its pending result row.
// Generous on purpose: no legitimate job runs this long.
const jobBufferMaxAge = 24 * time.Hour

// sweepReplayBuffers removes any per-job ring buffer whose terminal frame
// fired more than ttl ago. Non-terminal buffers are left alone until they
// exceed [jobBufferMaxAge]; then they are reaped too — their result row is
// failed and a terminal error chunk is appended so attached consumers
// close instead of pumping forever.
func (s *Scheduler) sweepReplayBuffers(ttl time.Duration) {
	now := time.Now()
	terminalCutoff := now.Add(-ttl)
	ageCutoff := now.Add(-jobBufferMaxAge)

	type expiredJob struct {
		id  string
		buf *jobBuffer
	}
	var expired []expiredJob
	s.jobsMu.Lock()
	for id, buf := range s.jobBuffers {
		terminal, at := buf.terminalReached()
		switch {
		case terminal && at.Before(terminalCutoff):
			delete(s.jobBuffers, id)
		case !terminal && buf.createdAt.Before(ageCutoff):
			delete(s.jobBuffers, id)
			expired = append(expired, expiredJob{id: id, buf: buf})
		}
	}
	s.jobsMu.Unlock()

	// Fail the expired jobs outside jobsMu — failResult hits the database,
	// and must land before the Append: the terminal frame unblocks attached
	// consumers straight into a result read.
	errText := fmt.Sprintf("job reaped: no terminal frame within %s", jobBufferMaxAge)
	for _, e := range expired {
		s.failResult(e.id, errText)
		e.buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: errText})
		s.logger.Warn().Str("request_id", e.id).Msg("reaped never-terminal job buffer")
	}
}

// dispatchLoop is the main event loop. Wakes on any pool-queue change
// (global or worker submissions, moves, lease releases — all surfaced on
// the pool's single notify channel), worker capacity changes (via kick),
// and a slow ticker for stuck-row paranoia.
func (s *Scheduler) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		s.queueMu.RLock()
		globalQ := s.globalQ
		pool := s.queuePool
		s.queueMu.RUnlock()
		if globalQ == nil || pool == nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}

		pending := s.dispatchPass(ctx)

		// When a row stayed on global because no worker was eligible yet
		// (e.g. all workers still benching), retry on a short interval
		// instead of waiting the slow ticker. The condition self-clears
		// once the row places, so this isn't a busy-spin — it polls only
		// while genuinely-unplaceable rows sit on global.
		var retry *time.Timer
		var retryCh <-chan time.Time
		if pending {
			retry = time.NewTimer(pendingRetryInterval)
			retryCh = retry.C
		}

		// The pool channel covers the global queue and every worker queue
		// — InitQueue and OnWorkerConnected open them all from the same
		// pool. Capacity 1: a signal fired during dispatchPass stays
		// buffered, so the receive below returns immediately — the same
		// sticky-wakeup property the per-queue channels had.
		select {
		case <-ctx.Done():
			if retry != nil {
				retry.Stop()
			}
			return
		case <-pool.NotifyCh():
		case <-s.wake:
		case <-ticker.C:
		case <-retryCh:
		}
		if retry != nil {
			retry.Stop()
		}
	}
}

// pendingRetryInterval is how soon the dispatcher retries a row it could
// not place this pass — one sitting on global with no eligible worker, or
// one bounced by the device-set gate (see [Scheduler.releaseLeaseForRetry])
// — short enough to mask a bench warm-up window, long enough not to
// busy-spin.
const pendingRetryInterval = 200 * time.Millisecond

// dispatchPass does one round of: drain global → device queues, then drain
// device queues → workers, then attempt work stealing for idle workers.
// Returns true when global still holds rows that couldn't be placed this
// pass, so the loop can retry sooner than the slow ticker.
func (s *Scheduler) dispatchPass(ctx context.Context) (pending bool) {
	pending = s.drainGlobal(ctx)
	s.drainDeviceQueues(ctx)
	s.attemptSteals(ctx)
	return pending
}

// drainGlobal peeks the global queue and tries to place each row on a
// worker queue. Rows that can't be placed right now (no workers, none
// benched, etc.) stay unleased on global and are retried next pass.
//
// Successful placements leave the global row LEASED — it remains the
// recovery anchor for the in-flight envelope. The lease is renewed by
// the pump and released on terminal frame (DeleteBoth) or worker
// disconnect (DeleteAndReleaseLease).
// Returns true when at least one peeked row had no eligible target this
// pass (so it stays on global) — the dispatch loop uses that to retry on a
// short interval instead of waiting for the slow ticker.
func (s *Scheduler) drainGlobal(ctx context.Context) (pending bool) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	if globalQ == nil {
		return false
	}
	msgs, err := globalQ.Peek(ctx, 32)
	if err != nil {
		s.logger.Warn().Err(err).Msg("peeking global queue")
		return false
	}
	for _, msg := range msgs {
		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Msg("unmarshalling global envelope")
			continue
		}
		env.GlobalMsgID = string(msg.ID)
		target, queuedSeconds := s.pickWorkerQueue(env)
		if target == nil {
			pending = true // no eligible target right now; row stays
			continue
		}
		env.QueuedSeconds = queuedSeconds
		_, leased, err := globalQ.LeaseAndSubmit(ctx, msg.ID, dispatchLeaseDuration, target.q, env)
		if err != nil {
			s.logger.Warn().Err(err).Str("message_id", string(msg.ID)).Str("target", target.name).Msg("lease-and-submit to worker queue")
			continue
		}
		if !leased {
			continue // race-loser: another placer already moved this row
		}
		s.creditTail(target.name, queuedSeconds, env.ModelID)
	}
	return pending
}

// workerQueueTarget pairs a worker queue with its owning worker so the
// scoring decision and the eventual AssignJob call share one lookup.
type workerQueueTarget struct {
	name   string
	q      queue.QueueInterface
	worker *worker.StreamWorker
}

// pickWorkerQueue chooses the worker queue on which env is expected to
// COMPLETE soonest, measured in wall-clock seconds:
//
//	score(w) = inflight_seconds(w) + tail_seconds(w) + load_latency(w, env)
//	           + env.Cost / throughput(w)
//
// The job's own compute time is part of the score: without it two idle
// workers tie at 0 and a heterogeneous fleet places by map-iteration
// accident rather than sending the job where it finishes first.
//
// load_latency is zero when the worker is effectively-warm for
// env.ModelID (see [loadLatencyForCand]); otherwise it's the projected
// upload size divided by the worker's measured host→device throughput.
//
// queuedSeconds is what this envelope will contribute to the chosen
// queue's tail_seconds: env.Cost / throughput_w + load_latency_w. The
// dispatcher pop subtracts this exact value, so tail stays consistent.
//
// Returns (nil, 0) when no candidate is eligible (no online enabled-and-
// benched worker with capacity > 0). drainGlobal leaves the row on
// global in that case and retries next tick.
func (s *Scheduler) pickWorkerQueue(env queue.Envelope) (*workerQueueTarget, float64) {
	candidates := s.WorkersForRuntime(env.RuntimeName)
	if len(candidates) == 0 {
		return nil, 0
	}

	// Snapshot the candidate set under the lock, then score outside it so
	// DB hits (tail_seconds reads, bench cache misses) don't block worker
	// connect/disconnect.
	type cand struct {
		t            workerQueueTarget
		throughput   float64
		loadBytesSec float64
	}
	defaultAxis := s.runtimeDefaultAxis(env.RuntimeName)
	var cands []cand
	s.queueMu.RLock()
	for _, w := range candidates {
		// Eligibility filter: TargetDeviceIDs (when set) must be a subset
		// of the worker's enabled devices. Skips workers that physically
		// can't host the operator's placement choice so MASS doesn't
		// pick a worker whose load will fail.
		if !s.eligibleWorker(w, env) {
			continue
		}
		// Capacity gate is residency-aware. A worker with the model
		// resident AND no blockers has a real concurrency cap
		// (pool_size) that we must respect — full pool means future
		// tasks queue, not displace. A worker WITHOUT the model
		// resident, or one that's resident-stale / has other-model
		// conflicts, reports a pool that won't survive dispatch
		// (it'll be evicted + reloaded with a fresh pool), so the
		// capacity number is meaningless. Exclude only resident-fitting
		// saturated workers. Capacity is net of MASS's own in-flight jobs
		// (heartbeat AvailableCapacity lags real-time dispatch) so the
		// picker doesn't route a burst onto a worker the dispatcher will
		// then refuse — see drainOneWorkerQueue.
		if env.ModelID != "" && workerHasModel(w, env.ModelID) &&
			len(residentsBlockingLoad(w, env.ModelID, s.predictDeviceSet(w))) == 0 &&
			w.AvailableCapacity()-s.inflightCountForWorker(w.ID()) <= 0 {
			continue
		}
		name := workerQueueName(w.ID())
		q, ok := s.devQueues[name]
		if !ok {
			continue
		}
		tput, _, ok := s.effectiveThroughput(w, env.CostAxis, defaultAxis)
		if !ok {
			continue // worker hasn't benched the requested axis or the runtime fallback
		}
		cands = append(cands, cand{
			t:            workerQueueTarget{name: name, q: q, worker: w},
			throughput:   tput,
			loadBytesSec: s.effectiveLoadThroughput(w),
		})
	}
	s.queueMu.RUnlock()

	if len(cands) == 0 {
		return nil, 0
	}

	fallbackLoadBytes := totalLoadBytes(env.Files)
	var best *workerQueueTarget
	var bestScore float64
	var bestQueuedSeconds float64
	for i := range cands {
		c := cands[i]
		tail := s.tailSeconds(c.t.name)
		inflight := s.getInflightSeconds(c.t.name)
		loadBytes := s.projectedLoadBytes(c.t.worker, env)
		if loadBytes <= 0 {
			loadBytes = fallbackLoadBytes
		}
		loadLat := loadLatencyForCand(c.t.worker, c.t.name, env, tail, loadBytes, c.loadBytesSec, s)
		// QueuedSeconds for this candidate = how long this task plus its
		// load (if any) will keep the worker busy.
		taskSec := env.Cost / c.throughput
		queuedSec := taskSec + loadLat
		score := inflight + tail + queuedSec
		if best == nil || score < bestScore {
			t := c.t
			best = &t
			bestScore = score
			bestQueuedSeconds = queuedSec
		}
	}
	return best, bestQueuedSeconds
}

// totalLoadBytes sums Files sizes. Returns 0 when there are no files
// (e.g. tests, or gateways that don't attach artifacts).
func totalLoadBytes(files []*workerpb.ModelFile) int64 {
	var total int64
	for _, f := range files {
		if f == nil {
			continue
		}
		if f.SizeBytes > 0 {
			total += f.SizeBytes
		}
	}
	return total
}

// loadLatencyForCand returns the wall-clock seconds the worker will spend
// loading env.ModelID before it can serve this envelope. Zero when the
// worker is already effectively-warm for the model.
//
// Effectively-warm rule:
//   - tail > 0: warm iff env.ModelID matches the queue's tail_model_id —
//     the model will be loaded anyway by the time this task is reached.
//     (Tail-model staleness vs. predicted placement isn't priced here;
//     by the time tail is reached, this task is at the head and the
//     dispatch-time gate will reconcile.)
//   - tail == 0: warm iff [residentsBlockingLoad] returns an empty set
//     AND the worker currently reports env.ModelID as a loaded model.
//     A stale-placement resident or an other-model conflict is *not*
//     warm — the dispatcher will pay a real evict-and-reload cost, so
//     the picker must too.
//
// loadBytesSec is the worker's effective host→device upload throughput
// in B/s (the bench column LoadGBs is GB/s; pickWorkerQueue multiplies
// by 1e9 before passing). Returns 0 when loadBytesSec is non-positive
// (no bench yet) so a missing throughput doesn't penalize the worker.
func loadLatencyForCand(w *worker.StreamWorker, queueName string, env queue.Envelope, tailSec float64, loadBytes int64, loadBytesSec float64, s *Scheduler) float64 {
	if env.ModelID == "" || loadBytes <= 0 {
		return 0
	}
	if tailSec > 0 {
		if s.tailModel(queueName) == env.ModelID {
			return 0
		}
	} else if workerHasModel(w, env.ModelID) && len(residentsBlockingLoad(w, env.ModelID, s.predictDeviceSet(w))) == 0 {
		return 0
	}
	if loadBytesSec <= 0 {
		return 0
	}
	return float64(loadBytes) / loadBytesSec
}

// tailSeconds returns the queued (not-yet-dispatched) wall-clock-seconds
// sum for queueName from the in-memory mirror. A missing entry reads 0 —
// the same fallback scoring used when the store row was absent.
func (s *Scheduler) tailSeconds(queueName string) float64 {
	s.tailMu.RLock()
	defer s.tailMu.RUnlock()
	return s.tails[queueName].seconds
}

// tailModel returns the model_id of the last task currently queued on
// queueName. Empty when the queue has never seen an enqueue, or when the
// entry was dropped on disconnect.
func (s *Scheduler) tailModel(queueName string) string {
	s.tailMu.RLock()
	defer s.tailMu.RUnlock()
	return s.tails[queueName].modelID
}

// getInflightSeconds returns the in-memory inflight wall-clock-seconds
// sum for queueName.
func (s *Scheduler) getInflightSeconds(queueName string) float64 {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	return s.inflightSeconds[queueName]
}

// debitTail subtracts delta (seconds) from the tail without touching
// tail_model_id — used on dispatch pop, where the head leaves the queue
// but the tail-of-queue is unchanged. Mirror first, then the store row
// best-effort; the clamp at 0 matches AddTailSeconds's SQL invariant.
func (s *Scheduler) debitTail(queueName string, delta float64) {
	s.tailMu.Lock()
	if t, ok := s.tails[queueName]; ok {
		t.seconds = max(0, t.seconds-delta)
		s.tails[queueName] = t
	}
	s.tailMu.Unlock()

	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return
	}
	if err := st.AddTailSeconds(queueName, -delta); err != nil {
		s.logger.Warn().Err(err).Str("queue", queueName).Float64("delta_seconds", delta).Msg("debiting tail")
	}
}

// startInflight atomically promotes requestID from "dispatching" to
// "inflight": it records the running seconds/reservation so concurrent
// scoring sees the worker's real load, transferring the dispatch marker
// under a single lock. seconds is the compute-only prediction and axis
// the throughput axis it divided by — see the dispatchEnvelope re-price
// and [inflightRecord]. modelID is stashed for the device-set gate so a
// subsequent dispatch can detect "MASS already assigned a job against this
// model" without waiting on a heartbeat. The worker-side jobID is filled
// in later by [Scheduler.attachWorkerJobID] once AssignJob returns.
//
// Returns false when a cancel landed during the dispatch window (the
// marker was flipped by [Scheduler.requestCancelDuringDispatch]): no
// inflight record is created and the caller must abort before AssignJob.
// Checking the flag in the same critical section that writes the record
// closes the race between the cancel and the promotion.
func (s *Scheduler) startInflight(queueName, requestID, modelID, runtimeName, axis string, seconds float64, reservedBytes int64) bool {
	workerID, _ := parseWorkerQueueName(queueName)
	// The factor effectiveThroughput applied to this prediction moments
	// ago in dispatchEnvelope — captured so observeThroughput can undo it
	// and record a bench-relative sample (see inflightRecord.correction).
	correction := s.correctionFactor(workerID, axis)
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if s.dispatchingByRequest[requestID] {
		return false // cancelled mid-dispatch; defer clearDispatching cleans the marker
	}
	delete(s.dispatchingByRequest, requestID)
	s.inflightSeconds[queueName] += seconds
	if reservedBytes > 0 && workerID != "" {
		s.memoryReservations[workerID] += reservedBytes
	}
	s.inflightByRequest[requestID] = inflightRecord{
		queueName:     queueName,
		seconds:       seconds,
		modelID:       modelID,
		workerID:      workerID,
		reservedBytes: reservedBytes,
		runtimeName:   runtimeName,
		axis:          axis,
		dispatchedAt:  time.Now(),
		correction:    correction,
	}
	return true
}

// workerHasInflightForModel reports whether MASS currently has an
// inflight record on workerID for modelID. Used as the device-set
// gate's activity signal because LoadedModelStatus.Active is
// heartbeat-driven and lags real-time dispatch by up to one
// heartbeat interval.
func (s *Scheduler) workerHasInflightForModel(workerID, modelID string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	for _, rec := range s.inflightByRequest {
		if rec.workerID == workerID && rec.modelID == modelID {
			return true
		}
	}
	return false
}

// inflightCountForWorker counts the jobs MASS currently has in flight on
// workerID. The worker's heartbeat AvailableCapacity lags real-time
// dispatch by up to one interval, so a burst could otherwise place more
// jobs than the pool holds before the count catches up; subtracting this
// from advertised capacity (see drainOneWorkerQueue) keeps a burst within
// pool_size immediately.
func (s *Scheduler) inflightCountForWorker(workerID string) int {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	n := 0
	for _, rec := range s.inflightByRequest {
		if rec.workerID == workerID {
			n++
		}
	}
	return n
}

// attachWorkerJobID fills in the worker-side jobID returned by AssignJob
// onto the existing inflight record for requestID. No-op when the record
// is gone (race with a fast terminal frame), which is fine — cancel
// would also be a no-op against a gone request.
func (s *Scheduler) attachWorkerJobID(requestID, workerJobID string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	rec, ok := s.inflightByRequest[requestID]
	if !ok {
		return
	}
	rec.workerJobID = workerJobID
	s.inflightByRequest[requestID] = rec
}

// markInflightCancelled flags the inflight record so the pump's terminal-
// error handler rewrites the result message to "cancelled by operator".
// Returns (queueName, workerJobID, ok) — ok=false when requestID is not
// inflight (already finished, never started, or cleared). Caller skips
// the wire CancelJob in that case.
func (s *Scheduler) markInflightCancelled(requestID string) (string, string, bool) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	rec, ok := s.inflightByRequest[requestID]
	if !ok {
		return "", "", false
	}
	rec.cancelledByOperator = true
	s.inflightByRequest[requestID] = rec
	return rec.workerID, rec.workerJobID, true
}

// isInflightCancelled reports whether requestID has been marked as
// operator-cancelled. Used by the pump to rewrite the result message.
// Returns false when the record is gone (post-terminal) — by then the
// flag's job is done and pump has already finalised.
func (s *Scheduler) isInflightCancelled(requestID string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	rec, ok := s.inflightByRequest[requestID]
	if !ok {
		return false
	}
	return rec.cancelledByOperator
}

// inflightRuntime returns the runtime name attached to requestID, or ""
// if no record exists. Used by terminal paths to label metric counters.
func (s *Scheduler) inflightRuntime(requestID string) string {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	rec, ok := s.inflightByRequest[requestID]
	if !ok {
		return ""
	}
	return rec.runtimeName
}

// observeThroughput feeds one completed job into the (worker|axis) EWMA so
// future scoring reflects how this worker actually performs versus its
// one-time bench. Reads the inflight record (worker, axis, predicted
// seconds, dispatch time) and folds predicted/actual into the running
// factor. Both sides of the ratio are compute-only: the record's
// seconds exclude load-switch latency and dispatchedAt is stamped
// after any LoadModel completed, so a cold load can't masquerade as
// compute speed. Call BEFORE finishInflight removes the record, and
// only on an ok terminal — error/cancel wall-clock isn't a throughput
// signal.
func (s *Scheduler) observeThroughput(requestID string) {
	s.inflightMu.Lock()
	rec, ok := s.inflightByRequest[requestID]
	s.inflightMu.Unlock()
	if !ok || rec.axis == "" || rec.workerID == "" || rec.seconds <= 0 {
		return
	}
	actual := time.Since(rec.dispatchedAt).Seconds()
	if actual < correctionMinActualSec {
		return // dominated by fixed overhead, not compute
	}
	rawRatio := rec.seconds / actual // >1: faster than predicted, <1: slower
	// The prediction already divided by the correction factor in force at
	// dispatch, so rawRatio measures the residual error of the CORRECTED
	// prediction. Multiply the factor back in to get a bench-relative
	// sample — the EWMA state is an absolute multiplier on benched
	// throughput, and feeding it corrected-prediction ratios would make
	// the fixed point sqrt(true ratio) instead of the true ratio.
	appliedCorrection := rec.correction
	if appliedCorrection <= 0 {
		appliedCorrection = 1
	}
	ratio := rawRatio * appliedCorrection
	clamped := false
	if ratio < 1/correctionClamp {
		ratio = 1 / correctionClamp
		clamped = true
	} else if ratio > correctionClamp {
		ratio = correctionClamp
		clamped = true
	}

	key := rec.workerID + "|" + rec.axis
	s.correctionMu.Lock()
	cur, seen := s.throughputCorrection[key]
	if !seen {
		cur = correctionState{factor: ratio, samples: 1}
	} else {
		cur.factor = (1-correctionAlpha)*cur.factor + correctionAlpha*ratio
		cur.samples++
	}
	s.throughputCorrection[key] = cur
	s.correctionMu.Unlock()

	// Persist the folded state so calibration survives a restart. Best-
	// effort: a failed write costs re-warming after the next restart, not
	// correctness now — the in-memory map already holds the sample.
	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st != nil {
		if err := st.UpsertThroughputCorrection(store.ThroughputCorrection{
			WorkerID: rec.workerID,
			Axis:     rec.axis,
			Factor:   cur.factor,
			Samples:  cur.samples,
		}); err != nil {
			s.logger.Warn().Err(err).Str("worker_id", rec.workerID).Str("axis", rec.axis).Msg("persisting throughput correction")
		}
	}

	// Calibration diagnostic: predicted vs actual wall-clock per job.
	// raw_ratio is relative to the corrected prediction (1.0 = the live
	// factor is dialled in); bench_ratio is the pre-clamp bench-relative
	// sample the EWMA folds — a bench_ratio pinned at correctionClamp
	// exposes a systematic bias the band is hiding. samples shows when
	// the factor starts applying (>= correctionMinSamples). model_id
	// isolates per-model error (e.g. vision jobs over-counted by the
	// projector estimate). Debug level: on when calibrating, quiet in
	// normal Info operation.
	s.logger.Debug().
		Str("worker_id", rec.workerID).
		Str("axis", rec.axis).
		Str("model_id", rec.modelID).
		Float64("predicted_sec", rec.seconds).
		Float64("actual_sec", actual).
		Float64("raw_ratio", rawRatio).
		Float64("bench_ratio", rawRatio*appliedCorrection).
		Bool("clamped", clamped).
		Float64("ewma_factor", cur.factor).
		Int("samples", cur.samples).
		Msg("throughput calibration sample")
}

// correctionFactor returns the live throughput multiplier for (workerID,
// axis), or 1.0 when fewer than correctionMinSamples jobs have completed
// (the bench prior stands alone until there's real evidence).
func (s *Scheduler) correctionFactor(workerID, axis string) float64 {
	if workerID == "" || axis == "" {
		return 1
	}
	s.correctionMu.Lock()
	defer s.correctionMu.Unlock()
	cur, ok := s.throughputCorrection[workerID+"|"+axis]
	if !ok || cur.samples < correctionMinSamples {
		return 1
	}
	return cur.factor
}

// restoreCorrections seeds the in-memory correction EWMA from rows
// persisted by earlier runs, so calibration survives a gateway restart
// instead of re-warming from the bench prior (correctionMinSamples jobs
// per key each run — a short queue never reopens the gate). Rows older
// than correctionMaxAge are ignored. Called once from [Scheduler.InitQueue],
// before the dispatcher starts.
func (s *Scheduler) restoreCorrections(st StateStoreInterface) {
	if st == nil {
		return
	}
	rows, err := st.ListThroughputCorrections()
	if err != nil {
		s.logger.Warn().Err(err).Msg("restoring throughput corrections")
		return
	}
	cutoff := time.Now().Add(-correctionMaxAge)
	restored := 0
	s.correctionMu.Lock()
	for _, row := range rows {
		if row.UpdatedAt.Before(cutoff) {
			continue
		}
		s.throughputCorrection[row.WorkerID+"|"+row.Axis] = correctionState{factor: row.Factor, samples: row.Samples}
		restored++
	}
	s.correctionMu.Unlock()
	if restored > 0 {
		s.logger.Info().Int("entries", restored).Msg("restored throughput corrections")
	}
}

// ResetCorrections drops every learned correction for workerID — in
// memory and persisted. Called when the baseline the factors are
// relative to changes: a fresh bench replaces the throughput prior, a
// device toggle changes the device set the prior sums across (see
// [Scheduler.throughputForAxis]). Stale evidence would mis-scale the
// new baseline until the EWMA re-converged, which is worse than
// re-warming from a correct prior.
func (s *Scheduler) ResetCorrections(workerID string) {
	if workerID == "" {
		return
	}
	prefix := workerID + "|"
	s.correctionMu.Lock()
	for key := range s.throughputCorrection {
		if strings.HasPrefix(key, prefix) {
			delete(s.throughputCorrection, key)
		}
	}
	s.correctionMu.Unlock()

	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return
	}
	if err := st.DeleteThroughputCorrections(workerID); err != nil {
		s.logger.Warn().Err(err).Str("worker_id", workerID).Msg("deleting persisted throughput corrections")
	}
}

// finishInflight removes requestID from the in-flight set. Safe to call
// multiple times — repeat calls after the first are no-ops.
//
// Float-dust epsilon: when many jobs with non-uniform seconds round-trip
// through credit/debit, the residual after subtracting the last record
// can be ~1e-15 instead of exactly zero. Treat anything within
// inflightFloatEpsilon as "fully drained" so the map clears for real.
func (s *Scheduler) finishInflight(requestID string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	rec, ok := s.inflightByRequest[requestID]
	if !ok {
		return
	}
	delete(s.inflightByRequest, requestID)
	if rec.reservedBytes > 0 && rec.workerID != "" {
		remaining := s.memoryReservations[rec.workerID] - rec.reservedBytes
		if remaining <= 0 {
			delete(s.memoryReservations, rec.workerID)
		} else {
			s.memoryReservations[rec.workerID] = remaining
		}
	}
	cur := s.inflightSeconds[rec.queueName]
	residual := cur - rec.seconds
	if residual <= inflightFloatEpsilon {
		delete(s.inflightSeconds, rec.queueName)
		return
	}
	s.inflightSeconds[rec.queueName] = residual
}

// markDispatching records that requestID has been leased and is being
// placed (model load + AssignJob) but is not yet inflight. Paired with
// [Scheduler.clearDispatching] via defer in [Scheduler.dispatchEnvelope].
func (s *Scheduler) markDispatching(requestID string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	s.dispatchingByRequest[requestID] = false
}

// clearDispatching ends the dispatching window for requestID.
func (s *Scheduler) clearDispatching(requestID string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	delete(s.dispatchingByRequest, requestID)
}

// requestCancelDuringDispatch records cancel intent against a job that is
// being placed but isn't yet inflight, returning true when requestID was
// in that window (so the caller treats the cancel as handled). The flag is
// consumed atomically by [Scheduler.startInflight], which aborts the
// promotion when it's set. Returns false when the job isn't dispatching —
// the caller falls through to the inflight cancel path.
func (s *Scheduler) requestCancelDuringDispatch(requestID string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if _, ok := s.dispatchingByRequest[requestID]; !ok {
		return false
	}
	s.dispatchingByRequest[requestID] = true
	return true
}

// inflightFloatEpsilon is the residual below which inflightSeconds is
// treated as fully drained. Calibrated for sums of ~30 non-uniform
// floats; well above observed drift (~1e-15) and well below any
// real-world per-job seconds value.
const inflightFloatEpsilon = 1e-9

// drainDeviceQueues starts one drain goroutine per device queue that
// isn't already draining. The drain must run off the dispatcher
// goroutine: dispatchEnvelope can block for minutes on a cold LoadModel,
// and one worker's load must not freeze dispatch to every other worker.
// One goroutine per queue at a time keeps per-worker FIFO order intact;
// a pass that finds a queue busy records the skip so the drainer kicks
// a follow-up pass when it exits.
func (s *Scheduler) drainDeviceQueues(ctx context.Context) {
	s.queueMu.RLock()
	type entry struct {
		name string
		q    queue.QueueInterface
	}
	queues := make([]entry, 0, len(s.devQueues))
	for name, q := range s.devQueues {
		queues = append(queues, entry{name, q})
	}
	s.queueMu.RUnlock()

	for _, e := range queues {
		if !s.tryMarkDraining(e.name) {
			continue
		}
		go func(name string, q queue.QueueInterface) {
			defer func() {
				if s.finishDraining(name) {
					s.kick()
				}
			}()
			s.drainOneWorkerQueue(ctx, name, q)
		}(e.name, e.q)
	}
}

// tryMarkDraining claims the drain slot for queue name. Returns false —
// and records that a re-check is wanted — when a drainer is already
// running there.
func (s *Scheduler) tryMarkDraining(name string) bool {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	if _, running := s.draining[name]; running {
		s.draining[name] = true // a pass was skipped; drainer will kick on exit
		return false
	}
	s.draining[name] = false
	return true
}

// finishDraining releases the drain slot for queue name, reporting
// whether a pass was skipped while the drainer ran (caller kicks the
// dispatcher so the queue is re-checked promptly).
func (s *Scheduler) finishDraining(name string) (rerun bool) {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	rerun = s.draining[name]
	delete(s.draining, name)
	return rerun
}

// isDraining reports whether a drain goroutine currently owns queue name.
func (s *Scheduler) isDraining(name string) bool {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	_, running := s.draining[name]
	return running
}

// drainOneWorkerQueue leases as many jobs from q as the worker can run
// in parallel right now (capped by AvailableCapacity) and dispatches
// each one. Returns silently when the queue is empty, the worker is
// gone, or the worker has no capacity. Pulling the full batch per pass
// — instead of one job per dispatcher tick — is what lets pool=N
// translate into actual N-way parallelism on the wire.
func (s *Scheduler) drainOneWorkerQueue(ctx context.Context, name string, q queue.QueueInterface) {
	workerID, ok := parseWorkerQueueName(name)
	if !ok {
		return
	}
	wIface := s.workers.Get(workerID)
	if wIface == nil {
		return // worker disconnected; OnWorkerDisconnected will drain us
	}
	sw, ok := wIface.(*worker.StreamWorker)
	if !ok || !sw.Status().Online {
		return
	}
	// Worker-wide capacity — the worker itself decides which of its
	// enabled devices each AssignJob lands on.
	//
	// AvailableCapacity comes from the worker's last heartbeat, which lags
	// real-time dispatch: a burst routed here across kick()-triggered passes
	// would otherwise reuse the same free-slot count and place more jobs than
	// the pool holds before the next heartbeat lands. Subtract the jobs MASS
	// already has in flight on this worker so the effective capacity reflects
	// reality now, not one heartbeat ago.
	//
	// The max(_, 1) floor only applies when nothing is in flight yet: a
	// worker with no model loaded reports 0 capacity, but we must still drain
	// one row so dispatchEnvelope can load-on-demand and create the pool.
	// Once jobs are in flight that floor would itself over-dispatch, so it's
	// gated on a zero inflight count.
	inflight := s.inflightCountForWorker(workerID)
	capacity := sw.AvailableCapacity() - inflight
	batch := capacity
	if inflight == 0 {
		batch = max(capacity, 1)
	}
	if batch <= 0 {
		return
	}

	msgs, err := q.Peek(ctx, batch)
	if err != nil {
		s.logger.Warn().Err(err).Str("queue", name).Msg("peeking worker queue")
		return
	}
	if len(msgs) == 0 {
		return
	}
	for _, msg := range msgs {
		s.leaseAndDispatch(ctx, sw, q, name, msg)
	}
}

// leaseAndDispatch is the per-message half of [drainOneDeviceQueue]:
// lease, check model affinity, hand off to [dispatchEnvelope]. Split
// out so the batch drain stays a tight loop.
func (s *Scheduler) leaseAndDispatch(ctx context.Context, sw *worker.StreamWorker, q queue.QueueInterface, name string, msg *queue.Message) {
	leased, err := q.LeaseByID(ctx, msg.ID, dispatchLeaseDuration)
	if err != nil {
		s.logger.Warn().Err(err).Str("queue", name).Str("message_id", string(msg.ID)).Msg("leasing device queue row")
		return
	}
	if leased == nil {
		return // race-loser
	}
	env, err := queue.UnmarshalEnvelope(leased.Body)
	if err != nil {
		s.logger.Warn().Err(err).Str("queue", name).Str("message_id", string(leased.ID)).Msg("unmarshalling device envelope")
		if delErr := q.Delete(ctx, leased.ID); delErr != nil {
			s.logger.Warn().Err(delErr).Str("queue", name).Str("message_id", string(leased.ID)).Msg("deleting unparseable envelope")
		}
		return
	}

	// Residency is NOT checked here: the dispatcher loads the model on
	// demand inside dispatchEnvelope using env.Files + env.LoadHints. A
	// worker that lost the model since this row was placed just pays the
	// load cost now.
	//
	// Bookkeeping (debitTail + startInflight + broadcast) happens inside
	// dispatchEnvelope once the device-set gate has admitted the job, so
	// a row that's bounced back for retry doesn't have to reverse it.
	s.dispatchEnvelope(sw, q, name, leased.ID, env)
}

// dispatchEnvelope loads env.ModelID on demand if needed, then calls
// AssignJob and hands the resulting chunk stream to [pumpWorkerChunks].
// The pump runs in its own goroutine so the dispatcher loop stays
// responsive. No ctx parameter — AssignJob/LoadModel don't take one
// (workers are streamed via their own conn lifecycle), and the cleanup
// paths use a fresh context so they survive a dispatcher shutdown
// mid-call.
//
// Device-set gate: [residentsBlockingLoad] returns every resident that
// must clear out before a fresh load on the predicted device set —
// other-model overlaps, and the target itself when its placement has
// gone stale (operator disabled one of its devices since load time).
// Active blockers release the lease so the next tick retries the same
// worker; idle blockers are evicted in place, then the cold-load
// proceeds. The same predicate also drives picker scoring and load-cost
// prediction so all three layers agree on what "warm" means.
func (s *Scheduler) dispatchEnvelope(sw *worker.StreamWorker, q queue.QueueInterface, queueName string, msgID queue.MessageID, env queue.Envelope) {
	s.jobsMu.Lock()
	buf, ok := s.jobBuffers[env.RequestID]
	s.jobsMu.Unlock()
	if !ok {
		// The submitter buffer is gone (post-terminal sweep raced ahead).
		// Mark the result failed and drop both queue rows so we don't keep
		// trying to place it.
		s.failResult(env.RequestID, "no replay buffer for request")
		s.cleanupRows(q, msgID, env.GlobalMsgID)
		return
	}

	// Keep both queue rows leased while this dispatch owns them (the
	// gate's evict round-trips, LoadModel, and streaming can each outlive
	// dispatchLeaseDuration). Stopped — synchronously — on every exit
	// path before the rows are released or deleted; the success path
	// hands the stop to pumpWorkerChunks.
	stopKeepAlive := s.startLeaseKeepAlive(q, msgID, env.GlobalMsgID, dispatchLeaseDuration)

	// Single device-set gate: walk every resident that blocks a fresh
	// load (target's own stale placement + other-model overlaps), bounce
	// while any is still serving traffic, evict the rest in place, then
	// load if the target isn't already warm on the current predicted set.
	// The same predicate drives picker scoring and load-cost prediction —
	// see [residentsBlockingLoad] — so picker, predictor, and dispatcher
	// agree on what "warm" means.
	predicted := s.predictDeviceSet(sw)
	blockers := residentsBlockingLoad(sw, env.ModelID, predicted)
	needsLoad := env.ModelID != "" && (!workerHasModel(sw, env.ModelID) || containsModelID(blockers, env.ModelID))

	var reservedBytes int64
	if needsLoad {
		// Reserve the projected post-grow memory until terminal frame so
		// the next heartbeat's stale used_memory doesn't admit a second
		// job that would push the worker over. Reserve the projected
		// pool-size, not just base: a concurrent cold-load to the same
		// worker would otherwise admit against slack that's about to be
		// consumed by pool growth.
		reservedBytes = s.projectedLoadBytes(sw, env)
		if reservedBytes <= 0 {
			reservedBytes = env.BaseLoadBytes
		}
	}

	if len(blockers) > 0 {
		// Activity is the union of two signals: the heartbeat-driven
		// Active count (truth for jobs MASS doesn't know about, e.g.
		// gateway-direct paths) and MASS's own inflight tracker (truth
		// for just-dispatched jobs that haven't been heartbeated back
		// yet). Without the inflight half, two submits in the same drain
		// pass both see Active=0 and silently co-locate.
		for _, lm := range blockers {
			if lm.Active > 0 || s.workerHasInflightForModel(sw.ID(), lm.ModelID) {
				s.logger.Debug().Str("worker", sw.ID()).Str("model_id", env.ModelID).Str("blocked_by", lm.ModelID).Int("blocked_active", lm.Active).Msg("resident blocks load; waiting for it to go idle")
				stopKeepAlive()
				s.releaseLeaseForRetry(q, msgID, env.RequestID)
				return
			}
		}
		// Every blocker is idle — evict before loading. Partial eviction
		// bounces for retry rather than loading on top of a half-cleared
		// device set.
		for _, lm := range blockers {
			n, err := s.EvictModel(sw.RuntimeName(), lm.ModelID, sw.ID())
			if err != nil || n == 0 {
				s.logger.Warn().Err(err).Str("worker", sw.ID()).Str("model_id", lm.ModelID).Int("evicted", n).Msg("evicting blocker before load")
				stopKeepAlive()
				s.releaseLeaseForRetry(q, msgID, env.RequestID)
				return
			}
			s.logger.Debug().Str("worker", sw.ID()).Str("evicted_model_id", lm.ModelID).Str("for_model_id", env.ModelID).Msg("evicted blocker to free device set")
		}
	}

	// Past the device-set gate — committed to placing this job. Mark it
	// dispatching so a cancel arriving during the (potentially seconds-long)
	// load can record intent instead of being lost: until startInflight runs
	// there's no inflight record and the global anchor is leased, so neither
	// the pending nor the running cancel path would otherwise see it.
	s.markDispatching(env.RequestID)
	defer s.clearDispatching(env.RequestID)

	if needsLoad {
		res, err := sw.LoadModel(worker.LoadModelRequest{
			ModelID:   env.ModelID,
			Files:     env.Files,
			LoadHints: env.LoadHints,
			Source:    env.Source,
		})
		if err != nil {
			s.logger.Warn().Err(err).Str("worker", sw.ID()).Str("model_id", env.ModelID).Uint8("attempt", env.Attempts+1).Msg("load-on-demand at dispatch")
			stopKeepAlive()
			if s.retryAfterLoadFailure(q, msgID, env) {
				return
			}
			s.failResult(env.RequestID, err.Error())
			buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: err.Error()})
			s.cleanupRows(q, msgID, env.GlobalMsgID)
			return
		}
		s.workers.NotifyLoadedChanged(sw.ID())
		s.logger.Debug().Str("worker", sw.ID()).Str("model_id", env.ModelID).Int32("pool_size", res.PoolSize).Msg("model loaded on demand")
	}

	// Gate cleared and (if needed) model loaded. Cross from "queued" to
	// "in flight": tail shrinks by the envelope's queued-seconds, the
	// in-flight tracker grows by the compute-only prediction. Both happen
	// before AssignJob so a concurrent pickWorkerQueue call scores the
	// next envelope against the updated load.
	//
	// The inflight seconds are re-priced here rather than taken from
	// env.QueuedSeconds: the queued figure also carries the load-switch
	// latency priced at placement, but any load has already completed by
	// this point (the inflight clock starts after LoadModel), so keeping
	// it would (a) overstate the worker's remaining busy-time in scoring
	// and (b) teach the correction EWMA that cold-loading workers beat
	// their bench — predicted included load seconds the measured
	// wall-clock never sees. The axis recorded is the one the throughput
	// lookup actually used (post-fallback) so the correction sample lands
	// on the key scoring reads. taskSec 0 (unbenched axis mid-toggle)
	// disables calibration for this job — observeThroughput skips
	// non-positive predictions.
	//
	// startInflight returns false when a cancel landed during the load
	// window: honour it here, before the job ever reaches the worker —
	// finalize as cancelled and drop both rows, mirroring the terminal-
	// cancel path, rather than dispatching work the operator abandoned.
	taskSec := 0.0
	usedAxis := env.CostAxis
	if tput, axis, ok := s.effectiveThroughput(sw, env.CostAxis, s.runtimeDefaultAxis(env.RuntimeName)); ok && tput > 0 {
		taskSec = env.Cost / tput
		usedAxis = axis
	}
	if !s.startInflight(queueName, env.RequestID, env.ModelID, env.RuntimeName, usedAxis, taskSec, reservedBytes) {
		s.logger.Info().Str("worker", sw.ID()).Str("request_id", env.RequestID).Msg("cancel landed during dispatch; aborting before assign")
		metrics.JobDispatched(env.RuntimeName, "cancelled")
		s.failResult(env.RequestID, "cancelled by operator")
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: "cancelled by operator"})
		stopKeepAlive()
		s.cleanupRows(q, msgID, env.GlobalMsgID)
		return
	}
	// The job is in flight for real now — surface it to async pollers.
	s.processingResult(env.RequestID)
	s.debitTail(queueName, env.QueuedSeconds)
	s.broadcastQueueChange()

	workerJobID, workerCh, err := sw.AssignJob(env.ModelID, env.Payload)
	if err != nil {
		s.logger.Warn().Err(err).Str("worker", sw.ID()).Str("model_id", env.ModelID).Msg("assigning job to worker")
		metrics.JobDispatched(env.RuntimeName, "error")
		s.failResult(env.RequestID, err.Error())
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: err.Error()})
		s.finishInflight(env.RequestID)
		stopKeepAlive()
		s.cleanupRows(q, msgID, env.GlobalMsgID)
		return
	}
	// Capture worker-side jobID so [Scheduler.CancelRunningJob] can
	// address this in-flight job via HubCancelJob. Done after AssignJob
	// returns to avoid stashing an uninitialised value.
	s.attachWorkerJobID(env.RequestID, workerJobID)

	go s.pumpWorkerChunks(sw, q, msgID, env.GlobalMsgID, env.RequestID, buf, workerCh, stopKeepAlive)
}

// startLeaseKeepAlive launches a goroutine that re-extends the
// worker-queue row's lease — and, when globalMsgID is set, the global
// durability anchor's — every leaseDur/3 for as long as the dispatch
// owns the rows. Nothing else extends these leases: without the
// keep-alive a cold LoadModel or a long prompt-processing gap would
// outlive the initial window and expose a running job's rows to
// re-dispatch and stealing.
//
// The tick is jittered ±10% per job: every in-flight job runs its own
// keep-alive, and unjittered they pile their Extends into the same instant
// against a MaxOpenConns(1) SQLite handle. Still far inside the lease
// window, which is what the interval has to respect.
//
// The returned stop function is SYNCHRONOUS and idempotent: it cancels
// the goroutine and waits for it to exit, so a caller about to release
// or delete the rows knows no further Extend can fire afterwards — an
// Extend racing a ReleaseLease would re-hide the released row for up to
// a full lease window. Extend failures are logged at debug and retried
// next tick: the row may legitimately be gone already (terminal race).
func (s *Scheduler) startLeaseKeepAlive(q queue.QueueInterface, msgID queue.MessageID, globalMsgID string, leaseDur time.Duration) (stop func()) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()

	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		ticker := time.NewTicker(jitter.Duration(leaseDur/3, 0.1))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			ctx := context.Background()
			if err := q.Extend(ctx, msgID, leaseDur); err != nil {
				s.logger.Debug().Err(err).Str("message_id", string(msgID)).Msg("extending worker row lease")
			}
			if globalQ != nil && globalMsgID != "" {
				if err := globalQ.Extend(ctx, queue.MessageID(globalMsgID), leaseDur); err != nil {
					s.logger.Debug().Err(err).Str("global_msg_id", globalMsgID).Msg("extending global anchor lease")
				}
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-exited
		})
	}
}

// predictDeviceSet returns the device IDs a fresh LoadModel call on w
// would occupy. Mirrors the C++ worker's allowed_load_devices() rule:
// every enabled GPU when any GPU is enabled, otherwise the CPU. The
// returned slice is sorted for stable comparison.
func (s *Scheduler) predictDeviceSet(w *worker.StreamWorker) []string {
	wID := w.ID()
	var gpus, cpus []string
	for _, dev := range w.Devices() {
		if !s.isDeviceEnabled(wID, dev.ID) {
			continue
		}
		switch dev.Type {
		case stats.DeviceTypeGPU:
			gpus = append(gpus, dev.ID)
		case stats.DeviceTypeCPU:
			cpus = append(cpus, dev.ID)
		}
	}
	if len(gpus) > 0 {
		sort.Strings(gpus)
		return gpus
	}
	sort.Strings(cpus)
	return cpus
}

// releaseLeaseForRetry bounces msgID back onto this worker's queue,
// visible again after pendingRetryInterval. Used when the device-set gate
// decides this worker can't take env right now — the row stays at the head
// of the queue and is re-tried once the overlapping resident goes idle.
//
// Deliberately uncapped and free: a legitimately busy blocker must not
// fail the job, so nothing is charged against Envelope.Attempts. What the
// delay buys is the pacing — a released row plus a wake-up made
// drain -> lease -> bounce -> kick cycle as fast as SQLite could serve it
// for as long as the blocker stayed active. One deferred row, one delayed
// wake-up: [queue.QueueInterface.Defer] deliberately signals nothing.
func (s *Scheduler) releaseLeaseForRetry(q queue.QueueInterface, msgID queue.MessageID, requestID string) {
	if err := q.Defer(context.Background(), msgID, pendingRetryInterval); err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Str("message_id", string(msgID)).Msg("deferring lease for retry")
	}
	time.AfterFunc(pendingRetryInterval, s.kick)
}

// retryAfterLoadFailure re-places env on the global queue with an
// incremented Attempts counter so the next dispatcher tick can pick a
// different worker. Returns true when the retry was scheduled (caller
// should return without failing the result); false when the attempt
// budget is exhausted or the queue subsystem is unavailable (caller
// falls through to the terminal-failure path).
//
// The worker queue row and its prior global anchor are deleted; a
// fresh envelope is submitted to global, getting a new GlobalMsgID.
// Result entry is left in "pending" state — the next dispatch will
// either complete it or, if it too fails and the budget is exhausted,
// the caller writes the failure.
func (s *Scheduler) retryAfterLoadFailure(q queue.QueueInterface, msgID queue.MessageID, env queue.Envelope) bool {
	maxAttempts := s.cfg.EffectiveLoadAttempts()
	if int(env.Attempts)+1 >= maxAttempts {
		return false
	}
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	if globalQ == nil {
		return false
	}
	ctx := context.Background()
	if env.GlobalMsgID != "" {
		if err := q.DeleteBoth(ctx, msgID, globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
			s.logger.Warn().Err(err).Str("request_id", env.RequestID).Msg("deleting old rows on load-failure retry")
			return false
		}
	} else {
		if err := q.Delete(ctx, msgID); err != nil {
			s.logger.Warn().Err(err).Str("request_id", env.RequestID).Msg("deleting worker row on load-failure retry")
			return false
		}
	}
	env.Attempts++
	env.GlobalMsgID = ""
	env.QueuedSeconds = 0
	res, err := globalQ.Submit(ctx, env)
	if err != nil {
		s.logger.Warn().Err(err).Str("request_id", env.RequestID).Msg("resubmitting envelope on load-failure retry")
		return false
	}
	s.logger.Info().Str("request_id", env.RequestID).Uint8("attempt", env.Attempts).Int("max_attempts", maxAttempts).Str("new_global_msg_id", res.ID).Msg("load failed; requeued for retry on a different worker")
	s.kick()
	s.broadcastQueueChange()
	return true
}

// cleanupRows deletes the worker queue row and (when present) the global
// queue anchor that pairs with it. Used on every pre-terminal failure
// path: orphan request, load failure, AssignJob failure. Best-effort —
// errors are logged but don't block the failure being recorded.
func (s *Scheduler) cleanupRows(q queue.QueueInterface, workerMsgID queue.MessageID, globalMsgID string) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	ctx := context.Background()
	if globalQ != nil && globalMsgID != "" {
		if err := q.DeleteBoth(ctx, workerMsgID, globalQ, queue.MessageID(globalMsgID)); err != nil {
			s.logger.Warn().Err(err).Str("worker_msg_id", string(workerMsgID)).Str("global_msg_id", globalMsgID).Msg("deleting both queue rows on failure")
		}
		s.broadcastQueueChange()
		return
	}
	if err := q.Delete(ctx, workerMsgID); err != nil {
		s.logger.Warn().Err(err).Str("worker_msg_id", string(workerMsgID)).Msg("deleting worker queue row on failure")
	}
	s.broadcastQueueChange()
}

// pumpWorkerChunks appends every chunk from workerCh into the per-job
// ring buffer. The dispatch's lease keep-alive (stopKeepAlive) keeps
// both queue rows leased while the stream runs; the pump stops it —
// synchronously — the moment the stream ends, before any row is deleted
// or handed over. On terminal frame the worker row and the global row
// are deleted atomically.
//
// The terminal frame is held back until the durable result is stored
// (the [jobBuffer] producer invariant): consumers unblock on it — the
// gateway's wait-for-result drain — and immediately read the result, so
// publishing it first would let them read a still-processing row.
//
// Disconnect handling: when the channel closes without a terminal frame
// AND the worker is offline, we treat the job as "to be redistributed"
// — the OnWorkerDisconnected reap will release the global lease so
// drainGlobal can re-place it. The result entry stays pending. In all
// other no-terminal cases (channel closed but worker still online,
// unexpected shutdown) we record a failure result, since there's no
// expected redistribution path.
//
// The buffer itself is retained for the configured replay TTL so a
// disconnected gateway can reconnect and resume.
func (s *Scheduler) pumpWorkerChunks(sw *worker.StreamWorker, q queue.QueueInterface, msgID queue.MessageID, globalMsgID, requestID string, buf *jobBuffer, workerCh <-chan *worker.JobChunk, stopKeepAlive func()) {
	var finalBody []byte
	var errText string
	var terminalChunk *worker.JobChunk
	for chunk := range workerCh {
		switch chunk.Type {
		case worker.JobChunkTypeCompleted:
			finalBody = chunk.Final
			terminalChunk = chunk
		case worker.JobChunkTypeError:
			errText = chunk.ErrText
			terminalChunk = chunk
		default:
			buf.Append(chunk)
		}
	}
	terminal := terminalChunk != nil
	// Stream over — every path below deletes the rows or hands them to the
	// disconnect drain, so the keep-alive must be provably stopped first
	// (a late Extend would re-hide a row a concurrent release just freed).
	stopKeepAlive()

	// Use a fresh context: the pump goroutine outlives the dispatcher
	// pass that spawned it, and we must finalize even if the dispatcher's
	// ctx has been cancelled (e.g. on shutdown).
	ctx := context.Background()
	// Read cancel flag + runtime BEFORE finishInflight removes the
	// record. Cancel flag and the worker-side error frame are racy by
	// construction (operator hit cancel before the worker noticed and
	// emitted terminal); runtime label is for metrics.
	wasCancelled := s.isInflightCancelled(requestID)
	runtimeName := s.inflightRuntime(requestID)
	// Feed the throughput correction loop on clean completions only —
	// before finishInflight removes the record we read from.
	if terminal && errText == "" && !wasCancelled {
		s.observeThroughput(requestID)
	}
	s.finishInflight(requestID)

	if terminal {
		if errText != "" {
			if wasCancelled {
				// Rewrite the worker's raw error ("Chat: cancelled by
				// operator" or whatever the worker chose to put on the
				// wire) so the gateway sees a stable, operator-facing
				// reason string regardless of which runtime is involved.
				errText = "cancelled by operator"
				terminalChunk.ErrText = errText
				metrics.JobDispatched(runtimeName, "cancelled")
			} else {
				metrics.JobDispatched(runtimeName, "error")
			}
			s.failResult(requestID, errText)
		} else {
			metrics.JobDispatched(runtimeName, "ok")
			s.completeResult(requestID, finalBody)
		}
		buf.Append(terminalChunk)
		s.deleteBothRows(ctx, q, msgID, globalMsgID, requestID)
		return
	}

	// A cancelled job whose stream closed without a terminal frame (e.g. the
	// worker crashed before observing the cancel) must NOT be redistributed —
	// the operator asked for it to stop. Finalize it as cancelled and drop both
	// rows, just like the terminal-cancel path.
	if wasCancelled {
		s.logger.Info().Str("worker", sw.ID()).Str("request_id", requestID).Msg("cancelled job lost its worker; finalizing as cancelled")
		metrics.JobDispatched(runtimeName, "cancelled")
		s.failResult(requestID, "cancelled by operator")
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: "cancelled by operator"})
		s.deleteBothRows(ctx, q, msgID, globalMsgID, requestID)
		return
	}

	// Non-terminal channel close. If the worker went offline, leave the
	// global row alone — OnWorkerDisconnected reaps the worker queue and
	// releases the global lease, drainGlobal will re-score on another
	// worker. The result's processing→pending revert belongs to that drain
	// too (it owns the release), not here.
	if !sw.Status().Online {
		s.logger.Info().Str("worker", sw.ID()).Str("request_id", requestID).Msg("worker disconnected mid-job; awaiting redistribution")
		return
	}
	// Worker is still online but stream closed without terminal — record
	// failure and clean up.
	errText = "worker stream closed before terminal frame"
	metrics.JobDispatched(runtimeName, "error")
	s.failResult(requestID, errText)
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeError, ErrText: errText})
	s.deleteBothRows(ctx, q, msgID, globalMsgID, requestID)
}

// deleteBothRows clears the worker queue row + the global anchor (when
// present) in one transaction.
func (s *Scheduler) deleteBothRows(ctx context.Context, q queue.QueueInterface, msgID queue.MessageID, globalMsgID, requestID string) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	if globalQ != nil && globalMsgID != "" {
		if err := q.DeleteBoth(ctx, msgID, globalQ, queue.MessageID(globalMsgID)); err != nil {
			s.logger.Warn().Err(err).Str("request_id", requestID).Msg("deleting both queue rows on terminal")
		}
		s.broadcastQueueChange()
		return
	}
	if err := q.Delete(ctx, msgID); err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("deleting worker queue row on terminal")
	}
	s.broadcastQueueChange()
}

// attemptSteals lets idle workers pull queued jobs from busy peers. Only
// runs when the runtime has multiple workers; otherwise it's a no-op.
func (s *Scheduler) attemptSteals(ctx context.Context) {
	type byRuntime struct {
		queues []workerQueueTarget
	}
	runtimes := map[string]*byRuntime{}

	s.queueMu.RLock()
	allWorkers := s.workers.All()
	for _, wi := range allWorkers {
		sw, ok := wi.(*worker.StreamWorker)
		if !ok || !sw.Status().Online {
			continue
		}
		if !s.isWorkerEnabled(sw.ID()) {
			continue
		}
		name := workerQueueName(sw.ID())
		q, ok := s.devQueues[name]
		if !ok {
			continue // worker not yet schedulable (e.g. awaiting bench)
		}
		rt := sw.RuntimeName()
		entry, exists := runtimes[rt]
		if !exists {
			entry = &byRuntime{}
			runtimes[rt] = entry
		}
		entry.queues = append(entry.queues, workerQueueTarget{name: name, q: q, worker: sw})
	}
	s.queueMu.RUnlock()

	for _, group := range runtimes {
		if len(group.queues) < 2 {
			continue
		}
		s.stealWithinRuntime(ctx, group.queues)
	}
}

// stealWithinRuntime picks an idle queue and tries to move one row from
// the deepest peer queue when the stealing worker can serve it.
func (s *Scheduler) stealWithinRuntime(ctx context.Context, queues []workerQueueTarget) {
	type stat struct {
		t     workerQueueTarget
		depth int
	}
	stats := make([]stat, 0, len(queues))
	for _, t := range queues {
		depth, err := t.q.Depth(ctx)
		if err != nil {
			continue
		}
		stats = append(stats, stat{t, depth})
	}
	if len(stats) < 2 {
		return
	}

	for _, idle := range stats {
		if idle.depth > 0 {
			continue
		}
		if idle.t.worker.AvailableCapacity() <= 0 {
			continue
		}
		// Find the deepest peer whose head row this idle worker can serve.
		var best *stat
		for i := range stats {
			peer := stats[i]
			if peer.t.worker.ID() == idle.t.worker.ID() {
				continue
			}
			if peer.depth <= stealThreshold {
				continue
			}
			// A draining peer's head rows are being leased concurrently by
			// its drain goroutine; Peek+MoveTo here would race that lease
			// and move a row that is already being dispatched. Steals only
			// run on the dispatcher goroutine, so no new drain can start
			// mid-steal — skipping marked peers closes the window.
			if s.isDraining(peer.t.name) {
				continue
			}
			if best == nil || peer.depth > best.depth {
				p := peer
				best = &p
			}
		}
		if best == nil {
			continue
		}
		msgs, err := best.t.q.Peek(ctx, 1)
		if err != nil || len(msgs) == 0 {
			continue
		}
		env, err := queue.UnmarshalEnvelope(msgs[0].Body)
		if err != nil {
			continue
		}
		// The stealing worker must actually be able to serve the row:
		// fetchable files (URL-less artifacts need a loopback worker),
		// memory fit, and a benched throughput axis. Depth + capacity
		// alone would steal onto a worker whose dispatch is guaranteed
		// to fail, burning the envelope's load-attempt budget. Same
		// generic predicates the picker scores with — nothing
		// runtime-specific.
		if !s.eligibleWorker(idle.t.worker, env) {
			continue
		}
		if _, _, ok := s.effectiveThroughput(idle.t.worker, env.CostAxis, s.runtimeDefaultAxis(env.RuntimeName)); !ok {
			continue
		}
		// Residency is no longer a hard gate — if the idle worker doesn't
		// have env.ModelID resident, dispatchEnvelope will load it from
		// env.Files at pop time. Stealing still trades a model-switch
		// load cost for a worker idle window, which is the bargain that
		// makes work stealing useful in the first place.
		moved, err := best.t.q.MoveTo(ctx, idle.t.q, msgs[0].ID, env.Priority)
		if err != nil {
			s.logger.Warn().Err(err).Str("from", best.t.name).Str("to", idle.t.name).Msg("work stealing")
			continue
		}
		if moved {
			s.debitTail(best.t.name, env.QueuedSeconds)
			s.creditTail(idle.t.name, env.QueuedSeconds, env.ModelID)
			s.logger.Debug().Str("from", best.t.name).Str("to", idle.t.name).Str("model_id", env.ModelID).Msg("stole queued job")
		}
	}
}

// --- Result store helpers ---

func (s *Scheduler) completeResult(requestID string, body []byte) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return
	}
	if err := results.Complete(requestID, body); err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("storing completed result")
	}
}

// processingResult marks requestID's result row as processing — called the
// moment a job truly goes in flight (after startInflight, so a cold load
// still reads as pending). Best-effort: a status write must not fail the
// dispatch.
func (s *Scheduler) processingResult(requestID string) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return
	}
	if err := results.Processing(requestID); err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("marking result processing")
	}
}

// pendingResult reverts requestID's result row from processing back to
// pending — the in-flight job lost its worker and awaits redistribution.
// Best-effort, and guarded store-side so a racing terminal write wins.
func (s *Scheduler) pendingResult(requestID string) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return
	}
	if err := results.Pending(requestID); err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("reverting result to pending")
	}
}

// resultIsTerminal reports whether requestID's result already carries a
// terminal status (done or error). Missing rows and store errors read as
// non-terminal so the caller falls back to its normal (requeue) path.
func (s *Scheduler) resultIsTerminal(requestID string) bool {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return false
	}
	r, err := results.Get(requestID)
	if err != nil {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("checking result status on drain")
		return false
	}
	return r != nil && (r.Status == queue.ResultStatusDone || r.Status == queue.ResultStatusError)
}

func (s *Scheduler) failResult(requestID, errText string) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return
	}
	if err := results.Fail(requestID, errText); err != nil && !errors.Is(err, sql.ErrNoRows) {
		s.logger.Warn().Err(err).Str("request_id", requestID).Msg("storing failed result")
	}
}

// --- Internal selection ---

func workerHasModel(w *worker.StreamWorker, modelID string) bool {
	if modelID == "" {
		return true
	}
	for _, lm := range w.LoadedModels() {
		if lm.ModelID == modelID {
			return true
		}
	}
	return false
}

// containsModelID reports whether any element of lms has the given
// ModelID. Used to detect "target is among the blockers" — i.e. the
// stale-self case — so the dispatcher knows to LoadModel after evicting.
func containsModelID(lms []worker.LoadedModelStatus, modelID string) bool {
	for _, lm := range lms {
		if lm.ModelID == modelID {
			return true
		}
	}
	return false
}

// residentsBlockingLoad returns every loaded model on w that would have
// to be evicted before a fresh LoadModel(targetModelID) onto predicted
// could proceed. Two flavours, both surfaced together so picker,
// predictor, and dispatcher reason from the same predicate:
//
//   - Same-model stale: target is resident but its DeviceIDs are not
//     fully inside predicted (operator disabled a device since load).
//     A reload would honour the new whitelist; the stale copy is in
//     the way.
//   - Other-model overlap: a different model's DeviceIDs intersect
//     predicted. llama.cpp can't co-locate two models on the same GPUs,
//     so the other model has to clear out.
//
// Empty DeviceIDs is treated asymmetrically: the *target's* empty set
// is "placement unknown, leave it alone" (pre-upgrade worker — refusing
// to churn is the safe call); a different model's empty set is
// "conservatively overlapping" for the same reason it's been that way
// since the original gate. Empty predicted returns nil — nothing to
// load anywhere.
func residentsBlockingLoad(w *worker.StreamWorker, targetModelID string, predicted []string) []worker.LoadedModelStatus {
	if len(predicted) == 0 {
		return nil
	}
	pred := make(map[string]struct{}, len(predicted))
	for _, id := range predicted {
		pred[id] = struct{}{}
	}
	var out []worker.LoadedModelStatus
	for _, lm := range w.LoadedModels() {
		if lm.ModelID == targetModelID {
			if len(lm.DeviceIDs) == 0 {
				continue // unknown placement → don't churn
			}
			stale := false
			for _, devID := range lm.DeviceIDs {
				if _, ok := pred[devID]; !ok {
					stale = true
					break
				}
			}
			if stale {
				out = append(out, lm)
			}
			continue
		}
		if len(lm.DeviceIDs) == 0 {
			out = append(out, lm)
			continue
		}
		for _, devID := range lm.DeviceIDs {
			if _, ok := pred[devID]; ok {
				out = append(out, lm)
				break
			}
		}
	}
	return out
}

// --- Scoring throughput ---

// effectiveThroughput returns the worker's realised throughput on the
// requested axis and the axis name actually used (== axis on exact
// match, == defaultAxis on fallback). Returns (0, "", false) when
// neither the requested axis nor the fallback is benched on any of the
// worker's enabled devices.
//
// Lookup order: try axis exact; if no device advertises it, try
// defaultAxis (the runtime's gateway-declared required axis). The
// fallback lets MASS still place jobs on workers that haven't been
// upgraded to bench every axis a gateway might request. Callers that
// record predictions must key them by usedAxis — the correction EWMA
// is folded in here per used axis, so a sample filed under the
// requested-but-unbenched axis would never be read back.
//
// Within an axis, throughput sums across the device set the worker will
// use for incoming work — see [Scheduler.deviceSet].
func (s *Scheduler) effectiveThroughput(w *worker.StreamWorker, axis, defaultAxis string) (val float64, usedAxis string, ok bool) {
	if axis != "" {
		if v := s.throughputForAxis(w, axis); v > 0 {
			return v * s.correctionFactor(w.ID(), axis), axis, true
		}
	}
	if defaultAxis != "" && defaultAxis != axis {
		if v := s.throughputForAxis(w, defaultAxis); v > 0 {
			return v * s.correctionFactor(w.ID(), defaultAxis), defaultAxis, true
		}
	}
	return 0, "", false
}

// throughputForAxis predicts the worker's compute throughput on axis
// across the device set it would use for incoming work.
//
// Model: llama.cpp's tensor-split assigns each layer's matmul slice to
// every participating device, then synchronises before the next layer.
// Wall-clock per layer is gated by the slowest device, so N devices
// deliver N × min(rates), not Σ rates. Homogeneous pairs collapse to
// "sum" cleanly (N × min == Σ); heterogeneous pairs honestly reflect
// the slowest-link gating that an operator observes when they enable
// a weak GPU and see throughput drop.
//
// An enabled-but-unbenched device is treated as "not yet measurable"
// and skipped — including it as 0 would zero the entire worker until
// the next bench cycle. The count (N) only includes benched devices,
// so the result is N_benched × min(benched_rates).
//
// Returns 0 when no device in the predicted set has a positive number
// on axis (unschedulable; eligibility gate surfaces it).
func (s *Scheduler) throughputForAxis(w *worker.StreamWorker, axis string) float64 {
	wID := w.ID()
	var (
		minRate  float64
		nBenched int
	)
	for _, devID := range s.deviceSet(w) {
		row, ok := s.getBenchmark(wID, devID)
		if !ok {
			continue
		}
		t := row.Throughput[axis]
		if t <= 0 {
			continue
		}
		if nBenched == 0 || t < minRate {
			minRate = t
		}
		nBenched++
	}
	if nBenched == 0 {
		return 0
	}
	return float64(nBenched) * minRate
}

// effectiveLoadThroughput returns the host→device upload bandwidth in
// bytes/sec we expect w to deliver when loading a model. This is the
// leg that dominates wall-clock cost during a model switch (disk →
// mmap → PCIe upload); STREAM memory bandwidth (MemoryGBs) describes
// device-local reads and over-estimates this by ~100× on discrete GPUs,
// which is why the scheduler reads LoadGBs specifically here.
//
// Sums LoadGBs across the device set the worker would use for incoming
// work — every device pulls its slice of weights over its own bus in
// parallel, so total upload throughput is additive across the placement
// set.
//
// Returns 0 when no device in the predicted set has a positive load_gbs.
// loadLatencyForCand treats that as "no penalty" so a missing bench
// number doesn't artificially block placement.
func (s *Scheduler) effectiveLoadThroughput(w *worker.StreamWorker) float64 {
	const gbToBytes = 1e9
	wID := w.ID()
	var sum float64
	for _, devID := range s.deviceSet(w) {
		row, ok := s.getBenchmark(wID, devID)
		if !ok || row.LoadGBs <= 0 {
			continue
		}
		sum += row.LoadGBs
	}
	return sum * gbToBytes
}

// deviceSet returns the canonical device IDs the worker would use for
// any incoming job: enabled GPUs, or a single enabled CPU when no GPU
// is enabled. Mirrors the C++ worker's allowed_load_devices() so the
// prediction matches what the worker will actually use at load time.
//
// Model residency is intentionally NOT consulted here: load-time is a
// separate component of the placement score (load_latency, gated by
// workerHasModel inside loadLatencyForCand). Compute throughput must
// reflect the operator's current enable mask, so a device toggled off
// stops contributing to predictions immediately rather than waiting
// for the next cold load.
//
// Returns nil when the worker has no usable devices (offline, not yet
// registered, or every device disabled); callers treat that as "no
// candidate."
func (s *Scheduler) deviceSet(w *worker.StreamWorker) []string {
	wID := w.ID()
	devs := w.Devices()
	if len(devs) == 0 {
		return nil
	}
	var gpus []string
	for _, dev := range devs {
		if dev.Type == stats.DeviceTypeGPU && s.isDeviceEnabled(wID, dev.ID) {
			gpus = append(gpus, dev.ID)
		}
	}
	if len(gpus) > 0 {
		return gpus
	}
	for _, dev := range devs {
		if dev.Type == stats.DeviceTypeCPU && s.isDeviceEnabled(wID, dev.ID) {
			return []string{dev.ID}
		}
	}
	return nil
}

// eligibleWorker reports whether w can host env. Three predicates today:
// at least one usable device, file reachability (URL-less load artifacts
// require a loopback worker), and (when env.BaseLoadBytes > 0) enough
// free memory across the predicted device set to fit the load.
// Composable shape so future filters (capability checks, etc.) slot
// in here.
func (s *Scheduler) eligibleWorker(w *worker.StreamWorker, env queue.Envelope) bool {
	if len(s.deviceSet(w)) == 0 {
		return false
	}
	if filesRequireLoopback(env.Files) && !w.IsLoopback() {
		s.logger.Debug().Str("worker_id", w.ID()).Str("model_id", env.ModelID).
			Msg("excluding non-loopback worker: envelope carries URL-less load files it cannot fetch")
		return false
	}
	return s.memoryEligible(w, env)
}

// filesRequireLoopback reports whether files contains a load artifact only
// a loopback worker can access: a ModelFile without a URL is shipped by
// host-local path (LocalPath), which a remote worker cannot reach.
func filesRequireLoopback(files []*workerpb.ModelFile) bool {
	for _, f := range files {
		if f == nil {
			continue
		}
		if f.GetUrl() == "" {
			return true
		}
	}
	return false
}

// memoryEligible reports whether w has enough free memory across its
// predicted device set to fit env's load. Returns true when:
//
//   - env.BaseLoadBytes is 0 (gateway couldn't estimate — fall back
//     to pay-on-failure for that submit).
//   - The model is already resident on w (no new load → no new
//     memory pressure).
//   - Sum of free bytes across the device set ≥ BaseLoadBytes. We
//     gate on the minimum (base, i.e. pool=1) rather than the
//     projected total so a tight-fitting model still gets a chance
//     to load: the projection caps the pool at whatever fits.
//
// "Free" subtracts both heartbeat-reported used memory and MASS's
// in-flight reservation ledger so two concurrent cold loads to the
// same worker don't both pass the gate before the first one's
// memory shows up in stats.
func (s *Scheduler) memoryEligible(w *worker.StreamWorker, env queue.Envelope) bool {
	if env.BaseLoadBytes <= 0 {
		return true
	}
	if env.ModelID != "" && workerHasModel(w, env.ModelID) {
		return true
	}
	return s.freeMemoryBytes(w) >= env.BaseLoadBytes
}

// feasibleByAnyWorker reports whether any online worker for env's
// runtime has total hardware memory ≥ env.BaseLoadBytes across its
// default device set. Submit calls this once before persisting so the
// operator gets fast feedback when the fleet is fundamentally too
// small for the requested model — no silent accumulation of stuck
// rows on global.
//
// Returns true when env.BaseLoadBytes is 0 (unknown — pass) or when
// at least one worker has the hardware to ever host the load,
// regardless of current memory pressure. The dispatch-time check
// (memoryEligible) handles "fits eventually but not right now."
func (s *Scheduler) feasibleByAnyWorker(env queue.Envelope) bool {
	if env.BaseLoadBytes <= 0 {
		return true
	}
	for _, w := range s.WorkersForRuntime(env.RuntimeName) {
		if s.totalMemoryBytes(w) >= env.BaseLoadBytes {
			return true
		}
	}
	return false
}

// defaultHeadroomPct mirrors the worker's compiled-in
// --vram-headroom-pct default (mass-worker-llama-cpp/main.cpp = 75)
// and is the runtime-agnostic last resort when neither the worker nor
// the gateway supplied a headroom value.
const defaultHeadroomPct int32 = 75

// effectiveHeadroomPct resolves the headroom watermark used to project
// w's pool growth, mirroring the worker's own precedence at load time
// (`hints.has_vram_headroom_pct() ? hint : flag`):
//
//  1. env.HeadroomPct — the operator's explicit per-load override; the
//     worker applies it over its own flag, so the projection must too.
//  2. The worker's registration-reported --vram-headroom-pct — the
//     per-worker truth when no override rides the load; the flag is
//     operator-configurable, so any assumed constant is wrong for a
//     reconfigured worker.
//  3. defaultHeadroomPct — backstop for unreporting workers.
//
// Values outside [1,100] mean "unset" at every level.
func effectiveHeadroomPct(w *worker.StreamWorker, env queue.Envelope) int32 {
	if env.HeadroomPct >= 1 && env.HeadroomPct <= 100 {
		return env.HeadroomPct
	}
	if pct := w.VRAMHeadroomPct(); pct >= 1 && pct <= 100 {
		return pct
	}
	return defaultHeadroomPct
}

// projectedLoadBytes returns the gateway-aware prediction of total
// device memory the load will consume on w. Combines the gateway's
// (base, per_slot) estimate and the effective headroom (see
// effectiveHeadroomPct) with the worker's current free memory to
// estimate the post-grow pool size:
//
//	pool       = floor((free − base) × headroom / 100 / per_slot)
//	load_bytes = base + pool × per_slot
//
// Edge cases:
//   - base == 0           → 0 (gateway unknown — caller treats as no
//     prediction; latency math falls back to file bytes).
//   - per_slot <= 0       → load_bytes = base (no concurrency
//     dimension; pool collapses to a single implicit slot folded
//     into base by the gateway).
//   - free <= base        → load_bytes = base (worker can't grow;
//     the per-slot term would be negative).
//
// Returns base when per_slot > 0 but no additional slot fits — the
// load still happens with exactly the base allocation. Returns 0
// only when base itself is 0 (the gateway has no estimate).
func (s *Scheduler) projectedLoadBytes(w *worker.StreamWorker, env queue.Envelope) int64 {
	if env.BaseLoadBytes <= 0 {
		return 0
	}
	if env.PerSlotBytes <= 0 {
		return env.BaseLoadBytes
	}
	headroom := effectiveHeadroomPct(w, env)
	free := s.freeMemoryBytes(w)
	available := (free - env.BaseLoadBytes) * int64(headroom) / 100
	if available <= 0 {
		return env.BaseLoadBytes
	}
	pool := available / env.PerSlotBytes
	return env.BaseLoadBytes + pool*env.PerSlotBytes
}

// totalMemoryBytes returns the worker's total memory (in bytes) across
// its current enabled device set. Uses the static TotalMemoryMB
// reported at registration time — the hardware ceiling that never
// changes.
func (s *Scheduler) totalMemoryBytes(w *worker.StreamWorker) int64 {
	set := s.deviceSet(w)
	if len(set) == 0 {
		return 0
	}
	byID := make(map[string]int, len(set))
	for _, id := range set {
		byID[id] = 0
	}
	for _, d := range w.Devices() {
		if _, ok := byID[d.ID]; ok {
			byID[d.ID] = d.TotalMemoryMB
		}
	}
	var total int64
	for _, mb := range byID {
		total += int64(mb) * 1024 * 1024
	}
	return total
}

// freeMemoryBytes returns the worker's free memory (in bytes) across
// its current enabled device set. Free = total - used - reservations.
// Reads heartbeat-reported used_memory live, then subtracts the
// worker-wide cold-load reservation ledger so jobs dispatched seconds
// apart don't both admit against the same not-yet-heartbeated slack.
func (s *Scheduler) freeMemoryBytes(w *worker.StreamWorker) int64 {
	set := s.deviceSet(w)
	if len(set) == 0 {
		return 0
	}
	inSet := make(map[string]bool, len(set))
	for _, id := range set {
		inSet[id] = true
	}
	totalsMB := make(map[string]int, len(set))
	for _, d := range w.Devices() {
		if inSet[d.ID] {
			totalsMB[d.ID] = d.TotalMemoryMB
		}
	}
	usedMB := make(map[string]int, len(set))
	for _, st := range w.Stats() {
		if inSet[st.DeviceID] {
			usedMB[st.DeviceID] = st.UsedMemoryMB
		}
	}
	var free int64
	for id, total := range totalsMB {
		used := usedMB[id]
		if delta := int64(total-used) * 1024 * 1024; delta > 0 {
			free += delta
		}
	}
	if r := s.getMemoryReservation(w.ID()); r > 0 {
		free -= r
	}
	if free < 0 {
		return 0
	}
	return free
}

// getMemoryReservation returns the current bytes-reserved sum for
// workerID. Held under the inflight mutex alongside the rest of the
// dispatch-lifecycle bookkeeping.
func (s *Scheduler) getMemoryReservation(workerID string) int64 {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	return s.memoryReservations[workerID]
}

// --- Bench data (read by pickWorkerQueue + workerIsSchedulable) ---

// getBenchmark returns the bench row for (workerID, deviceID), caching
// hits in-process so the hot scoring path doesn't issue one SQLite
// read per (candidate × device × envelope). A miss in the store is
// recorded as a zero-value row so repeat misses stay cheap; ok is
// false in that case so callers can branch on "no bench yet." A row
// counts as present when its Throughput map has at least one positive
// entry — the runtime-private axes the worker actually measured.
func (s *Scheduler) getBenchmark(workerID, deviceID string) (store.BenchmarkRow, bool) {
	s.benchMu.RLock()
	if dev, has := s.benchCache[workerID]; has {
		if row, ok := dev[deviceID]; ok {
			s.benchMu.RUnlock()
			return row, benchPresent(row)
		}
	}
	s.benchMu.RUnlock()

	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return store.BenchmarkRow{}, false
	}

	row, err := st.GetBenchmark(workerID, deviceID)
	if err != nil {
		// Negative cache: remember the miss with a zero row so the next
		// caller short-circuits without re-hitting the store.
		s.benchMu.Lock()
		if s.benchCache[workerID] == nil {
			s.benchCache[workerID] = make(map[string]store.BenchmarkRow)
		}
		s.benchCache[workerID][deviceID] = store.BenchmarkRow{}
		s.benchMu.Unlock()
		return store.BenchmarkRow{}, false
	}

	s.benchMu.Lock()
	if s.benchCache[workerID] == nil {
		s.benchCache[workerID] = make(map[string]store.BenchmarkRow)
	}
	s.benchCache[workerID][deviceID] = row
	s.benchMu.Unlock()
	return row, benchPresent(row)
}

// benchPresent reports whether a bench row carries any usable throughput
// measurement — at least one axis with a positive number.
func benchPresent(row store.BenchmarkRow) bool {
	for _, v := range row.Throughput {
		if v > 0 {
			return true
		}
	}
	return false
}

// InvalidateBench drops the cached row for (workerID, deviceID). Call
// after a fresh bench result lands in the store so the next scoring
// pass picks up the new number.
func (s *Scheduler) InvalidateBench(workerID, deviceID string) {
	s.benchMu.Lock()
	if dev, ok := s.benchCache[workerID]; ok {
		delete(dev, deviceID)
		if len(dev) == 0 {
			delete(s.benchCache, workerID)
		}
	}
	s.benchMu.Unlock()
	// The fresh bench replaces the prior the correction EWMA measured
	// against — learned factors don't transfer onto the new baseline.
	s.ResetCorrections(workerID)
}

// InvalidateWorkerBench drops every cached row for workerID. Useful
// on worker disconnect: stale rows are mostly harmless (the next
// connect re-benches anyway) but freeing the map saves memory.
func (s *Scheduler) InvalidateWorkerBench(workerID string) {
	s.benchMu.Lock()
	delete(s.benchCache, workerID)
	s.benchMu.Unlock()
}

// --- Cleanup helpers (called by main on a timer) ---

// CleanupResults removes job results older than the configured TTL.
func (s *Scheduler) CleanupResults(_ context.Context) (int64, error) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return 0, nil
	}
	return results.Cleanup(s.cfg.EffectiveResultTTL())
}

// StartResultCleanup launches a goroutine that periodically prunes the
// queue_results table. Runs until ctx is cancelled.
func (s *Scheduler) StartResultCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s.CleanupResults(ctx); err != nil {
					s.logger.Warn().Err(err).Msg("results cleanup")
				}
			}
		}
	}()
}

// StartIdleEviction launches a goroutine that periodically evicts loaded
// models that have been idle longer than the configured TTL.
func (s *Scheduler) StartIdleEviction(ctx context.Context) {
	go func() {
		ttl := s.cfg.EffectiveIdleEvictionTTL()
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.evictIdleOnce(s.cfg.EffectiveIdleEvictionTTL())
			}
		}
	}()
}

// evictIdleOnce scans every online worker for loaded models whose
// IdleSince exceeds ttl and unloads them.
func (s *Scheduler) evictIdleOnce(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	for _, w := range s.workers.All() {
		sw, ok := w.(*worker.StreamWorker)
		if !ok || !sw.Status().Online {
			continue
		}
		for _, lm := range sw.LoadedModels() {
			if lm.Active > 0 || lm.IdleSince.IsZero() || lm.IdleSince.After(cutoff) {
				continue
			}
			// EvictModel returns nil even when every unload send failed
			// (per-worker failures are logged and skipped inside), so gate
			// the success log on the count — otherwise a dead worker's
			// failed eviction reads as evicted.
			n, err := s.EvictModel(sw.RuntimeName(), lm.ModelID, sw.ID())
			if err != nil || n == 0 {
				s.logger.Warn().Err(err).
					Str("worker", sw.ID()).
					Str("model_id", lm.ModelID).
					Msg("idle eviction failed")
				continue
			}
			s.logger.Info().
				Str("worker", sw.ID()).
				Str("model_id", lm.ModelID).
				Dur("idle_for", time.Since(lm.IdleSince)).
				Msg("evicted idle model")
		}
	}
}
