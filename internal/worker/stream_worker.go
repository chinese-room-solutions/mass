package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ WorkerInterface = (*StreamWorker)(nil)

// ErrWorkerOffline is returned when a job is sent to a worker whose stream
// has closed.
var ErrWorkerOffline = errors.New("worker offline")

// Model-benchmark verdicts. The worker classifies its own failure: an
// allocation failure is a permanent capability answer for that device
// set, anything else is worth retrying.
var (
	// ErrBenchIncapable means this device set cannot run this model —
	// never retried automatically.
	ErrBenchIncapable = errors.New("model benchmark: device set cannot run this model")
	// ErrBenchTransient means the bench failed for a reason that may not
	// recur (crash, I/O, unclassifiable).
	ErrBenchTransient = errors.New("model benchmark: transient failure")
)

// jobSenderInterface is the interface for sending messages to the worker via
// the bidi stream.
type jobSenderInterface interface {
	Send(msg *workerpb.HubMessage) error
}

// JobChunk wraps one streamed result frame from a worker. Type carries the
// kind of frame (chunk, completed, error, progress) so callers can drive
// streaming response handling.
type JobChunk struct {
	Type    JobChunkType
	Chunk   []byte // when Type == JobChunkTypeChunk
	Final   []byte // when Type == JobChunkTypeCompleted
	Pct     float32
	Note    string
	ErrText string
}

// JobChunkType discriminates the JobChunk variants.
type JobChunkType int

const (
	JobChunkTypeChunk JobChunkType = iota
	JobChunkTypeProgress
	JobChunkTypeCompleted
	JobChunkTypeError
)

// LoadResult is what the worker returned from a HubLoadModel.
type LoadResult struct {
	PoolSize int32
}

// StreamWorker implements WorkerInterface for a worker on a bidi stream.
// MASS pushes jobs via the sender; the worker pushes results back which the
// hub feeds to DeliverJobChunk / DeliverLoadResult / DeliverUnloadResult.
type StreamWorker struct {
	mu          sync.RWMutex
	id          string
	name        string
	runtimeName string
	version     string // worker's own semver (required at handshake)
	compatible  string // semver range of runtime versions it decodes (required at handshake)
	devices     []stats.Device
	online      bool
	lastSeen    time.Time
	massURL     string
	modelsDir   string
	loopback    bool
	// vramHeadroomPct is the worker's effective --vram-headroom-pct
	// value reported at registration (1-100); 0 means the worker didn't
	// report and consumers fall back to their own default. Fixed for the
	// worker process's lifetime, so no lock.
	vramHeadroomPct int32

	sendMu sync.Mutex
	sender jobSenderInterface

	// Per-job streaming channels. Streaming jobs receive every chunk; the
	// channel is closed after JobCompleted or JobError. One-shot operations
	// (load/unload) use the dedicated maps below.
	pendingMu sync.Mutex
	jobs      map[string]chan *JobChunk
	loads     map[string]chan loadOutcome
	unloads   map[string]chan error
	benches   map[string]chan benchOutcome
	// modelBenches is keyed by model_id, not a job id: the wire contract
	// allows at most one model benchmark in flight per worker, so the
	// model is enough to correlate the reply.
	modelBenches map[string]chan modelBenchOutcome

	deviceStats     []stats.DeviceStats
	cacheFiles      []string
	loaded          []LoadedModelStatus
	availableCap    int
	activeJobsCount int

	cancelFn func()
	logger   zerolog.Logger
}

type loadOutcome struct {
	res LoadResult
	err error
}

type benchOutcome struct {
	results []bench.Result
	err     error
}

// ModelBenchmarkRequest is one gateway-authored benchmark of a model on
// this worker. Files travel exactly as in a load; the worker KEEPS them
// afterwards, which is what turns a benched model into a warm load.
type ModelBenchmarkRequest struct {
	ModelID   string
	Files     []*workerpb.ModelFile
	LoadHints []byte
	Payload   []byte
	Cost      float64
}

// ModelBenchmarkResult is the figures from one successful benchmark.
// ElapsedSecs times the payload alone; GraphSecs is one full-ubatch
// decode; BaseBytes is the load at pool size 1 and PerSlotBytes the cost
// of one additional slot.
type ModelBenchmarkResult struct {
	ElapsedSecs  float64
	GraphSecs    float64
	BaseBytes    int64
	PerSlotBytes int64
}

type modelBenchOutcome struct {
	res ModelBenchmarkResult
	err error
}

// NewFakeStreamWorker constructs a StreamWorker without a bidi stream. It
// reports devices + identity + an online status so the scheduler's read-
// side getters (used by selection logic) work in unit tests. Calls that
// require the worker bidi stream (AssignJob, LoadModel, ...) will fail —
// those paths belong to integration tests with a live hub.
func NewFakeStreamWorker(id, runtimeName string, devices []stats.Device, lastSeen time.Time) *StreamWorker {
	devCopy := make([]stats.Device, len(devices))
	copy(devCopy, devices)
	return &StreamWorker{
		id:           id,
		name:         id,
		runtimeName:  runtimeName,
		devices:      devCopy,
		online:       true,
		lastSeen:     lastSeen,
		jobs:         make(map[string]chan *JobChunk),
		loads:        make(map[string]chan loadOutcome),
		unloads:      make(map[string]chan error),
		benches:      make(map[string]chan benchOutcome),
		modelBenches: make(map[string]chan modelBenchOutcome),
	}
}

// SetFakeVRAMHeadroomPct seeds the registration-reported VRAM headroom
// watermark. Tests only.
func (w *StreamWorker) SetFakeVRAMHeadroomPct(pct int32) { w.vramHeadroomPct = pct }

// SetFakeVersionCompat seeds the registration-reported worker version and
// compatible range without a real registration. Tests only.
func (w *StreamWorker) SetFakeVersionCompat(version, compatible string) {
	w.version = version
	w.compatible = compatible
}

// SetFakeCapacity seeds the worker-wide AvailableCapacity without going
// through a real heartbeat. Tests only.
func (w *StreamWorker) SetFakeCapacity(workerCap int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.availableCap = workerCap
}

// SetFakeActiveJobs seeds the worker-wide ActiveJobs count without going
// through a real heartbeat. Tests only.
func (w *StreamWorker) SetFakeActiveJobs(active int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeJobsCount = active
}

// SetFakeLoadedModels seeds the worker's loaded-model set without going
// through a heartbeat. Tests only — exercises the scheduler's affinity
// path.
func (w *StreamWorker) SetFakeLoadedModels(loaded []LoadedModelStatus) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.loaded = append(w.loaded[:0:0], loaded...)
}

// SetFakeDeviceStats seeds the worker's per-device live stats
// (used / total MB) without a heartbeat. Tests only — exercises
// the memory-fit predicates that read from Stats().
func (w *StreamWorker) SetFakeDeviceStats(stats []stats.DeviceStats) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deviceStats = append(w.deviceStats[:0:0], stats...)
}

// SetFakeLoopback seeds the loopback flag without a real registration.
// Tests only — call before handing the worker to the scheduler.
func (w *StreamWorker) SetFakeLoopback(loopback bool) {
	w.loopback = loopback
}

// fakeSenderFn adapts a plain function into [jobSenderInterface] so
// tests can intercept outbound HubMessages without wiring a real bidi
// stream. Used by [StreamWorker.SetFakeSender].
type fakeSenderFn func(*workerpb.HubMessage) error

func (f fakeSenderFn) Send(msg *workerpb.HubMessage) error { return f(msg) }

// SetFakeSender installs a function-backed sender on the worker so
// tests can observe AssignJob / LoadModel / etc. dispatches and feed
// chunks back via DeliverJobChunk. Pass nil to restore the default
// (no sender; AssignJob fails with ErrWorkerOffline). Tests only.
func (w *StreamWorker) SetFakeSender(fn func(msg *workerpb.HubMessage) error) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if fn == nil {
		w.sender = nil
		return
	}
	w.sender = fakeSenderFn(fn)
}

// NewStreamWorker builds a worker from its server-assigned id and its
// registration message. The id is minted by MASS at enrollment (join-token
// flow) and echoed by the worker on every later connect — the register message
// no longer carries it.
func NewStreamWorker(id string, reg *workerpb.WorkerRegister, sender jobSenderInterface, massURL, modelsDir string, loopback bool, logger zerolog.Logger) *StreamWorker {
	devices := make([]stats.Device, len(reg.Devices))
	for i, d := range reg.Devices {
		devices[i] = stats.Device{
			ID:            d.Id,
			Name:          d.Name,
			Type:          deviceTypeFromProto(d.Type),
			TotalMemoryMB: int(d.TotalMemoryMb),
		}
	}
	return &StreamWorker{
		id:              id,
		name:            reg.Name,
		runtimeName:     reg.RuntimeName,
		version:         reg.Version,
		compatible:      reg.Compatible,
		devices:         devices,
		online:          true,
		lastSeen:        time.Now(),
		massURL:         massURL,
		modelsDir:       modelsDir,
		loopback:        loopback,
		vramHeadroomPct: reg.VramHeadroomPct,
		sender:          sender,
		jobs:            make(map[string]chan *JobChunk),
		loads:           make(map[string]chan loadOutcome),
		unloads:         make(map[string]chan error),
		benches:         make(map[string]chan benchOutcome),
		modelBenches:    make(map[string]chan modelBenchOutcome),
		logger:          logger.With().Str("worker", id).Str("runtime_name", reg.RuntimeName).Bool("loopback", loopback).Logger(),
	}
}

// --- Identity ---

func (w *StreamWorker) ID() string          { return w.id }
func (w *StreamWorker) Name() string        { return w.name }
func (w *StreamWorker) RuntimeName() string { return w.runtimeName }
func (w *StreamWorker) Version() string     { return w.version }
func (w *StreamWorker) Compatible() string  { return w.compatible }

// VRAMHeadroomPct returns the worker-reported effective VRAM headroom
// watermark (1-100), or 0 when the worker didn't report one.
func (w *StreamWorker) VRAMHeadroomPct() int32 { return w.vramHeadroomPct }

// --- Status / stats / hardware ---

func (w *StreamWorker) Status() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return WorkerStatus{Online: w.online, LastSeen: w.lastSeen}
}

// HeartbeatStale reports whether the worker hasn't sent a heartbeat in
// the given window. Used by the Hub's liveness watcher to detect zombie
// streams (TCP open, process frozen). Returns false for offline workers
// — they're already excluded from dispatch.
func (w *StreamWorker) HeartbeatStale(window time.Duration) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.online {
		return false
	}
	return time.Since(w.lastSeen) > window
}

func (w *StreamWorker) Stats() []stats.DeviceStats {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]stats.DeviceStats, len(w.deviceStats))
	copy(out, w.deviceStats)
	return out
}

func (w *StreamWorker) Devices() []stats.Device {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]stats.Device, len(w.devices))
	copy(out, w.devices)
	return out
}

func (w *StreamWorker) LoadedModels() []LoadedModelStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]LoadedModelStatus, len(w.loaded))
	copy(out, w.loaded)
	return out
}

func (w *StreamWorker) AvailableCapacity() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.availableCap
}

func (w *StreamWorker) ActiveJobs() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeJobsCount
}

// CacheFiles returns the worker's most recently reported cache file list.
func (w *StreamWorker) CacheFiles() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]string, len(w.cacheFiles))
	copy(out, w.cacheFiles)
	return out
}

// IsLoopback reports whether the worker is running on the same host as MASS.
func (w *StreamWorker) IsLoopback() bool { return w.loopback }

// MassURL returns the URL workers fetch artifacts from. Empty when unset.
func (w *StreamWorker) MassURL() string { return w.massURL }

// ModelsDir returns the (loopback) host's models directory. Empty when unset.
func (w *StreamWorker) ModelsDir() string { return w.modelsDir }

// SetCancelFn wires the function used to terminate the gRPC stream context.
func (w *StreamWorker) SetCancelFn(fn func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cancelFn = fn
}

// SetOffline marks the worker offline, cancels its stream, and wakes every
// in-flight caller with ErrWorkerOffline.
func (w *StreamWorker) SetOffline() {
	w.mu.Lock()
	w.online = false
	cancel := w.cancelFn
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	w.pendingMu.Lock()
	for id, ch := range w.jobs {
		close(ch)
		delete(w.jobs, id)
	}
	for id, ch := range w.loads {
		ch <- loadOutcome{err: ErrWorkerOffline}
		close(ch)
		delete(w.loads, id)
	}
	for id, ch := range w.unloads {
		ch <- ErrWorkerOffline
		close(ch)
		delete(w.unloads, id)
	}
	for id, ch := range w.benches {
		ch <- benchOutcome{err: ErrWorkerOffline}
		close(ch)
		delete(w.benches, id)
	}
	for id, ch := range w.modelBenches {
		ch <- modelBenchOutcome{err: ErrWorkerOffline}
		close(ch)
		delete(w.modelBenches, id)
	}
	w.pendingMu.Unlock()
}

// Disconnect cancels the stream and marks offline. Called when kicked from UI.
func (w *StreamWorker) Disconnect() { w.SetOffline() }

// --- Heartbeat ingestion (called by Hub) ---

// ApplyHeartbeat updates the worker's local view from a fresh heartbeat.
// Returns loadedChanged=true when the loaded-model set or any per-model
// Active/PoolSize counter moved — Hub uses that to push a Scheduler-tab
// SSE refresh without waiting for the next stats tick.
func (w *StreamWorker) ApplyHeartbeat(hb *workerpb.WorkerHeartbeat) (loadedChanged bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.online = true
	w.lastSeen = time.Now()
	w.activeJobsCount = int(hb.ActiveJobs)
	w.availableCap = int(hb.AvailableCapacity)

	if len(hb.DeviceStats) > 0 {
		dstats := make([]stats.DeviceStats, len(hb.DeviceStats))
		for i, ds := range hb.DeviceStats {
			dstats[i] = stats.DeviceStats{
				DeviceID:       ds.DeviceId,
				UsedMemoryMB:   int(ds.UsedMemoryMb),
				TotalMemoryMB:  int(ds.TotalMemoryMb),
				UtilizationPct: ds.UtilizationPct,
			}
		}
		w.deviceStats = dstats
	}
	w.cacheFiles = append(w.cacheFiles[:0:0], hb.CacheFiles...)

	// Heartbeats are the authoritative source for PoolSize/Active, but they
	// don't carry MASS-side metadata (Source, IdleSince) — preserve it
	// across refreshes by indexing the prior loaded list and merging.
	prev := make(map[string]LoadedModelStatus, len(w.loaded))
	for _, lm := range w.loaded {
		prev[lm.ModelID] = lm
	}
	now := time.Now()
	loaded := make([]LoadedModelStatus, len(hb.LoadedModels))
	for i, lm := range hb.LoadedModels {
		loaded[i] = LoadedModelStatus{
			ModelID:   lm.ModelId,
			PoolSize:  int(lm.PoolSize),
			Active:    int(lm.Active),
			DeviceIDs: append([]string(nil), lm.GetDeviceIds()...),
			Files:     append([]string(nil), lm.GetFiles()...),
		}
		if old, ok := prev[lm.ModelId]; ok {
			loaded[i].Source = old.Source
		}
		// IdleSince: stamp on the first heartbeat where Active==0;
		// preserve across subsequent idle heartbeats; clear when busy.
		if loaded[i].Active > 0 {
			loaded[i].IdleSince = time.Time{}
		} else if old, ok := prev[lm.ModelId]; ok && !old.IdleSince.IsZero() {
			loaded[i].IdleSince = old.IdleSince
		} else {
			loaded[i].IdleSince = now
		}
	}
	loadedChanged = !sameLoadedSet(w.loaded, loaded)
	w.loaded = loaded
	return loadedChanged
}

// sameLoadedSet compares two loaded-model snapshots for the fields the
// Scheduler tab renders: identity (ModelID), pool size, and active count.
// Source/Kind are MASS-side metadata that don't move on heartbeat, so they
// don't need to participate in the diff.
func sameLoadedSet(a, b []LoadedModelStatus) bool {
	if len(a) != len(b) {
		return false
	}
	idx := make(map[string]LoadedModelStatus, len(a))
	for _, lm := range a {
		idx[lm.ModelID] = lm
	}
	for _, lm := range b {
		prev, ok := idx[lm.ModelID]
		if !ok || prev.PoolSize != lm.PoolSize || prev.Active != lm.Active {
			return false
		}
	}
	return true
}

// --- Result delivery (called by Hub) ---

// DeliverJobChunk routes one streaming chunk to the waiting Schedule caller.
//
// The send happens while pendingMu is held, deliberately: [StreamWorker.SetOffline]
// closes job channels under the same lock, so lock-scoped delivery makes
// send and close mutually exclusive — a chunk that looked its channel up
// just before the worker went offline can no longer panic with
// send-on-closed-channel. Holding pendingMu across the send is the
// documented exception to the no-locks-across-blocking-work rule: the
// sole consumer is the scheduler's pumpWorkerChunks, which only appends
// each chunk to an in-memory ring buffer — bounded work, no I/O — so
// the channel drains promptly and the send cannot block indefinitely.
// Terminal frames delete + close under the same critical section, which
// keeps SetOffline from double-closing a channel Deliver just retired.
func (w *StreamWorker) DeliverJobChunk(jobID string, chunk *JobChunk) {
	terminal := chunk.Type == JobChunkTypeCompleted || chunk.Type == JobChunkTypeError
	w.pendingMu.Lock()
	ch, ok := w.jobs[jobID]
	if ok {
		if terminal {
			delete(w.jobs, jobID)
		}
		ch <- chunk
		if terminal {
			close(ch)
		}
	}
	w.pendingMu.Unlock()
	if !ok {
		w.logger.Warn().Str("job_id", jobID).Msg("received chunk for unknown job")
	}
}

// DeliverLoadResult routes a load-model result to the waiting EnsureModelLoaded.
func (w *StreamWorker) DeliverLoadResult(jobID string, res LoadResult, errMsg string) {
	w.pendingMu.Lock()
	ch, ok := w.loads[jobID]
	delete(w.loads, jobID)
	w.pendingMu.Unlock()
	if !ok {
		w.logger.Warn().Str("job_id", jobID).Msg("received load result for unknown job")
		return
	}
	if errMsg != "" {
		ch <- loadOutcome{err: ctxerr.With(fmt.Errorf("worker load: %s", errMsg), map[string]any{"worker_id": w.id, "job_id": jobID})}
	} else {
		ch <- loadOutcome{res: res}
	}
	close(ch)
}

// DeliverUnloadResult routes an unload acknowledgement to the waiting EvictModel.
func (w *StreamWorker) DeliverUnloadResult(jobID string, errMsg string) {
	w.pendingMu.Lock()
	ch, ok := w.unloads[jobID]
	delete(w.unloads, jobID)
	w.pendingMu.Unlock()
	if !ok {
		w.logger.Warn().Str("job_id", jobID).Msg("received unload result for unknown job")
		return
	}
	if errMsg != "" {
		ch <- ctxerr.With(fmt.Errorf("worker unload: %s", errMsg), map[string]any{"worker_id": w.id, "job_id": jobID})
	} else {
		ch <- nil
	}
	close(ch)
}

// --- Outbound message helpers ---

// AssignJob sends an opaque-payload job to the worker and returns a stream of
// chunks. The returned channel is closed after Completed/Error.
func (w *StreamWorker) AssignJob(modelID string, payload []byte) (string, <-chan *JobChunk, error) {
	jobID := uuid.NewString()
	ch := make(chan *JobChunk, 16)

	w.pendingMu.Lock()
	w.jobs[jobID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_AssignJob{
		AssignJob: &workerpb.HubAssignJob{
			JobId:   jobID,
			ModelId: modelID,
			Payload: payload,
		},
	}}); err != nil {
		w.pendingMu.Lock()
		delete(w.jobs, jobID)
		w.pendingMu.Unlock()
		return "", nil, err
	}
	return jobID, ch, nil
}

// CancelJob sends a best-effort cancel for an in-flight job.
func (w *StreamWorker) CancelJob(jobID string) error {
	return w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_CancelJob{
		CancelJob: &workerpb.HubCancelJob{JobId: jobID},
	}})
}

// LoadModelRequest groups the inputs for [StreamWorker.LoadModel].
type LoadModelRequest struct {
	ModelID   string
	Files     []*workerpb.ModelFile
	LoadHints []byte
	// MaxConcurrent pins the context pool to exactly this many slots and
	// turns the worker's own VRAM headroom gate off — MASS has already
	// sized the load against the model's measured base and per-slot
	// bytes, so its memory gate becomes the sole OOM protection.
	// Required (> 0): 0 silently reverts the load to the worker's legacy
	// grow-until-watermark behaviour, which is exactly the unbounded
	// growth the measured pool size exists to replace.
	MaxConcurrent int32
	// Source is the gateway-supplied caller identity ("app: <name>",
	// "direct"); MASS surfaces it in the Scheduler tab. Stored on the
	// per-instance LoadedModelStatus immediately after a successful load.
	Source string
}

// ErrNoPoolSize is returned when a load request omits MaxConcurrent.
// Refusing the send is deliberate: a 0 on the wire is indistinguishable
// from "grow until the watermark", so a caller that forgot to size the
// pool would silently get the behaviour MASS is supposed to have
// replaced. Failing loudly at the boundary keeps that impossible.
var ErrNoPoolSize = errors.New("load model: max_concurrent must be > 0")

// LoadModel asks the worker to load a model with a pinned context-pool
// size. Blocks until the worker reports the result. Returns
// [ErrNoPoolSize] without touching the wire when req.MaxConcurrent isn't
// positive.
func (w *StreamWorker) LoadModel(req LoadModelRequest) (LoadResult, error) {
	if req.MaxConcurrent <= 0 {
		return LoadResult{}, ctxerr.With(ErrNoPoolSize, map[string]any{"worker_id": w.id, "model_id": req.ModelID})
	}
	jobID := uuid.NewString()
	ch := make(chan loadOutcome, 1)

	w.pendingMu.Lock()
	w.loads[jobID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_LoadModel{
		LoadModel: &workerpb.HubLoadModel{
			JobId:         jobID,
			ModelId:       req.ModelID,
			Files:         req.Files,
			LoadHints:     req.LoadHints,
			MaxConcurrent: req.MaxConcurrent,
		},
	}}); err != nil {
		w.pendingMu.Lock()
		delete(w.loads, jobID)
		w.pendingMu.Unlock()
		return LoadResult{}, err
	}

	out, ok := <-ch
	if !ok {
		return LoadResult{}, ctxerr.With(fmt.Errorf("%w: load %s", ErrWorkerOffline, req.ModelID), map[string]any{"worker_id": w.id, "model_id": req.ModelID})
	}
	if out.err == nil {
		// Stamp the loaded-model index immediately so callers issuing a
		// follow-up Schedule don't race the next heartbeat. PoolSize/Active
		// get overwritten on the next heartbeat with worker-authoritative
		// values; Source is MASS-side metadata preserved across refreshes
		// via the merge in updateFromHeartbeat.
		w.markLoaded(req.ModelID, out.res.PoolSize, req.Source)
	}
	return out.res, out.err
}

// markLoaded inserts (or refreshes) a loaded-model entry under the write
// lock. Called immediately after a successful LoadModel so visibility on
// the loaded-model index doesn't depend on the heartbeat cycle.
func (w *StreamWorker) markLoaded(modelID string, poolSize int32, source string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.loaded {
		if w.loaded[i].ModelID == modelID {
			w.loaded[i].PoolSize = int(poolSize)
			if source != "" {
				w.loaded[i].Source = source
			}
			return
		}
	}
	w.loaded = append(w.loaded, LoadedModelStatus{
		ModelID:   modelID,
		PoolSize:  int(poolSize),
		Source:    source,
		IdleSince: time.Now(),
	})
}

// UnloadModel asks the worker to drop a loaded model. Blocks until ack.
func (w *StreamWorker) UnloadModel(modelID string) error {
	jobID := uuid.NewString()
	ch := make(chan error, 1)

	w.pendingMu.Lock()
	w.unloads[jobID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_UnloadModel{
		UnloadModel: &workerpb.HubUnloadModel{
			JobId:   jobID,
			ModelId: modelID,
		},
	}}); err != nil {
		w.pendingMu.Lock()
		delete(w.unloads, jobID)
		w.pendingMu.Unlock()
		return err
	}

	err, ok := <-ch
	if !ok {
		return ctxerr.With(fmt.Errorf("%w: unload %s", ErrWorkerOffline, modelID), map[string]any{"worker_id": w.id, "model_id": modelID})
	}
	return err
}

// DeleteCacheFiles tells the worker to drop a list of cached files.
func (w *StreamWorker) DeleteCacheFiles(filenames []string) error {
	if len(filenames) == 0 {
		return nil
	}
	return w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_DeleteCacheFiles{
		DeleteCacheFiles: &workerpb.HubDeleteCacheFiles{Filenames: filenames},
	}})
}

// SetEnabledDevices replaces the worker's in-memory device whitelist for
// new model loads with the explicit three-state set (see [EnabledDevices]).
// Already-loaded models are unaffected. Fire-and-forget; the worker stores
// the set in memory only and MASS resends on every reconnect (workers are
// stateless).
func (w *StreamWorker) SetEnabledDevices(enabled EnabledDevices) error {
	return w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_SetEnabledDevices{
		SetEnabledDevices: &workerpb.HubSetEnabledDevices{All: enabled.All, DeviceIds: enabled.IDs},
	}})
}

// --- BencherInterface ---

// Bench asks the worker to benchmark the named device. Blocks until the
// worker reports the result. An empty deviceID means "every device" — only
// the first result is returned to satisfy the BencherInterface contract;
// callers wanting all-devices semantics should use [BenchAll].
func (w *StreamWorker) Bench(deviceID string) (bench.Result, error) {
	results, err := w.benchSend(deviceID)
	if err != nil {
		return bench.Result{}, err
	}
	if len(results) == 0 {
		return bench.Result{}, ctxerr.With(fmt.Errorf("worker bench: no results"), map[string]any{"worker_id": w.id, "device_id": deviceID})
	}
	return results[0], nil
}

// BenchAll asks the worker to benchmark every device it enumerates. Blocks
// until all device results arrive in a single response.
func (w *StreamWorker) BenchAll() ([]bench.Result, error) {
	return w.benchSend("")
}

func (w *StreamWorker) benchSend(deviceID string) ([]bench.Result, error) {
	jobID := uuid.NewString()
	ch := make(chan benchOutcome, 1)

	w.pendingMu.Lock()
	w.benches[jobID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_Benchmark{
		Benchmark: &workerpb.HubBenchmark{
			JobId:    jobID,
			DeviceId: deviceID,
		},
	}}); err != nil {
		w.pendingMu.Lock()
		delete(w.benches, jobID)
		w.pendingMu.Unlock()
		return nil, err
	}

	out, ok := <-ch
	if !ok {
		return nil, ctxerr.With(fmt.Errorf("%w: bench %s", ErrWorkerOffline, deviceID), map[string]any{"worker_id": w.id})
	}
	return out.results, out.err
}

// BenchModel runs one gateway-authored benchmark of req.ModelID on this
// worker and blocks until the worker answers, ctx is cancelled, or the
// worker goes offline.
//
// There is deliberately no deadline of its own: the worker runs the
// bench on its control thread and the run can include a multi-gigabyte
// fetch, so the only bounds are the caller's context and the stream's
// lifetime. A worker that dies mid-bench returns [ErrWorkerOffline] and
// leaves no row, so a reconnect re-benches.
//
// A classified failure comes back wrapped around [ErrBenchIncapable] or
// [ErrBenchTransient].
func (w *StreamWorker) BenchModel(ctx context.Context, req ModelBenchmarkRequest) (ModelBenchmarkResult, error) {
	ch := make(chan modelBenchOutcome, 1)

	w.pendingMu.Lock()
	if _, busy := w.modelBenches[req.ModelID]; busy {
		w.pendingMu.Unlock()
		return ModelBenchmarkResult{}, ctxerr.With(fmt.Errorf("model benchmark already in flight"), map[string]any{"worker_id": w.id, "model_id": req.ModelID})
	}
	w.modelBenches[req.ModelID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_ModelBenchmark{
		ModelBenchmark: &workerpb.HubModelBenchmark{
			ModelId:   req.ModelID,
			Files:     req.Files,
			LoadHints: req.LoadHints,
			Payload:   req.Payload,
			Cost:      req.Cost,
		},
	}}); err != nil {
		w.pendingMu.Lock()
		delete(w.modelBenches, req.ModelID)
		w.pendingMu.Unlock()
		return ModelBenchmarkResult{}, err
	}

	select {
	case <-ctx.Done():
		w.pendingMu.Lock()
		delete(w.modelBenches, req.ModelID)
		w.pendingMu.Unlock()
		return ModelBenchmarkResult{}, ctx.Err()
	case out, ok := <-ch:
		if !ok {
			return ModelBenchmarkResult{}, ctxerr.With(fmt.Errorf("%w: model benchmark %s", ErrWorkerOffline, req.ModelID), map[string]any{"worker_id": w.id, "model_id": req.ModelID})
		}
		return out.res, out.err
	}
}

// DeliverModelBenchResult routes a model-benchmark reply to the waiting
// [StreamWorker.BenchModel] caller. The device is already free when this
// arrives — the worker unloads before it answers.
func (w *StreamWorker) DeliverModelBenchResult(modelID string, res ModelBenchmarkResult, err error) {
	w.pendingMu.Lock()
	ch, ok := w.modelBenches[modelID]
	delete(w.modelBenches, modelID)
	w.pendingMu.Unlock()
	if !ok {
		w.logger.Warn().Str("model_id", modelID).Msg("received model benchmark result for unknown model")
		return
	}
	ch <- modelBenchOutcome{res: res, err: err}
	close(ch)
}

// DeliverBenchResult routes a benchmark result to the waiting Bench caller.
func (w *StreamWorker) DeliverBenchResult(jobID string, results []bench.Result, errMsg string) {
	w.pendingMu.Lock()
	ch, ok := w.benches[jobID]
	delete(w.benches, jobID)
	w.pendingMu.Unlock()
	if !ok {
		w.logger.Warn().Str("job_id", jobID).Msg("received bench result for unknown job")
		return
	}
	if errMsg != "" {
		ch <- benchOutcome{err: ctxerr.With(fmt.Errorf("worker bench: %s", errMsg), map[string]any{"worker_id": w.id, "job_id": jobID})}
	} else {
		ch <- benchOutcome{results: results}
	}
	close(ch)
}

// --- Internal ---

func (w *StreamWorker) send(msg *workerpb.HubMessage) (err error) {
	w.mu.RLock()
	online := w.online
	w.mu.RUnlock()
	if !online {
		return ctxerr.With(fmt.Errorf("%w: worker %s", ErrWorkerOffline, w.id), map[string]any{"worker_id": w.id})
	}

	defer func() {
		if r := recover(); r != nil {
			err = ctxerr.With(fmt.Errorf("%w: panic from worker %s send: %v", ErrWorkerOffline, w.id, r), map[string]any{"worker_id": w.id})
		}
	}()
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.sender.Send(msg)
}

// deviceTypeFromProto maps the wire-side WorkerDeviceType enum to the
// internal stats.DeviceType string used throughout the scheduler.
func deviceTypeFromProto(t workerpb.WorkerDeviceType) stats.DeviceType {
	switch t {
	case workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_CPU:
		return stats.DeviceTypeCPU
	case workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_GPU:
		return stats.DeviceTypeGPU
	default:
		return ""
	}
}
