package worker

import (
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
	devices     []stats.Device
	online      bool
	lastSeen    time.Time
	massURL     string
	modelsDir   string
	loopback    bool

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

// NewStreamWorker builds a worker from its registration message.
func NewStreamWorker(reg *workerpb.WorkerRegister, sender jobSenderInterface, massURL, modelsDir string, loopback bool, logger zerolog.Logger) *StreamWorker {
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
		id:          reg.Id,
		name:        reg.Name,
		runtimeName: reg.RuntimeName,
		devices:     devices,
		online:      true,
		lastSeen:    time.Now(),
		massURL:     massURL,
		modelsDir:   modelsDir,
		loopback:    loopback,
		sender:      sender,
		jobs:        make(map[string]chan *JobChunk),
		loads:       make(map[string]chan loadOutcome),
		unloads:     make(map[string]chan error),
		benches:     make(map[string]chan benchOutcome),
		logger:      logger.With().Str("worker", reg.Id).Str("runtime_name", reg.RuntimeName).Bool("loopback", loopback).Logger(),
	}
}

// --- Identity ---

func (w *StreamWorker) ID() string          { return w.id }
func (w *StreamWorker) Name() string        { return w.name }
func (w *StreamWorker) RuntimeName() string { return w.runtimeName }

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
			ModelID:  lm.ModelId,
			PoolSize: int(lm.PoolSize),
			Active:   int(lm.Active),
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
func (w *StreamWorker) DeliverJobChunk(jobID string, chunk *JobChunk) {
	w.pendingMu.Lock()
	ch, ok := w.jobs[jobID]
	if ok && (chunk.Type == JobChunkTypeCompleted || chunk.Type == JobChunkTypeError) {
		delete(w.jobs, jobID)
	}
	w.pendingMu.Unlock()

	if !ok {
		w.logger.Warn().Str("job_id", jobID).Msg("received chunk for unknown job")
		return
	}
	ch <- chunk
	if chunk.Type == JobChunkTypeCompleted || chunk.Type == JobChunkTypeError {
		close(ch)
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
	// Source is the gateway-supplied caller identity ("app: <name>",
	// "direct"); MASS surfaces it in the Scheduler tab. Stored on the
	// per-instance LoadedModelStatus immediately after a successful load.
	Source string
}

// LoadModel asks the worker to load a model. Blocks until the worker reports
// the result.
func (w *StreamWorker) LoadModel(req LoadModelRequest) (LoadResult, error) {
	jobID := uuid.NewString()
	ch := make(chan loadOutcome, 1)

	w.pendingMu.Lock()
	w.loads[jobID] = ch
	w.pendingMu.Unlock()

	if err := w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_LoadModel{
		LoadModel: &workerpb.HubLoadModel{
			JobId:     jobID,
			ModelId:   req.ModelID,
			Files:     req.Files,
			LoadHints: req.LoadHints,
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
// new model loads. Empty deviceIDs means "every advertised device" — the
// bootstrap default. Already-loaded models are unaffected. Fire-and-
// forget; the worker stores the set in memory only and MASS resends on
// every reconnect (workers are stateless).
func (w *StreamWorker) SetEnabledDevices(deviceIDs []string) error {
	return w.send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_SetEnabledDevices{
		SetEnabledDevices: &workerpb.HubSetEnabledDevices{EnabledDeviceIds: deviceIDs},
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
