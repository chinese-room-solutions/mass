package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ WorkerInterface = (*StreamWorker)(nil)

// jobSenderInterface is the interface for sending messages to the worker via the bidi stream.
type jobSenderInterface interface {
	Send(msg *workerpb.HubMessage) error
}

// StreamWorker implements WorkerInterface for remote workers on a bidi
// stream — MASS pushes jobs, worker pushes results. Loopback workers
// (same-host, identified at register time) get model files by absolute
// path instead of URL; see [buildLocalModelFile].
type StreamWorker struct {
	mu        sync.RWMutex
	id        string
	name      string
	devices   []stats.Device
	online    bool
	lastSeen  time.Time
	massURL   string
	modelsDir string
	loopback  bool

	sendMu sync.Mutex
	sender jobSenderInterface

	pendingMu sync.Mutex
	pending   map[string]chan *workerpb.WorkerJobResult

	deviceStats []stats.DeviceStats // latest stats from heartbeat
	cacheFiles  []string            // latest cache file list (forward-slash relpaths under worker's modelsDir)

	cancelFn func() // cancels the hub's stream context
	logger   zerolog.Logger
}

// NewStreamWorker creates a stream worker from registration info. loopback
// signals that the worker connected from the same host as MASS, which lets
// model loads share files in place instead of copying.
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
		id:        reg.Id,
		name:      reg.Name,
		devices:   devices,
		online:    true,
		lastSeen:  time.Now(),
		massURL:   massURL,
		modelsDir: modelsDir,
		loopback:  loopback,
		sender:    sender,
		pending:   make(map[string]chan *workerpb.WorkerJobResult),
		logger:    logger.With().Str("worker", reg.Id).Bool("loopback", loopback).Logger(),
	}
}

func (a *StreamWorker) ID() string   { return a.id }
func (a *StreamWorker) Name() string { return a.name }

func (a *StreamWorker) Status() WorkerStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return WorkerStatus{Online: a.online, LastSeen: a.lastSeen}
}

func (a *StreamWorker) Stats() []stats.DeviceStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]stats.DeviceStats, len(a.deviceStats))
	copy(out, a.deviceStats)
	return out
}

func (a *StreamWorker) Devices() []stats.Device {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]stats.Device, len(a.devices))
	copy(out, a.devices)
	return out
}

// ErrWorkerOffline is returned by sendJob when the worker is no longer
// connected. Callers (e.g. remoteChatModel.Close on user-pressed Evict) can
// treat this as "nothing to do — the worker is already gone."
var ErrWorkerOffline = fmt.Errorf("worker offline")

// sendJob sends a job to the worker and blocks until the result arrives.
// Returns ErrWorkerOffline immediately if the worker has disconnected; the
// underlying stream is dead and Send would either error or panic depending
// on the connect-go internals.
func (a *StreamWorker) sendJob(msg *workerpb.HubMessage) (_ *workerpb.WorkerJobResult, err error) {
	a.mu.RLock()
	online := a.online
	a.mu.RUnlock()
	if !online {
		return nil, ctxerr.With(fmt.Errorf("%w: worker %s", ErrWorkerOffline, a.id), map[string]any{"worker_id": a.id})
	}

	jobID := uuid.NewString()
	msg.JobId = jobID

	ch := make(chan *workerpb.WorkerJobResult, 1)
	a.pendingMu.Lock()
	a.pending[jobID] = ch
	a.pendingMu.Unlock()

	defer func() {
		a.pendingMu.Lock()
		delete(a.pending, jobID)
		a.pendingMu.Unlock()
	}()

	// Guard sender.Send against panics from a stream that died after our
	// online check above. SetOffline races with disconnect detection; a
	// panic here would otherwise take down the whole process.
	sendErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = ctxerr.With(fmt.Errorf("%w: panic from worker %s send: %v", ErrWorkerOffline, a.id, r), map[string]any{"worker_id": a.id, "job_id": jobID})
			}
		}()
		a.sendMu.Lock()
		defer a.sendMu.Unlock()
		return a.sender.Send(msg)
	}()
	if sendErr != nil {
		return nil, ctxerr.With(fmt.Errorf("sending job to worker %s: %w", a.id, sendErr), map[string]any{"worker_id": a.id, "job_id": jobID})
	}

	result, ok := <-ch
	if !ok {
		return nil, ctxerr.With(fmt.Errorf("%w: worker %s disconnected while waiting for job %s", ErrWorkerOffline, a.id, jobID), map[string]any{"worker_id": a.id, "job_id": jobID})
	}
	if errMsg := result.GetError(); errMsg != nil {
		return nil, ctxerr.With(fmt.Errorf("worker %s job error: %s", a.id, errMsg.Message), map[string]any{"worker_id": a.id, "job_id": jobID})
	}
	return result, nil
}

// DeliverResult routes a job result to the waiting caller.
func (a *StreamWorker) DeliverResult(result *workerpb.WorkerJobResult) {
	a.pendingMu.Lock()
	ch, ok := a.pending[result.JobId]
	a.pendingMu.Unlock()
	if ok {
		ch <- result
	} else {
		a.logger.Warn().Str("job_id", result.JobId).Msg("received result for unknown job")
	}
}

// SetCancelFn sets the function to call to cancel the hub's stream context.
func (a *StreamWorker) SetCancelFn(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cancelFn = fn
}

// SetOffline marks the worker as offline, cancels the stream, and closes all pending job channels.
func (a *StreamWorker) SetOffline() {
	a.mu.Lock()
	a.online = false
	cancel := a.cancelFn
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	a.pendingMu.Lock()
	for id, ch := range a.pending {
		close(ch)
		delete(a.pending, id)
	}
	a.pendingMu.Unlock()
}

// Disconnect cancels the stream and marks offline. Called when kicked from UI.
func (a *StreamWorker) Disconnect() {
	a.SetOffline()
}

// --- WorkerInterface: BencherInterface ---

func (a *StreamWorker) Bench(deviceID string) (bench.Result, error) {
	result, err := a.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_Benchmark{Benchmark: &workerpb.HubBenchmark{DeviceId: deviceID}},
	})
	if err != nil {
		return bench.Result{}, err
	}
	br := result.GetBenchmark()
	if br == nil || len(br.Results) == 0 {
		return bench.Result{}, fmt.Errorf("no benchmark results returned")
	}
	r := br.Results[0]
	return bench.Result{
		DeviceID:      r.DeviceId,
		DeviceName:    r.DeviceName,
		MemoryGBs:     r.MemoryGbs,
		ComputeGFlops: r.ComputeGflops,
		BenchedAt:     time.Now(),
	}, nil
}

// --- WorkerInterface: ModelLoaderInterface ---

func (a *StreamWorker) LoadChatModel(_ zerolog.Logger, name string, cfg llm.ChatModelConfigInterface, placement llm.PlacementConfig) (llm.ChatModelInterface, error) {
	lc, ok := cfg.(llm.LlamaChatConfig)
	if !ok {
		return nil, ctxerr.With(ErrUnsupportedRuntime, map[string]any{"runtime": cfg.Runtime(), "kind": cfg.Kind()})
	}
	remoteCfg := lc
	adaptChatConfigForPlacement(&remoteCfg, placement)

	files := []*workerpb.ModelFile{
		buildLocalModelFile(workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY, lc.Path, a.modelsDir, a.massURL, a.loopback),
	}
	if lc.MmprojPath != "" {
		files = append(files, buildLocalModelFile(workerpb.ModelFileRole_MODEL_FILE_ROLE_PROJECTOR, lc.MmprojPath, a.modelsDir, a.massURL, a.loopback))
	}

	result, err := a.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_LoadChatModel{LoadChatModel: &workerpb.HubLoadChatModel{
			Config: &rpc.ChatModelConfig{
				Config: &rpc.ChatModelConfig_Llama{Llama: chatConfigToProto(remoteCfg, placement)},
			},
			Files: files,
		}},
	})
	if err != nil {
		return nil, err
	}
	lm := result.GetLoadModel()
	return newRemoteChatModel(lm.GetFingerprint(), name, a, lm.GetPoolSize()), nil
}

func (a *StreamWorker) LoadEmbeddingModel(_ zerolog.Logger, name string, cfg llm.EmbeddingModelConfigInterface, placement llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	lc, ok := cfg.(llm.LlamaEmbeddingConfig)
	if !ok {
		return nil, ctxerr.With(ErrUnsupportedRuntime, map[string]any{"runtime": cfg.Runtime(), "kind": cfg.Kind()})
	}
	files := []*workerpb.ModelFile{
		buildLocalModelFile(workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY, lc.Path, a.modelsDir, a.massURL, a.loopback),
	}

	result, err := a.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_LoadEmbeddingModel{LoadEmbeddingModel: &workerpb.HubLoadEmbeddingModel{
			Config: &rpc.EmbeddingModelConfig{
				Config: &rpc.EmbeddingModelConfig_Llama{Llama: embeddingConfigToProto(lc, placement)},
			},
			Files: files,
		}},
	})
	if err != nil {
		return nil, err
	}
	lm := result.GetLoadModel()
	return newRemoteEmbeddingModel(lm.GetFingerprint(), name, a, lm.GetPoolSize()), nil
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
