package worker

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/KernelPryanic/ctxerr"

	"connectrpc.com/connect"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass-proto/gen/go/worker/workerconnect"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ workerconnect.WorkerHubHandler = (*Hub)(nil)

// CanonicalSetFn returns the set of model file relpaths MASS still considers
// live. A worker reporting cache files outside this set may be told to
// reap them on the next reconcile pass.
type CanonicalSetFn func() map[string]struct{}

// RuntimeNameRegisteredFn reports whether MASS has the given runtime kind
// installed (and thus can route this worker's traffic). Workers whose
// runtime_name is not registered are rejected at handshake.
type RuntimeNameRegisteredFn func(runtimeName string) bool

// EnabledDevicesProviderFn returns the operator-controlled enabled-device
// whitelist for a worker. nil means "no persisted state" (worker comes up
// with all advertised devices enabled, MASS sends them as the explicit
// initial set so the worker's in-memory state is initialised).
type EnabledDevicesProviderFn func(workerID string, advertised []string) []string

// Heartbeat liveness window. A worker that hasn't sent a heartbeat in
// heartbeatStaleAfter is treated as dead even if the underlying stream
// is still open (TCP keepalive can take minutes to fire on a frozen
// process). heartbeatCheckInterval is how often the watcher polls.
//
// Workers tick heartbeats every few seconds in healthy operation, so
// 60s gives a generous margin for slow GCs / hot restarts before MASS
// boots them.
const (
	heartbeatStaleAfter    = 60 * time.Second
	heartbeatCheckInterval = 15 * time.Second
)

// Hub implements the WorkerHub ConnectRPC service on the MASS side.
// Workers connect as clients; the hub manages their lifecycle.
type Hub struct {
	workerconnect.UnimplementedWorkerHubHandler

	fleet          *Fleet
	massURL        string
	modelsDir      string
	canonical      CanonicalSetFn
	runtimeOK      RuntimeNameRegisteredFn
	enabledDevices EnabledDevicesProviderFn
	logger         zerolog.Logger
}

// NewHub creates a new WorkerHub service. canonical may be nil during early
// init; the hub then skips cache reconciliation until [Hub.SetCanonicalFn]
// is called. runtimeOK may be nil: when nil the hub admits workers of any
// runtime_name (useful in tests).
func NewHub(fleet *Fleet, massURL, modelsDir string, canonical CanonicalSetFn, runtimeOK RuntimeNameRegisteredFn, logger zerolog.Logger) *Hub {
	return &Hub{
		fleet:     fleet,
		massURL:   massURL,
		modelsDir: modelsDir,
		canonical: canonical,
		runtimeOK: runtimeOK,
		logger:    logger.With().Str("component", "worker_hub").Logger(),
	}
}

// SetCanonicalFn wires the canonical-set provider after construction.
func (h *Hub) SetCanonicalFn(fn CanonicalSetFn) { h.canonical = fn }

// SetRuntimeNameRegisteredFn wires the runtime registry check after
// construction. The runtimes manager isn't available when the hub is built.
func (h *Hub) SetRuntimeNameRegisteredFn(fn RuntimeNameRegisteredFn) { h.runtimeOK = fn }

// SetEnabledDevicesProvider wires the source of the operator-controlled
// enabled-device whitelist. When unset, the hub sends the worker's full
// advertised device list on connect (sane default before any toggle).
func (h *Hub) SetEnabledDevicesProvider(fn EnabledDevicesProviderFn) { h.enabledDevices = fn }

// Connect handles a bidirectional stream from a worker.
func (h *Hub) Connect(ctx context.Context, stream *connect.BidiStream[workerpb.WorkerMessage, workerpb.HubMessage]) error {
	// First message must be Register.
	firstMsg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("reading registration: %w", err)
	}
	reg := firstMsg.GetRegister()
	if reg == nil {
		return fmt.Errorf("first message must be Register, got %T", firstMsg.Msg)
	}
	if reg.RuntimeName == "" {
		return fmt.Errorf("worker %s register: runtime_name is required", reg.Id)
	}
	if h.runtimeOK != nil && !h.runtimeOK(reg.RuntimeName) {
		return ctxerr.With(fmt.Errorf("runtime kind %q is not installed", reg.RuntimeName), map[string]any{"worker_id": reg.Id, "runtime_name": reg.RuntimeName})
	}

	// Create a cancellable context so the stream can be killed from the UI.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	sender := &bidiSender{stream: stream}
	loopback := isLoopbackPeer(stream.Peer().Addr)
	worker := NewStreamWorker(reg, sender, h.massURL, h.modelsDir, loopback, h.logger)
	worker.SetCancelFn(streamCancel)

	if err := h.fleet.Register(worker); err != nil {
		return ctxerr.With(fmt.Errorf("registering worker %s: %w", reg.Id, err), map[string]any{"worker_id": reg.Id, "worker_name": reg.Name, "runtime_name": reg.RuntimeName})
	}
	h.logger.Info().Str("worker", reg.Id).Str("name", reg.Name).Str("runtime_name", reg.RuntimeName).Int("devices", len(reg.Devices)).Msg("worker connected")

	// Push the operator-controlled enabled-device whitelist to the worker.
	// Workers are stateless: this resync runs on every reconnect so the
	// worker's in-memory set matches MASS's persisted intent.
	advertised := make([]string, len(reg.Devices))
	for i, d := range reg.Devices {
		advertised[i] = d.Id
	}
	enabled := advertised
	if h.enabledDevices != nil {
		enabled = h.enabledDevices(reg.Id, advertised)
	}
	if err := worker.SetEnabledDevices(enabled); err != nil {
		h.logger.Warn().Err(err).Str("worker", reg.Id).Msg("pushing enabled devices on connect")
	}

	defer func() {
		worker.SetOffline()
		if err := h.fleet.Deregister(worker.ID()); err != nil {
			h.logger.Warn().Err(err).Str("worker", worker.ID()).Msg("deregistering worker on disconnect")
		}
		h.logger.Info().Str("worker", reg.Id).Msg("worker disconnected")
	}()

	// Liveness watcher: if the worker stops heartbeating but keeps
	// the stream open (frozen process, network split), TCP keepalives
	// can take minutes to surface the failure. Poll lastSeen and boot
	// the stream early so the scheduler stops dispatching to a zombie.
	// SetOffline cancels streamCtx, which unblocks stream.Receive() in
	// the loop below and lets Connect return cleanly.
	go func() {
		ticker := time.NewTicker(heartbeatCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				if worker.HeartbeatStale(heartbeatStaleAfter) {
					h.logger.Warn().Str("worker", reg.Id).Dur("stale_after", heartbeatStaleAfter).Msg("worker heartbeat stale; marking offline")
					worker.SetOffline()
					return
				}
			}
		}
	}()

	for {
		msg, err := stream.Receive()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return ctxerr.With(fmt.Errorf("receiving from worker %s: %w", reg.Id, err), map[string]any{"worker_id": reg.Id})
		}

		switch m := msg.Msg.(type) {
		case *workerpb.WorkerMessage_Register:
			h.logger.Warn().Str("worker", reg.Id).Msg("duplicate Register message ignored")
		case *workerpb.WorkerMessage_Heartbeat:
			loadedChanged := worker.ApplyHeartbeat(m.Heartbeat)
			h.fleet.NotifyUpdate(worker.ID())
			if loadedChanged {
				h.fleet.NotifyLoadedChanged(worker.ID())
			}
			h.maybeReconcile(ctx, worker)
		case *workerpb.WorkerMessage_JobResult:
			deliverJobResult(worker, m.JobResult)
		case *workerpb.WorkerMessage_LoadModel:
			lm := m.LoadModel
			worker.DeliverLoadResult(lm.JobId, LoadResult{PoolSize: lm.PoolSize}, lm.Error)
		case *workerpb.WorkerMessage_UnloadModel:
			um := m.UnloadModel
			worker.DeliverUnloadResult(um.JobId, um.Error)
		case *workerpb.WorkerMessage_Benchmark:
			br := m.Benchmark
			results := make([]bench.Result, len(br.Results))
			for i, d := range br.Results {
				results[i] = bench.Result{
					DeviceID:      d.DeviceId,
					DeviceName:    d.DeviceName,
					MemoryGBs:     d.MemoryGbs,
					ComputeGFlops: d.ComputeGflops,
					BenchedAt:     time.Now(),
				}
			}
			worker.DeliverBenchResult(br.JobId, results, br.Error)
		default:
			h.logger.Warn().Msgf("unknown worker message type %T", msg.Msg)
		}
	}
}

// maybeReconcile diffs the worker's reported cache_files against the canonical
// set and tells the worker to drop anything not in it. Best-effort: a failure
// to delete is logged but does not abort processing.
func (h *Hub) maybeReconcile(ctx context.Context, w *StreamWorker) {
	_ = ctx
	if h.canonical == nil {
		return
	}
	files := w.CacheFiles()
	if len(files) == 0 {
		return
	}
	canonical := h.canonical()
	var stale []string
	loaded := loadedModelIDs(w)
	for _, f := range files {
		if _, ok := canonical[f]; ok {
			continue
		}
		if _, isLoaded := loaded[f]; isLoaded {
			continue
		}
		stale = append(stale, f)
	}
	if len(stale) == 0 {
		return
	}
	if err := w.DeleteCacheFiles(stale); err != nil {
		h.logger.Warn().Err(err).Str("worker", w.ID()).Int("count", len(stale)).Msg("requesting cache file deletion")
	}
}

func loadedModelIDs(w *StreamWorker) map[string]struct{} {
	loaded := w.LoadedModels()
	ids := make(map[string]struct{}, len(loaded))
	for _, lm := range loaded {
		ids[lm.ModelID] = struct{}{}
	}
	return ids
}

// deliverJobResult converts a wire-side WorkerJobResult into a JobChunk and
// hands it to the worker's pending channel.
func deliverJobResult(w *StreamWorker, jr *workerpb.WorkerJobResult) {
	chunk := &JobChunk{}
	switch r := jr.Result.(type) {
	case *workerpb.WorkerJobResult_Chunk:
		chunk.Type = JobChunkTypeChunk
		chunk.Chunk = r.Chunk
	case *workerpb.WorkerJobResult_Progress:
		chunk.Type = JobChunkTypeProgress
		chunk.Pct = r.Progress.Pct
		chunk.Note = r.Progress.Note
	case *workerpb.WorkerJobResult_Completed:
		chunk.Type = JobChunkTypeCompleted
		chunk.Final = r.Completed.FinalResponse
	case *workerpb.WorkerJobResult_Error:
		chunk.Type = JobChunkTypeError
		chunk.ErrText = r.Error.Message
	default:
		return
	}
	w.DeliverJobChunk(jr.JobId, chunk)
}

// bidiSender wraps a BidiStream to implement the jobSenderInterface.
type bidiSender struct {
	stream *connect.BidiStream[workerpb.WorkerMessage, workerpb.HubMessage]
}

func (s *bidiSender) Send(msg *workerpb.HubMessage) error {
	return s.stream.Send(msg)
}

// isLoopbackPeer reports whether addr (as reported by [connect.Peer.Addr]) is
// a loopback host. Empty addr is treated as non-loopback so an unknown peer
// never gets the in-place file shortcut.
func isLoopbackPeer(addr string) bool {
	if addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return host == "localhost"
}
