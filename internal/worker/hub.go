package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/Masterminds/semver/v3"

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

// RuntimeVersionFn returns the installed version of runtimeName and whether it
// is installed. It is the join key into a worker's compatible range: the
// handshake rejects a worker whose range doesn't cover this version. Sibling to
// [RuntimeNameRegisteredFn] so the hub stays decoupled from the runtimes
// manager type.
type RuntimeVersionFn func(runtimeName string) (version string, ok bool)

// EnabledDevicesProviderFn returns the operator-controlled enabled-device
// whitelist for a worker in explicit three-state form (see
// [EnabledDevices]). advertised is the worker's registered device set, in
// registration order, for providers that resolve persisted rows against it.
type EnabledDevicesProviderFn func(workerID string, advertised []string) EnabledDevices

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
	enroller       *Enroller
	authDisabled   func() bool
	massURL        string
	modelsDir      string
	canonical      CanonicalSetFn
	runtimeOK      RuntimeNameRegisteredFn
	runtimeVersion RuntimeVersionFn
	enabledDevices EnabledDevicesProviderFn
	logger         zerolog.Logger
}

// NewHub creates a new WorkerHub service. enroller owns the join-token +
// per-worker-credential lifecycle. canonical may be nil during early init; the
// hub then skips cache reconciliation until [Hub.SetCanonicalFn] is called.
// runtimeOK may be nil: when nil the hub admits workers of any runtime_name
// (useful in tests). The auth-disabled predicate defaults to nil (auth always
// on) until wired via [Hub.SetAuthDisabledFn].
func NewHub(fleet *Fleet, enroller *Enroller, massURL, modelsDir string, canonical CanonicalSetFn, runtimeOK RuntimeNameRegisteredFn, logger zerolog.Logger) *Hub {
	return &Hub{
		fleet:     fleet,
		enroller:  enroller,
		massURL:   massURL,
		modelsDir: modelsDir,
		canonical: canonical,
		runtimeOK: runtimeOK,
		logger:    logger.With().Str("component", "worker_hub").Logger(),
	}
}

// SetAuthDisabledFn wires the live "no operator token configured" predicate.
// When it returns true a worker may enroll without a join token (a bare stream
// + Register still mints per-worker credentials); steady-state credential checks
// always apply once enrolled. When unset the hub always requires a join token to
// enroll.
func (h *Hub) SetAuthDisabledFn(fn func() bool) { h.authDisabled = fn }

// SetCanonicalFn wires the canonical-set provider after construction.
func (h *Hub) SetCanonicalFn(fn CanonicalSetFn) { h.canonical = fn }

// SetRuntimeNameRegisteredFn wires the runtime registry check after
// construction. The runtimes manager isn't available when the hub is built.
func (h *Hub) SetRuntimeNameRegisteredFn(fn RuntimeNameRegisteredFn) { h.runtimeOK = fn }

// SetRuntimeVersionFn wires the installed-runtime-version lookup used by the
// handshake compatibility check. When nil the check is skipped (workers of any
// version admitted — tests, and MASS builds without a runtimes manager).
func (h *Hub) SetRuntimeVersionFn(fn RuntimeVersionFn) { h.runtimeVersion = fn }

// SetEnabledDevicesProvider wires the source of the operator-controlled
// enabled-device whitelist. When unset, the hub sends all=true on connect
// (every device enabled — the sane default before any toggle).
func (h *Hub) SetEnabledDevicesProvider(fn EnabledDevicesProviderFn) { h.enabledDevices = fn }

// Connect handles a bidirectional stream from a worker.
//
// Auth follows the join-token enrollment contract, keyed on the stream's
// metadata (never the shared operator token — that path is gone):
//
//   - Enrollment: authorization: Bearer <join token> and no x-mass-worker-id.
//     The server validates the join token, mints a worker id + secret, persists
//     them, and sends WorkerEnrolled as the FIRST hub message. In no-auth mode
//     (no operator token configured) the join token may be absent — a bare
//     stream still enrolls.
//   - Steady state: authorization: Bearer <per-worker secret> and
//     x-mass-worker-id: <worker id>. The server bcrypt-verifies the pair and
//     sends no WorkerEnrolled.
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

	bearer := bearerFromHeader(stream.RequestHeader().Get("Authorization"))
	presentedID := stream.RequestHeader().Get("X-Mass-Worker-Id")

	// Validate the registration (runtime kind + compat range) BEFORE enrolling,
	// so a rejected worker never leaves an orphan credential row behind. The id
	// in these messages is the one the worker presented, or empty when it is
	// still enrolling — cosmetic either way.
	if reg.RuntimeName == "" {
		return fmt.Errorf("worker %s register: runtime_name is required", presentedID)
	}
	if h.runtimeOK != nil && !h.runtimeOK(reg.RuntimeName) {
		return ctxerr.With(fmt.Errorf("runtime kind %q is not installed", reg.RuntimeName), map[string]any{"worker_id": presentedID, "runtime_name": reg.RuntimeName})
	}
	if h.runtimeVersion != nil {
		installedVersion, ok := h.runtimeVersion(reg.RuntimeName)
		if err := checkRuntimeCompat(presentedID, reg, installedVersion, ok); err != nil {
			return ctxerr.With(err, map[string]any{"worker_id": presentedID, "runtime_name": reg.RuntimeName, "installed_version": installedVersion, "compatible": reg.Compatible})
		}
	}

	// Authenticate and resolve the worker id. Enrollment (no worker id header)
	// mints a new id+secret and must send WorkerEnrolled first; steady state
	// validates the presented id+secret.
	workerID, enrolled, err := h.authenticate(presentedID, bearer, reg.Name)
	if err != nil {
		return err
	}

	// WorkerEnrolled is sent exactly once, before any other hub message, so the
	// worker learns its id + secret and can reconnect in steady state.
	if enrolled != nil {
		if err := stream.Send(&workerpb.HubMessage{Msg: &workerpb.HubMessage_Enrolled{Enrolled: enrolled}}); err != nil {
			return ctxerr.With(fmt.Errorf("sending WorkerEnrolled: %w", err), map[string]any{"worker_id": workerID})
		}
	}

	// Create a cancellable context so the stream can be killed from the UI.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	sender := &bidiSender{stream: stream}
	loopback := isLoopbackPeer(stream.Peer().Addr)
	worker := NewStreamWorker(workerID, reg, sender, h.massURL, h.modelsDir, loopback, h.logger)
	worker.SetCancelFn(streamCancel)

	if err := h.fleet.Register(worker); err != nil {
		return ctxerr.With(fmt.Errorf("registering worker %s: %w", workerID, err), map[string]any{"worker_id": workerID, "worker_name": reg.Name, "runtime_name": reg.RuntimeName})
	}
	h.logger.Info().Str("worker", workerID).Str("name", reg.Name).Str("runtime_name", reg.RuntimeName).Int("devices", len(reg.Devices)).Bool("enrolled", enrolled != nil).Msg("worker connected")

	// Push the operator-controlled enabled-device whitelist to the worker.
	// Workers are stateless: this resync runs on every reconnect so the
	// worker's in-memory set matches MASS's persisted intent.
	advertised := make([]string, len(reg.Devices))
	for i, d := range reg.Devices {
		advertised[i] = d.Id
	}
	enabled := EnabledDevices{All: true}
	if h.enabledDevices != nil {
		enabled = h.enabledDevices(workerID, advertised)
	}
	if err := worker.SetEnabledDevices(enabled); err != nil {
		h.logger.Warn().Err(err).Str("worker", workerID).Msg("pushing enabled devices on connect")
	}

	defer func() {
		worker.SetOffline()
		if err := h.fleet.Deregister(worker.ID()); err != nil {
			h.logger.Warn().Err(err).Str("worker", worker.ID()).Msg("deregistering worker on disconnect")
		}
		h.logger.Info().Str("worker", workerID).Msg("worker disconnected")
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
					h.logger.Warn().Str("worker", workerID).Dur("stale_after", heartbeatStaleAfter).Msg("worker heartbeat stale; marking offline")
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
			return ctxerr.With(fmt.Errorf("receiving from worker %s: %w", workerID, err), map[string]any{"worker_id": workerID})
		}

		switch m := msg.Msg.(type) {
		case *workerpb.WorkerMessage_Register:
			h.logger.Warn().Str("worker", workerID).Msg("duplicate Register message ignored")
		case *workerpb.WorkerMessage_Heartbeat:
			loadedChanged := worker.ApplyHeartbeat(m.Heartbeat)
			h.fleet.NotifyUpdate(worker.ID())
			if loadedChanged {
				h.fleet.NotifyLoadedChanged(worker.ID())
			}
			h.maybeReconcile(worker)
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
					DeviceID:   d.DeviceId,
					DeviceName: d.DeviceName,
					MemoryGBs:  d.MemoryGbs,
					LoadGBs:    d.LoadGbs,
					Flops:      d.GetFlops(),
					BenchedAt:  time.Now(),
				}
			}
			worker.DeliverBenchResult(br.JobId, results, br.Error)
		default:
			h.logger.Warn().Str("type", fmt.Sprintf("%T", msg.Msg)).Msg("unknown worker message type")
		}
	}
}

// authenticate resolves the worker's identity from its stream metadata and the
// enrollment contract. When presentedID is empty the worker is enrolling: the
// join token (bearer) is validated (skipped in no-auth mode when absent), a new
// id+secret is minted, and the WorkerEnrolled to send first is returned. When
// presentedID is set the worker is reconnecting in steady state: the (id,
// secret) pair is bcrypt-verified and enrolled is nil. Errors are gRPC statuses
// so the worker sees a clear, actionable code.
func (h *Hub) authenticate(presentedID, bearer, name string) (workerID string, enrolled *workerpb.WorkerEnrolled, err error) {
	noAuth := h.authDisabled != nil && h.authDisabled()

	// Steady state: the worker echoes the id MASS assigned it, authenticated by
	// its per-worker secret. This check applies even in no-auth mode — once
	// enrolled, a worker always proves its identity.
	if presentedID != "" {
		if bearer == "" {
			return "", nil, connect.NewError(connect.CodeUnauthenticated,
				fmt.Errorf("worker %s: missing per-worker secret (authorization bearer)", presentedID))
		}
		if err := h.enroller.authenticateWorker(presentedID, bearer); err != nil {
			switch {
			case errors.Is(err, ErrUnknownWorker):
				return "", nil, connect.NewError(connect.CodeUnauthenticated,
					fmt.Errorf("worker %s is unknown or revoked; re-enroll with a join token", presentedID))
			case errors.Is(err, ErrBadWorkerSecret):
				return "", nil, connect.NewError(connect.CodeUnauthenticated,
					fmt.Errorf("worker %s: per-worker secret does not match", presentedID))
			default:
				return "", nil, connect.NewError(connect.CodeInternal, err)
			}
		}
		return presentedID, nil, nil
	}

	// Enrollment: no worker id yet. Require a valid join token unless auth is
	// disabled, in which case a bare stream still enrolls.
	if !noAuth {
		if bearer == "" {
			return "", nil, connect.NewError(connect.CodeUnauthenticated,
				fmt.Errorf("worker enrollment requires a join token (authorization bearer)"))
		}
		if err := h.enroller.validateJoinToken(bearer); err != nil {
			if errors.Is(err, ErrInvalidJoinToken) {
				return "", nil, connect.NewError(connect.CodeUnauthenticated, ErrInvalidJoinToken)
			}
			return "", nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	id, secret, err := h.enroller.Enroll(name)
	if err != nil {
		return "", nil, connect.NewError(connect.CodeInternal, fmt.Errorf("enrolling worker: %w", err))
	}
	return id, &workerpb.WorkerEnrolled{WorkerId: id, Secret: secret}, nil
}

// bearerFromHeader extracts the token from an "Authorization: Bearer <token>"
// header value, or "" when absent or not a bearer credential.
func bearerFromHeader(authHeader string) string {
	const prefix = "Bearer "
	if len(authHeader) < len(prefix) || authHeader[:len(prefix)] != prefix {
		return ""
	}
	return authHeader[len(prefix):]
}

// checkRuntimeCompat validates a worker's declared compatible range against the
// installed runtime version. installedOK reports whether the runtime is
// installed (the caller already rejected unregistered runtime_names, so a false
// here means the runtime vanished between the two lookups — treat as not
// installed).
//
// Both handshake fields are required: the worker binary at HEAD always sends
// its version and compatible range, so an empty value means a broken or foreign
// worker and is rejected with a distinct, operator-actionable error. A non-empty
// range that fails to parse, or an installed version that isn't semver, is
// likewise rejected — a worker that lies about its range must not slip through.
func checkRuntimeCompat(workerID string, reg *workerpb.WorkerRegister, installedVersion string, installedOK bool) error {
	if reg.Version == "" {
		return fmt.Errorf("worker %s register: version is required (worker sent none)", workerID)
	}
	if reg.Compatible == "" {
		return fmt.Errorf("worker %s (version %q) register: compatible range is required (worker sent none)",
			workerID, reg.Version)
	}
	if !installedOK {
		return fmt.Errorf("worker %s (version %q) declares compatible range %q but runtime %q is not installed",
			workerID, reg.Version, reg.Compatible, reg.RuntimeName)
	}
	if installedVersion == "" {
		return nil // runtime reports no version; accept (installed-side gap, not the worker's)
	}
	installed, err := semver.NewVersion(installedVersion)
	if err != nil {
		return fmt.Errorf("worker %s (version %q, compatible %q): installed runtime %q version %q is not valid semver",
			workerID, reg.Version, reg.Compatible, reg.RuntimeName, installedVersion)
	}
	constraint, err := semver.NewConstraint(reg.Compatible)
	if err != nil {
		return fmt.Errorf("worker %s (version %q): compatible range %q is not a valid semver constraint",
			workerID, reg.Version, reg.Compatible)
	}
	if !constraint.Check(installed) {
		return fmt.Errorf("worker %s (version %q) declares compatible range %q, which excludes installed runtime %q version %q",
			workerID, reg.Version, reg.Compatible, reg.RuntimeName, installedVersion)
	}
	return nil
}

// maybeReconcile diffs the worker's reported cache_files against the canonical
// set and tells the worker to drop anything MASS no longer considers live.
// Best-effort: a failure to delete is logged but does not abort processing.
//
// A reported cache_file is reaped only when it is neither in the canonical set
// nor protected by a currently-loaded model. All three sets are OPAQUE
// store-relative path strings (never parsed): an entry may denote a single
// file or a directory subtree, so matching is by exact equality OR directory
// path-prefix in either direction (a loaded dir protects the files under it; a
// loaded file is protected by a reported dir that covers it).
func (h *Hub) maybeReconcile(w *StreamWorker) {
	if h.canonical == nil {
		return
	}
	files := w.CacheFiles()
	if len(files) == 0 {
		return
	}
	canonical := h.canonical()
	// SAFETY: never mass-delete a worker's caches because the store was
	// briefly unreadable. The walker returns an empty set on walk error, so
	// an empty canonical set means "unknown", not "nothing is live" — skip.
	if len(canonical) == 0 {
		return
	}
	protected := loadedFiles(w)
	var stale []string
	for _, f := range files {
		if _, ok := canonical[f]; ok {
			continue
		}
		if isProtected(f, protected) {
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

// loadedFiles collects the store-relative keys backing every model currently
// loaded on the worker — the protected set for reconciliation.
func loadedFiles(w *StreamWorker) []string {
	loaded := w.LoadedModels()
	var files []string
	for _, lm := range loaded {
		files = append(files, lm.Files...)
	}
	return files
}

// isProtected reports whether the reported cache key f overlaps any protected
// key p. Overlap is exact equality or a directory relationship in either
// direction: f is under p (p is a loaded subtree covering f) or p is under f
// (f is a reported subtree covering a loaded file). Path components only —
// "gguf/a" never covers "gguf/ab".
func isProtected(f string, protected []string) bool {
	for _, p := range protected {
		if f == p || covers(p, f) || covers(f, p) {
			return true
		}
	}
	return false
}

// covers reports whether dir is a directory prefix of path (path is strictly
// under dir), matching on the "/" boundary so "a/b" covers "a/b/c" but not
// "a/bc".
func covers(dir, path string) bool {
	return len(path) > len(dir) && path[len(dir)] == '/' && path[:len(dir)] == dir
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
