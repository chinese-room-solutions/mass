package scheduler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"connectrpc.com/connect"
	"github.com/KernelPryanic/ctxerr"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	massapp "github.com/chinese-room-solutions/mass-sdk/app"
	"github.com/chinese-room-solutions/mass/internal/apps"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/installer"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/server"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
)

// AppState tracks the lifecycle state of a single app.
type AppState int

const (
	StateStopped AppState = iota
	StateStarting
	StateRunning
	StateStopping
	StateError
)

func (s AppState) String() string {
	switch s {
	case StateStopped:
		return "Stopped"
	case StateStarting:
		return "Starting..."
	case StateRunning:
		return "Running"
	case StateStopping:
		return "Stopping..."
	case StateError:
		return "Error"
	default:
		return "Unknown"
	}
}

// logRingBuffer is a fixed-size circular buffer for log lines.
type logRingBuffer struct {
	lines []string
	pos   int
	full  bool
}

func newLogRingBuffer(size int) *logRingBuffer {
	return &logRingBuffer{lines: make([]string, size)}
}

func (rb *logRingBuffer) Add(line string) {
	rb.lines[rb.pos] = line
	rb.pos++
	if rb.pos >= len(rb.lines) {
		rb.pos = 0
		rb.full = true
	}
}

// Lines returns all buffered lines in chronological order.
func (rb *logRingBuffer) Lines() []string {
	if !rb.full {
		return rb.lines[:rb.pos]
	}
	out := make([]string, len(rb.lines))
	copy(out, rb.lines[rb.pos:])
	copy(out[len(rb.lines)-rb.pos:], rb.lines[:rb.pos])
	return out
}

// logHTTPWriteErr logs HTTP-response write errors at Debug level.
// Write/Encode errors only happen when the client has disconnected — there
// is nothing useful we can do, but we surface them in debug logs to help
// trace mysterious "page hung" reports.
func (o *Scheduler) logHTTPWriteErr(err error, op string) {
	if err == nil {
		return
	}
	o.logger.Debug().Err(err).Str("op", op).Msg("http response write")
}

// ManagedApp holds all runtime state for one app.
type ManagedApp struct {
	Config      *config.AppConfig
	DiskMeta    *installer.AppMetadata // metadata read from app.yml on disk
	Info        *massapp.AppInfo       // populated after subprocess starts
	State       AppState
	Error       error
	ModelErrors []string                 // non-fatal model loading errors
	APIRoutes   []installer.ServiceRoute // HTTP routes from service.pb (available even when stopped)
	runtime     apps.AppRuntimeInterface
	cancelFn    context.CancelFunc
	logBuf      *logRingBuffer

	// On-demand support.
	readyCh    chan struct{} // closed when app reaches StateRunning; recreated on stop
	activeReqs int64         // atomic: number of in-flight requests
	idleTimer  *time.Timer   // fires idleStop after idle timeout
}

// StatusCallback is called whenever an app's state changes.
type StatusCallback func(appName string, state AppState, err error)

// LogCallback is called for each line written to an app's stderr.
type LogCallback func(appName, line string)

// RuntimeFactory creates a new AppRuntimeInterface instance.
// The default factory creates a process-based Manager.
type RuntimeFactory func(logger zerolog.Logger) apps.AppRuntimeInterface

// Scheduler manages all apps.
type Scheduler struct {
	mu  sync.Mutex
	cfg *config.Config
	// saveFn flushes cfg to disk when the scheduler learns app metadata
	// it didn't have at startup (debug-mode app connecting back with its
	// name/version). Injected to keep the scheduler decoupled from disk
	// I/O — tests pass a no-op.
	saveFn         func()
	apps           map[string]*ManagedApp
	appRoutes      map[string]string // full method path → app name (for API routing)
	logger         zerolog.Logger
	onStatus       []StatusCallback
	onLog          []LogCallback
	startFn        func(*Scheduler, *ManagedApp) error // overridable for tests; defaults to doStart
	runtimeFactory RuntimeFactory
	workers        *worker.Fleet
	pool           *modelPool

	// Per-fingerprint singleflight: concurrent LoadModel calls for the same
	// fingerprint wait for the first caller to finish, then reuse its result.
	loadGroup singleflight.Group

	// modelSizes caches the resolved model file size keyed by absolute path,
	// avoiding a repeated os.Stat on every submit. GGUF files are immutable
	// once written; the only way an entry becomes stale is deletion of the
	// underlying file, in which case the fingerprint stops appearing in new
	// submissions and the stale entry harmlessly lingers until restart.
	modelSizes sync.Map // map[string]uint64

	// Queue subsystem (initialized via InitQueue). The device-queue
	// registry lives on the dispatcher — see [Dispatcher.Add] /
	// [Dispatcher.Get] / [Dispatcher.All] — so there is one source of
	// truth for "which device queues exist in this process," guarded by
	// a single lock.
	queuePool  *queue.Pool          // shared owner of the SQL handle for every queue below
	queue      queue.QueueInterface // global queue
	results    queue.ResultStoreInterface
	dispatcher *Dispatcher
	stateStore store.DeviceQueueStateStoreInterface
	cancelQ    context.CancelFunc // cancels all queue goroutines
	qDone      chan struct{}      // closed when all queue goroutines exit
}

// New creates a new Scheduler with the given worker fleet.
// The fleet must have at least one worker registered (typically the local worker).
func New(cfg *config.Config, saveFn func(), logger zerolog.Logger, workers *worker.Fleet) *Scheduler {
	loader := workers.SelectWorker()
	var loaderID, loaderName string
	if loader != nil {
		loaderID = loader.ID()
		loaderName = loader.Name()
	}
	o := &Scheduler{
		cfg:       cfg,
		saveFn:    saveFn,
		apps:      make(map[string]*ManagedApp),
		appRoutes: make(map[string]string),
		logger:    logger.With().Str("component", "scheduler").Logger(),
		runtimeFactory: func(l zerolog.Logger) apps.AppRuntimeInterface {
			return apps.NewManager(l)
		},
		workers: workers,
	}
	o.pool = newModelPool(loader, loaderID, loaderName, o.logger, cfg.EffectiveModelIdleTimeout())
	o.startFn = (*Scheduler).doStart
	return o
}

// InitQueue initializes the two-level queue subsystem:
// a global queue for incoming requests and per-device queues for execution.
// Must be called after the database is open and migrations are applied.
func (o *Scheduler) InitQueue(db *sql.DB, appStore store.StoreInterface) {
	o.queuePool = queue.NewPool(db)
	o.queue = o.queuePool.Open("global")
	o.results = queue.NewResultStore(db)
	o.stateStore = appStore

	ctx, cancel := context.WithCancel(context.Background())
	o.cancelQ = cancel

	// Create dispatcher first so we can attach its pointer to each device
	// queue manager as we register them. The registry is owned by the
	// dispatcher (see [Dispatcher.Add] / [Dispatcher.Get]); the scheduler
	// no longer holds its own copy.
	o.dispatcher = NewDispatcher(DispatcherOpts{
		GlobalQueue: o.queue,
		Results:     o.results,
		StateStore:  appStore,
		BenchStore:  appStore,
		Workers:     o.workers,
		Pool:        o.pool,
		Logger:      o.logger.With().Str("component", "dispatcher").Logger(),
	})

	// Create per-device queues from all online workers.
	for _, ag := range o.workers.All() {
		if !ag.Status().Online {
			continue
		}
		devices := ag.Devices()
		for _, dev := range devices {
			queueName := DeviceQueueName(ag.ID(), dev.ID)
			deviceQ := o.queuePool.Open(queueName)

			dq := NewDeviceQueueManager(
				ag.ID(),
				[]string{dev.ID},
				deviceQ,
				o.queue,
				o.results,
				o.pool,
				appStore,
				o.modelsDir,
				o.logger.With().Str("device", queueName).Logger(),
			)
			dq.dispatcher = o.dispatcher
			dq.loadModelFn = o.LoadModel
			o.dispatcher.Add(queueName, dq)

			// Register queue state in DB. Enabled is honored on first insert
			// only; on conflict the existing user choice is preserved.
			if uErr := appStore.UpsertDeviceQueueState(store.DeviceQueueState{
				QueueName: queueName,
				WorkerID:  ag.ID(),
				DeviceIDs: []string{dev.ID},
				Enabled:   defaultDeviceEnabled(devices, dev),
			}); uErr != nil {
				o.logger.Warn().Err(uErr).Str("queue", queueName).Msg("upserting device queue state on init")
			}

			o.logger.Info().
				Str("queue", queueName).
				Str("agent", ag.Name()).
				Str("device", dev.Name).
				Msg("device queue created")
		}
	}

	// Listen for worker fleet changes to dynamically add/remove device
	// queues and to release in-flight tasks back to the global queue when an
	// agent disconnects (crash, kicked, network drop).
	o.workers.AddChangeCallback(func(evt worker.FleetChangeEvent) {
		switch evt.Kind {
		case worker.FleetChangeAdded:
			o.onWorkerRegistered(appStore, ctx, evt.WorkerID)
		case worker.FleetChangeRemoved:
			o.onWorkerRemoved(ctx, evt.WorkerID)
		}
	})

	// Start all goroutines.
	var wg sync.WaitGroup

	// Dispatcher goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.dispatcher.Run(ctx)
	}()

	// Per-device queue processors.
	for _, dq := range o.dispatcher.All() {
		wg.Add(1)
		go func(d *DeviceQueueManager) {
			defer wg.Done()
			d.Run(ctx)
		}(dq)
	}

	// TTL-based result cleanup loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.runResultCleanup(ctx)
	}()

	// One-shot reap of messages abandoned by a prior process lifetime
	// (MASS crash, no consumer to write the result). Cover global plus
	// every persisted device queue, including ones whose worker hasn't
	// reconnected. In-process failures already write a result inline, so
	// no background ticker is needed.
	o.reapAbandonedAtStartup(ctx, appStore)

	o.qDone = make(chan struct{})
	go func() {
		wg.Wait()
		close(o.qDone)
	}()

	o.logger.Info().
		Int("device_queues", o.dispatcher.Count()).
		Msg("two-level queue subsystem initialized")
}

// defaultDeviceEnabled decides the initial `enabled` flag on first insert.
// CPU is disabled when the same worker has any GPU (GPU is the intended
// path; CPU was just discovered alongside). CPU-only workers stay enabled
// so they work out of the box.
//
// Only consulted on first insert — user toggles persist across reconnects
// via UpsertDeviceQueueState's ON CONFLICT clause.
func defaultDeviceEnabled(devices []stats.Device, dev stats.Device) bool {
	if dev.Type != stats.DeviceTypeCPU {
		return true
	}
	for _, d := range devices {
		if d.Type == stats.DeviceTypeGPU {
			return false
		}
	}
	return true
}

// reapAbandonedAtStartup builds the list of queues that may hold messages
// from a prior process lifetime — the global queue plus a fresh handle for
// every persisted device queue (whether or not its worker has reconnected
// yet) — and asks the queue layer to mark abandoned messages as failed.
func (o *Scheduler) reapAbandonedAtStartup(ctx context.Context, appStore store.StoreInterface) {
	queues := []queue.QueueInterface{o.queue}

	states, err := appStore.ListDeviceQueueStates()
	if err != nil {
		o.logger.Warn().Err(err).Msg("listing device queue states for startup reap")
	} else {
		for _, st := range states {
			// A handle is enough — the queue layer only needs name + DB
			// to scan for abandoned rows. No goroutine is started.
			queues = append(queues, o.queuePool.Open(st.QueueName))
		}
	}

	n, err := queue.ReapAbandoned(ctx, queues, o.results, o.logger.With().Str("component", "queue_reaper").Logger())
	if err != nil {
		o.logger.Warn().Err(err).Msg("startup reap of abandoned messages failed")
		return
	}
	if n > 0 {
		o.logger.Info().Int("abandoned", n).Msg("startup reap recovered abandoned messages from prior process lifetime")
	}
}

// onWorkerRegistered creates device queues for a newly connected worker.
func (o *Scheduler) onWorkerRegistered(appStore store.StoreInterface, ctx context.Context, workerID string) {
	ag := o.workers.Get(workerID)
	if ag == nil {
		return
	}

	devices := ag.Devices()
	for _, dev := range devices {
		queueName := DeviceQueueName(workerID, dev.ID)

		deviceQ := o.queuePool.Open(queueName)
		dq := NewDeviceQueueManager(
			workerID,
			[]string{dev.ID},
			deviceQ,
			o.queue,
			o.results,
			o.pool,
			appStore,
			o.modelsDir,
			o.logger.With().Str("device", queueName).Logger(),
		)
		dq.dispatcher = o.dispatcher
		dq.loadModelFn = o.LoadModel
		// Add returns the existing manager (and ok=false) if a worker is
		// reconnecting; in that case we keep the prior manager and skip
		// the rest of the setup for this device.
		if _, added := o.dispatcher.Add(queueName, dq); !added {
			continue
		}

		// Enabled is honored on first insert only; on conflict the existing
		// user choice is preserved.
		if uErr := appStore.UpsertDeviceQueueState(store.DeviceQueueState{
			QueueName: queueName,
			WorkerID:  workerID,
			DeviceIDs: []string{dev.ID},
			Enabled:   defaultDeviceEnabled(devices, dev),
		}); uErr != nil {
			o.logger.Warn().Err(uErr).Str("queue", queueName).Msg("upserting device queue state on hot-add")
		}

		// Start the device queue processor goroutine.
		go dq.Run(ctx)

		o.logger.Info().
			Str("queue", queueName).
			Str("agent", ag.Name()).
			Str("device", dev.Name).
			Msg("device queue created for new agent")
	}
}

// onWorkerRemoved releases every task on the vanished worker's device
// queues back to global, then drops the device queues. Each release is
// transactional ([DrainToGlobal]) — no in-neither-queue window, tasks
// keep their original global position.
func (o *Scheduler) onWorkerRemoved(ctx context.Context, workerID string) {
	// Drop any pool entries that point at this worker. The worker's local
	// inference state died with it; leaving stale pool rows pointing at a
	// dead stream means Acquire/Evict on those entries either silently fails
	// or crashes (Send on a closed BidiStream can panic). The pool change
	// notification refreshes the Scheduler tab so the user sees the models
	// disappear in the same beat as the worker going offline.
	if o.pool != nil {
		if n := o.pool.EvictByWorker(workerID); n > 0 {
			o.logger.Info().Str("agent", workerID).Int("evicted", n).Msg("evicted pool instances after worker disconnect")
		}
	}

	if o.dispatcher == nil {
		return
	}
	// Snapshot the registry, then Remove each one belonging to workerID.
	// Remove holds the dispatcher's own lock so we don't need o.mu here;
	// drain happens outside the lock.
	var orphans []*DeviceQueueManager
	for _, dq := range o.dispatcher.All() {
		if dq.workerID != workerID {
			continue
		}
		if removed := o.dispatcher.Remove(dq.queueName); removed != nil {
			orphans = append(orphans, removed)
		}
	}

	for _, dq := range orphans {
		drained, err := dq.DrainToGlobal(ctx)
		if err != nil {
			o.logger.Error().Err(err).
				Str("agent", workerID).
				Str("queue", dq.queueName).
				Msg("draining device queue after agent disconnect")
			continue
		}
		// Forget any model the pool still associates with this device — the
		// agent that owned it is gone, so the loaded-hash bookkeeping is moot.
		dq.ClearLoadedHash()
		o.logger.Info().
			Str("agent", workerID).
			Str("queue", dq.queueName).
			Int("released", drained).
			Msg("released tasks to global queue after agent disconnect")
	}
}

// Queue returns the inference queue, or nil if not initialized.
func (o *Scheduler) Queue() queue.QueueInterface { return o.queue }

// Results returns the results store, or nil if not initialized.
func (o *Scheduler) Results() queue.ResultStoreInterface { return o.results }

// SetRuntimeFactory overrides the default runtime factory.
func (o *Scheduler) SetRuntimeFactory(f RuntimeFactory) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.runtimeFactory = f
}

// Workers returns the worker fleet.
func (o *Scheduler) Workers() *worker.Fleet {
	return o.workers
}

// ServeHTTP handles ConnectRPC and app HTTP requests.
func (o *Scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ping" {
		o.mu.Lock()
		hasRunning := false
		for _, mp := range o.apps {
			if mp.State == StateRunning {
				hasRunning = true
				break
			}
		}
		o.mu.Unlock()
		if hasRunning {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	// Core ConnectRPC endpoints — route through the durable queue.
	if strings.HasPrefix(r.URL.Path, "/mass.v1.Mass/") {
		// Extract source from X-Mass-Source header (apps set this).
		source := r.Header.Get("X-Mass-Source")
		if source == "" {
			source = "direct"
		}

		// Track the requesting app for idle timeout (keep it alive
		// while its inference request is in-flight).
		if appName := strings.TrimPrefix(source, "app:"); appName != source {
			o.TrackRequestStart(appName)
			defer o.TrackRequestEnd(appName)
		}

		// ChatCompletionStream bypasses the queue and goes straight to the
		// ConnectRPC stream handler — tokens must be delivered in real time
		// and the queue's request/response shape can't carry incremental
		// frames.
		method := strings.TrimPrefix(r.URL.Path, "/mass.v1.Mass/")
		if method == "ChatCompletionStream" {
			o.serveDirect(w, r, source)
			return
		}

		// Inference RPCs go through the queue for durability and prioritization.
		if o.queue != nil {
			o.serveViaQueue(w, r, method, source)
			return
		}

		// Fallback: direct execution if queue is not initialized.
		o.serveDirect(w, r, source)
		return
	}

	o.handleAppHTTP(w, r)
}

// serveDirect dispatches a ConnectRPC request straight to the in-process
// MASS handler — used for streaming RPCs (which can't go through the queue)
// and as the fallback when the queue is not initialized.
func (o *Scheduler) serveDirect(w http.ResponseWriter, r *http.Request, source string) {
	resolver := o.newResolver(source)
	defer resolver.ReleaseAll()
	srv := server.NewServer(o.logger, resolver)
	path, handler := rpcconnect.NewMassHandler(srv, connect.WithInterceptors(server.NewMetricsInterceptor()))
	http.StripPrefix(strings.TrimSuffix(path, "/"), handler).ServeHTTP(w, r)
}

// handleAppHTTP is the legacy app-routed-HTTP path that lives at the end
// of ServeHTTP. Extracted so ServeHTTP doesn't have to duplicate it.
func (o *Scheduler) handleAppHTTP(w http.ResponseWriter, r *http.Request) {
	// App API routing — check if the path matches a registered app service.
	o.mu.Lock()
	appName, routeFound := o.appRoutes[r.URL.Path]
	o.mu.Unlock()
	if routeFound {
		o.serveAppAPI(w, r, appName)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	//nolint:errchkjson // map[string]string cannot fail to marshal
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code": "not_found",
		"msg":  "unknown path",
	})
}

// serveAppAPI handles requests to app-registered API endpoints.
// It ensures the app subprocess is running, reads the JSON body, and
// forwards the call to the app's HandleRequest via gRPC.
func (o *Scheduler) serveAppAPI(w http.ResponseWriter, r *http.Request, appName string) {
	// Extract the short method name from the path (last segment).
	path := r.URL.Path
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		http.Error(w, `{"code":"bad_route","msg":"invalid path"}`, http.StatusBadRequest)
		return
	}
	method := path[idx+1:]

	// Ensure app is running (auto-start on-demand apps).
	if err := o.EnsureRunning(r.Context(), appName); err != nil {
		o.logger.Error().Err(err).Str("app", appName).Str("method", method).Msg("app API: failed to start app")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		//nolint:errchkjson
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "unavailable",
			"msg":  "app not available: " + err.Error(),
		})
		return
	}

	// Track for idle timeout.
	o.TrackRequestStart(appName)
	defer o.TrackRequestEnd(appName)

	// Read request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"reading request body"}`, http.StatusInternalServerError)
		return
	}

	// Get the loaded app and call HandleRequest.
	o.mu.Lock()
	mp := o.apps[appName]
	o.mu.Unlock()
	if mp == nil || mp.runtime == nil {
		http.Error(w, `{"code":"unavailable","msg":"app not running"}`, http.StatusServiceUnavailable)
		return
	}

	loaded := mp.runtime.GetApp(appName)
	if loaded == nil {
		http.Error(w, `{"code":"unavailable","msg":"app not loaded"}`, http.StatusServiceUnavailable)
		return
	}

	if wantsSSE(r) {
		o.serveAppAPIStream(w, r, loaded.App(), appName, method, body)
		return
	}

	result, err := loaded.App().HandleRequest(r.Context(), method, body)
	if err != nil {
		o.logger.Error().Err(err).Str("app", appName).Str("method", method).Msg("app API: HandleRequest error")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		//nolint:errchkjson
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "internal",
			"msg":  err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, wErr := w.Write(result)
	o.logHTTPWriteErr(wErr, "serveAppAPI write result")
}

// wantsSSE reports whether the caller asked for a streaming response via the
// Accept header.
func wantsSSE(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

// serveAppAPIStream forwards the app's streaming RPC to the caller as an
// SSE response. Each chunk produced by HandleRequestStream is emitted as a
// single "data:" line. Errors are emitted as one final "event: error" frame.
func (o *Scheduler) serveAppAPIStream(
	w http.ResponseWriter,
	r *http.Request,
	app massapp.AppInterface,
	appName, method string,
	body []byte,
) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}

	send := func(payload []byte) error {
		// SSE: each line of the chunk is prefixed with "data: ". For app
		// payloads we treat the entire chunk as one event; if the chunk has
		// embedded newlines we replicate them as separate "data:" lines per
		// the SSE spec.
		for _, line := range strings.Split(string(payload), "\n") {
			if _, err := io.WriteString(w, "data: "+line+"\n"); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
		flush()
		return nil
	}

	if err := app.HandleRequestStream(r.Context(), method, body, send); err != nil {
		o.logger.Error().Err(err).Str("app", appName).Str("method", method).Msg("app API: HandleRequestStream error")
		_, wErr := io.WriteString(w, "event: error\ndata: "+jsonEscapeOneLine(err.Error())+"\n\n")
		o.logHTTPWriteErr(wErr, "serveAppAPIStream error event")
		flush()
		return
	}

	_, wErr := io.WriteString(w, "data: [DONE]\n\n")
	o.logHTTPWriteErr(wErr, "serveAppAPIStream done event")
	flush()
}

// jsonEscapeOneLine returns msg as a single-line JSON-safe string suitable
// for embedding in an SSE data field.
func jsonEscapeOneLine(msg string) string {
	b, err := json.Marshal(map[string]string{"error": msg})
	if err != nil {
		return `{"error":"unknown"}`
	}
	return string(b)
}

// AddStatusCallback registers a callback invoked on app state changes.
func (o *Scheduler) AddStatusCallback(cb StatusCallback) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onStatus = append(o.onStatus, cb)
}

// modelsDir returns the centralized models directory path.
func (o *Scheduler) modelsDir() string {
	dataDir, err := o.cfg.EffectiveDataDir()
	if err != nil {
		return ""
	}
	return config.ModelsDir(dataDir)
}

// AddLogCallback registers a callback invoked for each app stderr log line.
func (o *Scheduler) AddLogCallback(cb LogCallback) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onLog = append(o.onLog, cb)
}

// broadcastLog stores the line in the app's ring buffer and calls all registered log callbacks.
// JSON log lines (e.g. from go-plugin) are reformatted to match zerolog console style.
func (o *Scheduler) broadcastLog(name, line string) {
	line = normalizeLogLine(line)
	o.mu.Lock()
	if mp, ok := o.apps[name]; ok && mp.logBuf != nil {
		mp.logBuf.Add(line)
	}
	cbs := make([]LogCallback, len(o.onLog))
	copy(cbs, o.onLog)
	o.mu.Unlock()
	for _, cb := range cbs {
		cb(name, line)
	}
}

// formatLogLine formats a MASS-generated message to match zerolog console output style.
func formatLogLine(level, msg string) string {
	return time.Now().Format(time.RFC3339Nano) + " " + level + " " + msg
}

// normalizeLogLine detects JSON-formatted log lines (e.g. go-plugin debug output)
// and reformats them to match the "timestamp LEVEL message key=value" style.
func normalizeLogLine(line string) string {
	if len(line) == 0 || line[0] != '{' {
		return line
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return line
	}

	// Extract standard fields.
	ts, _ := obj["@timestamp"].(string)
	level, _ := obj["@level"].(string)
	msg, _ := obj["@message"].(string)

	// Also support zerolog-style fields.
	if ts == "" {
		if v, ok := obj["time"].(string); ok {
			ts = v
		}
	}
	if level == "" {
		if v, ok := obj["level"].(string); ok {
			level = v
		}
	}
	if msg == "" {
		if v, ok := obj["message"].(string); ok {
			msg = v
		}
	}

	if ts == "" && level == "" && msg == "" {
		return line // not a recognizable log object
	}

	if ts == "" {
		ts = time.Now().Format(time.RFC3339Nano)
	}

	lvl := strings.ToUpper(level)
	switch lvl {
	case "DEBUG":
		lvl = "DBG"
	case "INFO":
		lvl = "INF"
	case "WARN", "WARNING":
		lvl = "WRN"
	case "ERROR":
		lvl = "ERR"
	case "FATAL":
		lvl = "FTL"
	case "TRACE":
		lvl = "TRC"
	}

	// Build key=value pairs from remaining fields.
	skip := map[string]bool{
		"@timestamp": true, "@level": true, "@message": true,
		"time": true, "level": true, "message": true,
		"@app": true,
	}
	var pairs []string
	for k, v := range obj {
		if skip[k] {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(pairs)

	result := ts + " " + lvl + " " + msg
	if len(pairs) > 0 {
		result += " " + strings.Join(pairs, " ")
	}
	return result
}

// SetLogLevel changes the log level for all running apps via RPC.
func (o *Scheduler) SetLogLevel(level string) {
	o.mu.Lock()
	var targets []*ManagedApp
	for _, mp := range o.apps {
		if mp.runtime != nil {
			targets = append(targets, mp)
		}
	}
	o.mu.Unlock()

	for _, mp := range targets {
		o.setAppLogLevel(mp, level)
	}
}

// setAppLogLevel pushes the log level to a single app via RPC.
func (o *Scheduler) setAppLogLevel(mp *ManagedApp, level string) {
	if mp.runtime == nil {
		return
	}
	if loaded := mp.runtime.GetApp(mp.Config.Name); loaded != nil {
		if err := loaded.App().SetLogLevel(level); err != nil {
			o.logger.Warn().Err(err).Str("app", mp.Config.Name).Msg("failed to set log level on app")
		}
	}
}

// GetLogHistory returns the buffered log lines for a app.
func (o *Scheduler) GetLogHistory(name string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	mp, ok := o.apps[name]
	if !ok || mp.logBuf == nil {
		return nil
	}
	return mp.logBuf.Lines()
}

func (o *Scheduler) notifyStatus(name string, state AppState, err error) {
	o.mu.Lock()
	cbs := make([]StatusCallback, len(o.onStatus))
	copy(cbs, o.onStatus)
	o.mu.Unlock()
	for _, cb := range cbs {
		cb(name, state, err)
	}
}

// expandAppConfig returns a copy of mc with macros expanded in Command and
// Config: ${DATA_DIR}, ${APPS_DIR} (= {DATA_DIR}/apps), and ${APP_DIR}
// (= {APPS_DIR}/{name}/{version}).
func (o *Scheduler) expandAppConfig(mc config.AppConfig) config.AppConfig {
	dataDir, _ := o.cfg.EffectiveDataDir()
	appDir := resolveAppDir(dataDir, mc.Name, mc.Version)
	vars := map[string]string{
		"DATA_DIR": dataDir,
		"APPS_DIR": config.AppInstallDir(dataDir),
		"APP_DIR":  appDir,
	}
	mc.Command = config.ExpandCommandVars(mc.Command, vars)
	mc.Config = config.ExpandVars(mc.Config, vars)
	return mc
}

// resolveAppDir returns the directory for an app, preferring the versioned
// layout ({dataDir}/apps/{name}/{version}) and falling back to the flat
// layout ({dataDir}/apps/{name}) if no version is specified or the
// versioned directory doesn't exist.
func resolveAppDir(dataDir, name, version string) string {
	if version != "" {
		vDir := config.AppVersionDir(dataDir, name, version)
		if _, err := os.Stat(vDir); err == nil {
			return vDir
		}
	}
	// Try to find the latest installed version.
	baseDir := config.AppDir(dataDir, name)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return baseDir // fallback to legacy flat layout
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// If any subdirectory contains a app.yml, it's a versioned layout.
		candidate := filepath.Join(baseDir, e.Name())
		if _, err := installer.ReadMetadataFromDir(candidate); err == nil {
			return candidate
		}
	}
	return baseDir
}

// Register reads app.yml from disk and registers the app without
// launching a subprocess. The app stays in StateStopped until Start()
// is called. This is the primary entry point at boot time.
func (o *Scheduler) Register(appCfg *config.AppConfig) error {
	name := appCfg.Name
	log := o.logger.With().Str("app", name).Logger()

	// Read metadata from disk.
	dataDir, err := o.cfg.EffectiveDataDir()
	if err != nil {
		return ctxerr.With(fmt.Errorf("getting data dir: %w", err), map[string]any{"app": name})
	}
	appDir := resolveAppDir(dataDir, name, appCfg.Version)
	meta, err := installer.ReadMetadataFromDir(appDir)
	if err != nil {
		log.Warn().Err(err).Msg("could not read app.yml, registering with config only")
	}

	// Parse service descriptor for API route registration.
	routes, err := installer.ParseServiceDescriptorFromDir(appDir, meta)
	if err != nil {
		log.Warn().Err(err).Msg("could not parse service descriptor")
	}

	o.mu.Lock()
	o.apps[name] = &ManagedApp{
		Config:    appCfg,
		DiskMeta:  meta,
		State:     StateStopped,
		APIRoutes: routes,
		logBuf:    newLogRingBuffer(500),
		readyCh:   make(chan struct{}),
	}
	// Index routes for fast lookup.
	for _, r := range routes {
		o.appRoutes[r.FullMethod] = name
	}
	o.mu.Unlock()

	version := ""
	if meta != nil {
		version = meta.Version
	}
	log.Info().
		Str("version", version).
		Int("api_routes", len(routes)).
		Msg("app registered (subprocess not started)")

	return nil
}

// debugRetryLoop polls for the app's .reattach.json file and connects
// once the debug app process becomes available.
func (o *Scheduler) debugRetryLoop(ctx context.Context, mp *ManagedApp, mgr apps.AppRuntimeInterface, modConf config.AppConfig) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	appName := mp.Config.Name
	o.logger.Info().Str("app", appName).Msg("debug mode: waiting for app process (polling every 2s)")

	for {
		select {
		case <-ctx.Done():
			o.mu.Lock()
			mp.State = StateError
			mp.Error = fmt.Errorf("debug: cancelled")
			select {
			case <-mp.readyCh:
			default:
				close(mp.readyCh)
			}
			o.mu.Unlock()
			o.notifyStatus(appName, StateError, mp.Error)
			return
		case <-ticker.C:
			err := mgr.LoadApp(ctx, modConf)
			if err != nil {
				o.logger.Debug().Err(err).Str("app", appName).Msg("debug mode: app not ready yet")
				continue
			}

			loaded := mgr.GetApp(appName)
			if loaded == nil {
				continue
			}

			info := loaded.Info

			if info.Name != "" && info.Name != mp.Config.Name {
				loaded.Name = info.Name
				mp.Config.Name = info.Name
			}

			o.mu.Lock()
			mp.Info = info
			mp.State = StateRunning
			mp.Error = nil
			mp.runtime = mgr
			select {
			case <-mp.readyCh:
			default:
				close(mp.readyCh)
			}
			o.mu.Unlock()
			o.notifyStatus(appName, StateRunning, nil)

			o.logger.Info().
				Str("app", info.Name).
				Str("version", info.Version).
				Msg("debug mode: app connected")

			o.saveFn()
			return
		}
	}
}

// Start launches the app subprocess and loads its models in a background goroutine.
func (o *Scheduler) Start(appName string) error {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("app %q not found", appName)
	}
	if mp.State == StateRunning || mp.State == StateStarting {
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("app %q is already %s", appName, mp.State), map[string]any{"app": appName, "state": mp.State.String()})
	}
	mp.State = StateStarting
	mp.Error = nil
	o.mu.Unlock()
	o.launchClaimedApp(mp)
	return nil
}

// launchClaimedApp performs the side effects of starting an app whose state
// has already been flipped to StateStarting under the scheduler lock. Both
// Start and EnsureRunning use this so the StateStopped → StateStarting
// transition is single-locked (no TOCTOU between two concurrent callers).
func (o *Scheduler) launchClaimedApp(mp *ManagedApp) {
	o.notifyStatus(mp.Config.Name, StateStarting, nil)

	// Debug mode: poll for an externally-started app process.
	if mp.Config.Debug {
		mgr := o.runtimeFactory(o.logger)
		mgr.SetLogCallback(o.broadcastLog)
		mp.runtime = mgr
		ctx, cancel := context.WithCancel(context.Background())
		mp.cancelFn = cancel
		go o.debugRetryLoop(ctx, mp, mgr, o.expandAppConfig(*mp.Config))
		return
	}

	go o.runStart(mp)
}

// runStart executes startFn, updates state, and signals readyCh. Used by both
// Start() and EnsureRunning() goroutines.
func (o *Scheduler) runStart(mp *ManagedApp) {
	appName := mp.Config.Name
	err := o.startFn(o, mp)
	o.mu.Lock()
	if err != nil {
		mp.State = StateError
		mp.Error = err
	} else {
		mp.State = StateRunning
		mp.Error = nil
	}
	state := mp.State
	select {
	case <-mp.readyCh:
	default:
		close(mp.readyCh)
	}
	o.mu.Unlock()
	o.notifyStatus(appName, state, err)

	if err != nil {
		o.logger.Error().Err(err).Str("app", appName).Msg("app start failed")
	}
}

// launchSubprocess creates a runtime, sets up environment, and starts the
// app's gRPC subprocess. On success it populates mp.runtime and mp.cancelFn.
func (o *Scheduler) launchSubprocess(mp *ManagedApp) error {
	appName := mp.Config.Name
	errCtx := map[string]any{"app": appName}

	mgr := o.runtimeFactory(o.logger)
	mgr.SetLogCallback(o.broadcastLog)

	// Build environment for the subprocess.
	extraEnv := []string{"MASS_LOG_FORMAT=console"}
	if o.cfg.DataDir != "" {
		extraEnv = append(extraEnv, "MASS_DATA_DIR="+o.cfg.DataDir)
	}
	if lvl, err := o.cfg.Logger.Level.MarshalText(); err == nil {
		extraEnv = append(extraEnv, "MASS_LOG_LEVEL="+string(lvl))
	}
	addr := o.cfg.EffectiveListenAddr()
	if addr[0] == ':' {
		addr = "localhost" + addr
	}
	extraEnv = append(extraEnv, "MASS_API_URL=http://"+addr)
	mgr.SetExtraEnv(extraEnv)

	ctx, cancel := context.WithCancel(context.Background())

	resolved := o.expandAppConfig(*mp.Config)
	if err := mgr.LoadApp(ctx, resolved); err != nil {
		cancel()
		return ctxerr.With(fmt.Errorf("loading app: %w", err), errCtx)
	}

	loaded := mgr.GetApp(appName)
	if loaded == nil {
		cancel()
		mgr.Shutdown()
		return ctxerr.With(fmt.Errorf("app loaded but not found in runtime"), errCtx)
	}

	// Push log level to the subprocess.
	if lvl, err := o.cfg.Logger.Level.MarshalText(); err == nil {
		if err := loaded.App().SetLogLevel(string(lvl)); err != nil {
			o.logger.Warn().Err(err).Str("app", appName).Msg("failed to set log level")
		}
	}

	mp.runtime = mgr
	mp.cancelFn = cancel
	return nil
}

// doStart launches the app subprocess and fetches its info.
// Model requirements are stored but not loaded — they are loaded on-demand
// by the resolver when the app makes its first inference request.
func (o *Scheduler) doStart(mp *ManagedApp) error {
	appName := mp.Config.Name
	log := o.logger.With().Str("app", appName).Logger()

	// --- 1. Launch subprocess ---
	if err := o.launchSubprocess(mp); err != nil {
		return ctxerr.With(fmt.Errorf("launching subprocess for %q: %w", appName, err), map[string]any{"app": appName})
	}

	// Fetch AppInfo from the running subprocess.
	// Model requirements are stored but NOT loaded eagerly — they will be
	// loaded on-demand when the app makes its first inference request.
	if loaded := mp.runtime.GetApp(appName); loaded != nil {
		if info, err := loaded.App().GetInfo(); err == nil {
			mp.Info = info
			// Adopt app's self-declared name.
			if info.Name != "" && info.Name != appName {
				loaded.Name = info.Name
				mp.Config.Name = info.Name
				log = o.logger.With().Str("app", info.Name).Logger()
			}
		}
	}

	if mp.Info != nil && len(mp.Info.Models) > 0 {
		log.Info().Int("models", len(mp.Info.Models)).Msg("app declares model requirements (will load on demand)")
	}

	return nil
}

// Stop unloads all models and kills the app subprocess.
func (o *Scheduler) Stop(appName string) error {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("app %q not found", appName)
	}
	if mp.State != StateRunning && mp.State != StateError {
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("app %q is not running (state: %s)", appName, mp.State), map[string]any{"app": appName, "state": mp.State.String()})
	}
	mp.State = StateStopping
	// Cancel any pending idle timer.
	if mp.idleTimer != nil {
		mp.idleTimer.Stop()
		mp.idleTimer = nil
	}
	o.mu.Unlock()
	o.notifyStatus(appName, StateStopping, nil)

	o.doStop(mp)
	o.broadcastLog(appName, formatLogLine("INF", "app stopped by user"))
	o.resetToStopped(mp)

	return nil
}

// resetToStopped transitions a app back to StateStopped after stopping,
// resetting readyCh for future on-demand use.
func (o *Scheduler) resetToStopped(mp *ManagedApp) {
	name := mp.Config.Name
	o.mu.Lock()
	mp.State = StateStopped
	mp.Error = nil
	mp.readyCh = make(chan struct{})
	o.mu.Unlock()
	o.notifyStatus(name, StateStopped, nil)
}

// doStop kills the app subprocess.
// Models loaded on behalf of this app are dynamic and managed by the pool's
// idle timeout — they are not removed here.
func (o *Scheduler) doStop(mp *ManagedApp) {
	mp.ModelErrors = nil

	// Kill subprocess.
	if mp.cancelFn != nil {
		mp.cancelFn()
		mp.cancelFn = nil
	}
	if mp.runtime != nil {
		mp.runtime.Shutdown()
		mp.runtime = nil
	}
	mp.Info = nil

	o.logger.Info().Str("app", mp.Config.Name).Msg("app stopped (subprocess killed)")
}

// Remove kills the app process and removes it from management.
func (o *Scheduler) Remove(appName string) {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	o.mu.Unlock()
	if !ok {
		return
	}

	o.doStop(mp)

	o.mu.Lock()
	// Clean up route index.
	for _, r := range mp.APIRoutes {
		delete(o.appRoutes, r.FullMethod)
	}
	delete(o.apps, appName)
	o.mu.Unlock()
}

// ShutdownAll stops the queue processor and all running apps.
func (o *Scheduler) ShutdownAll() {
	o.mu.Lock()
	names := make([]string, 0, len(o.apps))
	for name, mp := range o.apps {
		names = append(names, name)
		if mp.idleTimer != nil {
			mp.idleTimer.Stop()
			mp.idleTimer = nil
		}
	}
	o.mu.Unlock()

	for _, name := range names {
		o.mu.Lock()
		mp := o.apps[name]
		o.mu.Unlock()
		if mp != nil {
			o.doStop(mp)
		}
	}

	// Close models (may block waiting for in-flight predictions to finish).
	o.pool.CloseAll()

	// Cancel the queue processor and wait for it to finish so it doesn't
	// try to write to the database after it's closed.
	if o.cancelQ != nil {
		o.cancelQ()
		o.cancelQ = nil
	}
	if o.qDone != nil {
		<-o.qDone
	}
}

// Runtime returns the app's runtime, or nil if not running.
func (mp *ManagedApp) Runtime() apps.AppRuntimeInterface {
	return mp.runtime
}

// GetApp returns current state of an app.
func (o *Scheduler) GetApp(name string) *ManagedApp {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.apps[name]
}

// GetAllApps returns all managed apps.
func (o *Scheduler) GetAllApps() map[string]*ManagedApp {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[string]*ManagedApp, len(o.apps))
	for k, v := range o.apps {
		result[k] = v
	}
	return result
}

// PoolSnapshot returns metadata about all loaded model instances in the pool.
func (o *Scheduler) PoolSnapshot() []ModelInstanceInfo {
	return o.pool.Snapshot()
}

// LoadedSnapshot returns raw config snapshots for every loaded instance,
// suitable for the ListLoadedModels RPC. Loading instances are skipped.
func (o *Scheduler) LoadedSnapshot() []LoadedInstanceInfo {
	return o.pool.LoadedSnapshot()
}

// PoolSnapshotInstance returns metadata for a single instance by fingerprint.
func (o *Scheduler) PoolSnapshotInstance(fp string) (ModelInstanceInfo, bool) {
	return o.pool.SnapshotInstance(fp)
}

// PoolEvict forcefully evicts a model instance by fingerprint.
// Returns true if the instance was found and evicted.
func (o *Scheduler) PoolEvict(fp string) bool {
	// Look up instance info before evicting so we can clear device queue state.
	info, ok := o.pool.SnapshotInstance(fp)

	if !o.pool.Evict(fp) {
		return false
	}

	// Clear the device queue loaded hash so the device is seen as free.
	if ok && o.dispatcher != nil && len(info.DeviceIDs) > 0 {
		var queueName string
		if len(info.DeviceIDs) == 1 {
			queueName = DeviceQueueName(info.WorkerID, info.DeviceIDs[0])
		} else {
			queueName = DeviceGroupQueueName(info.WorkerID, info.DeviceIDs)
		}
		if uErr := o.dispatcher.stateStore.UpdateLoadedHash(queueName, ""); uErr != nil {
			o.logger.Warn().Err(uErr).Str("queue", queueName).Msg("clearing loaded_hash after unload")
		}
		// Also clear the in-memory hash on the device queue manager so the
		// next dispatched task triggers a fresh ensureModel.
		if dq := o.dispatcher.Get(queueName); dq != nil {
			dq.ClearLoadedHash()
		}
	}
	return true
}

// SetDeviceQueueEnabled enables or disables a device queue for scheduling.
// When disabling, any pending tasks in the queue are drained back to the global
// queue for redistribution. Returns the number of drained tasks.
func (o *Scheduler) SetDeviceQueueEnabled(ctx context.Context, queueName string, enabled bool) (int, error) {
	dq := o.dispatcher.Get(queueName)
	if dq == nil {
		return 0, fmt.Errorf("device queue %q not found", queueName)
	}

	if err := o.stateStore.SetEnabled(queueName, enabled); err != nil {
		return 0, ctxerr.With(fmt.Errorf("persisting enabled state: %w", err), map[string]any{
			"queue_name": queueName,
			"enabled":    enabled,
		})
	}

	var drained int
	if !enabled {
		var err error
		drained, err = dq.DrainToGlobal(ctx)
		if err != nil {
			return drained, ctxerr.With(fmt.Errorf("draining device queue: %w", err), map[string]any{
				"queue_name": queueName,
			})
		}
	}

	action := "enabled"
	if !enabled {
		action = "disabled"
	}
	o.logger.Info().
		Str("queue", queueName).
		Str("action", action).
		Int("drained", drained).
		Msg("device queue scheduling toggled")

	return drained, nil
}

// LoadModel loads a model into the pool and returns its fingerprint. The
// config's Kind() picks the chat or embedding path.
//
// mode controls auto-eviction:
//   - ModeStatic: pinned by an explicit user action (RPC LoadModel) — never
//     auto-evicted.
//   - ModeDynamic: triggered by an inference request — eligible for
//     idle-timeout eviction when activeReqs returns to zero.
func (o *Scheduler) LoadModel(cfg config.ModelConfigInterface, userPlacement config.PlacementConfig, source string, mode InstanceMode) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("model config is required")
	}
	if err := cfg.Validate(); err != nil {
		return "", fmt.Errorf("invalid %s config: %w", cfg.Kind(), err)
	}
	fp := cfg.Fingerprint()

	// If model is already loaded in pool, return existing fingerprint.
	if o.pool.HasChat(fp) || o.pool.HasEmbedding(fp) {
		return fp, nil
	}

	// Singleflight: if multiple callers request the same fingerprint concurrently
	// (e.g. batch requests), only the first one loads; others wait and reuse.
	result, err, _ := o.loadGroup.Do(fp, func() (any, error) {
		return o.doLoadModel(cfg, userPlacement, source, mode)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// vramHints carries the identity fields that influence placement decisions.
// Each runtime projects its config into this shape so the scheduler stays
// runtime-agnostic.
type vramHints struct {
	modelPath string
	auxPaths  []string
}

// vramHintsFor projects a model config into the scheduler's hint shape.
// Add a case when a new runtime joins.
func vramHintsFor(cfg config.ModelConfigInterface) vramHints {
	switch c := cfg.(type) {
	case config.LlamaChatConfig:
		h := vramHints{modelPath: c.Path}
		if c.MmprojPath != "" {
			h.auxPaths = append(h.auxPaths, c.MmprojPath)
		}
		return h
	case config.LlamaEmbeddingConfig:
		return vramHints{modelPath: c.Path}
	default:
		return vramHints{}
	}
}

// doLoadModel performs the actual model loading — device selection, placement
// computation, and agent dispatch. Called via singleflight to dedup concurrent
// loads of the same fingerprint.
func (o *Scheduler) doLoadModel(cfg config.ModelConfigInterface, userPlacement config.PlacementConfig, source string, mode InstanceMode) (string, error) {
	fp := cfg.Fingerprint()

	// Double-check after singleflight dedup (previous caller may have loaded it).
	if o.pool.HasChat(fp) || o.pool.HasEmbedding(fp) {
		return fp, nil
	}

	hints := vramHintsFor(cfg)

	// tryLoad attempts to load the model on the given candidate.
	// Returns the fingerprint on success, or an error.
	tryLoad := func(candidate *Candidate) (string, error) {
		ag := o.workers.Get(candidate.WorkerID)
		if ag == nil {
			return "", ctxerr.With(fmt.Errorf("agent %s not found", candidate.WorkerID), map[string]any{"worker_id": candidate.WorkerID, "queue": candidate.QueueName})
		}

		placement := o.computePlacement(userPlacement, candidate, hints.modelPath, hints.auxPaths)

		o.logger.Info().
			Str("fingerprint", fp).
			Str("path", hints.modelPath).
			Str("runtime", cfg.Runtime()).
			Str("kind", string(cfg.Kind())).
			Str("agent", candidate.WorkerID).
			Str("device_queue", candidate.QueueName).
			Int32("gpu_layers", placement.GpuLayers).
			Int32("max_concurrent", placement.MaxConcurrent).
			Str("tensor_split", placement.TensorSplit).
			Msg("loading model via scheduler placement")

		switch c := cfg.(type) {
		case config.LlamaChatConfig:
			model, err := ag.LoadChatModel(o.logger, "", c, placement)
			if err != nil {
				return "", err
			}
			fp = o.pool.RegisterChat(mode, source, "", c, placement, model, ag.ID(), ag.Name(), candidate.DeviceIDs...)
		case config.LlamaEmbeddingConfig:
			model, err := ag.LoadEmbeddingModel(o.logger, "", c, placement)
			if err != nil {
				return "", err
			}
			fp = o.pool.RegisterEmbedding(mode, source, "", c, placement, model, ag.ID(), ag.Name(), candidate.DeviceIDs...)
		default:
			return "", ctxerr.With(fmt.Errorf("unsupported model config %T", cfg), map[string]any{"runtime": cfg.Runtime(), "kind": cfg.Kind()})
		}

		if o.dispatcher != nil {
			if uErr := o.dispatcher.stateStore.UpdateLoadedHash(candidate.QueueName, fp); uErr != nil {
				o.logger.Warn().Err(uErr).Str("queue", candidate.QueueName).Str("fingerprint", fp).Msg("recording loaded_hash after load")
			}
		}
		return fp, nil
	}

	// Placement priority for manual loads:
	//   1. Best free local device (no eviction)
	//   2. Best free remote device (no eviction)
	//   3. Evict from the best device anywhere (prefer highest GFlops)
	// On per-candidate load failure (e.g. config incompatibility), log and
	// try the next option.
	type placementStep struct {
		name string
		find func() *Candidate
	}
	steps := []placementStep{
		{"free local device", func() *Candidate {
			return o.dispatcher.selectBestFreePlacement(fp, true)
		}},
		{"free remote device", func() *Candidate {
			return o.dispatcher.selectBestFreePlacement(fp, false)
		}},
		{"evict for best device", func() *Candidate {
			return o.dispatcher.selectBestEvictablePlacement(fp)
		}},
		{"any enabled device", func() *Candidate {
			// Manual load — no incoming task. Pass zero difficulty/size so
			// scoring collapses to "lightest queue on the strongest device."
			return o.dispatcher.selectPlacement(fp, 0, 0)
		}},
	}

	var lastErr error
	tried := make(map[string]bool) // avoid retrying the same candidate
	for _, step := range steps {
		candidate := step.find()
		if candidate == nil || tried[candidate.QueueName] {
			continue
		}
		tried[candidate.QueueName] = true

		result, err := tryLoad(candidate)
		if err == nil {
			return result, nil
		}
		o.logger.Warn().Err(err).
			Str("step", step.name).
			Str("agent", candidate.WorkerID).
			Str("queue", candidate.QueueName).
			Msg("placement failed, trying next option")
		lastErr = err
	}

	if lastErr != nil {
		return "", ctxerr.With(fmt.Errorf("all placement options failed: %w", lastErr), map[string]any{"runtime": cfg.Runtime(), "kind": string(cfg.Kind()), "model_path": hints.modelPath})
	}
	return "", ctxerr.With(fmt.Errorf("no device available for model (all offline or insufficient resources)"), map[string]any{"runtime": cfg.Runtime(), "kind": string(cfg.Kind()), "model_path": hints.modelPath})
}

// computePlacement fills in auto-calculated placement fields where the user
// hasn't provided overrides.
func (o *Scheduler) computePlacement(user config.PlacementConfig, candidate *Candidate, modelPath string, auxPaths []string) config.PlacementConfig {
	result := user

	// Model file size for GPU-layer estimation; includes auxiliary files
	// (e.g. mmproj) that also consume VRAM.
	modelSize, _ := ModelFileSize(modelPath)
	for _, p := range auxPaths {
		if s, err := ModelFileSize(p); err == nil {
			modelSize += s
		}
	}

	// Tensor split: auto if multi-device and user didn't specify.
	if result.TensorSplit == "" && len(candidate.DeviceIDs) > 1 {
		// Build device infos from agent's device list for memory info.
		ag := o.workers.Get(candidate.WorkerID)
		if ag != nil {
			var devInfos []DeviceInfo
			for _, did := range candidate.DeviceIDs {
				for _, dev := range ag.Devices() {
					if dev.ID == did {
						devInfos = append(devInfos, DeviceInfo{
							DeviceID:      did,
							TotalMemoryMB: dev.TotalMemoryMB,
						})
						break
					}
				}
			}
			if len(devInfos) > 1 {
				result.TensorSplit = CalcTensorSplit(devInfos)
			}
		}
	}

	// GPU layers: force CPU-only for CPU devices, auto-calc for GPU devices.
	// Convention: 0 = auto (all to GPU), -1 = CPU only, N > 0 = specific layers.
	if candidate.IsCPU() {
		result.GpuLayers = -1
	} else if result.GpuLayers == 0 && modelSize > 0 {
		result.GpuLayers = CalcGpuLayers(modelSize, int64(candidate.TotalMemoryMB))
	}

	// MaxConcurrent stays at the user-supplied value (or zero meaning "auto").
	// The worker is the source of truth: it allocates as many slots as VRAM
	// allows and reports the actual count back via WorkerLoadModelResult.
	// Pre-load the placement cannot honestly predict that number.
	return result
}

// newResolver creates a modelResolver that routes new model loads through the
// full scheduler pipeline (device selection, placement, agent dispatch) — the
// same workflow as the UI "Load Model" button.
func (o *Scheduler) newResolver(source string) *modelResolver {
	return &modelResolver{
		pool:      o.pool,
		source:    source,
		modelsDir: o.modelsDir(),
		loadModel: o.LoadModel,
	}
}

// AddPoolChangeCallback registers a callback invoked when the model pool changes.
func (o *Scheduler) AddPoolChangeCallback(cb PoolChangeCallback) {
	o.pool.AddChangeCallback(cb)
}

// --- Queue execution ---

// runResultCleanup periodically purges expired result entries from the
// results table according to the configured TTL. Blocks until ctx is cancelled.
// The TTL is re-read each tick so settings changes take effect without restart;
// an explicit zero (e.g. "0s") skips that tick and pauses cleanup.
func (o *Scheduler) runResultCleanup(ctx context.Context) {
	logger := o.logger.With().Str("component", "result_cleanup").Logger()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttl := o.cfg.EffectiveResultTTL()
			if ttl == 0 {
				continue
			}
			n, err := o.results.Cleanup(ttl)
			if err != nil {
				logger.Error().Err(err).Msg("cleaning up expired results")
			} else if n > 0 {
				logger.Info().Int64("removed", n).Msg("cleaned up expired results")
			}
		}
	}
}

// methodToRequestType maps RPC method names to queue request types.
var methodToRequestType = map[string]queue.RequestType{
	"ChatCompletion":      queue.RequestTypeChatCompletion,
	"BatchChatCompletion": queue.RequestTypeBatchChatCompletion,
	"Embedding":           queue.RequestTypeEmbedding,
	"BatchEmbedding":      queue.RequestTypeBatchEmbedding,
	"Tokenize":            queue.RequestTypeTokenize,
}

// newRequestProto returns an empty proto message for the given RPC method name.
func newRequestProto(method string) proto.Message {
	switch method {
	case "ChatCompletion":
		return &rpc.ChatCompletionRequest{}
	case "BatchChatCompletion":
		return &rpc.BatchChatCompletionRequest{}
	case "Embedding":
		return &rpc.EmbeddingRequest{}
	case "BatchEmbedding":
		return &rpc.BatchEmbeddingRequest{}
	case "Tokenize":
		return &rpc.TokenizeRequest{}
	default:
		return nil
	}
}

// newResponseProto returns an empty proto response message for the given RPC method.
func newResponseProto(method string) proto.Message {
	switch method {
	case "ChatCompletion":
		return &rpc.ChatCompletionResponse{}
	case "BatchChatCompletion":
		return &rpc.BatchChatCompletionResponse{}
	case "Embedding":
		return &rpc.EmbeddingResponse{}
	case "BatchEmbedding":
		return &rpc.BatchEmbeddingResponse{}
	case "Tokenize":
		return &rpc.TokenizeResponse{}
	default:
		return nil
	}
}

// extractDispatchHints parses the proto body to compute (fingerprint, model
// size) for queue dispatch. Both are stashed on the envelope so the
// dispatcher scores placements without re-parsing the proto. Either return
// value may be zero/empty if the model can't be resolved (e.g. external API
// model, missing path) — scoring treats the absence as "no swap cost known."
func (o *Scheduler) extractDispatchHints(reqType queue.RequestType, body []byte) (string, uint64) {
	modelsDir := o.modelsDir()
	switch reqType {
	case queue.RequestTypeChatCompletion:
		req := &rpc.ChatCompletionRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return "", 0
		}
		return o.chatHints(req.ModelConfig, modelsDir)
	case queue.RequestTypeBatchChatCompletion:
		req := &rpc.BatchChatCompletionRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return "", 0
		}
		return o.chatHints(req.ModelConfig, modelsDir)
	case queue.RequestTypeEmbedding:
		req := &rpc.EmbeddingRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return "", 0
		}
		return o.embeddingHints(req.ModelConfig, modelsDir)
	case queue.RequestTypeBatchEmbedding:
		req := &rpc.BatchEmbeddingRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return "", 0
		}
		return o.embeddingHints(req.ModelConfig, modelsDir)
	case queue.RequestTypeTokenize:
		// Tokenize reuses chat hints so model_config-bearing requests get
		// the same affinity/auto-load treatment as ChatCompletion. Legacy
		// `model`-only callers fall through to the resolver's loaded-only
		// lookup, so no hints are needed there.
		req := &rpc.TokenizeRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return "", 0
		}
		return o.chatHints(req.ModelConfig, modelsDir)
	}
	return "", 0
}

// logBatchEnqueue emits one info-level line for batch envelopes (no log
// for single requests). Includes the model fingerprint, batch size, and
// the slot count the dispatcher will price the envelope against — useful
// when investigating a slow-feeling batch (e.g. n=32 against slots=2 will
// take ~16 rounds).
func (o *Scheduler) logBatchEnqueue(reqType queue.RequestType, payload []byte, fp, requestID string) {
	bs := batchSize(reqType, payload)
	if bs <= 1 {
		return
	}
	slots := max(int(o.pool.modelMaxConcurrent(fp)), 1)
	kind := "chat"
	if isEmbeddingBatch(reqType) {
		kind = "embedding"
	}
	o.logger.Info().
		Str("request_id", requestID).
		Str("kind", kind).
		Str("fingerprint", fp).
		Int("batch_size", bs).
		Int("est_parallel_slots", slots).
		Msg("batch enqueued")
}

// chatHints returns (fingerprint, model size) for a chat-kind request.
// model_config is required by the API; an unset config yields ("", 0)
// which the dispatcher treats as "no signal" for placement scoring.
func (o *Scheduler) chatHints(mc *rpc.ChatModelConfig, modelsDir string) (string, uint64) {
	if mc == nil {
		return "", 0
	}
	cfg, _, err := ChatConfigFromProto(mc, modelsDir)
	if err != nil {
		return "", 0
	}
	return cfg.Fingerprint(), o.modelFileSize(cfg.Path)
}

// embeddingHints is the embedding counterpart to chatHints.
func (o *Scheduler) embeddingHints(mc *rpc.EmbeddingModelConfig, modelsDir string) (string, uint64) {
	if mc == nil {
		return "", 0
	}
	cfg, _, err := EmbeddingConfigFromProto(mc, modelsDir)
	if err != nil {
		return "", 0
	}
	return cfg.Fingerprint(), o.modelFileSize(cfg.Path)
}

// modelFileSize returns the cached size of the file at path, populating the
// cache from os.Stat on first call. A missing/unreadable file yields 0 with
// no error: scoring treats unknown sizes as "no swap cost known," which is
// the correct behavior for non-local models or files that haven't been
// downloaded yet.
func (o *Scheduler) modelFileSize(path string) uint64 {
	if path == "" {
		return 0
	}
	if v, ok := o.modelSizes.Load(path); ok {
		return v.(uint64)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	size := uint64(fi.Size())
	o.modelSizes.Store(path, size)
	return size
}

// isJSONContentType checks if the request Content-Type is JSON (ConnectRPC supports both JSON and protobuf).
func isJSONContentType(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "application/json")
}

// serveViaQueue reads the request body, submits it to the durable queue,
// waits for the processor to produce a result, and writes the response.
// From the caller's perspective this looks like a normal synchronous RPC.
// Supports both JSON and protobuf Content-Types.
func (o *Scheduler) serveViaQueue(w http.ResponseWriter, r *http.Request, method, source string) {
	reqType, ok := methodToRequestType[method]
	if !ok {
		http.Error(w, `{"code":"bad_route","msg":"unknown method"}`, http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"reading request body"}`, http.StatusInternalServerError)
		return
	}

	// If the request is JSON, convert to protobuf for the queue.
	// The queue and processor always work with protobuf bytes.
	wantJSON := isJSONContentType(r)
	if wantJSON {
		msg := newRequestProto(method)
		if msg == nil {
			http.Error(w, `{"code":"bad_route","msg":"unknown method"}`, http.StatusNotFound)
			return
		}
		// Sanitize invalid UTF-8 sequences — protobuf requires valid UTF-8 in
		// string fields, but clients (especially Windows terminals) may send
		// corrupted multi-byte characters. Replace invalid bytes with U+FFFD.
		body = bytes.ToValidUTF8(body, []byte("\uFFFD"))
		if err := protojson.Unmarshal(body, msg); err != nil {
			o.logger.Error().Err(err).Str("method", method).Msg("protojson unmarshal failed")
			http.Error(w, fmt.Sprintf(`{"code":"malformed","msg":"invalid JSON request: %s"}`, err.Error()), http.StatusBadRequest)
			return
		}
		body, err = proto.Marshal(msg)
		if err != nil {
			http.Error(w, `{"code":"internal","msg":"converting request"}`, http.StatusInternalServerError)
			return
		}
	}

	// Extract fingerprint and model size for placement scoring.
	fp, modelSize := o.extractDispatchHints(reqType, body)

	// Submit to queue.
	sub, err := o.queue.SubmitRaw(r.Context(), reqType, body, source, fp, modelSize, queue.PriorityMedium)
	if err != nil {
		o.logger.Error().Err(err).Str("method", method).Msg("failed to enqueue request")
		http.Error(w, `{"code":"internal","msg":"enqueue failed"}`, http.StatusInternalServerError)
		return
	}
	o.logBatchEnqueue(reqType, body, fp, sub.ID)

	// Create result entry for tracking.
	if err := o.results.Create(sub.ID, sub.RequestHash); err != nil {
		o.logger.Error().Err(err).Str("request_id", sub.ID).Msg("failed to create result entry")
		http.Error(w, `{"code":"internal","msg":"result tracking failed"}`, http.StatusInternalServerError)
		return
	}

	o.logger.Trace().Str("method", method).Str("request_id", sub.ID).Msg("request enqueued, waiting for result")

	// Block until the processor completes or the client disconnects.
	result, err := o.results.WaitForResult(sub.ID, 50*time.Millisecond, r.Context().Done())
	if err != nil {
		o.logger.Error().Err(err).Str("request_id", sub.ID).Msg("waiting for result")
		http.Error(w, `{"code":"internal","msg":"request cancelled or timed out"}`, http.StatusServiceUnavailable)
		return
	}

	if result.Status == queue.ResultStatusError {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		//nolint:errchkjson
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "internal",
			"msg":  result.Error,
		})
		return
	}

	o.writeQueueResponse(w, method, result.Body, wantJSON)
}

// writeQueueResponse writes a protobuf response body, converting to JSON if the client expects it.
func (o *Scheduler) writeQueueResponse(w http.ResponseWriter, method string, body []byte, wantJSON bool) {
	if wantJSON {
		msg := newResponseProto(method)
		if msg != nil {
			if err := proto.Unmarshal(body, msg); err == nil {
				jsonBytes, err := protojson.Marshal(msg)
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, wErr := w.Write(jsonBytes)
					o.logHTTPWriteErr(wErr, "writeQueueResponse json")
					return
				}
			}
		}
		// Fall through to protobuf on conversion failure.
	}
	w.Header().Set("Content-Type", "application/protobuf")
	w.WriteHeader(http.StatusOK)
	_, wErr := w.Write(body)
	o.logHTTPWriteErr(wErr, "writeQueueResponse protobuf")
}

// --- On-demand app loading ---

// EnsureRunning ensures a app is running. If the app is in StateStopped,
// it starts the app and blocks until ready. If already starting, it waits.
func (o *Scheduler) EnsureRunning(ctx context.Context, appName string) error {
	// Atomically: look up the app, decide whether to wait, claim, or reject,
	// and capture readyCh while we hold the lock. Without this single-locked
	// transition two concurrent callers could both observe StateStopped and
	// both call Start, racing the StateStopped → StateStarting flip.
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("app %q not found", appName)
	}

	switch mp.State {
	case StateRunning:
		o.mu.Unlock()
		return nil
	case StateStarting:
		// Another caller or manual start triggered it — wait.
		readyCh := mp.readyCh
		o.mu.Unlock()
		return o.waitForReady(ctx, appName, readyCh)
	case StateStopped:
		// We win the race to claim the start. Flip to StateStarting under the
		// same lock so any other concurrent EnsureRunning/Start caller sees
		// StateStarting and falls into the wait branch instead of trying to
		// claim it themselves.
		mp.State = StateStarting
		mp.Error = nil
		readyCh := mp.readyCh
		o.mu.Unlock()
		o.launchClaimedApp(mp)
		return o.waitForReady(ctx, appName, readyCh)
	default:
		state := mp.State
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("app %q is in state %s, cannot start on demand", appName, state), map[string]any{"app": appName, "state": state.String()})
	}
}

// waitForReady blocks until the app's readyCh is closed or the context is cancelled.
func (o *Scheduler) waitForReady(ctx context.Context, name string, readyCh <-chan struct{}) error {
	select {
	case <-readyCh:
		o.mu.Lock()
		mp := o.apps[name]
		o.mu.Unlock()
		if mp == nil {
			return fmt.Errorf("app %q disappeared", name)
		}
		if mp.State == StateError {
			return ctxerr.With(fmt.Errorf("app %q failed to start: %w", name, mp.Error), map[string]any{"app": name})
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrackRequestStart increments the active request count and cancels any idle timer.
func (o *Scheduler) TrackRequestStart(appName string) {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok {
		o.mu.Unlock()
		return
	}
	if mp.idleTimer != nil {
		mp.idleTimer.Stop()
		mp.idleTimer = nil
	}
	o.mu.Unlock()
	atomic.AddInt64(&mp.activeReqs, 1)
}

// TrackRequestEnd decrements the active request count. If it reaches zero
// and the app is on-demand, starts the idle timer.
func (o *Scheduler) TrackRequestEnd(appName string) {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok {
		o.mu.Unlock()
		return
	}
	newCount := atomic.AddInt64(&mp.activeReqs, -1)
	if newCount <= 0 && mp.Config.EffectiveLaunchMode() == config.LaunchModeOnDemand && mp.State == StateRunning {
		timeout := o.cfg.EffectiveAppIdleTimeout()
		mp.idleTimer = time.AfterFunc(timeout, func() {
			o.idleStop(appName)
		})
	}
	o.mu.Unlock()
}

// ArmIdleTimer starts the idle timer for an on-demand app if it has no
// active requests. Called after UI-triggered starts so the app eventually
// stops if no real requests arrive.
func (o *Scheduler) ArmIdleTimer(appName string) {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok || mp.State != StateRunning || mp.Config.EffectiveLaunchMode() != config.LaunchModeOnDemand {
		o.mu.Unlock()
		return
	}
	if atomic.LoadInt64(&mp.activeReqs) > 0 || mp.idleTimer != nil {
		o.mu.Unlock()
		return
	}
	timeout := o.cfg.EffectiveAppIdleTimeout()
	mp.idleTimer = time.AfterFunc(timeout, func() {
		o.idleStop(appName)
	})
	o.mu.Unlock()
}

// idleStop stops an on-demand app after its idle timeout expires.
func (o *Scheduler) idleStop(appName string) {
	o.mu.Lock()
	mp, ok := o.apps[appName]
	if !ok || mp.State != StateRunning {
		o.mu.Unlock()
		return
	}
	// Double-check no requests arrived while waiting for the lock.
	if atomic.LoadInt64(&mp.activeReqs) > 0 {
		o.mu.Unlock()
		return
	}
	mp.State = StateStopping
	mp.idleTimer = nil
	o.mu.Unlock()
	o.notifyStatus(appName, StateStopping, nil)

	o.doStop(mp)
	o.broadcastLog(appName, formatLogLine("INF", "on-demand app stopped after idle timeout"))
	o.resetToStopped(mp)

	o.logger.Info().Str("app", appName).Msg("on-demand app stopped after idle timeout")
}

// onDemandAppsWithModels returns names of on-demand apps in StateStopped.
// These apps will be started when the inference pool is empty and a request arrives.
func (o *Scheduler) onDemandAppsWithModels() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var names []string
	for name, mp := range o.apps {
		if mp.State == StateStopped &&
			mp.Config.EffectiveLaunchMode() == config.LaunchModeOnDemand {
			names = append(names, name)
		}
	}
	return names
}

// trackOnDemandApps calls TrackRequestStart for all running on-demand apps
// and returns their names (for deferred TrackRequestEnd).
func (o *Scheduler) trackOnDemandApps() []string {
	o.mu.Lock()
	var names []string
	for name, mp := range o.apps {
		if mp.State == StateRunning && mp.Config.EffectiveLaunchMode() == config.LaunchModeOnDemand {
			names = append(names, name)
		}
	}
	o.mu.Unlock()
	for _, name := range names {
		o.TrackRequestStart(name)
	}
	return names
}
