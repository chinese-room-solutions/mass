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

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"connectrpc.com/connect"
	"github.com/KernelPryanic/ctxerr"
	massmodule "github.com/chinese-room-solutions/mass-module"
	"github.com/chinese-room-solutions/mass/internal/agent"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/installer"
	"github.com/chinese-room-solutions/mass/internal/modules"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/server"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/rpc"
	"github.com/chinese-room-solutions/mass/rpc/rpcconnect"
	"github.com/rs/zerolog"
)

// ModuleState tracks the lifecycle state of a single module.
type ModuleState int

const (
	StateStopped ModuleState = iota
	StateStarting
	StateRunning
	StateStopping
	StateError
)

func (s ModuleState) String() string {
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

// ManagedModule holds all runtime state for one module.
type ManagedModule struct {
	Config      *config.ModuleConfig
	DiskMeta    *installer.ModuleMetadata // metadata read from module.yml on disk
	Info        *massmodule.ModuleInfo    // populated after subprocess starts
	State       ModuleState
	Error       error
	ModelErrors []string                 // non-fatal model loading errors
	APIRoutes   []installer.ServiceRoute // HTTP routes from service.pb (available even when stopped)
	runtime     modules.ModuleRuntimeInterface
	cancelFn    context.CancelFunc
	logBuf      *logRingBuffer

	// On-demand support.
	readyCh    chan struct{} // closed when module reaches StateRunning; recreated on stop
	activeReqs int64         // atomic: number of in-flight requests
	idleTimer  *time.Timer   // fires idleStop after idle timeout
}

// StatusCallback is called whenever a module's state changes.
type StatusCallback func(moduleName string, state ModuleState, err error)

// LogCallback is called for each line written to a module's stderr.
type LogCallback func(moduleName, line string)

// RuntimeFactory creates a new ModuleRuntimeInterface instance.
// The default factory creates a process-based Manager.
type RuntimeFactory func(logger zerolog.Logger) modules.ModuleRuntimeInterface

// Scheduler manages all modules.
type Scheduler struct {
	mu             sync.Mutex
	cfg            *config.Config
	saveFn         func()
	modules        map[string]*ManagedModule
	moduleRoutes   map[string]string // full method path → module name (for API routing)
	logger         zerolog.Logger
	onStatus       []StatusCallback
	onLog          []LogCallback
	startFn        func(*Scheduler, *ManagedModule) error // overridable for tests; defaults to doStart
	runtimeFactory RuntimeFactory
	agents         *agent.Registry
	pool           *modelPool

	// Per-fingerprint singleflight: concurrent LoadModel calls for the same
	// fingerprint wait for the first caller to finish, then reuse its result.
	loadGroup singleflight.Group

	// Queue subsystem (initialized via InitQueue).
	queue        queue.QueueInterface // global queue
	results      queue.ResultStoreInterface
	processor    *queue.Processor // legacy single processor (used as fallback)
	dispatcher   *Dispatcher
	deviceQueues map[string]*DeviceQueueManager
	stateStore   store.DeviceQueueStateStoreInterface
	cancelQ      context.CancelFunc // cancels all queue goroutines
	qDone        chan struct{}      // closed when all queue goroutines exit
}

// New creates a new Scheduler with the given agent registry.
// The registry must have at least one agent registered (typically the local agent).
func New(cfg *config.Config, saveFn func(), logger zerolog.Logger, agents *agent.Registry) *Scheduler {
	loader := agents.SelectAgent()
	var loaderID, loaderName string
	if loader != nil {
		loaderID = loader.ID()
		loaderName = loader.Name()
	}
	o := &Scheduler{
		cfg:          cfg,
		saveFn:       saveFn,
		modules:      make(map[string]*ManagedModule),
		moduleRoutes: make(map[string]string),
		logger:       logger.With().Str("component", "scheduler").Logger(),
		runtimeFactory: func(l zerolog.Logger) modules.ModuleRuntimeInterface {
			return modules.NewManager(l)
		},
		agents: agents,
	}
	o.pool = newModelPool(loader, loaderID, loaderName, o.logger, cfg.EffectiveModelIdleTimeout())
	o.startFn = (*Scheduler).doStart
	return o
}

// InitQueue initializes the two-level queue subsystem:
// a global queue for incoming requests and per-device queues for execution.
// Must be called after the database is open and migrations are applied.
func (o *Scheduler) InitQueue(db *sql.DB, appStore store.StoreInterface) {
	o.queue = queue.New(db) // global queue
	o.results = queue.NewResultStore(db)
	o.stateStore = appStore

	ctx, cancel := context.WithCancel(context.Background())
	o.cancelQ = cancel

	// Create per-device queues from all online agents.
	o.deviceQueues = make(map[string]*DeviceQueueManager)
	for _, ag := range o.agents.All() {
		if !ag.Status().Online {
			continue
		}
		for _, dev := range ag.Devices() {
			queueName := DeviceQueueName(ag.ID(), dev.ID)
			deviceQ := queue.NewNamed(db, queueName, 3, 30*time.Second)

			dq := NewDeviceQueueManager(
				ag.ID(),
				[]string{dev.ID},
				deviceQ,
				o.queue,
				o.results,
				o.pool,
				appStore,
				appStore,
				o.modelsDir,
				o.logger.With().Str("device", queueName).Logger(),
			)
			o.deviceQueues[queueName] = dq

			// Register queue state in DB.
			_ = appStore.UpsertDeviceQueueState(store.DeviceQueueState{
				QueueName: queueName,
				AgentID:   ag.ID(),
				DeviceIDs: []string{dev.ID},
			})

			o.logger.Info().
				Str("queue", queueName).
				Str("agent", ag.Name()).
				Str("device", dev.Name).
				Msg("device queue created")
		}
	}

	// Create dispatcher.
	o.dispatcher = NewDispatcher(DispatcherOpts{
		GlobalQueue:  o.queue,
		DeviceQueues: o.deviceQueues,
		Results:      o.results,
		StateStore:   appStore,
		BenchStore:   appStore,
		Agents:       o.agents,
		Pool:         o.pool,
		Logger:       o.logger.With().Str("component", "dispatcher").Logger(),
	})

	// Wire dispatcher and scheduler load function into device queues.
	for _, dq := range o.deviceQueues {
		dq.dispatcher = o.dispatcher
		dq.loadModelFn = o.LoadModel
	}

	// Listen for agent registry changes to dynamically add/remove device queues.
	o.agents.AddChangeCallback(func(evt agent.RegistryChangeEvent) {
		if evt.Kind == agent.RegistryChangeAdded {
			o.onAgentRegistered(db, appStore, ctx, evt.AgentID)
		}
	})

	// Also keep the legacy single processor for the cleanup loop (TTL-based result cleanup).
	o.processor = queue.NewProcessor(queue.ProcessorOpts{
		Queue:     o.queue,
		Results:   o.results,
		ExecuteFn: o.executeEnvelope,
		Logger:    o.logger.With().Str("component", "queue_cleanup").Logger(),
		ResultTTL: o.cfg.EffectiveResultTTL(),
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
	for _, dq := range o.deviceQueues {
		wg.Add(1)
		go func(d *DeviceQueueManager) {
			defer wg.Done()
			d.Run(ctx)
		}(dq)
	}

	// Cleanup goroutine (reuses legacy processor's cleanup loop only).
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.processor.RunCleanupOnly(ctx)
	}()

	o.qDone = make(chan struct{})
	go func() {
		wg.Wait()
		close(o.qDone)
	}()

	o.logger.Info().
		Int("device_queues", len(o.deviceQueues)).
		Msg("two-level queue subsystem initialized")
}

// onAgentRegistered creates device queues for a newly connected agent.
func (o *Scheduler) onAgentRegistered(db *sql.DB, appStore store.StoreInterface, ctx context.Context, agentID string) {
	ag := o.agents.Get(agentID)
	if ag == nil {
		return
	}

	for _, dev := range ag.Devices() {
		queueName := DeviceQueueName(agentID, dev.ID)

		// Skip if already exists (agent reconnecting).
		if _, exists := o.deviceQueues[queueName]; exists {
			continue
		}

		deviceQ := queue.NewNamed(db, queueName, 3, 30*time.Second)
		dq := NewDeviceQueueManager(
			agentID,
			[]string{dev.ID},
			deviceQ,
			o.queue,
			o.results,
			o.pool,
			appStore,
			appStore,
			o.modelsDir,
			o.logger.With().Str("device", queueName).Logger(),
		)
		dq.dispatcher = o.dispatcher
		dq.loadModelFn = o.LoadModel
		o.deviceQueues[queueName] = dq
		o.dispatcher.deviceQueues[queueName] = dq

		_ = appStore.UpsertDeviceQueueState(store.DeviceQueueState{
			QueueName: queueName,
			AgentID:   agentID,
			DeviceIDs: []string{dev.ID},
		})

		// Start the device queue processor goroutine.
		go dq.Run(ctx)

		o.logger.Info().
			Str("queue", queueName).
			Str("agent", ag.Name()).
			Str("device", dev.Name).
			Msg("device queue created for new agent")
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

// Agents returns the agent registry.
func (o *Scheduler) Agents() *agent.Registry {
	return o.agents
}

// ServeHTTP handles ConnectRPC and module HTTP requests.
func (o *Scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ping" {
		o.mu.Lock()
		hasRunning := false
		for _, mp := range o.modules {
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
		// Extract source from X-Mass-Source header (modules set this).
		source := r.Header.Get("X-Mass-Source")
		if source == "" {
			source = "direct"
		}

		// Track the requesting module for idle timeout (keep it alive
		// while its inference request is in-flight).
		if moduleName := strings.TrimPrefix(source, "module:"); moduleName != source {
			o.TrackRequestStart(moduleName)
			defer o.TrackRequestEnd(moduleName)
		}

		// SubmitRequest/GetResult are queue management RPCs — handle directly.
		method := strings.TrimPrefix(r.URL.Path, "/mass.v1.Mass/")
		if method == "SubmitRequest" {
			o.serveSubmitRequest(w, r)
			return
		}
		if method == "GetResult" {
			o.serveGetResult(w, r)
			return
		}

		// Inference RPCs go through the queue for durability and prioritization.
		if o.queue != nil {
			o.serveViaQueue(w, r, method, source)
			return
		}

		// Fallback: direct execution if queue is not initialized.
		resolver := o.newResolver(source)
		defer resolver.ReleaseAll()
		srv := server.NewServer(o.logger, resolver)
		path, handler := rpcconnect.NewMassHandler(srv, connect.WithInterceptors(server.NewMetricsInterceptor()))
		// Strip the path prefix so the handler sees the right URL.
		http.StripPrefix(strings.TrimSuffix(path, "/"), handler).ServeHTTP(w, r)
		return
	}

	// Module API routing — check if the path matches a registered module service.
	o.mu.Lock()
	moduleName, routeFound := o.moduleRoutes[r.URL.Path]
	o.mu.Unlock()
	if routeFound {
		o.serveModuleAPI(w, r, moduleName)
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

// serveModuleAPI handles requests to module-registered API endpoints.
// It ensures the module subprocess is running, reads the JSON body, and
// forwards the call to the module's HandleRequest via gRPC.
func (o *Scheduler) serveModuleAPI(w http.ResponseWriter, r *http.Request, moduleName string) {
	// Extract the short method name from the path (last segment).
	path := r.URL.Path
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		http.Error(w, `{"code":"bad_route","msg":"invalid path"}`, http.StatusBadRequest)
		return
	}
	method := path[idx+1:]

	// Ensure module is running (auto-start on-demand modules).
	if err := o.EnsureRunning(r.Context(), moduleName); err != nil {
		o.logger.Error().Err(err).Str("module", moduleName).Str("method", method).Msg("module API: failed to start module")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		//nolint:errchkjson
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "unavailable",
			"msg":  "module not available: " + err.Error(),
		})
		return
	}

	// Track for idle timeout.
	o.TrackRequestStart(moduleName)
	defer o.TrackRequestEnd(moduleName)

	// Read request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"reading request body"}`, http.StatusInternalServerError)
		return
	}

	// Get the loaded module and call HandleRequest.
	o.mu.Lock()
	mp := o.modules[moduleName]
	o.mu.Unlock()
	if mp == nil || mp.runtime == nil {
		http.Error(w, `{"code":"unavailable","msg":"module not running"}`, http.StatusServiceUnavailable)
		return
	}

	loaded := mp.runtime.GetModule(moduleName)
	if loaded == nil {
		http.Error(w, `{"code":"unavailable","msg":"module not loaded"}`, http.StatusServiceUnavailable)
		return
	}

	result, err := loaded.Module().HandleRequest(r.Context(), method, body)
	if err != nil {
		o.logger.Error().Err(err).Str("module", moduleName).Str("method", method).Msg("module API: HandleRequest error")
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
	_, _ = w.Write(result)
}

// AddStatusCallback registers a callback invoked on module state changes.
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

// AddLogCallback registers a callback invoked for each module stderr log line.
func (o *Scheduler) AddLogCallback(cb LogCallback) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onLog = append(o.onLog, cb)
}

// broadcastLog stores the line in the module's ring buffer and calls all registered log callbacks.
// JSON log lines (e.g. from go-plugin) are reformatted to match zerolog console style.
func (o *Scheduler) broadcastLog(name, line string) {
	line = normalizeLogLine(line)
	o.mu.Lock()
	if mp, ok := o.modules[name]; ok && mp.logBuf != nil {
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
		"@module": true,
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

// SetLogLevel changes the log level for all running modules via RPC.
func (o *Scheduler) SetLogLevel(level string) {
	o.mu.Lock()
	var targets []*ManagedModule
	for _, mp := range o.modules {
		if mp.runtime != nil {
			targets = append(targets, mp)
		}
	}
	o.mu.Unlock()

	for _, mp := range targets {
		o.setModuleLogLevel(mp, level)
	}
}

// setModuleLogLevel pushes the log level to a single module via RPC.
func (o *Scheduler) setModuleLogLevel(mp *ManagedModule, level string) {
	if mp.runtime == nil {
		return
	}
	if loaded := mp.runtime.GetModule(mp.Config.Name); loaded != nil {
		if err := loaded.Module().SetLogLevel(level); err != nil {
			o.logger.Warn().Err(err).Str("module", mp.Config.Name).Msg("failed to set log level on module")
		}
	}
}

// GetLogHistory returns the buffered log lines for a module.
func (o *Scheduler) GetLogHistory(name string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	mp, ok := o.modules[name]
	if !ok || mp.logBuf == nil {
		return nil
	}
	return mp.logBuf.Lines()
}

func (o *Scheduler) notifyStatus(name string, state ModuleState, err error) {
	o.mu.Lock()
	cbs := make([]StatusCallback, len(o.onStatus))
	copy(cbs, o.onStatus)
	o.mu.Unlock()
	for _, cb := range cbs {
		cb(name, state, err)
	}
}

// expandModuleConfig returns a copy of the ModuleConfig with macros expanded
// in the Command and Config fields.
//
// Supported macros:
//   - ${DATA_DIR}    — the MASS data directory
//   - ${MODULES_DIR} — the modules install directory ({DATA_DIR}/modules)
//   - ${MODULE_DIR}  — this module's install directory ({MODULES_DIR}/{name}/{version})
func (o *Scheduler) expandModuleConfig(mc config.ModuleConfig) config.ModuleConfig {
	dataDir, _ := o.cfg.EffectiveDataDir()
	moduleDir := resolveModuleDir(dataDir, mc.Name, mc.Version)
	vars := map[string]string{
		"DATA_DIR":    dataDir,
		"MODULES_DIR": config.ModuleInstallDir(dataDir),
		"MODULE_DIR":  moduleDir,
	}
	mc.Command = config.ExpandCommandVars(mc.Command, vars)
	mc.Config = config.ExpandVars(mc.Config, vars)
	return mc
}

// resolveModuleDir returns the directory for a module, preferring the versioned
// layout ({dataDir}/modules/{name}/{version}) and falling back to the legacy
// flat layout ({dataDir}/modules/{name}) if no version is specified or the
// versioned directory doesn't exist.
func resolveModuleDir(dataDir, name, version string) string {
	if version != "" {
		vDir := config.ModuleVersionDir(dataDir, name, version)
		if _, err := os.Stat(vDir); err == nil {
			return vDir
		}
	}
	// Try to find the latest installed version.
	baseDir := config.ModuleDir(dataDir, name)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return baseDir // fallback to legacy flat layout
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// If any subdirectory contains a module.yml, it's a versioned layout.
		candidate := filepath.Join(baseDir, e.Name())
		if _, err := installer.ReadMetadataFromDir(candidate); err == nil {
			return candidate
		}
	}
	return baseDir
}

// Register reads module.yml from disk and registers the module without
// launching a subprocess. The module stays in StateStopped until Start()
// is called. This is the primary entry point at boot time.
func (o *Scheduler) Register(moduleCfg *config.ModuleConfig) error {
	name := moduleCfg.Name
	log := o.logger.With().Str("module", name).Logger()

	// Read metadata from disk.
	dataDir, err := o.cfg.EffectiveDataDir()
	if err != nil {
		return ctxerr.With(fmt.Errorf("getting data dir: %w", err), map[string]any{"module": name})
	}
	moduleDir := resolveModuleDir(dataDir, name, moduleCfg.Version)
	meta, err := installer.ReadMetadataFromDir(moduleDir)
	if err != nil {
		log.Warn().Err(err).Msg("could not read module.yml, registering with config only")
	}

	// Parse service descriptor for API route registration.
	routes, err := installer.ParseServiceDescriptorFromDir(moduleDir, meta)
	if err != nil {
		log.Warn().Err(err).Msg("could not parse service descriptor")
	}

	o.mu.Lock()
	o.modules[name] = &ManagedModule{
		Config:    moduleCfg,
		DiskMeta:  meta,
		State:     StateStopped,
		APIRoutes: routes,
		logBuf:    newLogRingBuffer(500),
		readyCh:   make(chan struct{}),
	}
	// Index routes for fast lookup.
	for _, r := range routes {
		o.moduleRoutes[r.FullMethod] = name
	}
	o.mu.Unlock()

	version := ""
	uiPath := ""
	if meta != nil {
		version = meta.Version
		uiPath = meta.UIPath
	}
	log.Info().
		Str("version", version).
		Str("ui_path", uiPath).
		Int("api_routes", len(routes)).
		Msg("module registered (subprocess not started)")

	return nil
}

// debugRetryLoop polls for the module's .reattach.json file and connects
// once the debug module process becomes available.
func (o *Scheduler) debugRetryLoop(ctx context.Context, mp *ManagedModule, mgr modules.ModuleRuntimeInterface, modConf config.ModuleConfig) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	moduleName := mp.Config.Name
	o.logger.Info().Str("module", moduleName).Msg("debug mode: waiting for module process (polling every 2s)")

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
			o.notifyStatus(moduleName, StateError, mp.Error)
			return
		case <-ticker.C:
			err := mgr.LoadModule(ctx, modConf)
			if err != nil {
				o.logger.Debug().Err(err).Str("module", moduleName).Msg("debug mode: module not ready yet")
				continue
			}

			loaded := mgr.GetModule(moduleName)
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
			o.notifyStatus(moduleName, StateRunning, nil)

			o.logger.Info().
				Str("module", info.Name).
				Str("version", info.Version).
				Str("ui_path", info.UIPath).
				Msg("debug mode: module connected")

			o.saveFn()
			return
		}
	}
}

// Start launches the module subprocess and loads its models in a background goroutine.
func (o *Scheduler) Start(moduleName string) error {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("module %q not found", moduleName)
	}
	if mp.State == StateRunning || mp.State == StateStarting {
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("module %q is already %s", moduleName, mp.State), map[string]any{"module": moduleName, "state": mp.State.String()})
	}
	mp.State = StateStarting
	mp.Error = nil
	o.mu.Unlock()
	o.notifyStatus(moduleName, StateStarting, nil)

	// Debug mode: poll for an externally-started module process.
	if mp.Config.Debug {
		mgr := o.runtimeFactory(o.logger)
		mgr.SetLogCallback(o.broadcastLog)
		mp.runtime = mgr
		ctx, cancel := context.WithCancel(context.Background())
		mp.cancelFn = cancel
		go o.debugRetryLoop(ctx, mp, mgr, o.expandModuleConfig(*mp.Config))
		return nil
	}

	go o.runStart(mp)

	return nil
}

// runStart executes startFn, updates state, and signals readyCh. Used by both
// Start() and EnsureRunning() goroutines.
func (o *Scheduler) runStart(mp *ManagedModule) {
	moduleName := mp.Config.Name
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
	o.notifyStatus(moduleName, state, err)

	if err != nil {
		o.logger.Error().Err(err).Str("module", moduleName).Msg("module start failed")
	}
}

// launchSubprocess creates a runtime, sets up environment, and starts the
// module's gRPC subprocess. On success it populates mp.runtime and mp.cancelFn.
func (o *Scheduler) launchSubprocess(mp *ManagedModule) error {
	moduleName := mp.Config.Name
	errCtx := map[string]any{"module": moduleName}

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

	resolved := o.expandModuleConfig(*mp.Config)
	if err := mgr.LoadModule(ctx, resolved); err != nil {
		cancel()
		return ctxerr.With(fmt.Errorf("loading module: %w", err), errCtx)
	}

	loaded := mgr.GetModule(moduleName)
	if loaded == nil {
		cancel()
		mgr.Shutdown()
		return ctxerr.With(fmt.Errorf("module loaded but not found in runtime"), errCtx)
	}

	// Push log level to the subprocess.
	if lvl, err := o.cfg.Logger.Level.MarshalText(); err == nil {
		if err := loaded.Module().SetLogLevel(string(lvl)); err != nil {
			o.logger.Warn().Err(err).Str("module", moduleName).Msg("failed to set log level")
		}
	}

	mp.runtime = mgr
	mp.cancelFn = cancel
	return nil
}

// doStart launches the module subprocess and fetches its info.
// Model requirements are stored but not loaded — they are loaded on-demand
// by the resolver when the module makes its first inference request.
func (o *Scheduler) doStart(mp *ManagedModule) error {
	moduleName := mp.Config.Name
	log := o.logger.With().Str("module", moduleName).Logger()

	// --- 1. Launch subprocess ---
	if err := o.launchSubprocess(mp); err != nil {
		return ctxerr.With(fmt.Errorf("launching subprocess for %q: %w", moduleName, err), map[string]any{"module": moduleName})
	}

	// Fetch ModuleInfo from the running subprocess.
	// Model requirements are stored but NOT loaded eagerly — they will be
	// loaded on-demand when the module makes its first inference request.
	if loaded := mp.runtime.GetModule(moduleName); loaded != nil {
		if info, err := loaded.Module().GetInfo(); err == nil {
			mp.Info = info
			// Adopt module's self-declared name.
			if info.Name != "" && info.Name != moduleName {
				loaded.Name = info.Name
				mp.Config.Name = info.Name
				log = o.logger.With().Str("module", info.Name).Logger()
			}
		}
	}

	if mp.Info != nil && len(mp.Info.Models) > 0 {
		log.Info().Int("models", len(mp.Info.Models)).Msg("module declares model requirements (will load on demand)")
	}

	return nil
}

// Stop unloads all models and kills the module subprocess.
func (o *Scheduler) Stop(moduleName string) error {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("module %q not found", moduleName)
	}
	if mp.State != StateRunning && mp.State != StateError {
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("module %q is not running (state: %s)", moduleName, mp.State), map[string]any{"module": moduleName, "state": mp.State.String()})
	}
	mp.State = StateStopping
	// Cancel any pending idle timer.
	if mp.idleTimer != nil {
		mp.idleTimer.Stop()
		mp.idleTimer = nil
	}
	o.mu.Unlock()
	o.notifyStatus(moduleName, StateStopping, nil)

	o.doStop(mp)
	o.broadcastLog(moduleName, formatLogLine("INF", "module stopped by user"))
	o.resetToStopped(mp)

	return nil
}

// resetToStopped transitions a module back to StateStopped after stopping,
// resetting readyCh for future on-demand use.
func (o *Scheduler) resetToStopped(mp *ManagedModule) {
	name := mp.Config.Name
	o.mu.Lock()
	mp.State = StateStopped
	mp.Error = nil
	mp.readyCh = make(chan struct{})
	o.mu.Unlock()
	o.notifyStatus(name, StateStopped, nil)
}

// doStop kills the module subprocess.
// Models loaded on behalf of this module are dynamic and managed by the pool's
// idle timeout — they are not removed here.
func (o *Scheduler) doStop(mp *ManagedModule) {
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

	o.logger.Info().Str("module", mp.Config.Name).Msg("module stopped (subprocess killed)")
}

// Remove kills the module process and removes it from management.
func (o *Scheduler) Remove(moduleName string) {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	o.mu.Unlock()
	if !ok {
		return
	}

	o.doStop(mp)

	o.mu.Lock()
	// Clean up route index.
	for _, r := range mp.APIRoutes {
		delete(o.moduleRoutes, r.FullMethod)
	}
	delete(o.modules, moduleName)
	o.mu.Unlock()
}

// ShutdownAll stops the queue processor and all running modules.
func (o *Scheduler) ShutdownAll() {
	o.mu.Lock()
	names := make([]string, 0, len(o.modules))
	for name, mp := range o.modules {
		names = append(names, name)
		if mp.idleTimer != nil {
			mp.idleTimer.Stop()
			mp.idleTimer = nil
		}
	}
	o.mu.Unlock()

	for _, name := range names {
		o.mu.Lock()
		mp := o.modules[name]
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

// Runtime returns the module's runtime, or nil if not running.
func (mp *ManagedModule) Runtime() modules.ModuleRuntimeInterface {
	return mp.runtime
}

// GetModule returns current state of a module.
func (o *Scheduler) GetModule(name string) *ManagedModule {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.modules[name]
}

// GetAllModules returns all managed modules.
func (o *Scheduler) GetAllModules() map[string]*ManagedModule {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[string]*ManagedModule, len(o.modules))
	for k, v := range o.modules {
		result[k] = v
	}
	return result
}

// PoolSnapshot returns metadata about all loaded model instances in the pool.
func (o *Scheduler) PoolSnapshot() []ModelInstanceInfo {
	return o.pool.Snapshot()
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
			queueName = DeviceQueueName(info.AgentID, info.DeviceIDs[0])
		} else {
			queueName = DeviceGroupQueueName(info.AgentID, info.DeviceIDs)
		}
		_ = o.dispatcher.stateStore.UpdateLoadedHash(queueName, "")
	}
	return true
}

// SetDeviceQueueEnabled enables or disables a device queue for scheduling.
// When disabling, any pending tasks in the queue are drained back to the global
// queue for redistribution. Returns the number of drained tasks.
func (o *Scheduler) SetDeviceQueueEnabled(ctx context.Context, queueName string, enabled bool) (int, error) {
	o.mu.Lock()
	dq, ok := o.deviceQueues[queueName]
	o.mu.Unlock()
	if !ok {
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

// LoadModel loads a model into the pool as a user-managed static instance.
// modelType must be "chat" or "embedding". Returns the fingerprint of the loaded instance.
func (o *Scheduler) LoadModel(modelType string, chatCfg *config.ChatModelConfig, embedCfg *config.EmbeddingModelConfig, userPlacement config.PlacementConfig, source string) (string, error) {
	// Validate config and compute fingerprint.
	var fp string
	switch modelType {
	case "chat":
		if chatCfg == nil {
			return "", fmt.Errorf("chat config is required")
		}
		if err := chatCfg.Validate(); err != nil {
			return "", fmt.Errorf("invalid chat config: %w", err)
		}
		fp = config.ChatModelFingerprint(*chatCfg)
	case "embedding":
		if embedCfg == nil {
			return "", fmt.Errorf("embedding config is required")
		}
		if err := embedCfg.Validate(); err != nil {
			return "", fmt.Errorf("invalid embedding config: %w", err)
		}
		fp = config.EmbeddingModelFingerprint(*embedCfg)
	default:
		return "", ctxerr.With(fmt.Errorf("unknown model type: %s", modelType), map[string]any{"model_type": modelType})
	}

	// If model is already loaded in pool, return existing fingerprint.
	if o.pool.HasChat(fp) || o.pool.HasEmbedding(fp) {
		return fp, nil
	}

	// Singleflight: if multiple callers request the same fingerprint concurrently
	// (e.g. batch requests), only the first one loads; others wait and reuse.
	result, err, _ := o.loadGroup.Do(fp, func() (any, error) {
		return o.doLoadModel(modelType, chatCfg, embedCfg, userPlacement, source)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// doLoadModel performs the actual model loading — device selection, placement
// computation, and agent dispatch. Called via singleflight to dedup concurrent
// loads of the same fingerprint.
func (o *Scheduler) doLoadModel(modelType string, chatCfg *config.ChatModelConfig, embedCfg *config.EmbeddingModelConfig, userPlacement config.PlacementConfig, source string) (string, error) {
	// Derive fingerprint and model path from configs.
	var modelPath string
	var fp string
	switch modelType {
	case "chat":
		modelPath = chatCfg.Path
		fp = config.ChatModelFingerprint(*chatCfg)
	case "embedding":
		modelPath = embedCfg.Path
		fp = config.EmbeddingModelFingerprint(*embedCfg)
	}

	// Double-check after singleflight dedup (previous caller may have loaded it).
	if o.pool.HasChat(fp) || o.pool.HasEmbedding(fp) {
		return fp, nil
	}

	// Determine context size, cache type, and auxiliary files for VRAM estimation.
	var contextSize int32
	var cacheType string
	var auxPaths []string
	if chatCfg != nil {
		contextSize = chatCfg.ContextSize
		cacheType = chatCfg.CacheType
		if chatCfg.MmprojPath != "" {
			auxPaths = append(auxPaths, chatCfg.MmprojPath)
		}
	} else if embedCfg != nil {
		contextSize = embedCfg.ContextSize
	}

	// tryLoad attempts to load the model on the given candidate.
	// Returns the fingerprint on success, or an error.
	tryLoad := func(candidate *Candidate) (string, error) {
		ag := o.agents.Get(candidate.AgentID)
		if ag == nil {
			return "", ctxerr.With(fmt.Errorf("agent %s not found", candidate.AgentID), map[string]any{"agent_id": candidate.AgentID, "queue": candidate.QueueName})
		}

		placement := o.computePlacement(userPlacement, candidate, modelPath, contextSize, cacheType, auxPaths)

		o.logger.Info().
			Str("fingerprint", fp).
			Str("path", modelPath).
			Str("agent", candidate.AgentID).
			Str("device_queue", candidate.QueueName).
			Int32("gpu_layers", placement.GpuLayers).
			Int32("max_concurrent", placement.MaxConcurrent).
			Str("tensor_split", placement.TensorSplit).
			Msg("loading model via scheduler placement")

		switch modelType {
		case "chat":
			model, err := ag.LoadChatModel(o.logger, "", *chatCfg, placement)
			if err != nil {
				return "", err
			}
			fp = o.pool.RegisterChat(source, "", *chatCfg, placement, model, ag.ID(), ag.Name(), candidate.DeviceIDs...)
		case "embedding":
			model, err := ag.LoadEmbeddingModel(o.logger, "", *embedCfg, placement)
			if err != nil {
				return "", err
			}
			fp = o.pool.RegisterEmbedding(source, "", *embedCfg, placement, model, ag.ID(), ag.Name(), candidate.DeviceIDs...)
		}

		if o.dispatcher != nil {
			_ = o.dispatcher.stateStore.UpdateLoadedHash(candidate.QueueName, fp)
		}
		return fp, nil
	}

	// Placement priority for manual loads:
	// 1. Best free local device (no eviction)
	// 2. Best free remote device (no eviction)
	// 3. Evict from the best device anywhere (prefer strongest by GFlops)
	//
	// If loading fails on a candidate (e.g. config incompatibility with the
	// device), we log the error and try the next placement option.
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
			return o.dispatcher.selectPlacement(fp)
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
			Str("agent", candidate.AgentID).
			Str("queue", candidate.QueueName).
			Msg("placement failed, trying next option")
		lastErr = err
	}

	if lastErr != nil {
		return "", ctxerr.With(fmt.Errorf("all placement options failed: %w", lastErr), map[string]any{"model_type": modelType, "model_path": modelPath})
	}
	return "", ctxerr.With(fmt.Errorf("no device available for model (all offline or insufficient resources)"), map[string]any{"model_type": modelType, "model_path": modelPath})
}

// computePlacement fills in auto-calculated placement fields where the user
// hasn't provided overrides.
func (o *Scheduler) computePlacement(user config.PlacementConfig, candidate *Candidate, modelPath string, contextSize int32, cacheType string, auxPaths []string) config.PlacementConfig {
	result := user

	// Model file size for calculations — includes auxiliary files (e.g. mmproj)
	// that also consume VRAM.
	modelSize, _ := ModelFileSize(modelPath)
	for _, p := range auxPaths {
		if s, err := ModelFileSize(p); err == nil {
			modelSize += s
		}
	}
	modelSizeMB := int(modelSize / (1024 * 1024))
	modelSizeGB := float64(modelSize) / (1024 * 1024 * 1024)

	// Tensor split: auto if multi-device and user didn't specify.
	if result.TensorSplit == "" && len(candidate.DeviceIDs) > 1 {
		// Build device infos from agent's device list for memory info.
		ag := o.agents.Get(candidate.AgentID)
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

	// Max concurrent: auto if user didn't specify.
	if result.MaxConcurrent == 0 && modelSizeGB > 0 {
		kvCacheMB := int(EstimateKVCacheMB(modelSize, contextSize, cacheType))
		result.MaxConcurrent = CalcMaxConcurrent(
			candidate.GFlops, modelSizeGB,
			candidate.TotalMemoryMB, modelSizeMB,
			kvCacheMB,
		)
	}

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

// executeEnvelope processes a queue envelope by creating a resolver, dispatching
// to the server, and cleaning up the resolver afterward.
func (o *Scheduler) executeEnvelope(ctx context.Context, env queue.Envelope) ([]byte, error) {
	source := env.Source
	if source == "" {
		source = "direct"
	}
	resolver := o.newResolver(source)
	defer resolver.ReleaseAll()

	srv := server.NewServer(o.logger, resolver)

	switch env.Type {
	case queue.RequestTypeChatCompletion:
		return srv.ExecuteChatCompletion(ctx, env.Payload)
	case queue.RequestTypeBatchChatCompletion:
		return srv.ExecuteBatchChatCompletion(ctx, env.Payload)
	case queue.RequestTypeEmbedding:
		return srv.ExecuteEmbedding(ctx, env.Payload)
	case queue.RequestTypeBatchEmbedding:
		return srv.ExecuteBatchEmbedding(ctx, env.Payload)
	case queue.RequestTypeTokenize:
		return srv.ExecuteTokenize(ctx, env.Payload)
	default:
		return nil, ctxerr.With(fmt.Errorf("unknown request type: %d", env.Type), map[string]any{"type": env.Type, "source": source})
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

// extractFingerprint parses the proto body to compute the model config fingerprint for queue dispatch.
// Returns empty string if no model config is present (e.g. model specified by name).
func (o *Scheduler) extractFingerprint(reqType queue.RequestType, body []byte) string {
	modelsDir := o.modelsDir()
	switch reqType {
	case queue.RequestTypeChatCompletion:
		req := &rpc.ChatCompletionRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return ""
		}
		if mc := req.ModelConfig; mc != nil {
			if lc := mc.GetLlama(); lc != nil {
				cfg, _ := llamaChatToChatConfig(lc, modelsDir)
				return config.ChatModelFingerprint(cfg)
			}
		}
		if req.Model != "" {
			if path, err := config.ResolveModelPath(req.Model, modelsDir); err == nil {
				return config.ChatModelFingerprint(config.ChatModelConfig{Path: path})
			}
		}
	case queue.RequestTypeBatchChatCompletion:
		req := &rpc.BatchChatCompletionRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return ""
		}
		if len(req.Requests) > 0 {
			if mc := req.Requests[0].ModelConfig; mc != nil {
				if lc := mc.GetLlama(); lc != nil {
					cfg, _ := llamaChatToChatConfig(lc, modelsDir)
					return config.ChatModelFingerprint(cfg)
				}
			}
			if req.Requests[0].Model != "" {
				if path, err := config.ResolveModelPath(req.Requests[0].Model, modelsDir); err == nil {
					return config.ChatModelFingerprint(config.ChatModelConfig{Path: path})
				}
			}
		}
	case queue.RequestTypeEmbedding:
		req := &rpc.EmbeddingRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return ""
		}
		if mc := req.ModelConfig; mc != nil {
			if lc := mc.GetLlama(); lc != nil {
				cfg, _ := llamaEmbeddingToEmbeddingConfig(lc, modelsDir)
				return config.EmbeddingModelFingerprint(cfg)
			}
		}
		if req.Model != "" {
			if path, err := config.ResolveModelPath(req.Model, modelsDir); err == nil {
				return config.EmbeddingModelFingerprint(config.EmbeddingModelConfig{Path: path})
			}
		}
	case queue.RequestTypeBatchEmbedding:
		req := &rpc.BatchEmbeddingRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return ""
		}
		if mc := req.ModelConfig; mc != nil {
			if lc := mc.GetLlama(); lc != nil {
				cfg, _ := llamaEmbeddingToEmbeddingConfig(lc, modelsDir)
				return config.EmbeddingModelFingerprint(cfg)
			}
		}
		if req.Model != "" {
			if path, err := config.ResolveModelPath(req.Model, modelsDir); err == nil {
				return config.EmbeddingModelFingerprint(config.EmbeddingModelConfig{Path: path})
			}
		}
	case queue.RequestTypeTokenize:
		req := &rpc.TokenizeRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			return ""
		}
		if mc := req.ModelConfig; mc != nil {
			if lc := mc.GetLlama(); lc != nil {
				cfg, _ := llamaChatToChatConfig(lc, modelsDir)
				return config.ChatModelFingerprint(cfg)
			}
		}
		if req.Model != "" {
			if path, err := config.ResolveModelPath(req.Model, modelsDir); err == nil {
				return config.ChatModelFingerprint(config.ChatModelConfig{Path: path})
			}
		}
	}
	return ""
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

	// Streaming requests bypass the queue — tokens must be delivered in real time.
	if method == "ChatCompletion" {
		req := &rpc.ChatCompletionRequest{}
		if err := proto.Unmarshal(body, req); err == nil && req.Stream {
			o.serveChatStream(w, r, req, source)
			return
		}
	}

	// Extract fingerprint for queue dispatch scheduling.
	fp := o.extractFingerprint(reqType, body)

	// Submit to queue.
	sub, err := o.queue.SubmitRaw(r.Context(), reqType, body, source, fp, queue.PriorityMedium)
	if err != nil {
		o.logger.Error().Err(err).Str("method", method).Msg("failed to enqueue request")
		http.Error(w, `{"code":"internal","msg":"enqueue failed"}`, http.StatusInternalServerError)
		return
	}

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
					_, _ = w.Write(jsonBytes)
					return
				}
			}
		}
		// Fall through to protobuf on conversion failure.
	}
	w.Header().Set("Content-Type", "application/protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// serveChatStream handles streaming chat completion requests.
// It bypasses the queue and runs inference directly, writing SSE events as tokens arrive.
func (o *Scheduler) serveChatStream(w http.ResponseWriter, r *http.Request, req *rpc.ChatCompletionRequest, source string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"code":"internal","msg":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	resolver := o.newResolver(source)
	defer resolver.ReleaseAll()

	model, modelName, err := resolver.ResolveChat(req)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, `{"code":"invalid_argument","msg":"at least one message is required"}`, http.StatusBadRequest)
		return
	}

	messages := server.ProtoToMessages(req.Messages)

	completionReq := server.RPCToCompletionRequest(messages, req)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	requestID := uuid.NewString()

	deltaCh, errCh := model.SubmitStream(r.Context(), completionReq)

	var finalUsage *streamUsage
	for delta := range deltaCh {
		if delta.Usage != nil {
			finalUsage = &streamUsage{
				PromptTokens:     delta.Usage.PromptTokens,
				CompletionTokens: delta.Usage.CompletionTokens,
				TotalTokens:      delta.Usage.TotalTokens,
				TokensPerSecond:  delta.Usage.TokensPerSecond,
			}
			continue
		}
		chunk := streamChunk{
			ID:    requestID,
			Model: modelName,
			Delta: streamDelta{
				Content:          delta.Content,
				ReasoningContent: delta.ReasoningContent,
			},
		}
		//nolint:errchkjson // streamChunk is a simple struct that cannot fail to marshal
		data, _ := json.Marshal(chunk)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Check for stream error.
	select {
	case err := <-errCh:
		if err != nil {
			//nolint:errchkjson // map[string]string cannot fail to marshal
			errData, _ := json.Marshal(map[string]string{"error": err.Error()})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", errData)
			flusher.Flush()
			return
		}
	default:
	}

	// Send final chunk with usage before [DONE].
	if finalUsage != nil {
		//nolint:errchkjson
		data, _ := json.Marshal(streamChunk{
			ID:    requestID,
			Model: modelName,
			Usage: finalUsage,
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Send the [DONE] sentinel.
	_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// streamChunk is the SSE JSON payload for a streaming chat delta.
type streamChunk struct {
	ID    string       `json:"id"`
	Model string       `json:"model"`
	Delta streamDelta  `json:"delta,omitempty"`
	Usage *streamUsage `json:"usage,omitempty"`
}

type streamDelta struct {
	Content          string `json:"content,omitempty"`
	ReasoningContent string `json:"reasoning_content,omitempty"`
}

type streamUsage struct {
	PromptTokens     int     `json:"promptTokens"`
	CompletionTokens int     `json:"completionTokens"`
	TotalTokens      int     `json:"totalTokens"`
	TokensPerSecond  float64 `json:"tokensPerSecond"`
}

// serveSubmitRequest handles the async SubmitRequest RPC at the scheduler level.
// It determines the request type from the oneof, submits to the queue, and returns the ID.
func (o *Scheduler) serveSubmitRequest(w http.ResponseWriter, r *http.Request) {
	if o.queue == nil || o.results == nil {
		http.Error(w, `{"code":"unavailable","msg":"queue not initialized"}`, http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"reading request body"}`, http.StatusInternalServerError)
		return
	}

	wantJSON := isJSONContentType(r)

	req := &rpc.SubmitRequestRequest{}
	if wantJSON {
		body = bytes.ToValidUTF8(body, []byte("\uFFFD"))
		if err := protojson.Unmarshal(body, req); err != nil {
			http.Error(w, `{"code":"malformed","msg":"invalid JSON request"}`, http.StatusBadRequest)
			return
		}
	} else {
		if err := proto.Unmarshal(body, req); err != nil {
			http.Error(w, `{"code":"malformed","msg":"invalid protobuf"}`, http.StatusBadRequest)
			return
		}
	}

	var reqType queue.RequestType
	var payload []byte

	switch v := req.Request.(type) {
	case *rpc.SubmitRequestRequest_ChatCompletion:
		reqType = queue.RequestTypeChatCompletion
		payload, err = proto.Marshal(v.ChatCompletion)
	case *rpc.SubmitRequestRequest_BatchChatCompletion:
		reqType = queue.RequestTypeBatchChatCompletion
		payload, err = proto.Marshal(v.BatchChatCompletion)
	case *rpc.SubmitRequestRequest_Embedding:
		reqType = queue.RequestTypeEmbedding
		payload, err = proto.Marshal(v.Embedding)
	case *rpc.SubmitRequestRequest_BatchEmbedding:
		reqType = queue.RequestTypeBatchEmbedding
		payload, err = proto.Marshal(v.BatchEmbedding)
	case *rpc.SubmitRequestRequest_Tokenize:
		reqType = queue.RequestTypeTokenize
		payload, err = proto.Marshal(v.Tokenize)
	default:
		http.Error(w, `{"code":"invalid_argument","msg":"request oneof is required"}`, http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"serializing inner request"}`, http.StatusInternalServerError)
		return
	}

	fp := o.extractFingerprint(reqType, payload)
	priority := queue.Priority(req.Priority)
	sub, err := o.queue.SubmitRaw(r.Context(), reqType, payload, "direct", fp, priority)
	if err != nil {
		o.logger.Error().Err(err).Msg("failed to enqueue async request")
		http.Error(w, `{"code":"internal","msg":"enqueue failed"}`, http.StatusInternalServerError)
		return
	}

	if err := o.results.Create(sub.ID, sub.RequestHash); err != nil {
		o.logger.Error().Err(err).Str("request_id", sub.ID).Msg("failed to create result entry")
		http.Error(w, `{"code":"internal","msg":"result tracking failed"}`, http.StatusInternalServerError)
		return
	}

	resp := &rpc.SubmitRequestResponse{
		RequestId:   sub.ID,
		RequestHash: sub.RequestHash,
	}
	writeProtoResponse(w, resp, wantJSON)
}

// serveGetResult handles the async GetResult RPC at the scheduler level.
// It looks up the result by ID and optionally blocks until completion.
func (o *Scheduler) serveGetResult(w http.ResponseWriter, r *http.Request) {
	if o.results == nil {
		http.Error(w, `{"code":"unavailable","msg":"queue not initialized"}`, http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"reading request body"}`, http.StatusInternalServerError)
		return
	}

	wantJSON := isJSONContentType(r)

	req := &rpc.GetResultRequest{}
	if wantJSON {
		body = bytes.ToValidUTF8(body, []byte("\uFFFD"))
		if err := protojson.Unmarshal(body, req); err != nil {
			http.Error(w, `{"code":"malformed","msg":"invalid JSON request"}`, http.StatusBadRequest)
			return
		}
	} else {
		if err := proto.Unmarshal(body, req); err != nil {
			http.Error(w, `{"code":"malformed","msg":"invalid protobuf"}`, http.StatusBadRequest)
			return
		}
	}

	if req.RequestId == "" {
		http.Error(w, `{"code":"invalid_argument","msg":"request_id is required"}`, http.StatusBadRequest)
		return
	}

	var result *queue.Result
	if req.Wait {
		result, err = o.results.WaitForResult(req.RequestId, 50*time.Millisecond, r.Context().Done())
		if err != nil {
			http.Error(w, `{"code":"internal","msg":"request cancelled or timed out"}`, http.StatusServiceUnavailable)
			return
		}
	} else {
		result, err = o.results.Get(req.RequestId)
		if err != nil {
			http.Error(w, `{"code":"internal","msg":"looking up result"}`, http.StatusInternalServerError)
			return
		}
		if result == nil {
			http.Error(w, `{"code":"not_found","msg":"result not found"}`, http.StatusNotFound)
			return
		}
	}

	resp := &rpc.GetResultResponse{
		Status: string(result.Status),
		Body:   result.Body,
		Error:  result.Error,
	}
	writeProtoResponse(w, resp, wantJSON)
}

// writeProtoResponse marshals a proto message as JSON or protobuf and writes it to the response.
func writeProtoResponse(w http.ResponseWriter, msg proto.Message, asJSON bool) {
	if asJSON {
		out, err := protojson.Marshal(msg)
		if err != nil {
			http.Error(w, `{"code":"internal","msg":"marshalling response"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)
		return
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		http.Error(w, `{"code":"internal","msg":"marshalling response"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// --- On-demand module loading ---

// EnsureRunning ensures a module is running. If the module is in StateStopped,
// it starts the module and blocks until ready. If already starting, it waits.
func (o *Scheduler) EnsureRunning(ctx context.Context, moduleName string) error {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("module %q not found", moduleName)
	}

	switch mp.State {
	case StateRunning:
		o.mu.Unlock()
		return nil
	case StateStarting:
		// Another caller or manual start triggered it — wait.
		readyCh := mp.readyCh
		o.mu.Unlock()
		return o.waitForReady(ctx, moduleName, readyCh)
	case StateStopped:
		readyCh := mp.readyCh
		o.mu.Unlock()
		// Reuse Start() which sets StateStarting, fires notifyStatus, and
		// launches runStart in a goroutine.
		if err := o.Start(moduleName); err != nil {
			return err
		}
		return o.waitForReady(ctx, moduleName, readyCh)
	default:
		state := mp.State
		o.mu.Unlock()
		return ctxerr.With(fmt.Errorf("module %q is in state %s, cannot start on demand", moduleName, state), map[string]any{"module": moduleName, "state": state.String()})
	}
}

// waitForReady blocks until the module's readyCh is closed or the context is cancelled.
func (o *Scheduler) waitForReady(ctx context.Context, name string, readyCh <-chan struct{}) error {
	select {
	case <-readyCh:
		o.mu.Lock()
		mp := o.modules[name]
		o.mu.Unlock()
		if mp == nil {
			return fmt.Errorf("module %q disappeared", name)
		}
		if mp.State == StateError {
			return ctxerr.With(fmt.Errorf("module %q failed to start: %w", name, mp.Error), map[string]any{"module": name})
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrackRequestStart increments the active request count and cancels any idle timer.
func (o *Scheduler) TrackRequestStart(moduleName string) {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
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
// and the module is on-demand, starts the idle timer.
func (o *Scheduler) TrackRequestEnd(moduleName string) {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	if !ok {
		o.mu.Unlock()
		return
	}
	newCount := atomic.AddInt64(&mp.activeReqs, -1)
	if newCount <= 0 && mp.Config.EffectiveLaunchMode() == config.LaunchModeOnDemand && mp.State == StateRunning {
		timeout := o.cfg.EffectiveModuleIdleTimeout()
		mp.idleTimer = time.AfterFunc(timeout, func() {
			o.idleStop(moduleName)
		})
	}
	o.mu.Unlock()
}

// ArmIdleTimer starts the idle timer for an on-demand module if it has no
// active requests. Called after UI-triggered starts so the module eventually
// stops if no real requests arrive.
func (o *Scheduler) ArmIdleTimer(moduleName string) {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
	if !ok || mp.State != StateRunning || mp.Config.EffectiveLaunchMode() != config.LaunchModeOnDemand {
		o.mu.Unlock()
		return
	}
	if atomic.LoadInt64(&mp.activeReqs) > 0 || mp.idleTimer != nil {
		o.mu.Unlock()
		return
	}
	timeout := o.cfg.EffectiveModuleIdleTimeout()
	mp.idleTimer = time.AfterFunc(timeout, func() {
		o.idleStop(moduleName)
	})
	o.mu.Unlock()
}

// idleStop stops an on-demand module after its idle timeout expires.
func (o *Scheduler) idleStop(moduleName string) {
	o.mu.Lock()
	mp, ok := o.modules[moduleName]
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
	o.notifyStatus(moduleName, StateStopping, nil)

	o.doStop(mp)
	o.broadcastLog(moduleName, formatLogLine("INF", "on-demand module stopped after idle timeout"))
	o.resetToStopped(mp)

	o.logger.Info().Str("module", moduleName).Msg("on-demand module stopped after idle timeout")
}

// onDemandModulesWithModels returns names of on-demand modules in StateStopped.
// These modules will be started when the inference pool is empty and a request arrives.
func (o *Scheduler) onDemandModulesWithModels() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var names []string
	for name, mp := range o.modules {
		if mp.State == StateStopped &&
			mp.Config.EffectiveLaunchMode() == config.LaunchModeOnDemand {
			names = append(names, name)
		}
	}
	return names
}

// trackOnDemandModules calls TrackRequestStart for all running on-demand modules
// and returns their names (for deferred TrackRequestEnd).
func (o *Scheduler) trackOnDemandModules() []string {
	o.mu.Lock()
	var names []string
	for name, mp := range o.modules {
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
