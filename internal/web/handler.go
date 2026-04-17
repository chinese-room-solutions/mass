package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	"github.com/chinese-room-solutions/mass-proto/gen/go/worker/workerconnect"
	sdkhf "github.com/chinese-room-solutions/mass-sdk/huggingface"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/huggingface"
	"github.com/chinese-room-solutions/mass/internal/installer"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/starfederation/datastar-go/datastar"
)

// downloadState tracks an in-progress model download for pause/resume/cancel.
type downloadState struct {
	RepoID    string
	Filename  string
	GroupName string // server-computed display name for the model group

	Downloaded  int64 // current bytes (accessed under mu)
	Total       int64 // total bytes
	Paused      bool
	cancelFn    context.CancelFunc
	lastPersist time.Time // throttle DB writes
	mu          sync.Mutex
}

// Handler serves the MASS web UI and API.
type Handler struct {
	cfg       *config.Config
	orch      *scheduler.Scheduler
	installer *installer.Installer
	store     store.StoreInterface
	saveFn    func() // persists current config to disk
	logger    zerolog.Logger
	broker    *SSEBroker
	mux       *http.ServeMux
	hfMu      sync.Mutex
	hfState   *hfSearchState // server-side HF search pagination (Models tab)
	dlMu      sync.RWMutex
	downloads map[string]*downloadState // key: filename

	authHashMu sync.RWMutex
	authHash   []byte        // bcrypt hash of the auth token (nil = no auth)
	sessions   *SessionStore // browser session management

	sysLog *SystemLogBuffer // MASS system log ring buffer (nil = disabled)

	workers       *worker.Fleet
	onThemeChange func(dark bool) // optional: update native window title bar theme
}

// HandlerOptions bundles the dependencies needed to construct a Handler.
// All fields are required unless noted otherwise.
type HandlerOptions struct {
	Config    *config.Config
	Scheduler *scheduler.Scheduler
	Installer *installer.Installer
	SaveFn    func() // persists current config to disk
	Logger    zerolog.Logger
	Store     store.StoreInterface
	AuthHash  []byte // bcrypt hash of the auth token; nil disables auth
	Sessions  *SessionStore
	SysLog    *SystemLogBuffer // nil disables system log streaming
	Workers   *worker.Fleet
}

// NewHandler creates the main HTTP handler for the MASS web UI.
func NewHandler(opts HandlerOptions) (*Handler, error) {
	// Migrate legacy "owner--repo" model directories to "owner/repo" structure.
	if dataDir, err := opts.Config.EffectiveDataDir(); err == nil {
		MigrateModelDirs(config.ModelsDir(dataDir), opts.Logger)
	}

	h := &Handler{
		cfg:       opts.Config,
		orch:      opts.Scheduler,
		installer: opts.Installer,
		store:     opts.Store,
		saveFn:    opts.SaveFn,
		logger:    opts.Logger,
		broker:    NewSSEBroker(opts.Logger),
		downloads: make(map[string]*downloadState),
		authHash:  opts.AuthHash,
		sessions:  opts.Sessions,
		sysLog:    opts.SysLog,
		workers:   opts.Workers,
	}
	orch := opts.Scheduler
	sysLog := opts.SysLog
	workers := opts.Workers

	// Wire scheduler callbacks to SSE broker.
	orch.AddStatusCallback(func(name string, state scheduler.AppState, err error) {
		h.broker.Broadcast(SSEEvent{
			Type:    EventTypeStatus,
			AppName: name,
			State:   state,
			Error:   err,
		})
	})
	orch.AddLogCallback(func(name, line string) {
		if evt, ok := parseDownloadLine(name, line); ok {
			h.broker.Broadcast(evt)
			return
		}
		h.broker.Broadcast(SSEEvent{
			Type:    EventTypeLog,
			AppName: name,
			LogLine: line,
		})
	})

	// Wire model pool changes to SSE broker.
	orch.AddPoolChangeCallback(func(evt scheduler.PoolChangeEvent) {
		h.broker.Broadcast(SSEEvent{
			Type:        EventTypePoolChange,
			PoolChange:  evt.Kind,
			Fingerprint: evt.Fingerprint,
		})
	})

	// Wire system log buffer to SSE broker.
	if sysLog != nil {
		go func() {
			ch := sysLog.Subscribe()
			for line := range ch {
				h.broker.Broadcast(SSEEvent{
					Type:       EventTypeSystemLog,
					SysLogLine: line,
				})
			}
		}()
	}

	// Wire worker fleet changes to SSE broker.
	workers.AddChangeCallback(func(evt worker.FleetChangeEvent) {
		h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
		// Auto-benchmark newly connected workers' unbenchmarked devices.
		if evt.Kind == worker.FleetChangeAdded {
			go h.autoBenchmarkWorker(evt.WorkerID)
		}
	})

	// Periodic agent stats refresh for live utilization gauges.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerStats})
		}
	}()

	// Auto-benchmark devices on workers registered before callbacks were wired
	// (e.g. the local worker). Remote workers trigger via FleetChangeAdded.
	go func() {
		for _, ag := range workers.All() {
			h.autoBenchmarkWorker(ag.ID())
		}
	}()

	// Restore persisted downloads from the database. Downloads whose temp
	// files no longer exist on disk are cleaned up from the DB.
	h.loadPersistedDownloads()

	mux, err := h.buildMux()
	if err != nil {
		return nil, fmt.Errorf("building mux: %w", err)
	}
	h.mux = mux
	return h, nil
}

// loadPersistedDownloads restores download state from the database on startup.
// Downloads whose temp files no longer exist on disk have their DB rows removed.
// Surviving downloads are loaded as paused so the user can resume them manually.
func (h *Handler) loadPersistedDownloads() {
	if h.store == nil {
		return
	}
	rows, err := h.store.ListDownloads()
	if err != nil {
		h.logger.Error().Err(err).Msg("loading persisted downloads")
		return
	}

	dir := h.modelsDir()
	for _, row := range rows {
		// Check if the temp file still exists on disk.
		tempPath := huggingface.TempFilePath(row.RepoID, row.Filename, dir)
		if _, err := os.Stat(tempPath); err != nil {
			// Also check if the final file exists (download completed while DB wasn't updated).
			finalPath := filepath.Join(dir, sdkhf.SanitizeRepoID(row.RepoID), row.Filename)
			if _, err2 := os.Stat(finalPath); err2 != nil {
				h.logger.Info().Str("file", row.Filename).Msg("removing stale download DB entry (temp file missing)")
			} else {
				h.logger.Info().Str("file", row.Filename).Msg("removing stale download DB entry (already completed)")
			}
			if err := h.store.DeleteDownload(row.Filename); err != nil {
				h.logger.Warn().Err(err).Str("file", row.Filename).Msg("failed to delete stale download record")
			}
			continue
		}

		// Restore as paused — user must explicitly resume.
		ds := &downloadState{
			RepoID:     row.RepoID,
			Filename:   row.Filename,
			GroupName:  row.GroupName,
			Downloaded: row.Downloaded,
			Total:      row.Total,
			Paused:     true,
			cancelFn:   func() {}, // no-op until resumed
		}
		h.dlMu.Lock()
		h.downloads[row.Filename] = ds
		h.dlMu.Unlock()

		h.logger.Info().
			Str("file", row.Filename).
			Int64("downloaded", row.Downloaded).
			Int64("total", row.Total).
			Msg("restored persisted download (paused)")

		// Ensure DB status reflects paused state.
		if row.Status != "paused" {
			if err := h.store.SetStatus(row.Filename, "paused"); err != nil {
				h.logger.Warn().Err(err).Str("file", row.Filename).Msg("failed to update download status")
			}
		}
	}
}

func (h *Handler) buildMux() (*http.ServeMux, error) {
	mux := http.NewServeMux()

	// Static files (embedded in binary, overridable via MASS_PUBLIC_DIR).
	pfs, err := publicFS()
	if err != nil {
		return nil, err
	}
	mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServer(pfs)))

	// Page routes.
	mux.HandleFunc("GET /", h.handlePageDashboard)
	mux.HandleFunc("GET /login", h.handlePageLogin)
	mux.HandleFunc("POST /login", h.handlePostLogin)
	// --- /internal/ — UI-only plumbing: SSE event streams, HTML/Datastar
	// fragments, binary assets, browser state toggles. NOT part of the
	// public mass.v1 contract; can change shape without notice. External
	// callers should use ConnectRPC under /mass.v1.Mass/* instead.

	// SSE event streams.
	mux.HandleFunc("GET /internal/events", h.handleSSEEvents)
	mux.HandleFunc("GET /internal/sync-logs", h.handleSyncLogs)

	// App UI (icons, log streaming, deselection — purely browser concerns).
	mux.HandleFunc("GET /internal/apps/deselect", h.handleDeselectApp)
	mux.HandleFunc("GET /internal/apps/{name}/icon", h.handleAppIcon)
	mux.HandleFunc("GET /internal/apps/{name}/logs", h.handleAppRenderLogs)

	// Settings UI toggle.
	mux.HandleFunc("POST /internal/settings/theme", h.handleToggleTheme)

	// Scheduler tab UI (Datastar HTML rendering).
	mux.HandleFunc("GET /internal/scheduler", h.handleSchedulerInstances)
	mux.HandleFunc("GET /internal/scheduler/info", h.handleSchedulerInstanceInfo)

	// Workers benchmark progress (SSE wrapper around RunBenchmark).
	mux.HandleFunc("POST /internal/workers/benchmark", h.handleBenchmarkSSE)

	// --- /api/* — management endpoints, transitional (Phase 4 migrates
	// the dual-implemented ones to ConnectRPC). New external callers
	// should prefer ConnectRPC.

	// App lifecycle.
	mux.HandleFunc("POST /api/apps/install", h.handleInstallApp)
	mux.HandleFunc("POST /api/apps/{name}/start", h.handleStartApp)
	mux.HandleFunc("POST /api/apps/{name}/stop", h.handleStopApp)
	mux.HandleFunc("DELETE /api/apps/{name}", h.handleRemoveApp)
	mux.HandleFunc("POST /api/apps/{name}/launch-mode", h.handleSetLaunchMode)
	mux.HandleFunc("POST /api/apps/{name}/auto-start", h.handleToggleAutoStart)
	mux.HandleFunc("POST /api/apps/{name}/debug", h.handleToggleDebug)

	// Settings update.
	mux.HandleFunc("POST /api/settings", h.handleUpdateSettings)

	// Workers / devices (UI plumbing — rendered HTML cards + toggle-shape).
	mux.HandleFunc("GET /internal/workers", h.handleListWorkers)
	mux.HandleFunc("POST /internal/workers/toggle", h.handleToggleWorkerScheduling)
	mux.HandleFunc("POST /internal/workers/devices/toggle", h.handleToggleDeviceScheduling)
	mux.HandleFunc("POST /api/v1/benchmark", h.handleBenchmarkAPI)

	// WorkerHub: bidirectional stream for remote workers.
	// ConnectRPC uses POST for all RPCs, so prefix with POST to avoid
	// conflict with the GET / catch-all route.
	canonicalFn := func() map[string]struct{} { return CanonicalModelFiles(h.modelsDir()) }
	_, hubHandler := workerconnect.NewWorkerHubHandler(
		worker.NewHub(h.workers, "http://"+h.cfg.EffectiveListenAddr(), h.modelsDir(), canonicalFn, h.logger),
	)
	mux.Handle("POST "+workerconnect.WorkerHubConnectProcedure, hubHandler)

	// Worker model file fetch — public coordination endpoint that remote
	// workers hit to download model files MASS owns. Versioned because
	// it's a stable cross-process URL, not UI plumbing.
	mux.HandleFunc("GET /api/v1/models/fetch/{path...}", h.handleFetchModel)

	// Models tab UI (Datastar SSE / HTML rendering).
	mux.HandleFunc("GET /internal/models", h.handleListModels)
	mux.HandleFunc("DELETE /internal/models", h.handleDeleteModel)
	mux.HandleFunc("POST /internal/models/download", h.handleDownloadModel)
	mux.HandleFunc("POST /internal/models/download/pause", h.handleDownloadPause)
	mux.HandleFunc("POST /internal/models/download/resume", h.handleDownloadResume)
	mux.HandleFunc("POST /internal/models/download/cancel", h.handleDownloadCancel)
	mux.HandleFunc("GET /internal/models/info", h.handleModelInfo)

	// Scheduler tab UI eviction (Datastar).
	mux.HandleFunc("DELETE /internal/scheduler/evict", h.handleSchedulerEvict)

	// Public API v1 — JSON endpoints + embeddable HTML widgets that apps
	// reuse (model picker dialog, HF search results). Both shapes share
	// the same stability promise as ConnectRPC under /mass.v1.Mass/*.
	mux.HandleFunc("GET /api/v1/models", h.handleAPIListModels)
	mux.HandleFunc("POST /api/v1/models/load", h.handleLoadModel)
	mux.HandleFunc("POST /api/v1/models/import", h.handleImportModel)
	mux.HandleFunc("GET /api/v1/models/select", h.handleModelsSelect)
	mux.HandleFunc("POST /api/v1/models/search", h.handleSearchHF)
	mux.HandleFunc("POST /api/v1/models/search/more", h.handleSearchHFMore)
	mux.HandleFunc("GET /api/v1/browse/roots", h.handleBrowseRoots)
	mux.HandleFunc("GET /api/v1/browse", h.handleBrowseFiles)

	// ConnectRPC endpoints — public mass.v1 contract handled directly by
	// the web handler (not proxied to scheduler).
	mux.HandleFunc("POST "+rpcconnect.MassListModelsProcedure, h.handleRPCListModels)
	mux.HandleFunc("POST "+rpcconnect.MassListLoadedModelsProcedure, h.handleRPCListLoadedModels)
	mux.HandleFunc("POST "+rpcconnect.MassLoadModelProcedure, h.handleRPCLoadModel)
	mux.HandleFunc("POST "+rpcconnect.MassDownloadModelProcedure, h.handleRPCDownloadModel)
	mux.HandleFunc("POST "+rpcconnect.MassRunBenchmarkProcedure, h.handleRPCRunBenchmark)
	mux.HandleFunc("POST "+rpcconnect.MassSetDeviceEnabledProcedure, h.handleRPCSetDeviceEnabled)

	// Proxy remaining ConnectRPC (inference) and ping endpoints to scheduler.
	// App-registered services use paths like "/mass.<app>.v1.<Service>/<Method>"
	// and are routed by ServeHTTP before reaching this mux.
	mux.HandleFunc("POST /mass.v1.Mass/", h.handleAPIProxy)
	mux.HandleFunc("GET /ping", h.handlePing)

	return mux, nil
}

// SetOnThemeChange registers a callback invoked when the UI theme toggles.
// dark is true for dark mode, false for light mode.
func (h *Handler) SetOnThemeChange(fn func(dark bool)) {
	h.onThemeChange = fn
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// App-registered services use paths "/mass.<app>.v1.<Service>/<Method>"
	// — anything under /mass. that isn't a core MASS RPC. Routed straight
	// to the scheduler (ServeMux can't express this as a single literal).
	// Exclusions: /mass.v1.Mass/ (public inference API, on the mux) and
	// /mass.v1.worker.WorkerHub/ (worker bidi stream, also on the mux).
	if r.Method == http.MethodPost &&
		strings.HasPrefix(r.URL.Path, "/mass.") &&
		!strings.HasPrefix(r.URL.Path, "/mass.v1.Mass/") &&
		!strings.HasPrefix(r.URL.Path, "/mass.v1.worker.") {
		h.orch.ServeHTTP(w, r)
		return
	}
	h.mux.ServeHTTP(w, r)
}

// SSEBroker fans out SSE events to connected browser clients.
type SSEBroker struct {
	mu      sync.Mutex
	clients map[chan SSEEvent]struct{}
	logger  zerolog.Logger
}

// EventKind distinguishes event types.
type EventKind int

const (
	EventTypeStatus       EventKind = iota
	EventTypeProgress               // model loading progress
	EventTypeLog                    // app stderr log line
	EventTypeDownload               // model download progress
	EventTypeSystemLog              // MASS system log line
	EventTypePoolChange             // model pool changed (model loaded/evicted)
	EventTypeWorkerChange           // worker fleet changed (agent connected/disconnected)
	EventTypeWorkerStats            // agent stats update (gauges only, no DOM replace)
)

// SSEEvent is an event broadcast to SSE clients.
type SSEEvent struct {
	Type     EventKind
	AppName  string
	State    scheduler.AppState
	Error    error
	Progress string
	LogLine  string
	// Download progress fields (EventTypeDownload).
	DlFilename   string
	DlDownloaded int64
	DlTotal      int64
	DlDone       bool
	DlPath       string
	DlRepoID     string // for JS to find/create the correct group
	DlGroupName  string // server-computed group display name
	DlStart      bool   // download just started (insert row)
	DlPaused     bool   // download paused
	DlCancelled  bool   // download cancelled (remove row)
	SysLogLine   string // MASS system log line (EventTypeSystemLog)
	// Pool change fields (EventTypePoolChange).
	PoolChange  scheduler.PoolChangeKind // list vs status change
	Fingerprint string                   // set for PoolChangeStatus
}

// NewSSEBroker creates a new broker.
func NewSSEBroker(logger zerolog.Logger) *SSEBroker {
	return &SSEBroker{
		clients: make(map[chan SSEEvent]struct{}),
		logger:  logger,
	}
}

// Subscribe returns a channel that receives SSE events.
func (b *SSEBroker) Subscribe() chan SSEEvent {
	ch := make(chan SSEEvent, 32)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (b *SSEBroker) Unsubscribe(ch chan SSEEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Broadcast sends an event to all connected clients.
func (b *SSEBroker) Broadcast(evt SSEEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- evt:
		default: // drop if client is slow
			b.logger.Warn().Msg("SSE client slow, event dropped")
		}
	}
}

// isPathUnder returns the absolute form of path and true if it is inside dir.
func isPathUnder(path, dir string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	return abs, strings.HasPrefix(filepath.ToSlash(abs), filepath.ToSlash(dir))
}

// decodeJSON decodes a JSON request body into v. Returns nil if the body is nil.
func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(v)
}

// writeJSON writes a JSON response. Panics on unmarshalable v (programmer bug);
// tolerates client-disconnect errors silently.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !expectedClientDisconnect(err) {
		panic(err)
	}
}

// addDownload registers a new active download and returns its state.
func (h *Handler) addDownload(repoID, filename string, cancelFn context.CancelFunc) *downloadState {
	ds := &downloadState{
		RepoID:   repoID,
		Filename: filename,
		cancelFn: cancelFn,
	}
	h.dlMu.Lock()
	h.downloads[filename] = ds
	h.dlMu.Unlock()
	return ds
}

// getDownload returns the download state for a filename, or nil.
func (h *Handler) getDownload(filename string) *downloadState {
	h.dlMu.RLock()
	defer h.dlMu.RUnlock()
	return h.downloads[filename]
}

// removeDownload removes a download from the tracking map.
func (h *Handler) removeDownload(filename string) {
	h.dlMu.Lock()
	delete(h.downloads, filename)
	h.dlMu.Unlock()
}

// listActiveDownloads returns a snapshot of all active downloads.
func (h *Handler) listActiveDownloads() []*downloadState {
	h.dlMu.RLock()
	defer h.dlMu.RUnlock()
	out := make([]*downloadState, 0, len(h.downloads))
	for _, ds := range h.downloads {
		out = append(out, ds)
	}
	return out
}

// replayDownloads emits ExecuteScript calls on the given SSE connection to
// insert download rows for all active/paused downloads. Called from
// handleListModels after the model list is rendered, so the download rows
// are inserted into the freshly patched DOM (no race with list load).
func (h *Handler) replayDownloads(sse *datastar.ServerSentEventGenerator) {
	downloads := h.listActiveDownloads()
	if len(downloads) == 0 {
		return
	}
	for _, ds := range downloads {
		ds.mu.Lock()
		jsFile := jsStringEscape(ds.Filename)
		jsGroup := jsStringEscape(ds.GroupName)
		downloaded := ds.Downloaded
		total := ds.Total
		paused := ds.Paused
		ds.mu.Unlock()
		mustSSE(sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlStart('%s','%s')`, jsFile, jsGroup)))
		if total > 0 {
			pct := int(100 * downloaded / total)
			mustSSE(sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlProgress('%s',%d,%d,%d)`, jsFile, pct, downloaded, total)))
		}
		if paused {
			mustSSE(sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlPaused('%s')`, jsFile)))
		}
	}
}
