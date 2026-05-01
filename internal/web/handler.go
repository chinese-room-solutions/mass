// Package web hosts MASS's HTTP surface: the dashboard UI, the management
// RPC layer, and (in Stage 2) the /mass.<runtime>.* gateway proxy.
//
// Stage 1 keeps the surface intentionally minimal: login, theme/auth-token
// settings, a workers tab, and a runtimes scaffold. Inference + apps live
// elsewhere now (gateways) and will not return here.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/chinese-room-solutions/mass-proto/gen/go/worker/workerconnect"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/starfederation/datastar-go/datastar"
	"golang.org/x/crypto/bcrypt"
)

// HandlerOptions wires Handler to the rest of the process.
type HandlerOptions struct {
	Config    *config.Config
	Scheduler *scheduler.Scheduler
	Runtimes  *runtimes.Manager
	Downloads *downloads.Manager
	Store     store.StoreInterface
	SaveFn    func()
	Logger    zerolog.Logger
	AuthHash  []byte
	Sessions  *SessionStore
	SysLog    *SystemLogBuffer
	Workers   *worker.Fleet
	WorkerHub *worker.Hub
	ConfigDir string
	LogsDir   string
	DataDir   string
}

// Handler implements http.Handler for the MASS web UI and management API.
type Handler struct {
	cfg       *config.Config
	orch      *scheduler.Scheduler
	runtimes  *runtimes.Manager
	downloads *downloads.Manager
	store     store.StoreInterface
	saveFn    func()
	logger    zerolog.Logger
	sessions  *SessionStore
	sysLog    *SystemLogBuffer
	workers   *worker.Fleet
	workerHub *worker.Hub
	configDir string
	logsDir   string
	dataDir   string

	authHashMu sync.RWMutex
	authHash   []byte

	themeMu       sync.RWMutex
	onThemeChange func(dark bool)

	workersBroker   *WorkersBroker
	schedulerBroker *SchedulerBroker

	mux *http.ServeMux
}

// NewHandler builds the web handler.
func NewHandler(opts HandlerOptions) (*Handler, error) {
	h := &Handler{
		cfg:             opts.Config,
		orch:            opts.Scheduler,
		runtimes:        opts.Runtimes,
		downloads:       opts.Downloads,
		store:           opts.Store,
		saveFn:          opts.SaveFn,
		logger:          opts.Logger.With().Str("component", "web").Logger(),
		sessions:        opts.Sessions,
		sysLog:          opts.SysLog,
		workers:         opts.Workers,
		workerHub:       opts.WorkerHub,
		configDir:       opts.ConfigDir,
		logsDir:         opts.LogsDir,
		dataDir:         opts.DataDir,
		authHash:        opts.AuthHash,
		workersBroker:   NewWorkersBroker(opts.Logger),
		schedulerBroker: NewSchedulerBroker(opts.Logger),
	}
	if h.workers != nil {
		h.workers.AddChangeCallback(func(evt worker.FleetChangeEvent) {
			// Updated = heartbeat tick; covered by the periodic stats SSE,
			// no need to refetch the whole list (would collapse open cards).
			if evt.Kind == worker.FleetChangeUpdated {
				return
			}
			h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
			// Add/remove also moves rows on the Scheduler tab — when a
			// worker drops, every model loaded on it disappears.
			h.schedulerBroker.Broadcast(SchedulerEvent{})
			// Auto-bench newly-connected workers in the background so the
			// UI fills in numbers without an operator click. Devices that
			// already have stored results are skipped.
			if evt.Kind == worker.FleetChangeAdded {
				go h.autoBenchmarkWorker(evt.WorkerID)
			}
		})
		// Loaded-set / pool-counter changes wake every Scheduler-tab
		// subscriber. Models tab no longer needs a fan-out here — its
		// per-row SSE stream is sourced directly from each runtime gateway.
		h.workers.AddLoadedChangedCallback(func(_ string) {
			h.schedulerBroker.Broadcast(SchedulerEvent{})
		})
	}
	h.mux = h.buildRoutes()
	return h, nil
}

// SetOnThemeChange registers a callback fired whenever the theme changes
// (used by the GUI to update the native window).
func (h *Handler) SetOnThemeChange(fn func(dark bool)) {
	h.themeMu.Lock()
	h.onThemeChange = fn
	h.themeMu.Unlock()
}

// SetAuthHash atomically swaps the active auth-token hash. Empty disables auth.
func (h *Handler) SetAuthHash(hash []byte) {
	h.authHashMu.Lock()
	h.authHash = hash
	h.authHashMu.Unlock()
}

// ServeHTTP delegates to the internal mux.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) buildRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Static assets (no auth).
	if pubFS, err := publicFS(); err == nil {
		mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServer(pubFS)))
	}

	// Login.
	mux.HandleFunc("GET /login", h.handleLoginPage)
	mux.HandleFunc("POST /login", h.handleLoginSubmit)
	mux.HandleFunc("POST /logout", h.handleLogout)

	// Dashboard. {$} pins this to exactly "/" so it doesn't shadow the
	// /mass. gateway proxy below.
	mux.HandleFunc("GET /{$}", h.handleDashboard)

	// Internal browser-only HTML/SSE — not part of the public mass.v1 contract.
	mux.HandleFunc("POST /internal/settings/theme", h.handleToggleTheme)

	// Runtime gateway management.
	mux.HandleFunc("GET /api/runtimes", h.handleListRuntimes)
	mux.HandleFunc("POST /api/runtimes/install", h.handleInstallRuntime)
	mux.HandleFunc("DELETE /api/runtimes/{kind}", h.handleUninstallRuntime)
	mux.HandleFunc("POST /api/runtimes/{kind}/start", h.handleStartRuntime)
	mux.HandleFunc("POST /api/runtimes/{kind}/stop", h.handleStopRuntime)
	mux.HandleFunc("POST /api/runtimes/{kind}/auto-start", h.handleRuntimeAutoStartToggle)
	mux.HandleFunc("GET /api/runtimes/{kind}/logs", h.handleRuntimeLogsSSE)

	// Models tab refetch trigger. Each runtime gateway owns its own model
	// list (rendered via /mass.<kind>.v1/Models/List); MASS just notifies
	// the browser when something changed (install completed, runtime
	// state shifted) so the JS aggregator re-fans the list.
	mux.HandleFunc("GET /api/models/stream", h.handleModelsStreamSSE)

	// Install / download lifecycle.
	mux.HandleFunc("POST /api/models/install", h.handleInstallModel)
	mux.HandleFunc("POST /api/models/import", h.handleImportLocalModel)
	mux.HandleFunc("GET /api/groups/names", h.handleListGroupNames)
	mux.HandleFunc("POST /api/groups/rename", h.handleRenameGroup)
	mux.HandleFunc("POST /api/models/download/pause", h.handleDownloadPause)
	mux.HandleFunc("POST /api/models/download/resume", h.handleDownloadResume)
	mux.HandleFunc("POST /api/models/download/cancel", h.handleDownloadCancel)
	mux.HandleFunc("GET /api/models/downloads/events", h.handleDownloadsEventsSSE)
	mux.HandleFunc("POST /api/models/search", h.handleSearchHF)
	mux.HandleFunc("POST /api/models/search/more", h.handleSearchHFMore)

	// Scheduler view.
	mux.HandleFunc("GET /api/scheduler/list", h.handleSchedulerList)
	mux.HandleFunc("GET /api/scheduler/detail", h.handleSchedulerDetail)
	mux.HandleFunc("GET /api/scheduler/events", h.handleSchedulerEventsSSE)
	mux.HandleFunc("POST /api/scheduler/evict", h.handleSchedulerEvict)

	// Workers tab.
	mux.HandleFunc("GET /api/workers/list", h.handleWorkersList)
	mux.HandleFunc("GET /api/workers/events", h.handleWorkersEventsSSE)
	mux.HandleFunc("POST /api/workers/benchmark", h.handleWorkersBenchmark)
	mux.HandleFunc("POST /api/workers/{id}/toggle", h.handleToggleWorker)
	mux.HandleFunc("POST /api/workers/{id}/devices/{devID}/toggle", h.handleToggleDevice)

	// File browser (used by Settings + Install dialogs).
	mux.HandleFunc("GET /api/v1/browse/roots", h.handleBrowseRoots)
	mux.HandleFunc("GET /api/v1/browse", h.handleBrowseFiles)

	// Settings tab + system logs.
	mux.HandleFunc("POST /api/settings", h.handleSettingsSave)
	mux.HandleFunc("GET /api/sync-logs", h.handleSyncLogs)
	mux.HandleFunc("GET /api/syslogs", h.handleSysLogsSSE)

	// Runtime gateway proxy: HTTP → gateway HandleRequest stream.
	//
	// Go's ServeMux matches "/mass." exactly (no prefix semantics for non-"/"
	// patterns); the gateway URLs are "/mass.<kind>{.|/}<rest>", which would
	// never match. Use the catch-all and prefix-check inside.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/mass.") {
			h.handleRuntimeProxy(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Worker hub: bidi gRPC stream that workers connect to. ConnectRPC
	// serves both the Connect protocol and plain gRPC over the same path.
	if h.workerHub != nil {
		path, handler := workerconnect.NewWorkerHubHandler(h.workerHub)
		mux.Handle(path, handler)
	}

	return mux
}

// --- Pages ---

func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// Render the shell with empty Models / Scheduler / Workers. The browser
	// fetches each tab's content lazily via /api/{tab}/list when first
	// activated, keeping initial dashboard render O(1) regardless of model
	// count or worker fleet size. Runtimes is rendered inline because it's
	// already cheap (just a manifest list, no RPCs).
	data := templates.DashboardData{
		Theme:           h.cfg.Theme,
		ConfigDir:       h.configDir,
		LogsDir:         h.logsDir,
		DataDir:         h.cfg.DataDir,
		ListenAddr:      h.cfg.ListenAddr,
		AuthTokenSet:    func() bool { h.authHashMu.RLock(); defer h.authHashMu.RUnlock(); return len(h.authHash) > 0 }(),
		LogLevel:        logLevelString(h.cfg.Logger.Level),
		DevMode:         h.cfg.DevMode,
		ResultTTL:       h.cfg.ResultTTL,
		IdleEvictionTTL: h.cfg.IdleEvictionTTL,
		RegistryURL:     h.cfg.RegistryURL,
		TLSEnabled:      h.cfg.TLS.Enabled,
		TLSCertFile:     h.cfg.TLS.CertFile,
		Runtimes:        h.runtimeViews(),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := templates.Layout("MASS", templates.Shell(data), h.cfg.Theme).Render(r.Context(), w); err != nil {
		h.logger.Warn().Err(err).Msg("rendering dashboard")
	}
}

func (h *Handler) runtimeViews() []templates.RuntimeViewData {
	if h.runtimes == nil {
		return nil
	}
	mfs := h.runtimes.List()
	out := make([]templates.RuntimeViewData, len(mfs))
	for i, mf := range mfs {
		out[i] = templates.RuntimeViewData{
			RuntimeName: mf.RuntimeName,
			DisplayName: mf.DisplayName,
			Version:     mf.Version,
			Description: mf.Description,
			AutoStart:   mf.AutoStart,
			Running:     h.runtimes.IsRunning(mf.RuntimeName),
		}
	}
	return out
}

// schedulerInstances builds the typed Scheduler-tab views from the live
// worker fleet. One instance per (worker, loaded-model) pair.
func (h *Handler) schedulerInstances() []templates.SchedulerInstanceView {
	if h.workers == nil {
		return nil
	}
	var out []templates.SchedulerInstanceView
	for _, w := range h.workers.All() {
		sw, ok := w.(*worker.StreamWorker)
		if !ok {
			continue
		}
		// Pick the first device as a compact display chip.
		deviceID := ""
		if devs := sw.Devices(); len(devs) > 0 {
			deviceID = devs[0].ID
		}
		for _, lm := range sw.LoadedModels() {
			status := "Active"
			if lm.Active == 0 {
				status = "Idle"
			}
			filename, fingerprint := splitModelID(lm.ModelID)
			source := lm.Source
			if source == "" {
				source = "direct"
			}
			out = append(out, templates.SchedulerInstanceView{
				Key:         sw.ID() + ":" + lm.ModelID,
				ModelID:     lm.ModelID,
				Filename:    filename,
				Fingerprint: fingerprint,
				WorkerID:    sw.ID(),
				WorkerName:  sw.Name(),
				RuntimeName: sw.RuntimeName(),
				DeviceID:    deviceID,
				Source:      source,
				Mode:        "dynamic", // gateway-driven loads are dynamic by definition
				PoolSize:    lm.PoolSize,
				Active:      lm.Active,
				Status:      status,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// logLevelString renders the configured zerolog level as a lowercase token
// matching the values the Settings <sl-select> exposes.
func logLevelString(l config.LogLevel) string {
	b, _ := l.MarshalText()
	return string(b)
}

// splitModelID splits a gateway-built model ID at its trailing
// "#<fingerprint>" suffix, returning the prefix as filename and the
// fingerprint separately. Shape is gateway-defined and opaque to MASS;
// today llama-cpp emits "<filename>#<sha-prefix>". Returns the full ID
// as filename when no '#' is present.
func splitModelID(id string) (filename, fingerprint string) {
	if i := strings.LastIndexByte(id, '#'); i >= 0 {
		return id[:i], id[i+1:]
	}
	return id, ""
}

func (h *Handler) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := templates.Layout("MASS - Login", templates.LoginPage(""), h.cfg.Theme).Render(r.Context(), w); err != nil {
		h.logger.Warn().Err(err).Msg("rendering login page")
	}
}

func (h *Handler) handleLoginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	token := r.PostFormValue("token")

	h.authHashMu.RLock()
	hash := h.authHash
	h.authHashMu.RUnlock()

	if len(hash) == 0 || bcrypt.CompareHashAndPassword(hash, []byte(token)) != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = templates.Layout("MASS - Login", templates.LoginPage("Invalid token."), h.cfg.Theme).Render(r.Context(), w)
		return
	}

	id, err := h.sessions.Create()
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, id)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("mass_session"); err == nil && h.sessions != nil {
		h.sessions.Invalidate(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: "mass_session", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// --- Internal endpoints ---

func (h *Handler) handleToggleTheme(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Theme == "light" {
		h.cfg.Theme = "dark"
	} else {
		h.cfg.Theme = "light"
	}
	if h.saveFn != nil {
		h.saveFn()
	}
	h.themeMu.RLock()
	cb := h.onThemeChange
	h.themeMu.RUnlock()
	if cb != nil {
		cb(h.cfg.Theme != "light")
	}
	// Patch the $theme Datastar signal so every data-attr-bound element
	// (page background, logo image, sl-icon glyph, dialog tints) flips
	// alongside the OS window. The native window callback above only
	// affects the host chrome — the page itself listens for this signal.
	sse := datastar.NewSSE(w, r)
	if b, err := json.Marshal(map[string]any{"theme": h.cfg.Theme}); err == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("patching theme signal")
		}
	}
}

// --- Workers tab ---

// buildWorkerViews assembles per-worker render data from the live fleet
// plus any stored benchmark rows. Returned in fleet enumeration order.
func (h *Handler) buildWorkerViews() []templates.WorkerView {
	all := h.workers.All()
	views := make([]templates.WorkerView, 0, len(all))
	for _, wkr := range all {
		status := wkr.Status()

		statsMap := make(map[string]stats.DeviceStats)
		for _, s := range wkr.Stats() {
			statsMap[s.DeviceID] = s
		}

		// Snapshot the operator's per-device toggle state once per worker
		// — absent rows mean "enabled" (sane default for newly-connected
		// workers without any persisted intent).
		enabledByDev := map[string]bool{}
		if h.store != nil {
			if rows, err := h.store.ListWorkerDevicesEnabled(wkr.ID()); err == nil {
				for _, r := range rows {
					enabledByDev[r.DeviceID] = r.Enabled
				}
			}
		}

		var devices []templates.ComputeView
		anyEnabled := false
		for _, dev := range safeDevices(wkr) {
			devEnabled := true
			if v, ok := enabledByDev[dev.ID]; ok {
				devEnabled = v
			}
			if devEnabled {
				anyEnabled = true
			}
			cv := templates.ComputeView{
				DeviceID:   dev.ID,
				DeviceName: dev.Name,
				Type:       string(dev.Type),
				Enabled:    devEnabled,
				MemoryMB:   dev.TotalMemoryMB,
			}
			if row, err := h.store.GetBenchmark(wkr.ID(), dev.ID); err == nil {
				cv.MemoryGBs = row.MemoryGBs
				cv.ComputeGFlops = row.ComputeGFlops
				cv.HasBenchmark = true
			}
			if s, ok := statsMap[dev.ID]; ok {
				if s.TotalMemoryMB > 0 {
					cv.UsedMemoryMB = s.UsedMemoryMB
					cv.MemoryMB = s.TotalMemoryMB
					cv.HasStats = true
				}
				cv.UtilizationPct = s.UtilizationPct
				cv.HasUtilization = true
			}
			devices = append(devices, cv)
		}
		// "Worker enabled" is derived: any non-disabled device counts.
		// A worker with no devices yet (race window before first heartbeat)
		// is treated as enabled.
		workerEnabled := anyEnabled || len(devices) == 0

		views = append(views, templates.WorkerView{
			ID:          wkr.ID(),
			Name:        wkr.Name(),
			RuntimeName: wkr.RuntimeName(),
			Online:      status.Online,
			Enabled:     workerEnabled,
			Devices:     devices,
			ActiveJobs:  wkr.ActiveJobs(),
			Capacity:    wkr.AvailableCapacity(),
		})
	}
	return views
}

// safeDevices calls Devices() with panic recovery — a misbehaving worker
// implementation must not bring the dashboard down.
func safeDevices(wkr worker.WorkerInterface) (devices []stats.Device) {
	defer func() {
		if r := recover(); r != nil {
			devices = nil
		}
	}()
	return wkr.Devices()
}

// --- Runtime proxy ---
//
// /mass.<runtime_name>.<rest> is forwarded to the matching runtime gateway
// over its go-plugin gRPC stream. The HTTP body is streamed in as
// HTTPRequestChunk frames; the gateway streams HTTPResponseChunk frames
// back, the first of which carries the status code + headers.

func (h *Handler) handleRuntimeProxy(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/mass.")
	// Two route shapes share this proxy:
	//   /mass.<kind>.<rest>   — typed HTTP + OpenAI-compat (kind is followed by '.')
	//   /mass.<kind>/<rest>   — typed gRPC (kind is followed by '/')
	// The kind ends at whichever separator appears first.
	sepIdx := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == '.' || rest[i] == '/' {
			sepIdx = i
			break
		}
	}
	if sepIdx <= 0 {
		http.NotFound(w, r)
		return
	}
	kind := rest[:sepIdx]
	subPath := rest[sepIdx:] // includes the leading separator; gateway routes on it
	if h.runtimes == nil || !h.runtimes.IsInstalled(kind) {
		h.writeJSONError(w, http.StatusServiceUnavailable, "runtime gateway not installed", kind)
		return
	}
	gw, err := h.runtimes.Start(r.Context(), kind)
	if err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("starting runtime gateway")
		h.writeJSONError(w, http.StatusBadGateway, "runtime gateway unavailable: "+err.Error(), kind)
		return
	}
	if err := proxyToGateway(r.Context(), gw, subPath, w, r); err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("proxying to runtime gateway")
		return
	}
	// DELETE on a runtime path mutates the catalogue (model file
	// removed) — wake the Models SSE renderer so the affected group
	// card re-renders (or disappears, when its last file leaves)
	// without a manual refresh. Other methods (GET for reads, POST
	// for inference) leave the catalogue alone so we don't fire on
	// them — would be wasteful to re-walk on every chat message.
	if r.Method == http.MethodDelete {
		h.runtimes.FireStateChange(kind)
	}
}

// proxyToGateway streams the HTTP request into a HandleRequest gRPC stream
// and writes the response back to the client as the gateway emits frames.
func proxyToGateway(ctx context.Context, gw *runtimes.LoadedGateway, subPath string, w http.ResponseWriter, r *http.Request) error {
	stream, err := gw.Client().HandleRequest(ctx)
	if err != nil {
		return fmt.Errorf("opening gateway stream: %w", err)
	}

	// Send the request: first frame carries metadata + (optionally) body
	// bytes; subsequent frames carry body bytes only.
	path := subPath
	if r.URL.RawQuery != "" {
		path = subPath + "?" + r.URL.RawQuery
	}
	first := &gatewaypb.HTTPRequestChunk{
		Method:  r.Method,
		Path:    path,
		Headers: flattenHeaders(r.Header),
	}

	// Streaming send: header chunk first, then body chunks. We split body
	// reads at a generous 64 KiB to keep individual frames small enough for
	// most gRPC defaults.
	if err := stream.Send(first); err != nil {
		return fmt.Errorf("sending request header chunk: %w", err)
	}
	if err := streamRequestBody(stream, r.Body); err != nil {
		return fmt.Errorf("streaming request body: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("closing request stream: %w", err)
	}

	// Read response: the first frame is expected to set status + headers.
	// The final frame may carry trailers (used by gRPC to convey grpc-status /
	// grpc-message). HTTP/2 trailers travel via the "Trailer:" pseudo-prefix
	// on http.Header — net/http rewrites them into real HTTP/2 trailer frames.
	headerWritten := false
	flusher, _ := w.(http.Flusher)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if !headerWritten {
				return fmt.Errorf("receiving response header: %w", err)
			}
			return fmt.Errorf("receiving response body: %w", err)
		}
		if !headerWritten {
			for k, v := range resp.Headers {
				w.Header().Set(k, v)
			}
			status := int(resp.Status)
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			headerWritten = true
		}
		if len(resp.Body) > 0 {
			if _, err := w.Write(resp.Body); err != nil {
				if expectedClientDisconnect(err) {
					return nil
				}
				return fmt.Errorf("writing response body: %w", err)
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if resp.EndOfStream {
			for k, v := range resp.GetTrailers() {
				w.Header().Set(http.TrailerPrefix+k, v)
			}
			return nil
		}
	}
}

// streamRequestBody copies the request body into a sequence of HTTPRequestChunk
// frames. The final frame has end_of_stream=true.
func streamRequestBody(stream gatewaypb.RuntimeGateway_HandleRequestClient, body io.ReadCloser) error {
	defer func() { _ = body.Close() }()
	if body == nil {
		return stream.Send(&gatewaypb.HTTPRequestChunk{EndOfStream: true})
	}
	const chunkSize = 64 * 1024
	buf := make([]byte, chunkSize)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if err := stream.Send(&gatewaypb.HTTPRequestChunk{Body: buf[:n]}); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			return stream.Send(&gatewaypb.HTTPRequestChunk{EndOfStream: true})
		}
		if rerr != nil {
			return rerr
		}
	}
}

// flattenHeaders flattens an http.Header into a single-value map. Multi-value
// headers are joined with commas — fine for HTTP semantics on every header
// gateways are likely to inspect.
func flattenHeaders(in http.Header) map[string]string {
	out := make(map[string]string, len(in))
	for k, vs := range in {
		out[k] = strings.Join(vs, ",")
	}
	return out
}

// expectedClientDisconnect reports whether err is a "client went away" error
// we should swallow when writing to a response.
func expectedClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// http.ErrAbortHandler is a panic value, not an error; everything else
	// flows through as a network-level error. Stage 2 keeps this minimal —
	// add tighter classification when real bug reports demand it.
	return false
}

// writeJSONError writes a small JSON error body. Tolerates client disconnects
// silently so a closed write isn't logged as an internal failure. Marshalling
// is over a tiny map[string]string and cannot fail.
func (h *Handler) writeJSONError(w http.ResponseWriter, status int, msg, runtimeName string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, err := json.Marshal(map[string]string{"error": msg, "runtime_name": runtimeName})
	if err != nil {
		h.logger.Debug().Err(err).Msg("marshalling runtime proxy error response")
		return
	}
	if _, err := w.Write(body); err != nil {
		h.logger.Debug().Err(err).Msg("writing runtime proxy error response")
	}
}

// --- Misc ---

// Bcrypt is re-exported so cmd/mass can hash tokens without importing
// the underlying package directly.
func Bcrypt(token string) ([]byte, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("hashing token: %w", err), nil)
	}
	return hash, nil
}

// Ensure unused imports don't break builds while routes are stubbed.
var (
	_ = context.Background
	_ = time.Now
	_ = json.Marshal
)
