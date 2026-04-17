package web

import (
	"encoding/json"
	"fmt"
	"html"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/rs/zerolog"
	"github.com/starfederation/datastar-go/datastar"
	"golang.org/x/crypto/bcrypt"
)

// installAppFromPackage installs (or upgrades) an app from a .mass
// package on disk, registers it with the scheduler, and persists the
// updated app list. Returns the installed app's name and version.
func (h *Handler) installAppFromPackage(packagePath string) (string, string, error) {
	command, meta, err := h.installer.InstallFromArchive(packagePath)
	if err != nil {
		return "", "", fmt.Errorf("install failed: %w", err)
	}
	appDir := filepath.Dir(command)
	configPath := ""
	if _, statErr := os.Stat(filepath.Join(appDir, "config.yml")); statErr == nil {
		configPath = "${APP_DIR}/config.yml"
	}
	if existing := h.cfg.FindApp(meta.Name); existing != nil {
		existing.Command = command
		existing.Version = meta.Version
		existing.Source = "package"
		existing.Config = configPath
	} else {
		h.cfg.Apps = append(h.cfg.Apps, config.AppConfig{
			Name:    meta.Name,
			Command: command,
			Version: meta.Version,
			Source:  "package",
			Config:  configPath,
		})
	}
	h.saveFn()
	if err := h.orch.Register(h.cfg.FindApp(meta.Name)); err != nil {
		return meta.Name, meta.Version, fmt.Errorf("installed but registration failed: %w", err)
	}
	return meta.Name, meta.Version, nil
}

func (h *Handler) handleInstallApp(w http.ResponseWriter, r *http.Request) {
	var signals struct {
		PackagePath string `json:"packagePath"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		sse := datastar.NewSSE(w, r)
		patchInstallError(sse, "Invalid request: "+err.Error())
		return
	}

	sse := datastar.NewSSE(w, r)

	if signals.PackagePath == "" {
		patchInstallError(sse, "Package path is required")
		return
	}

	name, _, err := h.installAppFromPackage(signals.PackagePath)
	if err != nil {
		patchInstallError(sse, err.Error())
		return
	}

	// Re-render sidebar.
	allApps := h.orch.GetAllApps()
	apps := buildAppList(h.cfg, allApps)
	mustSSE(sse.PatchElements(templates.RenderAppList(apps, name),
		datastar.WithSelector("#app-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))

	patchAppContent(sse, `<div class="flex flex-col items-center justify-center h-64 text-center"><h2 class="text-lg font-semibold mb-2">App installed</h2><p class="text-neutral-400 text-sm">Press play to start.</p></div>`)

	if b, err := json.Marshal(map[string]any{"addAppOpen": false, "installing": false, "packagePath": ""}); err == nil {
		mustSSE(sse.PatchSignals(b))
	}
}

func (h *Handler) handleAppIcon(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	mp := h.orch.GetApp(name)
	if mp == nil || mp.DiskMeta == nil || mp.DiskMeta.Icon == "" {
		http.NotFound(w, r)
		return
	}
	dataDir, err := h.cfg.EffectiveDataDir()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	version := ""
	if mp.Config != nil {
		version = mp.Config.Version
	}
	iconPath := filepath.Join(config.AppVersionDir(dataDir, name, version), mp.DiskMeta.Icon)
	http.ServeFile(w, r, iconPath)
}

func (h *Handler) handleStartApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	h.logger.Info().Str("app", name).Msg("starting app")
	sse := datastar.NewSSE(w, r)

	if err := h.orch.Start(name); err != nil {
		h.logger.Error().Err(err).Str("app", name).Msg("failed to start app")
		mustSSE(sse.PatchElements(templates.RenderError(err.Error())))
		return
	}
	mustSSE(

		// Immediate feedback — status updates come via /internal/events SSE.
		sse.PatchElements(templates.RenderAppStatus(name, scheduler.StateStarting, nil, "")))
}

func (h *Handler) handleStopApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)

	if err := h.orch.Stop(name); err != nil {
		mustSSE(sse.PatchElements(templates.RenderError(err.Error())))
		return
	}
	mustSSE(sse.PatchElements(templates.RenderAppStatus(name, scheduler.StateStopped, nil, "")))
}

func (h *Handler) handleDeselectApp(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	if b, err := json.Marshal(map[string]any{"activeApp": ""}); err == nil {
		mustSSE(sse.PatchSignals(b))
	}
	apps := buildAppList(h.cfg, h.orch.GetAllApps())
	patchAppContent(sse, templates.RenderWelcomeState(len(apps) == 0))
}

func (h *Handler) handleRemoveApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)

	h.orch.Remove(name)
	h.cfg.RemoveApp(name)
	h.saveFn()

	// Delete the app's data directory only. Models are kept and must be
	// removed explicitly from the models page — uninstalling an app never
	// destroys downloads.
	// On Windows, file locks from the just-killed subprocess may linger briefly.
	if dataDir, err := h.cfg.EffectiveDataDir(); err == nil {
		if err := removeWithRetry(filepath.Join(dataDir, "apps", name), 5, 500*time.Millisecond); err != nil {
			h.logger.Warn().Err(err).Str("app", name).Msg("failed to remove app directory")
		}
	}

	// Re-render the sidebar app list and clear main content.
	allApps := h.orch.GetAllApps()
	apps := buildAppList(h.cfg, allApps)
	mustSSE(sse.PatchElements(templates.RenderAppList(apps, ""),
		datastar.WithSelector("#app-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
	if b, err := json.Marshal(map[string]any{"activeApp": ""}); err == nil {
		mustSSE(sse.PatchSignals(b))
	}

	patchAppContent(sse, templates.RenderWelcomeState(len(apps) == 0))
}

func (h *Handler) handleToggleTheme(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Theme == "light" {
		h.cfg.Theme = "dark"
	} else {
		h.cfg.Theme = "light"
	}
	h.saveFn()

	if h.onThemeChange != nil {
		h.onThemeChange(h.cfg.Theme == "dark")
	}

	sse := datastar.NewSSE(w, r)
	if b, err := json.Marshal(map[string]any{"theme": h.cfg.Theme}); err == nil {
		mustSSE(sse.PatchSignals(b))
	}
}

func (h *Handler) handleSetLaunchMode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindApp(name)
	if pc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	mode := config.LaunchMode(r.URL.Query().Get("mode"))
	switch mode {
	case config.LaunchModeManual, config.LaunchModeOnDemand:
		pc.LaunchMode = mode
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.saveFn()

	// If switching to on-demand and the app is already running, arm the
	// idle timer so it will stop after the configured timeout.
	if mode == config.LaunchModeOnDemand {
		h.orch.ArmIdleTimer(name)
	}

	sse := datastar.NewSSE(w, r)
	mustSSE(sse.PatchElements(
		templates.RenderLaunchModeDropdown(name, mode),
		datastar.WithSelector("#launch-mode-"+name),
		datastar.WithModeOuter(),
	))
}

func (h *Handler) handleToggleAutoStart(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindApp(name)
	if pc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pc.AutoStart = !pc.AutoStart
	h.saveFn()

	// Re-render the sidebar so the auto-start icon updates.
	sse := datastar.NewSSE(w, r)
	apps := buildAppList(h.cfg, h.orch.GetAllApps())
	mustSSE(sse.PatchElements(templates.RenderAppList(apps, name)))
}

func (h *Handler) handleToggleDebug(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindApp(name)
	if pc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pc.Debug = !pc.Debug
	h.saveFn()

	// Switching modes requires re-registration: stop current process and
	// re-register with the new debug setting.
	mp := h.orch.GetApp(name)
	if mp != nil && mp.State != scheduler.StateRunning {
		h.orch.Remove(name)
		if err := h.orch.Register(pc); err != nil {
			h.logger.Error().Err(err).Str("app", name).Msg("re-register after debug toggle failed")
		}
		h.saveFn()
	}

	// Re-render the sidebar so the bug icon color updates.
	sse := datastar.NewSSE(w, r)
	apps := buildAppList(h.cfg, h.orch.GetAllApps())
	mustSSE(sse.PatchElements(templates.RenderAppList(apps, name)))
}

func (h *Handler) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var signals struct {
		ListenAddr       string `json:"listenAddr"`
		DataDir          string `json:"dataDir"`
		AuthToken        string `json:"authToken"`
		AuthTokenEdited  bool   `json:"authTokenEdited"`
		ModelIdleTimeout string `json:"modelIdleTimeout"`
		AppIdleTimeout   string `json:"appIdleTimeout"`
		ResultTTL        string `json:"resultTtl"`
		LogLevel         string `json:"logLevel"`
		DevMode          bool   `json:"devMode"`
		TLSEnabled       bool   `json:"tlsEnabled"`
		TLSCertFile      string `json:"tlsCertFile"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		sse := datastar.NewSSE(w, r)
		mustSSE(sse.PatchElements(templates.RenderError("Invalid request")))
		return
	}

	h.cfg.ListenAddr = signals.ListenAddr
	h.cfg.DataDir = signals.DataDir
	h.cfg.ModelIdleTimeout = signals.ModelIdleTimeout
	h.cfg.AppIdleTimeout = signals.AppIdleTimeout
	h.cfg.ResultTTL = signals.ResultTTL
	h.cfg.DevMode = signals.DevMode
	h.cfg.TLS.Enabled = signals.TLSEnabled
	h.cfg.TLS.CertFile = signals.TLSCertFile

	// Apply log level change only if it actually changed.
	if signals.LogLevel != "" {
		var lvl config.LogLevel
		if err := lvl.UnmarshalText([]byte(signals.LogLevel)); err == nil && lvl != h.cfg.Logger.Level {
			h.cfg.Logger.Level = lvl
			zerolog.SetGlobalLevel(zerolog.Level(lvl))
			h.orch.SetLogLevel(signals.LogLevel)
			h.logger.Info().Str("new_level", signals.LogLevel).Msg("log level changed")
		}
	}

	h.saveFn()

	// Handle auth token update.
	// Only process when the user actually interacted with the field (authTokenEdited).
	if signals.AuthTokenEdited {
		if signals.AuthToken == "" {
			// User cleared the field — remove token.
			if err := h.store.DeleteSetting("auth_token"); err != nil {
				h.logger.Error().Err(err).Msg("deleting auth token")
			}
			h.authHashMu.Lock()
			h.authHash = nil
			h.authHashMu.Unlock()
			h.logger.Info().Msg("auth token removed")
		} else {
			// New token.
			hash, err := bcrypt.GenerateFromPassword([]byte(signals.AuthToken), bcrypt.DefaultCost)
			if err != nil {
				h.logger.Error().Err(err).Msg("hashing auth token")
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if err := h.store.SetSetting("auth_token", string(hash)); err != nil {
				h.logger.Error().Err(err).Msg("storing auth token")
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			h.authHashMu.Lock()
			h.authHash = hash
			h.authHashMu.Unlock()
			h.logger.Info().Msg("auth token updated")
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleBrowseRoots(w http.ResponseWriter, r *http.Request) {
	type rootEntry struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	roots := listRoots()
	entries := make([]rootEntry, 0, len(roots))
	for _, root := range roots {
		entries = append(entries, rootEntry{Name: root, Path: root})
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *Handler) handleBrowseFiles(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	ext := r.URL.Query().Get("ext")

	if dir == "" {
		if h.cfg.DataDir != "" {
			dir = h.cfg.DataDir
		} else if home, err := os.UserHomeDir(); err == nil {
			dir = home
		} else {
			dir = "."
		}
	}

	type fileEntry struct {
		Name  string `json:"name"`
		Path  string `json:"path"`
		IsDir bool   `json:"is_dir"`
	}

	var entries []fileEntry

	// Resolve to absolute path for consistent navigation.
	absDir, err := filepath.Abs(dir)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "resolving path: " + err.Error()})
		return
	}
	dir = absDir

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// Add parent directory.
	parent := filepath.Dir(dir)
	if parent != dir {
		entries = append(entries, fileEntry{Name: "..", Path: parent, IsDir: true})
	}

	for _, d := range dirEntries {
		if d.Type()&fs.ModeSymlink != 0 {
			continue
		}
		name := d.Name()
		path := filepath.Join(dir, name)
		if d.IsDir() {
			entries = append(entries, fileEntry{Name: name, Path: path, IsDir: true})
		} else if ext == "" || strings.HasSuffix(strings.ToLower(name), ext) {
			entries = append(entries, fileEntry{Name: name, Path: path, IsDir: false})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})

	writeJSON(w, http.StatusOK, entries)
}

func (h *Handler) handleAPIProxy(w http.ResponseWriter, r *http.Request) {
	// Delegate to scheduler's HTTP handler.
	h.orch.ServeHTTP(w, r)
}

func (h *Handler) handlePing(w http.ResponseWriter, r *http.Request) {
	h.orch.ServeHTTP(w, r)
}

func buildAppList(cfg *config.Config, allApps map[string]*scheduler.ManagedApp) []templates.AppViewData {
	apps := make([]templates.AppViewData, 0, len(cfg.Apps))
	seen := make(map[string]bool)

	for _, pc := range cfg.Apps {
		seen[pc.Name] = true
		mp := allApps[pc.Name]
		pvd := templates.AppViewData{Name: pc.Name, LaunchMode: pc.EffectiveLaunchMode(), AutoStart: pc.AutoStart, Debug: pc.Debug, HasIcon: appHasIcon(mp)}
		if mp != nil {
			pvd.State = mp.State
			pvd.Error = mp.Error
			pvd.Version = infoVersion(mp)
		}
		apps = append(apps, pvd)
	}

	for name, mp := range allApps {
		if !seen[name] {
			pvd := templates.AppViewData{
				Name:    name,
				State:   mp.State,
				Error:   mp.Error,
				Version: infoVersion(mp),
				HasIcon: appHasIcon(mp),
			}
			apps = append(apps, pvd)
		}
	}

	return apps
}

// removeWithRetry attempts os.RemoveAll up to maxAttempts times, waiting delay
// between attempts. On Windows, recently killed processes may hold file locks
// briefly, causing RemoveAll to fail with "Access is denied".
func removeWithRetry(path string, maxAttempts int, delay time.Duration) error {
	var err error
	for i := range maxAttempts {
		if err = os.RemoveAll(path); err == nil {
			return nil
		}
		if i < maxAttempts-1 {
			time.Sleep(delay)
		}
	}
	return err
}

// patchAppContent patches #app-content with inner mode via Datastar SSE.
func patchAppContent(sse *datastar.ServerSentEventGenerator, htmlStr string) {
	mustSSE(sse.PatchElements(htmlStr,
		datastar.WithSelector("#app-content"),
		datastar.WithMode(datastar.ElementPatchModeInner),
	))
}

// patchInstallError resets the installing state and shows an error inside the install dialog.
func patchInstallError(sse *datastar.ServerSentEventGenerator, msg string) {
	mustSSE(sse.PatchSignals([]byte(`{"installing":false}`)))
	mustSSE(sse.PatchElements(
		fmt.Sprintf(`<div id="install-error"><sl-alert variant="danger" open>%s</sl-alert></div>`, html.EscapeString(msg)),
		datastar.WithSelector("#install-error"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
}
