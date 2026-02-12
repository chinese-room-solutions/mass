package web

import (
	"encoding/json"
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

func (h *Handler) handleInstallModule(w http.ResponseWriter, r *http.Request) {
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

	command, meta, err := h.installer.InstallFromArchive(signals.PackagePath)
	if err != nil {
		patchInstallError(sse, "Install failed: "+err.Error())
		return
	}

	// Auto-detect config.yml next to the installed binary.
	moduleDir := filepath.Dir(command)
	configPath := ""
	if _, err := os.Stat(filepath.Join(moduleDir, "config.yml")); err == nil {
		configPath = "${MODULE_DIR}/config.yml"
	}

	// Check for existing module with same name.
	if existing := h.cfg.FindModule(meta.Name); existing != nil {
		// Update existing entry (upgrade).
		existing.Command = command
		existing.Version = meta.Version
		existing.Source = "package"
		existing.Config = configPath
	} else {
		// Create new module config entry.
		moduleCfg := config.ModuleConfig{
			Name:    meta.Name,
			Command: command,
			Version: meta.Version,
			Source:  "package",
			Config:  configPath,
		}
		h.cfg.Modules = append(h.cfg.Modules, moduleCfg)
	}
	h.saveFn()

	// Register the newly installed module (no subprocess started).
	moduleCfg := h.cfg.FindModule(meta.Name)
	if err := h.orch.Register(moduleCfg); err != nil {
		patchInstallError(sse, "Installed but registration failed: "+err.Error())
		return
	}

	// Re-render sidebar.
	allModules := h.orch.GetAllModules()
	modules := buildModuleList(h.cfg, allModules)
	_ = sse.PatchElements(templates.RenderModuleList(modules, meta.Name),
		datastar.WithSelector("#module-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	)

	patchContent(sse, `<div class="flex flex-col items-center justify-center h-64 text-center"><h2 class="text-lg font-semibold mb-2">Module installed</h2><p class="text-neutral-400 text-sm">Press play to start.</p></div>`)

	if b, err := json.Marshal(map[string]any{"addModuleOpen": false, "installing": false, "packagePath": ""}); err == nil {
		_ = sse.PatchSignals(b)
	}
}

func (h *Handler) handleModuleIcon(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	mp := h.orch.GetModule(name)
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
	iconPath := filepath.Join(config.ModuleVersionDir(dataDir, name, version), mp.DiskMeta.Icon)
	http.ServeFile(w, r, iconPath)
}

func (h *Handler) handleStartModule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	h.logger.Info().Str("module", name).Msg("starting module")
	sse := datastar.NewSSE(w, r)

	if err := h.orch.Start(name); err != nil {
		h.logger.Error().Err(err).Str("module", name).Msg("failed to start module")
		_ = sse.PatchElements(templates.RenderError(err.Error()))
		return
	}

	// Immediate feedback — status updates come via /api/events SSE.
	_ = sse.PatchElements(templates.RenderModuleStatus(name, scheduler.StateStarting, nil, ""))
}

func (h *Handler) handleStopModule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)

	if err := h.orch.Stop(name); err != nil {
		_ = sse.PatchElements(templates.RenderError(err.Error()))
		return
	}

	_ = sse.PatchElements(templates.RenderModuleStatus(name, scheduler.StateStopped, nil, ""))
}

func (h *Handler) handleDeselectModule(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	if b, err := json.Marshal(map[string]any{"activeModule": ""}); err == nil {
		_ = sse.PatchSignals(b)
	}
	modules := buildModuleList(h.cfg, h.orch.GetAllModules())
	patchContent(sse, templates.RenderWelcomeState(len(modules) == 0))
}

func (h *Handler) handleRemoveModule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)

	h.orch.Remove(name)
	h.cfg.RemoveModule(name)
	h.saveFn()

	// Delete the module's data directory and its models.
	// On Windows, file locks from the just-killed subprocess may linger briefly.
	if dataDir, err := h.cfg.EffectiveDataDir(); err == nil {
		if err := removeWithRetry(filepath.Join(dataDir, "modules", name), 5, 500*time.Millisecond); err != nil {
			h.logger.Warn().Err(err).Str("module", name).Msg("failed to remove module directory")
		}
		if err := removeWithRetry(filepath.Join(dataDir, "models", name), 5, 500*time.Millisecond); err != nil {
			h.logger.Warn().Err(err).Str("module", name).Msg("failed to remove module models directory")
		}
	}

	// Re-render the sidebar module list and clear main content.
	allModules := h.orch.GetAllModules()
	modules := buildModuleList(h.cfg, allModules)
	_ = sse.PatchElements(templates.RenderModuleList(modules, ""),
		datastar.WithSelector("#module-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	)
	// Remove module-specific signals (e.g. modelPath, contextSize) from the
	// Datastar store BEFORE patching content. When Datastar processes new
	if b, err := json.Marshal(map[string]any{"activeModule": ""}); err == nil {
		_ = sse.PatchSignals(b)
	}

	patchContent(sse, templates.RenderWelcomeState(len(modules) == 0))
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
		_ = sse.PatchSignals(b)
	}
}

func (h *Handler) handleSetLaunchMode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindModule(name)
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

	// If switching to on-demand and the module is already running, arm the
	// idle timer so it will stop after the configured timeout.
	if mode == config.LaunchModeOnDemand {
		h.orch.ArmIdleTimer(name)
	}

	sse := datastar.NewSSE(w, r)
	_ = sse.PatchElements(
		templates.RenderLaunchModeDropdown(name, mode),
		datastar.WithSelector("#launch-mode-"+name),
		datastar.WithModeOuter(),
	)
}

func (h *Handler) handleToggleAutoStart(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindModule(name)
	if pc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pc.AutoStart = !pc.AutoStart
	h.saveFn()

	// Re-render the sidebar so the auto-start icon updates.
	sse := datastar.NewSSE(w, r)
	modules := buildModuleList(h.cfg, h.orch.GetAllModules())
	_ = sse.PatchElements(templates.RenderModuleList(modules, name))
}

func (h *Handler) handleToggleDebug(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pc := h.cfg.FindModule(name)
	if pc == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pc.Debug = !pc.Debug
	h.saveFn()

	// Switching modes requires re-registration: stop current process and
	// re-register with the new debug setting.
	mp := h.orch.GetModule(name)
	if mp != nil && mp.State != scheduler.StateRunning {
		h.orch.Remove(name)
		if err := h.orch.Register(pc); err != nil {
			h.logger.Error().Err(err).Str("module", name).Msg("re-register after debug toggle failed")
		}
		h.saveFn()
	}

	// Re-render the sidebar so the bug icon color updates.
	sse := datastar.NewSSE(w, r)
	modules := buildModuleList(h.cfg, h.orch.GetAllModules())
	_ = sse.PatchElements(templates.RenderModuleList(modules, name))
}

func (h *Handler) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var signals struct {
		ListenAddr        string `json:"listenAddr"`
		DataDir           string `json:"dataDir"`
		AuthToken         string `json:"authToken"`
		AuthTokenEdited   bool   `json:"authTokenEdited"`
		ModelIdleTimeout  string `json:"modelIdleTimeout"`
		ModuleIdleTimeout string `json:"moduleIdleTimeout"`
		LogLevel          string `json:"logLevel"`
		DevMode           bool   `json:"devMode"`
		TLSEnabled        bool   `json:"tlsEnabled"`
		TLSCertFile       string `json:"tlsCertFile"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		sse := datastar.NewSSE(w, r)
		_ = sse.PatchElements(templates.RenderError("Invalid request"))
		return
	}

	h.cfg.ListenAddr = signals.ListenAddr
	h.cfg.DataDir = signals.DataDir
	h.cfg.ModelIdleTimeout = signals.ModelIdleTimeout
	h.cfg.ModuleIdleTimeout = signals.ModuleIdleTimeout
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
	// This prevents auto-saves triggered by other setting changes from deleting
	// the token (empty authToken signal sent when token wasn't touched).
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

func buildModuleList(cfg *config.Config, allModules map[string]*scheduler.ManagedModule) []templates.ModuleViewData {
	modules := make([]templates.ModuleViewData, 0, len(cfg.Modules))
	seen := make(map[string]bool)

	for _, pc := range cfg.Modules {
		seen[pc.Name] = true
		mp := allModules[pc.Name]
		pvd := templates.ModuleViewData{Name: pc.Name, LaunchMode: pc.EffectiveLaunchMode(), AutoStart: pc.AutoStart, Debug: pc.Debug, HasIcon: moduleHasIcon(mp)}
		if mp != nil {
			pvd.State = mp.State
			pvd.Error = mp.Error
			pvd.Version = infoVersion(mp)
		}
		modules = append(modules, pvd)
	}

	for name, mp := range allModules {
		if !seen[name] {
			pvd := templates.ModuleViewData{
				Name:    name,
				State:   mp.State,
				Error:   mp.Error,
				Version: infoVersion(mp),
				HasIcon: moduleHasIcon(mp),
			}
			modules = append(modules, pvd)
		}
	}

	return modules
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
