// Package runtimes manages installed runtime gateway packages and their
// running subprocesses.
//
// A runtime gateway is a .mass package: a Zip archive containing
//
//	runtime.yml         (manifest — see [Manifest])
//	bin/<binary>        (gateway executable; .exe on Windows)
//
// Once installed, MASS can launch the binary as a hashicorp/go-plugin
// subprocess and route /mass.<runtime_name>.* HTTP traffic into it via the
// RuntimeGateway gRPC contract defined in
// `mass-proto/proto/gateway/gateway.proto`. The gateway in turn calls back
// into MASS through the [scheduler.Scheduler] via the MassScheduler service
// hosted on go-plugin's broker.
package runtimes

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
)

// Errors surfaced to callers.
var (
	ErrRuntimeNotFound         = errors.New("runtime gateway not installed")
	ErrRuntimeAlreadyInstalled = errors.New("runtime gateway already installed")
	ErrRuntimeNotRunning       = errors.New("runtime gateway not running")
	ErrManifestMissing         = errors.New(".mass package missing runtime.yml")
	ErrBinaryMissing           = errors.New(".mass package missing bin/ binary")
)

// Manifest describes one runtime gateway package.
//
// `runtime.yml` is the on-disk authority for static identity (kind, version,
// display name). At runtime, the gateway's Init response can refresh
// display name and description — see [startGateway].
//
// AutoStart is operator state, persisted in the store and not in runtime.yml.
// The Manager reflects it onto the manifest on reload so callers see one
// coherent view of each installed runtime.
type Manifest struct {
	RuntimeName string `yaml:"runtime_name" json:"runtime_name"`
	Version     string `yaml:"version" json:"version"`
	DisplayName string `yaml:"display_name" json:"display_name"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	AutoStart   bool   `yaml:"-" json:"auto_start"`
	// Binary is the executable's path within the package, relative to the
	// archive root (e.g. "bin/mass-runtime-gateway-llama-cpp" or
	// "bin/mass-runtime-gateway-llama-cpp.exe"). When empty, the manager picks the
	// first executable in bin/.
	Binary string `yaml:"binary,omitempty" json:"binary,omitempty"`
}

// Manager owns the lifecycle of installed runtime gateways.
type Manager struct {
	dataDir string
	store   store.RuntimeStoreInterface
	logger  zerolog.Logger

	// sched, downloads, and logLevel are passed down to launched
	// gateways via the MassScheduler callback service. Set via
	// [SetScheduler] / [SetDownloads] / [SetLogLevel] before calling
	// Start.
	sched     *scheduler.Scheduler
	downloads *downloads.Manager
	logLevel  string

	mu              sync.RWMutex
	installed       map[string]Manifest       // runtime_name -> manifest (everything in the store)
	running         map[string]*LoadedGateway // runtime_name -> live subprocess
	cbSeq           uint64                    // monotonic id for AddOn* callback handles
	onStateChange   map[uint64]func(runtimeName string)
	onInstallChange map[uint64]func(installedKinds []string)
}

// NewManager builds a manager backed by {dataDir}/runtimes/ and the store.
// Pass [Manager.SetScheduler] before calling Start to enable callbacks.
func NewManager(dataDir string, st store.RuntimeStoreInterface, logger zerolog.Logger) (*Manager, error) {
	dir := config.RuntimesDir(dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, ctxerr.With(fmt.Errorf("creating runtimes dir: %w", err), map[string]any{"path": dir})
	}
	m := &Manager{
		dataDir:         dataDir,
		store:           st,
		logger:          logger.With().Str("component", "runtimes").Logger(),
		logLevel:        "info",
		installed:       make(map[string]Manifest),
		running:         make(map[string]*LoadedGateway),
		onStateChange:   make(map[uint64]func(runtimeName string)),
		onInstallChange: make(map[uint64]func(installedKinds []string)),
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	return m, nil
}

// SetScheduler wires the scheduler the manager hands to launched gateways.
// Must be called before [Manager.Start].
func (m *Manager) SetScheduler(s *scheduler.Scheduler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sched = s
}

// SetDownloads wires the downloads manager that backs gateway-
// initiated install callbacks (MassScheduler.DownloadFiles). Must
// be called before [Manager.Start] or DownloadFiles calls fail with
// Unavailable.
func (m *Manager) SetDownloads(d *downloads.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.downloads = d
}

// SetLogLevel sets the log_level passed to gateways at Init time.
func (m *Manager) SetLogLevel(level string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if level != "" {
		m.logLevel = level
	}
}

// reload reads the store and rebuilds the installed map. Called on startup
// and after install/uninstall.
func (m *Manager) reload() error {
	rows, err := m.store.ListRuntimes()
	if err != nil {
		return ctxerr.With(fmt.Errorf("listing runtimes: %w", err), nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.installed = make(map[string]Manifest, len(rows))
	for _, r := range rows {
		mf := Manifest{
			RuntimeName: r.RuntimeName,
			Version:     r.Version,
			DisplayName: r.DisplayName,
			Description: r.Description,
		}
		// Best-effort: refresh from on-disk runtime.yml so a fresh upgrade
		// reflects new fields without waiting for a restart.
		if onDisk, derr := readManifest(filepath.Join(r.InstallPath, "runtime.yml")); derr == nil {
			mf = onDisk
		}
		// AutoStart is store-owned; never overwritten by the on-disk manifest.
		mf.AutoStart = r.AutoStart
		m.installed[r.RuntimeName] = mf
	}
	return nil
}

// IsInstalled reports whether runtimeName is registered. Used by the worker
// hub to gate worker registrations.
func (m *Manager) IsInstalled(runtimeName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.installed[runtimeName]
	return ok
}

// IsRunning reports whether the runtime's subprocess is currently up.
func (m *Manager) IsRunning(runtimeName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.running[runtimeName]
	return ok
}

// List returns every installed runtime ordered by display name.
func (m *Manager) List() []Manifest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Manifest, 0, len(m.installed))
	for _, mf := range m.installed {
		out = append(out, mf)
	}
	return out
}

// Get returns the manifest for runtimeName, or [ErrRuntimeNotFound].
func (m *Manager) Get(runtimeName string) (Manifest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mf, ok := m.installed[runtimeName]
	if !ok {
		return Manifest{}, ctxerr.With(ErrRuntimeNotFound, map[string]any{"runtime_name": runtimeName})
	}
	return mf, nil
}

// LoadedGatewayFor returns the running gateway for runtimeName, or
// [ErrRuntimeNotRunning].
func (m *Manager) LoadedGatewayFor(runtimeName string) (*LoadedGateway, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.running[runtimeName]
	if !ok {
		return nil, ctxerr.With(ErrRuntimeNotRunning, map[string]any{"runtime_name": runtimeName})
	}
	return g, nil
}

// RunningGateways returns every currently-running gateway. Callers use it
// to fan out per-runtime work (HF install routing, the Models stream
// multiplexer).
func (m *Manager) RunningGateways() []*LoadedGateway {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*LoadedGateway, 0, len(m.running))
	for _, g := range m.running {
		out = append(out, g)
	}
	return out
}

// InstallFromPath unpacks a .mass runtime package into
// {dataDir}/runtimes/{runtime_name}/ and registers it in the store.
// Returns [ErrRuntimeAlreadyInstalled] when the kind already exists; call
// Uninstall first to replace.
func (m *Manager) InstallFromPath(packagePath string) (Manifest, error) {
	mf, installDir, err := m.extractPackage(packagePath)
	if err != nil {
		return Manifest{}, err
	}
	// Default new installs to AutoStart=true so the operator's first
	// experience is "install → it's running" instead of "install → click
	// Start → wait." Existing rows are untouched; the operator's Stop /
	// disable-autostart toggles in the Runtimes tab are still the source
	// of truth for follow-up changes.
	if err := m.store.UpsertRuntime(store.RuntimeRow{
		RuntimeName: mf.RuntimeName,
		Version:     mf.Version,
		DisplayName: mf.DisplayName,
		Description: mf.Description,
		InstallPath: installDir,
		AutoStart:   true,
	}); err != nil {
		_ = removeInstallArtifacts(installDir)
		return Manifest{}, err
	}
	if err := m.reload(); err != nil {
		return Manifest{}, err
	}
	m.logger.Info().Str("runtime_name", mf.RuntimeName).Str("version", mf.Version).Msg("runtime installed")
	m.fireInstallChange()
	// Start the gateway synchronously so the caller's SSE response can
	// render the row in its post-start "running" state. Init typically
	// takes ~100ms — the Init timeout in startGateway is 30s but that's
	// the failure ceiling, not the steady-state cost. A start failure
	// is logged and reflected in the row state; the install itself
	// stays committed since the binary is on disk and the row is in
	// the store, so the operator can retry from the Runtimes tab.
	if _, err := m.Start(context.Background(), mf.RuntimeName); err != nil {
		m.logger.Warn().Err(err).Str("runtime_name", mf.RuntimeName).Msg("auto-start after install failed")
	}
	return mf, nil
}

// extractPackage reads the .mass archive (Zip), validates it, and writes it
// to {dataDir}/runtimes/{runtime_name}/. Returns the parsed manifest and the
// install directory.
func (m *Manager) extractPackage(packagePath string) (Manifest, string, error) {
	zr, err := zip.OpenReader(packagePath)
	if err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("opening package: %w", err), map[string]any{"path": packagePath})
	}
	defer func() { _ = zr.Close() }()

	// Find runtime.yml first to get the runtime_name for the install dir.
	var manifestEntry *zip.File
	for _, f := range zr.File {
		if filepath.ToSlash(f.Name) == "runtime.yml" {
			manifestEntry = f
			break
		}
	}
	if manifestEntry == nil {
		return Manifest{}, "", ctxerr.With(ErrManifestMissing, map[string]any{"path": packagePath})
	}
	rc, err := manifestEntry.Open()
	if err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("opening runtime.yml: %w", err), map[string]any{"path": packagePath})
	}
	manifestBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("reading runtime.yml: %w", err), map[string]any{"path": packagePath})
	}
	var mf Manifest
	if err := yaml.Unmarshal(manifestBytes, &mf); err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("parsing runtime.yml: %w", err), map[string]any{"path": packagePath})
	}
	if err := validateRuntimeName(mf.RuntimeName); err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("runtime.yml: %w", err), map[string]any{"path": packagePath})
	}

	// Reject already-installed runtimes before touching the disk. Clearing
	// the install dir here would try to delete the running gateway binary,
	// which the OS keeps locked (Windows: "Access is denied").
	if m.IsInstalled(mf.RuntimeName) {
		return Manifest{}, "", ctxerr.With(ErrRuntimeAlreadyInstalled, map[string]any{"runtime_name": mf.RuntimeName})
	}

	installDir := config.RuntimeDir(m.dataDir, mf.RuntimeName)
	// Clear any leftover install artifacts from a previous attempt
	// (or from a prior uninstall) but preserve gateway state files —
	// catalogues etc. survive re-installs so the operator's model
	// groups carry over.
	if err := removeInstallArtifacts(installDir); err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("clearing install dir: %w", err), map[string]any{"path": installDir})
	}
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return Manifest{}, "", ctxerr.With(fmt.Errorf("creating install dir: %w", err), map[string]any{"path": installDir})
	}

	if err := unzipInto(zr, installDir); err != nil {
		_ = removeInstallArtifacts(installDir)
		return Manifest{}, "", err
	}

	// Resolve the binary path the gateway will be launched with.
	if _, err := resolveBinary(installDir, mf); err != nil {
		_ = removeInstallArtifacts(installDir)
		return Manifest{}, "", err
	}
	return mf, installDir, nil
}

// Uninstall stops the gateway (best-effort), removes the row, and
// removes the install artifacts. The gateway's persistent state
// (e.g. model catalogue files) is intentionally preserved so a
// re-install picks up where the operator left off. Convention:
// state files end with "-catalogue.json" or live in a "state/"
// subdirectory of the install dir. When a state-format breaking
// change ships, the *runtime* should drop its own state on load;
// MASS doesn't second-guess. Fires the install-change callback so
// subsystems holding per-runtime state can reconcile against the
// new installed set.
func (m *Manager) Uninstall(runtimeName string) error {
	if !m.IsInstalled(runtimeName) {
		return nil
	}
	if err := m.Stop(runtimeName); err != nil && !errors.Is(err, ErrRuntimeNotRunning) {
		m.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("stopping runtime before uninstall")
	}
	if err := m.store.DeleteRuntime(runtimeName); err != nil {
		return err
	}
	if err := removeInstallArtifacts(config.RuntimeDir(m.dataDir, runtimeName)); err != nil {
		m.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("removing runtime install artifacts")
	}
	if err := m.reload(); err != nil {
		return err
	}
	m.fireInstallChange()
	return nil
}

// removeInstallArtifacts deletes everything under installDir EXCEPT
// the gateway's persistent state — files matching "*-catalogue.json"
// at the top level and the "state/" subdirectory. Missing dir is fine.
func removeInstallArtifacts(installDir string) error {
	entries, err := os.ReadDir(installDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if name == "state" && e.IsDir() {
			continue
		}
		if !e.IsDir() && strings.HasSuffix(name, "-catalogue.json") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(installDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// fireInstallChange invokes every install-change callback with the snapshot
// of currently-installed runtime kinds. Callbacks run in a goroutine so a
// slow subscriber can't stall Install / Uninstall HTTP handlers.
func (m *Manager) fireInstallChange() {
	m.mu.RLock()
	cbs := make([]func(installedKinds []string), 0, len(m.onInstallChange))
	for _, cb := range m.onInstallChange {
		cbs = append(cbs, cb)
	}
	kinds := make([]string, 0, len(m.installed))
	for k := range m.installed {
		kinds = append(kinds, k)
	}
	m.mu.RUnlock()
	for _, cb := range cbs {
		go cb(kinds)
	}
}

// AddOnInstallChange registers a callback fired after Install / Uninstall
// completes, with the snapshot of currently-installed runtime kinds.
// Returns a stop function that deregisters the callback; SSE handlers
// must call it on connection close to avoid leaking subscribers.
func (m *Manager) AddOnInstallChange(fn func(installedKinds []string)) func() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cbSeq++
	id := m.cbSeq
	m.onInstallChange[id] = fn
	return func() {
		m.mu.Lock()
		delete(m.onInstallChange, id)
		m.mu.Unlock()
	}
}

// Start launches the gateway subprocess for runtimeName. Idempotent: a
// second Start while running returns the live gateway. Returns
// [ErrRuntimeNotFound] when not installed.
func (m *Manager) Start(ctx context.Context, runtimeName string) (*LoadedGateway, error) {
	mf, err := m.Get(runtimeName)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if g, ok := m.running[runtimeName]; ok {
		m.mu.Unlock()
		return g, nil
	}
	sched := m.sched
	dl := m.downloads
	logLevel := m.logLevel
	m.mu.Unlock()

	installDir := config.RuntimeDir(m.dataDir, runtimeName)
	binary, err := resolveBinary(installDir, mf)
	if err != nil {
		return nil, err
	}

	// Pass the install dir as the gateway's data_dir — the gateway
	// keeps its persistent state (model catalogue, etc.) inside it.
	// Uninstall preserves state files (see [Manager.Uninstall]); a
	// re-install lands fresh artifacts alongside the old state so
	// the operator's model groups survive runtime upgrades.
	//
	// models_dir is the shared models root: gateways walk
	// <root>/<each format they handle>/ themselves. The flat-by-
	// format layout lets multiple runtimes that share a format see
	// the same files without mirroring.
	gw, err := startGateway(ctx, mf, binary, installDir, config.ModelsDir(m.dataDir), logLevel, sched, dl, m.logger)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.running[runtimeName] = gw
	gw.Manifest.AutoStart = m.installed[runtimeName].AutoStart // preserve operator state
	m.installed[runtimeName] = gw.Manifest                     // refresh with Init response
	m.mu.Unlock()
	go m.watchGatewayExit(runtimeName, gw, gatewayExitPollInterval)
	m.fireStateChange(runtimeName)
	return gw, nil
}

// gatewayExitPollInterval is how often the exit watcher checks the gateway
// subprocess. go-plugin exposes subprocess liveness only as a polling
// predicate ([plugin.Client.Exited]), so a short ticker is the mechanism.
const gatewayExitPollInterval = 2 * time.Second

// watchGatewayExit reconciles the manager when a gateway subprocess dies
// out from under it (crash, OOM kill): without this the runtime stays
// "running" forever and every proxied request fails opaquely. One watcher
// goroutine per launched gateway; it exits when the gateway is stopped or
// replaced through the normal path (removed from m.running) or when the
// crash is handled. No auto-restart — the operator decides.
func (m *Manager) watchGatewayExit(runtimeName string, gw *LoadedGateway, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.RLock()
		current, ok := m.running[runtimeName]
		m.mu.RUnlock()
		if !ok || current != gw {
			return // stopped / replaced via Stop, Shutdown, or a fresh Start
		}
		if !gw.exited() {
			continue
		}
		m.mu.Lock()
		stillCurrent := m.running[runtimeName] == gw
		if stillCurrent {
			delete(m.running, runtimeName)
		}
		m.mu.Unlock()
		if stillCurrent {
			gw.Close() // release the callback server; Kill on a dead process is a no-op
			m.logger.Error().Str("runtime_name", runtimeName).Msg("gateway subprocess exited unexpectedly")
			m.fireStateChange(runtimeName)
		}
		return
	}
}

// Stop terminates a running gateway. Returns [ErrRuntimeNotRunning] when
// nothing is up — callers may treat that as success.
func (m *Manager) Stop(runtimeName string) error {
	m.mu.Lock()
	g, ok := m.running[runtimeName]
	if !ok {
		m.mu.Unlock()
		return ctxerr.With(ErrRuntimeNotRunning, map[string]any{"runtime_name": runtimeName})
	}
	delete(m.running, runtimeName)
	m.mu.Unlock()

	g.Close()
	m.fireStateChange(runtimeName)
	return nil
}

// SetAutoStart toggles the auto_start flag for runtimeName in the store and
// in the in-memory manifest. Returns [ErrRuntimeNotFound] when not installed.
func (m *Manager) SetAutoStart(runtimeName string, autoStart bool) error {
	m.mu.Lock()
	mf, ok := m.installed[runtimeName]
	if !ok {
		m.mu.Unlock()
		return ctxerr.With(ErrRuntimeNotFound, map[string]any{"runtime_name": runtimeName})
	}
	m.mu.Unlock()

	if err := m.store.SetRuntimeAutoStart(runtimeName, autoStart); err != nil {
		return err
	}
	m.mu.Lock()
	mf.AutoStart = autoStart
	m.installed[runtimeName] = mf
	m.mu.Unlock()
	m.fireStateChange(runtimeName)
	return nil
}

// FireStateChange notifies state-change subscribers that something they
// care about has changed for runtimeName. Lifecycle code calls this
// internally; callers outside the manager use it to surface catalogue
// changes (e.g. a model rename) so the Models SSE re-renders.
func (m *Manager) FireStateChange(runtimeName string) { m.fireStateChange(runtimeName) }

// fireStateChange invokes every state-change callback for runtimeName.
// Callbacks run in a goroutine so a slow subscriber (e.g. an SSE renderer
// that triggers a cold ListModels parse) can't stall the caller's HTTP
// handler — Start / Stop / SetAutoStart all return as soon as the
// in-memory state has flipped.
func (m *Manager) fireStateChange(runtimeName string) {
	m.mu.RLock()
	cbs := make([]func(string), 0, len(m.onStateChange))
	for _, cb := range m.onStateChange {
		cbs = append(cbs, cb)
	}
	m.mu.RUnlock()
	for _, cb := range cbs {
		go cb(runtimeName)
	}
}

// AddOnStateChange registers a callback fired whenever a runtime's running
// state, auto_start flag, or catalogue changes. Used by the web layer to
// push SSE updates to the dashboard. Returns a stop function that
// deregisters the callback; SSE handlers must call it on connection close.
func (m *Manager) AddOnStateChange(fn func(runtimeName string)) func() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cbSeq++
	id := m.cbSeq
	m.onStateChange[id] = fn
	return func() {
		m.mu.Lock()
		delete(m.onStateChange, id)
		m.mu.Unlock()
	}
}

// AutoStartAll launches every installed runtime whose AutoStart flag is set.
// Logs failures but does not abort — a broken runtime should not prevent
// MASS from booting. Called by main during startup.
func (m *Manager) AutoStartAll(ctx context.Context) {
	m.mu.RLock()
	kinds := make([]string, 0, len(m.installed))
	for kind, mf := range m.installed {
		if mf.AutoStart {
			kinds = append(kinds, kind)
		}
	}
	m.mu.RUnlock()
	for _, kind := range kinds {
		if _, err := m.Start(ctx, kind); err != nil {
			m.logger.Warn().Err(err).Str("runtime_name", kind).Msg("auto-start failed")
		}
	}
}

// Shutdown stops every running gateway. Called on MASS shutdown.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	gws := make([]*LoadedGateway, 0, len(m.running))
	for _, g := range m.running {
		gws = append(gws, g)
	}
	m.running = map[string]*LoadedGateway{}
	m.mu.Unlock()

	for _, g := range gws {
		g.Close()
	}
}

// --- Internals ---

// runtimeNamePattern is the shape an installable runtime_name must match.
// The name becomes both a directory under {dataDir}/runtimes/ (so it must
// be a safe path segment) and the /mass.<runtime_name>.* HTTP mount (so it
// must be a plain lowercase slug).
var runtimeNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// validateRuntimeName rejects manifest runtime_names that would escape the
// runtimes directory or collide with reserved API namespaces. "v1" is
// reserved: gateway traffic mounts at /mass.<runtime_name>.* and the public
// API owns /mass.v1.Mass.
func validateRuntimeName(name string) error {
	if name == "" {
		return errors.New("runtime_name required")
	}
	if !runtimeNamePattern.MatchString(name) {
		return fmt.Errorf("runtime_name %q invalid: must match %s", name, runtimeNamePattern)
	}
	if name == "v1" {
		return errors.New(`runtime_name "v1" is reserved (collides with the /mass.v1.Mass public API namespace)`)
	}
	return nil
}

func readManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var mf Manifest
	if err := yaml.Unmarshal(data, &mf); err != nil {
		return Manifest{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	return mf, nil
}

func unzipInto(zr *zip.ReadCloser, dest string) error {
	for _, f := range zr.File {
		// Reject any entry whose target escapes the install dir (safe-path).
		clean := filepath.Clean(filepath.FromSlash(f.Name))
		if strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
			return fmt.Errorf("zip: path escape %q", f.Name)
		}
		target := filepath.Join(dest, clean)
		// Defense in depth: even after cleaning, ensure the resolved path
		// stays within dest (handles cases where dest contains symlinks).
		if !strings.HasPrefix(target, filepath.Clean(dest)+string(filepath.Separator)) && target != filepath.Clean(dest) {
			return fmt.Errorf("zip: path escape %q (resolved outside dest)", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", target, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("mkdir parent of %s: %w", target, err)
		}
		mode := f.Mode()
		if mode == 0 {
			mode = 0o644
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
		if err != nil {
			return fmt.Errorf("opening %s: %w", target, err)
		}
		rc, err := f.Open()
		if err != nil {
			_ = out.Close()
			return fmt.Errorf("opening zip entry %s: %w", f.Name, err)
		}
		if _, err := io.Copy(out, rc); err != nil {
			_ = rc.Close()
			_ = out.Close()
			return fmt.Errorf("extracting %s: %w", target, err)
		}
		_ = rc.Close()
		if err := out.Close(); err != nil {
			return fmt.Errorf("closing %s: %w", target, err)
		}
	}
	return nil
}

// resolveBinary returns the absolute path to the gateway binary inside an
// installed package, honoring the manifest's `binary` hint and falling back
// to the first executable in bin/.
func resolveBinary(installDir string, mf Manifest) (string, error) {
	candidate := mf.Binary
	if candidate == "" {
		// Pick the first regular file in bin/. Prefer .exe on Windows.
		entries, err := os.ReadDir(filepath.Join(installDir, "bin"))
		if err != nil {
			return "", ctxerr.With(ErrBinaryMissing, map[string]any{"path": filepath.Join(installDir, "bin"), "runtime_name": mf.RuntimeName})
		}
		preferred := ""
		fallback := ""
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if runtime.GOOS == "windows" && strings.HasSuffix(strings.ToLower(name), ".exe") && preferred == "" {
				preferred = name
			} else if fallback == "" {
				fallback = name
			}
		}
		switch {
		case preferred != "":
			candidate = filepath.Join("bin", preferred)
		case fallback != "":
			candidate = filepath.Join("bin", fallback)
		default:
			return "", ctxerr.With(ErrBinaryMissing, map[string]any{"path": filepath.Join(installDir, "bin"), "runtime_name": mf.RuntimeName})
		}
	}
	abs := filepath.Join(installDir, filepath.FromSlash(candidate))
	if _, err := os.Stat(abs); err != nil {
		// Cross-platform manifests typically write the binary path without
		// an extension. On Windows the file ends in .exe — try that before
		// giving up so the same .mass package works on both OSes.
		if runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(abs), ".exe") {
			withExe := abs + ".exe"
			if _, err2 := os.Stat(withExe); err2 == nil {
				return withExe, nil
			}
		}
		return "", ctxerr.With(fmt.Errorf("gateway binary missing: %w", err), map[string]any{"runtime_name": mf.RuntimeName, "path": abs})
	}
	return abs, nil
}
