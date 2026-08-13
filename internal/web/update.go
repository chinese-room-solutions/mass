// MASS updates itself by running its own installer over its own install: the
// daemon notices a newer release, downloads that release's mass-setup, and
// hands it the recorded install directory with --relaunch. The setup waits for
// this process to exit, stages the new build, and starts the app again.
//
// The check is one goroutine that may fail silently (being offline is the
// normal case, not an error worth surfacing); the apply is a deliberate request
// that reports its failures properly — including the two MASS-only refusals
// below, the fleet gate and the operator-managed daemon.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass-sdk/selfupdate"
	"github.com/chinese-room-solutions/mass/internal/appspec"
)

// UpdaterInterface is the release surface the daemon needs, over mass-sdk's
// selfupdate. It exists so the check and the apply can be driven by a stub in
// tests without reaching for the network or a real installer.
type UpdaterInterface interface {
	// Latest is the newest published release tag at baseURL.
	Latest(ctx context.Context, baseURL string) (string, error)
	// IsNewer reports whether latest supersedes current. It is false for a dev
	// build, which is how a from-source run opts out of updates.
	IsNewer(current, latest string) bool
	// FetchSetup downloads tag's asset into destDir, verifies it against the
	// release's SHA256SUMS, makes it executable, and returns its path.
	FetchSetup(ctx context.Context, baseURL, tag, asset, destDir string) (string, error)
}

// SDKUpdater is the real implementation, backed by mass-sdk/selfupdate.
type SDKUpdater struct{}

func (SDKUpdater) Latest(ctx context.Context, baseURL string) (string, error) {
	return selfupdate.Latest(ctx, baseURL)
}

func (SDKUpdater) IsNewer(current, latest string) bool { return selfupdate.IsNewer(current, latest) }

func (SDKUpdater) FetchSetup(ctx context.Context, baseURL, tag, asset, destDir string) (string, error) {
	return selfupdate.FetchSetup(ctx, baseURL, tag, asset, destDir)
}

// updateState holds what the daemon knows about a newer release: the tag, or ""
// while none is known. One goroutine writes it at startup and every reader (the
// check endpoint, the apply) takes the lock.
type updateState struct {
	mu        sync.Mutex
	available string
}

func (u *updateState) get() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.available
}

func (u *updateState) set(tag string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.available = tag
}

// updateCheckTimeout bounds the startup check. Nothing waits on it, so this only
// stops a hung connection from holding the goroutine open forever.
const updateCheckTimeout = 30 * time.Second

// CheckForUpdate asks the configured repository for the newest release and
// records it when it supersedes this build. It never fails loudly: an
// unreachable repository is the normal state of an offline machine, so the
// outcome is a debug line. Blocking is fine — the caller runs it on its own
// goroutine, and ctx ends it when the daemon stops.
func (h *Handler) CheckForUpdate(ctx context.Context) {
	if h.updater == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, updateCheckTimeout)
	defer cancel()

	latest, err := h.updater.Latest(ctx, h.updateURL)
	if err != nil {
		h.logger.Debug().Err(err).Str("url", h.updateURL).Msg("checking for a newer MASS release")
		return
	}
	// IsNewer owns the dev-build guard: a from-source build never sees an update.
	if !h.updater.IsNewer(h.version, latest) {
		h.logger.Debug().Str("running", h.version).Str("latest", latest).Msg("MASS is up to date")
		return
	}
	h.update.set(latest)
	h.logger.Info().Str("running", h.version).Str("available", latest).Msg("a newer MASS release is available")
}

// Apply-side refusals the handler maps to 409. Each is the operator's
// situation rather than a fault, so each answers with the sentence to show.
var (
	// errNotInstalled reports an update asked for by a build that no installer
	// placed — a `go run`, a binary copied by hand. There is no recorded install
	// dir, and guessing one would overwrite something we don't own.
	errNotInstalled = errors.New(
		"this MASS wasn't installed by the MASS installer, so it can't update itself — " +
			"download the latest installer and run it")
	// errNeedsElevation reports an install this user can't rewrite (a machine-wide
	// directory). v1 doesn't elevate; the installer does, so send them there.
	errNeedsElevation = errors.New(
		"this MASS is installed system-wide, so updating it needs administrator rights — " +
			"download the latest installer and run it")
	// errOperatorManaged refuses to restart a daemon nobody asked to be
	// restarted. An on-demand daemon belongs to the GUI/CLI that spawned it and
	// comes back by itself; a `mass serve` is a fleet hub someone runs
	// deliberately, and taking it down from under its workers is not something a
	// dashboard click gets to do.
	errOperatorManaged = errors.New(
		"this MASS runs as an operator-managed server, so it won't restart itself — " +
			"stop it and run the installer, or update it from the desktop app")
)

// errIncompatibleFleet is the fleet gate's refusal: connected workers the
// candidate build would strand. Count carries how many, Names a sample for the
// message. It is only advisory in the sense that {"force": true} overrides it.
type errIncompatibleFleet struct {
	Count int
	Names []string
	Tag   string
}

func (e *errIncompatibleFleet) Error() string {
	msg := fmt.Sprintf("%s would strand %s", e.Tag, workersPhrase(e.Count))
	if len(e.Names) > 0 {
		msg += " (" + joinNames(e.Names) + ")"
	}
	return msg + " — a stranded worker exits and stays down until it is upgraded. " +
		"Upgrade the workers first, or send {\"force\": true} to update anyway."
}

func workersPhrase(n int) string {
	if n == 1 {
		return "1 connected worker"
	}
	return fmt.Sprintf("%d connected workers", n)
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// loadRecord reads the install record mass-setup wrote — where this MASS lives,
// and therefore what an update reinstalls over. Through a field so a test can
// stand in for a machine that has (or hasn't) been installed; nil means the
// real one.
func (h *Handler) loadRecord() (*install.Record, error) {
	if h.recordFn != nil {
		return h.recordFn()
	}
	return appspec.Spec.LoadRecord()
}

// updateStageDir is where a fetched installer lands: under MASS's own data dir,
// not the system temp. Keeping it in a directory MASS owns means the download's
// provenance and lifetime are ours (macOS treats quarantine and temp cleanup
// differently there), and stale leftovers are ours to clear.
const updateStageDir = "update"

// applyUpdate downloads tag's installer and runs it over the recorded install,
// detached, so it can replace this process's own files once it exits. It returns
// once the installer is running — the caller answers the request and shuts the
// daemon down.
func (h *Handler) applyUpdate(ctx context.Context, tag string) error {
	// An operator-managed `mass serve` is a fleet hub: its workers reconnect
	// after a restart, but choosing WHEN that happens is the operator's. Same
	// guardrail ensureDaemon applies when a skewed client meets a non-on-demand
	// daemon (cmd/mass/launch.go).
	if !h.onDemand {
		return errOperatorManaged
	}
	rec, err := h.loadRecord()
	if err != nil {
		return fmt.Errorf("reading the install record: %w", err)
	}
	if rec == nil || rec.InstallDir == "" {
		return errNotInstalled
	}
	if !writableDir(rec.InstallDir) {
		return errNeedsElevation
	}

	dir, err := h.updateStagePath()
	if err != nil {
		return err
	}
	setupPath, err := h.updater.FetchSetup(ctx, h.updateURL, tag, setupAssetName(), dir)
	if err != nil {
		return fmt.Errorf("downloading the MASS %s installer: %w", tag, err)
	}

	return h.runSetupDetached(setupPath, rec.InstallDir)
}

// setupAssetName is the release asset this platform installs from — the naked
// mass-setup binary for the running OS/arch.
func setupAssetName() string {
	name := "mass-setup_" + runtime.GOOS + "_" + runtime.GOARCH
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return name
}

// updateStagePath returns an empty directory under the data dir for the
// download. A previous update's leftovers are cleared first: the installer it
// holds has already run, and an interrupted download must not be mistaken for a
// good one.
func (h *Handler) updateStagePath() (string, error) {
	if h.dataDir == "" {
		return "", errors.New("no data directory is configured for the update download")
	}
	dir := filepath.Join(h.dataDir, updateStageDir)
	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("clearing the previous update download: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("preparing the update download dir: %w", err)
	}
	return dir, nil
}

// writableDir reports whether this process can rewrite the contents of dir —
// the question "can we install over it without elevation?", answered by doing
// the smallest version of the write rather than by reading permission bits
// (which say nothing useful on Windows).
func writableDir(dir string) bool {
	f, err := os.CreateTemp(dir, ".mass-update-probe-*")
	if err != nil {
		return false
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return true
}

// setupArgs is the non-interactive install the downloaded setup is asked to
// perform: exactly the recorded install, in the scope that dir implies, with
// the relaunch that brings the app back afterwards. Spelling the scope out
// matters — the setup's own default is per-scope, which would move a custom
// install directory.
func setupArgs(installDir string) []string {
	scope := install.ScopeSystem
	if install.IsUserScoped(installDir) {
		scope = install.ScopeUser
	}
	return []string{"--install", "--install-dir", installDir, "--scope", string(scope), "--relaunch"}
}

// runSetupDetached starts the downloaded installer as a detached child that
// outlives this process — it has to, since its whole job is to replace the
// binaries this process is running from.
func (h *Handler) runSetupDetached(setupPath, installDir string) error {
	cmd := exec.Command(setupPath, setupArgs(installDir)...) //nolint:gosec // our own verified download.
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launching the MASS installer: %w", err)
	}
	return cmd.Process.Release()
}

// --- The fleet pre-flight gate ---

// UpdateFleetGate is what the candidate-aware compat check found: how many
// connected workers the registry index says the candidate MASS build would
// reject at Register, and a sample of their names.
type UpdateFleetGate struct {
	Incompatible int      `json:"incompatible"`
	Names        []string `json:"names,omitempty"`
}

// fleetGateNameCap bounds the names carried in the response and the message —
// enough to recognise the fleet, not a full listing.
const fleetGateNameCap = 5

// updateFleetGate counts the connected workers whose registry-index row admits
// this MASS build but not candidate. It is the load-bearing half of the update
// flow: a version-skewed worker is rejected with FAILED_PRECONDITION at
// Register, which the worker treats as fatal — it exits and STAYS DOWN until
// somebody upgrades it (internal/worker/hub.go). So an unchecked MASS upgrade
// can silently empty the fleet.
//
// Cache-only, never network — the same posture as CheckWorkerCompat, whose
// verdict this predicts. Every inconclusive input (no cached index, no row for
// the worker's version, an unparseable range, a dev/pre-release version) counts
// as compatible, because that is exactly what Register would do with it:
// accept-with-warning. The gate must never claim a breakage the hub wouldn't
// enforce.
func (h *Handler) updateFleetGate(candidate string) UpdateFleetGate {
	idx, err := h.cachedIndex()
	if err != nil || idx == nil {
		h.logger.Debug().Err(err).Msg("no cached registry index for the update fleet gate")
		return UpdateFleetGate{}
	}
	return fleetGate(idx, h.fleetPairings(), candidate)
}

// fleetGate is updateFleetGate's verdict over an explicit index and fleet.
func fleetGate(idx *registry.Index, workers []workerPairing, candidate string) UpdateFleetGate {
	gate := UpdateFleetGate{}
	for _, w := range workers {
		if !workerRejectsMass(idx, w, candidate) {
			continue
		}
		gate.Incompatible++
		if len(gate.Names) < fleetGateNameCap {
			gate.Names = append(gate.Names, w.RuntimeName+" "+w.Version)
		}
	}
	return gate
}

// workerRejectsMass reports whether the index says w's version pairs with no
// MASS in candidate's range. Mirrors checkWorkerIndexCompat's row semantics:
// one admitting row is enough, and an inconclusive row admits.
func workerRejectsMass(idx *registry.Index, w workerPairing, candidate string) bool {
	wv, err := semver.NewVersion(w.Version)
	if err != nil {
		return false
	}
	rows := workerVersionRows(idx, w.RuntimeName, wv)
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		if ok, reason := admits(row.Mass, candidate); ok || reason != "" {
			return false
		}
	}
	return true
}

// --- HTTP surface ---

// UpdateCheckResponse is GET /api/update/check's answer: the running
// build, the newer tag the startup check found (empty when there is none), and
// the fleet the candidate would strand.
type UpdateCheckResponse struct {
	Version   string `json:"version"`
	Available string `json:"available"`
	UpdateFleetGate
}

// handleUpdateCheck reports what the startup check found and what applying it
// would cost the fleet. It reads stored state and the on-disk index cache — no
// network, so a dashboard may call it on every load.
func (h *Handler) handleUpdateCheck(w http.ResponseWriter, _ *http.Request) {
	resp := UpdateCheckResponse{Version: h.version, Available: h.update.get()}
	if resp.Available != "" {
		resp.UpdateFleetGate = h.updateFleetGate(resp.Available)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Debug().Err(err).Msg("writing the update check response")
	}
}

// updateApplyRequest is POST /api/update/apply's optional body. force
// overrides the fleet gate — safe by default, but the operator who has read the
// warning can still choose.
type updateApplyRequest struct {
	Force bool `json:"force"`
}

// handleUpdateApply installs the release the check found. It does the parts
// that can fail — the gates, resolving the install, downloading and verifying
// the installer — inside the request, so a caller that gets a 200 knows the
// installer is running. Only then does it retire the daemon, from a goroutine:
// the shutdown drains this very request.
func (h *Handler) handleUpdateApply(w http.ResponseWriter, r *http.Request) {
	tag := h.update.get()
	if tag == "" || h.updater == nil {
		http.Error(w, "no MASS update is available", http.StatusConflict)
		return
	}
	var req updateApplyRequest
	if r.Body != nil {
		// An empty or malformed body is simply "no force" — the flag is the only
		// thing in it, and a caller that sends nothing means the default.
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if !req.Force {
		if gate := h.updateFleetGate(tag); gate.Incompatible > 0 {
			http.Error(w, (&errIncompatibleFleet{
				Count: gate.Incompatible, Names: gate.Names, Tag: tag,
			}).Error(), http.StatusConflict)
			return
		}
	}

	h.logger.Info().Str("tag", tag).Bool("force", req.Force).Msg("applying a MASS update")
	if err := h.applyUpdate(r.Context(), tag); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errNotInstalled) || errors.Is(err, errNeedsElevation) ||
			errors.Is(err, errOperatorManaged) {
			status = http.StatusConflict
		} else {
			h.logger.Warn().Err(err).Msg("applying the MASS update")
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "updating", "version": tag}); err != nil {
		h.logger.Debug().Err(err).Msg("writing the update apply response")
	}

	// The installer is waiting for this process's files to be replaceable, so
	// the daemon has to go.
	h.shutdownMu.Lock()
	fn := h.shutdownFn
	h.shutdownMu.Unlock()
	if fn != nil {
		go fn()
	}
}
