package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/config"
)

// Single-box ("install-local") worker install. MASS resolves the worker
// installer for its own platform, fetches it through the shared artifact cache,
// and runs it locally with --non-interactive pointed at MASS's own listen
// address. The OS service manager supervises the worker thereafter; MASS does
// not. This mirrors what the operator does on a remote box (download the
// installer from /setup/worker-bin and run it), but for the MASS host itself
// and non-interactively.

// InstallLocalWorkerResult is the transport-neutral outcome of a single-box
// install: the resolved worker package/version plus the installer's trimmed
// combined output.
type InstallLocalWorkerResult struct {
	WorkerPackage string
	WorkerVersion string
	Output        string
}

// installerOutputTailBytes caps how much installer output is retained/surfaced
// (both in the success payload and in an error), keeping large build logs out
// of the response and audit line.
const installerOutputTailBytes = 4096

// localInstallJoinTokenTTL bounds the join token minted for a single-box
// install. The install runs synchronously in this handler, so a few minutes
// covers a slow download without leaving a long-lived token behind.
const localInstallJoinTokenTTL = 5 * time.Minute

// installLocalWorker installs a worker on the MASS host itself. runtimeName is
// inferred when empty and exactly one runtime is installed (mirroring
// join-command); scope defaults to "user". The installer joins with a
// short-TTL join token minted server-side (not the caller's own credential): the
// worker enrolls with it and receives its own per-worker secret. When auth is
// disabled no join token is minted and the installer connects with none.
// Audit-logs the install.
func (h *Handler) installLocalWorker(ctx context.Context, runtimeName, scope, name, actor string) (InstallLocalWorkerResult, error) {
	if h.runtimes == nil {
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if h.cfg == nil {
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: config", ErrOpUnavailable)
	}

	switch scope {
	case "", "user":
		scope = "user"
	case "system":
	default:
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: scope must be \"user\" or \"system\", got %q", ErrOpInvalid, scope)
	}

	runtimeName, err := h.resolveLocalRuntimeName(runtimeName)
	if err != nil {
		return InstallLocalWorkerResult{}, err
	}

	// Resolve the worker installer for this host's platform, reusing the same
	// package- and backend-inference rules as the /setup/worker-bin path (single
	// package/backend ⇒ used; multiple ⇒ errAmbiguousWorker/errAmbiguousBackend;
	// none ⇒ ErrNotResolved). Single-box install can't prompt, so ambiguity is a
	// hard error telling the operator the index must pin one.
	resolved, candidates, err := h.resolveWorkerArtifact(ctx, runtimeName, runtime.GOOS, runtime.GOARCH, "", "")
	if err != nil {
		switch {
		case errors.Is(err, errRuntimeNotInstalled):
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: %w", ErrOpNotFound, err)
		case errors.Is(err, errAmbiguousWorker):
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: multiple worker packages available for runtime %s (%s); pick one",
				ErrOpInvalid, runtimeName, strings.Join(candidates, ", "))
		case errors.Is(err, errAmbiguousBackend):
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: multiple worker backends available for %s/%s (%s); index must pin one",
				ErrOpInvalid, runtime.GOOS, runtime.GOARCH, strings.Join(candidates, ", "))
		case errors.Is(err, registry.ErrNotResolved):
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: %w", ErrOpNotFound, err)
		default:
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: %w", ErrOpRegistry, err)
		}
	}

	installerPath, err := h.artifactCache.ensure(ctx, resolved.Artifact)
	if err != nil {
		if errors.Is(err, ctx.Err()) {
			return InstallLocalWorkerResult{}, ctx.Err()
		}
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: fetching worker installer: %w", ErrOpRegistry, err)
	}

	// The cache is content-addressed by digest, so the on-disk name isn't
	// executable-shaped. Materialize a runnable copy (with the platform's exe
	// suffix) the OS will exec, then chmod +x on non-Windows.
	runPath, cleanup, err := h.stageInstaller(installerPath)
	if err != nil {
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: staging installer: %v", ErrOpRegistry, err)
	}
	defer cleanup()

	target := resolved.Package.Name + "@" + resolved.Version.Version

	// Mint a short-TTL join token for the installer to enroll with, unless auth
	// is disabled (then the worker enrolls with no token, mirroring a remote box
	// on a loopback-only MASS). The install runs synchronously and the token is
	// single-purpose, so a few minutes is ample.
	args := []string{"--non-interactive", "--mass-url", h.localWorkerURL(), "--scope", scope}
	if h.enroller != nil && !h.AuthDisabled() {
		token, _, err := h.enroller.MintJoinToken(localInstallJoinTokenTTL)
		if err != nil {
			return InstallLocalWorkerResult{}, fmt.Errorf("%w: minting join token: %v", ErrOpUnavailable, err)
		}
		args = append(args, "--token", token)
	}
	if name != "" {
		args = append(args, "--name", name)
	}

	cmd := exec.CommandContext(ctx, runPath, args...)
	out, runErr := cmd.CombinedOutput()
	output := tailString(string(out), installerOutputTailBytes)

	if runErr != nil {
		audit.Log(h.logger, "worker.install_local", target, audit.OutcomeError).
			Str("actor", actor).Str("scope", scope).Str("error", runErr.Error()).Msg("")
		return InstallLocalWorkerResult{}, fmt.Errorf("%w: worker installer failed: %v\n%s",
			ErrOpRegistry, runErr, output)
	}

	audit.Log(h.logger, "worker.install_local", target, audit.OutcomeOK).
		Str("actor", actor).Str("scope", scope).Msg("")
	return InstallLocalWorkerResult{
		WorkerPackage: resolved.Package.Name,
		WorkerVersion: resolved.Version.Version,
		Output:        output,
	}, nil
}

// resolveLocalRuntimeName returns runtimeName when set, else infers it: exactly
// one installed runtime ⇒ that one; none ⇒ ErrOpInvalid; several ⇒ ErrOpInvalid
// listing them. Mirrors the join-command inference, done server-side.
func (h *Handler) resolveLocalRuntimeName(runtimeName string) (string, error) {
	if runtimeName != "" {
		return runtimeName, nil
	}
	installed := h.runtimes.List()
	switch len(installed) {
	case 0:
		return "", fmt.Errorf("%w: no runtimes installed; install one first", ErrOpInvalid)
	case 1:
		return installed[0].RuntimeName, nil
	default:
		names := make([]string, 0, len(installed))
		for _, m := range installed {
			names = append(names, m.RuntimeName)
		}
		return "", fmt.Errorf("%w: multiple runtimes installed; specify one of: %s",
			ErrOpInvalid, strings.Join(names, ", "))
	}
}

// localWorkerURL is the URL the local worker dials MASS at: the configured
// listen address normalized to a loopback-reachable URL, matching what the
// dashboard's run command renders for a remote box. Scheme follows the TLS
// config.
func (h *Handler) localWorkerURL() string {
	scheme := "http"
	if h.cfg.TLS.Enabled {
		scheme = "https"
	}
	return config.LocalURL(scheme, h.cfg.EffectiveListenAddr())
}

// stageInstaller copies the content-addressed cache file to a runnable path in
// the registry-cache tmp dir (with the OS exe suffix) and, on non-Windows,
// makes it executable. Returns the path and a cleanup that removes it.
func (h *Handler) stageInstaller(cachedPath string) (string, func(), error) {
	tmpDir := filepath.Join(h.registryCacheDir(), "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", func() {}, fmt.Errorf("creating tmp dir: %w", err)
	}
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	runPath := filepath.Join(tmpDir, "mass-worker-setup"+suffix)

	src, err := os.ReadFile(cachedPath)
	if err != nil {
		return "", func() {}, fmt.Errorf("reading cached installer: %w", err)
	}
	if err := os.WriteFile(runPath, src, 0o755); err != nil {
		return "", func() {}, fmt.Errorf("writing runnable installer: %w", err)
	}
	return runPath, func() { _ = os.Remove(runPath) }, nil
}

// tailString returns the last n bytes of s (trimmed of surrounding space),
// prefixed with an ellipsis marker when truncated. Keeps large installer logs
// bounded in responses and audit lines.
func tailString(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "...\n" + s[len(s)-n:]
}
