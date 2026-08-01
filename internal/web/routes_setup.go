package web

import (
	"errors"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass-sdk/registry"
)

// The /setup endpoints proxy the worker installer artifact. They are exempt
// from bearer auth (see AuthMiddleware): the artifact is a public installer
// binary, and the join token (when any) rides only in the operator-pasted
// command line the dashboard hands out.

// handleSetupWorkerBin resolves the newest compatible worker installer for the
// installed runtime + requested platform and serves it through the artifact
// cache. Path:
// /setup/worker-bin/{runtime_name}?os=..&arch=..[&worker=..][&backend=..].
//
// The os/arch query values are normalized from uname-style names so the
// dashboard can embed raw `$(uname -s)`/`$(uname -m)` output: os accepts
// Linux/Darwin/Windows, arch accepts x86_64/AMD64/aarch64/ARM64 (all
// case-insensitive); already-canonical Go values pass through.
//
// Package selection: when no worker param is given and exactly one worker
// package resolves for the os/arch, it is used; when several do, this returns
// 409 listing them (retry with ?worker=<name>).
//
// Backend selection: when no backend param is given and exactly one backend has
// an artifact for the os/arch, it is used; when multiple do, this returns 409
// listing them (retry with ?backend=<name>). Automatic backend selection from
// probed hardware is a later phase (bootstrap hardware probing); until then the
// operator or a single-backend index disambiguates.
func (h *Handler) handleSetupWorkerBin(w http.ResponseWriter, r *http.Request) {
	runtimeName := r.PathValue("runtime_name")
	rawOS := r.URL.Query().Get("os")
	rawArch := r.URL.Query().Get("arch")
	backend := r.URL.Query().Get("backend")
	workerPkg := r.URL.Query().Get("worker")

	if runtimeName == "" || rawOS == "" || rawArch == "" {
		http.Error(w, "runtime_name (path) and os, arch (query) are required", http.StatusBadRequest)
		return
	}
	goos := normalizeGOOS(rawOS)
	goarch := normalizeGOARCH(rawArch)

	resolved, candidates, err := h.resolveWorkerArtifact(r.Context(), runtimeName, goos, goarch, backend, workerPkg)
	if err != nil {
		switch {
		case errors.Is(err, errRuntimeNotInstalled):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, errAmbiguousWorker):
			// 409: the caller must retry with ?worker=<one of these>.
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("multiple worker packages available for runtime " + runtimeName +
				"; retry with ?worker=<one of>:\n"))
			for _, c := range candidates {
				_, _ = w.Write([]byte("  " + c + "\n"))
			}
		case errors.Is(err, errAmbiguousBackend):
			// 409: the caller must retry with ?backend=<one of these>.
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("multiple backends have an artifact for " + goos + "/" + goarch +
				"; retry with ?backend=<one of>:\n"))
			for _, b := range candidates {
				_, _ = w.Write([]byte("  " + b + "\n"))
			}
		case errors.Is(err, registry.ErrNotResolved):
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			h.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("resolving worker artifact")
			http.Error(w, "resolving worker artifact: "+err.Error(), http.StatusBadGateway)
		}
		return
	}

	path, err := h.artifactCache.ensure(r.Context(), resolved.Artifact)
	if err != nil {
		if errors.Is(err, r.Context().Err()) {
			return // client went away
		}
		h.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("caching worker artifact")
		http.Error(w, "fetching worker artifact: "+err.Error(), http.StatusBadGateway)
		return
	}

	// ServeFile honors Range requests and sets Content-Length; download tools
	// resume/stream for free.
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeFile(w, r, path)
}

// normalizeGOOS maps a uname-style OS name to its Go GOOS value
// (case-insensitive). Already-canonical or unknown values pass through
// lowercased so resolution reports the mismatch normally.
func normalizeGOOS(os string) string {
	switch strings.ToLower(os) {
	case "linux":
		return "linux"
	case "darwin", "macos":
		return "darwin"
	case "windows":
		return "windows"
	default:
		return strings.ToLower(os)
	}
}

// normalizeGOARCH maps a uname-style machine name to its Go GOARCH value
// (case-insensitive). Already-canonical or unknown values pass through
// lowercased so resolution reports the mismatch normally.
func normalizeGOARCH(arch string) string {
	switch strings.ToLower(arch) {
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	default:
		return strings.ToLower(arch)
	}
}
