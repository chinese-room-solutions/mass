package web

import (
	"context"
	"errors"
	"fmt"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
)

// Runtime-manager operations shared by the dashboard's /api/runtimes handlers
// and the public mass.v1.Mass Connect API. Each method owns the audit-log
// call for its action and returns sentinel errors (see ops.go) so every
// transport maps them to its own status codes without re-deciding.

// RuntimeInfo is the transport-neutral view of one installed runtime gateway.
// Both the HTMX runtime views and the Connect Runtime message map from it.
type RuntimeInfo struct {
	RuntimeName string
	Version     string
	DisplayName string
	Description string
	AutoStart   bool
	Running     bool
}

// listRuntimeInfos returns every installed runtime as a neutral view, or nil
// when the manager isn't wired.
func (h *Handler) listRuntimeInfos() []RuntimeInfo {
	if h.runtimes == nil {
		return nil
	}
	mfs := h.runtimes.List()
	out := make([]RuntimeInfo, len(mfs))
	for i, mf := range mfs {
		out[i] = RuntimeInfo{
			RuntimeName: mf.RuntimeName,
			Version:     mf.Version,
			DisplayName: mf.DisplayName,
			Description: mf.Description,
			AutoStart:   mf.AutoStart,
			Running:     h.runtimes.IsRunning(mf.RuntimeName),
		}
	}
	return out
}

// installRuntime installs the .mass package at path and returns the newly
// installed runtime's neutral view. Maps the manager's install sentinels onto
// the ops sentinels so transports pick their own codes.
func (h *Handler) installRuntime(path, actor string) (RuntimeInfo, error) {
	if h.runtimes == nil {
		return RuntimeInfo{}, fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if path == "" {
		return RuntimeInfo{}, fmt.Errorf("%w: path is required", ErrOpInvalid)
	}
	mf, err := h.runtimes.InstallFromPath(path)
	if err != nil {
		switch {
		case errors.Is(err, runtimes.ErrRuntimeAlreadyInstalled),
			errors.Is(err, runtimes.ErrManifestMissing),
			errors.Is(err, runtimes.ErrBinaryMissing):
			// Pass through unwrapped so callers keep their errors.Is switches.
		default:
			h.logger.Warn().Err(err).Msg("installing runtime")
		}
		audit.Log(h.logger, "runtime.installed", path, audit.OutcomeError).
			Str("actor", actor).Str("error", err.Error()).Msg("")
		return RuntimeInfo{}, err
	}
	audit.Log(h.logger, "runtime.installed", path, audit.OutcomeOK).
		Str("actor", actor).Msg("")
	return RuntimeInfo{
		RuntimeName: mf.RuntimeName,
		Version:     mf.Version,
		DisplayName: mf.DisplayName,
		Description: mf.Description,
		AutoStart:   mf.AutoStart,
		Running:     h.runtimes.IsRunning(mf.RuntimeName),
	}, nil
}

// uninstallRuntime removes an installed runtime.
func (h *Handler) uninstallRuntime(name, actor string) error {
	if h.runtimes == nil {
		return fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if err := h.runtimes.Uninstall(name); err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", name).Msg("uninstalling runtime")
		audit.Log(h.logger, "runtime.uninstalled", name, audit.OutcomeError).
			Str("actor", actor).Str("error", err.Error()).Msg("")
		return err
	}
	audit.Log(h.logger, "runtime.uninstalled", name, audit.OutcomeOK).
		Str("actor", actor).Msg("")
	return nil
}

// startRuntime starts a runtime gateway.
func (h *Handler) startRuntime(ctx context.Context, name, actor string) error {
	if h.runtimes == nil {
		return fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if _, err := h.runtimes.Start(ctx, name); err != nil {
		if !errors.Is(err, runtimes.ErrRuntimeNotFound) {
			h.logger.Warn().Err(err).Str("runtime_name", name).Msg("starting runtime")
			audit.Log(h.logger, "runtime.started", name, audit.OutcomeError).
				Str("actor", actor).Str("error", err.Error()).Msg("")
		}
		return err
	}
	audit.Log(h.logger, "runtime.started", name, audit.OutcomeOK).
		Str("actor", actor).Msg("")
	return nil
}

// stopRuntime stops a runtime gateway. Swallows ErrRuntimeNotRunning — a stop
// on an already-stopped runtime is a no-op success (matches the dashboard).
func (h *Handler) stopRuntime(name, actor string) error {
	if h.runtimes == nil {
		return fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if err := h.runtimes.Stop(name); err != nil && !errors.Is(err, runtimes.ErrRuntimeNotRunning) {
		h.logger.Warn().Err(err).Str("runtime_name", name).Msg("stopping runtime")
		audit.Log(h.logger, "runtime.stopped", name, audit.OutcomeError).
			Str("actor", actor).Str("error", err.Error()).Msg("")
		return err
	}
	audit.Log(h.logger, "runtime.stopped", name, audit.OutcomeOK).
		Str("actor", actor).Msg("")
	return nil
}

// setRuntimeAutoStart sets a runtime's auto-start flag to an explicit value.
// The HTMX toggle handler reads the current value and passes its inverse.
func (h *Handler) setRuntimeAutoStart(name string, enabled bool, actor string) error {
	if h.runtimes == nil {
		return fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if err := h.runtimes.SetAutoStart(name, enabled); err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", name).Msg("toggling auto-start")
		return err
	}
	audit.Log(h.logger, "runtime.autostart_set", name, audit.OutcomeOK).
		Str("actor", actor).Bool("enabled", enabled).Msg("")
	return nil
}
