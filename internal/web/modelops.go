package web

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Model-store operations shared by the dashboard's /api/* handlers and the
// public mass.v1.Mass Connect API. Each follows the one rule: the gateway
// DECIDES (plan/read) and MASS EXECUTES the byte movement against its store.
// The methods return sentinel errors so each transport maps them to its own
// status codes without re-deciding.

var (
	// ErrModelOpRuntimeDown is returned when the named runtime isn't running.
	ErrModelOpRuntimeDown = errors.New("runtime not running")
	// ErrModelOpConflict is returned when a destination is already taken or
	// an import for it is already in flight.
	ErrModelOpConflict = errors.New("model already exists or import in progress")
	// ErrModelOpInvalid is returned for bad input (missing fields, unknown id).
	ErrModelOpInvalid = errors.New("invalid model operation request")
	// ErrModelOpUnavailable is returned when a needed subsystem is missing.
	ErrModelOpUnavailable = errors.New("model operation subsystem unavailable")
	// ErrModelOpBusy is returned when a delete is refused because a QUEUED
	// job still references the model's files. Maps to 409 / FailedPrecondition.
	ErrModelOpBusy = errors.New("model is in use by a queued job")
)

// ModelGroupView pairs a runtime name with the gateway's group, matching the
// mass.v1.Mass ListModels shape.
type ModelGroupView struct {
	RuntimeName string
	Group       *gatewaypb.Group
}

// listModels returns every running runtime's grouped catalogue (optionally one
// runtime), each group tagged with the runtime that owns it. Read-only: the
// gateway is the source of truth; MASS just relays. Groups present on multiple
// runtimes (same id) are emitted once per owning runtime.
func (h *Handler) listModels(ctx context.Context, runtimeName string) ([]ModelGroupView, error) {
	if h.runtimes == nil {
		return nil, fmt.Errorf("%w: runtimes manager", ErrModelOpUnavailable)
	}
	var gws []*runtimes.LoadedGateway
	if rt := strings.TrimSpace(runtimeName); rt != "" {
		gw, err := h.runtimes.LoadedGatewayFor(rt)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrModelOpRuntimeDown, rt)
		}
		gws = []*runtimes.LoadedGateway{gw}
	} else {
		gws = h.runtimes.RunningGateways()
	}

	var out []ModelGroupView
	for _, gw := range gws {
		groups, err := gw.ListGroups(ctx)
		if err != nil {
			h.logger.Warn().Err(err).Str("runtime", gw.RuntimeName()).Msg("list models: ListGroups failed")
			continue
		}
		for _, g := range groups {
			out = append(out, ModelGroupView{RuntimeName: gw.RuntimeName(), Group: g})
		}
	}
	return out, nil
}

// importLocalModel installs a model already on the MASS host: the gateway
// plans the file set, MASS copies the bytes. Returns the store-relative
// destinations.
func (h *Handler) importLocalModel(ctx context.Context, runtimeName, path, name, actor string) ([]string, error) {
	files, err := h.planImport(ctx, runtimeName, name, func(gw *runtimes.LoadedGateway) ([]*gatewaypb.DownloadFile, error) {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("%w: path is required", ErrModelOpInvalid)
		}
		return gw.PlanLocalImport(ctx, path, strings.TrimSpace(name))
	})
	if err != nil {
		return nil, err
	}
	rels := make([]string, 0, len(files))
	for _, f := range files {
		// file:// URL → plain source path for the local copy.
		src := strings.TrimPrefix(f.GetUrl(), "file://")
		rel, err := h.downloads.ImportLocal(src, f.GetRelPath(), runtimeName, downloads.LocalImportLabels{
			GroupName: f.GetGroupLabel(),
			Filename:  filepath.Base(f.GetRelPath()),
		})
		if err != nil {
			return nil, mapDownloadErr(err, f.GetRelPath())
		}
		rels = append(rels, rel)
	}
	audit.Log(h.logger, "model.imported", name, audit.OutcomeOK).
		Str("actor", actor).Str("runtime", runtimeName).Str("source", "local").Int("files", len(rels)).Msg("")
	return rels, nil
}

// importRemoteModel installs a model from a remote source (HF repo+filename):
// the gateway plans the URLs, MASS fetches the bytes. Returns the
// store-relative destinations queued.
func (h *Handler) importRemoteModel(ctx context.Context, runtimeName, repoID, filename, name, actor string) ([]string, error) {
	files, err := h.planImport(ctx, runtimeName, name, func(gw *runtimes.LoadedGateway) ([]*gatewaypb.DownloadFile, error) {
		if strings.TrimSpace(repoID) == "" || strings.TrimSpace(filename) == "" {
			return nil, fmt.Errorf("%w: repo_id and filename are required", ErrModelOpInvalid)
		}
		return gw.PlanRemoteImport(ctx, strings.TrimSpace(repoID), strings.TrimSpace(filename), strings.TrimSpace(name))
	})
	if err != nil {
		return nil, err
	}
	groupKey := runtimeName + ":" + strings.TrimSpace(name)
	rels := make([]string, 0, len(files))
	for _, f := range files {
		spec := downloads.Job{
			RelPath:     f.GetRelPath(),
			URL:         f.GetUrl(),
			RuntimeName: runtimeName,
			GroupKey:    groupKey,
			GroupName:   f.GetGroupLabel(),
			Filename:    filepath.Base(f.GetRelPath()),
			Total:       f.GetSizeBytes(),
		}
		if err := h.downloads.Start(spec); err != nil {
			if errors.Is(err, downloads.ErrAlreadyExists) || errors.Is(err, downloads.ErrAlreadyDone) {
				continue
			}
			if errors.Is(err, downloads.ErrInvalidRelPath) {
				return nil, mapDownloadErr(err, f.GetRelPath())
			}
			return nil, fmt.Errorf("queueing download %s: %w", f.GetRelPath(), err)
		}
		rels = append(rels, f.GetRelPath())
	}
	audit.Log(h.logger, "model.imported", name, audit.OutcomeOK).
		Str("actor", actor).Str("runtime", runtimeName).Str("source", "remote").Int("files", len(rels)).Msg("")
	return rels, nil
}

// deleteModel removes a model from the store. The gateway decides which files
// make it up (primary + companions); MASS removes them. Returns the
// store-relative files removed.
func (h *Handler) deleteModel(ctx context.Context, runtimeName, id, actor string) ([]string, error) {
	if h.downloads == nil {
		return nil, fmt.Errorf("%w: downloads manager", ErrModelOpUnavailable)
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("%w: id is required", ErrModelOpInvalid)
	}
	gw, err := h.runtimes.LoadedGatewayFor(runtimeName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrModelOpRuntimeDown, runtimeName)
	}
	relPaths, err := gw.PlanDelete(ctx, strings.TrimSpace(id))
	if err != nil {
		return nil, mapGatewayErr(err)
	}
	// Residency guard: the gateway refuses a delete only for ACTIVE running
	// jobs it can see. MASS also owns durable QUEUED jobs the gateway can't
	// see — refuse if any queued envelope's files intersect the doomed paths.
	// Byte-level path matching (exact or dir-prefix), NOT model knowledge.
	if h.orch != nil {
		queued, err := h.orch.QueuedModelFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("checking queued jobs before delete: %w", err)
		}
		if busy := firstPathOverlap(relPaths, queued); busy != "" {
			return nil, fmt.Errorf("%w: %s", ErrModelOpBusy, busy)
		}
	}
	if err := h.downloads.RemoveLocal(relPaths); err != nil {
		if errors.Is(err, downloads.ErrInvalidRelPath) {
			return nil, fmt.Errorf("%w: %s", ErrModelOpInvalid, err)
		}
		return nil, fmt.Errorf("removing model files: %w", err)
	}
	// Forget the model's measurements and tell every worker mirroring
	// its files to drop them.
	if h.orch != nil {
		h.orch.OnModelRemoved(relPaths)
	}
	h.runtimes.FireStateChange(runtimeName)
	audit.Log(h.logger, "model.deleted", id, audit.OutcomeOK).
		Str("actor", actor).Str("runtime", runtimeName).Int("files", len(relPaths)).Msg("")
	return relPaths, nil
}

// planImport validates the runtime + name and runs the gateway plan closure,
// returning the gateway and planned files. Shared by local + remote import.
func (h *Handler) planImport(_ context.Context, runtimeName, name string, plan func(*runtimes.LoadedGateway) ([]*gatewaypb.DownloadFile, error)) ([]*gatewaypb.DownloadFile, error) {
	if h.downloads == nil {
		return nil, fmt.Errorf("%w: downloads manager", ErrModelOpUnavailable)
	}
	if strings.TrimSpace(runtimeName) == "" {
		return nil, fmt.Errorf("%w: runtime_name is required", ErrModelOpInvalid)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: name is required", ErrModelOpInvalid)
	}
	gw, err := h.runtimes.LoadedGatewayFor(runtimeName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrModelOpRuntimeDown, runtimeName)
	}
	files, err := plan(gw)
	if err != nil {
		return nil, mapGatewayErr(err)
	}
	return files, nil
}

// mapGatewayErr collapses a gateway gRPC status into a model-ops sentinel.
func mapGatewayErr(err error) error {
	if errors.Is(err, ErrModelOpInvalid) {
		return err
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.AlreadyExists:
			return fmt.Errorf("%w: %s", ErrModelOpConflict, st.Message())
		case codes.NotFound, codes.InvalidArgument:
			return fmt.Errorf("%w: %s", ErrModelOpInvalid, st.Message())
		}
	}
	return err
}

// firstPathOverlap returns the first doomed path that overlaps a key in the
// queued set, or "" if none. Both sides are OPAQUE store-relative keys; an
// entry may name a file or a directory subtree, so overlap is exact equality
// or a directory relationship in either direction. Never parsed for meaning.
func firstPathOverlap(doomed []string, queued map[string]struct{}) string {
	for _, d := range doomed {
		if _, ok := queued[d]; ok {
			return d
		}
		for q := range queued {
			if pathCovers(d, q) || pathCovers(q, d) {
				return d
			}
		}
	}
	return ""
}

// pathCovers reports whether dir is a directory prefix of path (path strictly
// under dir), matching on the "/" boundary so "a/b" covers "a/b/c" but not
// "a/bc".
func pathCovers(dir, path string) bool {
	return len(path) > len(dir) && path[len(dir)] == '/' && path[:len(dir)] == dir
}

// mapDownloadErr collapses a downloads-manager error into a sentinel.
func mapDownloadErr(err error, relPath string) error {
	switch {
	case errors.Is(err, downloads.ErrAlreadyExists):
		return fmt.Errorf("%w: import already in progress: %s", ErrModelOpConflict, filepath.Base(relPath))
	case errors.Is(err, downloads.ErrAlreadyDone):
		return fmt.Errorf("%w: model already exists: %s", ErrModelOpConflict, relPath)
	case errors.Is(err, downloads.ErrInvalidRelPath):
		return fmt.Errorf("%w: %s", ErrModelOpInvalid, err)
	default:
		return err
	}
}
