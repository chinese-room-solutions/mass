package runtimes

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The two callbacks the scheduler's bench orchestrator needs from a
// running gateway: what models exist, and what request to measure each
// one with. Both live here rather than in the scheduler so the scheduler
// stays runtime-agnostic — it never talks to a gateway directly.

// BenchModels lists the models of runtimeName worth benching, pairing the
// gateway's own model id (what AuthorBenchPayload takes) with the
// store-relative key of its file (what benchmark rows and load artifacts
// are keyed by).
//
// Companion artifacts — a vision projector, say — are skipped: the
// gateway reports no model type for them because they don't run on their
// own, and asking it to author a bench payload for one is an error.
//
// modelsDir is MASS's models root, used to turn the gateway's absolute
// paths back into store-relative keys.
func (m *Manager) BenchModels(modelsDir string) scheduler.BenchModelsFn {
	return func(ctx context.Context, runtimeName string) ([]scheduler.BenchModel, error) {
		gw, err := m.LoadedGatewayFor(runtimeName)
		if err != nil {
			return nil, err
		}
		groups, err := gw.ListGroups(ctx)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("listing models to bench: %w", err), map[string]any{"runtime_name": runtimeName})
		}
		var out []scheduler.BenchModel
		for _, g := range groups {
			for _, mdl := range g.GetModels() {
				if mdl.GetModelType().GetKind() == 0 {
					continue // companion artifact; nothing to run on its own
				}
				key := storeKey(modelsDir, mdl.GetPath())
				if key == "" {
					continue
				}
				out = append(out, scheduler.BenchModel{ID: mdl.GetId(), Key: key})
			}
		}
		return out, nil
	}
}

// AuthorBenchPayload asks runtimeName's gateway for the request MASS
// benchmarks modelID with, translating the gateway's "no such model"
// answer into [scheduler.ErrBenchModelGone] — retryable and free,
// because it usually means a download is still landing.
func (m *Manager) AuthorBenchPayload() scheduler.BenchPayloadAuthorFn {
	return func(ctx context.Context, runtimeName, modelID string) (scheduler.BenchPayload, error) {
		gw, err := m.LoadedGatewayFor(runtimeName)
		if err != nil {
			return scheduler.BenchPayload{}, errors.Join(scheduler.ErrBenchModelGone, err)
		}
		resp, err := gw.AuthorBenchPayload(ctx, modelID)
		if err != nil {
			if st, ok := status.FromError(err); ok && (st.Code() == codes.NotFound || st.Code() == codes.FailedPrecondition) {
				return scheduler.BenchPayload{}, errors.Join(scheduler.ErrBenchModelGone, err)
			}
			return scheduler.BenchPayload{}, ctxerr.With(fmt.Errorf("authoring bench payload: %w", err),
				map[string]any{"runtime_name": runtimeName, "model_id": modelID})
		}
		return scheduler.BenchPayload{
			Payload:   resp.GetPayload(),
			LoadHints: resp.GetLoadHints(),
			Cost:      resp.GetCost(),
			Files:     resp.GetFiles(),
		}, nil
	}
}

// storeKey turns an absolute model path into the forward-slash key
// relative to the models root — the same namespace ModelFile.filename,
// DownloadFile.rel_path and the worker's cache_files all use. Empty when
// the path lies outside the root (nothing MASS owns).
func storeKey(modelsDir, absPath string) string {
	if modelsDir == "" || absPath == "" {
		return ""
	}
	rel, err := filepath.Rel(modelsDir, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(rel)
}
