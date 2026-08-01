package web

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

// massService implements the public mass.v1.Mass Connect API. Every method is
// a thin adapter over the shared model-ops core (modelops.go) — the same core
// the dashboard's /api/* handlers call. Connect gives gRPC + JSON-over-HTTP
// from one definition; this is the entry point apps and AI agents use to
// manage MASS's model store.
type massService struct {
	rpcconnect.UnimplementedMassHandler
	h *Handler
}

var _ rpcconnect.MassHandler = (*massService)(nil)

func (s *massService) ListModels(ctx context.Context, req *connect.Request[rpc.ListModelsRequest]) (*connect.Response[rpc.ListModelsResponse], error) {
	views, err := s.h.listModels(ctx, req.Msg.GetRuntimeName())
	if err != nil {
		return nil, connectErr(err)
	}
	groups := make([]*rpc.ModelGroup, 0, len(views))
	for _, v := range views {
		groups = append(groups, &rpc.ModelGroup{RuntimeName: v.RuntimeName, Group: v.Group})
	}
	return connect.NewResponse(&rpc.ListModelsResponse{Groups: groups}), nil
}

func (s *massService) ImportLocalModel(ctx context.Context, req *connect.Request[rpc.ImportLocalModelRequest]) (*connect.Response[rpc.ImportModelResponse], error) {
	rels, err := s.h.importLocalModel(ctx, req.Msg.GetRuntimeName(), req.Msg.GetPath(), req.Msg.GetOperatorName(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.ImportModelResponse{RelPaths: rels}), nil
}

func (s *massService) ImportRemoteModel(ctx context.Context, req *connect.Request[rpc.ImportRemoteModelRequest]) (*connect.Response[rpc.ImportModelResponse], error) {
	rels, err := s.h.importRemoteModel(ctx, req.Msg.GetRuntimeName(), req.Msg.GetRepoId(), req.Msg.GetFilename(), req.Msg.GetOperatorName(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.ImportModelResponse{RelPaths: rels}), nil
}

func (s *massService) DeleteModel(ctx context.Context, req *connect.Request[rpc.DeleteModelRequest]) (*connect.Response[rpc.DeleteModelResponse], error) {
	rels, err := s.h.deleteModel(ctx, req.Msg.GetRuntimeName(), req.Msg.GetId(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.DeleteModelResponse{RelPaths: rels}), nil
}

// connectErr maps a shared ops sentinel (modelops.go + ops.go) or a
// pass-through manager sentinel to a Connect status code.
func connectErr(err error) error {
	switch {
	// Model ops.
	case errors.Is(err, ErrModelOpConflict), errors.Is(err, runtimes.ErrRuntimeAlreadyInstalled):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, ErrModelOpInvalid), errors.Is(err, ErrOpInvalid),
		errors.Is(err, scheduler.ErrEvictGlobalRow):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrModelOpRuntimeDown), errors.Is(err, ErrModelOpBusy),
		errors.Is(err, ErrOpBusy), errors.Is(err, scheduler.ErrRowInFlight):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrModelOpUnavailable), errors.Is(err, ErrOpUnavailable),
		errors.Is(err, ErrOpRegistry), errors.Is(err, scheduler.ErrWorkerGone):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, ErrOpNotFound), errors.Is(err, runtimes.ErrRuntimeNotFound),
		errors.Is(err, scheduler.ErrUnknownQueue), errors.Is(err, scheduler.ErrNotInflight):
		return connect.NewError(connect.CodeNotFound, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// connectActor derives an audit actor string from a Connect request. Mirrors
// actorFromRequest for the HTTP side; falls back to "api" when no auth header.
func connectActor[T any](req *connect.Request[T]) string {
	if u := req.Header().Get("X-Mass-Actor"); u != "" {
		return u
	}
	return "api"
}
