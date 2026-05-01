package runtimes

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/KernelPryanic/ctxerr"
	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// schedulerServiceServer implements [gatewaypb.MassSchedulerServer]. It is
// the callback MASS exposes to a gateway subprocess so the gateway can
// schedule jobs, request loads/evictions, and inspect worker state without
// holding any direct reference to the scheduler.
//
// One server is constructed per launched gateway and pinned to that
// gateway's runtime kind. Schedule + EnsureModel calls are constrained to
// workers of that kind — gateways cannot accidentally talk to a different
// runtime's fleet.
type schedulerServiceServer struct {
	gatewaypb.UnimplementedMassSchedulerServer

	runtimeName string
	sched       *scheduler.Scheduler
	logger      zerolog.Logger
}

func newSchedulerServiceServer(runtimeName string, sched *scheduler.Scheduler, logger zerolog.Logger) *schedulerServiceServer {
	return &schedulerServiceServer{
		runtimeName: runtimeName,
		sched:       sched,
		logger:      logger.With().Str("runtime_name", runtimeName).Str("component", "scheduler_callback").Logger(),
	}
}

// Schedule submits a job and streams worker chunks back as ScheduleChunk
// frames until either Completed or Error is observed. The job_id is the one
// MASS assigns when it sends HubAssignJob to the worker; gateways receive
// it on the first frame.
func (s *schedulerServiceServer) Schedule(req *gatewaypb.ScheduleRequest, stream gatewaypb.MassScheduler_ScheduleServer) error {
	if req.GetModelId() == "" {
		return status.Error(codes.InvalidArgument, "schedule: model_id required")
	}

	ch, err := s.sched.Schedule(stream.Context(), scheduler.ScheduleRequest{
		RuntimeName:     s.runtimeName,
		ModelID:         req.ModelId,
		Payload:         req.Payload,
		Weight:          req.Weight,
		AffinityWorkers: req.AffinityWorkers,
	})
	if err != nil {
		return mapErrToGRPC(err)
	}

	// Stream chunks back to the gateway. The MASS-side scheduler hands us
	// channel-of-JobChunk frames whose semantics map 1:1 to ScheduleChunk.
	//
	// Context cancellation (gateway dropped, deadline expired) returns
	// immediately. The worker keeps grinding until it finishes — abandoning
	// in-flight jobs is CancelJob's job, not ours.
	ctx := stream.Context()
	for {
		var chunk *worker.JobChunk
		select {
		case <-ctx.Done():
			return nil
		case c, ok := <-ch:
			if !ok {
				return nil
			}
			chunk = c
		}
		out := &gatewaypb.ScheduleChunk{}
		switch chunk.Type {
		case worker.JobChunkTypeChunk:
			out.Frame = &gatewaypb.ScheduleChunk_Chunk{Chunk: chunk.Chunk}
		case worker.JobChunkTypeProgress:
			out.Frame = &gatewaypb.ScheduleChunk_Progress{Progress: &gatewaypb.ScheduleProgress{
				Pct:  chunk.Pct,
				Note: chunk.Note,
			}}
		case worker.JobChunkTypeCompleted:
			out.Frame = &gatewaypb.ScheduleChunk_Completed{Completed: &gatewaypb.ScheduleCompleted{
				FinalResponse: chunk.Final,
			}}
		case worker.JobChunkTypeError:
			out.Frame = &gatewaypb.ScheduleChunk_Error{Error: &gatewaypb.ScheduleError{
				Message: chunk.ErrText,
			}}
		default:
			continue
		}
		if err := stream.Send(out); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctxerr.With(fmt.Errorf("streaming schedule chunk: %w", err), map[string]any{"runtime_name": s.runtimeName, "model_id": req.ModelId})
		}
	}
}

// EnsureModelLoaded asks the scheduler to make sure the model is loaded on
// at least one worker of the gateway's kind. Idempotent.
func (s *schedulerServiceServer) EnsureModelLoaded(ctx context.Context, req *gatewaypb.EnsureModelLoadedRequest) (*gatewaypb.EnsureModelLoadedResponse, error) {
	_ = ctx
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "ensure_model_loaded: model_id required")
	}
	files := convertModelFiles(req.Files)
	instances, err := s.sched.EnsureModelLoaded(scheduler.EnsureModelLoadedRequest{
		RuntimeName: s.runtimeName,
		ModelID:     req.GetModelId(),
		Files:       files,
		LoadHints:   req.GetLoadHints(),
		Preferred:   req.GetPreferredWorkers(),
		Source:      req.GetSource(),
	})
	if err != nil {
		return nil, mapErrToGRPC(err)
	}
	out := &gatewaypb.EnsureModelLoadedResponse{
		Instances: make([]*gatewaypb.LoadedInstance, len(instances)),
	}
	for i, inst := range instances {
		out.Instances[i] = &gatewaypb.LoadedInstance{
			WorkerId: inst.WorkerID,
			PoolSize: inst.PoolSize,
		}
	}
	return out, nil
}

// EvictModel asks the scheduler to unload modelID from one (or all) workers
// of the gateway's kind.
func (s *schedulerServiceServer) EvictModel(ctx context.Context, req *gatewaypb.EvictModelRequest) (*gatewaypb.EvictModelResponse, error) {
	_ = ctx
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "evict_model: model_id required")
	}
	n, err := s.sched.EvictModel(s.runtimeName, req.ModelId, req.WorkerId)
	if err != nil {
		return nil, mapErrToGRPC(err)
	}
	return &gatewaypb.EvictModelResponse{EvictedCount: int32(n)}, nil
}

// ListWorkers returns the gateway's view of online workers of its runtime
// kind, with current capacity + loaded models.
func (s *schedulerServiceServer) ListWorkers(ctx context.Context, _ *gatewaypb.ListWorkersRequest) (*gatewaypb.ListWorkersResponse, error) {
	_ = ctx
	workers := s.sched.WorkersForRuntime(s.runtimeName)
	out := &gatewaypb.ListWorkersResponse{Workers: make([]*gatewaypb.WorkerSummary, 0, len(workers))}
	for _, w := range workers {
		devices := w.Devices()
		pbDevices := make([]*workerpb.WorkerDevice, len(devices))
		for i, d := range devices {
			pbDevices[i] = &workerpb.WorkerDevice{
				Id:            d.ID,
				Name:          d.Name,
				Type:          deviceTypeToProto(d.Type),
				TotalMemoryMb: int32(d.TotalMemoryMB),
			}
		}
		loaded := w.LoadedModels()
		pbLoaded := make([]*workerpb.LoadedModelStatus, len(loaded))
		for i, lm := range loaded {
			pbLoaded[i] = &workerpb.LoadedModelStatus{
				ModelId:  lm.ModelID,
				PoolSize: int32(lm.PoolSize),
				Active:   int32(lm.Active),
			}
		}
		out.Workers = append(out.Workers, &gatewaypb.WorkerSummary{
			Id:                w.ID(),
			Name:              w.Name(),
			Devices:           pbDevices,
			ActiveJobs:        int32(w.ActiveJobs()),
			AvailableCapacity: int32(w.AvailableCapacity()),
			LoadedModels:      pbLoaded,
		})
	}
	return out, nil
}

func convertModelFiles(in []*workerpb.ModelFile) []*workerpb.ModelFile {
	if len(in) == 0 {
		return nil
	}
	// Shallow copy is fine — proto messages are pointers and the scheduler
	// does not mutate them.
	out := make([]*workerpb.ModelFile, len(in))
	copy(out, in)
	return out
}

func deviceTypeToProto(t stats.DeviceType) workerpb.WorkerDeviceType {
	switch t {
	case stats.DeviceTypeCPU:
		return workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_CPU
	case stats.DeviceTypeGPU:
		return workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_GPU
	default:
		return workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_UNSPECIFIED
	}
}

// mapErrToGRPC translates scheduler errors into gRPC status codes a gateway
// can act on. Anything unrecognized becomes Internal.
func mapErrToGRPC(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, scheduler.ErrNoWorker):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, scheduler.ErrModelNotLoaded):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
