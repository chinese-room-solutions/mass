package runtimes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
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
	downloads   *downloads.Manager
	logger      zerolog.Logger
}

func newSchedulerServiceServer(runtimeName string, sched *scheduler.Scheduler, dl *downloads.Manager, logger zerolog.Logger) *schedulerServiceServer {
	return &schedulerServiceServer{
		runtimeName: runtimeName,
		sched:       sched,
		downloads:   dl,
		logger:      logger.With().Str("runtime_name", runtimeName).Str("component", "scheduler_callback").Logger(),
	}
}

// Submit enqueues a job on the scheduler and returns the assigned
// job_id. The chunk stream is fetched separately via [StreamChunks] so a
// gateway that dies and reconnects can resume without losing the result.
func (s *schedulerServiceServer) Submit(ctx context.Context, req *gatewaypb.SubmitRequest) (*gatewaypb.SubmitResponse, error) {
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "submit: model_id required")
	}
	jobID, err := s.sched.Submit(ctx, scheduler.SubmitRequest{
		RuntimeName: s.runtimeName,
		ModelID:     req.ModelId,
		Payload:     req.Payload,
		Cost:        req.Cost,
		Files:       req.Files,
		LoadHints:   req.LoadHints,
		HeadroomPct: req.HeadroomPct,
		Source:      req.Source,
		Priority:    priorityFromProto(req.GetPriority()),
	})
	if err != nil {
		return nil, mapErrToGRPC(err)
	}
	return &gatewaypb.SubmitResponse{JobId: jobID}, nil
}

// StreamChunks streams worker chunks for an already-submitted job. MASS
// replays buffered chunks with seq >= resume_seq, then pumps live ones
// until the terminal frame is observed or the gateway disconnects. A
// gateway reconnecting after a transient drop passes the highest seq it
// already observed plus one as resume_seq to pick up where it left off.
func (s *schedulerServiceServer) StreamChunks(req *gatewaypb.StreamChunksRequest, stream gatewaypb.MassScheduler_StreamChunksServer) error {
	if req.GetJobId() == "" {
		return status.Error(codes.InvalidArgument, "stream_chunks: job_id required")
	}
	ctx := stream.Context()
	ch, err := s.sched.StreamChunks(ctx, req.GetJobId(), req.GetResumeSeq())
	if err != nil {
		return mapErrToGRPC(err)
	}
	for {
		var sc scheduler.SequencedChunk
		select {
		case <-ctx.Done():
			return nil
		case c, ok := <-ch:
			if !ok {
				return nil
			}
			sc = c
		}
		out := &gatewaypb.ScheduleChunk{JobId: req.GetJobId(), Seq: sc.Seq}
		switch sc.Chunk.Type {
		case worker.JobChunkTypeChunk:
			out.Frame = &gatewaypb.ScheduleChunk_Chunk{Chunk: sc.Chunk.Chunk}
		case worker.JobChunkTypeProgress:
			out.Frame = &gatewaypb.ScheduleChunk_Progress{Progress: &gatewaypb.ScheduleProgress{
				Pct:  sc.Chunk.Pct,
				Note: sc.Chunk.Note,
			}}
		case worker.JobChunkTypeCompleted:
			out.Frame = &gatewaypb.ScheduleChunk_Completed{Completed: &gatewaypb.ScheduleCompleted{
				FinalResponse: sc.Chunk.Final,
			}}
		case worker.JobChunkTypeError:
			out.Frame = &gatewaypb.ScheduleChunk_Error{Error: &gatewaypb.ScheduleError{
				Message: sc.Chunk.ErrText,
			}}
		default:
			continue
		}
		if err := stream.Send(out); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctxerr.With(fmt.Errorf("streaming schedule chunk: %w", err), map[string]any{"runtime_name": s.runtimeName, "job_id": req.GetJobId()})
		}
	}
}

// EvictModel asks the scheduler to unload modelID from one (or all) workers
// of the gateway's kind.
func (s *schedulerServiceServer) EvictModel(_ context.Context, req *gatewaypb.EvictModelRequest) (*gatewaypb.EvictModelResponse, error) {
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "evict_model: model_id required")
	}
	n, err := s.sched.EvictModel(s.runtimeName, req.ModelId, req.WorkerId)
	if err != nil {
		return nil, mapErrToGRPC(err)
	}
	return &gatewaypb.EvictModelResponse{EvictedCount: int32(n)}, nil
}

// DownloadFiles enqueues every file the gateway's install UI resolved
// into MASS's downloads.Manager. The gateway runs the operator's
// registry choice through its own picker (HF, ModelScope, etc.) and
// hands MASS the URL + canonical relative-path list — MASS owns
// fetching, persistence, retry, and the in-flight panel UI.
//
// Returns the count of files MASS accepted into the queue. Files
// whose destination is already on disk (or already in flight) are
// silently skipped — same idempotence semantics the HTTP install
// handler used.
func (s *schedulerServiceServer) DownloadFiles(_ context.Context, req *gatewaypb.DownloadFilesRequest) (*gatewaypb.DownloadFilesResponse, error) {
	if s.downloads == nil {
		return nil, status.Error(codes.Unavailable, "download_files: downloads manager not wired")
	}
	groupName := strings.TrimSpace(req.GetGroupName())
	if groupName == "" {
		return nil, status.Error(codes.InvalidArgument, "download_files: group_name is required")
	}
	files := req.GetFiles()
	if len(files) == 0 {
		return nil, status.Error(codes.InvalidArgument, "download_files: at least one file is required")
	}

	// One groupKey per submission so all files cluster on the
	// in-flight panel. The exact shape doesn't matter so long as
	// it's stable for the duration of the install.
	groupKey := s.runtimeName + ":" + groupName

	// Reject the whole batch when any rel_path is hostile — a gateway
	// that sends one traversal path is not to be trusted with the rest.
	for _, f := range files {
		if err := downloads.ValidateRelPath(f.GetRelPath()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "download_files: %v", err)
		}
	}

	queued := 0
	for _, f := range files {
		spec := downloads.Job{
			RelPath:     f.GetRelPath(),
			URL:         f.GetUrl(),
			RuntimeName: s.runtimeName,
			GroupKey:    groupKey,
			GroupName:   groupName,
			Filename:    filepath.Base(f.GetRelPath()),
			Status:      store.DownloadStatusActive,
			Total:       f.GetSizeBytes(),
		}
		if err := s.downloads.Start(spec); err != nil {
			if errors.Is(err, downloads.ErrAlreadyExists) || errors.Is(err, downloads.ErrAlreadyDone) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "queueing download: %v", err)
		}
		queued++
	}
	return &gatewaypb.DownloadFilesResponse{Queued: int32(queued)}, nil
}

// GetResult fetches a submitted job's durable result by request_id. Reads
// MASS's persistent result store (retained for the result TTL), so a gateway
// can poll long after the job finished. Returns NotFound when the request_id
// is unknown or its result has expired.
func (s *schedulerServiceServer) GetResult(_ context.Context, req *gatewaypb.GetResultRequest) (*gatewaypb.GetResultResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "get_result: request_id required")
	}
	r, err := s.sched.GetResult(req.GetRequestId())
	if err != nil {
		return nil, mapErrToGRPC(err)
	}
	return &gatewaypb.GetResultResponse{
		Status: resultStatusToProto(r.Status),
		Body:   r.Body,
		Error:  r.Error,
	}, nil
}

// CancelJob cancels a submitted job by request_id, whether it is still
// pending or already running. Returns NotFound when no live job matches.
func (s *schedulerServiceServer) CancelJob(ctx context.Context, req *gatewaypb.CancelJobRequest) (*gatewaypb.CancelJobResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "cancel_job: request_id required")
	}
	if err := s.sched.CancelByRequestID(ctx, req.GetRequestId()); err != nil {
		return nil, mapErrToGRPC(err)
	}
	return &gatewaypb.CancelJobResponse{}, nil
}

// resultStatusToProto maps the store's lifecycle status onto the wire enum.
func resultStatusToProto(st queue.ResultStatus) gatewaypb.ResultStatus {
	switch st {
	case queue.ResultStatusPending:
		return gatewaypb.ResultStatus_RESULT_STATUS_PENDING
	case queue.ResultStatusProcessing:
		return gatewaypb.ResultStatus_RESULT_STATUS_PROCESSING
	case queue.ResultStatusDone:
		return gatewaypb.ResultStatus_RESULT_STATUS_DONE
	case queue.ResultStatusError:
		return gatewaypb.ResultStatus_RESULT_STATUS_ERROR
	default:
		return gatewaypb.ResultStatus_RESULT_STATUS_UNSPECIFIED
	}
}

// priorityFromProto maps the gateway-supplied JobPriority onto the queue's
// priority levels. UNSPECIFIED (the zero value) defaults to Medium so a
// gateway that omits the field gets the same behaviour as before.
func priorityFromProto(p gatewaypb.JobPriority) queue.Priority {
	switch p {
	case gatewaypb.JobPriority_JOB_PRIORITY_LOW:
		return queue.PriorityLow
	case gatewaypb.JobPriority_JOB_PRIORITY_HIGH:
		return queue.PriorityHigh
	case gatewaypb.JobPriority_JOB_PRIORITY_CRITICAL:
		return queue.PriorityCritical
	default:
		return queue.PriorityMedium
	}
}

// ListWorkers returns the gateway's view of online workers of its runtime
// kind, with current capacity + loaded models.
func (s *schedulerServiceServer) ListWorkers(_ context.Context, _ *gatewaypb.ListWorkersRequest) (*gatewaypb.ListWorkersResponse, error) {
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
	case errors.Is(err, scheduler.ErrInvalidCost),
		errors.Is(err, scheduler.ErrFieldTooLong):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, scheduler.ErrNoResult),
		errors.Is(err, scheduler.ErrNotInflight):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, scheduler.ErrWorkerGone):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
