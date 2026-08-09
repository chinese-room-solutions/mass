package web

import (
	"context"
	"time"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

// Connect adapters for the mass.v1.Mass control plane: runtimes, workers,
// loaded instances, the queue, and process status. Each is a thin adapter over
// the shared ops core (runtimeops.go, workerops.go, schedulerops.go,
// queueops.go) — the same core the dashboard's /api/* handlers call — mapping
// neutral view structs onto proto messages and sentinels onto Connect codes.

// --- Runtimes ---

func (s *massService) ListRuntimes(_ context.Context, _ *connect.Request[rpc.ListRuntimesRequest]) (*connect.Response[rpc.ListRuntimesResponse], error) {
	infos := s.h.listRuntimeInfos()
	out := make([]*rpc.Runtime, 0, len(infos))
	for _, ri := range infos {
		out = append(out, runtimeToProto(ri))
	}
	return connect.NewResponse(&rpc.ListRuntimesResponse{Runtimes: out}), nil
}

func (s *massService) InstallRuntime(_ context.Context, req *connect.Request[rpc.InstallRuntimeRequest]) (*connect.Response[rpc.InstallRuntimeResponse], error) {
	ri, err := s.h.installRuntime(req.Msg.GetPath(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.InstallRuntimeResponse{Runtime: runtimeToProto(ri)}), nil
}

func (s *massService) UninstallRuntime(_ context.Context, req *connect.Request[rpc.UninstallRuntimeRequest]) (*connect.Response[rpc.UninstallRuntimeResponse], error) {
	if err := s.h.uninstallRuntime(req.Msg.GetRuntimeName(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.UninstallRuntimeResponse{}), nil
}

func (s *massService) StartRuntime(ctx context.Context, req *connect.Request[rpc.StartRuntimeRequest]) (*connect.Response[rpc.StartRuntimeResponse], error) {
	if err := s.h.startRuntime(ctx, req.Msg.GetRuntimeName(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.StartRuntimeResponse{}), nil
}

func (s *massService) StopRuntime(_ context.Context, req *connect.Request[rpc.StopRuntimeRequest]) (*connect.Response[rpc.StopRuntimeResponse], error) {
	if err := s.h.stopRuntime(req.Msg.GetRuntimeName(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.StopRuntimeResponse{}), nil
}

func (s *massService) SetRuntimeAutoStart(_ context.Context, req *connect.Request[rpc.SetRuntimeAutoStartRequest]) (*connect.Response[rpc.SetRuntimeAutoStartResponse], error) {
	if err := s.h.setRuntimeAutoStart(req.Msg.GetRuntimeName(), req.Msg.GetEnabled(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.SetRuntimeAutoStartResponse{}), nil
}

// --- Package registry ---

func (s *massService) SearchPackages(ctx context.Context, req *connect.Request[rpc.SearchPackagesRequest]) (*connect.Response[rpc.SearchPackagesResponse], error) {
	res, err := s.h.searchPackages(ctx, req.Msg.GetKind(), req.Msg.GetQuery(), req.Msg.GetRuntimeName())
	if err != nil {
		return nil, connectErr(err)
	}
	pkgs := make([]*rpc.Package, 0, len(res.Packages))
	for _, p := range res.Packages {
		versions := make([]*rpc.PackageVersion, 0, len(p.Versions))
		for _, v := range p.Versions {
			versions = append(versions, &rpc.PackageVersion{Version: v.Version, HasArtifact: v.HasArtifact})
		}
		pkgs = append(pkgs, &rpc.Package{
			Name:        p.Name,
			Kind:        p.Kind,
			RuntimeName: p.RuntimeName,
			DisplayName: p.DisplayName,
			Description: p.Description,
			Versions:    versions,
		})
	}
	return connect.NewResponse(&rpc.SearchPackagesResponse{Packages: pkgs, Stale: res.Stale}), nil
}

func (s *massService) InstallRuntimeFromRegistry(ctx context.Context, req *connect.Request[rpc.InstallRuntimeFromRegistryRequest]) (*connect.Response[rpc.InstallRuntimeResponse], error) {
	ri, err := s.h.installRuntimeFromRegistry(ctx, req.Msg.GetName(), req.Msg.GetVersion(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.InstallRuntimeResponse{Runtime: runtimeToProto(ri)}), nil
}

func runtimeToProto(ri RuntimeInfo) *rpc.Runtime {
	return &rpc.Runtime{
		RuntimeName: ri.RuntimeName,
		Version:     ri.Version,
		DisplayName: ri.DisplayName,
		Description: ri.Description,
		AutoStart:   ri.AutoStart,
		Running:     ri.Running,
	}
}

// --- Workers ---

func (s *massService) ListWorkers(_ context.Context, _ *connect.Request[rpc.ListWorkersRequest]) (*connect.Response[rpc.ListWorkersResponse], error) {
	infos := s.h.workerInfos()
	out := make([]*rpc.Worker, 0, len(infos))
	for _, wi := range infos {
		devices := make([]*rpc.WorkerDevice, 0, len(wi.Devices))
		for _, di := range wi.Devices {
			devices = append(devices, &rpc.WorkerDevice{
				DeviceId:       di.DeviceID,
				DeviceName:     di.DeviceName,
				Type:           di.Type,
				Enabled:        di.Enabled,
				TotalMemoryMb:  int64(di.TotalMemoryMB),
				UsedMemoryMb:   int64(di.UsedMemoryMB),
				UtilizationPct: di.UtilizationPct,
				HasBenchmark:   di.HasBenchmark,
				MemoryGbs:      di.MemoryGBs,
				LoadGbs:        di.LoadGBs,
				Flops:          di.Flops,
			})
		}
		out = append(out, &rpc.Worker{
			Id:          wi.ID,
			Name:        wi.Name,
			RuntimeName: wi.RuntimeName,
			Online:      wi.Online,
			Enabled:     wi.Enabled,
			ActiveJobs:  int32(wi.ActiveJobs),
			Devices:     devices,
		})
	}
	return connect.NewResponse(&rpc.ListWorkersResponse{Workers: out}), nil
}

func (s *massService) SetWorkerEnabled(ctx context.Context, req *connect.Request[rpc.SetWorkerEnabledRequest]) (*connect.Response[rpc.SetWorkerEnabledResponse], error) {
	if err := s.h.setWorkerEnabled(ctx, req.Msg.GetWorkerId(), req.Msg.GetEnabled(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.SetWorkerEnabledResponse{}), nil
}

func (s *massService) SetWorkerDeviceEnabled(ctx context.Context, req *connect.Request[rpc.SetWorkerDeviceEnabledRequest]) (*connect.Response[rpc.SetWorkerDeviceEnabledResponse], error) {
	if err := s.h.setWorkerDeviceEnabled(ctx, req.Msg.GetWorkerId(), req.Msg.GetDeviceId(), req.Msg.GetEnabled(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.SetWorkerDeviceEnabledResponse{}), nil
}

func (s *massService) CreateJoinToken(_ context.Context, req *connect.Request[rpc.CreateJoinTokenRequest]) (*connect.Response[rpc.CreateJoinTokenResponse], error) {
	token, expiresAt, err := s.h.mintJoinToken(
		time.Duration(req.Msg.GetTtlSeconds())*time.Second, connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.CreateJoinTokenResponse{
		Token:     token,
		ExpiresAt: expiresAt,
	}), nil
}

func (s *massService) InstallLocalWorker(ctx context.Context, req *connect.Request[rpc.InstallLocalWorkerRequest]) (*connect.Response[rpc.InstallLocalWorkerResponse], error) {
	res, err := s.h.installLocalWorker(ctx,
		req.Msg.GetRuntimeName(), req.Msg.GetScope(), req.Msg.GetName(), connectActor(req))
	if err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.InstallLocalWorkerResponse{
		WorkerPackage: res.WorkerPackage,
		WorkerVersion: res.WorkerVersion,
		Output:        res.Output,
	}), nil
}

func (s *massService) BenchmarkWorkers(_ context.Context, req *connect.Request[rpc.BenchmarkWorkersRequest]) (*connect.Response[rpc.BenchmarkWorkersResponse], error) {
	results := s.h.benchmarkWorkers(req.Msg.GetWorkerIds(), req.Msg.GetDeviceIds())
	out := make([]*rpc.BenchmarkResult, 0, len(results))
	for _, r := range results {
		out = append(out, &rpc.BenchmarkResult{
			WorkerId:   r.WorkerID,
			DeviceId:   r.DeviceID,
			DeviceName: r.DeviceName,
			MemoryGbs:  r.MemoryGBs,
			LoadGbs:    r.LoadGBs,
			Flops:      r.Flops,
			Error:      r.Error,
		})
	}
	return connect.NewResponse(&rpc.BenchmarkWorkersResponse{Results: out}), nil
}

// --- Loaded instances ---

func (s *massService) ListInstances(_ context.Context, _ *connect.Request[rpc.ListInstancesRequest]) (*connect.Response[rpc.ListInstancesResponse], error) {
	infos := s.h.instanceInfos()
	out := make([]*rpc.Instance, 0, len(infos))
	for _, in := range infos {
		out = append(out, &rpc.Instance{
			Key:         in.Key,
			ModelId:     in.ModelID,
			Filename:    in.Filename,
			Fingerprint: in.Fingerprint,
			WorkerId:    in.WorkerID,
			WorkerName:  in.WorkerName,
			RuntimeName: in.RuntimeName,
			DeviceIds:   in.DeviceIDs,
			Source:      in.Source,
			Mode:        in.Mode,
			Status:      in.Status,
			PoolSize:    int32(in.PoolSize),
			Active:      int32(in.Active),
		})
	}
	return connect.NewResponse(&rpc.ListInstancesResponse{Instances: out}), nil
}

func (s *massService) EvictInstance(_ context.Context, req *connect.Request[rpc.EvictInstanceRequest]) (*connect.Response[rpc.EvictInstanceResponse], error) {
	if err := s.h.evictInstance(req.Msg.GetWorkerId(), req.Msg.GetModelId(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.EvictInstanceResponse{}), nil
}

// --- Queue ---

func (s *massService) GetQueue(ctx context.Context, _ *connect.Request[rpc.GetQueueRequest]) (*connect.Response[rpc.GetQueueResponse], error) {
	sections, err := s.h.queueSnapshot(ctx)
	if err != nil {
		return nil, connectErr(err)
	}
	out := make([]*rpc.QueueSection, 0, len(sections))
	for _, sec := range sections {
		out = append(out, queueSectionToProto(sec))
	}
	return connect.NewResponse(&rpc.GetQueueResponse{Sections: out}), nil
}

func (s *massService) CancelQueuedJob(ctx context.Context, req *connect.Request[rpc.CancelQueuedJobRequest]) (*connect.Response[rpc.CancelQueuedJobResponse], error) {
	if err := s.h.cancelQueuedJob(ctx, req.Msg.GetQueue(), req.Msg.GetMsgId(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.CancelQueuedJobResponse{}), nil
}

func (s *massService) CancelRunningJob(ctx context.Context, req *connect.Request[rpc.CancelRunningJobRequest]) (*connect.Response[rpc.CancelRunningJobResponse], error) {
	if err := s.h.cancelRunningJob(ctx, req.Msg.GetRequestId(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.CancelRunningJobResponse{}), nil
}

func (s *massService) EvictQueuedJob(ctx context.Context, req *connect.Request[rpc.EvictQueuedJobRequest]) (*connect.Response[rpc.EvictQueuedJobResponse], error) {
	if err := s.h.evictQueuedJob(ctx, req.Msg.GetQueue(), req.Msg.GetMsgId(), connectActor(req)); err != nil {
		return nil, connectErr(err)
	}
	return connect.NewResponse(&rpc.EvictQueuedJobResponse{}), nil
}

func queueSectionToProto(sec scheduler.QueueSection) *rpc.QueueSection {
	rows := make([]*rpc.QueueRow, 0, len(sec.Rows))
	for _, r := range sec.Rows {
		rows = append(rows, &rpc.QueueRow{
			MsgId:         r.MsgID,
			RequestId:     r.RequestID,
			RuntimeName:   r.RuntimeName,
			ModelId:       r.ModelID,
			Source:        r.Source,
			Priority:      int32(r.Priority),
			QueuedSeconds: r.QueuedSeconds,
			PayloadBytes:  int64(r.PayloadBytes),
			Running:       r.Inflight,
		})
	}
	return &rpc.QueueSection{
		Name:         sec.Name,
		WorkerId:     sec.WorkerID,
		DepthSeconds: sec.DepthSeconds,
		Rows:         rows,
	}
}

// --- Status ---

func (s *massService) GetStatus(ctx context.Context, _ *connect.Request[rpc.GetStatusRequest]) (*connect.Response[rpc.GetStatusResponse], error) {
	resp := &rpc.GetStatusResponse{Version: s.h.version}
	if s.h.cfg != nil {
		resp.ListenAddr = s.h.cfg.ListenAddr
	}
	for _, ri := range s.h.listRuntimeInfos() {
		resp.RuntimesInstalled++
		if ri.Running {
			resp.RuntimesRunning++
		}
	}
	for _, wi := range s.h.workerInfos() {
		resp.WorkersTotal++
		if wi.Online {
			resp.WorkersOnline++
		}
	}
	// Queue counters are best-effort — a missing scheduler leaves them zero
	// rather than failing the whole status probe.
	if sections, err := s.h.queueSnapshot(ctx); err == nil {
		for _, sec := range sections {
			for _, r := range sec.Rows {
				if r.Inflight {
					resp.RunningJobs++
				} else {
					resp.QueuedJobs++
				}
			}
		}
	}
	return connect.NewResponse(resp), nil
}
