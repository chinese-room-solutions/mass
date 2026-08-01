package web

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/stretchr/testify/require"
)

// The Mass Connect methods are thin adapters over the shared model-ops core;
// these cover the boundary the core owns without a live gateway: validation
// and sentinel→Connect-code mapping. The deep happy path (real gateway plan +
// byte copy) is covered by the gateway's PlanDelete test and the /api import
// path.
func TestMassService_ConnectErrorMapping(t *testing.T) {
	h := newTestHandler(t) // real runtimes manager, no gateways running, no downloads wired
	svc := &massService{h: h}
	ctx := context.Background()

	t.Run("ListModels with no running runtimes is empty, not an error", func(t *testing.T) {
		resp, err := svc.ListModels(ctx, connect.NewRequest(&rpc.ListModelsRequest{}))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetGroups())
	})

	t.Run("ImportLocalModel missing runtime_name -> InvalidArgument", func(t *testing.T) {
		_, err := svc.ImportLocalModel(ctx, connect.NewRequest(&rpc.ImportLocalModelRequest{
			Path: "/tmp/x.gguf", OperatorName: "m",
		}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("ImportRemoteModel missing operator_name -> InvalidArgument", func(t *testing.T) {
		_, err := svc.ImportRemoteModel(ctx, connect.NewRequest(&rpc.ImportRemoteModelRequest{
			RuntimeName: "llama-cpp", RepoId: "o/r", Filename: "f.gguf",
		}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("ImportLocalModel unknown runtime -> FailedPrecondition", func(t *testing.T) {
		_, err := svc.ImportLocalModel(ctx, connect.NewRequest(&rpc.ImportLocalModelRequest{
			RuntimeName: "not-installed", Path: "/tmp/x.gguf", OperatorName: "m",
		}))
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	})

	t.Run("DeleteModel missing id -> InvalidArgument", func(t *testing.T) {
		_, err := svc.DeleteModel(ctx, connect.NewRequest(&rpc.DeleteModelRequest{
			RuntimeName: "llama-cpp",
		}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("DeleteModel unknown runtime -> FailedPrecondition", func(t *testing.T) {
		_, err := svc.DeleteModel(ctx, connect.NewRequest(&rpc.DeleteModelRequest{
			RuntimeName: "not-installed", Id: "some/model.gguf",
		}))
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	})
}

// The control-plane Connect methods are thin adapters over the shared ops core
// (runtimeops/workerops/schedulerops/queueops). These cover the read paths and
// sentinel→Connect-code mapping the adapters own, without a live gateway,
// worker, or queue subsystem.
func TestMassService_ControlPlane(t *testing.T) {
	h := newTestHandler(t) // real managers, nothing installed/connected/queued
	svc := &massService{h: h}
	ctx := context.Background()

	t.Run("ListRuntimes empty on a fresh instance", func(t *testing.T) {
		resp, err := svc.ListRuntimes(ctx, connect.NewRequest(&rpc.ListRuntimesRequest{}))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetRuntimes())
	})

	t.Run("StartRuntime unknown -> NotFound", func(t *testing.T) {
		_, err := svc.StartRuntime(ctx, connect.NewRequest(&rpc.StartRuntimeRequest{
			RuntimeName: "not-installed",
		}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("SetRuntimeAutoStart unknown -> NotFound", func(t *testing.T) {
		_, err := svc.SetRuntimeAutoStart(ctx, connect.NewRequest(&rpc.SetRuntimeAutoStartRequest{
			RuntimeName: "not-installed", Enabled: true,
		}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("InstallRuntime missing path -> InvalidArgument", func(t *testing.T) {
		_, err := svc.InstallRuntime(ctx, connect.NewRequest(&rpc.InstallRuntimeRequest{}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("ListWorkers empty with no fleet connected", func(t *testing.T) {
		resp, err := svc.ListWorkers(ctx, connect.NewRequest(&rpc.ListWorkersRequest{}))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetWorkers())
	})

	t.Run("CreateJoinToken mints a token with default and explicit TTL", func(t *testing.T) {
		before := time.Now().Unix()

		resp, err := svc.CreateJoinToken(ctx, connect.NewRequest(&rpc.CreateJoinTokenRequest{}))
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(resp.Msg.GetToken(), "mjt_"), "token: %q", resp.Msg.GetToken())
		// 0 TTL selects the 3600s server default.
		require.GreaterOrEqual(t, resp.Msg.GetExpiresAt(), before+3600-5)

		resp2, err := svc.CreateJoinToken(ctx, connect.NewRequest(&rpc.CreateJoinTokenRequest{TtlSeconds: 60}))
		require.NoError(t, err)
		require.Less(t, resp2.Msg.GetExpiresAt(), before+3600)
		require.NotEqual(t, resp.Msg.GetToken(), resp2.Msg.GetToken())
	})

	t.Run("ListInstances empty with no loaded models", func(t *testing.T) {
		resp, err := svc.ListInstances(ctx, connect.NewRequest(&rpc.ListInstancesRequest{}))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetInstances())
	})

	t.Run("EvictInstance missing fields -> InvalidArgument", func(t *testing.T) {
		_, err := svc.EvictInstance(ctx, connect.NewRequest(&rpc.EvictInstanceRequest{}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("EvictInstance unknown worker -> NotFound", func(t *testing.T) {
		_, err := svc.EvictInstance(ctx, connect.NewRequest(&rpc.EvictInstanceRequest{
			WorkerId: "ghost", ModelId: "m.gguf",
		}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("GetQueue empty on a bare scheduler", func(t *testing.T) {
		resp, err := svc.GetQueue(ctx, connect.NewRequest(&rpc.GetQueueRequest{}))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetSections())
	})

	t.Run("CancelQueuedJob missing fields -> InvalidArgument", func(t *testing.T) {
		_, err := svc.CancelQueuedJob(ctx, connect.NewRequest(&rpc.CancelQueuedJobRequest{}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("CancelQueuedJob unknown queue -> NotFound", func(t *testing.T) {
		_, err := svc.CancelQueuedJob(ctx, connect.NewRequest(&rpc.CancelQueuedJobRequest{
			Queue: "worker|ghost", MsgId: "m1",
		}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("GetStatus reports version + zeroed counters", func(t *testing.T) {
		h.version = "test-1.2.3"
		resp, err := svc.GetStatus(ctx, connect.NewRequest(&rpc.GetStatusRequest{}))
		require.NoError(t, err)
		require.Equal(t, "test-1.2.3", resp.Msg.GetVersion())
		require.Equal(t, h.cfg.ListenAddr, resp.Msg.GetListenAddr())
		require.Zero(t, resp.Msg.GetRuntimesInstalled())
		require.Zero(t, resp.Msg.GetWorkersTotal())
		require.Zero(t, resp.Msg.GetQueuedJobs())
		require.Zero(t, resp.Msg.GetRunningJobs())
	})
}
