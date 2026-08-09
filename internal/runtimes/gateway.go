package runtimes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// LoadedGateway is a launched runtime gateway subprocess.
//
// Lifecycle:
//  1. Manager.Start spawns the binary via go-plugin (which handshakes,
//     dials gRPC, and gives us a [gatewayClient]).
//  2. Manager calls Init on the gateway. The gateway tells us its
//     runtime_name, version, supported formats. If the kind doesn't match
//     what we expect from the install, the launch is aborted.
//  3. The gateway is added to the manager's "running" map and is now
//     reachable via [LoadedGateway.Client] (HandleRequest, ListGroups,
//     PlanModelFiles, PlanLocalImport, RenameGroup).
//  4. On Stop or process exit, [LoadedGateway.Close] tears down the
//     plugin client + the callback gRPC server hosted on the broker.
type LoadedGateway struct {
	Kind     string
	Manifest Manifest

	pluginClient *plugin.Client
	client       *gatewayClient
	callbackSrv  *grpc.Server // serves MassScheduler back to the gateway
	closeOnce    sync.Once
	logger       zerolog.Logger

	// exited reports whether the gateway subprocess has terminated. Set to
	// the go-plugin client's Exited by startGateway; overridable in tests.
	exited func() bool
}

// RuntimeName reports the loaded gateway's runtime kind. Defined as a method
// so [LoadedGateway] satisfies the [GatewayLike] interface used by both the
// scheduler callback wiring and the model store.
func (g *LoadedGateway) RuntimeName() string { return g.Kind }

// RuntimeVersion reports the version string from the gateway manifest. Used
// by the model-metadata cache to invalidate rows when a runtime is upgraded.
func (g *LoadedGateway) RuntimeVersion() string { return g.Manifest.Version }

// Client returns the gRPC client MASS uses to invoke gateway methods. Safe to
// call from any goroutine; the underlying connection is multiplexed.
func (g *LoadedGateway) Client() gatewaypb.RuntimeGatewayClient {
	return g.client.gateway
}

// ListGroups asks the gateway for its fully-grouped catalogue. Each
// Group carries an opaque id (slug of the operator-typed name),
// display label, type, capabilities, and child Models (one per file
// on disk). The gateway owns grouping; MASS just renders.
func (g *LoadedGateway) ListGroups(ctx context.Context) ([]*gatewaypb.Group, error) {
	resp, err := g.client.gateway.ListGroups(ctx, &gatewaypb.GatewayListGroupsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetGroups(), nil
}

// PlanLocalImport plans a Browse Local install. groupName is the
// operator-typed identity (required). MASS calls this once per
// picked file; same-name imports merge into one Group.
func (g *LoadedGateway) PlanLocalImport(ctx context.Context, srcPath, groupName string) ([]*gatewaypb.DownloadFile, error) {
	resp, err := g.client.gateway.PlanLocalImport(ctx, &gatewaypb.PlanLocalImportRequest{
		SrcPath:   srcPath,
		GroupName: groupName,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFiles(), nil
}

// PlanRemoteImport asks the gateway to resolve a remote pick (repo+filename)
// into the files MASS should fetch. The gateway plans; MASS downloads.
func (g *LoadedGateway) PlanRemoteImport(ctx context.Context, repoID, filename, groupName string) ([]*gatewaypb.DownloadFile, error) {
	resp, err := g.client.gateway.PlanRemoteImport(ctx, &gatewaypb.PlanRemoteImportRequest{
		RepoId:    repoID,
		Filename:  filename,
		GroupName: groupName,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFiles(), nil
}

// PlanDelete asks the gateway which store-relative files make up the model
// identified by id (primary + companions). MASS removes them.
func (g *LoadedGateway) PlanDelete(ctx context.Context, id string) ([]string, error) {
	resp, err := g.client.gateway.PlanDelete(ctx, &gatewaypb.PlanDeleteRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return resp.GetRelPaths(), nil
}

// RenameGroup asks the gateway to rewrite every catalogue entry
// belonging to one Group so its display label matches new_name. The
// id is the slug MASS already holds (Group.id from ListGroups).
func (g *LoadedGateway) RenameGroup(ctx context.Context, id, newName string) error {
	_, err := g.client.gateway.RenameGroup(ctx, &gatewaypb.RenameGroupRequest{
		Id:      id,
		NewName: newName,
	})
	return err
}

// Close terminates the gateway subprocess and stops the callback server.
// Idempotent; safe to call multiple times.
func (g *LoadedGateway) Close() {
	g.closeOnce.Do(func() {
		if g.callbackSrv != nil {
			g.callbackSrv.GracefulStop()
		}
		if g.pluginClient != nil {
			g.pluginClient.Kill()
		}
		g.logger.Info().Str("runtime_name", g.Kind).Msg("gateway stopped")
	})
}

// startGateway launches the gateway binary, runs the Init handshake, and
// brings up the MassScheduler callback service on go-plugin's broker.
//
// Returns the LoadedGateway ready for HandleRequest / ListModels calls.
func startGateway(ctx context.Context, mf Manifest, binaryPath string, dataDir, modelsDir, logLevel string, sched *scheduler.Scheduler, dl *downloads.Manager, logger zerolog.Logger) (*LoadedGateway, error) {
	if binaryPath == "" {
		return nil, fmt.Errorf("startGateway: binary path required")
	}

	gwLogger := logger.With().Str("runtime_name", mf.RuntimeName).Logger()

	cmd := exec.Command(binaryPath)
	cmd.SysProcAttr = hiddenSysProcAttr()
	pluginClient := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  Handshake,
		Plugins:          PluginMap,
		Cmd:              cmd,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           hclogAdapter(gwLogger),
		StartTimeout:     30 * time.Second,
	})

	rpcClient, err := pluginClient.Client()
	if err != nil {
		pluginClient.Kill()
		return nil, ctxerr.With(fmt.Errorf("dialing gateway plugin: %w", err), map[string]any{"runtime_name": mf.RuntimeName, "binary": binaryPath})
	}

	raw, err := rpcClient.Dispense(PluginName)
	if err != nil {
		pluginClient.Kill()
		return nil, ctxerr.With(fmt.Errorf("dispensing %s: %w", PluginName, err), map[string]any{"runtime_name": mf.RuntimeName})
	}
	gw, ok := raw.(*gatewayClient)
	if !ok {
		pluginClient.Kill()
		return nil, fmt.Errorf("dispensed plugin is not a gatewayClient (got %T)", raw)
	}

	loaded := &LoadedGateway{
		Kind:         mf.RuntimeName,
		Manifest:     mf,
		pluginClient: pluginClient,
		client:       gw,
		logger:       gwLogger,
		exited:       pluginClient.Exited,
	}

	// Stand up the MassScheduler callback service on go-plugin's broker.
	// The gateway dials it through brokered gRPC; we never expose a TCP
	// port to the host network.
	callbackSrv, brokerID, err := startCallbackService(gw.broker, mf.RuntimeName, sched, dl, gwLogger)
	if err != nil {
		loaded.Close()
		return nil, err
	}
	loaded.callbackSrv = callbackSrv

	// Run Init last — gateway might want to open a callback connection
	// during Init, and the broker is now ready to serve it.
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := gw.gateway.Init(initCtx, &gatewaypb.InitRequest{
		DataDir:               dataDir,
		ModelsDir:             modelsDir,
		LogLevel:              logLevel,
		MassSchedulerBrokerId: brokerID,
		MassGatewayApiVersion: gatewaypb.GatewayAPIVersion,
	})
	if err != nil {
		loaded.Close()
		return nil, ctxerr.With(fmt.Errorf("gateway Init: %w", err), map[string]any{"runtime_name": mf.RuntimeName})
	}
	if resp.RuntimeName != mf.RuntimeName {
		loaded.Close()
		return nil, ctxerr.With(fmt.Errorf("gateway reported runtime_name %q but install expected %q", resp.RuntimeName, mf.RuntimeName), map[string]any{"runtime_name": mf.RuntimeName, "reported": resp.RuntimeName})
	}
	// Reject gateways built against a wire version MASS cannot speak.
	// Gateways too old return GatewayApiVersion=0 (zero value); gateways
	// too new advertise a version above MASS's pinned constant. Either
	// way refuse the launch with a clear message — operators should
	// reinstall a compatible package.
	if resp.GatewayApiVersion != gatewaypb.GatewayAPIVersion {
		loaded.Close()
		return nil, ctxerr.With(
			fmt.Errorf("gateway api version mismatch: gateway reports %d, MASS speaks %d", resp.GatewayApiVersion, gatewaypb.GatewayAPIVersion),
			map[string]any{
				"runtime_name":    mf.RuntimeName,
				"gateway_version": resp.GatewayApiVersion,
				"mass_version":    gatewaypb.GatewayAPIVersion,
			},
		)
	}
	// Reconcile the in-memory manifest with what the gateway just reported.
	loaded.Manifest.Version = resp.Version
	if resp.DisplayName != "" {
		loaded.Manifest.DisplayName = resp.DisplayName
	}
	if resp.Description != "" {
		loaded.Manifest.Description = resp.Description
	}
	gwLogger.Info().Str("version", resp.Version).Msg("gateway started")
	return loaded, nil
}

// startCallbackService stands up the MassScheduler gRPC server on a brokered
// connection so the gateway can invoke Schedule / EnsureModelLoaded / etc.
// Returns the broker stream ID the gateway must Dial to reach it; this is
// passed to the gateway via InitRequest.MassSchedulerBrokerId.
func startCallbackService(broker *plugin.GRPCBroker, runtimeName string, sched *scheduler.Scheduler, dl *downloads.Manager, logger zerolog.Logger) (*grpc.Server, uint32, error) {
	if broker == nil {
		return nil, 0, errors.New("startCallbackService: broker not provided by go-plugin")
	}
	brokerID := broker.NextId()

	srv := grpc.NewServer()
	gatewaypb.RegisterMassSchedulerServer(srv, newSchedulerServiceServer(runtimeName, sched, dl, logger))

	go func() {
		listener, err := broker.Accept(brokerID)
		if err != nil {
			logger.Error().Err(err).Msg("accepting callback connection")
			return
		}
		if err := srv.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Warn().Err(err).Msg("callback server stopped")
		}
	}()

	return srv, brokerID, nil
}

// hclogAdapter routes hashicorp/go-hclog output (used by go-plugin internals
// and the plugin's stderr) into our zerolog-based logger.
type zlogHCAdapter struct {
	logger zerolog.Logger
	name   string
}

func hclogAdapter(logger zerolog.Logger) hclog.Logger {
	return &zlogHCAdapter{logger: logger}
}

func (z *zlogHCAdapter) emit(level hclog.Level, msg string, args ...any) {
	var ev *zerolog.Event
	switch level {
	case hclog.Trace, hclog.Debug:
		ev = z.logger.Debug()
	case hclog.Info, hclog.NoLevel:
		ev = z.logger.Info()
	case hclog.Warn:
		ev = z.logger.Warn()
	case hclog.Error:
		ev = z.logger.Error()
	case hclog.Off:
		return
	}
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		ev = ev.Interface(key, args[i+1])
	}
	if z.name != "" {
		ev = ev.Str("hclog_logger", z.name)
	}
	ev.Msg(msg)
}

func (z *zlogHCAdapter) Log(level hclog.Level, msg string, args ...any) { z.emit(level, msg, args...) }
func (z *zlogHCAdapter) Trace(msg string, args ...any)                  { z.emit(hclog.Trace, msg, args...) }
func (z *zlogHCAdapter) Debug(msg string, args ...any)                  { z.emit(hclog.Debug, msg, args...) }
func (z *zlogHCAdapter) Info(msg string, args ...any)                   { z.emit(hclog.Info, msg, args...) }
func (z *zlogHCAdapter) Warn(msg string, args ...any)                   { z.emit(hclog.Warn, msg, args...) }
func (z *zlogHCAdapter) Error(msg string, args ...any)                  { z.emit(hclog.Error, msg, args...) }

func (z *zlogHCAdapter) IsTrace() bool { return zerolog.GlobalLevel() <= zerolog.TraceLevel }
func (z *zlogHCAdapter) IsDebug() bool { return zerolog.GlobalLevel() <= zerolog.DebugLevel }
func (z *zlogHCAdapter) IsInfo() bool  { return zerolog.GlobalLevel() <= zerolog.InfoLevel }
func (z *zlogHCAdapter) IsWarn() bool  { return zerolog.GlobalLevel() <= zerolog.WarnLevel }
func (z *zlogHCAdapter) IsError() bool { return zerolog.GlobalLevel() <= zerolog.ErrorLevel }

func (z *zlogHCAdapter) ImpliedArgs() []any            { return nil }
func (z *zlogHCAdapter) With(args ...any) hclog.Logger { return z }
func (z *zlogHCAdapter) Name() string                  { return z.name }
func (z *zlogHCAdapter) Named(name string) hclog.Logger {
	return &zlogHCAdapter{logger: z.logger.With().Str("plugin_logger", name).Logger(), name: name}
}
func (z *zlogHCAdapter) ResetNamed(name string) hclog.Logger {
	return &zlogHCAdapter{logger: z.logger.With().Str("plugin_logger", name).Logger(), name: name}
}
func (z *zlogHCAdapter) SetLevel(_ hclog.Level) {}
func (z *zlogHCAdapter) GetLevel() hclog.Level  { return hclog.Trace }
func (z *zlogHCAdapter) StandardLogger(_ *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(z, "", 0)
}
func (z *zlogHCAdapter) StandardWriter(_ *hclog.StandardLoggerOptions) io.Writer { return z }

// Write receives raw, unstructured subprocess output (go-plugin relays the
// plugin's stderr through here). It's diagnostic chatter, not an operational
// event, so it lands at Debug; the trailing newline is trimmed so the line
// isn't logged with a dangling break.
func (z *zlogHCAdapter) Write(p []byte) (int, error) {
	z.logger.Debug().Msg(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
