package runtimes

import (
	"context"
	"fmt"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

// Handshake is the go-plugin handshake config. The cookie + magic value let
// MASS verify that an executed binary is one of our gateways and not some
// random program. Gateway implementations must set the same values.
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "MASS_RUNTIME_PLUGIN",
	MagicCookieValue: "1f2c0c1e-mass-runtime-gateway",
}

// PluginName is the key gateways register their plugin under. Keep stable —
// changing it breaks every installed gateway.
const PluginName = "runtime_gateway"

// gatewayPlugin is the host-side plugin descriptor. MASS only ever consumes
// gateways (it never serves them itself), so [gatewayPlugin.GRPCServer]
// returns an error.
type gatewayPlugin struct {
	plugin.Plugin
}

// GRPCClient builds the host-side wrapper around a plugin client connection.
// This is what MASS receives back from go-plugin when it dispenses the
// runtime_gateway plugin.
func (gatewayPlugin) GRPCClient(_ context.Context, broker *plugin.GRPCBroker, conn *grpc.ClientConn) (any, error) {
	return &gatewayClient{
		gateway: gatewaypb.NewRuntimeGatewayClient(conn),
		broker:  broker,
	}, nil
}

// GRPCServer is unused on the host side — MASS never serves the gateway
// service.
func (gatewayPlugin) GRPCServer(_ *plugin.GRPCBroker, _ *grpc.Server) error {
	return fmt.Errorf("gatewayPlugin: MASS never hosts the gateway service")
}

// PluginMap is what MASS hands to plugin.NewClient. Gateway processes must
// declare the same map on their side via plugin.Serve.
var PluginMap = map[string]plugin.Plugin{
	PluginName: gatewayPlugin{},
}

// gatewayClient is the host-side handle MASS uses to talk to a launched
// gateway. It wraps the typed gRPC client and exposes the broker so the
// MassScheduler callback service can be served back to the plugin.
type gatewayClient struct {
	gateway gatewaypb.RuntimeGatewayClient
	broker  *plugin.GRPCBroker
}
