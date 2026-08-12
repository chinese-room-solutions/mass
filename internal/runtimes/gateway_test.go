package runtimes

import (
	"fmt"
	"testing"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/stretchr/testify/require"
)

func TestCheckGatewayProtocol(t *testing.T) {
	tests := []struct {
		name     string
		reported int32
		wantErr  bool
	}{
		{"version MASS speaks accepts", gatewaypb.SupportedProtocols[0], false},
		{"unset (outdated gateway) rejects", 0, true},
		{"newer than MASS speaks rejects", 99, true},
		{"negative rejects", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkGatewayProtocol(tt.reported)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			// The message must name both sides so the operator knows which
			// half to reinstall.
			require.Contains(t, err.Error(), fmt.Sprintf("%d", tt.reported))
			require.Contains(t, err.Error(), fmt.Sprintf("%v", gatewaypb.SupportedProtocols))
		})
	}
}
