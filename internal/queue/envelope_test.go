package queue_test

import (
	"strings"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
)

// Identity fields at exactly the 255-byte wire cap round-trip byte-exact —
// the format never truncates. One byte over is an invariant violation
// (inputs are validated at the Submit boundary) and must panic rather than
// silently corrupt identity.
func TestEnvelope_MarshalIdentityFieldCap(t *testing.T) {
	max := strings.Repeat("a", 255)

	t.Run("255-byte fields round-trip exactly", func(t *testing.T) {
		in := queue.Envelope{
			RuntimeName: max,
			ModelID:     max,
			Source:      max,
			RequestID:   max,
			GlobalMsgID: max,
			Payload:     []byte("p"),
		}
		out, err := queue.UnmarshalEnvelope(in.Marshal())
		require.NoError(t, err)
		require.Equal(t, max, out.RuntimeName)
		require.Equal(t, max, out.ModelID)
		require.Equal(t, max, out.Source)
		require.Equal(t, max, out.RequestID)
		require.Equal(t, max, out.GlobalMsgID)
		require.Equal(t, []byte("p"), out.Payload)
	})

	oversize := max + "a"
	tests := []struct {
		name string
		env  queue.Envelope
	}{
		{name: "runtime_name", env: queue.Envelope{RuntimeName: oversize}},
		{name: "model_id", env: queue.Envelope{ModelID: oversize}},
		{name: "source", env: queue.Envelope{Source: oversize}},
		{name: "request_id", env: queue.Envelope{RequestID: oversize}},
		{name: "global_msg_id", env: queue.Envelope{GlobalMsgID: oversize}},
	}
	for _, tt := range tests {
		t.Run("256-byte "+tt.name+" panics", func(t *testing.T) {
			require.Panics(t, func() { tt.env.Marshal() })
		})
	}
}
