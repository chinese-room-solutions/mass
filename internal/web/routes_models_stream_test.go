package web

import (
	"strings"
	"testing"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/stretchr/testify/require"
)

func TestModelTypeLabel(t *testing.T) {
	tests := []struct {
		name string
		mt   *gatewaypb.ModelTypeEntry
		want string
	}{
		{"nil", nil, ""},
		{"explicit label wins", &gatewaypb.ModelTypeEntry{Label: "Custom", Kind: gatewaypb.ModelTypeKind_MODEL_TYPE_CHAT}, "Custom"},
		{"chat kind", &gatewaypb.ModelTypeEntry{Kind: gatewaypb.ModelTypeKind_MODEL_TYPE_CHAT}, "Chat"},
		{"embedding kind", &gatewaypb.ModelTypeEntry{Kind: gatewaypb.ModelTypeKind_MODEL_TYPE_EMBEDDING}, "Embedding"},
		{"rerank kind", &gatewaypb.ModelTypeEntry{Kind: gatewaypb.ModelTypeKind_MODEL_TYPE_RERANK}, "Rerank"},
		{"unspecified kind", &gatewaypb.ModelTypeEntry{Kind: gatewaypb.ModelTypeKind_MODEL_TYPE_UNSPECIFIED}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, modelTypeLabel(tt.mt))
		})
	}
}

func TestPrettyRuntimeName(t *testing.T) {
	require.Equal(t, "llama.cpp", prettyRuntimeName("llama-cpp"))
	require.Equal(t, "vllm", prettyRuntimeName("vllm"))
	require.Equal(t, "", prettyRuntimeName(""))
}

func TestGroupDOMID(t *testing.T) {
	a := groupDOMID("group-one")
	b := groupDOMID("group-two")

	require.Len(t, a, 12, "12 hex chars (6 bytes)")
	require.NotEqual(t, a, b, "different inputs → different ids")
	require.Equal(t, a, groupDOMID("group-one"), "stable across calls")
	require.NotContains(t, a, "/", "DOM/CSS-safe")
	require.NotContains(t, a, " ")
}

func TestAppendUnique(t *testing.T) {
	got := appendUnique([]string{"a", "b"}, "b")
	require.Equal(t, []string{"a", "b"}, got, "duplicate not appended")

	got = appendUnique([]string{"a"}, "c")
	require.Equal(t, []string{"a", "c"}, got, "new value appended")

	got = appendUnique(nil, "x")
	require.Equal(t, []string{"x"}, got)
}

func TestWriteCapabilityIcons(t *testing.T) {
	var b strings.Builder
	writeCapabilityIcons(&b, nil, capIconStyleGroup)
	require.Empty(t, b.String(), "nil caps → nothing")

	b.Reset()
	writeCapabilityIcons(&b, &gatewaypb.Capabilities{Thinking: true, Vision: true}, capIconStyleGroup)
	out := b.String()
	require.Contains(t, out, "lightbulb", "thinking icon")
	require.Contains(t, out, "eye", "vision icon")
	require.NotContains(t, out, "mic", "audio not set → no mic icon")
}
