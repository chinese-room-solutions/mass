package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// rejectAppSource — direct unit test
// ---------------------------------------------------------------------------

func TestRejectAppSource(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		wantReject bool
	}{
		{name: "no header", header: "", wantReject: false},
		{name: "direct caller", header: "direct", wantReject: false},
		{name: "app caller", header: "app:embedding", wantReject: true},
		{name: "app prefix only", header: "app:", wantReject: true},
		{name: "look-alike but not prefix", header: "approved", wantReject: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/x", nil)
			if tt.header != "" {
				r.Header.Set("X-Mass-Source", tt.header)
			}
			w := httptest.NewRecorder()
			got := rejectAppSource(w, r)
			require.Equal(t, tt.wantReject, got)
			if tt.wantReject {
				require.Equal(t, http.StatusForbidden, w.Code)
				require.Contains(t, w.Body.String(), "not available to app callers")
			}
		})
	}
}

// TestSetDeviceEnabledRejectsAppSource locks in the only destructive RPC
// guarded by rejectAppSource today. If we add more guarded RPCs later,
// extend this into a table.
func TestSetDeviceEnabledRejectsAppSource(t *testing.T) {
	h := &Handler{} // reject path returns before any field is touched
	r := httptest.NewRequest(http.MethodPost, "/x",
		strings.NewReader(`{"queue_name":"q","enabled":true}`))
	r.Header.Set("X-Mass-Source", "app:malicious")
	w := httptest.NewRecorder()
	h.handleRPCSetDeviceEnabled(w, r)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "not available to app callers")
}

// ---------------------------------------------------------------------------
// chatConfigToProto / embeddingConfigToProto — used by ListLoadedModels to
// hand callers the exact identity+placement they should echo back to address
// the loaded instance instead of triggering a second load.
// ---------------------------------------------------------------------------

func TestChatConfigToProto(t *testing.T) {
	c := config.LlamaChatConfig{
		Path:         "/models/foo.gguf",
		ContextSize:  4096,
		BatchSize:    512,
		FlashAttn:    "enabled",
		Thinking:     true,
		MmprojPath:   "/models/foo-mmproj.gguf",
		ChatTemplate: "vicuna",
		CacheType:    "q8_0",
	}
	p := config.PlacementConfig{
		GpuLayers:     33,
		Threads:       8,
		MaxConcurrent: 4,
		MainGPU:       "0",
		TensorSplit:   "0.5,0.5",
	}
	got := chatConfigToProto(c, p)

	require.Equal(t, c.Path, got.Model)
	require.Equal(t, c.ContextSize, got.GetContextSize())
	require.Equal(t, c.BatchSize, got.GetBatchSize())
	require.NotNil(t, got.FlashAttn, "enabled -> non-nil")
	require.True(t, got.GetFlashAttn(), "enabled -> true")
	require.Equal(t, c.Thinking, got.Thinking)
	require.Equal(t, c.MmprojPath, got.Mmproj)
	require.Equal(t, c.ChatTemplate, got.ChatTemplate)
	require.Equal(t, rpc.CacheType_CACHE_TYPE_Q8_0, got.CacheType)
	require.Equal(t, p.GpuLayers, got.GetGpuLayers())
	require.Equal(t, p.Threads, got.GetThreads())
	require.Equal(t, p.MaxConcurrent, got.GetMaxConcurrent())
	require.Equal(t, p.MainGPU, got.MainGpu)
	require.Equal(t, []float32{0.5, 0.5}, got.TensorSplit)
}

func TestEmbeddingConfigToProto(t *testing.T) {
	c := config.LlamaEmbeddingConfig{
		Path:        "/models/embed.gguf",
		ContextSize: 512,
	}
	p := config.PlacementConfig{
		GpuLayers:     16,
		Threads:       4,
		MaxConcurrent: 2,
		MainGPU:       "0",
		TensorSplit:   "",
	}
	got := embeddingConfigToProto(c, p)
	require.Equal(t, c.Path, got.Model)
	require.Equal(t, c.ContextSize, got.GetContextSize())
	require.Equal(t, p.GpuLayers, got.GetGpuLayers())
	require.Equal(t, p.Threads, got.GetThreads())
	require.Equal(t, p.MaxConcurrent, got.GetMaxConcurrent())
	require.Equal(t, p.MainGPU, got.MainGpu)
	require.Empty(t, got.TensorSplit)
}

// TestRPCLoadModelValidation locks in the wire-level "config oneof is
// required" check. Happy paths are covered by scheduler-level tests.
func TestRPCLoadModelValidation(t *testing.T) {
	h := &Handler{cfg: config.Default(), logger: log.Logger.Level(zerolog.Disabled)}
	body, err := json.Marshal(map[string]any{})
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.handleRPCLoadModel(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "config oneof is required")
}
