package worker

import (
	"testing"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/stretchr/testify/require"
)

func TestAdaptChatConfigForPlacement(t *testing.T) {
	tests := []struct {
		name          string
		cfg           llm.LlamaChatConfig
		placement     llm.PlacementConfig
		wantFlashAttn string
		wantCacheType string
	}{
		{
			name:          "GPU placement keeps config",
			cfg:           llm.LlamaChatConfig{FlashAttn: "enabled", CacheType: "q8_0"},
			placement:     llm.PlacementConfig{GpuLayers: -1},
			wantFlashAttn: "enabled",
			wantCacheType: "q8_0",
		},
		{
			name:          "CPU placement clears quantized cache and flash attn",
			cfg:           llm.LlamaChatConfig{FlashAttn: "enabled", CacheType: "q8_0"},
			placement:     llm.PlacementConfig{GpuLayers: 0},
			wantFlashAttn: "",
			wantCacheType: "",
		},
		{
			name:          "CPU placement keeps f16 cache type",
			cfg:           llm.LlamaChatConfig{FlashAttn: "enabled", CacheType: "f16"},
			placement:     llm.PlacementConfig{GpuLayers: 0},
			wantFlashAttn: "",
			wantCacheType: "f16",
		},
		{
			name:          "partial GPU keeps config",
			cfg:           llm.LlamaChatConfig{FlashAttn: "enabled", CacheType: "q4_0"},
			placement:     llm.PlacementConfig{GpuLayers: 20},
			wantFlashAttn: "enabled",
			wantCacheType: "q4_0",
		},
		{
			name:          "CPU placement with empty config stays empty",
			cfg:           llm.LlamaChatConfig{},
			placement:     llm.PlacementConfig{GpuLayers: 0},
			wantFlashAttn: "",
			wantCacheType: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			adaptChatConfigForPlacement(&cfg, tt.placement)
			require.Equal(t, tt.wantFlashAttn, cfg.FlashAttn)
			require.Equal(t, tt.wantCacheType, cfg.CacheType)
		})
	}
}

// chatConfigToProto and embeddingConfigToProto serialize the wire-side fields
// only. Model/mmproj path fields stay empty by contract — those file references
// travel via [workerpb.HubLoadChatModel.Files] as fully-formed download URLs.

func TestChatConfigToProto(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		got := chatConfigToProto(llm.LlamaChatConfig{}, llm.PlacementConfig{})
		require.Equal(t, "", got.Model)
		require.Equal(t, int32(0), got.GetContextSize())
		require.Equal(t, int32(0), got.GetBatchSize())
		require.Equal(t, false, got.GetFlashAttn())
		require.Nil(t, got.FlashAttn, "FlashAttn unset for zero value")
		require.Equal(t, rpc.CacheType_CACHE_TYPE_UNSPECIFIED, got.CacheType)
		require.Empty(t, got.TensorSplit)
	})

	t.Run("fully populated", func(t *testing.T) {
		cfg := llm.LlamaChatConfig{
			ContextSize:  4096,
			BatchSize:    512,
			FlashAttn:    "enabled",
			Thinking:     true,
			ChatTemplate: "chatml",
			CacheType:    "q8_0",
		}
		placement := llm.PlacementConfig{
			Threads:       8,
			MaxConcurrent: 4,
			GpuLayers:     35,
			MainGPU:       "0",
			TensorSplit:   "0.5,0.5",
		}
		got := chatConfigToProto(cfg, placement)

		require.Equal(t, int32(4096), got.GetContextSize())
		require.Equal(t, int32(512), got.GetBatchSize())
		require.Equal(t, int32(8), got.GetThreads())
		require.Equal(t, int32(4), got.GetMaxConcurrent())
		require.NotNil(t, got.FlashAttn)
		require.True(t, got.GetFlashAttn(), "enabled -> true")
		require.Equal(t, int32(35), got.GetGpuLayers())
		require.Equal(t, "0", got.MainGpu)
		require.Equal(t, []float32{0.5, 0.5}, got.TensorSplit)
		require.True(t, got.Thinking)
		require.Equal(t, "chatml", got.ChatTemplate)
		require.Equal(t, rpc.CacheType_CACHE_TYPE_Q8_0, got.CacheType)
	})

	t.Run("flash_attn disabled round-trips as false", func(t *testing.T) {
		got := chatConfigToProto(llm.LlamaChatConfig{FlashAttn: "disabled"}, llm.PlacementConfig{})
		require.NotNil(t, got.FlashAttn)
		require.False(t, got.GetFlashAttn())
	})
}

func TestEmbeddingConfigToProto(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		got := embeddingConfigToProto(llm.LlamaEmbeddingConfig{}, llm.PlacementConfig{})
		require.Equal(t, "", got.Model)
		require.Equal(t, int32(0), got.GetContextSize())
	})

	t.Run("fully populated", func(t *testing.T) {
		cfg := llm.LlamaEmbeddingConfig{ContextSize: 2048}
		placement := llm.PlacementConfig{
			Threads:       4,
			MaxConcurrent: 2,
			GpuLayers:     20,
			MainGPU:       "1",
			TensorSplit:   "0.7,0.3",
		}
		got := embeddingConfigToProto(cfg, placement)
		require.Equal(t, int32(2048), got.GetContextSize())
		require.Equal(t, int32(4), got.GetThreads())
		require.Equal(t, int32(2), got.GetMaxConcurrent())
		require.Equal(t, int32(20), got.GetGpuLayers())
		require.Equal(t, "1", got.MainGpu)
		require.Equal(t, []float32{0.7, 0.3}, got.TensorSplit)
	})
}

func TestCompletionRequestToProto(t *testing.T) {
	t.Run("zero/empty request", func(t *testing.T) {
		got := completionRequestToProto(llm.CompletionRequest{})
		require.Empty(t, got.Messages)
		require.NotNil(t, got.Sampling)
	})

	t.Run("messages without parts", func(t *testing.T) {
		req := llm.CompletionRequest{
			Messages: []llm.ChatMessage{
				{Role: "system", Content: "You are helpful."},
				{Role: "user", Content: "Hello"},
			},
			MaxTokens:        512,
			Temperature:      0.7,
			TopP:             0.9,
			TopK:             40,
			Seed:             42,
			Stop:             []string{"\n", "END"},
			MinP:             0.05,
			RepeatPenalty:    1.1,
			FrequencyPenalty: 0.5,
			PresencePenalty:  0.3,
			EnableThinking:   true,
		}
		got := completionRequestToProto(req)

		require.Len(t, got.Messages, 2)
		require.Equal(t, rpc.Role_ROLE_SYSTEM, got.Messages[0].Role)
		require.Equal(t, "You are helpful.", got.Messages[0].Content)
		require.Equal(t, rpc.Role_ROLE_USER, got.Messages[1].Role)
		require.Equal(t, "Hello", got.Messages[1].Content)

		require.NotNil(t, got.Sampling)
		require.Equal(t, int32(512), got.Sampling.GetMaxTokens())
		require.InDelta(t, 0.7, got.Sampling.Temperature, 1e-6)
		require.InDelta(t, 0.9, got.Sampling.TopP, 1e-6)
		require.Equal(t, int32(40), got.Sampling.TopK)
		require.Equal(t, int32(42), got.Sampling.GetSeed())
		require.Equal(t, []string{"\n", "END"}, got.Sampling.Stop)
		require.InDelta(t, 0.05, got.Sampling.MinP, 1e-6)
		require.InDelta(t, 1.1, got.Sampling.RepeatPenalty, 1e-6)
		require.InDelta(t, 0.5, got.Sampling.FrequencyPenalty, 1e-6)
		require.InDelta(t, 0.3, got.Sampling.PresencePenalty, 1e-6)
		require.True(t, got.Sampling.EnableThinking)
	})

	t.Run("messages with multipart content", func(t *testing.T) {
		req := llm.CompletionRequest{
			Messages: []llm.ChatMessage{
				{
					Role:    "user",
					Content: "Describe this image",
					Parts: []llm.ContentPart{
						{Type: llm.ContentText, Text: "Describe this image"},
						{Type: llm.ContentImage, Data: []byte{0xFF, 0xD8, 0xFF}},
						{Type: llm.ContentAudio, Data: []byte{0x52, 0x49, 0x46, 0x46}},
						{Type: llm.ContentFile, Data: []byte{0x25, 0x50, 0x44, 0x46}, Filename: "doc.pdf"},
					},
				},
			},
		}
		got := completionRequestToProto(req)
		require.Len(t, got.Messages, 1)
		parts := got.Messages[0].Parts
		require.Len(t, parts, 4)

		require.Equal(t, "Describe this image", parts[0].GetText())
		require.NotNil(t, parts[1].GetImage())
		require.Equal(t, []byte{0xFF, 0xD8, 0xFF}, parts[1].GetImage().Data)
		require.NotNil(t, parts[2].GetAudio())
		require.Equal(t, []byte{0x52, 0x49, 0x46, 0x46}, parts[2].GetAudio().Data)
		require.NotNil(t, parts[3].GetFile())
		require.Equal(t, "doc.pdf", parts[3].GetFile().Filename)
		require.Equal(t, []byte{0x25, 0x50, 0x44, 0x46}, parts[3].GetFile().Data)
	})
}
