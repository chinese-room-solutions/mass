package agent

import (
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/rpc"
	"github.com/stretchr/testify/require"
)

func TestAdaptChatConfigForPlacement(t *testing.T) {
	tests := []struct {
		name          string
		cfg           llm.ChatModelConfig
		placement     llm.PlacementConfig
		wantFlashAttn string
		wantCacheType string
	}{
		{
			name:          "GPU placement keeps config",
			cfg:           llm.ChatModelConfig{FlashAttn: "enabled", CacheType: "q8_0"},
			placement:     llm.PlacementConfig{GpuLayers: -1},
			wantFlashAttn: "enabled",
			wantCacheType: "q8_0",
		},
		{
			name:          "CPU placement clears quantized cache and flash attn",
			cfg:           llm.ChatModelConfig{FlashAttn: "enabled", CacheType: "q8_0"},
			placement:     llm.PlacementConfig{GpuLayers: 0},
			wantFlashAttn: "",
			wantCacheType: "",
		},
		{
			name:          "CPU placement keeps f16 cache type",
			cfg:           llm.ChatModelConfig{FlashAttn: "enabled", CacheType: "f16"},
			placement:     llm.PlacementConfig{GpuLayers: 0},
			wantFlashAttn: "",
			wantCacheType: "f16",
		},
		{
			name:          "partial GPU keeps config",
			cfg:           llm.ChatModelConfig{FlashAttn: "enabled", CacheType: "q4_0"},
			placement:     llm.PlacementConfig{GpuLayers: 20},
			wantFlashAttn: "enabled",
			wantCacheType: "q4_0",
		},
		{
			name:          "CPU placement with empty config stays empty",
			cfg:           llm.ChatModelConfig{},
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

func TestChatConfigToProto(t *testing.T) {
	tests := []struct {
		name      string
		cfg       llm.ChatModelConfig
		placement llm.PlacementConfig
		want      *rpc.LlamaChatConfig
	}{
		{
			name: "zero value",
			cfg:  llm.ChatModelConfig{},
			want: &rpc.LlamaChatConfig{},
		},
		{
			name: "fully populated",
			cfg: llm.ChatModelConfig{
				Path:         "/models/llama-7b.gguf",
				ContextSize:  4096,
				BatchSize:    512,
				MaxTokens:    2048,
				FlashAttn:    "true",
				Thinking:     true,
				MmprojPath:   "/models/mmproj.bin",
				ChatTemplate: "chatml",
			},
			placement: llm.PlacementConfig{
				Threads:       8,
				MaxConcurrent: 4,
				GpuLayers:     35,
				MainGPU:       "0",
				TensorSplit:   "50,50",
			},
			want: &rpc.LlamaChatConfig{
				Model:         "/models/llama-7b.gguf",
				ContextSize:   4096,
				BatchSize:     512,
				Threads:       8,
				MaxConcurrent: 4,
				FlashAttn:     "true",
				GpuLayers:     35,
				MainGpu:       "0",
				TensorSplit:   "50,50",
				Thinking:      true,
				Mmproj:        "/models/mmproj.bin",
				ChatTemplate:  "chatml",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chatConfigToProto(tt.cfg, tt.placement)
			require.Equal(t, tt.want.Model, got.Model, "Path -> Model")
			require.Equal(t, tt.want.ContextSize, got.ContextSize)
			require.Equal(t, tt.want.BatchSize, got.BatchSize)
			require.Equal(t, tt.want.Threads, got.Threads)
			require.Equal(t, tt.want.MaxConcurrent, got.MaxConcurrent)
			require.Equal(t, tt.want.FlashAttn, got.FlashAttn)
			require.Equal(t, tt.want.GpuLayers, got.GpuLayers)
			require.Equal(t, tt.want.MainGpu, got.MainGpu, "MainGPU -> MainGpu")
			require.Equal(t, tt.want.TensorSplit, got.TensorSplit)
			require.Equal(t, tt.want.Thinking, got.Thinking)
			require.Equal(t, tt.want.Mmproj, got.Mmproj, "MmprojPath -> Mmproj")
			require.Equal(t, tt.want.ChatTemplate, got.ChatTemplate)
		})
	}
}

func TestEmbeddingConfigToProto(t *testing.T) {
	tests := []struct {
		name      string
		cfg       llm.EmbeddingModelConfig
		placement llm.PlacementConfig
		want      *rpc.LlamaEmbeddingConfig
	}{
		{
			name: "zero value",
			cfg:  llm.EmbeddingModelConfig{},
			want: &rpc.LlamaEmbeddingConfig{},
		},
		{
			name: "fully populated",
			cfg: llm.EmbeddingModelConfig{
				Path:        "/models/embed.gguf",
				ContextSize: 2048,
			},
			placement: llm.PlacementConfig{
				Threads:       4,
				MaxConcurrent: 2,
				GpuLayers:     20,
				MainGPU:       "1",
				TensorSplit:   "70,30",
			},
			want: &rpc.LlamaEmbeddingConfig{
				Model:         "/models/embed.gguf",
				ContextSize:   2048,
				Threads:       4,
				MaxConcurrent: 2,
				GpuLayers:     20,
				MainGpu:       "1",
				TensorSplit:   "70,30",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := embeddingConfigToProto(tt.cfg, tt.placement)
			require.Equal(t, tt.want.Model, got.Model, "Path -> Model")
			require.Equal(t, tt.want.ContextSize, got.ContextSize)
			require.Equal(t, tt.want.Threads, got.Threads)
			require.Equal(t, tt.want.MaxConcurrent, got.MaxConcurrent)
			require.Equal(t, tt.want.GpuLayers, got.GpuLayers)
			require.Equal(t, tt.want.MainGpu, got.MainGpu, "MainGPU -> MainGpu")
			require.Equal(t, tt.want.TensorSplit, got.TensorSplit)
		})
	}
}

func TestCompletionRequestToProto(t *testing.T) {
	tests := []struct {
		name string
		req  llm.CompletionRequest
		want *rpc.ChatCompletionRequest
	}{
		{
			name: "zero/empty request",
			req:  llm.CompletionRequest{},
			want: &rpc.ChatCompletionRequest{
				Messages: []*rpc.ChatMessage{},
			},
		},
		{
			name: "messages without parts",
			req: llm.CompletionRequest{
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
			},
			want: &rpc.ChatCompletionRequest{
				Messages: []*rpc.ChatMessage{
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
			},
		},
		{
			name: "messages with multipart content",
			req: llm.CompletionRequest{
				Messages: []llm.ChatMessage{
					{
						Role:    "user",
						Content: "Describe this image",
						Parts: []llm.ContentPart{
							{Type: llm.ContentText, Text: "Describe this image"},
							{Type: llm.ContentImage, Data: []byte{0xFF, 0xD8, 0xFF}, Filename: "photo.jpg"},
							{Type: llm.ContentAudio, Data: []byte{0x52, 0x49, 0x46, 0x46}, Filename: "clip.wav"},
							{Type: llm.ContentFile, Data: []byte{0x25, 0x50, 0x44, 0x46}, Filename: "doc.pdf"},
						},
					},
				},
				MaxTokens:   256,
				Temperature: 0.0,
			},
			want: &rpc.ChatCompletionRequest{
				Messages: []*rpc.ChatMessage{
					{
						Role:    "user",
						Content: "Describe this image",
						Parts: []*rpc.ContentPart{
							{Type: "text", Text: "Describe this image"},
							{Type: "image", Data: []byte{0xFF, 0xD8, 0xFF}, Filename: "photo.jpg"},
							{Type: "audio", Data: []byte{0x52, 0x49, 0x46, 0x46}, Filename: "clip.wav"},
							{Type: "file", Data: []byte{0x25, 0x50, 0x44, 0x46}, Filename: "doc.pdf"},
						},
					},
				},
				MaxTokens:   256,
				Temperature: 0.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := completionRequestToProto(tt.req)

			// Verify message count.
			require.Len(t, got.Messages, len(tt.want.Messages))

			// Verify each message.
			for i, wantMsg := range tt.want.Messages {
				gotMsg := got.Messages[i]
				require.Equal(t, wantMsg.Role, gotMsg.Role, "message[%d].Role", i)
				require.Equal(t, wantMsg.Content, gotMsg.Content, "message[%d].Content", i)

				// Verify parts.
				require.Len(t, gotMsg.Parts, len(wantMsg.Parts), "message[%d].Parts length", i)
				for j, wantPart := range wantMsg.Parts {
					gotPart := gotMsg.Parts[j]
					require.Equal(t, wantPart.Type, gotPart.Type, "message[%d].Parts[%d].Type", i, j)
					require.Equal(t, wantPart.Text, gotPart.Text, "message[%d].Parts[%d].Text", i, j)
					require.Equal(t, wantPart.Data, gotPart.Data, "message[%d].Parts[%d].Data", i, j)
					require.Equal(t, wantPart.Filename, gotPart.Filename, "message[%d].Parts[%d].Filename", i, j)
				}
			}

			// Verify numeric fields convert correctly (int -> int32).
			require.Equal(t, tt.want.MaxTokens, got.MaxTokens, "MaxTokens int->int32")
			require.Equal(t, tt.want.TopK, got.TopK, "TopK int->int32")
			require.Equal(t, tt.want.Seed, got.Seed, "Seed int->int32")

			// Verify float fields.
			require.InDelta(t, tt.want.Temperature, got.Temperature, 1e-6, "Temperature")
			require.InDelta(t, tt.want.TopP, got.TopP, 1e-6, "TopP")
			require.InDelta(t, tt.want.MinP, got.MinP, 1e-6, "MinP")
			require.InDelta(t, tt.want.RepeatPenalty, got.RepeatPenalty, 1e-6, "RepeatPenalty")
			require.InDelta(t, tt.want.FrequencyPenalty, got.FrequencyPenalty, 1e-6, "FrequencyPenalty")
			require.InDelta(t, tt.want.PresencePenalty, got.PresencePenalty, 1e-6, "PresencePenalty")

			// Verify Stop slice passes through.
			require.Equal(t, tt.want.Stop, got.Stop, "Stop slice")

			// Verify bool.
			require.Equal(t, tt.want.EnableThinking, got.EnableThinking, "EnableThinking")
		})
	}
}
