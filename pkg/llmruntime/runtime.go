// Package llmruntime provides concrete model loader and benchmarker
// implementations for llama.cpp. This package requires CGO.
//
// It re-exports constructors from internal/llm so that external binaries
// (e.g., mass-agent) can create loaders and benchers without importing
// internal packages directly.
package llmruntime

import (
	"github.com/chinese-room-solutions/mass/internal/llm"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	pkgllm "github.com/chinese-room-solutions/mass/pkg/llm"
)

// NewLlamaLoader creates a new LlamaLoader that loads GGUF models via llama.cpp.
func NewLlamaLoader() pkgllm.ModelLoaderInterface {
	return llm.NewLlamaLoader()
}

// NewBencher creates a new Bencher for CPU and CUDA GPU benchmarking.
func NewBencher() bench.BencherInterface {
	return &llm.Bencher{}
}
