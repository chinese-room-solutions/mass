package scheduler

import (
	"fmt"
	"os"
)

// ModelFileSize returns the file size in bytes for the given model path.
func ModelFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat model file %s: %w", path, err)
	}
	return info.Size(), nil
}

// Heaviness computes a relative cost score for scheduling.
// M = model size in bytes, I = input size in bytes.
// Higher values indicate heavier requests that take longer.
func Heaviness(modelSizeBytes int64, inputSizeBytes int) float64 {
	return float64(modelSizeBytes) * float64(inputSizeBytes)
}

// EstimateVRAM estimates the VRAM needed to load a model in MB.
// Uses model file size plus a context size overhead factor.
// This is an approximation — actual usage depends on quantization, KV cache, etc.
func EstimateVRAM(modelSizeBytes int64, contextSize int32, cacheType string) int64 {
	modelMB := modelSizeBytes / (1024 * 1024)
	return modelMB + EstimateKVCacheMB(modelSizeBytes, contextSize, cacheType)
}

// EstimateKVCacheMB estimates the KV cache memory per concurrent slot in MB.
// Scales with model size, context size, and KV cache quantization type.
//
// Baseline: ~128 MB per 2K context for a 4 GB model file at q8_0 (default).
// Scaling: linear with model file size (proxy for parameter count × quantization).
func EstimateKVCacheMB(modelSizeBytes int64, contextSize int32, cacheType string) int64 {
	if contextSize <= 0 || modelSizeBytes <= 0 {
		return 0
	}
	// Base: 128 MB per 2K context for a 4 GB model file at q8_0.
	const baseMBPer2K = 128
	const baseModelBytes = 4 * 1024 * 1024 * 1024 // 4 GB

	modelScale := float64(modelSizeBytes) / float64(baseModelBytes)
	contextBlocks := float64(contextSize) / 2048.0
	return int64(modelScale * contextBlocks * baseMBPer2K * kvCacheTypeScale(cacheType))
}

// kvCacheTypeScale returns the memory multiplier for a KV cache type
// relative to the q8_0 baseline.
func kvCacheTypeScale(cacheType string) float64 {
	switch cacheType {
	case "f16":
		return 2.0 // 16-bit: 2x of q8_0
	case "q4_0", "q4_1":
		return 0.5 // 4-bit: half of q8_0
	default:
		return 1.0 // q8_0 or empty (default)
	}
}
