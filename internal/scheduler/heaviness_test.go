package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeaviness(t *testing.T) {
	// Larger model + larger input = higher heaviness.
	h1 := Heaviness(2_500_000_000, 1000)  // 2.5 GB model, 1KB input
	h2 := Heaviness(2_500_000_000, 10000) // 2.5 GB model, 10KB input
	h3 := Heaviness(500_000_000, 10000)   // 0.5 GB model, 10KB input

	require.Greater(t, h2, h1, "larger input = higher heaviness")
	require.Greater(t, h2, h3, "larger model = higher heaviness")
}

func TestEstimateKVCacheMB(t *testing.T) {
	const gb4 = 4 * 1024 * 1024 * 1024 // 4 GB model (baseline)
	const gb2 = 2 * 1024 * 1024 * 1024 // 2 GB model (half baseline)

	tests := []struct {
		name       string
		modelBytes int64
		ctxSize    int32
		cacheType  string
		wantMB     int64
	}{
		{"zero context", gb4, 0, "", 0},
		{"zero model", 0, 4096, "", 0},
		{"4GB model 2K q8_0", gb4, 2048, "", 128},
		{"4GB model 4K q8_0", gb4, 4096, "", 256},
		{"2GB model 4K q8_0", gb2, 4096, "", 128},          // half model = half KV cache
		{"4GB model 4K f16", gb4, 4096, "f16", 512},        // 2x of q8_0
		{"4GB model 4K q4_0", gb4, 4096, "q4_0", 128},      // 0.5x of q8_0
		{"4GB model 30K q8_0", gb4, 30000, "", 1875},       // 30000/2048 * 128 ≈ 1875
		{"4GB model 30K q4_0", gb4, 30000, "q4_0", 937},    // half of q8_0
		{"2.5GB model 30K q8_0", 2500000000, 30000, "", 0}, // verify it's reasonable
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateKVCacheMB(tt.modelBytes, tt.ctxSize, tt.cacheType)
			if tt.name == "2.5GB model 30K q8_0" {
				// Just check it's reasonable: should be less than total model size
				require.Greater(t, got, int64(0))
				require.Less(t, got, int64(2500), "KV cache for small model should be reasonable")
			} else {
				require.Equal(t, tt.wantMB, got)
			}
		})
	}
}

func TestEstimateVRAM(t *testing.T) {
	tests := []struct {
		name        string
		modelBytes  int64
		contextSize int32
		wantMinMB   int64
	}{
		{"small model no context", 500_000_000, 0, 476},
		{"2.5GB model 4K context", 2_500_000_000, 4096, 2384},
		{"8GB model 8K context", 8_000_000_000, 8192, 7629},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateVRAM(tt.modelBytes, tt.contextSize, "")
			require.GreaterOrEqual(t, got, tt.wantMinMB)
		})
	}
}

func TestKvCacheTypeScale(t *testing.T) {
	tests := []struct {
		cacheType string
		want      float64
	}{
		{"", 1.0},
		{"q8_0", 1.0},
		{"f16", 2.0},
		{"q4_0", 0.5},
		{"q4_1", 0.5},
		{"unknown", 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.cacheType, func(t *testing.T) {
			require.Equal(t, tt.want, kvCacheTypeScale(tt.cacheType))
		})
	}
}
