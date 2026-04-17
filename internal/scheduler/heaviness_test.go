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

func TestEnvelopeDifficulty(t *testing.T) {
	const m = 4_000_000_000 // 4 GB model
	const p = 1000          // 1 KB payload total

	tests := []struct {
		name           string
		payloadLen     int
		modelSize      uint64
		batchSize      int
		parallelSlots  int
		embeddingBatch bool
		want           float64
	}{
		{"unknown model size", p, 0, 1, 1, false, 0},
		{"empty payload", 0, m, 1, 1, false, 0},
		{"single request", p, m, 1, 1, false, float64(m) * float64(p)},
		// Chat batch of 4 with 2 parallel slots: wall-clock = 2 rounds.
		{"chat batch slots-bound", p, m, 4, 2, false, float64(m) * float64(p) / 2},
		// Chat batch of 4 with 8 parallel slots: rounds bounded by batch size.
		{"chat batch fits in slots", p, m, 4, 8, false, float64(m) * float64(p) / 4},
		// Cold dispatch: parallelSlots = 1 → batch prices as serial.
		{"chat batch cold dispatch", p, m, 4, 1, false, float64(m) * float64(p)},
		// Embedding batch: one forward pass, slots irrelevant.
		{"embedding batch ignores slots", p, m, 4, 1, true, float64(m) * float64(p) / 4},
		{"embedding batch slots high", p, m, 4, 8, true, float64(m) * float64(p) / 4},
		// Defensive clamps.
		{"zero batch coerces to 1", p, m, 0, 1, false, float64(m) * float64(p)},
		{"zero slots coerces to 1", p, m, 4, 0, false, float64(m) * float64(p)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := envelopeDifficulty(tt.payloadLen, tt.modelSize, tt.batchSize, tt.parallelSlots, tt.embeddingBatch)
			require.Equal(t, tt.want, got)
		})
	}
}
