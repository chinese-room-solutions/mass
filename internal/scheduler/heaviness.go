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

// envelopeDifficulty returns the per-task difficulty for placement scoring
// and tail-sum maintenance. Centralized so dispatch, dequeue, and steal all
// compute the same value from the same envelope.
//
// Three regimes:
//   - Single request: M × payloadLen.
//   - Chat batch: parallel fan-out, wall-clock scales with rounds not
//     items → M × payloadLen / min(batchSize, parallelSlots).
//   - Embedding batch (llama.cpp multi-sequence): one forward pass over
//     all inputs → M × (payloadLen / batchSize).
//
// parallelSlots = model's max_concurrent (≥1) when known, else 1 for cold
// dispatch. Pricing cold batches as serial steers the dispatcher toward
// already-loaded devices — desirable, since loaded > cold for batches.
//
// Returns 0 when model size or payload is unknown (no signal contributed).
func envelopeDifficulty(payloadLen int, modelSizeBytes uint64, batchSize, parallelSlots int, embeddingBatch bool) float64 {
	if modelSizeBytes == 0 || payloadLen == 0 {
		return 0
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if parallelSlots < 1 {
		parallelSlots = 1
	}
	base := Heaviness(int64(modelSizeBytes), payloadLen)
	if batchSize == 1 {
		return base
	}
	if embeddingBatch {
		return base / float64(batchSize)
	}
	rounds := min(batchSize, parallelSlots)
	return base / float64(rounds)
}
