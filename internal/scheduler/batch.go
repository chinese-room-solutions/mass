package scheduler

import (
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"google.golang.org/protobuf/proto"
)

// batchSize returns the number of inference items inside an envelope's
// payload. Single-request types return 1; batch types return the count of
// inner requests/inputs. Malformed payloads conservatively return 1 — the
// dispatcher then prices the envelope as a single task, which is wrong by
// at most the parallelism factor and never under-counts wait time.
func batchSize(reqType queue.RequestType, payload []byte) int {
	switch reqType {
	case queue.RequestTypeBatchChatCompletion:
		req := &rpc.BatchChatCompletionRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return 1
		}
		if n := len(req.Items); n > 0 {
			return n
		}
		return 1
	case queue.RequestTypeBatchEmbedding:
		req := &rpc.BatchEmbeddingRequest{}
		if err := proto.Unmarshal(payload, req); err != nil {
			return 1
		}
		if n := len(req.Inputs); n > 0 {
			return n
		}
		return 1
	default:
		return 1
	}
}

// isEmbeddingBatch reports whether the request type is a batch the worker
// executes as one llama_decode call (true multi-sequence batching) rather
// than parallel fan-out.
func isEmbeddingBatch(reqType queue.RequestType) bool {
	return reqType == queue.RequestTypeBatchEmbedding
}
