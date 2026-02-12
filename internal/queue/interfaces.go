package queue

import (
	"context"
	"time"
)

// MessageID is an opaque identifier for a queued message.
type MessageID string

// Message represents a message received from the queue.
type Message struct {
	ID   MessageID
	Body []byte
}

// QueueInterface abstracts the message queue for inference requests.
// Implementations must support priority ordering (higher = processed first).
type QueueInterface interface {
	// SubmitRaw enqueues a pre-serialized request payload.
	SubmitRaw(ctx context.Context, reqType RequestType, payload []byte, source, fingerprint string, priority Priority) (SubmitResult, error)
	// SubmitEnvelope enqueues a complete envelope (preserves all fields including RequestID).
	SubmitEnvelope(ctx context.Context, env Envelope, priority Priority) (SubmitResult, error)
	// Receive retrieves the next message from the queue.
	// Returns nil, nil if the queue is empty.
	Receive(ctx context.Context) (*Message, error)
	// Delete removes a processed message from the queue.
	Delete(ctx context.Context, id MessageID) error
	// Extend extends the processing timeout for a message.
	Extend(ctx context.Context, id MessageID, d time.Duration) error

	// ReceiveBatch retrieves up to limit messages without blocking.
	// Used by device queue processors for concurrent execution.
	ReceiveBatch(ctx context.Context, limit int) ([]*Message, error)
	// Peek reads queued messages without consuming them, ordered by priority DESC.
	// Used for work stealing decisions and fingerprint inspection.
	Peek(ctx context.Context, limit int) ([]*Message, error)
	// ReceiveByID consumes a specific message by ID.
	// Used for work stealing. Returns nil, nil if already consumed.
	ReceiveByID(ctx context.Context, id MessageID) (*Message, error)
	// Requeue returns a message to the queue, preserving original priority.
	// Used on failure or fingerprint mismatch during batch pull.
	Requeue(ctx context.Context, msg *Message, priority Priority) error
	// Depth returns the number of pending (unconsumed) messages.
	Depth(ctx context.Context) (int, error)
}

// ResultStoreInterface abstracts the result store for queued requests.
type ResultStoreInterface interface {
	// Create inserts a new pending result entry.
	Create(id, requestHash string) error
	// MarkProcessing transitions a result to processing status.
	MarkProcessing(id string) error
	// Complete stores the response body and marks the result as done.
	Complete(id string, body []byte) error
	// Fail marks a result as failed with an error message.
	Fail(id string, errMsg string) error
	// Get retrieves a result by ID. Returns nil, nil if not found.
	Get(id string) (*Result, error)
	// FindByHash looks up a cached result by request hash within the TTL.
	FindByHash(requestHash string, ttl time.Duration) (*Result, error)
	// Cleanup removes results older than the given TTL.
	Cleanup(ttl time.Duration) (int64, error)
	// WaitForResult polls until the result is completed or done is closed.
	WaitForResult(id string, pollInterval time.Duration, done <-chan struct{}) (*Result, error)
}

// Compile-time checks that concrete types satisfy the interfaces.
var (
	_ QueueInterface       = (*Queue)(nil)
	_ ResultStoreInterface = (*ResultStore)(nil)
)
