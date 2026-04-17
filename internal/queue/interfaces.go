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

// QueueInterface abstracts the inference request queue.
//
// **Implementations must be SQL-backed.** The contract assumes:
//   - Atomic cross-queue operations ([QueueInterface.MoveTo],
//     [QueueInterface.ReleaseLeaseAndDelete]) — one transaction.
//   - Release preserves the original `created` timestamp so a released
//     message returns to its FIFO spot, not the head or tail.
//   - Real-time [QueueInterface.ListAbandoned] (no eventual consistency).
//   - Priority ordering (higher = processed first).
//
// Non-SQL backends (RabbitMQ, NATS, SQS, Kafka) can't satisfy this — they
// have fundamentally different cross-queue atomicity and abandonment
// semantics. If ever needed, a separate looser interface, not this one.
//
// Today: SQLite (goqite). Postgres on the roadmap.
type QueueInterface interface {
	// SubmitRaw enqueues a pre-serialized request payload. modelSizeBytes is
	// the size of the resolved model file (0 if unknown), used by the
	// scheduler for placement scoring without re-parsing the proto.
	SubmitRaw(ctx context.Context, reqType RequestType, payload []byte, source, fingerprint string, modelSizeBytes uint64, priority Priority) (SubmitResult, error)
	// SubmitEnvelope enqueues a complete envelope (preserves all fields including RequestID).
	SubmitEnvelope(ctx context.Context, env Envelope, priority Priority) (SubmitResult, error)
	// Receive retrieves the next message from the queue.
	// Returns nil, nil if the queue is empty.
	Receive(ctx context.Context) (*Message, error)
	// Delete removes a processed message from the queue.
	Delete(ctx context.Context, id MessageID) error
	// Extend extends the processing timeout for a message.
	Extend(ctx context.Context, id MessageID, d time.Duration) error

	// Peek reads queued messages without consuming them, ordered by priority DESC.
	// Used for work stealing decisions and fingerprint inspection.
	Peek(ctx context.Context, limit int) ([]*Message, error)
	// ReceiveByID consumes a specific message by ID.
	// Used for work stealing. Returns nil, nil if already consumed.
	ReceiveByID(ctx context.Context, id MessageID) (*Message, error)
	// LeaseByID claims a message by ID without removing it: bumps timeout
	// to now+leaseDur and increments delivery count. Used by the dispatcher
	// so the row stays available for Extend/Delete/ReleaseLeaseAndDelete.
	// Returns nil, nil if missing, already leased, or past budget.
	LeaseByID(ctx context.Context, id MessageID, leaseDur time.Duration) (*Message, error)
	// LeaseAndSubmit atomically leases leaseID on this queue and submits env
	// to dst in one transaction. Returns leased=false (with no error) when
	// the source row is missing, already leased, or past its budget — the
	// caller should treat that as a race-loser. Both queues must be backed
	// by the same database.
	LeaseAndSubmit(ctx context.Context, leaseID MessageID, leaseDur time.Duration, dst QueueInterface, env Envelope, priority Priority) (result SubmitResult, leased bool, err error)
	// Requeue returns a message to the queue, preserving original priority.
	// Used on failure or fingerprint mismatch during batch pull.
	Requeue(ctx context.Context, msg *Message, priority Priority) error
	// Depth returns the number of pending (unconsumed) messages.
	Depth(ctx context.Context) (int, error)
	// ListAbandoned returns messages past their delivery budget with
	// expired leases (no consumer processing). Used at startup to recover
	// dangling rows after a crash; caller writes a failure result and
	// Deletes each. Implementation must scope to this queue only.
	ListAbandoned(ctx context.Context) ([]*Message, error)

	// NotifyCh returns a channel that is signalled when a new message is
	// submitted to this queue. The channel has capacity 1 — multiple submits
	// while no consumer is reading collapse into a single wake-up. Consumers
	// use this to wake up when there is work to do without polling.
	NotifyCh() <-chan struct{}

	// MoveTo atomically consumes msgID and submits its envelope to dst.
	// Used by work stealing. Atomic: on nil error, the row is on dst and
	// gone from here, or both queues are unchanged. Returns (false, nil)
	// on race-loss to another consumer/stealer. Both queues must share a
	// database — mismatch panics (wiring bug).
	MoveTo(ctx context.Context, dst QueueInterface, msgID MessageID, priority Priority) (bool, error)

	// ReleaseLeaseAndDelete atomically releases the lease on otherMsgID in
	// other (consumers see it immediately) and deletes msgID here. Used by
	// device-queue drain: device row dropped, global counterpart handed
	// back, in one step — no in-neither-queue window, no fresh `created`
	// timestamp to send the task to the back.
	//
	// Both queues must share a database — mismatch panics (wiring bug).
	ReleaseLeaseAndDelete(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error
}

// ResultStoreInterface abstracts the result store for queued requests.
// SQL-backed only — see [QueueInterface] for rationale. The polling model
// in WaitForResult fits SQL backends; pub/sub backends would need a
// different interface.
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
