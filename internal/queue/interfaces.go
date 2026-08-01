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

// PeekRow is one row returned by [QueueInterface.PeekAll]. Carries the
// message data plus a Leased flag — true when the row's lease is currently
// active (dispatched to a consumer; hidden from [QueueInterface.Receive]).
// Used by the operator UI to surface in-flight rows as read-only.
type PeekRow struct {
	Message
	Leased bool
}

// QueueInterface abstracts the durable job queue.
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
	// Submit enqueues a fully-built envelope. The envelope's RequestID and
	// GlobalMsgID are typically set by the caller; otherwise re-queue
	// tracking falls back to the goqite message ID.
	Submit(ctx context.Context, env Envelope) (SubmitResult, error)
	// Receive retrieves the next message from the queue.
	// Returns nil, nil if the queue is empty.
	Receive(ctx context.Context) (*Message, error)
	// Delete removes a processed message from the queue.
	Delete(ctx context.Context, id MessageID) error
	// UpdateBody rewrites the message body in place. Used by re-estimation
	// flows that refresh per-envelope fields without changing queue order
	// or lease state. Returns nil when the row is missing.
	UpdateBody(ctx context.Context, id MessageID, body []byte) error
	// Extend extends the processing timeout for a message.
	Extend(ctx context.Context, id MessageID, d time.Duration) error

	// Peek reads queued messages without consuming them, ordered by priority DESC.
	// Used for work stealing decisions and affinity inspection.
	Peek(ctx context.Context, limit int) ([]*Message, error)
	// PeekAll reads up to limit rows including leased ones. Each row carries
	// a Leased flag so operator views can distinguish pending from in-flight.
	// Same ordering as Peek (priority DESC, created ASC).
	PeekAll(ctx context.Context, limit int) ([]*PeekRow, error)
	// LeaseByID claims a message by ID without removing it: bumps timeout
	// to now+leaseDur and increments delivery count. Used by the dispatcher
	// so the row stays available for Extend/Delete/ReleaseLeaseAndDelete.
	// Returns nil, nil if missing, already leased, or past budget.
	LeaseByID(ctx context.Context, id MessageID, leaseDur time.Duration) (*Message, error)
	// LeaseAndSubmit atomically leases leaseID on this queue and submits env
	// to dst in one transaction. Returns leased=false (with no error) when
	// the source row is missing, already leased, or past its budget — the
	// caller should treat that as a race-loser. Both queues must be backed
	// by the same database. The destination row's priority is taken from
	// env.Priority.
	LeaseAndSubmit(ctx context.Context, leaseID MessageID, leaseDur time.Duration, dst QueueInterface, env Envelope) (result SubmitResult, leased bool, err error)
	// Depth returns the number of pending (unconsumed) messages.
	Depth(ctx context.Context) (int, error)
	// ListAbandoned returns messages past their delivery budget with
	// expired leases (no consumer processing). Used at startup to recover
	// dangling rows after a crash; caller writes a failure result and
	// Deletes each. Implementation must scope to this queue only.
	ListAbandoned(ctx context.Context) ([]*Message, error)

	// Name returns the queue's identifier — "global" or "worker|<id>" in
	// MASS today. Used by error-wrapping in operator-facing flows.
	Name() string

	// MoveTo atomically consumes msgID and submits its envelope to dst.
	// Used by work stealing. Atomic: on nil error, the row is on dst and
	// gone from here, or both queues are unchanged. Only pending rows
	// move — a leased or past-budget row returns (false, nil), same as
	// race-loss to another consumer/stealer. Both queues must share a
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

	// DeleteBoth atomically deletes msgID here and otherMsgID in other.
	// Used on terminal frame: the worker queue row and the global queue row
	// that anchors its durability go away together. Both queues must share
	// a database — mismatch panics (wiring bug).
	DeleteBoth(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error

	// DeleteAndReleaseLease atomically deletes msgID here and releases the
	// lease on otherMsgID in other so drainGlobal sees it again. Used by
	// worker disconnect handling: the worker queue row is gone, the global
	// anchor returns to the placement pool.
	DeleteAndReleaseLease(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error

	// ReleaseLease moves msgID's timeout into the past so the row is
	// immediately visible to consumers again. Used when a placement decision
	// needs to be re-scored after the original handoff was made (e.g. the
	// chosen worker dropped before dispatch).
	ReleaseLease(ctx context.Context, msgID MessageID) error

	// Defer hides msgID for delay, then makes it visible to consumers
	// again without charging a delivery. Used to bounce a row the consumer
	// can't take right now — the delay is what keeps a permanently-blocked
	// row from spinning the lease-bounce cycle. Signals nothing: waking the
	// consumer after delay is the caller's job.
	Defer(ctx context.Context, msgID MessageID, delay time.Duration) error
}

// ResultStoreInterface abstracts the result store for queued requests.
// SQL-backed only — see [QueueInterface] for rationale.
type ResultStoreInterface interface {
	// Create inserts a new pending result entry.
	Create(id string) error
	// Processing marks a pending result as processing (job dispatched to a
	// worker). No-op when the row is no longer pending.
	Processing(id string) error
	// Pending reverts a processing result back to pending (job lost its
	// worker, awaiting redistribution). No-op when not processing.
	Pending(id string) error
	// Complete stores the response body and marks the result as done.
	Complete(id string, body []byte) error
	// Fail marks a result as failed with an error message.
	Fail(id string, errMsg string) error
	// Get retrieves a result by ID. Returns nil, nil if not found.
	Get(id string) (*Result, error)
	// Cleanup removes terminal (done/error) results completed longer than
	// ttl ago. Live rows are never TTL-pruned.
	Cleanup(ttl time.Duration) (int64, error)
}

// Compile-time checks that concrete types satisfy the interfaces.
var (
	_ QueueInterface       = (*Queue)(nil)
	_ ResultStoreInterface = (*ResultStore)(nil)
)
