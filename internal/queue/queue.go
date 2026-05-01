// Package queue provides a durable, prioritized job queue backed by goqite
// (SQLite). Results are stored alongside for cache lookups and async retrieval.
//
// Job payloads are runtime-agnostic opaque bytes — encoded by the runtime
// gateway that submitted the job, decoded by the worker of the matching
// runtime kind. MASS treats them as bytes throughout.
//
// **Postgres readiness:** SQL-only by design (see [QueueInterface]).
// Dialect-specific work, when Postgres lands, is in two places:
//   - SQL `strftime` calls — easiest to replace by computing timestamps in
//     Go and passing as bound parameters.
//   - Paired migration files in `internal/store/migrations`.
//
// Interface and Pool shape don't need to change.
package queue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"maragu.dev/goqite"
)

// Priority controls queue ordering (higher = processed first).
type Priority int

const (
	PriorityLow      Priority = iota // background/batch jobs
	PriorityMedium                   // default for scheduled work
	PriorityHigh                     // elevated by user or gateway explicitly
	PriorityCritical                 // urgent/real-time, set explicitly
)

// Queue wraps goqite to provide typed job queueing.
type Queue struct {
	q      *goqite.Queue
	db     *sql.DB
	name   string
	notify chan struct{} // capacity-1 wake-up channel signalled on submit
}

// MaxReceive is the per-message attempt budget. Set to 1: one delivery,
// no in-queue retries. On failure (worker crash, lease expiry without
// Delete) the [Sweeper] writes an error result and deletes the row.
//
// Rationale: MASS uses the queue for scheduling/order, not fault tolerance.
// Retry policy belongs to the caller — they own deadlines, idempotency,
// and whether retry even makes sense. Silent in-queue retries would hide
// flaky workers and double up with caller-side retry.
//
// [Queue.Extend] does NOT consume the budget — only actual redeliveries
// (visibility timeout firing without Delete) do.
const MaxReceive = 1

// DefaultVisibilityTimeout is the standard processing-lease duration. A
// consumer that doesn't [Queue.Delete] or [Queue.Extend] within the window
// loses the message; at MaxReceive=1 the row goes to [Sweeper] for
// failure reporting.
const DefaultVisibilityTimeout = 30 * time.Second

// Pool owns one [*sql.DB] handle and constructs [*Queue] instances against
// it. Use Pool when multiple queues share one database — required for
// atomic cross-queue operations like [Queue.MoveTo]. Also the natural
// place for a dialect field when Postgres support lands.
type Pool struct {
	db *sql.DB
}

// NewPool creates a Pool backed by db.
func NewPool(db *sql.DB) *Pool {
	return &Pool{db: db}
}

// Open returns a [*Queue] named name, backed by the pool's database.
// Multiple queue names coexist in the same goqite table. Sharing one
// returned handle across goroutines is fine and recommended.
func (p *Pool) Open(name string) *Queue {
	return newQueue(p.db, name)
}

// New creates a Queue with the default "global" name. Prefer [Pool.Open]
// for new code — it makes the shared-DB invariant explicit.
func New(db *sql.DB) *Queue {
	return newQueue(db, "global")
}

// NewNamed creates a Queue with custom name, max receive, and timeout.
// Prefer [Pool.Open] for new code; NewNamed is for tests that need to
// override the retry budget or visibility timeout.
func NewNamed(db *sql.DB, name string, maxReceive int, timeout time.Duration) *Queue {
	q := goqite.New(goqite.NewOpts{
		DB:         db,
		Name:       name,
		MaxReceive: maxReceive,
		Timeout:    timeout,
	})
	return &Queue{q: q, db: db, name: name, notify: make(chan struct{}, 1)}
}

// newQueue is the canonical constructor used by Pool.Open and New. Always
// uses [MaxReceive] and [DefaultVisibilityTimeout] — the values production
// code actually wants.
func newQueue(db *sql.DB, name string) *Queue {
	q := goqite.New(goqite.NewOpts{
		DB:         db,
		Name:       name,
		MaxReceive: MaxReceive,
		Timeout:    DefaultVisibilityTimeout,
	})
	return &Queue{q: q, db: db, name: name, notify: make(chan struct{}, 1)}
}

// NotifyCh returns a channel signalled on each submit. Capacity 1 — multiple
// submits while nobody reads collapse into one wake-up.
func (q *Queue) NotifyCh() <-chan struct{} { return q.notify }

// signal performs a non-blocking send to the notify channel.
func (q *Queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Name returns the queue's name.
func (q *Queue) Name() string { return q.name }

// MaxRetries is the maximum number of times a task can be re-queued before being dropped.
const MaxRetries = 5

// Envelope wraps an opaque, gateway-encoded payload with scheduler metadata.
//
// Wire format (little-endian):
//
//	[1B priority][1B retries][8B difficulty]
//	[1B runtime_name_len][runtime_name]
//	[1B model_id_len][model_id]
//	[1B source_len][source]
//	[1B reqid_len][reqid]
//	[1B gmid_len][gmid]
//	[payload]
type Envelope struct {
	Priority    Priority // preserved across queue hops
	Retries     uint8    // incremented on each re-queue; dropped at MaxRetries
	Difficulty  uint64   // gateway-supplied weight hint; used by scheduler scoring
	RuntimeName string   // routes to workers of this kind (e.g. "llama-cpp")
	ModelID     string   // gateway-defined opaque ID; affinity key (which workers have it loaded)
	Source      string   // who submitted: "direct", "gateway:<runtime_name>", etc.
	RequestID   string   // original request ID for result tracking across queue hops
	GlobalMsgID string   // original global queue message ID for durability tracking
	Payload     []byte   // gateway-defined opaque job bytes (sent to worker as HubAssignJob.payload)
}

// envelopeHeaderBytes is the fixed-size prefix in the wire format: priority +
// retries + difficulty. Variable-length fields follow.
const envelopeHeaderBytes = 1 + 1 + 8

// Marshal serializes the envelope to bytes.
func (e Envelope) Marshal() []byte {
	rk := truncate255(e.RuntimeName)
	mid := truncate255(e.ModelID)
	src := truncate255(e.Source)
	rid := truncate255(e.RequestID)
	gmid := truncate255(e.GlobalMsgID)

	buf := make([]byte, envelopeHeaderBytes+5+len(rk)+len(mid)+len(src)+len(rid)+len(gmid)+len(e.Payload))
	buf[0] = byte(e.Priority)
	buf[1] = e.Retries
	binary.LittleEndian.PutUint64(buf[2:10], e.Difficulty)
	off := envelopeHeaderBytes

	off = writeLenPrefixed(buf, off, rk)
	off = writeLenPrefixed(buf, off, mid)
	off = writeLenPrefixed(buf, off, src)
	off = writeLenPrefixed(buf, off, rid)
	off = writeLenPrefixed(buf, off, gmid)
	copy(buf[off:], e.Payload)
	return buf
}

// UnmarshalEnvelope deserializes an envelope from bytes.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	if len(data) < envelopeHeaderBytes {
		return Envelope{}, fmt.Errorf("envelope too short for header")
	}
	env := Envelope{
		Priority:   Priority(data[0]),
		Retries:    data[1],
		Difficulty: binary.LittleEndian.Uint64(data[2:10]),
	}
	off := envelopeHeaderBytes

	var err error
	if env.RuntimeName, off, err = readLenPrefixed(data, off, "runtime_name"); err != nil {
		return Envelope{}, err
	}
	if env.ModelID, off, err = readLenPrefixed(data, off, "model_id"); err != nil {
		return Envelope{}, err
	}
	if env.Source, off, err = readLenPrefixed(data, off, "source"); err != nil {
		return Envelope{}, err
	}
	if env.RequestID, off, err = readLenPrefixed(data, off, "request_id"); err != nil {
		return Envelope{}, err
	}
	if env.GlobalMsgID, off, err = readLenPrefixed(data, off, "global_msg_id"); err != nil {
		return Envelope{}, err
	}
	env.Payload = data[off:]
	return env, nil
}

// truncate255 caps a string at 255 bytes so its length fits in a single byte.
func truncate255(s string) string {
	if len(s) > 255 {
		return s[:255]
	}
	return s
}

// writeLenPrefixed writes a 1-byte length followed by the string at off,
// returning the new offset.
func writeLenPrefixed(buf []byte, off int, s string) int {
	buf[off] = byte(len(s))
	copy(buf[off+1:off+1+len(s)], s)
	return off + 1 + len(s)
}

// readLenPrefixed reads a 1-byte length and that many bytes as a string,
// returning the value and the new offset.
func readLenPrefixed(data []byte, off int, field string) (string, int, error) {
	if len(data) < off+1 {
		return "", 0, fmt.Errorf("envelope too short for %s length", field)
	}
	n := int(data[off])
	if len(data) < off+1+n {
		return "", 0, fmt.Errorf("envelope too short for %s body (need %d bytes)", field, n)
	}
	return string(data[off+1 : off+1+n]), off + 1 + n, nil
}

// SubmitResult contains the queue message ID and request hash for cache lookups.
type SubmitResult struct {
	ID          string // goqite message ID
	RequestHash string // SHA-256 hex of canonical (runtime_name, model_id, payload) tuple
}

// Submit enqueues a fully-built envelope. The envelope's RequestID and
// GlobalMsgID are typically set by the caller; otherwise they remain empty
// and re-queue tracking falls back to the goqite message ID.
func (q *Queue) Submit(ctx context.Context, env Envelope) (SubmitResult, error) {
	hash := EnvelopeHash(env)
	id, err := q.q.SendAndGetID(ctx, goqite.Message{
		Body:     env.Marshal(),
		Priority: int(env.Priority),
	})
	if err != nil {
		return SubmitResult{}, ctxerr.With(fmt.Errorf("enqueuing envelope: %w", err), map[string]any{"queue": q.name, "runtime_name": env.RuntimeName, "model_id": env.ModelID})
	}
	q.signal()
	return SubmitResult{ID: string(id), RequestHash: hash}, nil
}

// Receive retrieves the next message from the queue.
// Returns nil, nil if the queue is empty.
func (q *Queue) Receive(ctx context.Context) (*Message, error) {
	msg, err := q.q.Receive(ctx)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return &Message{
		ID:   MessageID(msg.ID),
		Body: msg.Body,
	}, nil
}

// Delete removes a processed message from the queue.
func (q *Queue) Delete(ctx context.Context, id MessageID) error {
	return q.q.Delete(ctx, goqite.ID(id))
}

// ListAbandoned returns messages past their delivery budget with expired
// leases. Goqite never reschedules these (`received >= MaxReceive`); used
// at startup to recover from crashes that left rows dangling.
func (q *Queue) ListAbandoned(ctx context.Context) ([]*Message, error) {
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, body
		FROM goqite
		WHERE queue = ? AND received >= ? AND timeout <= ?`,
		q.name, MaxReceive, now,
	)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing abandoned messages: %w", err), map[string]any{"queue": q.name})
	}
	defer func() { _ = rows.Close() }()

	var out []*Message
	for rows.Next() {
		var id string
		var body []byte
		if err := rows.Scan(&id, &body); err != nil {
			return nil, ctxerr.With(fmt.Errorf("scanning abandoned message: %w", err), map[string]any{"queue": q.name})
		}
		out = append(out, &Message{ID: MessageID(id), Body: body})
	}
	if err := rows.Err(); err != nil {
		return nil, ctxerr.With(fmt.Errorf("iterating abandoned messages: %w", err), map[string]any{"queue": q.name})
	}
	return out, nil
}

// Extend extends the processing timeout for a message.
func (q *Queue) Extend(ctx context.Context, id MessageID, d time.Duration) error {
	return q.q.Extend(ctx, goqite.ID(id), d)
}

// Peek reads up to limit queued messages without consuming them.
// Messages are ordered by priority DESC, created ASC (same as Receive).
func (q *Queue) Peek(ctx context.Context, limit int) ([]*Message, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, body FROM goqite
		WHERE queue = ? AND timeout < strftime('%Y-%m-%dT%H:%M:%fZ')
		ORDER BY priority DESC, created
		LIMIT ?`, q.name, limit)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("peeking queue %s: %w", q.name, err), map[string]any{"queue": q.name})
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var msgs []*Message
	for rows.Next() {
		var m Message
		var id string
		if err := rows.Scan(&id, &m.Body); err != nil {
			return nil, fmt.Errorf("scanning peek row: %w", err)
		}
		m.ID = MessageID(id)
		msgs = append(msgs, &m)
	}
	return msgs, rows.Err()
}

// ReceiveByID consumes a specific message by ID.
// Returns nil, nil if the message doesn't exist or is already consumed.
func (q *Queue) ReceiveByID(ctx context.Context, id MessageID) (*Message, error) {
	var body []byte
	err := q.db.QueryRowContext(ctx, `
		DELETE FROM goqite WHERE id = ? AND queue = ? RETURNING body`, string(id), q.name).Scan(&body)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, ctxerr.With(fmt.Errorf("receiving by ID %s: %w", id, err), map[string]any{"queue": q.name, "message_id": string(id)})
	}
	return &Message{ID: id, Body: body}, nil
}

// LeaseByID claims a message by ID without removing it: bumps timeout to
// now+leaseDur and increments the delivery count. The row stays available
// for [Queue.Extend], [Queue.Delete], or [Queue.ReleaseLeaseAndDelete].
//
// Returns nil, nil if the row is missing, already leased, or past its
// delivery budget. Same visibility + budget guard as goqite's Receive,
// so crash-recovered rows past budget aren't re-dispatched.
func (q *Queue) LeaseByID(ctx context.Context, id MessageID, leaseDur time.Duration) (*Message, error) {
	var msg *Message
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		m, err := q.leaseByIDTx(ctx, tx, id, leaseDur)
		if err != nil {
			return err
		}
		msg = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// leaseByIDTx is the in-transaction variant of [Queue.LeaseByID], used by
// composite operations like [Queue.LeaseAndSubmit].
func (q *Queue) leaseByIDTx(ctx context.Context, tx *sql.Tx, id MessageID, leaseDur time.Duration) (*Message, error) {
	// Timestamp computed in Go to keep SQL dialect-agnostic for Postgres.
	newTimeout := time.Now().UTC().Add(leaseDur).Format("2006-01-02T15:04:05.000Z")
	var body []byte
	err := tx.QueryRowContext(ctx, `
		UPDATE goqite
		SET received = received + 1, timeout = ?
		WHERE id = ? AND queue = ?
		  AND timeout <= strftime('%Y-%m-%dT%H:%M:%fZ')
		  AND received < ?
		RETURNING body`,
		newTimeout, string(id), q.name, MaxReceive,
	).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("leasing by ID %s: %w", id, err), map[string]any{"queue": q.name, "message_id": string(id)})
	}
	return &Message{ID: id, Body: body}, nil
}

// assertSameDB returns other as a *Queue backed by the same database.
// Panics on mismatch — atomic cross-queue ops require one *sql.DB; that's
// a wiring bug, not a recoverable condition.
func assertSameDB(q *Queue, other QueueInterface) *Queue {
	oq, ok := other.(*Queue)
	if !ok {
		panic(fmt.Sprintf("queue: expected *Queue, got %T — atomic cross-queue operations require both queues from the same SQL backend", other))
	}
	if oq.db != q.db {
		panic(fmt.Sprintf("queue: %q and %q are backed by different databases — atomic cross-queue operations require both queues to share one *sql.DB", q.name, oq.name))
	}
	return oq
}

// inTx runs fn in a transaction, committing on success and rolling back
// on error or panic.
func (q *Queue) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing tx: %w", err)
	}
	return nil
}

// MoveTo atomically consumes msgID and submits its envelope to dst. Used
// by work stealing. Returns (false, nil) on race-loss to another consumer.
// Panics if dst is on a different database — wiring bug.
func (q *Queue) MoveTo(ctx context.Context, dst QueueInterface, msgID MessageID, priority Priority) (bool, error) {
	dq := assertSameDB(q, dst)

	moved := false
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(ctx, `
			DELETE FROM goqite WHERE id = ? AND queue = ? RETURNING body`,
			string(msgID), q.name).Scan(&body); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // race-loser: leave moved=false, succeed silently
			}
			return ctxerr.With(fmt.Errorf("consuming source row %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
		}
		if _, err := dq.q.SendAndGetIDTx(ctx, tx, goqite.Message{
			Body:     body,
			Priority: int(priority),
		}); err != nil {
			return ctxerr.With(fmt.Errorf("inserting destination row: %w", err), map[string]any{"queue": dq.name})
		}
		moved = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if moved {
		dq.signal()
	}
	return moved, nil
}

// ReleaseLeaseAndDelete atomically releases the lease on otherMsgID in
// other (consumers see it again immediately) and deletes msgID from this
// queue. Used by device-queue drain to hand a task back to the dispatcher
// without losing its original position.
//
// Panics if other is on a different database — wiring bug.
func (q *Queue) ReleaseLeaseAndDelete(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error {
	oq := assertSameDB(q, other)

	err := q.inTx(ctx, func(tx *sql.Tx) error {
		// Reset received=0 alongside the lease release: handing a task back
		// is a voluntary reroute, not a delivery failure, so the retry
		// budget must not be consumed. Without this, a released row at
		// MaxReceive=1 would be unreachable to any future Receive and die
		// silently.
		if _, err := tx.ExecContext(ctx,
			`UPDATE goqite SET timeout = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second'), received = 0 WHERE queue = ? AND id = ?`,
			oq.name, string(otherMsgID)); err != nil {
			return ctxerr.With(fmt.Errorf("releasing lease on %s: %w", otherMsgID, err), map[string]any{"queue": oq.name, "message_id": string(otherMsgID)})
		}
		if err := q.q.DeleteTx(ctx, tx, goqite.ID(msgID)); err != nil {
			return ctxerr.With(fmt.Errorf("deleting %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
		}
		return nil
	})
	if err != nil {
		return err
	}
	oq.signal()
	return nil
}

// LeaseAndSubmit atomically leases leaseID on this queue and inserts env
// into dst. Used by the dispatcher to hand a global-queue task to a
// device queue: global row stays under lease (Extend/Delete still work)
// while the device row is created in the same transaction.
//
// Returns (SubmitResult{}, false, nil) if the global row is missing,
// already leased, or past budget — caller treats as race-loser. On DB
// error, both queues are unchanged.
//
// Panics if dst is on a different database — wiring bug.
func (q *Queue) LeaseAndSubmit(ctx context.Context, leaseID MessageID, leaseDur time.Duration, dst QueueInterface, env Envelope) (SubmitResult, bool, error) {
	dq := assertSameDB(q, dst)

	var result SubmitResult
	leased := false
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		msg, err := q.leaseByIDTx(ctx, tx, leaseID, leaseDur)
		if err != nil {
			return err
		}
		if msg == nil {
			return nil // race-loser: leave leased=false, succeed silently
		}
		id, err := dq.q.SendAndGetIDTx(ctx, tx, goqite.Message{
			Body:     env.Marshal(),
			Priority: int(env.Priority),
		})
		if err != nil {
			return ctxerr.With(fmt.Errorf("inserting destination row: %w", err), map[string]any{"queue": dq.name})
		}
		result = SubmitResult{ID: string(id), RequestHash: EnvelopeHash(env)}
		leased = true
		return nil
	})
	if err != nil {
		return SubmitResult{}, false, err
	}
	if leased {
		dq.signal()
	}
	return result, leased, nil
}

// Requeue returns a message to the queue. All envelope metadata is
// preserved so the requeued task scores identically next time.
func (q *Queue) Requeue(ctx context.Context, msg *Message, priority Priority) error {
	env, err := UnmarshalEnvelope(msg.Body)
	if err != nil {
		return fmt.Errorf("unmarshalling envelope for requeue: %w", err)
	}
	env.Priority = priority
	_, err = q.Submit(ctx, env)
	return err
}

// Depth returns the number of pending (unconsumed) messages in the queue.
func (q *Queue) Depth(ctx context.Context) (int, error) {
	var count int
	err := q.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM goqite
		WHERE queue = ? AND timeout < strftime('%Y-%m-%dT%H:%M:%fZ')`, q.name).Scan(&count)
	if err != nil {
		return 0, ctxerr.With(fmt.Errorf("counting queue depth for %s: %w", q.name, err), map[string]any{"queue": q.name})
	}
	return count, nil
}

// EnvelopeHash computes a deterministic SHA-256 hash of an envelope's
// identity tuple (runtime_name, model_id, payload). Used for cache lookups
// — same job submitted twice yields the same hash.
func EnvelopeHash(env Envelope) string {
	h := sha256.New()
	h.Write([]byte(env.RuntimeName))
	h.Write([]byte{0})
	h.Write([]byte(env.ModelID))
	h.Write([]byte{0})
	h.Write(env.Payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}
