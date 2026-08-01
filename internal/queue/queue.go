// Package queue provides a durable, prioritized job queue backed by goqite.
// Results are stored alongside for cache lookups and async retrieval.
//
// Job payloads are runtime-agnostic opaque bytes — encoded by the runtime
// gateway that submitted the job, decoded by the worker of the matching
// runtime kind. MASS treats them as bytes throughout.
//
// Supports both goqite SQL flavors (SQLite, Postgres) via [Dialect] passed
// to [NewPool]. Goqite's own SQL is dialect-aware; this package's
// hand-written goqite-table queries (Peek, PeekAll, Depth, LeaseByID,
// MoveTo, ReleaseLeaseAndDelete, ListAbandoned) use [Dialect.now] /
// [Dialect.timeoutParam] to bind the `timeout` column correctly for each
// flavor (TEXT on SQLite, TIMESTAMPTZ on Postgres).
package queue

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/KernelPryanic/ctxerr"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/store"
	"google.golang.org/protobuf/proto"
	"maragu.dev/goqite"
)

// Dialect aliases [store.Dialect]. Re-exported so queue callers don't have
// to import the store package just to construct a [*Pool].
type Dialect = store.Dialect

// Re-exported dialect values for the same reason.
const (
	DialectSQLite   = store.DialectSQLite
	DialectPostgres = store.DialectPostgres
)

// sqlFlavor returns the goqite SQL flavor for d.
func sqlFlavor(d Dialect) goqite.SQLFlavor {
	if d == DialectPostgres {
		return goqite.SQLFlavorPostgreSQL
	}
	return goqite.SQLFlavorSQLite
}

// nowTimeoutParam returns the bind value to compare against the goqite
// `timeout` column for "now". SQLite stores timeout as TEXT (RFC3339-milli);
// Postgres stores it as TIMESTAMPTZ.
func nowTimeoutParam(d Dialect, now time.Time) any {
	if d == DialectPostgres {
		return now
	}
	return now.UTC().Format("2006-01-02T15:04:05.000Z")
}

// offsetTimeoutParam returns the bind value for "now + delta" against the
// goqite `timeout` column (delta may be negative for past times).
func offsetTimeoutParam(d Dialect, now time.Time, delta time.Duration) any {
	return nowTimeoutParam(d, now.Add(delta))
}

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
	q       *goqite.Queue
	db      *sql.DB
	dialect Dialect
	name    string
	// notify is the capacity-1 wake-up channel signalled whenever this
	// queue gains a visible row. Queues from [Pool.Open] share the pool's
	// channel (see [Pool.NotifyCh]); queues from [New]/[NewNamed] get a
	// private one nobody listens to.
	notify chan struct{}
}

// rebind rewrites `?` to `$N` placeholders for the queue's dialect.
func (q *Queue) rebind(query string) string { return store.Rebind(q.dialect, query) }

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
// atomic cross-queue operations like [Queue.MoveTo].
type Pool struct {
	db      *sql.DB
	dialect Dialect
	notify  chan struct{} // capacity-1; shared by every Queue from Open
}

// NewPool creates a Pool backed by db with the given dialect. The dialect
// must match the SQL backend underlying db; mismatched dialect will produce
// runtime SQL errors as goqite emits queries for the wrong flavor.
func NewPool(db *sql.DB, dialect Dialect) *Pool {
	return &Pool{db: db, dialect: dialect, notify: make(chan struct{}, 1)}
}

// NotifyCh returns the pool-level notification channel: signalled whenever
// any queue opened from this pool gains a visible row (submit, cross-queue
// move, lease release). Capacity 1 — signals while nobody reads coalesce
// into one wake-up, and a signal arriving while the consumer is mid-pass
// stays buffered so the next receive returns immediately; no wake-up is
// ever lost. One channel regardless of queue count, so a dispatcher can
// select on it without per-queue fan-in goroutines.
func (p *Pool) NotifyCh() <-chan struct{} { return p.notify }

// Open returns a [*Queue] named name, backed by the pool's database.
// Multiple queue names coexist in the same goqite table. Sharing one
// returned handle across goroutines is fine and recommended.
func (p *Pool) Open(name string) *Queue {
	return newQueue(p.db, p.dialect, name, p.notify)
}

// New creates a Queue with the default "global" name. Prefer [Pool.Open]
// for new code — it makes the shared-DB invariant explicit.
func New(db *sql.DB, dialect Dialect) *Queue {
	return newQueue(db, dialect, "global", make(chan struct{}, 1))
}

// NewNamed creates a Queue with custom name, max receive, and timeout.
// Prefer [Pool.Open] for new code; NewNamed is for tests that need to
// override the retry budget or visibility timeout.
func NewNamed(db *sql.DB, dialect Dialect, name string, maxReceive int, timeout time.Duration) *Queue {
	q := goqite.New(goqite.NewOpts{
		DB:         db,
		Name:       name,
		MaxReceive: maxReceive,
		Timeout:    timeout,
		SQLFlavor:  sqlFlavor(dialect),
	})
	return &Queue{q: q, db: db, dialect: dialect, name: name, notify: make(chan struct{}, 1)}
}

// newQueue is the canonical constructor used by Pool.Open and New. Always
// uses [MaxReceive] and [DefaultVisibilityTimeout] — the values production
// code actually wants.
func newQueue(db *sql.DB, dialect Dialect, name string, notify chan struct{}) *Queue {
	q := goqite.New(goqite.NewOpts{
		DB:         db,
		Name:       name,
		MaxReceive: MaxReceive,
		Timeout:    DefaultVisibilityTimeout,
		SQLFlavor:  sqlFlavor(dialect),
	})
	return &Queue{q: q, db: db, dialect: dialect, name: name, notify: notify}
}

// signal performs a non-blocking send to the notify channel.
func (q *Queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Name returns the queue's name.
func (q *Queue) Name() string { return q.name }

// Envelope wraps an opaque, gateway-encoded payload with scheduler metadata.
//
// Wire format (little-endian):
//
//	[1B priority][1B retries]
//	[8B cost float64][8B queued_seconds float64]
//	[8B base_load_bytes int64][8B per_slot_bytes int64][4B headroom_pct int32]
//	[1B runtime_name_len][runtime_name]
//	[1B model_id_len][model_id]
//	[1B source_len][source]
//	[1B reqid_len][reqid]
//	[1B gmid_len][gmid]
//	[1B cost_axis_len][cost_axis]
//	[4B load_artifacts_len][load_artifacts]
//	[payload]
//
// load_artifacts is a marshaled worker.HubLoadModel proto carrying Files
// and LoadHints — the artifacts MASS may need to ship to a worker when
// the dispatcher pops this envelope and the worker isn't already holding
// the model. Empty when the gateway didn't attach any (e.g. tests).
type Envelope struct {
	Priority Priority // preserved across queue hops
	// Attempts counts how many times the scheduler has tried to
	// dispatch this envelope. 0 on first submit; incremented when a
	// load failure triggers a release-back-to-global retry. Compared
	// against Config.LoadAttempts to cap retries.
	Attempts uint8
	// Cost is the gateway's prediction of how expensive this job is on
	// the runtime's reference workload, in runtime-private cost units.
	// Divided by the chosen worker's throughput on CostAxis to score
	// candidates and produce QueuedSeconds. Units are runtime-private;
	// MASS does not interpret.
	Cost float64
	// QueuedSeconds is the expected wall-clock cost this envelope adds to
	// the worker queue it landed on, in seconds. Computed at enqueue from
	// Cost/throughput_w + load_latency_w (load_latency is non-zero only
	// when the placement choice forced a model switch). Subtracted from
	// the queue's tail_seconds sum on dispatch pop, so the running tail
	// stays consistent with what's still ahead in real time.
	QueuedSeconds float64
	// CostAxis names the throughput dimension Cost divides by. Worker
	// must advertise this axis, or the runtime's default-axis is used
	// as fallback at scoring time. Runtime-private vocabulary; MASS does
	// not interpret the string.
	CostAxis    string
	RuntimeName string                // routes to workers of this kind (e.g. "llama-cpp")
	ModelID     string                // gateway-defined opaque ID; affinity key (which workers have it loaded)
	Source      string                // who submitted: "direct", "gateway:<runtime_name>", etc.
	RequestID   string                // original request ID for result tracking across queue hops
	GlobalMsgID string                // original global queue message ID for durability tracking
	Files       []*workerpb.ModelFile // load artifacts (passed to worker if model not resident)
	LoadHints   []byte                // gateway-defined; forwarded as HubLoadModel.load_hints
	Payload     []byte                // gateway-defined opaque job bytes (sent to worker as HubAssignJob.payload)
	// BaseLoadBytes is the gateway's prediction of the fixed device
	// memory cost the load pays regardless of concurrency — weights +
	// activation scratch. MASS uses it both at Submit time (reject
	// when no fleet member's total memory could host it) and at
	// dispatch (skip workers whose free memory wouldn't fit even one
	// slot). 0 = unknown — predicates pass through.
	BaseLoadBytes int64
	// PerSlotBytes is the gateway's prediction of the incremental
	// cost per additional concurrent slot (KV at the configured ctx
	// for llama-cpp; per-stream state for TTS; etc.). MASS combines
	// it with the chosen worker's free memory and HeadroomPct to
	// project the post-grow pool size for load wall-clock. 0 = no
	// concurrency dimension (projection collapses to pool=1).
	PerSlotBytes int64
	// HeadroomPct is the device-memory watermark (1-100) the worker
	// will respect when growing the pool. 0 = unknown; MASS falls
	// back to a runtime-agnostic constant.
	HeadroomPct int32
}

// envelopeHeaderBytes is the fixed-size prefix in the wire format:
// priority + retries + cost + queued_seconds + base_load_bytes +
// per_slot_bytes + headroom_pct. Variable-length fields follow.
const envelopeHeaderBytes = 1 + 1 + 8 + 8 + 8 + 8 + 4

// Marshal serializes the envelope to bytes. Panics when an identity field
// exceeds the wire format's 1-byte length prefix — gateway-supplied fields
// are validated at the Submit boundary and MASS-generated IDs can't grow
// that large, so an oversize value here is an invariant violation, not a
// recoverable input error. Truncating instead would silently corrupt
// identity (residency matching, cancellation, result tracking).
func (e Envelope) Marshal() []byte {
	rk := fit255("runtime_name", e.RuntimeName)
	mid := fit255("model_id", e.ModelID)
	src := fit255("source", e.Source)
	rid := fit255("request_id", e.RequestID)
	gmid := fit255("global_msg_id", e.GlobalMsgID)
	axis := fit255("cost_axis", e.CostAxis)
	loadBlob := marshalLoadArtifacts(e.Files, e.LoadHints)

	buf := make([]byte, envelopeHeaderBytes+6+len(rk)+len(mid)+len(src)+len(rid)+len(gmid)+len(axis)+4+len(loadBlob)+len(e.Payload))
	buf[0] = byte(e.Priority)
	buf[1] = e.Attempts
	binary.LittleEndian.PutUint64(buf[2:10], math.Float64bits(e.Cost))
	binary.LittleEndian.PutUint64(buf[10:18], math.Float64bits(e.QueuedSeconds))
	binary.LittleEndian.PutUint64(buf[18:26], uint64(e.BaseLoadBytes))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(e.PerSlotBytes))
	binary.LittleEndian.PutUint32(buf[34:38], uint32(e.HeadroomPct))
	off := envelopeHeaderBytes

	off = writeLenPrefixed(buf, off, rk)
	off = writeLenPrefixed(buf, off, mid)
	off = writeLenPrefixed(buf, off, src)
	off = writeLenPrefixed(buf, off, rid)
	off = writeLenPrefixed(buf, off, gmid)
	off = writeLenPrefixed(buf, off, axis)
	off = writeLenPrefixedU32(buf, off, loadBlob)
	copy(buf[off:], e.Payload)
	return buf
}

// UnmarshalEnvelope deserializes an envelope from bytes.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	if len(data) < envelopeHeaderBytes {
		return Envelope{}, fmt.Errorf("envelope too short for header")
	}
	env := Envelope{
		Priority:      Priority(data[0]),
		Attempts:      data[1],
		Cost:          math.Float64frombits(binary.LittleEndian.Uint64(data[2:10])),
		QueuedSeconds: math.Float64frombits(binary.LittleEndian.Uint64(data[10:18])),
		BaseLoadBytes: int64(binary.LittleEndian.Uint64(data[18:26])),
		PerSlotBytes:  int64(binary.LittleEndian.Uint64(data[26:34])),
		HeadroomPct:   int32(binary.LittleEndian.Uint32(data[34:38])),
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
	if env.CostAxis, off, err = readLenPrefixed(data, off, "cost_axis"); err != nil {
		return Envelope{}, err
	}
	var loadBlob []byte
	if loadBlob, off, err = readLenPrefixedU32(data, off, "load_artifacts"); err != nil {
		return Envelope{}, err
	}
	if env.Files, env.LoadHints, err = unmarshalLoadArtifacts(loadBlob); err != nil {
		return Envelope{}, err
	}
	env.Payload = data[off:]
	return env, nil
}

// marshalLoadArtifacts packs the per-load files + load_hints into a
// worker.HubLoadModel proto blob. Returns nil when both inputs are empty.
// Reusing HubLoadModel rather than a queue-private message keeps one
// definition of "load artifacts" shared by the worker contract and the
// queue.
func marshalLoadArtifacts(files []*workerpb.ModelFile, loadHints []byte) []byte {
	if len(files) == 0 && len(loadHints) == 0 {
		return nil
	}
	blob, err := proto.Marshal(&workerpb.HubLoadModel{
		Files:     files,
		LoadHints: loadHints,
	})
	if err != nil {
		// Marshaling proto messages we constructed in-memory shouldn't fail;
		// signal a wiring bug rather than a recoverable condition.
		panic(fmt.Errorf("queue: marshalling envelope load artifacts: %w", err))
	}
	return blob
}

// unmarshalLoadArtifacts reverses [marshalLoadArtifacts]. Empty input
// returns (nil, nil, nil) — Envelope.Marshal omits the blob in that case
// and the reader sees a zero-length section.
func unmarshalLoadArtifacts(blob []byte) ([]*workerpb.ModelFile, []byte, error) {
	if len(blob) == 0 {
		return nil, nil, nil
	}
	var msg workerpb.HubLoadModel
	if err := proto.Unmarshal(blob, &msg); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling envelope load artifacts: %w", err)
	}
	return msg.Files, msg.LoadHints, nil
}

// writeLenPrefixedU32 writes a 4-byte length followed by the bytes at off,
// returning the new offset. Used for sections that may exceed 255 bytes
// (e.g. marshaled proto blobs).
func writeLenPrefixedU32(buf []byte, off int, b []byte) int {
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(b)))
	copy(buf[off+4:off+4+len(b)], b)
	return off + 4 + len(b)
}

// readLenPrefixedU32 reads a 4-byte length and that many bytes, returning
// the slice (sharing memory with data) and the new offset.
func readLenPrefixedU32(data []byte, off int, field string) ([]byte, int, error) {
	if len(data) < off+4 {
		return nil, 0, fmt.Errorf("envelope too short for %s length", field)
	}
	n := int(binary.LittleEndian.Uint32(data[off : off+4]))
	if len(data) < off+4+n {
		return nil, 0, fmt.Errorf("envelope too short for %s body (need %d bytes)", field, n)
	}
	return data[off+4 : off+4+n], off + 4 + n, nil
}

// fit255 returns s after asserting it fits the wire format's 1-byte length
// prefix. See [Envelope.Marshal] for why oversize panics.
func fit255(field, s string) string {
	if len(s) > 255 {
		panic(fmt.Sprintf("queue: envelope %s is %d bytes, exceeds the 255-byte wire cap", field, len(s)))
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

// SubmitResult carries the goqite message ID of the enqueued row.
type SubmitResult struct {
	ID string // goqite message ID
}

// Submit enqueues a fully-built envelope. The envelope's RequestID and
// GlobalMsgID are typically set by the caller; otherwise they remain empty
// and re-queue tracking falls back to the goqite message ID.
func (q *Queue) Submit(ctx context.Context, env Envelope) (SubmitResult, error) {
	id, err := q.q.SendAndGetID(ctx, goqite.Message{
		Body:     env.Marshal(),
		Priority: int(env.Priority),
	})
	if err != nil {
		return SubmitResult{}, ctxerr.With(fmt.Errorf("enqueuing envelope: %w", err), map[string]any{"queue": q.name, "runtime_name": env.RuntimeName, "model_id": env.ModelID})
	}
	q.signal()
	return SubmitResult{ID: string(id)}, nil
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

// UpdateBody rewrites the message body in place. Used by re-estimation
// flows that need to refresh per-envelope fields (e.g. QueuedSeconds
// recomputed against a changed worker device set) without affecting
// queue ordering or lease state.
//
// Returns nil when the row is missing — callers treat that as "already
// gone, nothing to update" rather than a hard error.
func (q *Queue) UpdateBody(ctx context.Context, id MessageID, body []byte) error {
	_, err := q.db.ExecContext(ctx, q.rebind(`
		UPDATE goqite SET body = ? WHERE id = ? AND queue = ?`),
		body, string(id), q.name)
	if err != nil {
		return ctxerr.With(fmt.Errorf("updating body for %s: %w", id, err),
			map[string]any{"queue": q.name, "message_id": string(id)})
	}
	return nil
}

// ListAbandoned returns messages past their delivery budget with expired
// leases. Goqite never reschedules these (`received >= MaxReceive`); used
// at startup to recover from crashes that left rows dangling.
func (q *Queue) ListAbandoned(ctx context.Context) ([]*Message, error) {
	now := nowTimeoutParam(q.dialect, time.Now())
	rows, err := q.db.QueryContext(ctx, q.rebind(`
		SELECT id, body
		FROM goqite
		WHERE queue = ? AND received >= ? AND timeout <= ?`),
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
// The visibility comparison is `timeout <= now`, matching goqite's
// Receive and [Queue.LeaseByID]: timestamps are millisecond-truncated,
// so a strict `<` would hide a row submitted in the same millisecond.
func (q *Queue) Peek(ctx context.Context, limit int) ([]*Message, error) {
	rows, err := q.db.QueryContext(ctx, q.rebind(`
		SELECT id, body FROM goqite
		WHERE queue = ? AND timeout <= ?
		ORDER BY priority DESC, created
		LIMIT ?`), q.name, nowTimeoutParam(q.dialect, time.Now()), limit)
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

// PeekAll reads up to limit rows from the queue without consuming them,
// including currently-leased rows. Each row carries a Leased flag so the
// operator UI can show in-flight rows as read-only "running" entries.
// Ordering matches [Queue.Peek] (priority DESC, created ASC).
//
// The Leased flag is computed in SQL rather than by parsing the scanned
// column in Go: Postgres scans a TIMESTAMPTZ in a driver-dependent format
// a Go-side parse silently failed on (every row reported unleased). On
// SQLite the compare is TEXT-vs-TEXT over goqite's fixed-width
// RFC3339-milli format, where lexicographic order equals chronological
// order.
//
// Leased means "delivered to a consumer and the window is still open" —
// hidden from Peek AND delivered at least once. A row hidden by
// [Queue.Defer] has never been delivered (received is reset to 0), so it
// reports pending: it's waiting for its retry, not running anywhere.
func (q *Queue) PeekAll(ctx context.Context, limit int) ([]*PeekRow, error) {
	rows, err := q.db.QueryContext(ctx, q.rebind(`
		SELECT id, body, (timeout > ? AND received > 0) FROM goqite
		WHERE queue = ?
		ORDER BY priority DESC, created
		LIMIT ?`), nowTimeoutParam(q.dialect, time.Now()), q.name, limit)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("peeking-all queue %s: %w", q.name, err), map[string]any{"queue": q.name})
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []*PeekRow
	for rows.Next() {
		var (
			id     string
			body   []byte
			leased bool
		)
		if err := rows.Scan(&id, &body, &leased); err != nil {
			return nil, fmt.Errorf("scanning peek-all row: %w", err)
		}
		out = append(out, &PeekRow{
			Message: Message{ID: MessageID(id), Body: body},
			Leased:  leased,
		})
	}
	return out, rows.Err()
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
	now := time.Now()
	newTimeout := offsetTimeoutParam(q.dialect, now, leaseDur)
	nowParam := nowTimeoutParam(q.dialect, now)
	var body []byte
	err := tx.QueryRowContext(ctx, q.rebind(`
		UPDATE goqite
		SET received = received + 1, timeout = ?
		WHERE id = ? AND queue = ?
		  AND timeout <= ?
		  AND received < ?
		RETURNING body`),
		newTimeout, string(id), q.name, nowParam, MaxReceive,
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
// by work stealing. Only a pending row moves: the DELETE requires an
// expired lease and remaining delivery budget (same visibility guard as
// Receive/LeaseByID), so a row a dispatcher currently holds — or an
// abandoned one awaiting the sweeper — can never be stolen into
// duplicate execution. Returns (false, nil) when the row is missing,
// leased, or past budget — callers treat all three as race-loss.
// Panics if dst is on a different database — wiring bug.
func (q *Queue) MoveTo(ctx context.Context, dst QueueInterface, msgID MessageID, priority Priority) (bool, error) {
	dq := assertSameDB(q, dst)

	moved := false
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(ctx, q.rebind(`
			DELETE FROM goqite
			WHERE id = ? AND queue = ? AND timeout <= ? AND received < ?
			RETURNING body`),
			string(msgID), q.name, nowTimeoutParam(q.dialect, time.Now()), MaxReceive).Scan(&body); err != nil {
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

// DeleteBoth atomically deletes msgID from this queue and otherMsgID from
// other. Used on terminal frame: the worker queue row and the global queue
// row that anchors its durability go away together so no half-life rows
// linger.
//
// Panics if other is on a different database — wiring bug.
func (q *Queue) DeleteBoth(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error {
	oq := assertSameDB(q, other)
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		if err := q.q.DeleteTx(ctx, tx, goqite.ID(msgID)); err != nil {
			return ctxerr.With(fmt.Errorf("deleting self row %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
		}
		if err := oq.q.DeleteTx(ctx, tx, goqite.ID(otherMsgID)); err != nil {
			return ctxerr.With(fmt.Errorf("deleting other row %s: %w", otherMsgID, err), map[string]any{"queue": oq.name, "message_id": string(otherMsgID)})
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ReleaseLease moves msgID's timeout into the past so consumers see it
// again immediately. Used when a worker disconnects mid-job: the global
// anchor needs to become available for drainGlobal to re-score against
// surviving workers. Idempotent — a missing row is a no-op.
func (q *Queue) ReleaseLease(ctx context.Context, msgID MessageID) error {
	expiredTimeout := offsetTimeoutParam(q.dialect, time.Now(), -1*time.Second)
	if _, err := q.db.ExecContext(ctx,
		q.rebind(`UPDATE goqite SET timeout = ?, received = 0 WHERE queue = ? AND id = ?`),
		expiredTimeout, q.name, string(msgID)); err != nil {
		return ctxerr.With(fmt.Errorf("releasing lease on %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
	}
	q.signal()
	return nil
}

// Defer hides msgID for delay, after which consumers see it again. Used to
// bounce a row the consumer can't take right now: unlike [Queue.ReleaseLease]
// the row does NOT come back immediately, so a blocker that stays busy can't
// spin the drain-lease-bounce cycle. Idempotent — a missing row is a no-op.
//
// The delivery count is reset (a bounce is a voluntary reroute, not a failed
// delivery): the row stays reachable at MaxReceive=1, and [Queue.PeekAll]
// keeps reporting it as pending rather than in-flight.
//
// No wake-up is signalled — the row isn't visible yet. Re-arming the consumer
// after delay is the caller's job.
func (q *Queue) Defer(ctx context.Context, msgID MessageID, delay time.Duration) error {
	if _, err := q.db.ExecContext(ctx,
		q.rebind(`UPDATE goqite SET timeout = ?, received = 0 WHERE queue = ? AND id = ?`),
		offsetTimeoutParam(q.dialect, time.Now(), delay), q.name, string(msgID)); err != nil {
		return ctxerr.With(fmt.Errorf("deferring %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
	}
	return nil
}

// DeleteAndReleaseLease atomically deletes msgID from this queue and
// releases the lease on otherMsgID in other (making it visible to
// drainGlobal again). Used by OnWorkerDisconnected to reap a worker queue
// row while handing its durability anchor back to global for re-scoring.
//
// Panics if other is on a different database — wiring bug.
func (q *Queue) DeleteAndReleaseLease(ctx context.Context, msgID MessageID, other QueueInterface, otherMsgID MessageID) error {
	oq := assertSameDB(q, other)
	err := q.inTx(ctx, func(tx *sql.Tx) error {
		if err := q.q.DeleteTx(ctx, tx, goqite.ID(msgID)); err != nil {
			return ctxerr.With(fmt.Errorf("deleting self row %s: %w", msgID, err), map[string]any{"queue": q.name, "message_id": string(msgID)})
		}
		expiredTimeout := offsetTimeoutParam(q.dialect, time.Now(), -1*time.Second)
		if _, err := tx.ExecContext(ctx,
			q.rebind(`UPDATE goqite SET timeout = ?, received = 0 WHERE queue = ? AND id = ?`),
			expiredTimeout, oq.name, string(otherMsgID)); err != nil {
			return ctxerr.With(fmt.Errorf("releasing lease on %s: %w", otherMsgID, err), map[string]any{"queue": oq.name, "message_id": string(otherMsgID)})
		}
		return nil
	})
	if err != nil {
		return err
	}
	oq.signal()
	return nil
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
		expiredTimeout := offsetTimeoutParam(q.dialect, time.Now(), -1*time.Second)
		if _, err := tx.ExecContext(ctx,
			q.rebind(`UPDATE goqite SET timeout = ?, received = 0 WHERE queue = ? AND id = ?`),
			expiredTimeout, oq.name, string(otherMsgID)); err != nil {
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
		result = SubmitResult{ID: string(id)}
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

// Depth returns the number of pending (unconsumed) messages in the queue.
// Visibility comparison matches [Queue.Peek] (`timeout <= now`).
func (q *Queue) Depth(ctx context.Context) (int, error) {
	var count int
	err := q.db.QueryRowContext(ctx, q.rebind(`
		SELECT COUNT(*) FROM goqite
		WHERE queue = ? AND timeout <= ?`), q.name, nowTimeoutParam(q.dialect, time.Now())).Scan(&count)
	if err != nil {
		return 0, ctxerr.With(fmt.Errorf("counting queue depth for %s: %w", q.name, err), map[string]any{"queue": q.name})
	}
	return count, nil
}
