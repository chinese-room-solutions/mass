// Package queue provides a durable request queue backed by goqite (SQLite).
// Requests are enqueued with a priority, processed by a worker pool, and results
// stored for cache lookups and async retrieval.
package queue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"google.golang.org/protobuf/proto"
	"maragu.dev/goqite"

	"github.com/chinese-room-solutions/mass/rpc"
)

// Priority controls queue ordering (higher = processed first).
type Priority int

const (
	PriorityLow      Priority = iota // background/batch jobs
	PriorityMedium                   // default for all requests (API, module, etc.)
	PriorityHigh                     // elevated by user or module explicitly
	PriorityCritical                 // urgent/real-time, set explicitly
)

// RequestType identifies the kind of inference request in the queue.
type RequestType byte

const (
	RequestTypeChatCompletion      RequestType = 0
	RequestTypeBatchChatCompletion RequestType = 1
	RequestTypeEmbedding           RequestType = 2
	RequestTypeBatchEmbedding      RequestType = 3
	RequestTypeTokenize            RequestType = 4
)

// Queue wraps goqite to provide typed inference request queueing.
type Queue struct {
	q    *goqite.Queue
	db   *sql.DB
	name string
}

// New creates a new Queue with the default "global" queue name.
func New(db *sql.DB) *Queue {
	return NewNamed(db, "global", 3, 30*time.Second)
}

// NewNamed creates a Queue with the given name, max receive count, and processing timeout.
// Multiple queues with different names can coexist in the same goqite table.
func NewNamed(db *sql.DB, name string, maxReceive int, timeout time.Duration) *Queue {
	q := goqite.New(goqite.NewOpts{
		DB:         db,
		Name:       name,
		MaxReceive: maxReceive,
		Timeout:    timeout,
	})
	return &Queue{q: q, db: db, name: name}
}

// Name returns the queue's name.
func (q *Queue) Name() string { return q.name }

// MaxRetries is the maximum number of times a task can be re-queued before being dropped.
const MaxRetries = 5

// Envelope wraps a serialized proto request with its type, source, and model
// fingerprint for queue transport.
// Wire format: [1B type][1B priority][1B retries][1B source_len][source][1B fp_len][fp][1B reqid_len][reqid][1B gmid_len][gmid][payload].
type Envelope struct {
	Type        RequestType
	Priority    Priority // preserved across queue hops
	Retries     uint8    // incremented on each re-queue; dropped at MaxRetries
	Source      string   // who submitted: "direct", "module:<name>"
	Fingerprint string   // model config fingerprint (may be empty)
	RequestID   string   // original request ID for result tracking across queue hops
	GlobalMsgID string   // original global queue message ID for durability tracking
	Payload     []byte
}

// Marshal serializes the envelope to bytes.
func (e Envelope) Marshal() []byte {
	src := e.Source
	if len(src) > 255 {
		src = src[:255]
	}
	fp := e.Fingerprint
	if len(fp) > 255 {
		fp = fp[:255]
	}
	rid := e.RequestID
	if len(rid) > 255 {
		rid = rid[:255]
	}
	gmid := e.GlobalMsgID
	if len(gmid) > 255 {
		gmid = gmid[:255]
	}
	buf := make([]byte, 7+len(src)+len(fp)+len(rid)+len(gmid)+len(e.Payload))
	buf[0] = byte(e.Type)
	buf[1] = byte(e.Priority)
	buf[2] = e.Retries
	buf[3] = byte(len(src))
	copy(buf[4:4+len(src)], src)
	off := 4 + len(src)
	buf[off] = byte(len(fp))
	copy(buf[off+1:off+1+len(fp)], fp)
	off += 1 + len(fp)
	buf[off] = byte(len(rid))
	copy(buf[off+1:off+1+len(rid)], rid)
	off += 1 + len(rid)
	buf[off] = byte(len(gmid))
	copy(buf[off+1:off+1+len(gmid)], gmid)
	copy(buf[off+1+len(gmid):], e.Payload)
	return buf
}

// UnmarshalEnvelope deserializes an envelope from bytes.
// Wire format: [1B type][1B priority][1B retries][1B source_len][source][1B fp_len][fp][1B reqid_len][reqid][1B gmid_len][gmid][payload].
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	if len(data) < 4 {
		return Envelope{}, fmt.Errorf("envelope too short")
	}

	reqType := RequestType(data[0])
	priority := Priority(data[1])
	retries := data[2]

	srcLen := int(data[3])
	off := 4 + srcLen
	if len(data) < off {
		return Envelope{}, fmt.Errorf("envelope too short for source")
	}
	source := string(data[4:off])

	// Fingerprint.
	if len(data) < off+1 {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Payload: data[off:]}, nil
	}
	fpLen := int(data[off])
	if len(data) < off+1+fpLen {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Payload: data[off:]}, nil
	}
	fp := string(data[off+1 : off+1+fpLen])
	off += 1 + fpLen

	// RequestID.
	if len(data) < off+1 {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Fingerprint: fp, Payload: data[off:]}, nil
	}
	ridLen := int(data[off])
	if len(data) < off+1+ridLen {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Fingerprint: fp, Payload: data[off:]}, nil
	}
	rid := string(data[off+1 : off+1+ridLen])
	off += 1 + ridLen

	// GlobalMsgID.
	if len(data) < off+1 {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Fingerprint: fp, RequestID: rid, Payload: data[off:]}, nil
	}
	gmidLen := int(data[off])
	if len(data) < off+1+gmidLen {
		return Envelope{Type: reqType, Priority: priority, Retries: retries, Source: source, Fingerprint: fp, RequestID: rid, Payload: data[off:]}, nil
	}
	gmid := string(data[off+1 : off+1+gmidLen])
	payload := data[off+1+gmidLen:]

	return Envelope{
		Type:        reqType,
		Priority:    priority,
		Retries:     retries,
		Source:      source,
		Fingerprint: fp,
		RequestID:   rid,
		GlobalMsgID: gmid,
		Payload:     payload,
	}, nil
}

// SubmitResult contains the queue message ID and request hash for cache lookups.
type SubmitResult struct {
	ID          string // goqite message ID
	RequestHash string // SHA-256 hex of canonical request
}

// SubmitChatCompletion enqueues a chat completion request.
func (q *Queue) SubmitChatCompletion(ctx context.Context, req *rpc.ChatCompletionRequest, priority Priority) (SubmitResult, error) {
	return q.submit(ctx, RequestTypeChatCompletion, req, priority)
}

// SubmitBatchChatCompletion enqueues a batch chat completion request.
func (q *Queue) SubmitBatchChatCompletion(ctx context.Context, req *rpc.BatchChatCompletionRequest, priority Priority) (SubmitResult, error) {
	return q.submit(ctx, RequestTypeBatchChatCompletion, req, priority)
}

// SubmitEmbedding enqueues an embedding request.
func (q *Queue) SubmitEmbedding(ctx context.Context, req *rpc.EmbeddingRequest, priority Priority) (SubmitResult, error) {
	return q.submit(ctx, RequestTypeEmbedding, req, priority)
}

// SubmitBatchEmbedding enqueues a batch embedding request.
func (q *Queue) SubmitBatchEmbedding(ctx context.Context, req *rpc.BatchEmbeddingRequest, priority Priority) (SubmitResult, error) {
	return q.submit(ctx, RequestTypeBatchEmbedding, req, priority)
}

// SubmitTokenize enqueues a tokenize request.
func (q *Queue) SubmitTokenize(ctx context.Context, req *rpc.TokenizeRequest, priority Priority) (SubmitResult, error) {
	return q.submit(ctx, RequestTypeTokenize, req, priority)
}

// SubmitEnvelope enqueues a complete envelope, preserving all fields including RequestID.
func (q *Queue) SubmitEnvelope(ctx context.Context, env Envelope, priority Priority) (SubmitResult, error) {
	reqHash := RequestHash(env.Type, env.Payload)

	id, err := q.q.SendAndGetID(ctx, goqite.Message{
		Body:     env.Marshal(),
		Priority: int(priority),
	})
	if err != nil {
		return SubmitResult{}, ctxerr.With(fmt.Errorf("enqueuing envelope: %w", err), map[string]any{"queue": q.name})
	}

	return SubmitResult{
		ID:          string(id),
		RequestHash: reqHash,
	}, nil
}

// SubmitRaw enqueues a pre-serialized request payload with the given type, source, fingerprint, and priority.
func (q *Queue) SubmitRaw(ctx context.Context, reqType RequestType, payload []byte, source, fingerprint string, priority Priority) (SubmitResult, error) {
	reqHash := RequestHash(reqType, payload)
	envelope := Envelope{Type: reqType, Priority: priority, Source: source, Fingerprint: fingerprint, Payload: payload}

	id, err := q.q.SendAndGetID(ctx, goqite.Message{
		Body:     envelope.Marshal(),
		Priority: int(priority),
	})
	if err != nil {
		return SubmitResult{}, ctxerr.With(fmt.Errorf("enqueuing request: %w", err), map[string]any{"queue": q.name, "type": reqType, "source": source, "fingerprint": fingerprint})
	}

	return SubmitResult{
		ID:          string(id),
		RequestHash: reqHash,
	}, nil
}

func (q *Queue) submit(ctx context.Context, reqType RequestType, msg proto.Message, priority Priority) (SubmitResult, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("marshalling request: %w", err)
	}
	return q.SubmitRaw(ctx, reqType, payload, "direct", "", priority)
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

// Extend extends the processing timeout for a message.
func (q *Queue) Extend(ctx context.Context, id MessageID, d time.Duration) error {
	return q.q.Extend(ctx, goqite.ID(id), d)
}

// ReceiveBatch retrieves up to limit messages without blocking.
func (q *Queue) ReceiveBatch(ctx context.Context, limit int) ([]*Message, error) {
	var msgs []*Message
	for range limit {
		msg, err := q.q.Receive(ctx)
		if err != nil {
			return msgs, err
		}
		if msg == nil {
			break
		}
		msgs = append(msgs, &Message{ID: MessageID(msg.ID), Body: msg.Body})
	}
	return msgs, nil
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
	defer func() { _ = rows.Close() }()

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

// Requeue returns a message to the queue with the given priority.
func (q *Queue) Requeue(ctx context.Context, msg *Message, priority Priority) error {
	env, err := UnmarshalEnvelope(msg.Body)
	if err != nil {
		return fmt.Errorf("unmarshalling envelope for requeue: %w", err)
	}
	_, err = q.SubmitRaw(ctx, env.Type, env.Payload, env.Source, env.Fingerprint, priority)
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

// RequestHash computes a deterministic SHA-256 hash of a request for cache lookups.
func RequestHash(reqType RequestType, payload []byte) string {
	h := sha256.New()
	h.Write([]byte{byte(reqType)})
	h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}
