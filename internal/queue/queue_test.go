package queue

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/stretchr/testify/require"
)

// setupTestDB creates a temporary SQLite database with the goqite and results schemas.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	ddl, err := os.ReadFile("../store/migrations/000001_init.up.sql")
	require.NoError(t, err, "reading migration file")

	// Use a temp file so multiple goroutines can share the database.
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", tmpFile+"?_journal_mode=WAL&_busy_timeout=10000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(ddl))
	require.NoError(t, err, "applying migration")

	return db
}

func TestQueue_SubmitAndReceive(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	ctx := context.Background()

	req := &rpc.ChatCompletionRequest{
		ModelConfig: &rpc.ChatModelConfig{
			Config: &rpc.ChatModelConfig_Llama{Llama: &rpc.LlamaChatConfig{Model: "test-model"}},
		},
		Messages: []*rpc.ChatMessage{
			{Role: rpc.Role_ROLE_USER, Content: "hello"},
		},
	}

	result, err := q.SubmitChatCompletion(ctx, req, PriorityMedium)
	require.NoError(t, err)
	require.NotEmpty(t, result.ID)
	require.NotEmpty(t, result.RequestHash)
	require.Len(t, result.RequestHash, 64) // SHA-256 hex

	// Receive the message.
	msg, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Unmarshal the envelope.
	env, err := UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, RequestTypeChatCompletion, env.Type)
	require.Equal(t, "direct", env.Source)
	require.NotEmpty(t, env.Payload)

	// Delete the message.
	err = q.Delete(ctx, msg.ID)
	require.NoError(t, err)

	// Queue should be empty now.
	msg2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg2)
}

func TestQueue_PriorityOrdering(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	ctx := context.Background()

	// Submit all 4 priority levels in scrambled order. The Input field
	// doubles as a tag so we can verify the receive order.
	_, err := q.SubmitEmbedding(ctx, &rpc.EmbeddingRequest{Input: "low"}, PriorityLow)
	require.NoError(t, err)

	_, err = q.SubmitEmbedding(ctx, &rpc.EmbeddingRequest{Input: "critical"}, PriorityCritical)
	require.NoError(t, err)

	_, err = q.SubmitEmbedding(ctx, &rpc.EmbeddingRequest{Input: "medium"}, PriorityMedium)
	require.NoError(t, err)

	_, err = q.SubmitEmbedding(ctx, &rpc.EmbeddingRequest{Input: "high"}, PriorityHigh)
	require.NoError(t, err)

	// Receive should return highest priority first: critical, high, medium, low.
	expected := []string{"critical", "high", "medium", "low"}
	for _, want := range expected {
		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
		env, _ := UnmarshalEnvelope(msg.Body)
		req := &rpc.EmbeddingRequest{}
		require.NoError(t, proto.Unmarshal(env.Payload, req))
		require.Equal(t, want, req.Input, "expected priority order")
		require.NoError(t, q.Delete(ctx, msg.ID))
	}
}

func TestRequestHash_Deterministic(t *testing.T) {
	payload := []byte("test-payload")
	h1 := RequestHash(RequestTypeChatCompletion, payload)
	h2 := RequestHash(RequestTypeChatCompletion, payload)
	require.Equal(t, h1, h2)
}

func TestRequestHash_DifferentTypes(t *testing.T) {
	payload := []byte("same-payload")
	h1 := RequestHash(RequestTypeChatCompletion, payload)
	h2 := RequestHash(RequestTypeEmbedding, payload)
	require.NotEqual(t, h1, h2)
}

func TestEnvelope_MarshalRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{"chat", Envelope{Type: RequestTypeChatCompletion, Source: "direct", Payload: []byte("chat-data")}},
		{"with_fingerprint", Envelope{Type: RequestTypeChatCompletion, Priority: PriorityHigh, Source: "direct", Fingerprint: "abc123def45678", Payload: []byte("chat-data")}},
		{"embedding", Envelope{Type: RequestTypeEmbedding, Priority: PriorityCritical, Retries: 2, Source: "app:playground", Fingerprint: "fp1234", RequestID: "req-99", Payload: []byte("embed-data")}},
		{"with_global_msg_id", Envelope{Type: RequestTypeChatCompletion, Priority: PriorityHigh, Source: "direct", Fingerprint: "fp1234", RequestID: "req-1", GlobalMsgID: "global-msg-abc123", Payload: []byte("data")}},
		{"all_fields", Envelope{Type: RequestTypeBatchChatCompletion, Priority: PriorityCritical, Retries: 3, ModelSizeBytes: 4_000_000_000, Source: "app:embed", Fingerprint: "fp9999", RequestID: "req-42", GlobalMsgID: "gm-xyz-789", Payload: []byte("full")}},
		{"empty_payload", Envelope{Type: RequestTypeTokenize, Payload: []byte{}}},
		{"empty_source", Envelope{Type: RequestTypeChatCompletion, Payload: []byte("data")}},
		{"max_retries", Envelope{Type: RequestTypeChatCompletion, Retries: 255, Source: "direct", Payload: []byte("x")}},
		{"large_model_size", Envelope{Type: RequestTypeChatCompletion, ModelSizeBytes: 70_000_000_000, Source: "direct", Fingerprint: "fp", Payload: []byte("p")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.env.Marshal()
			got, err := UnmarshalEnvelope(data)
			require.NoError(t, err)
			require.Equal(t, tt.env.Type, got.Type)
			require.Equal(t, tt.env.Priority, got.Priority)
			require.Equal(t, tt.env.Retries, got.Retries)
			require.Equal(t, tt.env.ModelSizeBytes, got.ModelSizeBytes)
			require.Equal(t, tt.env.Source, got.Source)
			require.Equal(t, tt.env.Fingerprint, got.Fingerprint)
			require.Equal(t, tt.env.RequestID, got.RequestID)
			require.Equal(t, tt.env.GlobalMsgID, got.GlobalMsgID)
			require.Equal(t, tt.env.Payload, got.Payload)
		})
	}
}

func TestUnmarshalEnvelope_TooShort(t *testing.T) {
	_, err := UnmarshalEnvelope(nil)
	require.Error(t, err)
	_, err = UnmarshalEnvelope([]byte{})
	require.Error(t, err)
	// Header alone (11 bytes) is the minimum; anything shorter must error.
	_, err = UnmarshalEnvelope(make([]byte, 10))
	require.Error(t, err)
}

func TestUnmarshalEnvelope_SourceTruncated(t *testing.T) {
	// header (11 bytes) + source_len=5 but only 2 bytes of source follow.
	buf := make([]byte, 11)
	buf = append(buf, 0x05, 'a', 'b')
	_, err := UnmarshalEnvelope(buf)
	require.Error(t, err)
}

func TestQueue_MessageRedeliveryAfterTimeout(t *testing.T) {
	db := setupTestDB(t)
	// Very short timeout (200ms) so message redelivers quickly.
	q := NewNamed(db, "redelivery-test", 3, 200*time.Millisecond)
	ctx := context.Background()

	_, err := q.SubmitRaw(ctx, RequestTypeChatCompletion, []byte("durable"), "direct", "fp1", 0, PriorityMedium)
	require.NoError(t, err)

	// Receive the message (makes it invisible for 200ms).
	msg1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg1)

	// Immediately try to receive again — should be empty (message invisible).
	msg2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg2, "message should be invisible after first receive")

	// Wait for the visibility timeout to expire.
	time.Sleep(300 * time.Millisecond)

	// Message should be redelivered.
	msg3, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg3, "message should redeliver after timeout")

	env, err := UnmarshalEnvelope(msg3.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("durable"), env.Payload)

	// Clean up.
	require.NoError(t, q.Delete(ctx, msg3.ID))
}

func TestQueue_ExtendPreventsRedelivery(t *testing.T) {
	db := setupTestDB(t)
	// Very short timeout (200ms).
	q := NewNamed(db, "extend-test", 3, 200*time.Millisecond)
	ctx := context.Background()

	_, err := q.SubmitRaw(ctx, RequestTypeChatCompletion, []byte("extended"), "direct", "", 0, PriorityMedium)
	require.NoError(t, err)

	msg, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Extend the message visibility well beyond the timeout.
	require.NoError(t, q.Extend(ctx, msg.ID, 5*time.Second))

	// Wait past the original timeout.
	time.Sleep(300 * time.Millisecond)

	// Message should NOT redeliver because we extended it.
	msg2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg2, "extended message should not redeliver")

	// Now delete it (simulating successful completion).
	require.NoError(t, q.Delete(ctx, msg.ID))
}
