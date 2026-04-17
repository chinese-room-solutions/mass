// Package queuetest provides reusable contract tests for queue.QueueInterface
// and queue.ResultStoreInterface implementations. Any queue/result provider can
// run these tests to validate conformance with the expected behavior.
package queuetest

import (
	"context"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
)

// QueueFactory creates a fresh QueueInterface for each test. The contract
// suite is backend-agnostic: any SQL implementation of QueueInterface
// should pass it. To wire a new backend (e.g. Postgres), add a sibling
// test file that calls [RunQueueTests] with a factory for that backend.
// See goqite_test.go for the SQLite reference.
type QueueFactory func(t *testing.T) queue.QueueInterface

// ResultStoreFactory creates a fresh ResultStoreInterface for each test.
// Same backend-agnostic intent as [QueueFactory].
type ResultStoreFactory func(t *testing.T) queue.ResultStoreInterface

// RunQueueTests validates QueueInterface behavior.
func RunQueueTests(t *testing.T, factory QueueFactory) {
	t.Helper()

	t.Run("submit and receive", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		payload := []byte("test-payload")
		sub, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, payload, "direct", "", 0, queue.PriorityMedium)
		require.NoError(t, err)
		require.NotEmpty(t, sub.ID)
		require.NotEmpty(t, sub.RequestHash)
		require.Len(t, sub.RequestHash, 64) // SHA-256 hex

		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.NotEmpty(t, msg.Body)

		env, err := queue.UnmarshalEnvelope(msg.Body)
		require.NoError(t, err)
		require.Equal(t, queue.RequestTypeChatCompletion, env.Type)
		require.Equal(t, "direct", env.Source)
		require.Equal(t, payload, env.Payload)

		require.NoError(t, q.Delete(ctx, msg.ID))

		// Queue should be empty now.
		msg2, err := q.Receive(ctx)
		require.NoError(t, err)
		require.Nil(t, msg2)
	})

	t.Run("submit and receive with fingerprint", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		payload := []byte("test-payload")
		sub, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, payload, "direct", "abc123def456", 0, queue.PriorityMedium)
		require.NoError(t, err)
		require.NotEmpty(t, sub.ID)

		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		env, err := queue.UnmarshalEnvelope(msg.Body)
		require.NoError(t, err)
		require.Equal(t, "abc123def456", env.Fingerprint)
		require.Equal(t, payload, env.Payload)
		require.NoError(t, q.Delete(ctx, msg.ID))
	})

	t.Run("priority ordering", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		_, err := q.SubmitRaw(ctx, queue.RequestTypeEmbedding, []byte("low"), "direct", "", 0, queue.PriorityLow)
		require.NoError(t, err)
		_, err = q.SubmitRaw(ctx, queue.RequestTypeEmbedding, []byte("critical"), "direct", "", 0, queue.PriorityCritical)
		require.NoError(t, err)
		_, err = q.SubmitRaw(ctx, queue.RequestTypeEmbedding, []byte("medium"), "direct", "", 0, queue.PriorityMedium)
		require.NoError(t, err)

		// Highest priority first.
		msg1, err := q.Receive(ctx)
		require.NoError(t, err)
		env1, _ := queue.UnmarshalEnvelope(msg1.Body)
		require.Equal(t, []byte("critical"), env1.Payload)
		require.NoError(t, q.Delete(ctx, msg1.ID))

		msg2, err := q.Receive(ctx)
		require.NoError(t, err)
		env2, _ := queue.UnmarshalEnvelope(msg2.Body)
		require.Equal(t, []byte("medium"), env2.Payload)
		require.NoError(t, q.Delete(ctx, msg2.ID))

		msg3, err := q.Receive(ctx)
		require.NoError(t, err)
		env3, _ := queue.UnmarshalEnvelope(msg3.Body)
		require.Equal(t, []byte("low"), env3.Payload)
		require.NoError(t, q.Delete(ctx, msg3.ID))
	})

	t.Run("receive empty returns nil", func(t *testing.T) {
		q := factory(t)
		msg, err := q.Receive(context.Background())
		require.NoError(t, err)
		require.Nil(t, msg)
	})

	t.Run("peek does not consume", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		_, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("peek-me"), "direct", "fp1", 0, queue.PriorityMedium)
		require.NoError(t, err)

		peeked, err := q.Peek(ctx, 10)
		require.NoError(t, err)
		require.Len(t, peeked, 1)

		// Message should still be receivable.
		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.NoError(t, q.Delete(ctx, msg.ID))
	})

	t.Run("receive by ID", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		sub, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("by-id"), "direct", "", 0, queue.PriorityMedium)
		require.NoError(t, err)

		msg, err := q.ReceiveByID(ctx, queue.MessageID(sub.ID))
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.Equal(t, queue.MessageID(sub.ID), msg.ID)

		// Should be gone now.
		msg2, err := q.ReceiveByID(ctx, queue.MessageID(sub.ID))
		require.NoError(t, err)
		require.Nil(t, msg2)
	})

	t.Run("requeue", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		_, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("requeue-me"), "direct", "fp1", 0, queue.PriorityHigh)
		require.NoError(t, err)

		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Requeue it.
		require.NoError(t, q.Delete(ctx, msg.ID))
		require.NoError(t, q.Requeue(ctx, msg, queue.PriorityHigh))

		// Should be receivable again.
		msg2, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg2)

		env, err := queue.UnmarshalEnvelope(msg2.Body)
		require.NoError(t, err)
		require.Equal(t, "fp1", env.Fingerprint)
		require.Equal(t, []byte("requeue-me"), env.Payload)
		require.NoError(t, q.Delete(ctx, msg2.ID))
	})

	t.Run("extend keeps message invisible", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		_, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("extend-me"), "direct", "", 0, queue.PriorityMedium)
		require.NoError(t, err)

		// Receive the message (makes it invisible).
		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Extend its visibility.
		require.NoError(t, q.Extend(ctx, msg.ID, 10*time.Second))

		// Message should not be receivable by another consumer (still invisible).
		msg2, err := q.Receive(ctx)
		require.NoError(t, err)
		require.Nil(t, msg2, "extended message should remain invisible")

		// Clean up.
		require.NoError(t, q.Delete(ctx, msg.ID))
	})

	t.Run("global msg id preserved through envelope", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		env := queue.Envelope{
			Type:        queue.RequestTypeChatCompletion,
			Priority:    queue.PriorityHigh,
			Source:      "direct",
			Fingerprint: "fp123",
			RequestID:   "req-1",
			GlobalMsgID: "global-msg-abc",
			Payload:     []byte("test"),
		}
		_, err := q.SubmitEnvelope(ctx, env, queue.PriorityHigh)
		require.NoError(t, err)

		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		got, err := queue.UnmarshalEnvelope(msg.Body)
		require.NoError(t, err)
		require.Equal(t, "global-msg-abc", got.GlobalMsgID)
		require.Equal(t, "req-1", got.RequestID)
		require.Equal(t, "fp123", got.Fingerprint)
		require.NoError(t, q.Delete(ctx, msg.ID))
	})

	t.Run("depth", func(t *testing.T) {
		q := factory(t)
		ctx := context.Background()

		d, err := q.Depth(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, d)

		for range 3 {
			_, err := q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("x"), "direct", "", 0, queue.PriorityMedium)
			require.NoError(t, err)
		}

		d, err = q.Depth(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, d)

		msg, err := q.Receive(ctx)
		require.NoError(t, err)
		require.NoError(t, q.Delete(ctx, msg.ID))

		d, err = q.Depth(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, d)
	})
}

// RunResultStoreTests validates ResultStoreInterface behavior.
func RunResultStoreTests(t *testing.T, factory ResultStoreFactory) {
	t.Helper()

	t.Run("create and get", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-abc"))
		r, err := rs.Get("req-1")
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Equal(t, "req-1", r.ID)
		require.Equal(t, "hash-abc", r.RequestHash)
		require.Equal(t, queue.ResultStatusPending, r.Status)
	})

	t.Run("get not found returns nil", func(t *testing.T) {
		rs := factory(t)
		r, err := rs.Get("nonexistent")
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("lifecycle pending to done", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-abc"))

		require.NoError(t, rs.MarkProcessing("req-1"))
		r, _ := rs.Get("req-1")
		require.Equal(t, queue.ResultStatusProcessing, r.Status)

		body := []byte("result-data")
		require.NoError(t, rs.Complete("req-1", body))
		r, _ = rs.Get("req-1")
		require.Equal(t, queue.ResultStatusDone, r.Status)
		require.Equal(t, body, r.Body)
		require.NotNil(t, r.CompletedAt)
	})

	t.Run("fail stores error", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-abc"))
		require.NoError(t, rs.Fail("req-1", "something went wrong"))
		r, _ := rs.Get("req-1")
		require.Equal(t, queue.ResultStatusError, r.Status)
		require.Equal(t, "something went wrong", r.Error)
		require.NotNil(t, r.CompletedAt)
	})

	t.Run("find by hash cache hit", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-xyz"))
		body := []byte("cached-response")
		require.NoError(t, rs.Complete("req-1", body))
		r, err := rs.FindByHash("hash-xyz", 24*time.Hour)
		require.NoError(t, err)
		require.NotNil(t, r)
		require.Equal(t, body, r.Body)
	})

	t.Run("find by hash cache miss", func(t *testing.T) {
		rs := factory(t)
		r, err := rs.FindByHash("hash-xyz", 24*time.Hour)
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("find by hash pending not returned", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-xyz"))
		r, err := rs.FindByHash("hash-xyz", 24*time.Hour)
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("cleanup removes old results", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-1"))
		require.NoError(t, rs.Complete("req-1", []byte("data")))

		time.Sleep(10 * time.Millisecond)
		n, err := rs.Cleanup(time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)

		r, _ := rs.Get("req-1")
		require.Nil(t, r)
	})

	t.Run("wait for result", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-abc"))

		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = rs.Complete("req-1", []byte("done"))
		}()

		neverClose := make(chan struct{})
		r, err := rs.WaitForResult("req-1", 10*time.Millisecond, neverClose)
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusDone, r.Status)
		require.Equal(t, []byte("done"), r.Body)
	})

	t.Run("wait for result cancelled", func(t *testing.T) {
		rs := factory(t)
		require.NoError(t, rs.Create("req-1", "hash-abc"))

		cancelled := make(chan struct{})
		close(cancelled)

		r, err := rs.WaitForResult("req-1", 10*time.Millisecond, cancelled)
		require.Error(t, err)
		require.Nil(t, r)
	})
}
