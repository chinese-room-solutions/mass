package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatLease_PreventsRedelivery verifies that heartbeatLease keeps a
// goqite message invisible past its original visibility timeout. Without this
// extension, an in-flight executeOne could see its message redelivered and a
// duplicate dispatched to the worker pool — the exact bug observed when
// pdf2doc and playground were both queued and pdf2doc's pages had >30s
// inference time.
//
// The test uses a short visibility timeout and overrides heartbeatInterval
// for the duration of the test so it runs in seconds rather than minutes.
func TestHeartbeatLease_PreventsRedelivery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hb.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const (
		visibility = 500 * time.Millisecond
		extendBy   = 1 * time.Second
	)
	q := queue.NewNamed(s.DB(), "test", 3, visibility)

	ctx := context.Background()
	_, err = q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("payload"), "direct", "fp", 0, queue.PriorityMedium)
	require.NoError(t, err)

	msg, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Override the package-level heartbeatInterval so the test runs quickly.
	prev := heartbeatIntervalForTesting(visibility / 4)
	t.Cleanup(func() { _ = heartbeatIntervalForTesting(prev) })

	dq := &DeviceQueueManager{logger: zerolog.Nop(), queueName: "test"}
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go dq.heartbeatLease(hbCtx, q, msg.ID, extendBy, "req-1", "test")

	// Sleep well past the original visibility timeout. Without the heartbeat
	// the message would be redelivered; with it, it stays invisible.
	time.Sleep(visibility * 3)

	got, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "message must remain invisible while heartbeat extends its lease")
}

// TestHeartbeatLease_StopsAfterCancel verifies that once the heartbeat
// context is cancelled, the message becomes visible again at the next
// visibility expiry. This is what allows redelivery to recover from a
// crashed worker.
func TestHeartbeatLease_StopsAfterCancel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hb2.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const visibility = 300 * time.Millisecond
	q := queue.NewNamed(s.DB(), "test", 3, visibility)

	ctx := context.Background()
	_, err = q.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("p"), "direct", "fp", 0, queue.PriorityMedium)
	require.NoError(t, err)

	msg, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	prev := heartbeatIntervalForTesting(visibility / 4)
	t.Cleanup(func() { _ = heartbeatIntervalForTesting(prev) })

	dq := &DeviceQueueManager{logger: zerolog.Nop(), queueName: "test"}
	hbCtx, hbCancel := context.WithCancel(ctx)
	go dq.heartbeatLease(hbCtx, q, msg.ID, visibility, "req-1", "test")

	// Let the heartbeat run briefly, then stop it.
	time.Sleep(visibility / 2)
	hbCancel()

	// After ~visibility from the last extend, the message becomes visible.
	time.Sleep(visibility * 3)
	got, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, got, "message must redeliver after heartbeat stops")
}
