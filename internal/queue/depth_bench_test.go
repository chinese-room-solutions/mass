package queue_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

// BenchmarkQueueDepthVsPeek quantifies the cost gap between counting
// pending rows via Depth (COUNT(*)) and via Peek + len (which SELECTs
// id + body, dragging every payload through the driver). Both primitives
// stay in use — Peek for actual row inspection, Depth wherever only the
// count matters (metrics sweep, work stealing) — so the comparison stays
// meaningful. Payloads are sized to a realistic multi-KB job body; no
// assertions on absolute time.
func BenchmarkQueueDepthVsPeek(b *testing.B) {
	const (
		rows        = 500
		payloadSize = 64 * 1024
	)
	st, err := store.Open(store.DialectSQLite, filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	b.Cleanup(func() { _ = st.Close() })

	q := queue.NewPool(st.DB(), st.Dialect()).Open("bench")
	ctx := context.Background()
	payload := bytes.Repeat([]byte{0xAB}, payloadSize)
	for i := range rows {
		_, err := q.Submit(ctx, queue.Envelope{
			RequestID:   fmt.Sprintf("req-%d", i),
			RuntimeName: "bench-rt",
			Payload:     payload,
		})
		require.NoError(b, err)
	}

	b.Run("peek", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			msgs, err := q.Peek(ctx, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(msgs) != rows {
				b.Fatalf("peeked %d rows, want %d", len(msgs), rows)
			}
		}
	})
	b.Run("depth", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := q.Depth(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if n != rows {
				b.Fatalf("depth %d, want %d", n, rows)
			}
		}
	})
}
