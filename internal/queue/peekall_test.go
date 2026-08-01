package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
)

// PeekAll's Leased flag is computed in SQL so it works on every dialect
// — the old Go-side timestamp parse silently reported every Postgres
// row as unleased, which made re-estimation rewrite in-flight rows as
// if they were pending. SQLite-backed: a leased row must report
// Leased=true, its pending sibling false, and a lapsed lease false again.
func TestQueue_PeekAll_ReportsLeaseState(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	pending, err := q.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-pending", Payload: []byte("p"),
	})
	require.NoError(t, err)
	inflight, err := q.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-inflight", Payload: []byte("p"),
	})
	require.NoError(t, err)
	lapsed, err := q.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-lapsed", Payload: []byte("p"),
	})
	require.NoError(t, err)

	leased, err := q.LeaseByID(ctx, queue.MessageID(inflight.ID), time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)
	// A lease already in the past reads as pending again — expiry, not
	// the lease's existence, is what the flag reports.
	expired, err := q.LeaseByID(ctx, queue.MessageID(lapsed.ID), -time.Second)
	require.NoError(t, err)
	require.NotNil(t, expired)

	rows, err := q.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	byID := map[string]bool{}
	for _, r := range rows {
		byID[string(r.ID)] = r.Leased
	}
	require.False(t, byID[pending.ID], "never-leased row must report Leased=false")
	require.True(t, byID[inflight.ID], "actively leased row must report Leased=true")
	require.False(t, byID[lapsed.ID], "expired lease must report Leased=false")
}
