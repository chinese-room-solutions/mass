package scheduler

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// connectBenchedWorker registers a fake benched worker and materialises
// its queue, giving the tail mirror and the store row a common starting
// point (0 / "").
func connectBenchedWorker(t *testing.T, s *Scheduler, st *store.Store, workerID string) *worker.StreamWorker {
	t.Helper()
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: workerID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Flops: 100, BenchedAt: time.Now(),
	}))
	w := newFakeWorker(workerID, gpu1())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	return w
}

// The tail mirror is what scoring reads; the store row is the durable
// copy. Every mutation path — connect, credit, reconnect, debit,
// over-debit (clamp), re-estimate — must leave the two in agreement.
func TestTailMirror_StaysInSyncWithStore(t *testing.T) {
	s, st := newTestScheduler(t)
	const workerID = "w1"
	qName := workerQueueName(workerID)
	w := connectBenchedWorker(t, s, st, workerID)

	steps := []struct {
		name      string
		act       func()
		wantSec   float64
		wantModel string
	}{
		{"connect initializes zero tail", func() {}, 0, ""},
		{"credit extends tail and moves tail model", func() { s.creditTail(qName, 2.5, "m-a") }, 2.5, "m-a"},
		{"second credit accumulates", func() { s.creditTail(qName, 1.5, "m-b") }, 4.0, "m-b"},
		{"reconnect must not zero a live tail", func() { s.OnWorkerConnected(w) }, 4.0, "m-b"},
		{"debit shrinks tail, keeps tail model", func() { s.debitTail(qName, 2.5) }, 1.5, "m-b"},
		{"over-debit clamps at zero", func() { s.debitTail(qName, 99) }, 0, "m-b"},
		{"re-estimate over an empty queue resets both", func() {
			s.creditTail(qName, 3, "m-c")
			s.ReestimateWorkerQueue(context.Background(), workerID)
		}, 0, ""},
	}
	for _, step := range steps {
		step.act()
		require.InDelta(t, step.wantSec, s.tailSeconds(qName), 1e-9, "%s: mirror seconds", step.name)
		require.Equal(t, step.wantModel, s.tailModel(qName), "%s: mirror model", step.name)
		row, err := st.GetWorkerQueueState(qName)
		require.NoError(t, err, "%s: store row", step.name)
		require.InDelta(t, step.wantSec, row.TailSeconds, 1e-9, "%s: store seconds", step.name)
		require.Equal(t, step.wantModel, row.TailModelID, "%s: store model", step.name)
	}

	// Disconnect drops both the mirror entry and the store row; reads
	// fall back to 0 / "".
	s.OnWorkerDisconnected(workerID)
	require.Zero(t, s.tailSeconds(qName))
	require.Empty(t, s.tailModel(qName))
	_, err := st.GetWorkerQueueState(qName)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

// A restart builds a fresh Scheduler over the same store;
// recoverPersistedQueues must rehydrate the mirror from the persisted
// rows so tails survive instead of resetting every queue to 0.
func TestTailMirror_RehydratesAfterRestart(t *testing.T) {
	s, st := newTestScheduler(t)
	const workerID = "w1"
	qName := workerQueueName(workerID)
	connectBenchedWorker(t, s, st, workerID)
	s.creditTail(qName, 3.5, "m-a")

	second := New(&config.Config{}, zerolog.Nop(), worker.NewFleet())
	second.InitQueue(queue.NewPool(st.DB(), st.Dialect()), queue.NewResultStore(st.DB(), st.Dialect()), st)
	second.recoverPersistedQueues()

	require.InDelta(t, 3.5, second.tailSeconds(qName), 1e-9)
	require.Equal(t, "m-a", second.tailModel(qName))
}
