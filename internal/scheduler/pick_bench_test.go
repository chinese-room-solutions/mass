package scheduler

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// BenchmarkPickWorkerQueue scores one envelope against a fleet of 8
// benched workers with non-empty tails through the real SQLite-backed
// store — the exact shape of the 200ms pending-retry loop re-scoring an
// unplaceable global row. The envelope carries load files and a model
// that mismatches every tail so scoring walks the full tail_seconds +
// tail_model + load-latency path per candidate. Deterministic setup; no
// assertions on absolute time.
func BenchmarkPickWorkerQueue(b *testing.B) {
	const (
		runtimeName = "llama-cpp"
		axis        = "q4k_matvec"
		workers     = 8
	)
	st, err := store.Open(store.DialectSQLite, filepath.Join(b.TempDir(), "bench.db"))
	require.NoError(b, err)
	b.Cleanup(func() { _ = st.Close() })

	s := New(&config.Config{}, zerolog.Nop(), worker.NewFleet())
	s.InitQueue(queue.NewPool(st.DB(), st.Dialect()), queue.NewResultStore(st.DB(), st.Dialect()), st)

	for i := range workers {
		id := fmt.Sprintf("w%d", i)
		require.NoError(b, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: id, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{axis: 100 + float64(i)},
			LoadGBs:    10,
			BenchedAt:  time.Now(),
		}))
		w := worker.NewFakeStreamWorker(id, runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(4)
		require.NoError(b, s.workers.Register(w))
		s.OnWorkerConnected(w)
		// Non-zero tail with a model that never matches the envelope's,
		// so every candidate pays the tail read AND the tail-model read.
		s.creditTail("worker|"+id, 1+float64(i), "m-resident-"+id)
	}

	env := queue.Envelope{
		RuntimeName: runtimeName,
		ModelID:     "m-bench",
		Cost:        100,
		CostAxis:    axis,
		Files:       []*workerpb.ModelFile{{Url: "http://models.local/m-bench.gguf", SizeBytes: 4 << 30}},
	}
	b.ReportAllocs()
	for b.Loop() {
		target, _ := s.pickWorkerQueue(env)
		if target == nil {
			b.Fatal("pickWorkerQueue returned no target")
		}
	}
}
