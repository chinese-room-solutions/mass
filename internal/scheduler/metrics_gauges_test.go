package scheduler

import (
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// refreshGauges must zero the per-runtime workers-registered and
// jobs-inflight series when the runtime vanishes from the snapshot —
// otherwise the labeled series freezes at its last non-zero value forever
// and operators alerting on 0 never fire.
func TestRefreshGauges_ZeroesVanishedRuntimes(t *testing.T) {
	// Runtime names unique to this test: the Prometheus registry is
	// process-global and shared across the package's tests.
	const rtWorkers = "rt-gauge-workers"
	const rtInflight = "rt-gauge-inflight"

	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w-gauge", rtWorkers,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.inflightMu.Lock()
	s.inflightByRequest["req-gauge"] = inflightRecord{queueName: "worker|w-gauge", runtimeName: rtInflight}
	s.inflightMu.Unlock()

	s.refreshGauges()
	require.Equal(t, 1.0, gaugeValue(t, "mass_workers_registered", rtWorkers))
	require.Equal(t, 1.0, gaugeValue(t, "mass_jobs_inflight", rtInflight))

	// The last worker and the last inflight job of their runtimes vanish.
	require.NoError(t, s.workers.Deregister("w-gauge"))
	s.inflightMu.Lock()
	delete(s.inflightByRequest, "req-gauge")
	s.inflightMu.Unlock()

	s.refreshGauges()
	require.Equal(t, 0.0, gaugeValue(t, "mass_workers_registered", rtWorkers),
		"vanished runtime's workers gauge must read an explicit 0")
	require.Equal(t, 0.0, gaugeValue(t, "mass_jobs_inflight", rtInflight),
		"vanished runtime's inflight gauge must read an explicit 0")
}

// gaugeValue reads one labeled gauge sample from the process-global
// registry the metrics package registers into.
func gaugeValue(t *testing.T, family, runtime string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "runtime" && lp.GetValue() == runtime {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("no %s sample with runtime=%q found", family, runtime)
	return 0
}
