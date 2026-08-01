// Package metrics exposes Prometheus collectors for the MASS coordinator.
// Labels are bounded by `runtime` (a handful) and never by `model` or
// `worker_id`, which scale with library/fleet size.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(
		mHTTPRequests,
		mHTTPDuration,
		mWorkersRegistered,
		mQueueDepth,
		mJobsInflight,
		mJobsDispatched,
		mDispatchLatency,
		mWorkerDisconnects,
	)
}

var (
	mHTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mass_http_requests_total",
			Help: "HTTP requests handled by the MASS web server.",
		},
		[]string{"method", "path", "status"},
	)

	mHTTPDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mass_http_request_duration_seconds",
			Help:    "HTTP request duration distribution.",
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"method", "path"},
	)

	mWorkersRegistered = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mass_workers_registered",
			Help: "Number of workers currently registered with MASS.",
		},
		[]string{"runtime"},
	)

	mQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mass_queue_depth",
			Help: "Pending rows in scheduler queues. queue=global is unplaced; queue=worker is the sum across all worker queues.",
		},
		[]string{"queue"},
	)

	mJobsInflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mass_jobs_inflight",
			Help: "Jobs currently executing on a worker, by runtime.",
		},
		[]string{"runtime"},
	)

	mJobsDispatched = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mass_jobs_dispatched_total",
			Help: "Jobs that reached a terminal state. outcome=ok|error|cancelled.",
		},
		[]string{"runtime", "outcome"},
	)

	mDispatchLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mass_dispatch_latency_seconds",
			Help:    "Time from Submit to first chunk forwarded to the caller.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
		[]string{"runtime"},
	)

	mWorkerDisconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mass_worker_disconnects_total",
			Help: "Worker disconnect events, by runtime.",
		},
		[]string{"runtime"},
	)
)

// HTTPRequest records one finished HTTP request.
func HTTPRequest(method, path, status string, seconds float64) {
	mHTTPRequests.WithLabelValues(method, path, status).Inc()
	mHTTPDuration.WithLabelValues(method, path).Observe(seconds)
}

// WorkersRegistered sets the gauge for `runtime` to n. Call from a single
// reconciliation site to avoid races.
func WorkersRegistered(runtime string, n int) {
	mWorkersRegistered.WithLabelValues(runtime).Set(float64(n))
}

// QueueDepth sets the pending-row gauge for queue (global|worker).
func QueueDepth(queue string, n int) {
	mQueueDepth.WithLabelValues(queue).Set(float64(n))
}

// JobsInflight sets the inflight gauge for runtime.
func JobsInflight(runtime string, n int) {
	mJobsInflight.WithLabelValues(runtime).Set(float64(n))
}

// JobDispatched records a terminal outcome (ok|error|cancelled).
func JobDispatched(runtime, outcome string) {
	mJobsDispatched.WithLabelValues(runtime, outcome).Inc()
}

// DispatchLatency observes the Submit → first-chunk wall-clock seconds.
func DispatchLatency(runtime string, seconds float64) {
	mDispatchLatency.WithLabelValues(runtime).Observe(seconds)
}

// WorkerDisconnect records one worker disconnect event.
func WorkerDisconnect(runtime string) {
	mWorkerDisconnects.WithLabelValues(runtime).Inc()
}
