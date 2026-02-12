package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(
		mRequestsReceived,
		mRequestDuration,
		mInferenceDuration,
		mActiveRequests,
		mModelLoaded,
	)
}

var (
	mRequestsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mass_requests_received_total",
			Help: "Total number of received requests.",
		},
		[]string{"method"},
	)

	mRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mass_request_duration_seconds",
			Help:    "Request duration distribution.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
		},
		[]string{"method"},
	)

	mInferenceDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mass_inference_duration_seconds",
			Help:    "Inference (Predict) duration distribution.",
			Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"model"},
	)

	mActiveRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mass_active_requests",
			Help: "Number of currently active inference requests.",
		},
		[]string{"model"},
	)

	mModelLoaded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mass_model_loaded",
			Help: "Whether model is loaded (1) or not (0).",
		},
		[]string{"model"},
	)
)

func RequestReceived(method string) {
	mRequestsReceived.WithLabelValues(method).Inc()
}

func RequestDuration(method string, seconds float64) {
	mRequestDuration.WithLabelValues(method).Observe(seconds)
}

func InferenceDuration(model string, seconds float64) {
	mInferenceDuration.WithLabelValues(model).Observe(seconds)
}

func ActiveRequestsInc(model string) {
	mActiveRequests.WithLabelValues(model).Inc()
}

func ActiveRequestsDec(model string) {
	mActiveRequests.WithLabelValues(model).Dec()
}

func ModelLoaded(model string) {
	mModelLoaded.WithLabelValues(model).Set(1)
}
