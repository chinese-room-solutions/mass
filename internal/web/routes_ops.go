package web

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/chinese-room-solutions/mass/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// handleMetrics exposes the Prometheus registry. Auth-bypassed (see auth.go).
func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

// MetricsMiddleware records per-request count + duration histograms.
// Uses the ServeMux's matched pattern as the path label so high-
// cardinality fields (UUIDs in URL, etc.) don't blow up the registry.
// /metrics is excluded so scrape traffic doesn't show up in its own
// histograms.
func (h *Handler) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		_, pattern := h.mux.Handler(r)
		if pattern == "" {
			pattern = "unmatched"
		}
		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		metrics.HTTPRequest(r.Method, pattern, strconv.Itoa(sw.status), time.Since(start).Seconds())
	})
}

// statusRecorder captures the response status for the metrics label.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.NewResponseController reach the underlying writer (used by
// datastar.NewSSE). Flush is also implemented directly so SSE handlers that
// type-assert w.(http.Flusher) keep working through this wrapper — without
// these the wrapper hides http.Flusher and every streaming response either
// 500s ("streaming unsupported") or panics ("feature not supported").
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// handleHealth is a liveness probe — 200 if the process can respond.
// No dependency checks; orchestrators use this to decide on restart.
func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleReady reports whether MASS can serve traffic right now. Checks
// the database is reachable. Worker availability is intentionally NOT
// gated here: workers churn independently and flipping ready=false
// during normal turnover would cause spurious LB traffic loss.
func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if h.store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("store: not initialised"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.store.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("store: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
