package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// nonFlusherWriter is an http.ResponseWriter that deliberately does NOT
// implement http.Flusher. statusRecorder.Flush must tolerate wrapping such a
// target (guarded type assertion → no-op, no panic).
type nonFlusherWriter struct{ header http.Header }

func (n *nonFlusherWriter) Header() http.Header {
	if n.header == nil {
		n.header = http.Header{}
	}
	return n.header
}
func (n *nonFlusherWriter) Write(b []byte) (int, error) { return len(b), nil }
func (n *nonFlusherWriter) WriteHeader(int)             {}

// TestStatusRecorder_PreservesFlusher is the regression guard for the SSE
// outage: MetricsMiddleware wraps every response in statusRecorder, and if the
// wrapper hides http.Flusher every streaming handler 500s or datastar.NewSSE
// panics. The wrapper must satisfy http.Flusher and Unwrap back to its target.
func TestStatusRecorder_PreservesFlusher(t *testing.T) {
	rec := httptest.NewRecorder() // implements http.Flusher
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}

	_, isFlusher := any(sr).(http.Flusher)
	require.True(t, isFlusher, "statusRecorder must satisfy http.Flusher")

	unwrapper, ok := any(sr).(interface{ Unwrap() http.ResponseWriter })
	require.True(t, ok, "statusRecorder must support Unwrap for http.NewResponseController")
	require.Equal(t, http.ResponseWriter(rec), unwrapper.Unwrap())

	// Flush must reach the wrapped flusher without panicking.
	require.NotPanics(t, func() { sr.Flush() })
	require.True(t, rec.Flushed)
}

// TestStatusRecorder_FlushTolerablesNonFlusher ensures Flush on a wrapper whose
// target is not a flusher is a safe no-op (no panic), matching the guarded
// type-assertion in the implementation.
func TestStatusRecorder_FlushTolerablesNonFlusher(t *testing.T) {
	sr := &statusRecorder{ResponseWriter: &nonFlusherWriter{}, status: http.StatusOK}
	require.NotPanics(t, func() { sr.Flush() })
}

func TestStatusRecorder_CapturesFirstStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}

	sr.WriteHeader(http.StatusTeapot)
	sr.WriteHeader(http.StatusInternalServerError) // second call ignored

	require.Equal(t, http.StatusTeapot, sr.status)
	require.Equal(t, http.StatusTeapot, rec.Code)
}

// TestMetricsMiddleware_SSEStillFlushes drives a streaming handler through the
// full MetricsMiddleware wrapper and asserts the flush propagates — the
// end-to-end version of the regression guard above.
func TestMetricsMiddleware_SSEStillFlushes(t *testing.T) {
	h := newTestHandler(t)

	streamed := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f, ok := w.(http.Flusher)
		require.True(t, ok, "handler must see a Flusher through the middleware")
		_, _ = w.Write([]byte("event: ping\n\n"))
		f.Flush()
		streamed = true
	})

	r := httptest.NewRequest(http.MethodGet, "/api/scheduler/events", nil)
	w := httptest.NewRecorder()
	h.MetricsMiddleware(inner).ServeHTTP(w, r)

	require.True(t, streamed)
	require.True(t, w.Flushed)
	require.Contains(t, w.Body.String(), "event: ping")
}

func TestHandleHealth(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.handleHealth(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "ok", w.Body.String())
}

func TestHandleReady(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	h.handleReady(w, r)

	// Store is a live SQLite handle, so Ping succeeds and ready is 200.
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "ok", w.Body.String())
}
