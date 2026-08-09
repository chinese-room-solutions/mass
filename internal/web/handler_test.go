package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestSplitModelID(t *testing.T) {
	tests := []struct {
		name            string
		id              string
		wantFilename    string
		wantFingerprint string
	}{
		{"with fingerprint", "model.gguf#abc123", "model.gguf", "abc123"},
		{"no fingerprint", "model.gguf", "model.gguf", ""},
		{"empty", "", "", ""},
		{"trailing hash only", "model#", "model", ""},
		{"multiple hashes splits on last", "a#b#c", "a#b", "c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename, fingerprint := splitModelID(tt.id)
			require.Equal(t, tt.wantFilename, filename)
			require.Equal(t, tt.wantFingerprint, fingerprint)
		})
	}
}

func TestFlattenHeaders(t *testing.T) {
	in := http.Header{
		"Content-Type":  {"application/json"},
		"X-Multi":       {"a", "b", "c"},
		"Authorization": {"Bearer tok"},
	}
	out := flattenHeaders(in)
	require.Equal(t, "application/json", out["Content-Type"])
	require.Equal(t, "a,b,c", out["X-Multi"])
	require.Equal(t, "Bearer tok", out["Authorization"])
}

func TestExpectedClientDisconnect(t *testing.T) {
	require.False(t, expectedClientDisconnect(nil))
	require.True(t, expectedClientDisconnect(context.Canceled))
	require.True(t, expectedClientDisconnect(context.DeadlineExceeded))
	require.False(t, expectedClientDisconnect(errors.New("some other error")))
}

func TestActorFromRequest(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	require.Equal(t, "operator", actorFromRequest(r))

	r.Header.Set("X-Mass-Source", "module:playground")
	require.Equal(t, "module:playground", actorFromRequest(r))
}

func TestRemoteAddr(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{"host:port", "192.0.2.1:54321", "192.0.2.1"},
		{"no port", "192.0.2.1", "192.0.2.1"},
		{"ipv6", "[::1]:8080", "::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tt.remoteAddr
			require.Equal(t, tt.want, remoteAddr(r))
		})
	}
}

func TestEscapeHTML(t *testing.T) {
	require.Equal(t, "&lt;script&gt;", escapeHTML("<script>"))
	require.Equal(t, "a &amp; b", escapeHTML("a & b"))
	require.Equal(t, "plain", escapeHTML("plain"))
}

// panicDevicer is a WorkerInterface whose Devices() panics — safeDevices must
// swallow it so one misbehaving worker can't take down the dashboard render.
type panicDevicer struct {
	worker.WorkerInterface
}

func (panicDevicer) Devices() []stats.Device { panic("boom") }

func TestSafeDevices_RecoversPanic(t *testing.T) {
	require.NotPanics(t, func() {
		got := safeDevices(panicDevicer{})
		require.Nil(t, got)
	})
}
