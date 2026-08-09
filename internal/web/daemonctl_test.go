package web

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// loopbackReq builds a request that presents a loopback RemoteAddr, the way a
// real local launcher's connection would.
func loopbackReq(method, target string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = "127.0.0.1:54321"
	return r
}

func TestDaemonPing(t *testing.T) {
	h := newTestHandler(t)
	h.version = "1.2.3"
	h.onDemand = true

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, loopbackReq(http.MethodGet, "/internal/daemon/ping"))
	require.Equal(t, http.StatusOK, rec.Code)

	var ping DaemonPing
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ping))
	require.Equal(t, "1.2.3", ping.Version)
	require.True(t, ping.OnDemand)
}

// TestDaemonControlLoopbackOnly: the control surface manages the daemon on its
// own host; a remote caller is refused whatever it presents.
func TestDaemonControlLoopbackOnly(t *testing.T) {
	h := newTestHandler(t)
	h.SetShutdownFunc(func() {})
	for _, tt := range []struct {
		method, path string
	}{
		{http.MethodGet, "/internal/daemon/ping"},
		{http.MethodPost, "/internal/daemon/shutdown"},
		{http.MethodGet, "/internal/gui/channel"},
	} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(tt.method, tt.path, nil)
		r.RemoteAddr = "192.0.2.7:1000" // httptest's non-loopback default range
		h.ServeHTTP(rec, r)
		require.Equal(t, http.StatusForbidden, rec.Code, "%s %s", tt.method, tt.path)
	}
}

func TestDaemonShutdown(t *testing.T) {
	h := newTestHandler(t)

	// Not wired (no serve loop owns the process): refuse rather than pretend.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, loopbackReq(http.MethodPost, "/internal/daemon/shutdown"))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	fired := make(chan struct{})
	h.SetShutdownFunc(func() { close(fired) })
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, loopbackReq(http.MethodPost, "/internal/daemon/shutdown"))
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"stopping"}`, rec.Body.String())
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("the shutdown func was not called")
	}
}

// TestDaemonControlAuth: with an operator token set, ping and the GUI channel
// stay reachable for the tokenless local launcher, while shutdown demands the
// token.
func TestDaemonControlAuth(t *testing.T) {
	h := newTestHandler(t)
	h.SetShutdownFunc(func() {})
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	h.SetAuthHash(hash)
	authed := h.AuthMiddleware(h)

	rec := httptest.NewRecorder()
	authed.ServeHTTP(rec, loopbackReq(http.MethodGet, "/internal/daemon/ping"))
	require.Equal(t, http.StatusOK, rec.Code, "ping must not need the operator token")

	rec = httptest.NewRecorder()
	authed.ServeHTTP(rec, loopbackReq(http.MethodPost, "/internal/daemon/shutdown"))
	require.Equal(t, http.StatusSeeOther, rec.Code, "tokenless shutdown must be refused")

	rec = httptest.NewRecorder()
	r := loopbackReq(http.MethodPost, "/internal/daemon/shutdown")
	r.Header.Set("Authorization", "Bearer secret")
	authed.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestGUIChannel: the window receives the current theme base on attach and a
// broadcast when the theme changes.
func TestGUIChannel(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/gui/channel")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	sc := bufio.NewScanner(resp.Body)
	readEvent := func() (name, data string) {
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if name != "" {
					return name, data
				}
			case strings.HasPrefix(line, "event:"):
				name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		t.Fatal("the channel closed before delivering an event")
		return "", ""
	}

	name, data := readEvent()
	require.Equal(t, "theme", name)
	require.Equal(t, "dark", data, "the attach event carries the configured base")

	h.gui.broadcast("light")
	name, data = readEvent()
	require.Equal(t, "theme", name)
	require.Equal(t, "light", data)
}
