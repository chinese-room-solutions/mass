package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// fakeDaemon serves the daemon control surface the launcher probes: a ping
// with the given identity, and a shutdown endpoint answering shutdownStatus.
func fakeDaemon(t *testing.T, pingBody string, shutdownStatus int, shutdowns *atomic.Int32) daemonEndpoint {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/daemon/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(pingBody))
	})
	mux.HandleFunc("POST /internal/daemon/shutdown", func(w http.ResponseWriter, _ *http.Request) {
		shutdowns.Add(1)
		w.WriteHeader(shutdownStatus)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return daemonEndpoint{base: srv.URL, client: srv.Client()}
}

func TestProbeDaemon(t *testing.T) {
	t.Run("alive", func(t *testing.T) {
		var n atomic.Int32
		ep := fakeDaemon(t, `{"version":"9.9","on_demand":true}`, http.StatusOK, &n)
		ping, state, err := probeDaemon(context.Background(), ep)
		require.NoError(t, err)
		require.Equal(t, daemonAlive, state)
		require.Equal(t, "9.9", ping.Version)
		require.True(t, ping.OnDemand)
	})

	t.Run("down", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		ep := daemonEndpoint{base: srv.URL, client: srv.Client()}
		srv.Close() // nothing listens any more.
		_, state, err := probeDaemon(context.Background(), ep)
		require.NoError(t, err)
		require.Equal(t, daemonDown, state)
	})

	t.Run("foreign status", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		t.Cleanup(srv.Close)
		ep := daemonEndpoint{base: srv.URL, client: srv.Client()}
		_, state, err := probeDaemon(context.Background(), ep)
		require.Equal(t, daemonForeign, state)
		require.ErrorContains(t, err, "not a MASS daemon")
	})

	t.Run("foreign body", func(t *testing.T) {
		var n atomic.Int32
		ep := fakeDaemon(t, `<html>hello</html>`, http.StatusOK, &n)
		_, state, err := probeDaemon(context.Background(), ep)
		require.Equal(t, daemonForeign, state)
		require.Error(t, err)
	})
}

// TestEnsureDaemonAttachAndReplace covers the decision matrix that ends in an
// attach (no spawn): same build, a skewed but operator-managed daemon, and a
// skewed on-demand daemon that refuses the shutdown. The replace-then-spawn
// path execs a real process, so it stays out of unit scope.
func TestEnsureDaemonAttachAndReplace(t *testing.T) {
	origVersion := version
	t.Cleanup(func() { version = origVersion })
	version = "2.0"

	tests := []struct {
		name           string
		ping           string
		shutdownStatus int
		wantShutdowns  int32
	}{
		{"same build attaches", `{"version":"2.0","on_demand":true}`, http.StatusOK, 0},
		{"skewed operator daemon is left alone", `{"version":"1.0","on_demand":false}`, http.StatusOK, 0},
		{"skewed on-demand daemon that refuses to stop is attached", `{"version":"1.0","on_demand":true}`, http.StatusUnauthorized, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var shutdowns atomic.Int32
			ep := fakeDaemon(t, tt.ping, tt.shutdownStatus, &shutdowns)
			require.NoError(t, ensureDaemon(context.Background(), ep, zerolog.Nop()))
			require.Equal(t, tt.wantShutdowns, shutdowns.Load())
		})
	}
}

func TestEnsureDaemonForeignAddressFails(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	ep := daemonEndpoint{base: srv.URL, client: srv.Client()}
	err := ensureDaemon(context.Background(), ep, zerolog.Nop())
	require.ErrorContains(t, err, "not a MASS daemon")
}

// TestShouldEnsureLocalDaemon: only a verb aimed at the local config's
// address may boot a daemon — an explicit --addr or $MASS_ADDR names a
// specific server.
func TestShouldEnsureLocalDaemon(t *testing.T) {
	newFS := func(withAddr bool, args ...string) *flag.FlagSet {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		if withAddr {
			fs.String("addr", "http://127.0.0.1:3455", "")
		}
		require.NoError(t, fs.Parse(args))
		return fs
	}

	t.Run("default addr spawns", func(t *testing.T) {
		require.True(t, shouldEnsureLocalDaemon(newFS(true)))
	})
	t.Run("explicit --addr does not", func(t *testing.T) {
		require.False(t, shouldEnsureLocalDaemon(newFS(true, "--addr", "http://other:1")))
	})
	t.Run("MASS_ADDR does not", func(t *testing.T) {
		t.Setenv("MASS_ADDR", "http://other:1")
		require.False(t, shouldEnsureLocalDaemon(newFS(true)))
	})
	t.Run("local verb does not", func(t *testing.T) {
		require.False(t, shouldEnsureLocalDaemon(newFS(false)))
	})
}
