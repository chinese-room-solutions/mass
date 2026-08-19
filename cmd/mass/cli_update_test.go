package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// `mass update` asks the daemon for a live check and says what to do about the
// answer; --apply asks the daemon to install, and the daemon's own 409 sentence
// is what reaches the user. An explicit --addr keeps every case off the local
// daemon.
func TestRunCLIUpdate(t *testing.T) {
	tests := []struct {
		name     string
		routes   map[string]http.HandlerFunc
		args     []string
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{
			name: "up to date",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/check": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"version":"v0.4.0","available":""}`))
				},
			},
			args:     []string{"update"},
			wantCode: exitOK,
			wantOut:  "mass v0.4.0 — up to date\n",
		},
		{
			name: "an update is available",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/check": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"version":"v0.4.0","available":"v0.5.0"}`))
				},
			},
			args:     []string{"update"},
			wantCode: exitOK,
			wantOut:  "v0.5.0 available (run mass update --apply)\n",
		},
		{
			name: "an update that would strand workers warns",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/check": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(
						`{"version":"v0.4.0","available":"v0.5.0","incompatible":2,"names":["llama-cpp 0.2.0"]}`))
				},
			},
			args:     []string{"update"},
			wantCode: exitOK,
			wantOut:  "2 connected workers would be stranded",
		},
		{
			name: "apply reports the installer is running",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/apply": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"status":"updating","version":"v0.5.0"}`))
				},
			},
			args:     []string{"update", "--apply"},
			wantCode: exitOK,
			wantOut:  "installing mass v0.5.0",
		},
		{
			name: "the daemon's refusal is what the user sees",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/apply": func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "v0.5.0 would strand 1 connected worker", http.StatusConflict)
				},
			},
			args:     []string{"update", "--apply"},
			wantCode: exitError,
			wantErr:  "would strand 1 connected worker",
		},
		{
			name: "force is passed through",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/apply": func(w http.ResponseWriter, r *http.Request) {
					var body [64]byte
					n, _ := r.Body.Read(body[:])
					if string(body[:n]) != `{"force":true}` {
						http.Error(w, "force not sent: "+string(body[:n]), http.StatusBadRequest)
						return
					}
					_, _ = w.Write([]byte(`{"status":"updating","version":"v0.5.0"}`))
				},
			},
			args:     []string{"update", "--apply", "--force"},
			wantCode: exitOK,
			wantOut:  "installing mass v0.5.0",
		},
		{
			name: "a check that couldn't reach the release host fails loudly",
			routes: map[string]http.HandlerFunc{
				"POST /api/update/check": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"version":"v0.4.0","error":"dial tcp: no route to host"}`))
				},
			},
			args:     []string{"update"},
			wantCode: exitError,
			wantErr:  "no route to host",
		},
		{
			name:     "positional arguments are a usage error",
			args:     []string{"update", "now"},
			wantCode: exitUsage,
			wantErr:  "usage:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			for pattern, h := range tt.routes {
				mux.HandleFunc(pattern, h)
			}
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			args := append(append([]string{}, tt.args...), "--addr", srv.URL)
			var code int
			out, errOut := capture(t, func() { code = runCLI(args) })

			require.Equal(t, tt.wantCode, code)
			if tt.wantOut != "" {
				require.Contains(t, out, tt.wantOut)
			}
			if tt.wantErr != "" {
				require.Contains(t, errOut, tt.wantErr)
			}
		})
	}
}
