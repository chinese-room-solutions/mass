package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMatchesExt(t *testing.T) {
	tests := []struct {
		name string
		file string
		exts []string
		want bool
	}{
		{"empty exts matches anything", "model.gguf", nil, true},
		{"matching ext", "model.gguf", []string{".gguf"}, true},
		{"non-matching ext", "model.txt", []string{".gguf"}, false},
		{"one of several", "pkg.mass", []string{".gguf", ".mass"}, true},
		{"case insensitive input", "MODEL.GGUF", []string{".gguf"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, matchesExt(strings.ToLower(tt.file), tt.exts))
		})
	}
}

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"single", "a", []string{"a"}},
		{"multiple with spaces", " a , b ,c", []string{"a", "b", "c"}},
		{"drops blanks", "a,,b,", []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseCSV(tt.in))
		})
	}
}

func TestCompactDuration(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want string
	}{
		{"whole hour trims to h", time.Hour, "1h"},
		{"hour and minutes", 90 * time.Minute, "1h30m"},
		{"minutes only", 30 * time.Minute, "30m"},
		{"seconds only", 45 * time.Second, "45s"},
		{"minutes and seconds", 2*time.Minute + 30*time.Second, "2m30s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, compactDuration(tt.in))
		})
	}
}

func TestJoinTokenExpiryNote(t *testing.T) {
	// Round-tripping through a unix expiry ~1h out yields the compact form.
	got := joinTokenExpiryNote(time.Now().Add(time.Hour).Unix())
	require.Equal(t, "valid for 1h", got)
	require.Equal(t, "expired", joinTokenExpiryNote(time.Now().Add(-time.Minute).Unix()))
}

func TestLogLevelString(t *testing.T) {
	h := newTestHandler(t)
	// Round-trips through the same MarshalText the Settings <sl-select> reads.
	require.NotEmpty(t, logLevelString(h.cfg.Logger.Level))
}

func TestHandleBrowseFiles_ListsDirWithExtFilter(t *testing.T) {
	h := newTestHandler(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.mass"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.txt"), []byte("x"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o755))

	r := httptest.NewRequest(http.MethodGet, "/api/v1/browse?dir="+dir+"&ext=.mass", nil)
	w := httptest.NewRecorder()
	h.handleBrowseFiles(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var entries []browseEntry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &entries))

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	require.True(t, names[".."], "should include parent entry")
	require.True(t, names["sub"], "directories always listed")
	require.True(t, names["a.mass"], "matching ext listed")
	require.False(t, names["b.txt"], "non-matching ext filtered out")
}

func TestHandleBrowseFiles_BadDirIs400(t *testing.T) {
	h := newTestHandler(t)
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	r := httptest.NewRequest(http.MethodGet, "/api/v1/browse?dir="+missing, nil)
	w := httptest.NewRecorder()
	h.handleBrowseFiles(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBrowseRoots(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/browse/roots", nil)
	w := httptest.NewRecorder()
	h.handleBrowseRoots(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var roots []browseRoot
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &roots))
	require.NotEmpty(t, roots, "at least one filesystem root")
}

func TestHandleSettingsSave_MutatesConfig(t *testing.T) {
	h := newTestHandler(t)

	// Saving a log level applies it to the global zerolog level immediately;
	// restore it after so the change doesn't leak into other tests.
	prevLevel := zerolog.GlobalLevel()
	t.Cleanup(func() { zerolog.SetGlobalLevel(prevLevel) })

	body := `{
		"listenAddr": ":9999",
		"resultTTL": "12h",
		"idleEvictionTTL": "45s",
		"loadAttempts": 3,
		"registryURL": "https://reg.example.com",
		"logLevel": "debug",
		"devMode": true,
		"tlsEnabled": true,
		"tlsCertFile": "/tmp/cert.pem"
	}`
	r := httptest.NewRequest(http.MethodPost, "/api/settings", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.handleSettingsSave(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, ":9999", h.cfg.ListenAddr)
	require.Equal(t, "12h", h.cfg.ResultTTL)
	require.Equal(t, "45s", h.cfg.IdleEvictionTTL)
	require.Equal(t, 3, h.cfg.LoadAttempts)
	require.Equal(t, "https://reg.example.com", h.cfg.RegistryURL)
	require.True(t, h.cfg.DevMode)
	require.True(t, h.cfg.TLS.Enabled)
	require.Equal(t, "/tmp/cert.pem", h.cfg.TLS.CertFile)
	require.Equal(t, "debug", logLevelString(h.cfg.Logger.Level))
	// The level was applied at runtime, not just stored on the config.
	require.Equal(t, zerolog.DebugLevel, zerolog.GlobalLevel())
}

func TestHandleSettingsSave_BadBodyIs400(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/api/settings", strings.NewReader("{not json"))
	w := httptest.NewRecorder()
	h.handleSettingsSave(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSettingsSave_AuthTokenSetsHash(t *testing.T) {
	h := newTestHandler(t)
	require.Empty(t, h.authHash)

	body := `{"authToken": "hunter2"}`
	r := httptest.NewRequest(http.MethodPost, "/api/settings", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.handleSettingsSave(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotEmpty(t, h.authHash, "non-empty token must set an auth hash")

	// Persisted to the store so it survives restart.
	stored, err := h.store.GetSetting("auth_token")
	require.NoError(t, err)
	require.NotEmpty(t, stored)
}

func TestHandleToggleDevice_ValidationPaths(t *testing.T) {
	h := newTestHandler(t)

	tests := []struct {
		name       string
		workerID   string
		deviceID   string
		wantStatus int
	}{
		{"missing worker id", "", "gpu0", http.StatusBadRequest},
		{"missing device id", "w1", "", http.StatusBadRequest},
		{"unknown worker", "w1", "gpu0", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/workers/x/devices/y/toggle", nil)
			r.SetPathValue("id", tt.workerID)
			r.SetPathValue("devID", tt.deviceID)
			w := httptest.NewRecorder()
			h.handleToggleDevice(w, r)
			require.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

func TestHandleToggleWorker_ValidationPaths(t *testing.T) {
	h := newTestHandler(t)

	tests := []struct {
		name       string
		workerID   string
		wantStatus int
	}{
		{"missing worker id", "", http.StatusBadRequest},
		{"unknown worker", "w1", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/workers/x/toggle", nil)
			r.SetPathValue("id", tt.workerID)
			w := httptest.NewRecorder()
			h.handleToggleWorker(w, r)
			require.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

func TestHandleRuntimeAutoStartToggle_UnknownIs404(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/api/runtimes/nope/auto-start", nil)
	r.SetPathValue("kind", "nope")
	w := httptest.NewRecorder()
	h.handleRuntimeAutoStartToggle(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// computeEnabledDevices maps persisted toggle rows onto the explicit
// three-state whitelist: no rows → all, some rows → exact enabled subset,
// everything disabled → all=false with an empty set (previously
// unrepresentable — the old bare-list encoding flipped it to all-enabled).
func TestComputeEnabledDevices_ThreeStates(t *testing.T) {
	devices := []stats.Device{{ID: "gpu0"}, {ID: "gpu1"}, {ID: "cpu0"}}
	tests := []struct {
		name    string
		rows    map[string]bool // persisted toggles (deviceID → enabled)
		wantAll bool
		wantIDs []string
	}{
		{
			name:    "no rows: all enabled",
			rows:    nil,
			wantAll: true,
		},
		{
			name:    "one disabled: exact subset, absent rows default enabled",
			rows:    map[string]bool{"gpu1": false},
			wantIDs: []string{"gpu0", "cpu0"},
		},
		{
			name:    "every device disabled: explicit none",
			rows:    map[string]bool{"gpu0": false, "gpu1": false, "cpu0": false},
			wantIDs: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			for id, enabled := range tt.rows {
				require.NoError(t, h.store.SetWorkerDeviceEnabled("worker-x", id, enabled))
			}
			got := h.computeEnabledDevices("worker-x", devices)
			require.Equal(t, tt.wantAll, got.All)
			if tt.wantAll {
				return
			}
			require.ElementsMatch(t, tt.wantIDs, got.IDs)
		})
	}
}

func TestHandleDashboard_RendersWithoutPanic(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	require.NotPanics(t, func() { h.handleDashboard(w, r) })
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "text/html")
	require.Contains(t, strings.ToLower(w.Body.String()), "<!doctype html>")
}

func TestHandleLoginPage_Renders(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/login", nil)
	w := httptest.NewRecorder()

	require.NotPanics(t, func() { h.handleLoginPage(w, r) })
	require.Contains(t, strings.ToLower(w.Body.String()), "<!doctype html>")
	require.Contains(t, w.Body.String(), "Auth Token")
}
