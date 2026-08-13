package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubUpdater stands in for mass-sdk/selfupdate: no network, no installer.
type stubUpdater struct {
	latest    string
	latestErr error
	newer     bool
	fetchErr  error
	fetched   bool
}

func (s *stubUpdater) Latest(context.Context, string) (string, error) {
	return s.latest, s.latestErr
}

func (s *stubUpdater) IsNewer(_, _ string) bool { return s.newer }

func (s *stubUpdater) FetchSetup(_ context.Context, _, _, _, destDir string) (string, error) {
	if s.fetchErr != nil {
		return "", s.fetchErr
	}
	s.fetched = true
	return destDir + "/mass-setup", nil
}

func TestCheckForUpdate(t *testing.T) {
	tests := []struct {
		name string
		up   *stubUpdater
		want string
	}{
		{"a newer release is recorded", &stubUpdater{latest: "v0.5.0", newer: true}, "v0.5.0"},
		{"an equal release is not", &stubUpdater{latest: "v0.4.0", newer: false}, ""},
		{"an unreachable repository is silent", &stubUpdater{latestErr: errors.New("offline")}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			h.updater = tt.up
			h.CheckForUpdate(context.Background())
			require.Equal(t, tt.want, h.update.get())
		})
	}

	t.Run("no updater leaves the surface inert", func(t *testing.T) {
		h := newTestHandler(t)
		h.CheckForUpdate(context.Background())
		require.Empty(t, h.update.get())
	})
}

// TestFleetGate exercises the candidate-aware compat check against the shared
// registry fixture. Its worker rows carry mass ranges: 0.1.0 wants ">=0.1",
// 0.2.0 wants ">=0.2", and 0.3.0/0.4.0/0.5.0 list none at all.
func TestFleetGate(t *testing.T) {
	idx := compatIndex(t)
	tests := []struct {
		name      string
		workers   []workerPairing
		candidate string
		want      int
		wantNames []string
	}{
		{
			name:      "a worker whose mass range excludes the candidate is counted",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "0.2.0"}},
			candidate: "0.1.5",
			want:      1,
			wantNames: []string{"test-rt 0.2.0"},
		},
		{
			name:      "a candidate inside every range strands nobody",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "0.1.0"}, {RuntimeName: "test-rt", Version: "0.2.0"}},
			candidate: "0.9.0",
			want:      0,
		},
		{
			name:      "an unlisted mass range admits everything",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "0.4.0"}},
			candidate: "0.0.1",
			want:      0,
		},
		{
			name:      "one admitting row among several is enough",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "0.5.0"}},
			candidate: "0.0.1",
			want:      0,
		},
		{
			name:      "a worker version the index doesn't list is not counted",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "9.9.9"}},
			candidate: "0.1.0",
			want:      0,
		},
		{
			name:      "a non-semver worker version is not counted",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "dev"}},
			candidate: "0.1.0",
			want:      0,
		},
		{
			name:      "a pre-release candidate is inconclusive, so nobody is counted",
			workers:   []workerPairing{{RuntimeName: "test-rt", Version: "0.2.0"}},
			candidate: "0.1.5-4-gabc123",
			want:      0,
		},
		{
			name:      "a worker of an unknown runtime has no rows",
			workers:   []workerPairing{{RuntimeName: "ghost", Version: "0.2.0"}},
			candidate: "0.1.5",
			want:      0,
		},
		{
			name: "names are capped",
			workers: []workerPairing{
				{RuntimeName: "test-rt", Version: "0.2.0"}, {RuntimeName: "test-rt", Version: "0.2.0"},
				{RuntimeName: "test-rt", Version: "0.2.0"}, {RuntimeName: "test-rt", Version: "0.2.0"},
				{RuntimeName: "test-rt", Version: "0.2.0"}, {RuntimeName: "test-rt", Version: "0.2.0"},
			},
			candidate: "0.1.5",
			want:      6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := fleetGate(idx, tt.workers, tt.candidate)
			require.Equal(t, tt.want, gate.Incompatible)
			require.LessOrEqual(t, len(gate.Names), fleetGateNameCap)
			if tt.wantNames != nil {
				require.Equal(t, tt.wantNames, gate.Names)
			}
		})
	}
}

// TestUpdateFleetGateWithoutIndex pins the cache-only posture: with nothing
// cached the gate is empty rather than an error or a network fetch.
func TestUpdateFleetGateWithoutIndex(t *testing.T) {
	h := newTestHandler(t)
	require.Equal(t, UpdateFleetGate{}, h.updateFleetGate("0.1.0"))
}

func TestHandleUpdateCheck(t *testing.T) {
	tests := []struct {
		name      string
		available string
		wantAvail string
	}{
		{"nothing available", "", ""},
		{"a tag the startup check found", "v0.5.0", "v0.5.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			h.version = "0.4.0"
			h.update.set(tt.available)

			rec := httptest.NewRecorder()
			h.handleUpdateCheck(rec, httptest.NewRequest(http.MethodGet, "/api/update/check", nil))
			require.Equal(t, http.StatusOK, rec.Code)

			var got UpdateCheckResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, "0.4.0", got.Version)
			require.Equal(t, tt.wantAvail, got.Available)
			require.Zero(t, got.Incompatible)
		})
	}
}

// noInstallRecord stands in for a MASS no installer placed.
func noInstallRecord() (*install.Record, error) { return nil, nil }

func TestHandleUpdateApply(t *testing.T) {
	tests := []struct {
		name       string
		available  string
		updater    UpdaterInterface
		onDemand   bool
		body       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "nothing to install",
			updater:    &stubUpdater{},
			onDemand:   true,
			wantStatus: http.StatusConflict,
			wantBody:   "no MASS update is available",
		},
		{
			name:       "no updater wired",
			available:  "v0.5.0",
			onDemand:   true,
			wantStatus: http.StatusConflict,
			wantBody:   "no MASS update is available",
		},
		{
			name:       "an operator-managed daemon refuses to restart itself",
			available:  "v0.5.0",
			updater:    &stubUpdater{},
			onDemand:   false,
			wantStatus: http.StatusConflict,
			wantBody:   "operator-managed server",
		},
		{
			name:       "no install record",
			available:  "v0.5.0",
			updater:    &stubUpdater{},
			onDemand:   true,
			wantStatus: http.StatusConflict,
			wantBody:   "wasn't installed by the MASS installer",
		},
		{
			name:       "an empty body means no force",
			available:  "v0.5.0",
			updater:    &stubUpdater{},
			onDemand:   true,
			body:       "",
			wantStatus: http.StatusConflict,
			wantBody:   "wasn't installed by the MASS installer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			h.update.set(tt.available)
			h.updater = tt.updater
			h.onDemand = tt.onDemand
			// This machine may or may not have MASS installed; the tests below
			// are about the refusals, so say so explicitly.
			h.recordFn = noInstallRecord

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}
			req := httptest.NewRequest(http.MethodPost, "/api/update/apply", body)
			rec := httptest.NewRecorder()
			h.handleUpdateApply(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}

	// A recorded install this process can't rewrite is the "needs admin rights"
	// refusal — probed by attempting the write, not by reading mode bits.
	t.Run("an unwritable install dir needs administrator rights", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can write anywhere, so the probe can't fail")
		}
		dir := filepath.Join(t.TempDir(), "readonly")
		require.NoError(t, os.Mkdir(dir, 0o500))
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

		h := newTestHandler(t)
		h.onDemand = true
		h.updater = &stubUpdater{}
		h.update.set("v0.5.0")
		h.recordFn = func() (*install.Record, error) { return &install.Record{InstallDir: dir}, nil }

		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "administrator rights")
	})
}

// TestUpdateApplyFleetGate proves the gate runs before anything is downloaded,
// and that force is the only way past it.
func TestUpdateApplyFleetGate(t *testing.T) {
	// The stranding pair from the fixture: worker 0.2.0 wants MASS ">=0.2",
	// which the 0.1.5 candidate does not satisfy.
	newGated := func(t *testing.T) (*Handler, *stubUpdater) {
		t.Helper()
		h := newTestHandler(t)
		h.version = "0.1.0"
		h.onDemand = true
		up := &stubUpdater{}
		h.updater = up
		h.recordFn = noInstallRecord
		h.update.set("0.1.5")
		serveCompatIndex(t, h)
		require.NoError(t, h.workers.Register(worker.NewStreamWorker("w1",
			&workerpb.WorkerRegister{Name: "w1", RuntimeName: "test-rt", Version: "0.2.0"},
			nil, "", "", true, zerolog.Nop())))
		return h, up
	}

	t.Run("an incompatible fleet refuses with the count", func(t *testing.T) {
		h, up := newGated(t)
		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))

		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "1 connected worker")
		require.Contains(t, rec.Body.String(), "test-rt 0.2.0")
		require.False(t, up.fetched, "the gate must run before anything is downloaded")
	})

	t.Run("force gets past the gate and on to the next refusal", func(t *testing.T) {
		h, up := newGated(t)
		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply",
			strings.NewReader(`{"force":true}`)))

		// No install record in a test environment, so the apply stops there —
		// past the gate, which is what this asserts.
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "wasn't installed by the MASS installer")
		require.False(t, up.fetched)
	})
}

func TestSetupArgsAndAssetName(t *testing.T) {
	args := setupArgs("/opt/mass")
	require.Equal(t, []string{"--install", "--install-dir", "/opt/mass", "--scope", "system", "--relaunch"}, args)
	require.True(t, strings.HasPrefix(setupAssetName(), "mass-setup_"))
}
