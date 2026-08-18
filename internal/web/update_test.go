package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// releaseServer stands in for GitHub: /releases/latest redirects to the tag,
// which is the whole of what selfupdate.Latest reads.
func releaseServer(t *testing.T, tag string) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /releases/latest", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/releases/tag/"+tag, http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// deadServer is a URL nothing answers on — the offline machine.
func deadServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	return url
}

// seedUpdate makes the handler know tag is available, by taking one real check
// against a release server that publishes it.
func seedUpdate(t *testing.T, h *Handler, current, tag string) {
	t.Helper()
	h.version = current
	h.update.Version = current
	h.update.BaseURL = releaseServer(t, tag)
	st, err := h.update.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, tag, st.Available)
}

// stubFetch stands in for the verified download: no network, no installer.
type stubFetch struct {
	err     error
	fetched bool
	// setup is the stand-in installer's contents. Empty means the fetch only
	// reports a path, which is enough for every case that never runs it.
	setup string
}

func (s *stubFetch) fn(_ context.Context, _, _, _, destDir string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	s.fetched = true
	path := filepath.Join(destDir, "mass-setup")
	if s.setup != "" {
		if err := os.WriteFile(path, []byte(s.setup), 0o700); err != nil {
			return "", err
		}
	}
	return path, nil
}

// noInstallRecord stands in for a MASS no installer placed.
func noInstallRecord() (*install.Record, error) { return nil, nil }

// TestUpdateCheckNow covers the live check the operator asks for: the answer is
// recorded, and a repository it can't reach is reported in the body at 200
// rather than as an HTTP failure — "couldn't ask" must not read as "up to date".
func TestUpdateCheckNow(t *testing.T) {
	tests := []struct {
		name      string
		version   string
		published string
		offline   bool
		wantAvail string
		wantErr   bool
	}{
		{
			name:      "a newer release is found",
			version:   "0.4.0",
			published: "v0.5.0",
			wantAvail: "v0.5.0",
		},
		{
			name:      "an equal release is not",
			version:   "0.5.0",
			published: "v0.5.0",
		},
		{
			name:      "a build from source never has an update",
			version:   "dev",
			published: "v0.5.0",
		},
		{
			name:    "an unreachable repository is reported, not swallowed",
			version: "0.4.0",
			offline: true,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			h.version = tt.version
			h.update.Version = tt.version
			if tt.offline {
				h.update.BaseURL = deadServer(t)
			} else {
				h.update.BaseURL = releaseServer(t, tt.published)
			}

			rec := httptest.NewRecorder()
			h.handleUpdateCheckNow(rec, httptest.NewRequest(http.MethodPost, "/api/update/check", nil))
			require.Equal(t, http.StatusOK, rec.Code)

			var got UpdateCheckResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, tt.version, got.Version)
			require.Equal(t, tt.wantAvail, got.Available)
			require.NotZero(t, got.CheckedAt, "the answer says when it was taken")
			if tt.wantErr {
				require.NotEmpty(t, got.Err)
			} else {
				require.Empty(t, got.Err)
			}

			// The cached read agrees with the check that just ran.
			cached := httptest.NewRecorder()
			h.handleUpdateCheck(cached, httptest.NewRequest(http.MethodGet, "/api/update/check", nil))
			require.Equal(t, http.StatusOK, cached.Code)
			require.JSONEq(t, rec.Body.String(), cached.Body.String())
		})
	}
}

// TestHandleUpdateCheck pins the cached read: cheap, and honest about never
// having asked.
func TestHandleUpdateCheck(t *testing.T) {
	t.Run("nothing checked yet", func(t *testing.T) {
		h := newTestHandler(t)
		h.version = "0.4.0"
		h.update.Version = "0.4.0"

		rec := httptest.NewRecorder()
		h.handleUpdateCheck(rec, httptest.NewRequest(http.MethodGet, "/api/update/check", nil))
		require.Equal(t, http.StatusOK, rec.Code)

		var got UpdateCheckResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, "0.4.0", got.Version)
		require.Empty(t, got.Available)
		require.Empty(t, got.Err)
		require.Zero(t, got.CheckedAt)
		require.Zero(t, got.Incompatible)
	})

	t.Run("a tag a check found", func(t *testing.T) {
		h := newTestHandler(t)
		seedUpdate(t, h, "0.4.0", "v0.5.0")

		rec := httptest.NewRecorder()
		h.handleUpdateCheck(rec, httptest.NewRequest(http.MethodGet, "/api/update/check", nil))
		require.Equal(t, http.StatusOK, rec.Code)

		var got UpdateCheckResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, "v0.5.0", got.Available)
		require.Zero(t, got.Incompatible)
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

func TestHandleUpdateApply(t *testing.T) {
	tests := []struct {
		name       string
		available  bool
		onDemand   bool
		body       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "nothing to install",
			onDemand:   true,
			wantStatus: http.StatusConflict,
			wantBody:   "no MASS update is available",
		},
		{
			name:       "an operator-managed daemon refuses to restart itself",
			available:  true,
			onDemand:   false,
			wantStatus: http.StatusConflict,
			wantBody:   "operator-managed server",
		},
		{
			name:       "no install record",
			available:  true,
			onDemand:   true,
			wantStatus: http.StatusConflict,
			wantBody:   "wasn't installed by the MASS installer",
		},
		{
			name:       "an empty body means no force",
			available:  true,
			onDemand:   true,
			body:       "",
			wantStatus: http.StatusConflict,
			wantBody:   "wasn't installed by the MASS installer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			if tt.available {
				seedUpdate(t, h, "0.4.0", "v0.5.0")
			}
			h.onDemand = tt.onDemand
			// This machine may or may not have MASS installed; the tests here
			// are about the refusals, so say so explicitly.
			h.applier.LoadRecord = noInstallRecord
			h.applier.FetchSetup = (&stubFetch{}).fn

			req := httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader(tt.body))
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
		if runtime.GOOS == "windows" {
			// Windows carries no mode bits on a directory: Mkdir(0o500) leaves
			// it writable, so the probe succeeds and there is no refusal to
			// assert. Denying the write needs an ACL, which is a bigger prop
			// than the refusal is worth. The refusal itself is OS-neutral.
			t.Skip("directory mode bits are not enforced on Windows")
		}
		dir := filepath.Join(t.TempDir(), "readonly")
		require.NoError(t, os.Mkdir(dir, 0o500))
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

		h := newTestHandler(t)
		h.onDemand = true
		seedUpdate(t, h, "0.4.0", "v0.5.0")
		h.applier.FetchSetup = (&stubFetch{}).fn
		h.applier.LoadRecord = func() (*install.Record, error) { return &install.Record{InstallDir: dir}, nil }

		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "administrator rights")
	})

	// A download that fails is a fault, not a refusal: 500, with the reason.
	t.Run("a failed download is a server error", func(t *testing.T) {
		h := newTestHandler(t)
		h.onDemand = true
		seedUpdate(t, h, "0.4.0", "v0.5.0")
		h.applier.LoadRecord = func() (*install.Record, error) {
			return &install.Record{InstallDir: t.TempDir()}, nil
		}
		h.applier.FetchSetup = (&stubFetch{err: errors.New("offline")}).fn

		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Contains(t, rec.Body.String(), "offline")
	})
}

// TestUpdateApplyNotifiesTheWindow proves the daemon tells the attached window
// before it retires. Without that event the window would just reconnect to the
// relaunched build's daemon on the same port and the user would be left with
// two MASS windows.
func TestUpdateApplyNotifiesTheWindow(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stand-in installer is a shell script")
	}
	installDir := t.TempDir()
	h := newTestHandler(t)
	h.onDemand = true
	seedUpdate(t, h, "0.4.0", "v0.5.0")
	h.applier.LoadRecord = func() (*install.Record, error) { return &install.Record{InstallDir: installDir}, nil }
	// A stand-in installer that exits at once: the apply only has to be able to
	// start it, and the event under test is sent after that.
	h.applier.FetchSetup = (&stubFetch{setup: "#!/bin/sh\nexit 0\n"}).fn

	ch := h.gui.subscribe()
	defer h.gui.unsubscribe(ch)

	var shutdown atomic.Bool
	h.SetShutdownFunc(func() { shutdown.Store(true) })

	rec := httptest.NewRecorder()
	h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	select {
	case ev := <-ch:
		require.Equal(t, GUIEventUpdateRestarting, ev.name)
		require.Equal(t, "v0.5.0", ev.data, "the event carries the incoming tag")
	default:
		t.Fatal("the daemon retired without telling the window")
	}

	// The shutdown is deliberately delayed so that event gets down the stream
	// before Shutdown closes it.
	require.False(t, shutdown.Load(), "the shutdown must not pre-empt the notice")
	require.Eventually(t, shutdown.Load, 5*time.Second, 20*time.Millisecond)
}

// TestUpdateApplyFleetGate proves the gate runs before anything is downloaded,
// and that force is the only way past it.
func TestUpdateApplyFleetGate(t *testing.T) {
	// The stranding pair from the fixture: worker 0.2.0 wants MASS ">=0.2",
	// which the 0.1.5 candidate does not satisfy.
	newGated := func(t *testing.T) (*Handler, *stubFetch) {
		t.Helper()
		h := newTestHandler(t)
		h.onDemand = true
		seedUpdate(t, h, "0.1.0", "0.1.5")
		fetch := &stubFetch{}
		h.applier.FetchSetup = fetch.fn
		h.applier.LoadRecord = noInstallRecord
		serveCompatIndex(t, h)
		require.NoError(t, h.workers.Register(worker.NewStreamWorker("w1",
			&workerpb.WorkerRegister{Name: "w1", RuntimeName: "test-rt", Version: "0.2.0"},
			nil, "", "", true, zerolog.Nop())))
		return h, fetch
	}

	t.Run("an incompatible fleet refuses with the count", func(t *testing.T) {
		h, fetch := newGated(t)
		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply", strings.NewReader("{}")))

		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "1 connected worker")
		require.Contains(t, rec.Body.String(), "test-rt 0.2.0")
		require.False(t, fetch.fetched, "the gate must run before anything is downloaded")
	})

	t.Run("force gets past the gate and on to the next refusal", func(t *testing.T) {
		h, fetch := newGated(t)
		rec := httptest.NewRecorder()
		h.handleUpdateApply(rec, httptest.NewRequest(http.MethodPost, "/api/update/apply",
			strings.NewReader(`{"force":true}`)))

		// No install record in a test environment, so the apply stops there —
		// past the gate, which is what this asserts.
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Contains(t, rec.Body.String(), "wasn't installed by the MASS installer")
		require.False(t, fetch.fetched)
	})
}
