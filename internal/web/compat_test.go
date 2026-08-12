package web

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// compatIndexYAML is the fixture the index-level compat checks read: runtime
// "test-rt" plus two worker packages joined to it, covering an in-range row, a
// row with no ranges at all, a row whose range is garbage, and one version
// listed by both packages with only the second admitting.
const compatIndexYAML = `schema_version: 1
packages:
  - name: test-rt
    kind: runtime
    runtime_name: test-rt
    versions:
      - version: 0.1.0
        mass: ">=0.1"
        artifacts: {}
  - name: test-rt-worker
    kind: worker
    runtime_name: test-rt
    versions:
      - version: 0.1.0
        runtime: ">=0.1 <0.2"
        mass: ">=0.1"
        artifacts: {}
      - version: 0.2.0
        runtime: ">=0.1 <0.3"
        mass: ">=0.2"
        artifacts: {}
      - version: 0.3.0
        runtime: "not-a-range"
        artifacts: {}
      - version: 0.4.0
        artifacts: {}
      - version: 0.5.0
        runtime: ">=9.0"
        artifacts: {}
  - name: test-rt-worker-alt
    kind: worker
    runtime_name: test-rt
    versions:
      - version: 0.5.0
        runtime: ">=0.1"
        artifacts: {}
`

func compatIndex(t *testing.T) *registry.Index {
	t.Helper()
	idx, err := registry.ParseIndex([]byte(compatIndexYAML))
	require.NoError(t, err)
	return idx
}

func TestCheckWorkerIndexCompat(t *testing.T) {
	tests := []struct {
		name           string
		workerVersion  string
		runtimeVersion string
		massVersion    string
		wantErr        []string // substrings the rejection must name
		wantReason     string   // substring of the accept-with-warning reason
	}{
		{
			name:           "row match in range accepts",
			workerVersion:  "0.1.0",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
		},
		{
			name:           "runtime range miss rejects",
			workerVersion:  "0.1.0",
			runtimeVersion: "0.2.0",
			massVersion:    "1.0.0",
			wantErr:        []string{">=0.1 <0.2", "0.2.0", "runtime"},
		},
		{
			name:           "mass range miss rejects",
			workerVersion:  "0.2.0",
			runtimeVersion: "0.2.0",
			massVersion:    "0.1.0",
			wantErr:        []string{">=0.2", "0.1.0", "MASS"},
		},
		{
			name:           "no matching row accepts",
			workerVersion:  "9.9.9",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
			wantReason:     "no test-rt worker version 9.9.9",
		},
		{
			name:           "non-semver worker version accepts",
			workerVersion:  "dev",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
			wantReason:     "not semver",
		},
		{
			name:           "empty worker version accepts",
			workerVersion:  "",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
			wantReason:     "not semver",
		},
		{
			name:           "unparseable range in matched row accepts",
			workerVersion:  "0.3.0",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
			wantReason:     "not a valid semver constraint",
		},
		{
			name:           "row without ranges accepts",
			workerVersion:  "0.4.0",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
		},
		{
			name:           "dev MASS version accepts",
			workerVersion:  "0.1.0",
			runtimeVersion: "0.1.5",
			massVersion:    "dev",
			wantReason:     `installed version "dev" is not semver`,
		},
		{
			name:           "git-describe MASS version accepts",
			workerVersion:  "0.1.0",
			runtimeVersion: "0.1.5",
			massVersion:    "v0.2.1-4-g83b4176",
			wantReason:     `installed version "v0.2.1-4-g83b4176" is a dev/pre-release build`,
		},
		{
			name:           "one admitting row is enough",
			workerVersion:  "0.5.0",
			runtimeVersion: "0.1.5",
			massVersion:    "1.0.0",
		},
	}
	idx := compatIndex(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, err := checkWorkerIndexCompat(idx, "test-rt", tt.workerVersion, tt.runtimeVersion, tt.massVersion)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				for _, want := range tt.wantErr {
					require.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			if tt.wantReason == "" {
				require.Empty(t, reason)
				return
			}
			require.Contains(t, reason, tt.wantReason)
		})
	}
}

func TestCountIncompatibleWorkers(t *testing.T) {
	idx := compatIndex(t)
	workers := []workerPairing{
		{RuntimeName: "test-rt", Version: "0.1.0"}, // runtime >=0.1 <0.2: stranded by 0.2.0
		{RuntimeName: "test-rt", Version: "0.2.0"}, // runtime >=0.1 <0.3: fine
		{RuntimeName: "test-rt", Version: "0.3.0"}, // unparseable range: never counted
		{RuntimeName: "test-rt", Version: "9.9.9"}, // no row: never counted
		{RuntimeName: "test-rt", Version: "dev"},   // non-semver: never counted
		{RuntimeName: "other-rt", Version: "0.1.0"},
	}
	tests := []struct {
		name        string
		runtimeName string
		candidate   string
		want        int
	}{
		{"upgrade strands the out-of-range worker", "test-rt", "0.2.0", 1},
		{"upgrade inside every range strands nobody", "test-rt", "0.1.9", 0},
		{"unknown runtime strands nobody", "ghost", "0.2.0", 0},
		{"worker of another runtime is ignored", "other-rt", "9.9.9", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, countIncompatibleWorkers(idx, workers, tt.runtimeName, tt.candidate))
		})
	}
}

// serveCompatIndex points the handler at a local registry serving the compat
// fixture and warms the on-disk cache, which is all CheckWorkerCompat reads.
func serveCompatIndex(t *testing.T, h *Handler) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(compatIndexYAML))
	}))
	t.Cleanup(srv.Close)
	h.cfg.RegistryURL = srv.URL + "/index.yml"
	client, err := h.registryClient()
	require.NoError(t, err)
	_, err = client.Fetch(context.Background())
	require.NoError(t, err)
}

// captureLogs redirects the handler's logger into a buffer so the
// accept-with-warning paths can be asserted on.
func captureLogs(h *Handler) *bytes.Buffer {
	var buf bytes.Buffer
	h.logger = zerolog.New(&buf)
	return &buf
}

func TestCheckWorkerCompat(t *testing.T) {
	t.Run("no cached index accepts with a warning", func(t *testing.T) {
		h := newTestHandler(t)
		installTestRuntimeVersion(t, h, "0.2.0")
		logs := captureLogs(h)

		require.NoError(t, h.CheckWorkerCompat("test-rt", "0.1.0"))
		require.Contains(t, logs.String(), "no cached registry index")
	})

	t.Run("runtime not installed accepts with a warning", func(t *testing.T) {
		h := newTestHandler(t)
		logs := captureLogs(h)

		require.NoError(t, h.CheckWorkerCompat("test-rt", "0.1.0"))
		require.Contains(t, logs.String(), "is not installed")
	})

	t.Run("index row admits the pair", func(t *testing.T) {
		h := newTestHandler(t)
		h.version = "1.0.0"
		installTestRuntimeVersion(t, h, "0.1.5")
		serveCompatIndex(t, h)
		logs := captureLogs(h)

		require.NoError(t, h.CheckWorkerCompat("test-rt", "0.1.0"))
		require.Empty(t, logs.String())
	})

	t.Run("index row excludes the installed runtime", func(t *testing.T) {
		h := newTestHandler(t)
		h.version = "1.0.0"
		installTestRuntimeVersion(t, h, "0.2.5")
		serveCompatIndex(t, h)

		err := h.CheckWorkerCompat("test-rt", "0.1.0")
		require.Error(t, err)
		require.Contains(t, err.Error(), ">=0.1 <0.2")
		require.Contains(t, err.Error(), "0.2.5")
	})

	t.Run("no row for the worker version accepts with a warning", func(t *testing.T) {
		h := newTestHandler(t)
		h.version = "1.0.0"
		installTestRuntimeVersion(t, h, "0.1.5")
		serveCompatIndex(t, h)
		logs := captureLogs(h)

		require.NoError(t, h.CheckWorkerCompat("test-rt", "9.9.9"))
		require.Contains(t, logs.String(), "no test-rt worker version 9.9.9")
	})
}
