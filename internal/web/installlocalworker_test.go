package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/stretchr/testify/require"
)

// fakeInstallerFixture serves a registry index (runtime "test-rt" + worker
// "test-rt-worker") plus a fake installer script for the server's own platform.
// The installer echoes its argv and exits with the given code, so the op test
// can assert the flags MASS passes and the failure path. backends lets a test
// force the ambiguous-backend branch (>1 backend on the platform).
type fakeInstallerFixture struct {
	server   *httptest.Server
	indexURL string
	sha      string
}

// installerScript builds a fake mass-worker-setup that records its argv to
// stdout and exits with exitCode. POSIX-sh; skipped on Windows where exec of a
// shebang script is not meaningful.
func installerScript(exitCode int) []byte {
	return []byte(fmt.Sprintf("#!/bin/sh\necho \"ARGV: $*\"\nexit %d\n", exitCode))
}

func newFakeInstallerFixture(t *testing.T, exitCode int, backends ...string) *fakeInstallerFixture {
	t.Helper()
	if len(backends) == 0 {
		backends = []string{"cpu"}
	}
	goos, goarch := runtime.GOOS, runtime.GOARCH
	installer := installerScript(exitCode)
	fix := &fakeInstallerFixture{sha: sha256Hex(installer)}

	mux := http.NewServeMux()
	mux.HandleFunc("/installer", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(installer)
	})
	mux.HandleFunc("/index.yml", func(w http.ResponseWriter, r *http.Request) {
		artLines := ""
		for _, b := range backends {
			artLines += fmt.Sprintf("          %s/%s/%s: {url: \"http://%s/installer\", sha256: %s}\n",
				goos, goarch, b, r.Host, fix.sha)
		}
		index := fmt.Sprintf(`schema_version: 1
packages:
  - name: test-rt
    kind: runtime
    runtime_name: test-rt
    display_name: Test Runtime
    versions:
      - version: 0.1.0
        mass: ">=0.1"
        artifacts:
          %s/%s: {url: "http://%s/rt.mass", sha256: %s}
  - name: test-rt-worker
    kind: worker
    runtime_name: test-rt
    versions:
      - version: 0.1.0
        runtime: ">=0.1 <0.2"
        artifacts:
%s`, goos, goarch, r.Host, fix.sha, artLines)
		_, _ = w.Write([]byte(index))
	})
	fix.server = httptest.NewServer(mux)
	t.Cleanup(fix.server.Close)
	fix.indexURL = fix.server.URL + "/index.yml"
	return fix
}

func TestInstallLocalWorker(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake installer is a POSIX-sh script; not exec'able on Windows")
	}

	t.Run("success plumbs url, scope, and a minted join token", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		h.cfg.ListenAddr = "127.0.0.1:34999"
		// Auth enabled: the installer must enroll with a server-minted join token
		// (mjt_ prefix), never the caller's own credential.
		h.SetAuthHash([]byte("operator-hash"))
		installTestRuntime(t, h)

		res, err := h.installLocalWorker(context.Background(), "test-rt", "user", "", "tester")
		require.NoError(t, err)
		require.Equal(t, "test-rt-worker", res.WorkerPackage)
		require.Equal(t, "0.1.0", res.WorkerVersion)
		// The installer echoed the argv MASS handed it.
		require.Contains(t, res.Output, "--non-interactive")
		require.Contains(t, res.Output, "--mass-url http://127.0.0.1:34999")
		require.Contains(t, res.Output, "--scope user")
		require.Contains(t, res.Output, "--token mjt_")
	})

	t.Run("auth disabled omits the token flag", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		// No auth hash set ⇒ auth disabled ⇒ no join token, no --token flag.
		res, err := h.installLocalWorker(context.Background(), "test-rt", "user", "", "tester")
		require.NoError(t, err)
		require.NotContains(t, res.Output, "--token")
	})

	t.Run("runtime inferred when exactly one installed", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		res, err := h.installLocalWorker(context.Background(), "", "", "", "tester")
		require.NoError(t, err)
		require.Equal(t, "test-rt-worker", res.WorkerPackage)
		require.Contains(t, res.Output, "--scope user", "empty scope defaults to user")
	})

	t.Run("no runtime installed is invalid", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL

		_, err := h.installLocalWorker(context.Background(), "", "", "", "tester")
		require.ErrorIs(t, err, ErrOpInvalid)
	})

	t.Run("named runtime not installed is not found", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, err := h.installLocalWorker(context.Background(), "ghost-rt", "", "", "tester")
		require.ErrorIs(t, err, ErrOpNotFound)
	})

	t.Run("multiple backends is invalid", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0, "vulkan", "cuda")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, err := h.installLocalWorker(context.Background(), "test-rt", "", "", "tester")
		require.ErrorIs(t, err, ErrOpInvalid)
		require.Contains(t, err.Error(), "vulkan")
		require.Contains(t, err.Error(), "cuda")
	})

	t.Run("installer non-zero exit surfaces stderr tail", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 2)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, err := h.installLocalWorker(context.Background(), "test-rt", "user", "", "tester")
		require.ErrorIs(t, err, ErrOpRegistry)
		require.Contains(t, err.Error(), "ARGV:", "the installer's output is included in the error")
	})

	t.Run("bad scope is invalid", func(t *testing.T) {
		fix := newFakeInstallerFixture(t, 0)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, err := h.installLocalWorker(context.Background(), "test-rt", "root", "", "tester")
		require.ErrorIs(t, err, ErrOpInvalid)
	})
}

func TestLocalWorkerURL(t *testing.T) {
	tests := []struct {
		name       string
		listenAddr string
		tls        bool
		want       string
	}{
		{"loopback http", "127.0.0.1:3455", false, "http://127.0.0.1:3455"},
		{"wildcard normalizes to localhost", "0.0.0.0:3455", false, "http://localhost:3455"},
		{"empty host normalizes to localhost", ":3455", false, "http://localhost:3455"},
		{"tls yields https", "127.0.0.1:8443", true, "https://127.0.0.1:8443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			h.cfg.ListenAddr = tt.listenAddr
			h.cfg.TLS.Enabled = tt.tls
			require.Equal(t, tt.want, h.localWorkerURL())
		})
	}
}

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name, header, want string
	}{
		{"bearer", "Bearer abc123", "abc123"},
		{"empty", "", ""},
		{"non-bearer", "Basic Zm9v", ""},
		{"bearer prefix only", "Bearer ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, bearerToken(tt.header))
		})
	}
}

func TestVersionHasArtifact(t *testing.T) {
	// Worker keys are os/arch/backend; runtime keys os/arch. The server key is
	// runtime.GOOS/GOARCH.
	rtKey := serverPlatform().Key()
	wPrefix := rtKey + "/"
	art := func(keys ...string) map[string]registry.Artifact {
		m := make(map[string]registry.Artifact, len(keys))
		for _, k := range keys {
			m[k] = registry.Artifact{SHA256: "x"}
		}
		return m
	}

	tests := []struct {
		name string
		kind registry.Kind
		arts map[string]registry.Artifact
		want bool
	}{
		{"worker any backend on platform", registry.KindWorker, art(rtKey + "/cpu"), true},
		{"worker other platform", registry.KindWorker, art("other/arch/cpu"), false},
		{"runtime exact key", registry.KindRuntime, art(rtKey), true},
		{"runtime rejects worker-style key", registry.KindRuntime, art(rtKey + "/cpu"), false},
		{"empty artifacts", registry.KindWorker, art(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, versionHasArtifact(tt.kind, tt.arts, rtKey, wPrefix))
		})
	}
}
