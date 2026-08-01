package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/stretchr/testify/require"
)

// workerRegistryFixture serves an index.yml with one runtime and one worker
// package, plus the worker installer artifact. backends lists the os/arch/backend
// keys the single worker version advertises for the server's os/arch — one entry
// per backend, all pointing at the same served artifact.
type workerRegistryFixture struct {
	server   *httptest.Server
	indexURL string
	artifact []byte
	sha      string
	goos     string
	goarch   string
}

// newWorkerRegistryFixture stands up a registry serving runtime "test-rt" v0.1.0
// and worker "test-rt-worker" v0.1.0 (compatible ">=0.1 <0.2") whose artifacts
// cover the given backends on goos/goarch.
func newWorkerRegistryFixture(t *testing.T, goos, goarch string, backends ...string) *workerRegistryFixture {
	t.Helper()
	artifact := []byte("#!/bin/sh\necho installer\n")
	fix := &workerRegistryFixture{
		artifact: artifact,
		sha:      sha256Hex(artifact),
		goos:     goos,
		goarch:   goarch,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/installer", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(artifact)
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

// workerPkgSpec describes one worker package the multi-package fixture serves:
// its name and the backends its single v0.1.0 (runtime ">=0.1 <0.2") version
// advertises for the fixture's os/arch.
type workerPkgSpec struct {
	name     string
	backends []string
}

// newMultiWorkerRegistryFixture stands up a registry serving runtime "test-rt"
// v0.1.0 plus one worker package per spec (all joined to test-rt), each with a
// v0.1.0 version advertising its backends on goos/goarch. Used to exercise
// worker-package selection when a runtime has more than one worker package.
func newMultiWorkerRegistryFixture(t *testing.T, goos, goarch string, specs ...workerPkgSpec) *workerRegistryFixture {
	t.Helper()
	artifact := []byte("#!/bin/sh\necho installer\n")
	fix := &workerRegistryFixture{
		artifact: artifact,
		sha:      sha256Hex(artifact),
		goos:     goos,
		goarch:   goarch,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/installer", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(artifact)
	})
	mux.HandleFunc("/index.yml", func(w http.ResponseWriter, r *http.Request) {
		pkgs := ""
		for _, s := range specs {
			artLines := ""
			for _, b := range s.backends {
				artLines += fmt.Sprintf("          %s/%s/%s: {url: \"http://%s/installer\", sha256: %s}\n",
					goos, goarch, b, r.Host, fix.sha)
			}
			pkgs += fmt.Sprintf(`  - name: %s
    kind: worker
    runtime_name: test-rt
    display_name: %s Worker
    versions:
      - version: 0.1.0
        runtime: ">=0.1 <0.2"
        artifacts:
%s`, s.name, s.name, artLines)
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
%s`, goos, goarch, r.Host, fix.sha, pkgs)
		_, _ = w.Write([]byte(index))
	})
	fix.server = httptest.NewServer(mux)
	t.Cleanup(fix.server.Close)
	fix.indexURL = fix.server.URL + "/index.yml"
	return fix
}

// installTestRuntime installs the fixture runtime (test-rt v0.1.0) into h so
// worker-bin resolution has an installed runtime whose version is the join key.
func installTestRuntime(t *testing.T, h *Handler) {
	t.Helper()
	pkg := buildMassPackage(t, "test-rt", "0.1.0")
	path := filepath.Join(t.TempDir(), "rt.mass")
	require.NoError(t, os.WriteFile(path, pkg, 0o644))
	_, err := h.runtimes.InstallFromPath(path)
	require.NoError(t, err)
}

func TestNormalizeGOOS(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Linux", "linux"},
		{"linux", "linux"},
		{"LINUX", "linux"},
		{"Darwin", "darwin"},
		{"darwin", "darwin"},
		{"macos", "darwin"},
		{"Windows", "windows"},
		{"windows", "windows"},
		{"plan9", "plan9"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeGOOS(tt.in))
		})
	}
}

func TestNormalizeGOARCH(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"x86_64", "amd64"},
		{"AMD64", "amd64"},
		{"amd64", "amd64"},
		{"aarch64", "arm64"},
		{"ARM64", "arm64"},
		{"arm64", "arm64"},
		{"riscv64", "riscv64"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeGOARCH(tt.in))
		})
	}
}

func TestHandleSetupWorkerBin_AuthExempt(t *testing.T) {
	// A configured auth token must NOT gate the installer proxy: the middleware
	// exempts /setup/*. Drive through the full handler (mux + middleware). No
	// runtime is installed so the endpoint 404s — but reaching that (not 401)
	// proves the bypass.
	h := newTestHandler(t)
	h.SetAuthHash([]byte("$2a$10$abcdefghijklmnopqrstuv")) // any non-empty hash

	srv := httptest.NewServer(h.AuthMiddleware(h))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/setup/worker-bin/test-rt?os=Linux&arch=x86_64")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestResolveWorkerArtifact(t *testing.T) {
	goos, goarch := "linux", "amd64"

	t.Run("single backend inferred", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		resolved, backends, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "")
		require.NoError(t, err)
		require.Nil(t, backends)
		require.Equal(t, fix.sha, resolved.Artifact.SHA256)
	})

	t.Run("multiple backends ambiguous", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan", "cuda")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, backends, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "")
		require.ErrorIs(t, err, errAmbiguousBackend)
		require.ElementsMatch(t, []string{"vulkan", "cuda"}, backends)
	})

	t.Run("explicit backend picks it", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan", "cuda")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		resolved, _, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "cuda", "")
		require.NoError(t, err)
		require.Equal(t, fix.sha, resolved.Artifact.SHA256)
	})

	t.Run("runtime not installed", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL

		_, _, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "")
		require.ErrorIs(t, err, errRuntimeNotInstalled)
	})

	t.Run("no artifact for platform", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, _, err := h.resolveWorkerArtifact(context.Background(), "test-rt", "windows", "arm64", "", "")
		require.ErrorIs(t, err, registry.ErrNotResolved)
	})

	t.Run("single package auto-selected", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "test-rt-worker", backends: []string{"vulkan"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		resolved, cand, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "")
		require.NoError(t, err)
		require.Nil(t, cand)
		require.Equal(t, fix.sha, resolved.Artifact.SHA256)
	})

	t.Run("multiple packages ambiguous", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}},
			workerPkgSpec{name: "worker-b", backends: []string{"cuda"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, cand, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "")
		require.ErrorIs(t, err, errAmbiguousWorker)
		require.Equal(t, []string{"worker-a", "worker-b"}, cand)
	})

	t.Run("explicit worker package picks it", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}},
			workerPkgSpec{name: "worker-b", backends: []string{"cuda"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		resolved, _, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "worker-b")
		require.NoError(t, err)
		require.Equal(t, fix.sha, resolved.Artifact.SHA256)
	})

	t.Run("unknown worker package errors", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		_, _, err := h.resolveWorkerArtifact(context.Background(), "test-rt", goos, goarch, "", "no-such")
		require.ErrorIs(t, err, registry.ErrNotResolved)
	})
}

func TestWorkerOptionsFor(t *testing.T) {
	goos, goarch := "linux", "amd64"

	t.Run("zero packages", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		opts, err := h.workerOptionsFor(context.Background(), "test-rt")
		require.NoError(t, err)
		require.Empty(t, opts)
	})

	t.Run("one package", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "test-rt-worker", backends: []string{"vulkan", "cuda"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		opts, err := h.workerOptionsFor(context.Background(), "test-rt")
		require.NoError(t, err)
		require.Len(t, opts, 1)
		require.Equal(t, "test-rt-worker", opts[0].Name)
		require.Equal(t, "test-rt-worker Worker", opts[0].DisplayName)
		require.Equal(t, []string{"cuda", "vulkan"}, opts[0].Backends)
	})

	t.Run("two packages sorted", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-b", backends: []string{"cuda"}},
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		opts, err := h.workerOptionsFor(context.Background(), "test-rt")
		require.NoError(t, err)
		require.Len(t, opts, 2)
		require.Equal(t, "worker-a", opts[0].Name)
		require.Equal(t, []string{"vulkan"}, opts[0].Backends)
		require.Equal(t, "worker-b", opts[1].Name)
		require.Equal(t, []string{"cuda"}, opts[1].Backends)
	})

	t.Run("runtime not installed", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch)
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL

		_, err := h.workerOptionsFor(context.Background(), "ghost-rt")
		require.ErrorIs(t, err, errRuntimeNotInstalled)
	})

	t.Run("other runtime yields no options", func(t *testing.T) {
		// A runtime with no worker packages joined to it: installed, but the
		// index has no workers for its name.
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "test-rt-worker", backends: []string{"vulkan"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)
		pkg := buildMassPackage(t, "other-rt", "0.1.0")
		path := filepath.Join(t.TempDir(), "other.mass")
		require.NoError(t, os.WriteFile(path, pkg, 0o644))
		_, err := h.runtimes.InstallFromPath(path)
		require.NoError(t, err)

		opts, err := h.workerOptionsFor(context.Background(), "other-rt")
		require.NoError(t, err)
		require.Empty(t, opts)
	})
}

func TestHandleSetupWorkerBin(t *testing.T) {
	goos, goarch := "linux", "amd64"

	t.Run("serves artifact and caches it", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os="+goos+"&arch="+goarch, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, fix.artifact, w.Body.Bytes())
		// Cache file appears under registry-cache/artifacts/<sha256>.
		cached := filepath.Join(h.registryCacheDir(), "artifacts", fix.sha)
		_, err := os.Stat(cached)
		require.NoError(t, err)
	})

	t.Run("uname-style os/arch normalized", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os=Linux&arch=x86_64", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, fix.artifact, w.Body.Bytes())
	})

	t.Run("multi backend 409 lists backends", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan", "cuda")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os="+goos+"&arch="+goarch, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), "vulkan")
		require.Contains(t, w.Body.String(), "cuda")
	})

	t.Run("multi worker package 409 lists packages", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}},
			workerPkgSpec{name: "worker-b", backends: []string{"cuda"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os="+goos+"&arch="+goarch, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), "?worker=")
		require.Contains(t, w.Body.String(), "worker-a")
		require.Contains(t, w.Body.String(), "worker-b")
	})

	t.Run("explicit worker query selects package", func(t *testing.T) {
		fix := newMultiWorkerRegistryFixture(t, goos, goarch,
			workerPkgSpec{name: "worker-a", backends: []string{"vulkan"}},
			workerPkgSpec{name: "worker-b", backends: []string{"cuda"}})
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL
		installTestRuntime(t, h)

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os="+goos+"&arch="+goarch+"&worker=worker-b", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, fix.artifact, w.Body.Bytes())
	})

	t.Run("runtime not installed 404", func(t *testing.T) {
		fix := newWorkerRegistryFixture(t, goos, goarch, "vulkan")
		h := newTestHandler(t)
		h.cfg.RegistryURL = fix.indexURL

		r := httptest.NewRequest(http.MethodGet,
			"/setup/worker-bin/test-rt?os="+goos+"&arch="+goarch, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("missing query 400", func(t *testing.T) {
		h := newTestHandler(t)
		r := httptest.NewRequest(http.MethodGet, "/setup/worker-bin/test-rt", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// countingArtifactServer serves body at /a and counts requests into hits.
func countingArtifactServer(t *testing.T, body []byte, hits *int32) registry.Artifact {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(hits, 1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return registry.Artifact{URL: srv.URL + "/a", SHA256: sha256Hex(body)}
}

func TestArtifactCache(t *testing.T) {
	body := []byte("artifact-bytes")
	sha := sha256Hex(body)

	t.Run("miss downloads then hit serves cached", func(t *testing.T) {
		var hits int32
		art := countingArtifactServer(t, body, &hits)
		c := newArtifactCache(filepath.Join(t.TempDir(), "artifacts"))

		p1, err := c.ensure(context.Background(), art)
		require.NoError(t, err)
		got, err := os.ReadFile(p1)
		require.NoError(t, err)
		require.Equal(t, body, got)

		// Second call is a cache hit — no further download.
		p2, err := c.ensure(context.Background(), art)
		require.NoError(t, err)
		require.Equal(t, p1, p2)
		require.Equal(t, int32(1), atomic.LoadInt32(&hits))
	})

	t.Run("pre-dropped file served without download", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "artifacts")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, sha), body, 0o644))
		c := newArtifactCache(dir)
		// A URL that would fail if fetched; the pre-dropped file wins.
		art := registry.Artifact{URL: "http://127.0.0.1:1/never", SHA256: sha}

		p, err := c.ensure(context.Background(), art)
		require.NoError(t, err)
		got, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, body, got)
	})

	t.Run("concurrent requests single download", func(t *testing.T) {
		var hits int32
		art := countingArtifactServer(t, body, &hits)
		c := newArtifactCache(filepath.Join(t.TempDir(), "artifacts"))

		const n = 8
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = c.ensure(context.Background(), art)
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, int32(1), atomic.LoadInt32(&hits), "concurrent requests must not double-download")
	})
}
