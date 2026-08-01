package web

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/stretchr/testify/require"
)

// buildMassPackage returns the bytes of a minimal valid .mass package: a zip
// with runtime.yml naming the runtime plus a stub gateway binary under bin/.
func buildMassPackage(t *testing.T, runtimeName, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	manifest := fmt.Sprintf("runtime_name: %s\nversion: %s\ndisplay_name: %s\nbinary: bin/gw\n",
		runtimeName, version, runtimeName)
	mf, err := zw.Create("runtime.yml")
	require.NoError(t, err)
	_, err = mf.Write([]byte(manifest))
	require.NoError(t, err)

	bin, err := zw.Create("bin/gw")
	require.NoError(t, err)
	_, err = bin.Write([]byte("#!/bin/sh\nexit 0\n"))
	require.NoError(t, err)

	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// registryFixture serves an index.yml and a runtime artifact over HTTP. The
// artifact URL is filled in with the test server's own address once it starts.
// pad adds that many extra artifact-less runtime packages ("pad-rt-N") to the
// index, read at request time — tests set it to exercise list windowing.
type registryFixture struct {
	server   *httptest.Server
	indexURL string
	pad      int
}

// Fixed identity of the package the fixture serves.
const (
	fixtureRuntimeName = "test-rt"
	fixtureVersion     = "0.1.0"
)

// newRegistryFixture stands up an httptest server serving a single-runtime
// index (package "test-rt" v0.1.0) whose artifact is a real .mass built
// in-test. When tamperSHA is true the index pins a wrong digest so downloads
// fail the checksum check.
func newRegistryFixture(t *testing.T, tamperSHA bool) *registryFixture {
	t.Helper()
	runtimeName, version := fixtureRuntimeName, fixtureVersion
	artifact := buildMassPackage(t, runtimeName, version)
	sha := sha256Hex(artifact)
	if tamperSHA {
		sha = sha256Hex([]byte("not the artifact"))
	}
	platform := registry.RuntimePlatform(runtime.GOOS, runtime.GOARCH).Key()

	fix := &registryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/artifact.mass", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(artifact)
	})
	mux.HandleFunc("/index.yml", func(w http.ResponseWriter, r *http.Request) {
		index := fmt.Sprintf(`schema_version: 1
packages:
  - name: %s
    kind: runtime
    runtime_name: %s
    display_name: Test Runtime
    description: A test runtime gateway.
    versions:
      - version: %s
        mass: ">=0.1"
        artifacts:
          %s: {url: "http://%s/artifact.mass", sha256: %s}
`, runtimeName, runtimeName, version, platform, r.Host, sha)
		for i := 0; i < fix.pad; i++ {
			index += fmt.Sprintf(`  - name: pad-rt-%d
    kind: runtime
    runtime_name: pad-rt-%d
    display_name: Pad Runtime %d
    versions:
      - version: 0.1.0
        mass: ">=0.1"
`, i, i, i)
		}
		_, _ = w.Write([]byte(index))
	})
	fix.server = httptest.NewServer(mux)
	t.Cleanup(fix.server.Close)
	fix.indexURL = fix.server.URL + "/index.yml"
	return fix
}

func TestSearchPackages(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	tests := []struct {
		name    string
		kind    string
		query   string
		rt      string
		wantLen int
	}{
		{name: "all", wantLen: 1},
		{name: "kind runtime", kind: "runtime", wantLen: 1},
		{name: "kind worker excludes runtime", kind: "worker", wantLen: 0},
		{name: "query match", query: "test", wantLen: 1},
		{name: "query miss", query: "nonexistent", wantLen: 0},
		{name: "runtime filter match", rt: "test-rt", wantLen: 1},
		{name: "runtime filter miss", rt: "other", wantLen: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := h.searchPackages(context.Background(), tt.kind, tt.query, tt.rt)
			require.NoError(t, err)
			require.Len(t, res.Packages, tt.wantLen)
			if tt.wantLen > 0 {
				p := res.Packages[0]
				require.Equal(t, "test-rt", p.Name)
				require.Equal(t, "runtime", p.Kind)
				require.Len(t, p.Versions, 1)
				require.True(t, p.Versions[0].HasArtifact, "artifact should exist for the server platform")
			}
			require.False(t, res.Stale)
		})
	}
}

func TestSearchPackages_Unreachable(t *testing.T) {
	h := newTestHandler(t)
	// A closed server address: fetch fails and there is no cache.
	fix := newRegistryFixture(t, false)
	url := fix.indexURL
	fix.server.Close()
	h.cfg.RegistryURL = url

	_, err := h.searchPackages(context.Background(), "", "", "")
	require.ErrorIs(t, err, ErrOpRegistry)
}

func TestInstallRuntimeFromRegistry(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	ri, err := h.installRuntimeFromRegistry(context.Background(), "test-rt", "", "tester")
	require.NoError(t, err)
	require.Equal(t, "test-rt", ri.RuntimeName)
	require.Equal(t, "0.1.0", ri.Version)

	// The runtime is now installed and listed.
	require.True(t, h.runtimes.IsInstalled("test-rt"))
	names := make([]string, 0)
	for _, m := range h.runtimes.List() {
		names = append(names, m.RuntimeName)
	}
	require.Contains(t, names, "test-rt")
}

func TestInstallRuntimeFromRegistry_ChecksumMismatch(t *testing.T) {
	fix := newRegistryFixture(t, true) // tampered digest
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	_, err := h.installRuntimeFromRegistry(context.Background(), "test-rt", "", "tester")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpRegistry)
	require.ErrorIs(t, err, registry.ErrChecksumMismatch)
	require.False(t, h.runtimes.IsInstalled("test-rt"), "a failed download must not install")
}

func TestInstallRuntimeFromRegistry_VersionMismatch(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	_, err := h.installRuntimeFromRegistry(context.Background(), "test-rt", "9.9.9", "tester")
	require.ErrorIs(t, err, ErrOpNotFound)
}

func TestInstallRuntimeFromRegistry_UnknownPackage(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	_, err := h.installRuntimeFromRegistry(context.Background(), "no-such-pkg", "", "tester")
	require.ErrorIs(t, err, ErrOpNotFound)
}

func TestIsNewerVersion(t *testing.T) {
	tests := []struct {
		name               string
		candidate, current string
		want               bool
	}{
		{"strictly newer", "0.2.0", "0.1.0", true},
		{"equal not newer", "0.1.0", "0.1.0", false},
		{"older not newer", "0.1.0", "0.2.0", false},
		{"patch newer", "0.1.1", "0.1.0", true},
		{"unparseable candidate", "latest", "0.1.0", false},
		{"unparseable current", "0.2.0", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isNewerVersion(tt.candidate, tt.current))
		})
	}
}

func TestCountIncompatibleWorkers(t *testing.T) {
	workers := []workerCompat{
		{RuntimeName: "llama-cpp", Compatible: ">=0.1 <0.2"}, // excludes 0.2.0
		{RuntimeName: "llama-cpp", Compatible: ">=0.1 <0.3"}, // covers 0.2.0
		{RuntimeName: "llama-cpp", Compatible: ""},           // empty range; counted incompatible
		{RuntimeName: "llama-cpp", Compatible: "garbage"},    // unparseable; counted incompatible
		{RuntimeName: "other-rt", Compatible: ">=0.1 <0.2"},  // different runtime; ignored
	}
	tests := []struct {
		name        string
		runtimeName string
		candidate   string
		want        int
		wantErr     bool
	}{
		{"upgrade strands out-of-range + empty + unparseable", "llama-cpp", "0.2.0", 3, false},
		{"upgrade within all ranges strands empty + unparseable", "llama-cpp", "0.1.5", 2, false},
		{"other runtime unaffected", "other-rt", "0.2.0", 1, false},
		{"no workers for runtime", "ghost", "0.2.0", 0, false},
		{"unparseable candidate errors", "llama-cpp", "latest", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := countIncompatibleWorkers(workers, tt.runtimeName, tt.candidate)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, n)
		})
	}
}

func TestMassVersionForResolve(t *testing.T) {
	h := newTestHandler(t)

	h.version = "dev"
	require.Equal(t, unresolvableMassVersion, h.massVersionForResolve())

	h.version = ""
	require.Equal(t, unresolvableMassVersion, h.massVersionForResolve())

	h.version = "1.2.3"
	require.Equal(t, "1.2.3", h.massVersionForResolve())
}
