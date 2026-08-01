package runtimes

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// runtime_name flows from the .mass manifest into config.RuntimeDir (a bare
// filepath.Join) and the /mass.<runtime_name>.* HTTP mount. An unsanitized
// name can escape the runtimes directory or shadow the /mass.v1.Mass public
// API namespace.
func TestValidateRuntimeName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{name: "simple", in: "llama-cpp", ok: true},
		{name: "digits", in: "onnx2", ok: true},
		{name: "leading digit", in: "7b-runner", ok: true},
		{name: "empty", in: "", ok: false},
		{name: "reserved v1", in: "v1", ok: false},
		{name: "uppercase", in: "Llama", ok: false},
		{name: "leading hyphen", in: "-llama", ok: false},
		{name: "path traversal", in: "../evil", ok: false},
		{name: "path separator", in: "a/b", ok: false},
		{name: "backslash", in: `a\b`, ok: false},
		{name: "dot", in: "a.b", ok: false},
		{name: "space", in: "a b", ok: false},
		{name: "underscore", in: "a_b", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRuntimeName(tt.in)
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// InstallFromPath must refuse a package whose manifest names a hostile or
// reserved runtime_name before anything lands on disk.
func TestInstallFromPath_RejectsBadRuntimeName(t *testing.T) {
	tests := []struct {
		name        string
		runtimeName string
	}{
		{name: "traversal", runtimeName: "../evil"},
		{name: "reserved v1", runtimeName: "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			m, err := NewManager(dataDir, fakeRuntimeStore{}, zerolog.Nop())
			require.NoError(t, err)

			pkg := writeTestPackage(t, tt.runtimeName)
			_, err = m.InstallFromPath(pkg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "runtime_name")
		})
	}
}

// writeTestPackage builds a minimal .mass zip (runtime.yml + bin/gw) with
// the given runtime_name and returns its path.
func writeTestPackage(t *testing.T, runtimeName string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pkg.mass")
	f, err := os.Create(path)
	require.NoError(t, err)
	zw := zip.NewWriter(f)

	mfw, err := zw.Create("runtime.yml")
	require.NoError(t, err)
	_, err = fmt.Fprintf(mfw, "runtime_name: %q\nversion: \"0.0.1\"\ndisplay_name: Test\n", runtimeName)
	require.NoError(t, err)

	bw, err := zw.Create("bin/gw")
	require.NoError(t, err)
	_, err = bw.Write([]byte("#!/bin/sh\n"))
	require.NoError(t, err)

	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
	return path
}

// A gateway subprocess that dies must not stay "running" forever: the
// exit watcher removes it from the manager and fires the state-change
// callback so the UI reflects the crash.
func TestWatchGatewayExit(t *testing.T) {
	tests := []struct {
		name        string
		exits       bool // subprocess dies vs. operator stops it first
		wantRemoved bool
	}{
		{name: "crashed subprocess is removed", exits: true, wantRemoved: true},
		{name: "operator stop wins — watcher exits silently", exits: false, wantRemoved: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewManager(t.TempDir(), fakeRuntimeStore{}, zerolog.Nop())
			require.NoError(t, err)

			var exited atomic.Bool
			gw := &LoadedGateway{
				Kind:   "llama-cpp",
				logger: zerolog.Nop(),
				exited: exited.Load,
			}
			m.mu.Lock()
			m.running["llama-cpp"] = gw
			m.mu.Unlock()

			stateChanged := make(chan string, 1)
			stop := m.AddOnStateChange(func(runtimeName string) {
				select {
				case stateChanged <- runtimeName:
				default:
				}
			})
			defer stop()

			done := make(chan struct{})
			go func() {
				m.watchGatewayExit("llama-cpp", gw, time.Millisecond)
				close(done)
			}()

			if tt.exits {
				exited.Store(true)
				select {
				case name := <-stateChanged:
					require.Equal(t, "llama-cpp", name)
				case <-time.After(5 * time.Second):
					t.Fatal("watcher never fired the state change")
				}
			} else {
				require.NoError(t, m.Stop("llama-cpp"))
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher goroutine never exited")
			}
			require.False(t, m.IsRunning("llama-cpp"))
		})
	}
}

// fakeRuntimeStore satisfies store.RuntimeStoreInterface without a DB.
type fakeRuntimeStore struct{}

func (fakeRuntimeStore) UpsertRuntime(store.RuntimeRow) error   { return nil }
func (fakeRuntimeStore) SetRuntimeAutoStart(string, bool) error { return nil }
func (fakeRuntimeStore) GetRuntime(string) (store.RuntimeRow, error) {
	return store.RuntimeRow{}, nil
}
func (fakeRuntimeStore) ListRuntimes() ([]store.RuntimeRow, error) { return nil, nil }
func (fakeRuntimeStore) DeleteRuntime(string) error                { return nil }
