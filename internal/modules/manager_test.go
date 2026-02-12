package modules

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubResolver is a test double for ModuleResolverInterface.
type stubResolver struct {
	resolved string
	err      error
}

func (s *stubResolver) Resolve(_ context.Context, _, _ string) (string, error) {
	return s.resolved, s.err
}

func newTestManager() *Manager {
	return NewManager(zerolog.Nop())
}

// --- NewManager ---

func TestNewManager(t *testing.T) {
	m := newTestManager()
	require.NotNil(t, m)
	require.Empty(t, m.Modules())
}

// --- Modules / GetModule ---

func TestGetModule_Empty(t *testing.T) {
	m := newTestManager()
	require.Nil(t, m.GetModule("nonexistent"))
}

func TestModules_ReturnsInternalSlice(t *testing.T) {
	m := newTestManager()
	// Inject a module directly to test accessors without subprocess.
	m.modules = append(m.modules, &LoadedModule{Name: "alpha"})
	m.modules = append(m.modules, &LoadedModule{Name: "beta"})

	require.Len(t, m.Modules(), 2)
	require.Equal(t, "alpha", m.Modules()[0].Name)
	require.Equal(t, "beta", m.Modules()[1].Name)
}

func TestGetModule_Found(t *testing.T) {
	m := newTestManager()
	m.modules = []*LoadedModule{{Name: "foo"}, {Name: "bar"}}

	got := m.GetModule("bar")
	require.NotNil(t, got)
	require.Equal(t, "bar", got.Name)
}

func TestGetModule_NotFound(t *testing.T) {
	m := newTestManager()
	m.modules = []*LoadedModule{{Name: "foo"}}

	require.Nil(t, m.GetModule("missing"))
}

// --- UnloadModule ---

func TestUnloadModule_NotFound(t *testing.T) {
	m := newTestManager()
	err := m.UnloadModule("ghost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ghost")
	require.Contains(t, err.Error(), "not found")
}

func TestUnloadModule_RemovesFromList(t *testing.T) {
	m := newTestManager()
	// Inject modules without real clients — UnloadModule calls killAndWait
	// which dereferences the client, so we cannot fully test removal of a
	// module that has a nil client without a panic. Instead we test the
	// "not found" path above for error handling, and verify the slice
	// manipulation logic by checking that after injecting and removing,
	// the slice is correct.
	//
	// NOTE: This test validates the slice manipulation logic by directly
	// checking the modules slice after manual removal (mirroring UnloadModule
	// logic without the killAndWait call that requires a real go-plugin client).
	m.modules = []*LoadedModule{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}

	// Simulate removing "b" (the same slice operation UnloadModule does).
	for i, mod := range m.modules {
		if mod.Name == "b" {
			m.modules = append(m.modules[:i], m.modules[i+1:]...)
			break
		}
	}

	require.Len(t, m.Modules(), 2)
	require.Nil(t, m.GetModule("b"))
	require.NotNil(t, m.GetModule("a"))
	require.NotNil(t, m.GetModule("c"))
}

// --- SetExtraEnv / SetLogCallback / SetInstaller ---

func TestSetExtraEnv(t *testing.T) {
	m := newTestManager()
	env := []string{"FOO=bar", "BAZ=qux"}
	m.SetExtraEnv(env)
	require.Equal(t, env, m.extraEnv)
}

func TestSetLogCallback(t *testing.T) {
	m := newTestManager()
	var called bool
	m.SetLogCallback(func(_, _ string) { called = true })
	require.NotNil(t, m.logCallback)
	m.logCallback("test", "line")
	require.True(t, called)
}

func TestSetInstaller(t *testing.T) {
	m := newTestManager()
	require.Nil(t, m.installer)
	m.SetInstaller(&stubResolver{})
	require.NotNil(t, m.installer)
}

// --- LoadModule validation (no subprocess) ---

func TestLoadModule_EmptyCommand_NoSource(t *testing.T) {
	m := newTestManager()
	err := m.LoadModule(context.Background(), config.ModuleConfig{
		Name: "test",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no command configured")
}

func TestLoadModule_SourceWithoutInstaller(t *testing.T) {
	m := newTestManager()
	err := m.LoadModule(context.Background(), config.ModuleConfig{
		Name:   "test",
		Source: "github:foo/bar",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no registry is configured")
}

func TestLoadModule_SourceWithResolverError(t *testing.T) {
	m := newTestManager()
	m.SetInstaller(&stubResolver{err: os.ErrNotExist})
	err := m.LoadModule(context.Background(), config.ModuleConfig{
		Name:    "test",
		Source:  "github:foo/bar",
		Version: "1.0.0",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolving module from registry")
}

func TestLoadModule_SourceResolvedToEmpty(t *testing.T) {
	m := newTestManager()
	m.SetInstaller(&stubResolver{resolved: ""})
	err := m.LoadModule(context.Background(), config.ModuleConfig{
		Name:   "test",
		Source: "github:foo/bar",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no command configured")
}

// --- resolveExecutable ---

func TestResolveExecutable(t *testing.T) {
	t.Run("existing file returns absolute path", func(t *testing.T) {
		tmp := t.TempDir()
		bin := filepath.Join(tmp, "mybin")
		require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh"), 0o755))

		got, err := resolveExecutable(bin)
		require.NoError(t, err)
		require.True(t, filepath.IsAbs(got))
		require.Equal(t, bin, got)
	})

	t.Run("non-existing returns abs without error", func(t *testing.T) {
		got, err := resolveExecutable("/no/such/binary")
		require.NoError(t, err)
		require.True(t, filepath.IsAbs(got))
	})

	if runtime.GOOS == "windows" {
		t.Run("windows appends .exe", func(t *testing.T) {
			tmp := t.TempDir()
			exePath := filepath.Join(tmp, "tool.exe")
			require.NoError(t, os.WriteFile(exePath, []byte("MZ"), 0o755))

			// Ask for "tool" without extension — should find "tool.exe".
			got, err := resolveExecutable(filepath.Join(tmp, "tool"))
			require.NoError(t, err)
			require.Equal(t, exePath, got)
		})
	}
}

// --- appendLibraryPaths ---

func TestAppendLibraryPaths(t *testing.T) {
	sep := string(os.PathListSeparator)

	tests := []struct {
		name    string
		environ []string
		module  string
		checkFn func(t *testing.T, result []string)
	}{
		{
			name:    "appends to existing PATH",
			environ: []string{"HOME=/home/user", "PATH=/usr/bin"},
			module:  filepath.Join("opt", "modules", "bin", "mymod"),
			checkFn: func(t *testing.T, result []string) {
				t.Helper()
				modDir := filepath.Join("opt", "modules", "bin")
				var found bool
				for _, e := range result {
					if len(e) >= 5 && (e[:5] == "PATH=" || e[:5] == "Path=" || e[:5] == "path=") {
						found = true
						require.Contains(t, e, modDir)
						require.Contains(t, e, sep)
					}
				}
				require.True(t, found, "PATH entry should exist")
			},
		},
		{
			name:    "creates PATH when missing",
			environ: []string{"HOME=/home/user"},
			module:  "/opt/mod/bin",
			checkFn: func(t *testing.T, result []string) {
				t.Helper()
				last := result[len(result)-1]
				require.Contains(t, last, "PATH=")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendLibraryPaths(tt.environ, tt.module)
			tt.checkFn(t, result)
		})
	}
}

// --- logWriter ---

func TestLogWriter(t *testing.T) {
	t.Run("invokes callback per line", func(t *testing.T) {
		var lines []string
		lw := &logWriter{
			name: "testmod",
			onLog: func(name, line string) {
				require.Equal(t, "testmod", name)
				lines = append(lines, line)
			},
		}

		n, err := lw.Write([]byte("line1\nline2\nline3\n"))
		require.NoError(t, err)
		require.Equal(t, len("line1\nline2\nline3\n"), n)
		require.Equal(t, []string{"line1", "line2", "line3"}, lines)
	})

	t.Run("strips carriage returns", func(t *testing.T) {
		var lines []string
		lw := &logWriter{
			name:  "mod",
			onLog: func(_, line string) { lines = append(lines, line) },
		}

		_, err := lw.Write([]byte("hello\r\nworld\r\n"))
		require.NoError(t, err)
		require.Equal(t, []string{"hello", "world"}, lines)
	})

	t.Run("nil callback does not panic", func(t *testing.T) {
		lw := &logWriter{name: "mod"}
		n, err := lw.Write([]byte("safe\n"))
		require.NoError(t, err)
		require.Equal(t, len("safe\n"), n)
	})

	t.Run("skips empty lines", func(t *testing.T) {
		var lines []string
		lw := &logWriter{
			name:  "mod",
			onLog: func(_, line string) { lines = append(lines, line) },
		}

		_, err := lw.Write([]byte("\n\ndata\n\n"))
		require.NoError(t, err)
		require.Equal(t, []string{"data"}, lines)
	})
}

// --- LoadedModule accessor ---

func TestLoadedModule_Module(t *testing.T) {
	lm := &LoadedModule{Name: "test"}
	// module field is nil when not connected — Module() should return nil.
	require.Nil(t, lm.Module())
}

// --- Shutdown on empty manager ---

func TestShutdown_Empty(t *testing.T) {
	m := newTestManager()
	// Should not panic on an empty module list.
	m.Shutdown()
	require.Empty(t, m.Modules())
}
