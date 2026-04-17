package apps

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

// stubResolver is a test double for AppResolverInterface.
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
	require.Empty(t, m.apps)
}

// --- GetApp ---

func TestGetApp_Empty(t *testing.T) {
	m := newTestManager()
	require.Nil(t, m.GetApp("nonexistent"))
}

func TestGetApp_Found(t *testing.T) {
	m := newTestManager()
	m.apps = []*LoadedApp{{Name: "foo"}, {Name: "bar"}}

	got := m.GetApp("bar")
	require.NotNil(t, got)
	require.Equal(t, "bar", got.Name)
}

func TestGetApp_NotFound(t *testing.T) {
	m := newTestManager()
	m.apps = []*LoadedApp{{Name: "foo"}}

	require.Nil(t, m.GetApp("missing"))
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

// --- LoadApp validation (no subprocess) ---

func TestLoadApp_EmptyCommand_NoSource(t *testing.T) {
	m := newTestManager()
	err := m.LoadApp(context.Background(), config.AppConfig{
		Name: "test",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no command configured")
}

func TestLoadApp_SourceWithoutInstaller(t *testing.T) {
	m := newTestManager()
	err := m.LoadApp(context.Background(), config.AppConfig{
		Name:   "test",
		Source: "github:foo/bar",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no registry is configured")
}

func TestLoadApp_SourceWithResolverError(t *testing.T) {
	m := newTestManager()
	m.SetInstaller(&stubResolver{err: os.ErrNotExist})
	err := m.LoadApp(context.Background(), config.AppConfig{
		Name:    "test",
		Source:  "github:foo/bar",
		Version: "1.0.0",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolving app from registry")
}

func TestLoadApp_SourceResolvedToEmpty(t *testing.T) {
	m := newTestManager()
	m.SetInstaller(&stubResolver{resolved: ""})
	err := m.LoadApp(context.Background(), config.AppConfig{
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
		app     string
		checkFn func(t *testing.T, result []string)
	}{
		{
			name:    "appends to existing PATH",
			environ: []string{"HOME=/home/user", "PATH=/usr/bin"},
			app:     filepath.Join("opt", "apps", "bin", "myapp"),
			checkFn: func(t *testing.T, result []string) {
				t.Helper()
				appDir := filepath.Join("opt", "apps", "bin")
				var found bool
				for _, e := range result {
					if len(e) >= 5 && (e[:5] == "PATH=" || e[:5] == "Path=" || e[:5] == "path=") {
						found = true
						require.Contains(t, e, appDir)
						require.Contains(t, e, sep)
					}
				}
				require.True(t, found, "PATH entry should exist")
			},
		},
		{
			name:    "creates PATH when missing",
			environ: []string{"HOME=/home/user"},
			app:     "/opt/app/bin",
			checkFn: func(t *testing.T, result []string) {
				t.Helper()
				last := result[len(result)-1]
				require.Contains(t, last, "PATH=")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendLibraryPaths(tt.environ, tt.app)
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

// --- LoadedApp accessor ---

func TestLoadedApp_App(t *testing.T) {
	lm := &LoadedApp{Name: "test"}
	// app field is nil when not connected — App() should return nil.
	require.Nil(t, lm.App())
}

// --- Shutdown on empty manager ---

func TestShutdown_Empty(t *testing.T) {
	m := newTestManager()
	// Should not panic on an empty app list.
	m.Shutdown()
	require.Empty(t, m.apps)
}
