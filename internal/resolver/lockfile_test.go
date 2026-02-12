package resolver

import (
	"path/filepath"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/require"
)

func TestLockFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "module.lock")

	original := &LockFile{
		Modules: map[string]LockedModule{
			"foo": {Version: "1.2.3", Source: "github:test/foo", SHA256: "abc123"},
			"bar": {Version: "2.0.0", Source: "github:test/bar"},
		},
	}

	require.NoError(t, WriteLockFile(path, original))

	loaded, err := ReadLockFile(path)
	require.NoError(t, err)
	require.Equal(t, original.Modules, loaded.Modules)
}

func TestReadLockFileMissing(t *testing.T) {
	lf, err := ReadLockFile(filepath.Join(t.TempDir(), "nonexistent.lock"))
	require.NoError(t, err)
	require.NotNil(t, lf.Modules)
	require.Empty(t, lf.Modules)
}

func TestLockFileFromResolved(t *testing.T) {
	modules := []ResolvedModule{
		{Name: "foo", Version: mustVersion("1.0.0"), Source: "github:test/foo"},
		{Name: "bar", Version: mustVersion("2.5.0"), Source: "github:test/bar"},
	}

	lf := LockFileFromResolved(modules)
	require.Len(t, lf.Modules, 2)
	require.Equal(t, "1.0.0", lf.Modules["foo"].Version)
	require.Equal(t, "2.5.0", lf.Modules["bar"].Version)
}

func TestLockFileToResolved(t *testing.T) {
	lf := &LockFile{
		Modules: map[string]LockedModule{
			"bar": {Version: "2.0.0", Source: "github:test/bar"},
			"foo": {Version: "1.0.0", Source: "github:test/foo"},
		},
	}

	resolved, err := lf.ToResolved()
	require.NoError(t, err)
	require.Len(t, resolved, 2)
	// Sorted by name.
	require.Equal(t, "bar", resolved[0].Name)
	require.Equal(t, "foo", resolved[1].Name)
}

func TestLockFileToResolvedInvalidVersion(t *testing.T) {
	lf := &LockFile{
		Modules: map[string]LockedModule{
			"foo": {Version: "not-a-version"},
		},
	}

	_, err := lf.ToResolved()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid version")
}

func TestLockFileIsStale(t *testing.T) {
	lf := &LockFile{
		Modules: map[string]LockedModule{
			"foo": {Version: "1.5.0", Source: "github:test/foo"},
			"bar": {Version: "2.0.0", Source: "github:test/bar"},
		},
	}

	tests := []struct {
		name  string
		deps  []Dependency
		stale bool
	}{
		{
			name:  "all satisfied",
			deps:  []Dependency{{Name: "foo", Version: "^1.0.0"}, {Name: "bar", Version: ">=2.0.0"}},
			stale: false,
		},
		{
			name:  "new dependency not in lock",
			deps:  []Dependency{{Name: "foo", Version: "^1.0.0"}, {Name: "baz", Version: ">=1.0.0"}},
			stale: true,
		},
		{
			name:  "locked version no longer satisfies constraint",
			deps:  []Dependency{{Name: "foo", Version: "^2.0.0"}},
			stale: true,
		},
		{
			name:  "no constraints",
			deps:  []Dependency{{Name: "foo"}},
			stale: false,
		},
		{
			name:  "empty deps",
			deps:  nil,
			stale: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.stale, lf.IsStale(tt.deps))
		})
	}
}

func TestLockFileIsStaleInvalidConstraint(t *testing.T) {
	lf := &LockFile{
		Modules: map[string]LockedModule{
			"foo": {Version: "1.0.0"},
		},
	}
	// Invalid constraint string should cause stale=true.
	deps := []Dependency{{Name: "foo", Version: "not-valid!!!"}}
	require.True(t, lf.IsStale(deps))
}

func TestMustVersion(t *testing.T) {
	v := mustVersion("1.2.3")
	require.Equal(t, uint64(1), v.Major())
	require.Equal(t, uint64(2), v.Minor())
	require.Equal(t, uint64(3), v.Patch())
}

// Ensure that *semver.Version.String() round-trips correctly.
func TestSemverRoundTrip(t *testing.T) {
	v, err := semver.NewVersion("1.2.3")
	require.NoError(t, err)
	v2, err := semver.NewVersion(v.String())
	require.NoError(t, err)
	require.True(t, v.Equal(v2))
}
