package resolver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// --- Mock provider ---

type mockProvider struct {
	versions []*semver.Version
	// downloaded tracks calls to Download.
	downloaded []string
}

func (m *mockProvider) ListVersions(_ context.Context, _ string) ([]*semver.Version, error) {
	return m.versions, nil
}

func (m *mockProvider) Download(_ context.Context, _ string, version *semver.Version, dstPath string) error {
	m.downloaded = append(m.downloaded, version.String())
	// Create a minimal zip with a app.yml.
	return createTestArchive(dstPath, version.String())
}

type mockFactory struct {
	providers map[string]*mockProvider
}

func (f *mockFactory) ProviderFor(source string) (ProviderInterface, error) {
	p, ok := f.providers[source]
	if !ok {
		return nil, fmt.Errorf("unknown source %q", source)
	}
	return p, nil
}

// --- Helpers ---

func mustVersion(s string) *semver.Version {
	v, err := semver.NewVersion(s)
	if err != nil {
		panic(err)
	}
	return v
}

// installTestApp creates an apps/{name}/{version}/app.yml on disk.
func installTestApp(t *testing.T, installDir, name, version string, deps []Dependency) {
	t.Helper()
	dir := filepath.Join(installDir, name, version)
	require.NoError(t, os.MkdirAll(dir, 0755))

	type metaYAML struct {
		Name         string       `yaml:"name"`
		Version      string       `yaml:"version"`
		SDKVersion   string       `yaml:"sdk_version"`
		Command      string       `yaml:"command"`
		Dependencies []Dependency `yaml:"dependencies,omitempty"`
	}
	meta := metaYAML{
		Name:         name,
		Version:      version,
		SDKVersion:   "2",
		Command:      "dummy",
		Dependencies: deps,
	}
	data, err := yaml.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "app.yml"), data, 0644))
}

// createTestArchive creates a minimal .mass (zip) with app.yml inside.
func createTestArchive(path, _ string) error {
	// We can't easily create a zip here without archive/zip, so write a
	// app.yml directly. The resolver's installOne calls ExtractZip which
	// needs a real zip. For unit tests, we test resolution separately.
	// This is a placeholder — integration tests would use real archives.
	return os.WriteFile(path, []byte("PK"), 0644)
}

// --- Tests ---

func TestPickBest(t *testing.T) {
	tests := []struct {
		name        string
		versions    []string
		constraints []string
		want        string
	}{
		{
			name:        "no constraints picks highest",
			versions:    []string{"1.0.0", "2.0.0", "1.5.0"},
			constraints: nil,
			want:        "2.0.0",
		},
		{
			name:        "caret constraint",
			versions:    []string{"1.0.0", "1.5.0", "2.0.0"},
			constraints: []string{"^1.0.0"},
			want:        "1.5.0",
		},
		{
			name:        "gte constraint",
			versions:    []string{"0.5.0", "1.0.0", "1.5.0"},
			constraints: []string{">=1.0.0"},
			want:        "1.5.0",
		},
		{
			name:        "multiple constraints intersect",
			versions:    []string{"1.0.0", "1.5.0", "2.0.0", "2.5.0"},
			constraints: []string{">=1.0.0", "<2.0.0"},
			want:        "1.5.0",
		},
		{
			name:        "no match returns nil",
			versions:    []string{"1.0.0", "1.5.0"},
			constraints: []string{">=3.0.0"},
			want:        "",
		},
		{
			name:     "empty versions",
			versions: nil,
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var versions []*semver.Version
			for _, s := range tt.versions {
				versions = append(versions, mustVersion(s))
			}

			var constraints []*semver.Constraints
			for _, s := range tt.constraints {
				c, err := semver.NewConstraint(s)
				require.NoError(t, err)
				constraints = append(constraints, c)
			}

			got := pickBest(versions, constraints)
			if tt.want == "" {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tt.want, got.String())
			}
		})
	}
}

func TestTopoSort(t *testing.T) {
	tests := []struct {
		name    string
		graph   map[string]*graphNode
		want    []string
		wantErr bool
	}{
		{
			name: "simple chain A->B->C",
			graph: map[string]*graphNode{
				"A": {name: "A", deps: []Dependency{{Name: "B"}}},
				"B": {name: "B", deps: []Dependency{{Name: "C"}}},
				"C": {name: "C"},
			},
			want: []string{"C", "B", "A"},
		},
		{
			name: "diamond A->B,C; B->D; C->D",
			graph: map[string]*graphNode{
				"A": {name: "A", deps: []Dependency{{Name: "B"}, {Name: "C"}}},
				"B": {name: "B", deps: []Dependency{{Name: "D"}}},
				"C": {name: "C", deps: []Dependency{{Name: "D"}}},
				"D": {name: "D"},
			},
			want: []string{"D", "B", "C", "A"},
		},
		{
			name: "cycle detected",
			graph: map[string]*graphNode{
				"A": {name: "A", deps: []Dependency{{Name: "B"}}},
				"B": {name: "B", deps: []Dependency{{Name: "A"}}},
			},
			wantErr: true,
		},
		{
			name:  "empty graph",
			graph: map[string]*graphNode{},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := topoSort(tt.graph)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "cycle")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, order)
			}
		})
	}
}

func TestResolveFromInstalled(t *testing.T) {
	installDir := t.TempDir()

	// Install apps on disk.
	installTestApp(t, installDir, "foo", "1.0.0", nil)
	installTestApp(t, installDir, "foo", "1.5.0", nil)
	installTestApp(t, installDir, "bar", "2.0.0", nil)

	factory := &mockFactory{
		providers: map[string]*mockProvider{
			"github:test/foo": {versions: []*semver.Version{mustVersion("1.0.0"), mustVersion("1.5.0"), mustVersion("2.0.0")}},
		},
	}

	r := NewResolver(installDir, factory, zerolog.Nop())

	deps := []Dependency{
		{Name: "foo", Version: "^1.0.0", Source: "github:test/foo"},
		{Name: "bar", Version: ">=2.0.0"},
	}

	resolved, err := r.Resolve(context.Background(), deps)
	require.NoError(t, err)
	require.Len(t, resolved, 2)

	// Build a map for order-independent assertions.
	byName := make(map[string]ResolvedApp)
	for _, m := range resolved {
		byName[m.Name] = m
	}

	// foo: highest installed matching ^1.0.0 is 1.5.0
	require.Equal(t, "1.5.0", byName["foo"].Version.String())
	require.True(t, byName["foo"].Installed)

	// bar: 2.0.0 is installed and matches >=2.0.0
	require.Equal(t, "2.0.0", byName["bar"].Version.String())
	require.True(t, byName["bar"].Installed)
}

func TestResolveNeedsDownload(t *testing.T) {
	installDir := t.TempDir()

	// Only foo 1.0.0 installed but constraint requires ^2.0.0
	installTestApp(t, installDir, "foo", "1.0.0", nil)

	provider := &mockProvider{
		versions: []*semver.Version{mustVersion("1.0.0"), mustVersion("2.0.0"), mustVersion("2.5.0")},
	}
	factory := &mockFactory{
		providers: map[string]*mockProvider{"github:test/foo": provider},
	}

	r := NewResolver(installDir, factory, zerolog.Nop())

	deps := []Dependency{
		{Name: "foo", Version: "^2.0.0", Source: "github:test/foo"},
	}

	resolved, err := r.Resolve(context.Background(), deps)
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	require.Equal(t, "foo", resolved[0].Name)
	require.Equal(t, "2.5.0", resolved[0].Version.String())
	require.False(t, resolved[0].Installed)
}

func TestResolveConflictingConstraints(t *testing.T) {
	installDir := t.TempDir()

	// A depends on C ^1.0.0; B depends on C ^2.0.0 — no version can satisfy both.
	installTestApp(t, installDir, "A", "1.0.0", []Dependency{{Name: "C", Version: "^1.0.0", Source: "github:test/c"}})
	installTestApp(t, installDir, "B", "1.0.0", []Dependency{{Name: "C", Version: "^2.0.0", Source: "github:test/c"}})
	installTestApp(t, installDir, "C", "1.5.0", nil)

	provider := &mockProvider{
		versions: []*semver.Version{mustVersion("1.0.0"), mustVersion("1.5.0"), mustVersion("2.0.0"), mustVersion("2.5.0")},
	}
	factory := &mockFactory{
		providers: map[string]*mockProvider{"github:test/c": provider},
	}

	r := NewResolver(installDir, factory, zerolog.Nop())

	deps := []Dependency{
		{Name: "A", Version: ">=1.0.0"},
		{Name: "B", Version: ">=1.0.0"},
	}

	_, err := r.Resolve(context.Background(), deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no")
}

func TestResolveTransitiveDeps(t *testing.T) {
	installDir := t.TempDir()

	// A -> B -> C (all installed)
	installTestApp(t, installDir, "A", "1.0.0", []Dependency{{Name: "B", Version: ">=1.0.0"}})
	installTestApp(t, installDir, "B", "1.0.0", []Dependency{{Name: "C", Version: ">=1.0.0"}})
	installTestApp(t, installDir, "C", "1.0.0", nil)

	factory := &mockFactory{providers: map[string]*mockProvider{}}
	r := NewResolver(installDir, factory, zerolog.Nop())

	deps := []Dependency{
		{Name: "A", Version: ">=1.0.0"},
	}

	resolved, err := r.Resolve(context.Background(), deps)
	require.NoError(t, err)

	// Should resolve all 3 in topological order: C, B, A
	require.Len(t, resolved, 3)
	names := make([]string, len(resolved))
	for i, m := range resolved {
		names[i] = m.Name
	}
	require.Equal(t, []string{"C", "B", "A"}, names)
}

func TestListInstalledVersions(t *testing.T) {
	installDir := t.TempDir()

	installTestApp(t, installDir, "mod", "1.0.0", nil)
	installTestApp(t, installDir, "mod", "2.0.0", nil)
	installTestApp(t, installDir, "mod", "1.5.0", nil)

	r := NewResolver(installDir, nil, zerolog.Nop())
	versions := r.listInstalledVersions("mod")
	require.Len(t, versions, 3)
}

func TestMatchesAll(t *testing.T) {
	v := mustVersion("1.5.0")

	c1, err := semver.NewConstraint(">=1.0.0")
	require.NoError(t, err)
	c2, err := semver.NewConstraint("<2.0.0")
	require.NoError(t, err)
	c3, err := semver.NewConstraint(">=2.0.0")
	require.NoError(t, err)

	require.True(t, matchesAll(v, []*semver.Constraints{c1, c2}))
	require.False(t, matchesAll(v, []*semver.Constraints{c1, c3}))
	require.True(t, matchesAll(v, nil)) // no constraints = always match
}
