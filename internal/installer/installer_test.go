package installer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// installTestModule creates a modules/{name}/{version}/module.yml.
func installTestModule(t *testing.T, installDir, name, version string) {
	t.Helper()
	dir := filepath.Join(installDir, name, version)
	require.NoError(t, os.MkdirAll(dir, 0755))

	meta := ModuleMetadata{
		Name:       name,
		Version:    version,
		SDKVersion: "1",
		Command:    "dummy",
	}
	data, err := yaml.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "module.yml"), data, 0644))
}

func TestCheckDependencies_Satisfied(t *testing.T) {
	installDir := t.TempDir()
	installTestModule(t, installDir, "foo", "1.5.0")
	installTestModule(t, installDir, "bar", "2.0.0")

	inst := NewInstaller("", installDir, zerolog.Nop())
	err := inst.CheckDependencies([]Dependency{
		{Name: "foo", Version: "^1.0.0"},
		{Name: "bar", Version: ">=2.0.0"},
	})
	require.NoError(t, err)
}

func TestCheckDependencies_NotInstalled(t *testing.T) {
	installDir := t.TempDir()
	inst := NewInstaller("", installDir, zerolog.Nop())

	err := inst.CheckDependencies([]Dependency{
		{Name: "missing", Version: ">=1.0.0"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not installed")
}

func TestCheckDependencies_VersionMismatch(t *testing.T) {
	installDir := t.TempDir()
	installTestModule(t, installDir, "foo", "1.0.0")

	inst := NewInstaller("", installDir, zerolog.Nop())
	err := inst.CheckDependencies([]Dependency{
		{Name: "foo", Version: "^2.0.0"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "none match")
}

func TestCheckDependencies_MultipleVersionsSatisfied(t *testing.T) {
	installDir := t.TempDir()
	installTestModule(t, installDir, "foo", "1.0.0")
	installTestModule(t, installDir, "foo", "2.0.0")

	inst := NewInstaller("", installDir, zerolog.Nop())
	err := inst.CheckDependencies([]Dependency{
		{Name: "foo", Version: "^2.0.0"},
	})
	require.NoError(t, err)
}

func TestCheckDependencies_NoConstraint(t *testing.T) {
	installDir := t.TempDir()
	installTestModule(t, installDir, "foo", "0.1.0")

	inst := NewInstaller("", installDir, zerolog.Nop())
	err := inst.CheckDependencies([]Dependency{
		{Name: "foo"}, // no version constraint
	})
	require.NoError(t, err)
}

func TestListInstalled_VersionedLayout(t *testing.T) {
	installDir := t.TempDir()
	installTestModule(t, installDir, "foo", "1.0.0")
	installTestModule(t, installDir, "foo", "2.0.0")
	installTestModule(t, installDir, "bar", "3.0.0")

	inst := NewInstaller("", installDir, zerolog.Nop())
	installed, err := inst.ListInstalled()
	require.NoError(t, err)
	require.Len(t, installed, 3)

	names := make(map[string]bool)
	for _, m := range installed {
		names[m.Name+"@"+m.Version] = true
	}
	require.True(t, names["foo@1.0.0"])
	require.True(t, names["foo@2.0.0"])
	require.True(t, names["bar@3.0.0"])
}

func TestListInstalled_LegacyFlatLayout(t *testing.T) {
	installDir := t.TempDir()

	// Legacy flat layout: modules/{name}/module.yml (no version subdir).
	dir := filepath.Join(installDir, "legacy")
	require.NoError(t, os.MkdirAll(dir, 0755))
	meta := ModuleMetadata{
		Name:       "legacy",
		Version:    "0.1.0",
		SDKVersion: "1",
		Command:    "dummy",
	}
	data, err := yaml.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "module.yml"), data, 0644))

	inst := NewInstaller("", installDir, zerolog.Nop())
	installed, err := inst.ListInstalled()
	require.NoError(t, err)
	require.Len(t, installed, 1)
	require.Equal(t, "legacy", installed[0].Name)
}

func TestListInstalled_EmptyDir(t *testing.T) {
	inst := NewInstaller("", t.TempDir(), zerolog.Nop())
	installed, err := inst.ListInstalled()
	require.NoError(t, err)
	require.Empty(t, installed)
}

func TestListInstalled_NonexistentDir(t *testing.T) {
	inst := NewInstaller("", filepath.Join(t.TempDir(), "nonexistent"), zerolog.Nop())
	installed, err := inst.ListInstalled()
	require.NoError(t, err)
	require.Nil(t, installed)
}
