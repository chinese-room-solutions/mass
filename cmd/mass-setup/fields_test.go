package main

import (
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/stretchr/testify/require"
)

// The factory defaults must never put the data dir at the install dir — uninstall
// deletes the install dir wholesale, so a collision would wipe the user's data.
func TestDefaultPrefill_InstallAndDataDistinct(t *testing.T) {
	p := defaultPrefill()
	require.Equal(t, install.ScopeUser, p.scope) // User leads (no elevation)
	require.NotEmpty(t, p.installDir)
	require.NotEmpty(t, p.dataDir)
	same, err := install.SameDir(p.installDir, p.dataDir)
	require.NoError(t, err)
	require.False(t, same, "install %q and data %q must differ", p.installDir, p.dataDir)
}

// The default scope is User, so a normal install needs no elevation.
func TestDefaultCollected_IsPerUser(t *testing.T) {
	c := defaultCollected()
	require.Equal(t, install.ScopeUser, c.scope)
	require.True(t, c.perUser)
	require.False(t, install.NeedsElevation(c.installDir))
}

// scopeDataDir moves the data dir with the scope: user → the per-user data dir,
// system → the SDK's machine-wide convention. The two must differ.
func TestScopeDataDir_MovesWithScope(t *testing.T) {
	user := scopeDataDir(install.ScopeUser)
	system := scopeDataDir(install.ScopeSystem)
	require.NotEmpty(t, user)
	require.Equal(t, appSpec.SystemDataDir(), system)
	require.NotEqual(t, user, system)
}

// collectFrom reads perUser from the Scope field; the install/data dirs are
// taken verbatim from the (possibly hand-edited) path fields.
func TestCollectFrom_PerUserFollowsScope(t *testing.T) {
	tests := []struct {
		name        string
		scope       install.Scope
		wantPerUser bool
	}{
		{"user scope", install.ScopeUser, true},
		{"system scope", install.ScopeSystem, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fields := buildFields(prefill{
				scope:      tc.scope,
				installDir: appSpec.ScopeInstallDir(tc.scope),
				dataDir:    scopeDataDir(tc.scope),
				listenAddr: ":3455",
			})
			c := collectFrom(fields)
			require.Equal(t, tc.scope, c.scope)
			require.Equal(t, tc.wantPerUser, c.perUser)
			require.Equal(t, appSpec.ScopeInstallDir(tc.scope), c.installDir)
		})
	}
}

// Flipping the Scope field re-seeds the install + data dirs to that scope's
// defaults, keeping the listen address.
func TestPrefillForScope_ReseedsDirs(t *testing.T) {
	// Start in the User defaults, then switch the field to System.
	fields := buildFields(defaultPrefill())
	fields[fieldScope].Value = install.ScopeSystem.Label()
	fields[fieldListenAddr].Value = ":9000"

	p := prefillForScope(fields)
	require.Equal(t, install.ScopeSystem, p.scope)
	require.Equal(t, appSpec.ScopeInstallDir(install.ScopeSystem), p.installDir)
	require.Equal(t, scopeDataDir(install.ScopeSystem), p.dataDir)
	require.Equal(t, ":9000", p.listenAddr) // preserved
}

// applyFlags maps --scope / --user onto the scope and its default dirs; explicit
// --install-dir / --data-dir override, and --user is shorthand for --scope user.
func TestApplyFlags_Scope(t *testing.T) {
	tests := []struct {
		name        string
		scopeFlag   string
		userFlag    bool
		installDir  string
		wantScope   install.Scope
		wantPerUser bool
		wantInstall string
	}{
		{"scope system", "system", false, "", install.ScopeSystem, false, appSpec.ScopeInstallDir(install.ScopeSystem)},
		{"scope user", "user", false, "", install.ScopeUser, true, appSpec.ScopeInstallDir(install.ScopeUser)},
		{"--user shorthand", "", true, "", install.ScopeUser, true, appSpec.ScopeInstallDir(install.ScopeUser)},
		{"--user overrides scope system", "system", true, "", install.ScopeUser, true, appSpec.ScopeInstallDir(install.ScopeUser)},
		{"explicit install-dir overrides scope default", "system", false, "/custom/mass", install.ScopeSystem, false, "/custom/mass"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := defaultCollected()
			require.NoError(t, applyFlags(&c, tc.installDir, "", "", tc.scopeFlag, tc.userFlag))
			require.Equal(t, tc.wantScope, c.scope)
			require.Equal(t, tc.wantPerUser, c.perUser)
			require.Equal(t, tc.wantInstall, c.installDir)
		})
	}
}
