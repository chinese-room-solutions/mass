package main

import (
	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass-sdk/tui"
	"github.com/chinese-room-solutions/mass/internal/config"
)

// Field indices into buildFields() — kept in one place so the form, the
// scope/data-dir reload triggers, and collectFrom agree on the order. Theme and
// log level are deliberately NOT installer settings — they're trivially changed
// in the MASS UI, so the installer only collects what must be right before first
// launch (the scope, where the app lives, its data, and the address it serves).
const (
	fieldScope = iota
	fieldInstallDir
	fieldDataDir
	fieldListenAddr
)

// collected is everything the form gathers, mapped to MASS's config on install.
type collected struct {
	scope      install.Scope
	installDir string
	dataDir    string
	listenAddr string
	perUser    bool
}

// prefill is the seed for the form's fields, gathered from the install record +
// any config already present at the data dir.
type prefill struct {
	scope      install.Scope
	installDir string
	dataDir    string
	listenAddr string
}

// scopeDataDir is the default data dir for a scope: the per-user data dir
// (MASS owns that path) for ScopeUser, the SDK's machine-wide convention for
// ScopeSystem. Returns "" if the per-user dir can't be resolved.
func scopeDataDir(scope install.Scope) string {
	if scope == install.ScopeSystem {
		return appSpec.SystemDataDir()
	}
	d, err := config.DefaultDataDir()
	if err != nil {
		return ""
	}
	return d
}

// defaultCollected returns the per-OS defaults (used by the non-interactive
// install face before flags are overlaid).
func defaultCollected() collected {
	p := defaultPrefill()
	return collected{
		scope:      p.scope,
		installDir: p.installDir,
		dataDir:    p.dataDir,
		listenAddr: p.listenAddr,
		perUser:    p.scope == install.ScopeUser,
	}
}

// defaultPrefill is the per-OS factory default seed. The scope defaults to User
// (no elevation) — MASS is a user-launched desktop app, not a system service —
// and the install/data dirs follow from that scope. The operator can switch to
// System in the form, which moves both dirs to machine-wide locations and
// prompts for elevation on install.
func defaultPrefill() prefill {
	scope := install.AvailableScopes()[0] // User leads
	return prefill{
		scope:      scope,
		installDir: appSpec.ScopeInstallDir(scope),
		dataDir:    scopeDataDir(scope),
		listenAddr: config.DefaultListenAddr,
	}
}

// prefillForScope re-seeds the install + data dirs to the scope's defaults when
// the operator flips the Scope field, keeping the listen address.
func prefillForScope(fields []tui.Field) prefill {
	scope, err := install.ParseScope(fields[fieldScope].Value)
	if err != nil {
		scope = install.AvailableScopes()[0]
	}
	return prefill{
		scope:      scope,
		installDir: appSpec.ScopeInstallDir(scope),
		dataDir:    scopeDataDir(scope),
		listenAddr: fields[fieldListenAddr].Value,
	}
}

// loadPrefill seeds the form from the install record (a prior install's
// locations) then the config at that data dir, falling back to per-OS defaults.
// The scope is inferred from the recorded install dir so the Scope field shows
// the prior install's scope.
func loadPrefill() prefill {
	p := defaultPrefill()
	if rec, err := appSpec.LoadRecord(); err == nil && rec != nil {
		if rec.InstallDir != "" {
			p.installDir = rec.InstallDir
		}
		if rec.DataDir != "" {
			p.dataDir = rec.DataDir
		}
	}
	p.scope = scopeForInstallDir(p.installDir)
	return mergeConfigInto(p, p.dataDir)
}

// scopeForInstallDir infers the scope from an install dir: a user-scoped path is
// ScopeUser, anything machine-wide is ScopeSystem.
func scopeForInstallDir(dir string) install.Scope {
	if install.IsUserScoped(dir) {
		return install.ScopeUser
	}
	return install.ScopeSystem
}

// prefillForDataDir re-seeds downstream fields from the config at the data dir
// the operator just typed (called from the form's OnFieldEdited), keeping the
// scope, install dir, and the just-entered data dir.
func prefillForDataDir(fields []tui.Field) prefill {
	p := defaultPrefill()
	scope, err := install.ParseScope(fields[fieldScope].Value)
	if err != nil {
		scope = install.AvailableScopes()[0]
	}
	p.scope = scope
	p.installDir = fields[fieldInstallDir].Value
	p.dataDir = fields[fieldDataDir].Value
	return mergeConfigInto(p, p.dataDir)
}

// mergeConfigInto overlays any existing MASS config at dataDir onto p. A missing
// or unreadable config leaves the defaults — first-run is the common case.
func mergeConfigInto(p prefill, dataDir string) prefill {
	cfgDir, err := config.DefaultDir()
	if err != nil {
		return p
	}
	cfg, _, err := config.Load(cfgDir)
	if err != nil {
		return p
	}
	if cfg.ListenAddr != "" {
		p.listenAddr = cfg.ListenAddr
	}
	_ = dataDir // reserved: MASS config dir is fixed today; kept for symmetry
	return p
}

// buildFields builds the form's field list (in display order) from the prefill.
// Scope leads: it's the choice that drives the install/data dir defaults below.
func buildFields(p prefill) []tui.Field {
	return []tui.Field{
		fieldScope:      {Label: "Installation scope", Kind: tui.FieldChoice, Choices: scopeLabels(), Value: p.scope.Label()},
		fieldInstallDir: {Label: "Install directory", Kind: tui.FieldPath, Value: p.installDir},
		fieldDataDir:    {Label: "Data directory", Kind: tui.FieldPath, Value: p.dataDir},
		fieldListenAddr: {Label: "Listen address (host:port)", Kind: tui.FieldText, Value: p.listenAddr},
	}
}

// scopeLabels is the Scope field's choice list, in AvailableScopes order.
func scopeLabels() []string {
	scopes := install.AvailableScopes()
	labels := make([]string, len(scopes))
	for i, s := range scopes {
		labels[i] = s.Label()
	}
	return labels
}

// collectFrom assembles the collected result from the edited fields. perUser
// follows the chosen scope, while the elevation gate (NeedsElevation) reads the
// actual install dir — so a System scope with a hand-edited home path still
// behaves correctly.
func collectFrom(fields []tui.Field) collected {
	scope, err := install.ParseScope(fields[fieldScope].Value)
	if err != nil {
		scope = install.AvailableScopes()[0]
	}
	return collected{
		scope:      scope,
		installDir: fields[fieldInstallDir].Value,
		dataDir:    fields[fieldDataDir].Value,
		listenAddr: fields[fieldListenAddr].Value,
		perUser:    scope == install.ScopeUser,
	}
}
