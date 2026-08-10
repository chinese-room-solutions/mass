package web

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/audit"
)

// Theme operations: listing the installed themes, browsing the registry's
// theme packages, and installing/removing them through the SDK's live uikit
// registry. Transport-neutral like the other ops files — they return the ops
// sentinels (see ops.go) and own their audit-log calls.
//
// A theme is a bare, self-describing .css file (the SDK validates it): the
// file name is the theme id and CSS class suffix, and /* label: */ and
// /* base: */ directives ride in its comments. It installs into the shared
// <config>/mass/themes dir that both MASS and Grimoire load, so an install is
// visible on the next render with no restart.

const (
	// registryThemeKind is the index kind for theme packages. The SDK leaves
	// Kind an open string, so the value lives with its consumer.
	registryThemeKind = registry.Kind("theme")
	// themePackagePrefix names theme packages: theme-<id>, where the suffix is
	// the theme id.
	themePackagePrefix = "theme-"
	// themeArtifactKeyAny is the platform key theme artifacts use — CSS is
	// platform-independent.
	themeArtifactKeyAny = "any"
	// maxThemeBytes caps a downloaded theme artifact. A theme is a page of CSS
	// declarations; anything past 1 MiB is not a theme.
	maxThemeBytes = 1 << 20
)

// InstalledTheme is one registered theme: built-ins plus everything loaded from
// the shared themes dir.
type InstalledTheme struct {
	ID      string // theme id: config value, $theme signal, CSS class suffix
	Label   string
	Base    string // "dark" | "light"
	Builtin bool   // built-ins can't be removed
}

// AvailableTheme is one theme package the registry offers that isn't installed
// yet, folded to the version an install would pick.
type AvailableTheme struct {
	Name        string // registry package name (theme-<id>)
	ID          string // theme id it installs as
	Label       string
	Description string
	Version     string
}

// installedThemes lists every registered theme, filtered by a case-insensitive
// substring match on label or id when query is non-empty. It reads the live
// uikit registry, so it works with no registry connectivity at all.
func (h *Handler) installedThemes(query string) []InstalledTheme {
	q := strings.ToLower(strings.TrimSpace(query))
	all := uikit.Themes()
	out := make([]InstalledTheme, 0, len(all))
	for _, ti := range all {
		id := string(ti.Name)
		if q != "" && !strings.Contains(strings.ToLower(ti.Label), q) && !strings.Contains(id, q) {
			continue
		}
		out = append(out, InstalledTheme{
			ID:      id,
			Label:   ti.Label,
			Base:    string(ti.Base),
			Builtin: ti.Name == uikit.ThemeDark || ti.Name == uikit.ThemeLight,
		})
	}
	return out
}

// availableThemes lists the registry's theme packages that aren't installed
// yet, honoring the search query. Stale reports that the index came from the
// cache because the registry was unreachable. Fetch failures map to
// ErrOpRegistry so the caller can show a note beside the installed list.
func (h *Handler) availableThemes(ctx context.Context, query string) (themes []AvailableTheme, stale bool, err error) {
	res, err := h.searchPackages(ctx, string(registryThemeKind), query, "")
	if err != nil {
		return nil, false, err
	}
	installed := make(map[string]bool)
	for _, ti := range uikit.Themes() {
		installed[string(ti.Name)] = true
	}
	out := make([]AvailableTheme, 0, len(res.Packages))
	for _, p := range res.Packages {
		id := themeIDFromPackage(p.Name)
		if installed[id] {
			continue
		}
		version, ok := newestListedVersion(p.Versions)
		if !ok {
			continue
		}
		label := p.DisplayName
		if label == "" {
			label = id
		}
		out = append(out, AvailableTheme{
			Name:        p.Name,
			ID:          id,
			Label:       label,
			Description: p.Description,
			Version:     version,
		})
	}
	return out, res.Stale, nil
}

// installThemeFromRegistry resolves a theme package's newest listed version,
// downloads its sha256-verified .css into the registry cache's temp dir, and
// installs it live through uikit — which validates it against the theme
// contract, writes it into the shared themes dir, and registers it immediately.
// Installing an already-installed theme overwrites it: that's the update path.
func (h *Handler) installThemeFromRegistry(ctx context.Context, name, actor string) error {
	if name == "" {
		return fmt.Errorf("%w: package name is required", ErrOpInvalid)
	}
	client, err := h.registryClient()
	if err != nil {
		return err
	}
	res, err := client.Fetch(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("fetching registry index")
		return fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}
	pkg := res.Index.FindPackage(name)
	if pkg == nil || pkg.Kind != registryThemeKind {
		return fmt.Errorf("%w: %s is not a theme package", ErrOpNotFound, name)
	}
	version, ok := newestListedVersion(themeVersionViews(pkg.Versions))
	if !ok {
		return fmt.Errorf("%w: %s has no installable version", ErrOpNotFound, name)
	}
	artifact, ok := themeArtifact(pkg, version)
	if !ok {
		return fmt.Errorf("%w: %s@%s has no %q artifact", ErrOpNotFound, name, version, themeArtifactKeyAny)
	}

	id := themeIDFromPackage(name)
	target := fmt.Sprintf("%s@%s", name, version)
	failed := func(err error) error {
		audit.Log(h.logger, "theme.installed", target, audit.OutcomeError).
			Str("actor", actor).Str("source", "registry").Str("error", err.Error()).Msg("")
		return err
	}

	tmpDir := filepath.Join(h.registryCacheDir(), "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return failed(fmt.Errorf("%w: creating download dir: %v", ErrOpRegistry, err))
	}
	destPath := filepath.Join(tmpDir, id+".css")
	defer func() { _ = os.Remove(destPath) }()

	if err := registry.Download(ctx, nil, artifact, destPath); err != nil {
		h.logger.Warn().Err(err).Str("package", target).Msg("downloading theme artifact")
		return failed(fmt.Errorf("%w: %w", ErrOpRegistry, err))
	}
	fi, err := os.Stat(destPath)
	if err != nil {
		return failed(fmt.Errorf("%w: reading downloaded theme: %v", ErrOpRegistry, err))
	}
	if fi.Size() > maxThemeBytes {
		return failed(fmt.Errorf("%w: theme artifact is %d bytes; the cap is %d", ErrOpInvalid, fi.Size(), maxThemeBytes))
	}
	css, err := os.ReadFile(destPath)
	if err != nil {
		return failed(fmt.Errorf("%w: reading downloaded theme: %v", ErrOpRegistry, err))
	}

	if _, err := uikit.InstallTheme(id, css); err != nil {
		return failed(fmt.Errorf("%w: %w", ErrOpInvalid, err))
	}
	audit.Log(h.logger, "theme.installed", target, audit.OutcomeOK).
		Str("actor", actor).Str("source", "registry").Msg("")
	return nil
}

// removeInstalledTheme deletes a pluggable theme by id, live. Built-ins are
// refused (uikit.ErrThemeBuiltin) and an id that isn't loaded is
// uikit.ErrThemeNotInstalled — both surface unwrapped so transports can map
// them.
func (h *Handler) removeInstalledTheme(id, actor string) error {
	if id == "" {
		return fmt.Errorf("%w: theme id is required", ErrOpInvalid)
	}
	if err := uikit.RemoveTheme(id); err != nil {
		audit.Log(h.logger, "theme.removed", id, audit.OutcomeError).
			Str("actor", actor).Str("error", err.Error()).Msg("")
		return err
	}
	audit.Log(h.logger, "theme.removed", id, audit.OutcomeOK).Str("actor", actor).Msg("")
	return nil
}

// themeIDFromPackage derives the theme id from its package name (theme-neon →
// neon). A name without the prefix maps to itself; uikit's name validation then
// decides.
func themeIDFromPackage(name string) string {
	if id, ok := strings.CutPrefix(name, themePackagePrefix); ok && id != "" {
		return id
	}
	return name
}

// newestListedVersion returns the newest version with an installable artifact.
// Versions are plain semver maintained append-newest-last in the hand-edited
// index, so "newest" is the last qualifying entry. It answers from the listing
// alone and so ignores compatibility ranges — where an install can resolve,
// prefer the version it picks (PackageView.Installable).
func newestListedVersion(versions []PackageVersionView) (string, bool) {
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].HasArtifact {
			return versions[i].Version, true
		}
	}
	return "", false
}

// themeVersionViews maps raw index versions onto the neutral view shape so
// newestListedVersion works on both the searched list and a freshly fetched
// package.
func themeVersionViews(versions []registry.Version) []PackageVersionView {
	out := make([]PackageVersionView, len(versions))
	for i, v := range versions {
		_, has := v.Artifacts[themeArtifactKeyAny]
		out[i] = PackageVersionView{Version: v.Version, HasArtifact: has}
	}
	return out
}

// themeArtifact returns the named version's platform-independent artifact.
func themeArtifact(pkg *registry.Package, version string) (registry.Artifact, bool) {
	for _, v := range pkg.Versions {
		if v.Version != version {
			continue
		}
		a, ok := v.Artifacts[themeArtifactKeyAny]
		return a, ok
	}
	return registry.Artifact{}, false
}
