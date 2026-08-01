package templates

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/a-h/templ"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/stretchr/testify/require"
)

// loadPluggableThemes registers two pluggable themes into the process-global
// uikit registry: a dark-based one ("synthwave") and a light-based one
// ("paper"). It redirects os.UserConfigDir at the platform-appropriate env var
// so uikit.LoadThemes scans a temp dir instead of the real one, and writes the
// theme files itself (not relying on the SDK's first-run seeding). The
// registry is global, so callers register once per test binary.
func loadPluggableThemes(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	var themesDir string
	switch runtime.GOOS {
	case "windows":
		t.Setenv("AppData", dir)
		themesDir = filepath.Join(dir, "mass", "themes")
	case "darwin":
		t.Setenv("HOME", dir)
		themesDir = filepath.Join(dir, "Library", "Application Support", "mass", "themes")
	default:
		t.Setenv("XDG_CONFIG_HOME", dir)
		themesDir = filepath.Join(dir, "mass", "themes")
	}
	require.NoError(t, os.MkdirAll(themesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(themesDir, "synthwave.css"),
		[]byte("/* base: dark */\n/* label: Synthwave */\n--mass-bg-base: #140c28;\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(themesDir, "paper.css"),
		[]byte("/* base: light */\n/* label: Paper */\n--mass-bg-base: #ffffff;\n"), 0o644))
	require.NoError(t, uikit.LoadThemes())
	// Sanity: both registered.
	_, ok := uikit.LookupTheme("synthwave")
	require.True(t, ok)
	_, ok = uikit.LookupTheme("paper")
	require.True(t, ok)
}

func TestHTMLThemeClass(t *testing.T) {
	loadPluggableThemes(t)
	tests := []struct {
		name  string
		theme string
		want  string
	}{
		{"dark builtin", "dark", "sl-theme-dark dark"},
		{"light builtin", "light", "sl-theme-light"},
		{"dark-base pluggable", "synthwave", "sl-theme-dark sl-theme-synthwave dark mass-theme-custom"},
		{"light-base pluggable", "paper", "sl-theme-light sl-theme-paper mass-theme-custom"},
		{"unknown falls back to dark", "nope", "sl-theme-dark dark"},
		{"empty falls back to dark", "", "sl-theme-dark dark"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, htmlThemeClass(tt.theme))
		})
	}
}

func TestBodyThemeClass(t *testing.T) {
	loadPluggableThemes(t)
	tests := []struct {
		name  string
		theme string
		want  string
	}{
		{"dark builtin", "dark", "bg-neutral-950 text-neutral-100"},
		{"light builtin", "light", "bg-neutral-100 text-neutral-900"},
		{"dark-base pluggable", "synthwave", "bg-neutral-950 text-neutral-100"},
		{"light-base pluggable keys on base", "paper", "bg-neutral-100 text-neutral-900"},
		{"unknown falls back to dark", "nope", "bg-neutral-950 text-neutral-100"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, bodyThemeClass(tt.theme))
		})
	}
}

// Layout must inline the loaded pluggable-theme CSS via themesStyle: templ
// treats <style> content as raw text, so an expression placed inside a style
// element renders literally instead of evaluating (the regression this guards).
func TestLayoutInlinesPluggableThemeCSS(t *testing.T) {
	loadPluggableThemes(t)
	var buf strings.Builder
	require.NoError(t, Layout("MASS", templ.Raw(""), "synthwave").Render(context.Background(), &buf))
	html := buf.String()
	require.Contains(t, html, "html.sl-theme-synthwave, .sl-theme-synthwave {")
	require.Contains(t, html, "window.__massThemes")
	require.NotContains(t, html, "@templ.Raw")
}

func TestDashboardSignalsTheme(t *testing.T) {
	loadPluggableThemes(t)
	tests := []struct {
		name      string
		theme     string
		wantTheme string
		wantBase  string
	}{
		{"dark builtin", "dark", "dark", "dark"},
		{"light builtin", "light", "light", "light"},
		{"dark-base pluggable", "synthwave", "synthwave", "dark"},
		{"light-base pluggable", "paper", "paper", "light"},
		{"unknown normalizes to dark", "nope", "dark", "dark"},
		{"empty normalizes to dark", "", "dark", "dark"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sig map[string]any
			require.NoError(t, json.Unmarshal([]byte(dashboardSignals(DashboardData{Theme: tt.theme})), &sig))
			require.Equal(t, tt.wantTheme, sig["theme"])
			require.Equal(t, tt.wantBase, sig["themeBase"])
		})
	}
}

// TestRenderThemeManagerLayout pins the dialog body's paging and sizing
// contract. The two sections must be independently scrollable grid rows in a
// panel that stops growing at the ceiling but shrinks back to content
// (max-content) — that combination is what lets a short section hand its unused
// height to the other one instead of reserving half regardless.
func TestRenderThemeManagerLayout(t *testing.T) {
	out := RenderThemeManager(ThemeManagerView{
		Installed:     []InstalledThemeView{{ID: "dark", Label: "Carbon", Base: "dark", Builtin: true}},
		Available:     []AvailableThemeView{{Name: "theme-neon", Label: "Neon", Version: "0.2.0"}},
		InstalledNext: 10,
		AvailableNext: 10,
	})

	require.Contains(t, out, "display:grid")
	require.Contains(t, out, "grid-template-rows:minmax(0,auto) minmax(0,auto)")
	require.Contains(t, out, "max-height:max-content")
	require.Equal(t, 2, strings.Count(out, "overflow-y:auto"), "each section scrolls on its own")

	// One Show More row per section, each widening only its own window.
	require.Equal(t, 2, strings.Count(out, "Show More"))
	require.Contains(t, out, "$themeInstalledLimit = 10")
	require.Contains(t, out, "$themeAvailableLimit = 10")
	require.Contains(t, out, `name="chevron-down"`)
}

// TestRenderThemeManagerNoShowMore pins the boundary: a zero next-window means
// the section is showing everything, so no row is offered.
func TestRenderThemeManagerNoShowMore(t *testing.T) {
	out := RenderThemeManager(ThemeManagerView{
		Installed: []InstalledThemeView{{ID: "dark", Label: "Carbon", Base: "dark", Builtin: true}},
		Available: []AvailableThemeView{{Name: "theme-neon", Label: "Neon"}},
	})
	require.NotContains(t, out, "Show More")
}
