package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"testing"

	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/stretchr/testify/require"
)

// Identity of the theme package the fixture serves. The package name carries
// the theme- prefix; the installed theme id is the suffix.
const (
	fixtureThemePkg = "theme-neon"
	fixtureThemeID  = "neon"
)

// fixtureThemeCSS is a valid bare theme artifact: declarations and comments
// only, carrying its own label and base directives.
const fixtureThemeCSS = "/* label: Neon */\n/* base: light */\n--mass-bg-base: #101015;\n"

// newThemeRegistryFixture stands up an httptest server serving an index with
// one theme package (two versions, newest listed last per the index convention)
// alongside a runtime package that must never leak into theme results. The
// theme artifact is the real CSS, digest-pinned; tamperSHA pins a wrong digest
// so the download fails verification. pad adds that many filler theme packages
// ("Padding N") so the Available section can be pushed past its window.
func newThemeRegistryFixture(t *testing.T, tamperSHA bool, pad int) *registryFixture {
	t.Helper()
	css := []byte(fixtureThemeCSS)
	sha := sha256Hex(css)
	if tamperSHA {
		sha = sha256Hex([]byte("not the theme"))
	}
	platform := registry.RuntimePlatform("nonesuch", "nonesuch").Key()

	fix := &registryFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/theme.css", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(css)
	})
	mux.HandleFunc("/index.yml", func(w http.ResponseWriter, r *http.Request) {
		index := fmt.Sprintf(`schema_version: 1
packages:
  - name: %s
    kind: theme
    display_name: Neon
    description: A neon theme.
    versions:
      - version: 0.1.0
        artifacts:
          any: {url: "http://%s/theme.css", sha256: %s}
      - version: 0.2.0
        artifacts:
          any: {url: "http://%s/theme.css", sha256: %s}
  - name: some-runtime
    kind: runtime
    runtime_name: some-runtime
    display_name: Some Runtime
    versions:
      - version: 1.0.0
        mass: ">=0.1"
        artifacts:
          %s: {url: "http://%s/theme.css", sha256: %s}
`, fixtureThemePkg, r.Host, sha, r.Host, sha, platform, r.Host, sha)
		for i := 0; i < pad; i++ {
			index += fmt.Sprintf(`  - name: theme-pad%d
    kind: theme
    display_name: Padding %d
    versions:
      - version: 0.1.0
        artifacts:
          any: {url: "http://%s/theme.css", sha256: %s}
`, i, i, r.Host, sha)
		}
		_, _ = w.Write([]byte(index))
	})
	fix.server = httptest.NewServer(mux)
	t.Cleanup(fix.server.Close)
	fix.indexURL = fix.server.URL + "/index.yml"
	return fix
}

// newThemeTestHandler builds a test handler pointed at a theme registry with
// uikit's shared themes dir redirected into a temp dir, so installs land in
// scratch. uikit's theme registry is process-global, so any theme a case
// installs is unregistered again on cleanup.
func newThemeTestHandler(t *testing.T, tamperSHA bool) *Handler {
	t.Helper()
	return newPaddedThemeTestHandler(t, tamperSHA, 0)
}

// newPaddedThemeTestHandler is newThemeTestHandler with pad extra theme
// packages in the registry, for the windowing cases.
func newPaddedThemeTestHandler(t *testing.T, tamperSHA bool, pad int) *Handler {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Cleanup(func() { _ = uikit.RemoveTheme(fixtureThemeID) })

	fix := newThemeRegistryFixture(t, tamperSHA, pad)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	return h
}

// getThemeRegistry drives GET /api/themes/registry through the real mux with
// the given search query and returns the SSE body. Datastar sends signals for a
// GET action in the "datastar" query param, not the body.
func getThemeRegistry(t *testing.T, h *Handler, query string) string {
	t.Helper()
	return getThemeRegistryWindowed(t, h, query, 0, 0)
}

// getThemeRegistryWindowed is getThemeRegistry with explicit section windows; a
// zero limit is left out of the signals entirely, standing for a client that
// hasn't widened that section yet.
func getThemeRegistryWindowed(t *testing.T, h *Handler, query string, installedLimit, availableLimit int) string {
	t.Helper()
	signals := map[string]any{}
	if query != "" {
		signals["themeQuery"] = query
	}
	if installedLimit > 0 {
		signals["themeInstalledLimit"] = installedLimit
	}
	if availableLimit > 0 {
		signals["themeAvailableLimit"] = availableLimit
	}
	url := "/api/themes/registry"
	if len(signals) > 0 {
		sig, err := json.Marshal(signals)
		require.NoError(t, err)
		url += "?datastar=" + neturl.QueryEscape(string(sig))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w.Body.String()
}

// postTheme drives a theme install/remove through the real mux and returns the
// SSE body.
func postTheme(t *testing.T, h *Handler, path string) string {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
	return w.Body.String()
}

func TestHandleThemeRegistry(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "lists built-ins and the registry theme",
			// Built-in labels come from uikit; the registry row shows the newest
			// listed version, not the first.
			wantContains: []string{"theme-manager", "Carbon", "Cream", "built-in", "Neon", "0.2.0"},
			// Kind filtering: a runtime package must not reach the theme dialog.
			wantAbsent: []string{"Some Runtime"},
		},
		{
			name:         "query filters both sections",
			query:        "neon",
			wantContains: []string{"Neon", "No installed themes match your search."},
			wantAbsent:   []string{"Carbon"},
		},
		{
			name:  "query matching only an installed theme empties Available",
			query: "carbon",
			// Installed still lists Carbon; Available degrades to the labeled note.
			wantContains: []string{"Carbon", "No themes match your search."},
			wantAbsent:   []string{"Neon"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newThemeTestHandler(t, false)
			body := getThemeRegistry(t, h, tt.query)
			for _, want := range tt.wantContains {
				require.Contains(t, body, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, body, absent)
			}
		})
	}
}

// TestHandleThemeRegistry_Window pins the paging contract for the Available
// section: the server renders at most one window of rows, the "Show More" row
// appears only while rows are held back, and the widened window the row asks
// for is honored on the next fetch. The registry serves 13 installable themes
// (Neon + 12 padding), so both the default window and a once-widened one hold
// rows back.
func TestHandleThemeRegistry_Window(t *testing.T) {
	const pad = 12
	tests := []struct {
		name           string
		query          string
		availableLimit int
		wantRows       int
		wantShowMore   bool
	}{
		{
			name:         "default window caps the section and offers more",
			wantRows:     templates.ThemePageSize,
			wantShowMore: true,
		},
		{
			name:           "a widened window renders more rows, still capped",
			availableLimit: templates.ThemePageSize * 2,
			wantRows:       templates.ThemePageSize * 2,
			wantShowMore:   true,
		},
		{
			name: "the last window drops the Show More row",
			// 1 + pad installable themes all fit.
			availableLimit: 1 + pad,
			wantRows:       1 + pad,
			wantShowMore:   false,
		},
		{
			name:  "a query narrowing under the window drops the row",
			query: "neon",
			// Only Neon matches, so nothing is held back at the default window.
			wantRows:     1,
			wantShowMore: false,
		},
		{
			name: "a widened window still honors the query",
			// The query trims the list before the window does: 12 padding
			// themes under a window of 13 leaves nothing behind.
			query:          "padding",
			availableLimit: 1 + pad,
			wantRows:       pad,
			wantShowMore:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPaddedThemeTestHandler(t, false, pad)

			body := getThemeRegistryWindowed(t, h, tt.query, 0, tt.availableLimit)

			require.Equal(t, tt.wantRows, strings.Count(body, "/api/themes/install/"),
				"one Install button per rendered Available row")
			if tt.wantShowMore {
				require.Contains(t, body, "Show More")
				require.Contains(t, body, "$themeAvailableLimit =")
			} else {
				require.NotContains(t, body, "Show More")
			}
		})
	}
}

// TestHandleThemeRegistry_WindowsAreIndependent pins that each section carries
// its own window: widening Available leaves Installed at its own limit. Only two
// themes are built in, so Installed never pages here — the assertion is that the
// two signals don't share state.
func TestHandleThemeRegistry_WindowsAreIndependent(t *testing.T) {
	h := newPaddedThemeTestHandler(t, false, 12)

	body := getThemeRegistryWindowed(t, h, "", 1, templates.ThemePageSize*2)

	// Installed windowed to one row: Carbon shows, Cream is held back behind
	// its own Show More.
	require.Contains(t, body, "Carbon")
	require.NotContains(t, body, "Cream")
	require.Contains(t, body, "$themeInstalledLimit =")
	// Available got its own, wider window.
	require.Equal(t, templates.ThemePageSize*2, strings.Count(body, "/api/themes/install/"))
}

// TestHandleThemeRegistry_Unreachable pins the degrade contract: the installed
// list still renders from the live uikit registry, and only the Available
// section collapses to a labeled note.
func TestHandleThemeRegistry_Unreachable(t *testing.T) {
	h := newThemeTestHandler(t, false)
	url := h.cfg.RegistryURL
	// Point at a dead address with no cached index behind it.
	h.cfg.RegistryURL = url + ".missing"

	body := getThemeRegistry(t, h, "")
	require.Contains(t, body, "Registry unavailable")
	require.Contains(t, body, "Carbon", "installed themes must survive a registry failure")
	require.NotContains(t, body, "Neon")
}

func TestHandleThemeInstall(t *testing.T) {
	h := newThemeTestHandler(t, false)

	body := postTheme(t, h, "/api/themes/install/"+fixtureThemePkg)

	// Installed live in uikit, with the artifact's own directives applied.
	info, ok := uikit.LookupTheme(fixtureThemeID)
	require.True(t, ok)
	require.Equal(t, "Neon", info.Label)
	require.Equal(t, uikit.ThemeLight, info.Base)

	// The response re-renders the picker, the inlined CSS, and the dialog so
	// the theme is selectable without a reload, and releases the row. The menu
	// patch must be a replace, not a morph — morphing sl-menu's children stops
	// it emitting sl-select, deadening the picker and its Browse row.
	require.Contains(t, body, `id="theme-menu"`)
	require.Contains(t, body, "mode replace")
	require.Contains(t, body, `id="mass-themes"`)
	require.Contains(t, body, "html.sl-theme-neon")
	require.Contains(t, body, `id="theme-manager"`)
	require.Contains(t, body, `"themeBusy"`)
	// The row moved from Available to Installed, so it now offers Remove and
	// activates on click via the palette's signal+post contract.
	require.Contains(t, body, "/api/themes/remove/"+fixtureThemeID)
	require.Contains(t, body, "/internal/settings/theme")
}

func TestHandleThemeInstall_Errors(t *testing.T) {
	tests := []struct {
		name       string
		tamperSHA  bool
		pkg        string
		wantInBody string
	}{
		{
			name:       "unknown package",
			pkg:        "theme-nope",
			wantInBody: "Install failed",
		},
		{
			name:       "runtime package is not a theme",
			pkg:        "some-runtime",
			wantInBody: "is not a theme package",
		},
		{
			name:       "checksum mismatch",
			tamperSHA:  true,
			pkg:        fixtureThemePkg,
			wantInBody: "Install failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newThemeTestHandler(t, tt.tamperSHA)

			body := postTheme(t, h, "/api/themes/install/"+tt.pkg)

			require.Contains(t, body, tt.wantInBody)
			require.Contains(t, body, `id="theme-alert"`)
			// A failure must release the row, or every button stays disabled.
			require.Contains(t, body, `"themeBusy"`)
			_, ok := uikit.LookupTheme(fixtureThemeID)
			require.False(t, ok, "a failed install must not register a theme")
		})
	}
}

func TestHandleThemeRemove(t *testing.T) {
	h := newThemeTestHandler(t, false)
	postTheme(t, h, "/api/themes/install/"+fixtureThemePkg)
	_, ok := uikit.LookupTheme(fixtureThemeID)
	require.True(t, ok)

	body := postTheme(t, h, "/api/themes/remove/"+fixtureThemeID)

	_, ok = uikit.LookupTheme(fixtureThemeID)
	require.False(t, ok, "remove must unregister the theme live")
	require.Contains(t, body, `id="theme-menu"`)
	require.Contains(t, body, `id="theme-manager"`)
	require.NotContains(t, body, "html.sl-theme-neon", "the removed theme's CSS must leave the page")
}

// TestHandleThemeRemove_ActiveThemeFallsBack pins the recovery path: removing
// the theme the server is currently using resets it to the default built-in and
// patches the page, so it isn't left styled by CSS that no longer exists.
func TestHandleThemeRemove_ActiveThemeFallsBack(t *testing.T) {
	h := newThemeTestHandler(t, false)
	postTheme(t, h, "/api/themes/install/"+fixtureThemePkg)
	h.cfg.Theme = fixtureThemeID
	var saved bool
	h.saveFn = func() { saved = true }

	body := postTheme(t, h, "/api/themes/remove/"+fixtureThemeID)

	require.Equal(t, string(uikit.ThemeDark), h.cfg.Theme)
	require.True(t, saved, "the fallback must be persisted")
	require.Contains(t, body, `"theme"`)
	require.Contains(t, body, `"themeBase"`)
}

func TestHandleThemeRemove_Errors(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		wantInBody string
	}{
		{
			name:       "built-in is refused",
			id:         string(uikit.ThemeDark),
			wantInBody: "Built-in themes cannot be removed.",
		},
		{
			name:       "not installed",
			id:         "never-installed",
			wantInBody: "That theme is not installed.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newThemeTestHandler(t, false)

			body := postTheme(t, h, "/api/themes/remove/"+tt.id)

			require.Contains(t, body, tt.wantInBody)
			require.Contains(t, body, `"themeBusy"`)
		})
	}
}

// TestAvailableThemesExcludesInstalled pins that a theme already registered
// drops out of the Available list rather than showing an inert row.
func TestAvailableThemesExcludesInstalled(t *testing.T) {
	h := newThemeTestHandler(t, false)

	available, stale, err := h.availableThemes(context.Background(), "")
	require.NoError(t, err)
	require.False(t, stale)
	require.Len(t, available, 1)
	require.Equal(t, fixtureThemePkg, available[0].Name)
	require.Equal(t, fixtureThemeID, available[0].ID)
	require.Equal(t, "0.2.0", available[0].Version, "newest listed version wins")

	err = h.installThemeFromRegistry(context.Background(), fixtureThemePkg, "tester")
	require.NoError(t, err)

	available, _, err = h.availableThemes(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, available)
}

func TestNewestThemeVersion(t *testing.T) {
	tests := []struct {
		name     string
		versions []PackageVersionView
		want     string
		wantOK   bool
	}{
		{name: "empty", wantOK: false},
		{
			name: "last listed wins",
			versions: []PackageVersionView{
				{Version: "0.1.0", HasArtifact: true},
				{Version: "0.2.0", HasArtifact: true},
			},
			want: "0.2.0", wantOK: true,
		},
		{
			name: "skips versions without an artifact",
			versions: []PackageVersionView{
				{Version: "0.1.0", HasArtifact: true},
				{Version: "0.2.0", HasArtifact: false},
			},
			want: "0.1.0", wantOK: true,
		},
		{
			name:     "no artifact at all",
			versions: []PackageVersionView{{Version: "0.1.0", HasArtifact: false}},
			wantOK:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := newestListedVersion(tt.versions)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestWindowRows(t *testing.T) {
	rows := []string{"a", "b", "c", "d"}
	tests := []struct {
		name     string
		rows     []string
		limit    int
		want     []string
		wantNext int
	}{
		{name: "empty", limit: 5},
		{name: "under the window", rows: rows, limit: 5, want: rows},
		{name: "exactly the window", rows: rows, limit: 4, want: rows},
		{
			name: "over the window", rows: rows, limit: 2,
			want: rows[:2], wantNext: 2 + templates.ThemePageSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, next := windowRows(tt.rows, tt.limit, templates.ThemePageSize)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantNext, next)
		})
	}
}

func TestThemeIDFromPackage(t *testing.T) {
	tests := []struct {
		name string
		pkg  string
		want string
	}{
		{name: "strips the prefix", pkg: "theme-neon", want: "neon"},
		{name: "bare name maps to itself", pkg: "neon", want: "neon"},
		{name: "prefix only maps to itself", pkg: "theme-", want: "theme-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, themeIDFromPackage(tt.pkg))
		})
	}
}
