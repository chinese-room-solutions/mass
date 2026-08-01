package web

import (
	"archive/zip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeMassPackage builds a minimal .mass archive (a zip) at a temp path and
// returns it. manifestYML is written as runtime.yml; if withBinary is true a
// dummy bin/gateway entry is added. Used to drive the install error paths.
func writeMassPackage(t *testing.T, manifestYML string, withBinary bool) string {
	t.Helper()
	pkgPath := filepath.Join(t.TempDir(), "pkg.mass")
	f, err := os.Create(pkgPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	zw := zip.NewWriter(f)
	if manifestYML != "" {
		w, err := zw.Create("runtime.yml")
		require.NoError(t, err)
		_, err = w.Write([]byte(manifestYML))
		require.NoError(t, err)
	}
	if withBinary {
		w, err := zw.Create("bin/gateway")
		require.NoError(t, err)
		_, err = w.Write([]byte("#!/bin/sh\nexit 0\n"))
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return pkgPath
}

// installNamedRuntime installs a runtime with the given name into h from a
// freshly-built .mass package, so tests can set up a known installed set.
func installNamedRuntime(t *testing.T, h *Handler, name string) {
	t.Helper()
	pkgPath := filepath.Join(t.TempDir(), name+".mass")
	require.NoError(t, os.WriteFile(pkgPath, buildMassPackage(t, name, "1.0.0"), 0o600))
	_, err := h.installRuntime(pkgPath, "tester")
	require.NoError(t, err)
}

// postInstall drives handleInstallRuntime with the given packagePath signal and
// returns the recorded SSE response body.
func postInstall(t *testing.T, h *Handler, packagePath string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"packagePath": packagePath})
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/api/runtimes/install", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.handleInstallRuntime(w, r)
	return w.Body.String()
}

func TestHandleInstallRuntime_ValidationErrors(t *testing.T) {
	tests := []struct {
		name        string
		packagePath func(t *testing.T) string
		wantInBody  string
	}{
		{
			name:        "empty path",
			packagePath: func(*testing.T) string { return "" },
			wantInBody:  "Package path is required",
		},
		{
			name:        "whitespace-only path",
			packagePath: func(*testing.T) string { return "   " },
			wantInBody:  "Package path is required",
		},
		{
			name:        "nonexistent file",
			packagePath: func(*testing.T) string { return filepath.Join(t.TempDir(), "missing.mass") },
			wantInBody:  "install-error",
		},
		{
			name: "missing runtime.yml",
			packagePath: func(t *testing.T) string {
				t.Helper()
				return writeMassPackage(t, "", true)
			},
			wantInBody: "missing runtime.yml",
		},
		{
			name: "missing binary",
			packagePath: func(t *testing.T) string {
				t.Helper()
				return writeMassPackage(t, "runtime_name: test-rt\nversion: 1.0.0\n", false)
			},
			wantInBody: "missing the gateway binary",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			body := postInstall(t, h, tt.packagePath(t))
			require.Contains(t, body, tt.wantInBody)
			// Every error path re-enables the Install button by clearing
			// $installing so the dialog never gets stuck on the spinner.
			require.Contains(t, body, `"installing":false`)
		})
	}
}

func TestHandleListRuntimes_Empty(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodGet, "/api/runtimes", nil)
	w := httptest.NewRecorder()
	h.handleListRuntimes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	var out []runtimeView
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Empty(t, out)
}

func TestHandleRegistryAvailable(t *testing.T) {
	tests := []struct {
		name          string
		registryQuery string
		wantContains  []string
		wantAbsent    []string
	}{
		{
			name:         "no query lists the runtime",
			wantContains: []string{"registry-available", "Test Runtime", "0.1.0"},
		},
		{
			name:          "query matching the runtime keeps it",
			registryQuery: "test",
			wantContains:  []string{"registry-available", "Test Runtime"},
		},
		{
			name:          "query with no match shows the search-specific empty message",
			registryQuery: "nonexistent",
			wantContains:  []string{"registry-available", "No runtimes match your search."},
			wantAbsent:    []string{"Test Runtime", "No runtimes available in the registry."},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fix := newRegistryFixture(t, false)
			h := newTestHandler(t)
			h.cfg.RegistryURL = fix.indexURL

			// Datastar sends signals for a GET action in the "datastar" query
			// param, not the body (see datastar.ReadSignals).
			url := "/api/runtimes/registry"
			if tt.registryQuery != "" {
				sig, err := json.Marshal(map[string]string{"registryQuery": tt.registryQuery})
				require.NoError(t, err)
				url += "?datastar=" + neturl.QueryEscape(string(sig))
			}
			r := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()
			h.handleRegistryAvailable(w, r)

			body := w.Body.String()
			for _, want := range tt.wantContains {
				require.Contains(t, body, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, body, absent)
			}
		})
	}
}

// TestHandleRegistryAvailable_Window pins the list's paging: the default
// window renders one page plus the Show More row, and a widened $registryLimit
// renders everything with the row gone.
func TestHandleRegistryAvailable_Window(t *testing.T) {
	fix := newRegistryFixture(t, false)
	fix.pad = 7 // 8 packages total
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	get := func(limit int) string {
		url := "/api/runtimes/registry"
		if limit > 0 {
			sig, err := json.Marshal(map[string]int{"registryLimit": limit})
			require.NoError(t, err)
			url += "?datastar=" + neturl.QueryEscape(string(sig))
		}
		r := httptest.NewRequest(http.MethodGet, url, nil)
		w := httptest.NewRecorder()
		h.handleRegistryAvailable(w, r)
		return w.Body.String()
	}

	body := get(0)
	require.Contains(t, body, "Show More")
	require.Contains(t, body, "Pad Runtime 3") // 5th row of the first window
	require.NotContains(t, body, "Pad Runtime 4")

	body = get(10)
	require.NotContains(t, body, "Show More")
	require.Contains(t, body, "Pad Runtime 6")
}

// TestHandleRegistryAvailable_InstalledShowsRemove pins the row flip: an
// installed runtime's row offers Remove through the confirm dialog instead of
// a dead disabled Install button.
func TestHandleRegistryAvailable_InstalledShowsRemove(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installNamedRuntime(t, h, fixtureRuntimeName)

	r := httptest.NewRequest(http.MethodGet, "/api/runtimes/registry", nil)
	w := httptest.NewRecorder()
	h.handleRegistryAvailable(w, r)

	body := w.Body.String()
	require.Contains(t, body, "$confirmUninstall = &#39;test-rt&#39;")
	require.Contains(t, body, `name="trash"`)
}

func TestHandleRegistryAvailable_Unreachable(t *testing.T) {
	fix := newRegistryFixture(t, false)
	url := fix.indexURL
	fix.server.Close()
	h := newTestHandler(t)
	h.cfg.RegistryURL = url

	r := httptest.NewRequest(http.MethodGet, "/api/runtimes/registry", nil)
	w := httptest.NewRecorder()
	h.handleRegistryAvailable(w, r)
	require.Contains(t, w.Body.String(), "Registry unavailable")
}

func TestHandleRegistryInstall(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	r := httptest.NewRequest(http.MethodPost, "/api/runtimes/registry/install/test-rt", nil)
	r.SetPathValue("name", "test-rt")
	w := httptest.NewRecorder()
	h.handleRegistryInstall(w, r)

	require.True(t, h.runtimes.IsInstalled("test-rt"))
	body := w.Body.String()
	// The refreshed available list marks it installed and offers Remove.
	require.Contains(t, body, "Installed")
	require.Contains(t, body, "$confirmUninstall = &#39;test-rt&#39;")
	// The per-row loading signal is cleared so the button stops spinning and
	// every Install button re-enables.
	require.Contains(t, body, `"registryInstalling":""`)
	// The Workers tab's "Add worker" button follows the live runtime set.
	require.Contains(t, body, `"hasRuntimes":true`)
}

func TestHandleRegistryInstall_ChecksumFailureSurfaces(t *testing.T) {
	fix := newRegistryFixture(t, true) // tampered digest
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	r := httptest.NewRequest(http.MethodPost, "/api/runtimes/registry/install/test-rt", nil)
	r.SetPathValue("name", "test-rt")
	w := httptest.NewRecorder()
	h.handleRegistryInstall(w, r)

	require.False(t, h.runtimes.IsInstalled("test-rt"))
	body := w.Body.String()
	require.Contains(t, body, "registry-install-alert")
	// The error path must also clear the loading signal, otherwise a failed
	// install leaves every Install button disabled forever.
	require.Contains(t, body, `"registryInstalling":""`)
}

// TestHandleUninstallRuntime_RefreshesRegistry pins the dialog round-trip: a
// Remove clicked inside the registry dialog re-renders the list with the row
// flipped back to Install.
func TestHandleUninstallRuntime_RefreshesRegistry(t *testing.T) {
	fix := newRegistryFixture(t, false)
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installNamedRuntime(t, h, fixtureRuntimeName)

	r := httptest.NewRequest(http.MethodDelete, "/api/runtimes/test-rt", nil)
	r.SetPathValue("kind", "test-rt")
	w := httptest.NewRecorder()
	h.handleUninstallRuntime(w, r)

	body := w.Body.String()
	require.Contains(t, body, "registry-available")
	require.Contains(t, body, "registry/install/test-rt")
	require.NotContains(t, body, "$confirmUninstall = &#39;test-rt&#39;")
}

func TestHandleUninstallRuntime_UnknownIsNoOp(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodDelete, "/api/runtimes/nope", nil)
	r.SetPathValue("kind", "nope")
	w := httptest.NewRecorder()
	h.handleUninstallRuntime(w, r)

	// Uninstalling something that isn't installed is a graceful no-op: the
	// handler re-renders the (still-empty) runtime list via SSE rather than
	// erroring, and clears $activeRuntime so the pane returns to welcome.
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"activeRuntime":""`)
}

// postInstallWithSignals drives handleInstallRuntime with both a packagePath
// and an addWorkerRuntime signal, returning the SSE body.
func postInstallWithSignals(t *testing.T, h *Handler, packagePath, addWorkerRuntime string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"packagePath":      packagePath,
		"addWorkerRuntime": addWorkerRuntime,
	})
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/api/runtimes/install", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	h.handleInstallRuntime(w, r)
	return w.Body.String()
}

// deleteRuntimeWithSignals drives handleUninstallRuntime with an
// addWorkerRuntime signal, returning the SSE body. Datastar sends signals for a
// DELETE action in the "datastar" query param, not the body (see
// datastar.ReadSignals).
func deleteRuntimeWithSignals(t *testing.T, h *Handler, kind, addWorkerRuntime string) string {
	t.Helper()
	sig, err := json.Marshal(map[string]string{"addWorkerRuntime": addWorkerRuntime})
	require.NoError(t, err)
	url := "/api/runtimes/" + kind + "?datastar=" + neturl.QueryEscape(string(sig))
	r := httptest.NewRequest(http.MethodDelete, url, nil)
	r.SetPathValue("kind", kind)
	w := httptest.NewRecorder()
	h.handleUninstallRuntime(w, r)
	return w.Body.String()
}

// The Add-worker dialog's runtime picker must track the live runtime set: every
// install/uninstall re-patches #add-worker-runtime-picker and corrects the
// $addWorkerRuntime signal (empty→first, removed→first, valid→unchanged).
func TestHandleInstallRuntime_AddWorkerPicker(t *testing.T) {
	// First install into an empty MASS: the picker patch fires and the empty
	// selection is corrected to the just-installed runtime.
	h := newTestHandler(t)
	pkg := filepath.Join(t.TempDir(), "test-rt.mass")
	require.NoError(t, os.WriteFile(pkg, buildMassPackage(t, "test-rt", "1.0.0"), 0o600))
	body := postInstallWithSignals(t, h, pkg, "")
	require.Contains(t, body, `id="add-worker-runtime-picker"`)
	require.Contains(t, body, `"addWorkerRuntime":"test-rt"`)

	// Second install with a still-valid selection: the picker re-patches but the
	// valid selection is left untouched (no addWorkerRuntime signal patch).
	pkg2 := filepath.Join(t.TempDir(), "second-rt.mass")
	require.NoError(t, os.WriteFile(pkg2, buildMassPackage(t, "second-rt", "1.0.0"), 0o600))
	body = postInstallWithSignals(t, h, pkg2, "test-rt")
	require.Contains(t, body, `id="add-worker-runtime-picker"`)
	require.NotContains(t, body, `"addWorkerRuntime":`)
}

func TestHandleUninstallRuntime_AddWorkerPicker(t *testing.T) {
	tests := []struct {
		name           string
		current        string // client's $addWorkerRuntime before uninstall
		wantCorrection string // expected patched value, "" means no signal patch
	}{
		{
			name:           "removed runtime falls back to first remaining",
			current:        "rt-a", // rt-a is the one we uninstall
			wantCorrection: "rt-b",
		},
		{
			name:           "valid selection left untouched",
			current:        "rt-b", // survives the uninstall of rt-a
			wantCorrection: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			installNamedRuntime(t, h, "rt-a")
			installNamedRuntime(t, h, "rt-b")

			body := deleteRuntimeWithSignals(t, h, "rt-a", tt.current)
			require.Contains(t, body, `id="add-worker-runtime-picker"`)
			if tt.wantCorrection == "" {
				require.NotContains(t, body, `"addWorkerRuntime":`)
			} else {
				require.Contains(t, body, `"addWorkerRuntime":"`+tt.wantCorrection+`"`)
			}
		})
	}
}

func TestHandleUninstallRuntime_LastRuntimeClearsPicker(t *testing.T) {
	h := newTestHandler(t)
	installNamedRuntime(t, h, "only-rt")

	body := deleteRuntimeWithSignals(t, h, "only-rt", "only-rt")
	require.Contains(t, body, `id="add-worker-runtime-picker"`)
	// The removed runtime was the selection and nothing remains, so the signal
	// is corrected to "".
	require.Contains(t, body, `"addWorkerRuntime":""`)
}

// The Workers tab's "Add worker" button is gated on $hasRuntimes, so every
// runtime-set change must patch that signal — without it the button only
// catches up on a page reload.
func TestPatchAddWorkerPicker_HasRuntimesSignal(t *testing.T) {
	h := newTestHandler(t)
	pkg := filepath.Join(t.TempDir(), "test-rt.mass")
	require.NoError(t, os.WriteFile(pkg, buildMassPackage(t, "test-rt", "1.0.0"), 0o600))

	// Zero → one runtime turns the button on.
	require.Contains(t, postInstallWithSignals(t, h, pkg, ""), `"hasRuntimes":true`)

	// A second install re-asserts the signal even though the still-valid
	// $addWorkerRuntime selection needs no correction.
	pkg2 := filepath.Join(t.TempDir(), "second-rt.mass")
	require.NoError(t, os.WriteFile(pkg2, buildMassPackage(t, "second-rt", "1.0.0"), 0o600))
	body := postInstallWithSignals(t, h, pkg2, "test-rt")
	require.Contains(t, body, `"hasRuntimes":true`)
	require.NotContains(t, body, `"addWorkerRuntime":`)

	// Uninstalling down to one leaves it on; removing the last turns it off.
	require.Contains(t, deleteRuntimeWithSignals(t, h, "second-rt", "test-rt"), `"hasRuntimes":true`)
	require.Contains(t, deleteRuntimeWithSignals(t, h, "test-rt", "test-rt"), `"hasRuntimes":false`)
}
