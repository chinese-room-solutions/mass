package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// getAddWorkerOptions drives handleAddWorkerOptions with the given signals and
// returns the SSE body. Datastar carries GET-action signals in the "datastar"
// query param.
func getAddWorkerOptions(t *testing.T, h *Handler, worker, backend string) string {
	t.Helper()
	sig, err := json.Marshal(map[string]string{
		"addWorkerRuntime": "test-rt",
		"addWorkerWorker":  worker,
		"addWorkerBackend": backend,
	})
	require.NoError(t, err)
	url := "/api/workers/add-dialog-options?datastar=" + neturl.QueryEscape(string(sig))
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.handleAddWorkerOptions(w, r)
	return w.Body.String()
}

func TestHandleAddWorkerOptions_SinglePackage(t *testing.T) {
	fix := newMultiWorkerRegistryFixture(t, "linux", "amd64",
		workerPkgSpec{name: "test-rt-worker", backends: []string{"cuda"}})
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installTestRuntime(t, h)

	body := getAddWorkerOptions(t, h, "", "")
	require.Contains(t, body, `id="add-worker-worker-picker"`)
	require.Contains(t, body, "test-rt-worker Worker")
	// A lone package is pinned explicitly so the command carries &worker=. The
	// single backend hides the backend select.
	require.Contains(t, body, `"addWorkerWorker":"test-rt-worker"`)
	require.Contains(t, body, `"addWorkerBackend":""`)
	require.NotContains(t, body, `label="Backend"`)
}

func TestHandleAddWorkerOptions_MultiplePackagesAndBackends(t *testing.T) {
	fix := newMultiWorkerRegistryFixture(t, "linux", "amd64",
		workerPkgSpec{name: "aaa-worker", backends: []string{"cuda", "vulkan"}},
		workerPkgSpec{name: "bbb-worker", backends: []string{"cpu"}})
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installTestRuntime(t, h)

	// No current worker: defaults to the first package (sorted: aaa-worker),
	// which has >1 backend so the backend select appears with Auto.
	body := getAddWorkerOptions(t, h, "", "")
	require.Contains(t, body, "aaa-worker Worker")
	require.Contains(t, body, "bbb-worker Worker")
	require.Contains(t, body, `"addWorkerWorker":"aaa-worker"`)
	require.Contains(t, body, `label="Backend"`)
	require.Contains(t, body, ">Auto<")
	require.Contains(t, body, "vulkan")

	// Selecting the single-backend package hides the backend select and resets
	// the backend signal.
	body = getAddWorkerOptions(t, h, "bbb-worker", "vulkan")
	require.Contains(t, body, `"addWorkerWorker":"bbb-worker"`)
	require.Contains(t, body, `"addWorkerBackend":""`)
	require.NotContains(t, body, `label="Backend"`)

	// A still-valid backend for the multi-backend package is retained.
	body = getAddWorkerOptions(t, h, "aaa-worker", "vulkan")
	require.Contains(t, body, `"addWorkerWorker":"aaa-worker"`)
	require.Contains(t, body, `"addWorkerBackend":"vulkan"`)
}

func TestHandleAddWorkerOptions_RegistryUnreachable(t *testing.T) {
	fix := newMultiWorkerRegistryFixture(t, "linux", "amd64",
		workerPkgSpec{name: "test-rt-worker", backends: []string{"cuda"}})
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installTestRuntime(t, h)
	fix.server.Close() // registry now unreachable, no cache

	body := getAddWorkerOptions(t, h, "", "")
	require.Contains(t, body, ">Worker</label>")
	require.Contains(t, body, "load worker packages from the registry")
	require.Contains(t, body, `"addWorkerWorker":""`)
	require.Contains(t, body, `"addWorkerBackend":""`)
}

func TestHandleAddWorkerOptions_NoWorkerPackages(t *testing.T) {
	// Runtime installed, registry reachable, but no worker packages joined to it:
	// the Worker field must keep its labeled row and show the empty-state note
	// (not vanish), with both signals cleared.
	fix := newMultiWorkerRegistryFixture(t, "linux", "amd64")
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL
	installTestRuntime(t, h)

	body := getAddWorkerOptions(t, h, "", "")
	require.Contains(t, body, ">Worker</label>")
	require.Contains(t, body, "No workers for this runtime in the registry.")
	require.NotContains(t, body, "<sl-select")
	require.Contains(t, body, `"addWorkerWorker":""`)
	require.Contains(t, body, `"addWorkerBackend":""`)
}

func TestHandleAddWorkerOptions_RuntimeNotInstalled(t *testing.T) {
	fix := newMultiWorkerRegistryFixture(t, "linux", "amd64")
	h := newTestHandler(t)
	h.cfg.RegistryURL = fix.indexURL

	// No runtime installed → workerOptionsFor returns errRuntimeNotInstalled,
	// which the handler treats as a load failure (muted note, empty signals).
	body := getAddWorkerOptions(t, h, "", "")
	require.Contains(t, body, "load worker packages from the registry")
}
