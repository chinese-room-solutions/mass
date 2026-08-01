package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// RenderAddWorkerRuntimePicker renders the runtime select only with a choice to
// make (>1 installed runtime); otherwise the stable-id container stays empty and
// hidden, so it doesn't consume a row-gap in the dialog's flex column.
func TestRenderAddWorkerRuntimePicker(t *testing.T) {
	tests := []struct {
		name         string
		rs           []RuntimeViewData
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "no runtimes hides the empty container",
			wantContains: []string{`id="add-worker-runtime-picker"`, `style="display:none"`},
			wantAbsent:   []string{"<sl-select"},
		},
		{
			name:         "single runtime hides the empty container (no choice)",
			rs:           []RuntimeViewData{{RuntimeName: "llama-cpp", DisplayName: "llama.cpp"}},
			wantContains: []string{`id="add-worker-runtime-picker"`, `style="display:none"`},
			wantAbsent:   []string{"<sl-select"},
		},
		{
			name: "several runtimes show a visible select",
			rs: []RuntimeViewData{
				{RuntimeName: "llama-cpp", DisplayName: "llama.cpp"},
				{RuntimeName: "dummy", DisplayName: "Dummy"},
			},
			wantContains: []string{"<sl-select", `label="Runtime"`, `value="llama-cpp"`, `value="dummy"`, "$addWorkerRuntime"},
			wantAbsent:   []string{"display:none"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderAddWorkerRuntimePicker(tt.rs)
			for _, want := range tt.wantContains {
				require.Contains(t, got, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, got, absent)
			}
		})
	}
}

// RenderAddWorkerWorkerPicker renders the worker select fragment. Zero options ⇒
// an empty container; any options ⇒ a select bound to $addWorkerWorker (a lone
// package is pinned too, so the command carries &worker=); a load failure ⇒ the
// muted fallback note. Every branch renders something, so the container is never
// hidden.
func TestRenderAddWorkerWorkerPicker(t *testing.T) {
	tests := []struct {
		name         string
		opts         []WorkerOptionView
		loadFailed   bool
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "zero options keeps the labeled row with an empty-state note",
			opts: nil,
			wantContains: []string{
				`id="add-worker-worker-picker"`,
				">Worker</label>",
				"No workers for this runtime in the registry.",
			},
			wantAbsent: []string{"<sl-select", "Couldn't load", "display:none"},
		},
		{
			name:         "single option is bound to the signal (pinned)",
			opts:         []WorkerOptionView{{Name: "llama-worker", DisplayName: "Llama Worker"}},
			wantContains: []string{"<sl-select", `label="Worker"`, `value="llama-worker"`, "Llama Worker", "$addWorkerWorker"},
			wantAbsent:   []string{`data-attr:value="''"`},
		},
		{
			name: "multiple options bound to signal",
			opts: []WorkerOptionView{
				{Name: "llama-worker", DisplayName: "Llama Worker"},
				{Name: "vllm-worker", DisplayName: "vLLM Worker"},
			},
			wantContains: []string{
				"$addWorkerWorker", `label="Worker"`,
				`value="llama-worker"`, "Llama Worker",
				`value="vllm-worker"`, "vLLM Worker",
				"/api/workers/add-dialog-options",
			},
		},
		{
			name:       "load failure renders the labeled row with a muted note",
			loadFailed: true,
			wantContains: []string{
				">Worker</label>",
				"load worker packages from the registry",
			},
			wantAbsent: []string{"<sl-select"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderAddWorkerWorkerPicker(tt.opts, tt.loadFailed)
			for _, want := range tt.wantContains {
				require.Contains(t, got, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, got, absent)
			}
		})
	}
}

// RenderAddWorkerBackendPicker renders the backend select only when the package
// advertises more than one backend; otherwise the container stays empty and
// hidden, so it doesn't consume a row-gap in the dialog's flex column. The empty
// "Auto" option is always first when shown.
func TestRenderAddWorkerBackendPicker(t *testing.T) {
	tests := []struct {
		name         string
		backends     []string
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "no backends hides the empty container",
			backends:     nil,
			wantContains: []string{`id="add-worker-backend-picker"`, `style="display:none"`},
			wantAbsent:   []string{"<sl-select"},
		},
		{
			name:         "single backend hides the select (no choice)",
			backends:     []string{"cuda"},
			wantContains: []string{`id="add-worker-backend-picker"`, `style="display:none"`},
			wantAbsent:   []string{"<sl-select"},
		},
		{
			name:         "multiple backends show Auto plus each in a visible container",
			backends:     []string{"cuda", "vulkan"},
			wantContains: []string{"<sl-select", `value=""`, "Auto", "cuda", "vulkan", "$addWorkerBackend"},
			wantAbsent:   []string{"display:none"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderAddWorkerBackendPicker(tt.backends)
			for _, want := range tt.wantContains {
				require.Contains(t, got, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, got, absent)
			}
		})
	}
}

func TestAddWorkerSelection(t *testing.T) {
	multi := []WorkerOptionView{
		{Name: "llama-worker", DisplayName: "Llama", Backends: []string{"cuda", "vulkan"}},
		{Name: "vllm-worker", DisplayName: "vLLM", Backends: []string{"cuda"}},
	}
	tests := []struct {
		name        string
		opts        []WorkerOptionView
		curWorker   string
		curBackend  string
		wantWorker  string
		wantBackend string
	}{
		{name: "zero options clears both", opts: nil, wantWorker: "", wantBackend: ""},
		{
			name:       "single option pins its name",
			opts:       []WorkerOptionView{{Name: "solo", DisplayName: "Solo", Backends: []string{"cuda", "vulkan"}}},
			wantWorker: "solo", wantBackend: "",
		},
		{
			name:      "single option retains a still-valid backend",
			opts:      []WorkerOptionView{{Name: "solo", DisplayName: "Solo", Backends: []string{"cuda", "vulkan"}}},
			curWorker: "solo", curBackend: "vulkan",
			wantWorker: "solo", wantBackend: "vulkan",
		},
		{
			name: "several options defaults to first package",
			opts: multi, curWorker: "", curBackend: "",
			wantWorker: "llama-worker", wantBackend: "",
		},
		{
			name: "several options retains a still-valid current worker",
			opts: multi, curWorker: "vllm-worker",
			wantWorker: "vllm-worker", wantBackend: "",
		},
		{
			name: "stale current worker falls back to first",
			opts: multi, curWorker: "gone-worker",
			wantWorker: "llama-worker", wantBackend: "",
		},
		{
			name: "valid backend retained for the selected worker",
			opts: multi, curWorker: "llama-worker", curBackend: "vulkan",
			wantWorker: "llama-worker", wantBackend: "vulkan",
		},
		{
			name: "backend invalid for the selected worker is reset",
			opts: multi, curWorker: "vllm-worker", curBackend: "vulkan",
			wantWorker: "vllm-worker", wantBackend: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWorker, gotBackend := AddWorkerSelection(tt.opts, tt.curWorker, tt.curBackend)
			require.Equal(t, tt.wantWorker, gotWorker)
			require.Equal(t, tt.wantBackend, gotBackend)
		})
	}
}

func TestBackendsForWorker(t *testing.T) {
	opts := []WorkerOptionView{
		{Name: "a", Backends: []string{"cuda", "vulkan"}},
		{Name: "b", Backends: []string{"cpu"}},
	}
	require.Equal(t, []string{"cuda", "vulkan"}, BackendsForWorker(opts, "a"))
	require.Equal(t, []string{"cpu"}, BackendsForWorker(opts, "b"))
	require.Nil(t, BackendsForWorker(opts, ""))
	require.Nil(t, BackendsForWorker(opts, "missing"))
}

// The download commands must fold in &worker= / &backend= only when the
// respective signal is non-empty (&amp; in the rendered attribute), and the run
// commands must gate --token on the auth-disabled signal.
func TestAddWorkerDialog_CommandsCarryWorkerBackend(t *testing.T) {
	got := renderToString(addWorkerDialog(DashboardData{
		Runtimes: []RuntimeViewData{{RuntimeName: "llama-cpp", DisplayName: "llama.cpp"}},
	}))
	// Download rows: worker-bin URL with uname params, pins gated on the signals.
	require.Contains(t, got, "/setup/worker-bin/' + $addWorkerRuntime")
	require.Contains(t, got, "?os=$(uname -s)&amp;arch=$(uname -m)")
	require.Contains(t, got, "?os=windows&amp;arch=AMD64")
	require.Contains(t, got, "'&amp;worker=' + $addWorkerWorker")
	require.Contains(t, got, "'&amp;backend=' + $addWorkerBackend")
	require.Contains(t, got, "$addWorkerWorker === ''")
	require.Contains(t, got, "$addWorkerBackend === ''")
	// Run rows: installer invocation with --token gated on the token being
	// non-empty (so a no-token MASS and the pre-mint window stay tokenless).
	require.Contains(t, got, "./mass-worker-setup --mass-url ")
	require.Contains(t, got, ".\\\\mass-worker-setup.exe --mass-url ")
	require.Contains(t, got, "$addWorkerToken === '' ? '' : ' --token ' + $addWorkerToken")
	require.NotContains(t, got, "$addWorkerAuthDisabled ? '' : ' --token '")
	// Run-row label.
	require.Contains(t, got, "2. Run it")
	// Each of the four copy buttons flips to a success check on click and resets
	// after a delay, keyed by its own row number (1-4). The reset timer is
	// single-flight (a shared window handle cleared on each click) so a later
	// click always gets the full delay from its own moment.
	for i := 1; i <= 4; i++ {
		require.Contains(t, got, fmt.Sprintf("$addWorkerCopied === %d ? 'check-lg' : 'clipboard'", i))
		require.Contains(t, got, fmt.Sprintf("clearTimeout(window._massCopyReset); $addWorkerCopied = %d; window._massCopyReset = setTimeout(() => $addWorkerCopied = 0, 1500)", i))
	}
	// Every command derives its base from the editable MASS-address signal (with
	// a trailing slash trimmed), not the browser origin.
	require.Contains(t, got, "$addWorkerMassURL.replace(/\\/$/, '')")
	require.NotContains(t, got, "+ window.location.origin +")
	// The address field is present and prefilled once per open from the origin
	// (only when empty, so an edited value survives reopen).
	require.Contains(t, got, `label="MASS address"`)
	require.Contains(t, got, "if($addWorkerMassURL === '') $addWorkerMassURL = window.location.origin")
	// A loopback address raises a muted warning.
	require.Contains(t, got, "workers on other machines can't reach it")
	// The dialog opens the options fetch alongside the token mint.
	require.True(t, strings.Contains(got, "/api/workers/add-dialog-options"))
}

// The Workers tab's "Add worker" button is always rendered and gated on the
// $hasRuntimes signal, so an install/uninstall can flip it over SSE without a
// reload. With zero runtimes it also carries a server-rendered display:none so
// it doesn't flash before Datastar hydrates.
func TestShell_AddWorkerButtonFollowsHasRuntimes(t *testing.T) {
	const button = `<sl-button size="small" data-show="$hasRuntimes"`
	tests := []struct {
		name       string
		rs         []RuntimeViewData
		wantHidden bool
	}{
		{name: "no runtimes renders the button hidden", wantHidden: true},
		{
			name: "one runtime renders the button visible",
			rs:   []RuntimeViewData{{RuntimeName: "llama-cpp", DisplayName: "llama.cpp"}},
		},
		{
			name: "several runtimes render the button visible",
			rs: []RuntimeViewData{
				{RuntimeName: "llama-cpp", DisplayName: "llama.cpp"},
				{RuntimeName: "dummy", DisplayName: "Dummy"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := DashboardData{Runtimes: tt.rs}
			got := renderToString(Shell(data))
			require.Contains(t, got, "Add worker")
			if tt.wantHidden {
				require.Contains(t, got, button+` style="display:none"`)
				require.Contains(t, dashboardSignals(data), `"hasRuntimes":false`)
			} else {
				require.Contains(t, got, button+` data-on:click="$addWorkerOpen = true">`)
				require.NotContains(t, got, button+` style="display:none"`)
				require.Contains(t, dashboardSignals(data), `"hasRuntimes":true`)
			}
		})
	}
}
