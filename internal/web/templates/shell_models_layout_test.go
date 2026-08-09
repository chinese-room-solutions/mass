package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The Models tab is a two-row grid: the side panels and the list body share
// row 1, the Install/Browse bar owns row 2. That is what stops both panels at
// the bar's divider line instead of letting them run past it, so the grid
// placement of all four participants is load-bearing.
func TestModelsTabPanelsStopAtTheDivider(t *testing.T) {
	html := renderToString(DashboardPage(DashboardData{}))

	tests := []struct {
		name string
		want string
	}{
		{
			name: "tab is a two-row grid whose first row absorbs the free space",
			want: `id="models-tab" style="display:grid;grid-template-columns:auto minmax(0,44rem) auto;grid-template-rows:minmax(0,1fr) auto;`,
		},
		{name: "bench panel sits in row 1", want: `id="models-bench-panel"`},
		{name: "bench panel opens into row 1", want: `? 'grid-area:1/1;`},
		{name: "props panel opens into row 1", want: `? 'grid-area:1/3;`},
		{name: "button bar owns row 2", want: `id="models-list-footer" style="grid-area:2/2;`},
		{name: "button bar draws the divider", want: `border-top:1px solid var(--mass-border)`},
		{name: "props panel scrolls its own overflow", want: `overflow-y:auto;overflow-x:hidden;transition:width`},
		{name: "list keeps its own scroll area", want: `id="models-list-scroll" style="flex:1;overflow-y:auto;`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Contains(t, html, tt.want)
		})
	}

	// Closed or open, neither panel may claim a row of its own: both must
	// stay in row 1, whose height is the bound.
	for _, id := range []string{"models-bench-panel", "models-props-panel"} {
		start := strings.Index(html, `id="`+id+`"`)
		require.GreaterOrEqual(t, start, 0, id+" must exist")
		end := start + strings.Index(html[start:], ">")
		require.NotContains(t, html[start:end], "grid-area:2/", id+" must stay in grid row 1")
	}
}
