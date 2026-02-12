package scheduler

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeLogLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string // substrings the output must contain
		exact    string   // if non-empty, output must equal this exactly
	}{
		{
			name:  "plain text unchanged",
			input: "2026-03-10T00:54:16Z INF module starting",
			exact: "2026-03-10T00:54:16Z INF module starting",
		},
		{
			name:  "empty string unchanged",
			input: "",
			exact: "",
		},
		{
			name:  "go-plugin JSON reformatted",
			input: `{"@level":"debug","@message":"plugin address","@timestamp":"2026-03-10T00:54:16.612820+01:00","address":"127.0.0.1:10000","network":"tcp"}`,
			contains: []string{
				"2026-03-10T00:54:16.612820+01:00",
				"DBG",
				"plugin address",
				"address=127.0.0.1:10000",
				"network=tcp",
			},
		},
		{
			name:  "zerolog-style JSON reformatted",
			input: `{"time":"2026-03-10T00:54:16Z","level":"info","message":"hello world","key":"val"}`,
			contains: []string{
				"2026-03-10T00:54:16Z",
				"INF",
				"hello world",
				"key=val",
			},
		},
		{
			name:  "warn level normalized",
			input: `{"@level":"warn","@message":"something","@timestamp":"2026-03-10T00:00:00Z"}`,
			exact: "2026-03-10T00:00:00Z WRN something",
		},
		{
			name:  "error level normalized",
			input: `{"@level":"error","@message":"bad thing","@timestamp":"2026-03-10T00:00:00Z"}`,
			exact: "2026-03-10T00:00:00Z ERR bad thing",
		},
		{
			name:  "invalid JSON unchanged",
			input: `{not json}`,
			exact: `{not json}`,
		},
		{
			name:  "JSON without log fields unchanged",
			input: `{"foo":"bar"}`,
			exact: `{"foo":"bar"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeLogLine(tt.input)
			if tt.exact != "" {
				require.Equal(t, tt.exact, got)
			}
			for _, sub := range tt.contains {
				require.True(t, strings.Contains(got, sub), "output %q should contain %q", got, sub)
			}
		})
	}
}
