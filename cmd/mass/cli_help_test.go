package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCLIHelp: every command answers --help with its own synopsis, exit 0, and
// without reaching the server — there is none in these tests, so a command that
// ran would fail on connect. Agents reach for --help before anything else.
func TestCLIHelp(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"leaf command", []string{"runtimes", "install", "--help"}, "runtimes install NAME[@version]"},
		{"leaf with -h", []string{"runtimes", "install", "-h"}, "runtimes install NAME[@version]"},
		{"single-word command", []string{"status", "--help"}, "mass status"},
		{"three-word chain", []string{"workers", "device", "--help"}, "workers device enable|disable"},
		{"group verb lists members", []string{"queue", "--help"}, "queue commands:"},
		{"group verb models", []string{"models", "--help"}, "models commands:"},
		{"help after arguments", []string{"runtimes", "start", "llama-cpp", "--help"}, "runtimes start NAME"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			out, errOut := capture(t, func() { code = runCLI(tt.args) })
			require.Equal(t, exitOK, code)
			require.Contains(t, out, tt.want)
			require.Empty(t, errOut, "help goes to stdout")
		})
	}
}

// TestCLIHelpFlagsLine: the footer names only the flags that reach the command.
// skill reaches no server, so the connection flags must not appear under it.
func TestCLIHelpFlagsLine(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantAddr bool
	}{
		{"server verb", []string{"queue", "list", "--help"}, true},
		{"local verb", []string{"skill", "install", "--help"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			out, _ := capture(t, func() { code = runCLI(tt.args) })
			require.Equal(t, exitOK, code)
			require.Contains(t, out, "--json")
			if tt.wantAddr {
				require.Contains(t, out, "--addr URL")
			} else {
				require.NotContains(t, out, "--addr")
			}
		})
	}
}

// TestCLIHelpUnknownChain: a --help for something that isn't a command is a
// usage error, not a silent exit 0.
func TestCLIHelpUnknownChain(t *testing.T) {
	var code int
	_, errOut := capture(t, func() { code = runCLI([]string{"bogus", "--help"}) })
	require.Equal(t, exitUsage, code)
	require.Contains(t, errOut, "unknown command")
}

// TestCLIHelpAfterTerminator: "--" ends the flags, so an argument literally
// spelled --help still reaches the command.
func TestCLIHelpAfterTerminator(t *testing.T) {
	require.False(t, helpRequested([]string{"note", "--", "--help"}))
	require.True(t, helpRequested([]string{"note", "--help", "--"}))
}

// TestCommandListCoversEveryVerb keeps the help table honest: every dispatch
// route must have an entry, or `<verb> --help` silently falls through to a
// usage error.
func TestCommandListCoversEveryVerb(t *testing.T) {
	routes := []string{
		"serve",
		"status",
		"models list", "models import-local", "models import-remote", "models delete",
		"runtimes list", "runtimes search", "runtimes install", "runtimes uninstall",
		"runtimes start", "runtimes stop", "runtimes auto-start",
		"workers list", "workers install-local", "workers join-command",
		"workers enable", "workers disable", "workers device", "workers benchmark",
		"scheduler list", "scheduler evict",
		"queue list", "queue cancel", "queue cancel-running", "queue evict",
		"skill show", "skill install",
		"update",
	}
	documented := make(map[string]bool, len(commands))
	for _, c := range commands {
		documented[c.Name] = true
		require.True(t, strings.HasPrefix(c.Synopsis, c.Name),
			"%q: the synopsis must start with the command name", c.Name)
		require.NotEmpty(t, strings.TrimSpace(c.Detail), "%q: --help needs detail", c.Name)
	}
	for _, r := range routes {
		require.True(t, documented[r], "%q has no help entry", r)
	}
	require.Len(t, commands, len(routes), "the table has an entry no verb dispatches")

	// Every documented chain must start at a top-level verb the dispatcher knows.
	for _, c := range commands {
		top, _, _ := strings.Cut(c.Name, " ")
		require.Contains(t, verbs, top, "%q documents a verb the dispatcher has no route for", c.Name)
	}
}
