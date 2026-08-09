package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass"
	"github.com/stretchr/testify/require"
)

// TestCLISkillShow: the bare verb and `show` both print the embedded skill with
// nothing added around it, so `mass skill > SKILL.md` yields the file itself.
func TestCLISkillShow(t *testing.T) {
	for _, args := range [][]string{{"skill"}, {"skill", "show"}} {
		var code int
		out, errOut := capture(t, func() { code = runCLI(args) })
		require.Equal(t, exitOK, code, "%v", args)
		require.Equal(t, mass.AgentSkill(), out, "%v must print the file verbatim", args)
		require.Empty(t, errOut)
	}
}

// TestCLISkillShowJSON: --json wraps the same bytes in an API shape, so a script
// can read the skill without parsing Markdown out of a stream.
func TestCLISkillShowJSON(t *testing.T) {
	var code int
	out, _ := capture(t, func() { code = runCLI([]string{"skill", "show", "--json"}) })
	require.Equal(t, exitOK, code)

	var got struct {
		Name    string `json:"name"`
		File    string `json:"file"`
		Content string `json:"content"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, "mass-cli", got.Name)
	require.Equal(t, "SKILL.md", got.File)
	require.Equal(t, mass.AgentSkill(), got.Content)
}

// TestCLISkillInstall: the skill lands at DIR/<name>/SKILL.md with the
// directories created, and a second install overwrites — the upgrade path, so a
// stale skill can't outlive the binary it documents.
func TestCLISkillInstall(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "agent", "skills")
	path := filepath.Join(dir, "mass-cli", "SKILL.md")

	var code int
	out, errOut := capture(t, func() { code = runCLI([]string{"skill", "install", dir}) })
	require.Equal(t, exitOK, code)
	require.Contains(t, out, path)
	require.Empty(t, errOut)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, mass.AgentSkill(), string(data))

	require.NoError(t, os.WriteFile(path, []byte("stale"), 0o644))
	_, _ = capture(t, func() { code = runCLI([]string{"skill", "install", dir}) })
	require.Equal(t, exitOK, code)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, mass.AgentSkill(), string(data))
}

// TestCLISkillUsage: the misuses that would otherwise write somewhere unintended
// stop at exit 2.
func TestCLISkillUsage(t *testing.T) {
	tests := [][]string{
		{"skill", "bogus"},
		{"skill", "install"},
		{"skill", "install", "a", "b"},
		{"skill", "show", "extra"},
	}
	for _, args := range tests {
		var code int
		_, _ = capture(t, func() { code = runCLI(args) })
		require.Equal(t, exitUsage, code, "%v", args)
	}
}
