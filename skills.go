package mass

import "embed"

// AgentSkillName is the skill's directory name, used as the leaf when it is
// installed into an agent's skill directory.
const AgentSkillName = "mass-cli"

// AgentSkillFS holds the agent skill shipped inside the binary: a plain
// Markdown file teaching an AI agent to drive the mass CLI. It is not tied to
// any one agent — the format (YAML frontmatter, then instructions) is what the
// common skill loaders read, and `mass skill` hands it to whichever directory
// the user's agent looks in.
//
// It lives under skills/ at the repo root because go:embed only reaches files
// at or below its own directory.
//
//go:embed skills/mass-cli/SKILL.md
var AgentSkillFS embed.FS

// AgentSkill returns the skill's Markdown. The read cannot fail: go:embed
// resolves the path at build time.
func AgentSkill() string {
	data, err := AgentSkillFS.ReadFile("skills/" + AgentSkillName + "/SKILL.md")
	if err != nil {
		panic(err) // unreachable: the file is embedded at build time.
	}
	return string(data)
}
