package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chinese-room-solutions/mass"
	"github.com/chinese-room-solutions/mass-sdk/fsutil"
)

// cmdSkill dispatches `mass skill`: the agent instruction file shipped inside
// the binary (mass.AgentSkillFS). Bare or `show` prints it to stdout so it can
// be piped anywhere; `install DIR` writes it into DIR. It reaches no server and
// no network — the file travels with the binary, so the instructions always
// describe the verbs this build actually has.
//
// Nothing here names a particular agent: the caller says where its agent reads
// skills from, and the layout written (DIR/<name>/SKILL.md) is what the common
// loaders expect.
func cmdSkill(args []string) int {
	if len(args) == 0 {
		return cmdSkillShow(nil)
	}
	switch args[0] {
	case "show":
		return cmdSkillShow(args[1:])
	case "install":
		return cmdSkillInstall(args[1:])
	default:
		return subUsage("skill", []string{"show", "install"})
	}
}

// cmdSkillShow handles `mass skill show` (and the bare `mass skill`): print the
// skill's Markdown to stdout, undecorated so it pipes cleanly. --json wraps it
// as {name, file, content}.
func cmdSkillShow(args []string) int {
	fs := flag.NewFlagSet("skill show", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit the skill as JSON")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		return failUsage("mass skill show [--json]")
	}
	content := mass.AgentSkill()
	if *asJSON {
		return printAnyJSON(map[string]string{
			"name":    mass.AgentSkillName,
			"file":    "SKILL.md",
			"content": content,
		})
	}
	fmt.Print(content)
	return exitOK
}

// cmdSkillInstall handles `mass skill install DIR`: write the skill to
// DIR/<name>/SKILL.md, creating DIR and the leaf as needed. An existing file is
// overwritten rather than refused — reinstalling after an upgrade is the point,
// since a stale skill describes verbs the binary may no longer have.
func cmdSkillInstall(args []string) int {
	fs := flag.NewFlagSet("skill install", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit the installed path as JSON")
	name, rest := peelName(args)
	if err := fs.Parse(rest); err != nil {
		return exitUsage
	}
	if name == "" || fs.NArg() != 0 {
		return failUsage("mass skill install DIR (the directory your agent reads skills from)")
	}
	dir := filepath.Join(name, mass.AgentSkillName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "error: creating %s: %s\n", dir, err)
		return exitError
	}
	path := filepath.Join(dir, "SKILL.md")
	if err := fsutil.WriteFileAtomic(path, []byte(mass.AgentSkill()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error: writing %s: %s\n", path, err)
		return exitError
	}
	if *asJSON {
		return printAnyJSON(map[string]string{"name": mass.AgentSkillName, "path": path})
	}
	return confirm("installed %s to %s", mass.AgentSkillName, path)
}
