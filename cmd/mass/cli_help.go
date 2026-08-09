package main

import (
	"fmt"
	"io"
	"strings"
)

// commandDoc is one command's entry in the CLI reference: the synopsis line
// shown in the top-level list, and the detail `<command> --help` adds under it.
// Detail carries what the synopsis can't — what the flags mean, which
// preconditions apply, what the exit code says.
type commandDoc struct {
	Name     string // verb chain, e.g. "models list"; matched against the args.
	Synopsis string // the command's arguments, as written on the usage line.
	Short    string // half-line gloss for the usage list; "" when the synopsis says it.
	Local    bool   // reaches no server, so the connection flags don't apply.
	Detail   string // free text, printed under the synopsis by --help.
}

// commands is the single source for both the top-level usage list and every
// `--help`. Order is the printed order.
var commands = []commandDoc{
	{"serve", "serve [--idle-timeout D]", "run the daemon in the foreground", true, `
Run the MASS daemon — API, dashboard, worker hub, runtime gateways — in the
foreground, with no window. This is what a server or a container runs;
Ctrl-C / SIGTERM stops it. With --idle-timeout the daemon retires itself
after that long with no client traffic (worker connections don't count) —
the GUI and the other verbs spawn it this way, detached with 2m, when no
daemon answers on the configured address. 0 (the default) never retires.`},

	{"status", "status", "orchestrator health overview", false, `
One-line summary of the orchestrator: version, listen address, how many
runtimes are installed and running, how many workers are known and online, and
the queued/running job counts. --json returns the same as a GetStatus message.`},

	{"models list", "models list [--runtime R]", "list installed models", false, `
List the models each runtime gateway holds — id, runtime, size, and the file it
came from. --runtime narrows to one gateway. The id is what an inference
request sends as "model".`},

	{"models import-local", "models import-local --runtime R --path P", "", false, `
Import a model file already on the server's disk into a runtime's model store.
--path is a path on the MASS host, not on the machine running the CLI.`},

	{"models import-remote", "models import-remote --runtime R --repo O/M --file F", "", false, `
Import a model from a remote repository (e.g. Hugging Face). The command
returns as soon as the download is planned — the bytes arrive in the
background, and the model appears in "models list" when it lands.`},

	{"models delete", "models delete --runtime R --id ID", "", false, `
Delete a model from a runtime's store, removing its files. Instances already
loaded from it keep running until evicted.`},

	{"runtimes list", "runtimes list", "list installed runtime gateways", false, `
List the installed runtime gateways — name, version, whether the process is
running, whether it auto-starts, and its description.`},

	{"runtimes search", "runtimes search [QUERY] [--kind K] [--runtime R]", "search the registry", false, `
Search the package registry. --kind filters to "runtime" or "worker";
--runtime filters to packages for one runtime. INSTALLABLE says whether any
version ships an artifact for the server's platform. An unreachable registry
degrades to a cached index with a warning on stderr.`},

	{"runtimes install", "runtimes install NAME[@version] | --path PKG.mass", "", false, `
Install a runtime gateway, and start it with auto-start on. NAME is the
registry PACKAGE name (mass-runtime-gateway-llama-cpp), not the runtime name
the package declares (llama-cpp) — both show in "runtimes search". --path
installs a .mass package from the server's disk instead. Downloads bytes, so it
runs under the 10m timeout.`},

	{"runtimes uninstall", "runtimes uninstall NAME", "", false, `
Remove an installed runtime gateway. NAME is the RUNTIME name (llama-cpp), not
the package name it was installed under.`},

	{"runtimes start", "runtimes start NAME", "", false, `
Start an installed runtime gateway's process. NAME is the runtime name.`},

	{"runtimes stop", "runtimes stop NAME", "", false, `
Stop a running runtime gateway. Workers bound to it lose their inference path
until it comes back.`},

	{"runtimes auto-start", "runtimes auto-start NAME --enabled=BOOL", "", false, `
Set whether a runtime gateway starts with MASS. Pass --enabled=true or
--enabled=false explicitly: the flag defaults to false, so omitting it turns
auto-start off.`},

	{"workers list", "workers list", "list the worker fleet", false, `
List every known worker — id, name, whether it is online, its runtime, and its
device IDs. The device IDs are what "workers device" and "workers benchmark"
take.`},

	{"workers install-local", "workers install-local [--runtime R] [--scope user|system] [--name N]", "install a worker on this host", false, `
Build and install a worker on the MASS host itself, enrolled against this
server. --scope user installs for the current user, system installs
machine-wide (needs privilege). With one runtime installed, --runtime is
inferred.`},

	{"workers join-command", "workers join-command [--runtime R] [--worker W] [--backend B] [--mass-url U] [--ttl D]", "mint a join token", false, `
Mint a single-use join token and print the command that enrolls a worker on
another machine with it. --ttl bounds how long the token stays valid;
--mass-url overrides the address the worker will dial (use it when the server's
own listen address isn't reachable from the worker).`},

	{"workers enable", "workers enable ID", "", false, `
Re-enable a worker for scheduling. A worker is enabled unless every one of its
devices was explicitly disabled.`},

	{"workers disable", "workers disable ID", "", false, `
Take a worker out of scheduling without disconnecting it. Its running jobs
finish; no new ones are dispatched.`},

	{"workers device", "workers device enable|disable WORKER DEVICE", "toggle one device", false, `
Enable or disable one device on one worker — how you drain a single GPU while
the worker's other devices keep serving. The setting persists across reconnects.`},

	{"workers benchmark", "workers benchmark [--workers a,b] [--devices x,y]", "re-measure throughput", false, `
Re-measure throughput for the selected workers and devices, refreshing what the
scheduler ranks on. Empty selectors mean the whole fleet. This loads a model on
every selected device and can run for minutes — the default timeout is 10m,
raise it with --timeout for a large fleet.`},

	{"scheduler list", "scheduler list", "list loaded model instances", false, `
List the model instances currently resident on workers — which worker, which
device, which model, and how busy each one is.`},

	{"scheduler evict", "scheduler evict --worker W --model M", "unload one instance", false, `
Unload one model instance from one worker, freeing its device memory. The next
request for that model pays the load cost again.`},

	{"queue list", "queue list", "inspect queued and running jobs", false, `
List the job queues, one section per queue, with each row's msg_id, model, and
whether it is in flight. The msg_id and request_id here are what the cancel
verbs take.`},

	{"queue cancel", "queue cancel --queue Q --msg-id ID", "cancel a job that hasn't started", false, `
Drop a queued job that hasn't been dispatched yet. Use "queue cancel-running"
for one already in flight.`},

	{"queue cancel-running", "queue cancel-running --request-id ID", "cancel a job in flight", false, `
Cancel a job already running on a worker, aborting its generation.`},

	{"queue evict", "queue evict --queue Q --msg-id ID", "drop a job without cancelling", false, `
Remove a queued message from its queue without signalling a cancellation to the
caller. For clearing a queue of jobs whose client is long gone.`},

	{"skill show", "skill show", "print the agent skill file", true, `
Print the agent skill — the Markdown instruction file that teaches an AI agent
to drive this CLI — to stdout, undecorated so it pipes. A bare "mass skill"
does the same. It ships inside the binary, so it always documents this build.
The file is agent-neutral: any agent that reads Markdown instructions can use it.`},

	{"skill install", "skill install DIR", "write the agent skill into DIR", true, `
Write the agent skill to DIR/mass-cli/SKILL.md, creating the directories as
needed. DIR is wherever your agent discovers skills — this command has no
built-in default and assumes no particular agent. An existing file is
overwritten: reinstalling after an upgrade is how the instructions stay in step
with the verbs. Reaches no server, so it works on a fresh install.`},
}

// helpRequested reports whether a command's own arguments ask for help: a bare
// -h/--help/-help token before any "--" terminator. A flag *value* of "--help"
// (`--name --help`) is indistinguishable here and reads as the request — write
// `--name=--help` when you genuinely mean that string.
func helpRequested(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "-h" || a == "--help" || a == "-help" {
			return true
		}
	}
	return false
}

// verbChain is the leading run of non-flag tokens in args — the verb chain the
// help request is about ("workers device --help" → ["workers", "device"]).
func verbChain(args []string) []string {
	var chain []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			break
		}
		chain = append(chain, a)
	}
	return chain
}

// printHelp writes the help for the verb chain in args: one command's synopsis
// and detail, or — for a group verb like "models", or nothing at all — the list
// of commands under it. Returns false when the chain matches no command, so the
// caller can fall through to its own usage error.
//
// The chain is tried longest-first and shortened on a miss, so a --help written
// after the command's own arguments (`mass runtimes start llama-cpp --help`)
// still finds the command rather than the value.
func printHelp(w io.Writer, chain []string) bool {
	if len(chain) == 0 {
		usage(w)
		return true
	}
	for n := len(chain); n > 0; n-- {
		if printCommandHelp(w, strings.Join(chain[:n], " ")) {
			return true
		}
	}
	return false
}

// printCommandHelp writes the help for exactly one verb chain: the command's
// own detail, or the member list of a group. Reports whether prefix matched.
func printCommandHelp(w io.Writer, prefix string) bool {
	for _, c := range commands {
		if c.Name == prefix {
			_, _ = fmt.Fprintf(w, "mass %s\n%s\n\n%s\n",
				c.Synopsis, strings.TrimRight(c.Detail, "\n"), flagsLine(c))
			return true
		}
	}
	// Not a full command: treat it as a group ("models", "queue") and list its
	// members, so `mass models --help` is useful rather than an error.
	var group []commandDoc
	for _, c := range commands {
		if strings.HasPrefix(c.Name, prefix+" ") {
			group = append(group, c)
		}
	}
	if len(group) == 0 {
		return false
	}
	_, _ = fmt.Fprintf(w, "%s commands:\n", prefix)
	for _, c := range group {
		_, _ = fmt.Fprintf(w, "  mass %s\n", c.Synopsis)
	}
	_, _ = fmt.Fprintf(w, "\nRun `mass %s <sub> --help` for one command's detail.\n", prefix)
	return true
}

// flagsLine is the footer under one command's help: the flags that reach it. A
// local command touches no server, so naming the connection flags there would
// send the reader looking for an effect they can't have. serve is local in
// that sense but has its own flag instead of --json.
func flagsLine(c commandDoc) string {
	if c.Name == "serve" {
		return "Flags: --idle-timeout DUR"
	}
	if c.Local {
		return "Flags: --json"
	}
	return "Flags: --addr URL, --token T, --json, --timeout DUR"
}

// commandList renders the Commands: block of the top-level usage from the same
// table --help reads, so the two can't drift.
func commandList() string {
	const col = 66 // where the gloss starts, when the synopsis leaves room.
	var b strings.Builder
	for _, c := range commands {
		line := "  " + c.Synopsis
		if c.Short != "" && len(line) < col {
			line += strings.Repeat(" ", col-len(line)) + c.Short
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}
