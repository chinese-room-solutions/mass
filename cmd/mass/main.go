package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func init() {
	// Respect container memory limits (cgroup) with system fallback.
	_, _ = memlimit.SetGoMemLimitWithOpts(
		memlimit.WithProvider(
			memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem),
		),
	)
}

func main() {
	// Subcommand dispatch: a first non-flag argument (e.g. `mass status`)
	// runs the CLI and exits — `serve` (the headless daemon) is one of its
	// verbs. `-version` falls through to the flag handling below.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		os.Exit(runCLI(os.Args[1:]))
	}

	showVersion := flag.Bool("version", false, "Print version and exit")
	// `mass --help` lands here (leading dash skips the CLI dispatch above), so
	// stdlib usage alone would hide the management verbs. Show both.
	flag.Usage = func() {
		usage(os.Stderr)
		fmt.Fprintln(os.Stderr, "\nApp flags (bare `mass` opens the desktop app):")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("mass", version)
		return
	}

	// Bare `mass`: the desktop app — a thin client that attaches to the local
	// daemon, starting one on demand. In a nogui build it runs the daemon in
	// the foreground instead (see gui_nogui.go).
	os.Exit(runApp())
}
