// Command mass-setup is the terminal installer for MASS — the human face that
// configures MASS and installs it as a normal user-launched desktop app
// (staging the binary, creating a launcher, recording the location). MASS is a
// GUI program, not a system service, so there is no service registration here.
//
// Two faces, one outcome: run with no flags at a terminal to get the arrow-key
// wizard; pass flags (--install / --uninstall + --install-dir/--data-dir/...)
// to do the same non-interactively for scripted/fleet installs.
//
// Windows hosting note: the committed rsrc_windows_*.syso embed an asInvoker +
// supportedOS(Win10/11) manifest, built from mass-setup.exe.manifest by
// `make winres`. WITHOUT it, a double-clicked manifest-less console exe is
// treated as legacy under the "Let Windows decide" default-terminal setting and
// hosted by conhost, which draws a fixed, non-themeable gray scrollbar gutter.
// WITH the manifest, Windows routes it to Windows Terminal, whose scrollbar
// auto-hides and is theme-tinted — so the full-window indigo card reads clean.
// (It also stops the "*setup*"-name installer-detection heuristic from
// auto-elevating.)
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass-sdk/term"
	"github.com/chinese-room-solutions/mass-sdk/tui"
	"github.com/chinese-room-solutions/mass/internal/appspec"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

// appSpec is MASS's installer identity, shared with the daemon so both agree
// on the install record.
var appSpec = appspec.Spec

func main() {
	var (
		showVersion = flag.Bool("version", false, "Print version and exit")
		doInstall   = flag.Bool("install", false, "Install non-interactively (no wizard)")
		doUninstall = flag.Bool("uninstall", false, "Uninstall non-interactively (no wizard)")
		installDir  = flag.String("install-dir", "", "Install directory (default: per-scope)")
		dataDir     = flag.String("data-dir", "", "Data directory (default: per-scope)")
		listenAddr  = flag.String("listen-addr", "", "MASS listen address host:port (default: 127.0.0.1:3455)")
		scope       = flag.String("scope", "", "Install scope: user (no elevation) or system (machine-wide)")
		perUser     = flag.Bool("user", false, "Shorthand for --scope user")
		// The updater's flag, not the operator's: MASS's own self-update runs
		// this installer over a live install and needs the app back afterwards.
		relaunch = flag.Bool("relaunch", false,
			"With --install: wait for the running app's files to be replaceable, then start it again afterwards")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println("mass-setup", version)
		return
	}

	tag := "[ " + version + " ]"

	// Non-interactive faces: same outcome as the wizard, scripted. This is also
	// what the elevated install child runs (--install): it does the privileged
	// work and prints plain lines, while the parent wizard window shows the result.
	switch {
	case *doUninstall:
		c := defaultCollected()
		if err := applyFlags(&c, *installDir, *dataDir, *listenAddr, *scope, *perUser); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(runUninstall(c.installDir, c.perUser, tag, endPlain).code)
	case *doInstall:
		c := defaultCollected()
		if err := applyFlags(&c, *installDir, *dataDir, *listenAddr, *scope, *perUser); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		c.relaunch = *relaunch
		os.Exit(runInstall(c, tag, endPlain).code)
	}

	// Interactive wizard.
	os.Exit(runWizard(tag))
}

// applyFlags overlays non-empty CLI flags onto a collected config. The scope
// (--scope, or --user as shorthand) re-seeds the install/data dirs to that
// scope's defaults unless an explicit --install-dir / --data-dir overrides them,
// and sets perUser — so the elevated child (re-run with explicit dirs and
// --scope system) still places its launcher machine-wide.
func applyFlags(c *collected, installDir, dataDir, listenAddr, scope string, perUser bool) error {
	if scope != "" || perUser {
		s := install.AvailableScopes()[0]
		if scope != "" {
			parsed, err := install.ParseScope(scope)
			if err != nil {
				return err
			}
			s = parsed
		}
		if perUser {
			s = install.ScopeUser
		}
		c.scope = s
		c.installDir = appSpec.ScopeInstallDir(s)
		c.dataDir = scopeDataDir(s)
		c.perUser = s == install.ScopeUser
	}
	if installDir != "" {
		c.installDir = installDir
	}
	if dataDir != "" {
		c.dataDir = dataDir
	}
	if listenAddr != "" {
		c.listenAddr = listenAddr
	}
	return nil
}

// runWizard loops the arrow-key form: it shows the form, dispatches the chosen
// action, and — when the action's result screen returns "Back" — re-shows the
// form with the operator's edits intact, so only "Exit" closes the setup. Falls
// back to a brief linear notice when the terminal can't host the form.
func runWizard(tag string) int {
	fields := buildFields(loadPrefill())

	// One screen session for the whole wizard, like the C++ worker's outer
	// term::Screen: without it every form→phase→result transition flashes the
	// operator's PRIMARY screen back for a moment, and Konsole drapes whatever
	// text selection sits there over the next view's content. leaveSummary
	// releases early so the exit trace lands on the restored terminal.
	releaseScreen = tui.HoldScreen()
	defer releaseScreen()

	for {
		res, err := tui.RunForm(tui.FormSpec{
			BannerArt: massArt,
			Tag:       tag,
			Hint:      "Arrow keys move · edit a field · then choose an action.",
			Fields:    fields,
			Actions:   []string{"Install", "Uninstall", "Exit"},
			// Re-seed downstream fields on a change: the scope drives the
			// install/data dir defaults; a new data dir pre-fills from any config
			// already there.
			OnFieldEdited: func(idx int, fields []tui.Field) []tui.Field {
				switch idx {
				case fieldScope:
					return buildFields(prefillForScope(fields))
				case fieldDataDir:
					return buildFields(prefillForDataDir(fields))
				}
				return nil
			},
			// Snap the window to the form's grid via CSI 8t on Windows (the console
			// opens full-window) and macOS (Terminal.app opens at its profile default,
			// commonly 80×24, and honours the resize escape). NOT on Linux: the bundle
			// sizes konsole at launch, and a CSI 8t there desyncs cursor tracking,
			// stranding sudo's password caret a row below its prompt during an elevated
			// install.
			ResizeOnEnter: runtime.GOOS == "windows" || runtime.GOOS == "darwin",
		})
		if err != nil {
			releaseScreen() // the message belongs on the operator's own screen
			fmt.Fprintln(os.Stderr, "setup:", err)
			return 1
		}
		// Declined: the terminal can't host the form (too small, no raw mode).
		// Cancelled: the operator aborted. Either way show the linear notice
		// rather than falling through and treating it as a completed Install
		// (which, on Declined, flashes the window closed).
		if res.Declined || res.Cancelled {
			releaseScreen() // the notice belongs on the operator's own screen
			return runLinearFallback(tag)
		}

		// Keep the operator's edits so a "Back" from the result screen re-shows
		// the form exactly as they left it.
		fields = res.Fields
		c := collectFrom(res.Fields)

		var outcome actionOutcome
		switch res.ActionIndex {
		case actionInstall:
			outcome = runInstall(c, tag, endWizard)
		case actionUninstall:
			outcome = runUninstall(c.installDir, c.perUser, tag, endWizard)
		default: // Exit
			return 0
		}
		if outcome.back {
			continue // result screen → Back → re-show the form
		}
		return outcome.code
	}
}

// runLinearFallback is shown when raw-mode/the form is unavailable (piped, dumb
// terminal): a short non-interactive notice rather than a degraded form.
func runLinearFallback(tag string) int {
	fmt.Print(term.Banner(massArt, tag, term.TerminalWidth()))
	fmt.Println(term.Muted("    This terminal can't host the interactive wizard."))
	fmt.Println(term.Muted("    Re-run in a real terminal, or use --install / --uninstall with flags."))
	return 0
}

const (
	actionInstall = iota
	actionUninstall
)
