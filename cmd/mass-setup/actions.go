package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/chinese-room-solutions/mass-sdk/install"
	"github.com/chinese-room-solutions/mass-sdk/term"
	"github.com/chinese-room-solutions/mass-sdk/tui"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/icon"
)

// stageIconFile writes the embedded app icon to a temp PNG so the installer can
// place it (the Linux icon theme / macOS bundle). Best-effort: a failure
// returns "" and the install proceeds with a generic icon.
func stageIconFile() string {
	path := filepath.Join(os.TempDir(), "mass-setup-icon.png")
	if err := os.WriteFile(path, icon.PNG, 0o644); err != nil {
		return ""
	}
	return path
}

// actionOutcome is what an install/uninstall reports back to the wizard loop:
// code is the process exit code for the scripted/Exit path, and back is true when
// the operator chose "Back" on the themed result screen and the wizard should
// re-show the form. (back is never set outside the wizard face.)
type actionOutcome struct {
	code int
	back bool
}

// endMode selects the end-of-action screen.
type endMode int

const (
	endPlain  endMode = iota // scripted/elevated child: plain stdout lines, no screen
	endWizard                // interactive wizard: themed Back/Exit (Back re-shows the form)
)

// runInstall writes MASS's config, stages the app, creates a launcher, and
// records the install. mode selects the end screen: themed Back/Exit for the
// wizard, or plain stdout lines for the scripted/elevated-child face.
func runInstall(c collected, tag string, mode endMode) actionOutcome {
	// The install dir and data dir must not coincide: uninstall removes the
	// install dir wholesale, so sharing it with the data dir would wipe the
	// operator's config/state on removal. The defaults are already distinct; this
	// guards a manual edit that makes them equal. A resolve error means a path we
	// can't normalize — fail closed rather than risk staging onto the data dir.
	if same, err := install.SameDir(c.installDir, c.dataDir); err != nil || same {
		return fail(tag, "The data directory must be different from the install "+
			"directory (uninstalling removes the install directory).", mode)
	}

	// Installing into a machine-wide location (e.g. C:\Program Files, /opt) needs
	// admin rights. Check once up front and, when needed, relaunch elevated —
	// rather than failing mid-stage with "Access is denied". NeedsElevation is
	// false for a user-scoped dir, an already-elevated process, or a dir the user
	// owns, so the prompt appears only when the write genuinely can't proceed.
	if install.NeedsElevation(c.installDir) {
		switch elevate("Installing to", c.installDir, installArgs(c), tag, mode) {
		case elevationRelaunched:
			// An elevated child is doing the privileged install in its own (UAC)
			// window; this window shows the result so Back still returns to the form.
			// The child owns the launcher and PATH steps, so report the intended
			// outcome here — its own console carries what actually happened.
			return finish(tag, installSummary(c, true, install.CLIResult{OnPath: true}), mode)
		case elevationDeclined:
			// Operator chose not to elevate → back to the form (no error), so they
			// can pick a user-writable directory instead.
			return actionOutcome{back: mode == endWizard}
		case elevationFailed:
			return fail(tag, "Installing to "+c.installDir+" needs Administrator rights "+
				"(the elevation prompt was dismissed or unavailable). Pick a user-writable directory instead.", mode)
		case elevationReady:
			// Already elevated (the relaunched child re-enters here) → fall through
			// and do the install in this process.
		}
	}

	// Self-update: the app that asked for this install is still exiting, and on
	// Windows its exe can't be overwritten until it has. Wait for the staged
	// binary to become replaceable before touching it.
	if c.relaunch {
		waitReplaceable(appSpec.StagedExePath(c.installDir), replaceableWait)
	}

	res, err := doInstall(c, tag, mode)
	if err != nil {
		return fail(tag, err.Error(), mode)
	}

	// Self-update: bring the app back up. A relaunch that fails is a warning —
	// the new build is staged, and the user can start it themselves.
	if c.relaunch {
		if err := startApp(c.installDir); err != nil {
			fmt.Fprintln(os.Stderr, term.FailMark()+"could not restart MASS: "+err.Error())
		}
	}

	// A launcher failure leaves LauncherPath empty; the install is still good.
	return finish(tag, installSummary(c, res.LauncherPath != "", res.CLI), mode)
}

// doInstall writes the config and installs, drawing the step list as it goes:
// without it the form just sits there while files are copied and the next thing the
// operator sees is the result screen. The phase page is closed before this returns,
// so the result screen doesn't nest inside it.
func doInstall(c collected, tag string, mode endMode) (install.Result, error) {
	ph, closePhase := openPhase(tag, mode)
	defer closePhase()

	// Persist the wizard-collected settings so MASS reads them on first launch.
	if err := saveConfig(c); err != nil {
		return install.Result{}, fmt.Errorf("writing config: %w", err)
	}
	ph.Line(term.OKMark() + term.Cool("Settings saved"))

	ph.Heading("Installing")
	ph.Line("Extracting files")

	res, err := appSpec.Install(install.Plan{
		InstallDir: c.installDir,
		DataDir:    c.dataDir,
		PerUser:    c.perUser,
		Icon:       stageIconFile(),
		Hooks: install.Hooks{
			Progress: ph.Progress,
			Step: func(step install.Step, err error) {
				reportStep(ph, c.installDir, step, err)
			},
		},
	})
	// Returned as-is: the SDK's install errors already name the step they failed at
	// (ErrStage is "install: staging failed"), and this message goes straight onto
	// the error screen — a second "install:" in front of it reads as a bug.
	return res, err
}

// openPhase prepares the step list of an action, with the close that ends it. In
// the wizard it gets a page of its own on the alternate screen — headed by the
// banner, every row indented onto the content band under it — which the close
// leaves again, so the operator's terminal is exactly as they left it. The scripted
// face, and the elevated child whose console ends up in the parent's log, print the
// same rows flush at column 0 on no screen at all: that output is a transcript.
func openPhase(tag string, mode endMode) (*term.Phase, func()) {
	if mode == endWizard {
		return tui.OpenPhase(massArt, tag, term.TerminalWidth())
	}
	ph := term.NewPhase(os.Stdout)
	return ph, ph.Close // still released, in case a step returned mid-bar
}

// reportStep draws one row of the install step list. The text stays short because
// the summary already carries where MASS and its data landed and how to launch it;
// these rows say only what happened. A best-effort step that failed (the launcher,
// PATH) marks itself ✖ and the install carries on — Install doesn't treat it as
// fatal either, since the binary is staged and runnable.
func reportStep(ph *term.Phase, installDir string, step install.Step, err error) {
	switch step {
	case install.StepStage:
		if err != nil {
			ph.Line(term.FailMark() + "Staging failed")
			return
		}
		ph.Line(term.OKMark() + "Staged MASS into " + installDir)
	case install.StepLauncher:
		if err != nil {
			ph.Line(term.FailMark() + "Could not create the launcher")
			return
		}
		ph.Line(term.OKMark() + "Created the launcher")
	case install.StepPath:
		if err != nil {
			ph.Line(term.FailMark() + "Could not expose `" + appSpec.ExeName + "` on PATH")
			return
		}
		ph.Line(term.OKMark() + "Exposed `" + appSpec.ExeName + "` on PATH")
	case install.StepRecord:
		// No row of its own: the summary names where the install landed, and a
		// failure here aborts to the error screen with the reason.
	}
}

// elevationResult is the outcome of the up-front admin-rights check.
type elevationResult int

const (
	elevationReady      elevationResult = iota // already elevated — proceed in this process
	elevationRelaunched                        // an elevated child was started; this process reports the result
	elevationDeclined                          // operator said No at the confirm — return to the form, no error
	elevationFailed                            // UAC/relaunch couldn't be started — report an error
)

// elevate asks the operator (themed Yes/No, Yes pre-selected) whether to relaunch
// as Administrator for a privileged action on dir, then does so with relaunchArgs.
// It mirrors the worker: the confirm keeps the wizard's look over the step, and a
// declined prompt cleanly returns to the form instead of erroring. In the scripted
// face (not the wizard) it skips the prompt and relaunches directly. verb is the
// action word for the prompt ("Installing to" / "Removing from").
func elevate(verb, dir string, relaunchArgs []string, tag string, mode endMode) elevationResult {
	if mode == endWizard {
		if choice, err := tui.Confirm(massArt, tag,
			[]string{
				verb + " " + dir + " requires Administrator rights.",
				"Relaunch as Administrator now?",
			}, true); err == nil && !choice {
			return elevationDeclined
		}
	}
	paintElevationNotice(tag)
	switch install.RelaunchElevated(relaunchArgs) {
	case install.ElevatedChildStarted, install.ElevatedWorkSucceeded:
		return elevationRelaunched
	case install.ElevationDeclined:
		return elevationDeclined
	default:
		return elevationFailed
	}
}

// paintElevationNotice themes the screen sudo is about to prompt on. On POSIX
// sudo reads the password inline on this same terminal, but we can't style its
// "[sudo] password for …:" line — so we set the stage above it: clear the screen
// and show the banner + a centered note, ending at the left margin so sudo's
// bare prompt lands cleanly on the next line. No-op on Windows, where elevation
// opens a separate UAC window rather than prompting here.
//
// Unlike the form screens, this does NOT run through term.OnPage: that pads the
// final line with background fill to width-1, leaving the cursor at the right
// edge so sudo's prompt would start mid-line and wrap. Plain centered lines +
// trailing newlines keep the cursor at column 0, matching the worker.
func paintElevationNotice(tag string) {
	if runtime.GOOS == "windows" {
		return
	}
	cols := term.TerminalWidth()
	var b strings.Builder
	if term.StylingEnabled() {
		// Leave any alt screen the preceding confirm modal entered, re-show the
		// cursor (the modal hid it with ?25l), and clear to a fresh home screen so
		// the banner + sudo's prompt land cleanly rather than over stale TUI output.
		b.WriteString("\033[?1049l\033[?25h\033[2J\033[H")
	}
	b.WriteString(term.Banner(massArt, tag, cols))
	b.WriteString("\n")
	b.WriteString(term.Center(term.Cool("Elevating with sudo to finish."), cols) + "\n")
	b.WriteString(term.Center(term.Muted("Enter your password at the prompt below (input is hidden)."), cols) + "\n\n")
	fmt.Print(b.String())
}

// installSummary is what an install leaves behind: where the app and its data went,
// and how to launch it. Kept lean — no per-step "Launcher created" chatter, and no
// row that restates the headline.
//
// The launcher and PATH steps are best-effort, and their ✖ in the step list dies
// with the phase screen — so a failure has to be said HERE, the one thing that
// survives, or the operator exits believing the install is complete and finds no
// menu entry. The launch row promises only what actually works, and a note names
// what was lost. launcherOK is false when the launcher step failed; cli reports the
// PATH outcome, whose own hint (bin dir not yet on PATH / reopen your terminal)
// rides along as a note too. Used by both the direct and elevated-relaunch paths.
func installSummary(c collected, launcherOK bool, cli install.CLIResult) term.Summary {
	var ways []string
	if launcherOK {
		ways = append(ways, "your applications menu")
	}
	if cli.OnPath {
		ways = append(ways, "`"+appSpec.ExeName+"` from a terminal")
	}
	launch := strings.Join(ways, ", or ")
	if launch == "" {
		launch = "the install directory above"
	}

	s := term.Summary{
		Kind:     term.SummaryOK,
		Headline: appSpec.DisplayName + " " + version + " installed",
		Rows: []term.SummaryRow{
			{Label: "installed", Value: c.installDir},
			{Label: "data", Value: c.dataDir},
			{Label: "launch", Value: launch},
		},
	}
	if !launcherOK {
		s.Rows = append(s.Rows, term.SummaryRow{Label: "note", Value: "no applications-menu entry " +
			"could be created; start " + appSpec.DisplayName + " from the install directory above"})
	}
	// A PATH step that failed outright reports no hint of its own, so say it.
	if !cli.OnPath && cli.Hint == "" {
		s.Rows = append(s.Rows, term.SummaryRow{Label: "note", Value: "`" + appSpec.ExeName +
			"` could not be added to your PATH; run it from the install directory above"})
	}
	if cli.Hint != "" {
		s.Rows = append(s.Rows, term.SummaryRow{Label: "note", Value: cli.Hint})
	}
	return s
}

// runUninstall removes the launcher, the staged install, and the record. mode
// selects the end screen (wizard Back/Exit, or plain lines for the scripted face).
func runUninstall(installDir string, perUser bool, tag string, mode endMode) actionOutcome {
	// When no install dir was given (the scripted --uninstall face), recover it
	// from the install record.
	if installDir == "" {
		if rec, err := appSpec.LoadRecord(); err == nil && rec != nil {
			installDir = rec.InstallDir
		}
	}
	if installDir == "" {
		installDir = appSpec.DefaultInstallDir()
	}

	// Uninstall deletes the install dir wholesale — confirm first in the wizard so
	// a stray Enter on the form's Uninstall button can't remove MASS unprompted.
	// "No" is pre-selected (destructive default). The scripted --uninstall face
	// skips this: it's already an explicit, non-interactive request (and what the
	// elevated child re-runs).
	if mode == endWizard {
		if choice, err := tui.Confirm(massArt, tag,
			[]string{
				"Remove MASS from " + installDir + "?",
				"Your data directory is left untouched.",
			}, false); err == nil && !choice {
			return actionOutcome{back: true}
		}
	}

	// Removing a machine-wide install needs admin rights too — same up-front
	// elevation gate as install, rather than failing on a denied unlink.
	if install.NeedsElevation(installDir) {
		switch elevate("Removing from", installDir, uninstallArgs(installDir, perUser), tag, mode) {
		case elevationRelaunched:
			return finish(tag, uninstallSummary(installDir, false), mode)
		case elevationDeclined:
			return actionOutcome{back: mode == endWizard}
		case elevationFailed:
			return fail(tag, "Removing "+installDir+" needs Administrator rights "+
				"(the elevation prompt was dismissed or unavailable).", mode)
		case elevationReady:
		}
	}

	selfSkipped, err := appSpec.Uninstall(installDir, perUser)
	if err != nil {
		return fail(tag, "removing "+installDir+": "+err.Error(), mode)
	}
	return finish(tag, uninstallSummary(installDir, selfSkipped), mode)
}

// uninstallSummary is what a removal leaves behind. Used by both the direct and
// elevated-relaunch paths (the latter can't read the child's filesLeft, so it
// reports the intended outcome).
func uninstallSummary(installDir string, filesLeft bool) term.Summary {
	s := term.Summary{
		Kind:     term.SummaryOK,
		Headline: appSpec.DisplayName + " " + version + " removed",
		Rows:     []term.SummaryRow{{Label: "from", Value: installDir}},
	}
	if filesLeft {
		s.Rows = append(s.Rows, term.SummaryRow{
			Label: "note",
			Value: "the install directory is in use by this uninstaller; remove it manually",
		})
	}
	return s
}

// installArgs / uninstallArgs build the non-interactive flag list the elevated
// child runs (--install / --uninstall), so it performs exactly the action the
// operator chose without re-prompting. The child does the privileged file work
// and closes; THIS window shows the result + Back, so the child draws no screen.
func installArgs(c collected) []string {
	args := []string{
		"--install",
		"--scope", string(c.scope),
		"--install-dir", c.installDir,
		"--data-dir", c.dataDir,
		"--listen-addr", c.listenAddr,
	}
	if c.relaunch {
		args = append(args, "--relaunch")
	}
	return args
}

func uninstallArgs(installDir string, perUser bool) []string {
	scope := install.ScopeSystem
	if perUser {
		scope = install.ScopeUser
	}
	return []string{"--uninstall", "--scope", string(scope), "--install-dir", installDir}
}

// saveConfig overlays the collected settings onto MASS's loaded config and
// writes it back, so MASS starts pre-configured.
func saveConfig(c collected) error {
	cfgDir, err := config.DefaultDir()
	if err != nil {
		return err
	}
	cfg, _, err := config.Load(cfgDir)
	if err != nil {
		return err
	}
	cfg.ListenAddr = c.listenAddr
	cfg.DataDir = c.dataDir
	return config.Save(cfg, cfgDir)
}

// finish ends a completed action: the themed Back/Exit screen in the wizard (Back
// re-shows the form), plain lines in the scripted face — and either way the
// summary is left on the operator's own terminal on the way out. Everything the
// wizard draws lives on the alternate screen and dies with it, so that trace is
// all they keep of where MASS landed and how to launch it.
//
// One summary, two layouts: on the screen its rows go in the form's field columns
// (SummaryRows), so the result reads as the form filled in; the trace gets the
// compact two columns Lines renders, which stand on their own in a scrollback.
func finish(tag string, s term.Summary, mode endMode) actionOutcome {
	if mode == endWizard {
		if back, err := tui.BackOrExit(massArt, tag, tui.SummaryRows(s, term.TerminalWidth())); err == nil {
			if back {
				return actionOutcome{back: true}
			}
			leaveSummary(os.Stdout, s) // the screen is restored by now
			return actionOutcome{}
		}
	}
	leaveSummary(os.Stdout, s)
	return actionOutcome{}
}

// fail reports an error on the end screen (or stderr for the scripted face), and
// leaves the reason on the restored terminal as finish leaves its summary. Exits
// with code 1. The error screen wraps + paints the message itself (✖ + accent),
// so pass it plain — a long abort message word-wraps to a margin instead of
// running edge-to-edge. In the wizard, Back returns to the form so the operator
// can correct and retry (e.g. pick a user-writable install dir).
func fail(tag, msg string, mode endMode) actionOutcome {
	s := term.Summary{Kind: term.SummaryFail, Headline: msg}
	if mode == endWizard {
		if back, err := tui.ErrorScreen(massArt, tag, msg); err == nil {
			if !back {
				leaveSummary(os.Stderr, s) // the screen is restored by now
			}
			return actionOutcome{code: 1, back: back}
		}
	}
	leaveSummary(os.Stderr, s)
	return actionOutcome{code: 1}
}

// leaveSummary prints the summary — the same lines the themed screen showed, from
// the same value, so the two can't disagree. Writing the result to the console is
// genuinely fire-and-forget (the console IS the output, no recovery path) — the
// AGENTS errcheck exemption.
// releaseScreen drops the wizard's held screen session (set by runWizard;
// a no-op for the scripted faces, which never hold one).
var releaseScreen = func() {}

func leaveSummary(w io.Writer, s term.Summary) {
	releaseScreen() // the trace belongs on the operator's own screen
	for _, l := range s.Lines() {
		_, _ = fmt.Fprintln(w, l)
	}
}
