package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/chinese-room-solutions/mass-sdk/selfupdate"
)

// The --relaunch half of the self-update: the app asks this installer to stage a
// newer build over its own install, then exits. Two things follow from that —
// the staged exe may still be locked while the old processes wind down (Windows
// holds a running image open), and once the new one is in place somebody has to
// start it again, since no operator is sitting at this terminal.

const (
	// replaceableWait bounds the wait for the old build's processes to exit. Past
	// it the install proceeds anyway: a stage that fails reports its own error,
	// which is a better message than a silent give-up here.
	replaceableWait = 30 * time.Second
	// replaceablePoll is how often the exe is probed meanwhile.
	replaceablePoll = 200 * time.Millisecond
)

// waitReplaceable blocks until exe can be replaced (POSIX: at once; Windows:
// once every process running it has exited), or until timeout. A path that
// doesn't exist yet is replaceable — a first install has nothing to wait for.
func waitReplaceable(exe string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for !selfupdate.Replaceable(exe) {
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(replaceablePoll)
	}
}

// startApp launches the freshly staged MASS detached, with no arguments — bare
// `mass` is the desktop app, which starts its own daemon on demand. The
// installer exits immediately after, so the child must not be tied to it —
// hence Release and the discarded streams.
func startApp(installDir string) error {
	exe := appSpec.StagedExePath(installDir)
	cmd := exec.Command(exe)
	cmd.Dir = filepath.Dir(exe)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", exe, err)
	}
	return cmd.Process.Release()
}
