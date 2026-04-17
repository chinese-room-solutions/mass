package apps

import (
	"os/exec"
	"syscall"
)

// hideConsole prevents the subprocess from opening a visible console window.
func hideConsole(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW
	}
}
