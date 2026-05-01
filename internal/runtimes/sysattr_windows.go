package runtimes

import "syscall"

// hiddenSysProcAttr returns a SysProcAttr that hides the subprocess console
// window so launching a gateway from the GUI doesn't pop a black terminal.
//
// CREATE_NO_WINDOW (0x08000000) prevents allocating a console for the
// child; HideWindow is the documented Go flag that maps to it.
func hiddenSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x08000000}
}
