package main

import "syscall"

var (
	kernel32      = syscall.NewLazyDLL("kernel32.dll")
	attachConsole = kernel32.NewProc("AttachConsole")
	allocConsole  = kernel32.NewProc("AllocConsole")
)

const attachParentProcess = ^uintptr(0) // ATTACH_PARENT_PROCESS = (DWORD)-1

// attachOrAllocConsole attaches to the parent process console (e.g. when
// launched from cmd.exe or PowerShell) or allocates a new one. An interactive
// `mass serve` needs it so its stderr logs are visible; a detached spawn
// skips it (its stderr is the spawn log).
func attachOrAllocConsole() {
	r, _, _ := attachConsole.Call(attachParentProcess)
	if r == 0 {
		allocConsole.Call() //nolint:errcheck // syscall
	}
}
