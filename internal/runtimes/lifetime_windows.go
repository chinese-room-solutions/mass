package runtimes

import (
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Gateways are stopped by Manager.Shutdown on a clean exit, but a kill that
// runs no shutdown code — taskkill /F, a crash, the machine's session ending —
// leaves them running, still holding a model resident, with no parent to talk
// to. go-plugin has no protection of its own: it hands the child MASS's stdin
// rather than a pipe, so the child never sees an EOF when MASS dies.
//
// On Windows the OS can enforce it. Every gateway joins one job object created
// with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE; the job's only handle belongs to
// this process, so however MASS dies, Windows closes it and terminates
// everything in the job. Descendants a gateway spawns are in the job too.
var (
	jobOnce   sync.Once
	jobHandle windows.Handle
	jobErr    error
)

func childJob() (windows.Handle, error) {
	jobOnce.Do(func() {
		h, err := windows.CreateJobObject(nil, nil)
		if err != nil {
			jobErr = fmt.Errorf("creating job object: %w", err)
			return
		}
		info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
			BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
				LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
			},
		}
		if _, err := windows.SetInformationJobObject(h, windows.JobObjectExtendedLimitInformation,
			uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
			_ = windows.CloseHandle(h)
			jobErr = fmt.Errorf("setting job limits: %w", err)
			return
		}
		jobHandle = h
	})
	return jobHandle, jobErr
}

// superviseChild ties pid's lifetime to this process. The handle is closed
// straight away — the job holds its own reference to the process.
func superviseChild(pid int) error {
	job, err := childJob()
	if err != nil {
		return err
	}
	proc, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("opening process %d: %w", pid, err)
	}
	defer func() { _ = windows.CloseHandle(proc) }()
	if err := windows.AssignProcessToJobObject(job, proc); err != nil {
		return fmt.Errorf("assigning process %d to job: %w", pid, err)
	}
	return nil
}
