package web

import "errors"

// Sentinel errors shared by the control-plane ops (runtimeops.go, workerops.go,
// schedulerops.go, queueops.go). Each op returns one of these (or a manager
// sentinel passed through unwrapped) so every transport — the dashboard's
// /api/* handlers and the public mass.v1.Mass Connect API — maps them to its
// own status codes without re-deciding.
var (
	// ErrOpNotFound is returned when a targeted entity (runtime, worker,
	// device, loaded instance) doesn't exist.
	ErrOpNotFound = errors.New("not found")
	// ErrOpBusy is returned when an operation is refused because the target is
	// mid-transition (e.g. a worker re-estimating after a recent toggle).
	ErrOpBusy = errors.New("busy; retry in a moment")
	// ErrOpInvalid is returned for bad input (missing required fields).
	ErrOpInvalid = errors.New("invalid request")
	// ErrOpUnavailable is returned when a needed subsystem isn't wired.
	ErrOpUnavailable = errors.New("subsystem unavailable")
	// ErrOpRegistry is returned when a registry operation fails for a reason
	// outside the caller's control (index fetch, resolution, download,
	// checksum mismatch). The wrapped error carries the specifics.
	ErrOpRegistry = errors.New("registry operation failed")
)
