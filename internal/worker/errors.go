package worker

import "errors"

// ErrUnsupportedRuntime is returned when a worker receives a config for an
// inference runtime it does not understand (e.g. a non-llama chat config on
// a llama-only worker).
var ErrUnsupportedRuntime = errors.New("worker: unsupported runtime config")
