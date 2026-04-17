package web

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"
)

// expectedClientDisconnect reports whether err is one of the expected
// client-disconnect errors (broken pipe, closed connection, context cancel)
// that we should silently tolerate when writing to an HTTP response.
func expectedClientDisconnect(err error) bool {
	return errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// mustSSE wraps an SSE write and panics on programmer errors but tolerates
// expected client-disconnect errors. Use this instead of `_ = sse.Patch...()`
// to surface real bugs while still allowing graceful handling of dead clients.
func mustSSE(err error) {
	if err == nil || expectedClientDisconnect(err) {
		return
	}
	panic(err)
}

// mustHTTPWrite writes to an http.ResponseWriter, tolerating client-disconnect
// errors and panicking on anything else. Use for raw byte writes where there
// is no useful recovery path.
func mustHTTPWrite(w http.ResponseWriter, b []byte) {
	if _, err := w.Write(b); err != nil && !expectedClientDisconnect(err) {
		panic(err)
	}
}
