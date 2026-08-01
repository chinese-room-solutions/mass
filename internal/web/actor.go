package web

import (
	"net"
	"net/http"
)

// actorFromRequest returns the audit actor for r. Today MASS is
// single-operator, so a UI-originated request yields "operator". The
// X-Mass-Source header lets module callers self-identify (e.g.
// "module:playground") so audit lines distinguish operator-initiated
// changes from background subsystem changes. When multi-user lands,
// this is the single seam to replace.
func actorFromRequest(r *http.Request) string {
	if src := r.Header.Get("X-Mass-Source"); src != "" {
		return src
	}
	return "operator"
}

// remoteAddr returns the client IP for audit logs. Strips the port
// component so log filters group cleanly per-host.
func remoteAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
