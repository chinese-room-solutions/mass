package web

// changeEvent is the payload for change-only SSE brokers (Scheduler, Queue):
// JS reacts to any value by re-fetching the tab's list endpoint, so the
// struct carries no fields.
type changeEvent struct{}

// WorkersEventKind distinguishes the two kinds of update the Workers tab
// listens for. Stats fires periodically (gauge refresh, in-place); Change
// fires when the fleet shape itself changes (connect/disconnect/heartbeat
// snapshot, or after a benchmark run lands new persisted numbers).
type WorkersEventKind int

const (
	// WorkersEventStats — periodic stats tick. JS updates gauges in place.
	WorkersEventStats WorkersEventKind = iota
	// WorkersEventChange — fleet/list changed. JS re-fetches /api/workers/list.
	WorkersEventChange
)

// WorkersEvent is one notification fanned out to every active Workers-tab
// SSE subscriber.
type WorkersEvent struct {
	Kind WorkersEventKind
}
