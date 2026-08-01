// Package audit provides a uniform shape for state-change logs. Every
// security-relevant or operator-visible action emits one structured
// line through this helper so external aggregators (Loki, Splunk, etc.)
// can filter on component=audit + action.
//
// Naming convention: <noun>.<verb> in past tense for events that
// happened, e.g. "model.installed", "worker.disabled". Use Outcome to
// distinguish accepted/denied/error.
package audit

import (
	"github.com/rs/zerolog"
)

// Outcome is the result classification for an audited action.
type Outcome string

const (
	OutcomeOK      Outcome = "ok"
	OutcomeDenied  Outcome = "denied"
	OutcomeError   Outcome = "error"
	OutcomeAttempt Outcome = "attempt"
)

// Log starts an audit log event. Caller adds extra fields and ends with
// .Msg(""). Component is fixed to "audit" so downstream filters work.
//
//	audit.Log(h.logger, "model.installed", modelID, audit.OutcomeOK).
//	    Str("actor", actor).
//	    Str("source", "ui").
//	    Msg("")
//
//nolint:zerologlint // caller is required to terminate with .Msg(""); see godoc above.
func Log(logger zerolog.Logger, action, target string, outcome Outcome) *zerolog.Event {
	return logger.Info().
		Str("component", "audit").
		Str("action", action).
		Str("target", target).
		Str("outcome", string(outcome))
}
