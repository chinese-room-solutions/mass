// Package jitter spreads periodic work in time so independent clients and
// jobs don't fire in lockstep.
package jitter

import (
	"math/rand/v2"
	"time"
)

// Duration returns d perturbed by up to frac of itself in either direction
// (frac 0.2 gives ±20%). d is returned unchanged when the spread would round
// away to nothing.
func Duration(d time.Duration, frac float64) time.Duration {
	spread := time.Duration(float64(d) * frac)
	if d <= 0 || spread <= 0 {
		return d
	}
	return d - spread + time.Duration(rand.Int64N(int64(2*spread)))
}
