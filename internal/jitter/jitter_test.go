package jitter_test

import (
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/jitter"
	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	tests := []struct {
		name    string
		d       time.Duration
		frac    float64
		wantMin time.Duration
		wantMax time.Duration
		spread  bool
	}{
		{"20 percent", 15 * time.Second, 0.2, 12 * time.Second, 18 * time.Second, true},
		{"10 percent", 30 * time.Second, 0.1, 27 * time.Second, 33 * time.Second, true},
		{"zero fraction", time.Second, 0, time.Second, time.Second, false},
		{"zero duration", 0, 0.2, 0, 0, false},
		{"negative duration", -time.Second, 0.2, -time.Second, -time.Second, false},
		{"spread rounds to nothing", time.Nanosecond, 0.2, time.Nanosecond, time.Nanosecond, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := make(map[time.Duration]struct{})
			for range 200 {
				got := jitter.Duration(tt.d, tt.frac)
				require.GreaterOrEqual(t, got, tt.wantMin)
				require.LessOrEqual(t, got, tt.wantMax)
				seen[got] = struct{}{}
			}
			if tt.spread {
				require.Greater(t, len(seen), 1, "callers must not land on the same interval")
			} else {
				require.Len(t, seen, 1)
			}
		})
	}
}
