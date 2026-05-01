package worker

import (
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/stretchr/testify/require"
)

// HeartbeatStale fires only on online workers whose lastSeen drifted
// past the window — offline workers always return false (they're
// already excluded from dispatch via Status().Online).
func TestStreamWorker_HeartbeatStale(t *testing.T) {
	tests := []struct {
		name     string
		online   bool
		lastSeen time.Time
		window   time.Duration
		want     bool
	}{
		{
			name:     "offline worker never reports stale",
			online:   false,
			lastSeen: time.Now().Add(-time.Hour),
			window:   time.Minute,
			want:     false,
		},
		{
			name:     "fresh heartbeat",
			online:   true,
			lastSeen: time.Now().Add(-5 * time.Second),
			window:   time.Minute,
			want:     false,
		},
		{
			name:     "lastSeen exactly at window edge stays alive",
			online:   true,
			lastSeen: time.Now().Add(-time.Minute),
			window:   time.Minute,
			want:     false,
		},
		{
			name:     "stale heartbeat past window",
			online:   true,
			lastSeen: time.Now().Add(-2 * time.Minute),
			window:   time.Minute,
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &StreamWorker{online: tt.online, lastSeen: tt.lastSeen}
			require.Equal(t, tt.want, w.HeartbeatStale(tt.window))
		})
	}
}

// IdleSince merge across heartbeats: stamps once when Active first
// hits 0, preserves across subsequent idle heartbeats, clears the
// instant Active goes back above 0. Drives the idle-eviction sweep.
func TestStreamWorker_ApplyHeartbeat_IdleSince(t *testing.T) {
	tests := []struct {
		name          string
		prev          []LoadedModelStatus
		hbActive      int32
		wantStamped   bool // true: IdleSince should be set after this heartbeat
		wantPreserved bool // true: the new IdleSince should equal prev's
	}{
		{
			name:        "first heartbeat, busy → no stamp",
			prev:        nil,
			hbActive:    1,
			wantStamped: false,
		},
		{
			name:        "first heartbeat, idle → fresh stamp",
			prev:        nil,
			hbActive:    0,
			wantStamped: true,
		},
		{
			name:          "still idle → preserve prior stamp",
			prev:          []LoadedModelStatus{{ModelID: "m", IdleSince: time.Now().Add(-time.Minute)}},
			hbActive:      0,
			wantStamped:   true,
			wantPreserved: true,
		},
		{
			name:        "was idle, now busy → clear stamp",
			prev:        []LoadedModelStatus{{ModelID: "m", IdleSince: time.Now().Add(-time.Minute)}},
			hbActive:    2,
			wantStamped: false,
		},
		{
			name:        "was busy, now idle → fresh stamp",
			prev:        []LoadedModelStatus{{ModelID: "m"}}, // IdleSince zero
			hbActive:    0,
			wantStamped: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &StreamWorker{loaded: tt.prev}
			hb := &workerpb.WorkerHeartbeat{
				LoadedModels: []*workerpb.LoadedModelStatus{
					{ModelId: "m", PoolSize: 1, Active: tt.hbActive},
				},
			}
			w.ApplyHeartbeat(hb)
			require.Len(t, w.loaded, 1)
			got := w.loaded[0]
			if tt.wantStamped {
				require.False(t, got.IdleSince.IsZero(), "expected IdleSince stamped")
				if tt.wantPreserved {
					require.Equal(t, tt.prev[0].IdleSince, got.IdleSince, "expected prior IdleSince preserved")
				}
			} else {
				require.True(t, got.IdleSince.IsZero(), "expected IdleSince cleared")
			}
		})
	}
}
