package worker

import (
	"sync"
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
			// Sleep + window are intentionally generous so the few
			// microseconds between fixture build and assertion don't
			// push the elapsed time past the window.
			name:     "lastSeen well within window stays alive",
			online:   true,
			lastSeen: time.Now().Add(-time.Second),
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

// DeliverJobChunk and SetOffline race on the per-job chunk channels: the
// hub delivers chunks from the worker's recv loop while the stale-
// heartbeat watcher (or a UI kick) flips the worker offline. Before the
// send moved under pendingMu, a chunk that looked its channel up just
// before SetOffline closed it panicked with send-on-closed-channel and
// took MASS down. Hammer the pair across many iterations — the panic
// (and -race) would fail this test without the fix.
func TestStreamWorker_DeliverJobChunkRacesSetOffline(t *testing.T) {
	for range 2000 {
		w := NewFakeStreamWorker("w1", "rt", nil, time.Now())
		w.SetFakeSender(func(*workerpb.HubMessage) error { return nil })
		jobID, ch, err := w.AssignJob("m", []byte("p"))
		require.NoError(t, err)

		// Consumer mirrors pumpWorkerChunks: drain until closed.
		drained := make(chan struct{})
		go func() {
			for range ch {
			}
			close(drained)
		}()

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for range 20 {
				w.DeliverJobChunk(jobID, &JobChunk{Type: JobChunkTypeChunk})
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			w.SetOffline()
		}()
		close(start)
		wg.Wait()
		<-drained
	}
}

// ApplyHeartbeat must copy the store-relative Files list verbatim from each
// LoadedModelStatus so the hub and scheduler can see the cache keys backing a
// loaded model. The values are opaque path strings — file or directory — and
// MASS carries them through untouched.
func TestStreamWorker_ApplyHeartbeat_Files(t *testing.T) {
	w := &StreamWorker{}
	hb := &workerpb.WorkerHeartbeat{
		LoadedModels: []*workerpb.LoadedModelStatus{
			{ModelId: "a", Files: []string{"gguf/llama/model.gguf", "gguf/llama/mmproj.gguf"}},
			{ModelId: "b", Files: []string{"onnx/whisper"}},
			{ModelId: "c"},
		},
	}
	w.ApplyHeartbeat(hb)
	require.Len(t, w.loaded, 3)
	require.Equal(t, []string{"gguf/llama/model.gguf", "gguf/llama/mmproj.gguf"}, w.loaded[0].Files)
	require.Equal(t, []string{"onnx/whisper"}, w.loaded[1].Files)
	require.Empty(t, w.loaded[2].Files)
}

// A load with no pool size must be refused at the boundary rather than
// sent. On the wire a 0 is indistinguishable from "grow until the VRAM
// watermark", so a caller that forgot to size the pool would silently
// get the unbounded behaviour the measured sizing exists to replace —
// and MASS's memory gate, which is the only OOM protection once a load
// is pinned, would be reasoning about a pool the worker never built.
func TestStreamWorker_LoadModel_RejectsUnsetPoolSize(t *testing.T) {
	tests := []struct {
		name          string
		maxConcurrent int32
		wantSent      bool
	}{
		{name: "unset is refused", maxConcurrent: 0, wantSent: false},
		{name: "negative is refused", maxConcurrent: -1, wantSent: false},
		{name: "positive is sent verbatim", maxConcurrent: 7, wantSent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewFakeStreamWorker("w1", "llama-cpp", nil, time.Now())
			sent := make(chan *workerpb.HubLoadModel, 1)
			w.SetFakeSender(func(msg *workerpb.HubMessage) error {
				if lm := msg.GetLoadModel(); lm != nil {
					sent <- lm
				}
				return nil
			})

			done := make(chan error, 1)
			go func() {
				_, err := w.LoadModel(LoadModelRequest{ModelID: "m-1", MaxConcurrent: tt.maxConcurrent})
				done <- err
			}()

			if !tt.wantSent {
				require.ErrorIs(t, <-done, ErrNoPoolSize)
				require.Empty(t, sent, "a rejected load must not reach the wire")
				return
			}
			lm := <-sent
			require.Equal(t, tt.maxConcurrent, lm.GetMaxConcurrent())
			// Unblock the goroutine so it doesn't outlive the test.
			w.DeliverLoadResult(lm.GetJobId(), LoadResult{PoolSize: tt.maxConcurrent}, "")
			require.NoError(t, <-done)
		})
	}
}
