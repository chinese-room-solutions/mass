package scheduler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// workerQueueName is the canonical "worker|<id>" shape the dispatcher
// uses as the row key in goqite + worker_queue_state. Round-trips with
// parseWorkerQueueName, and the parser must reject anything that
// doesn't match the shape (defends against stale persisted rows,
// including legacy "dev|*" entries from before the worker-level move).
func TestWorkerQueueName_RoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		worker    string
		wantName  string
		parseOK   bool
		parseW    string
		parseOnly bool // when set, skip the build path (tests reject-only cases)
		raw       string
	}{
		{
			name:     "simple",
			worker:   "w1",
			wantName: "worker|w1",
			parseOK:  true,
			parseW:   "w1",
		},
		{
			name:     "hyphenated worker id",
			worker:   "host-01",
			wantName: "worker|host-01",
			parseOK:  true,
			parseW:   "host-01",
		},
		{
			name:      "rejects bare prefix",
			parseOnly: true,
			raw:       "worker|",
			parseOK:   false,
		},
		{
			name:      "rejects non-worker prefix",
			parseOnly: true,
			raw:       "global",
			parseOK:   false,
		},
		{
			name:      "rejects legacy dev row",
			parseOnly: true,
			raw:       "dev|w1|gpu:0",
			parseOK:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.raw
			if !tt.parseOnly {
				input = workerQueueName(tt.worker)
				require.Equal(t, tt.wantName, input)
			}
			gotW, ok := parseWorkerQueueName(input)
			require.Equal(t, tt.parseOK, ok)
			if tt.parseOK && !tt.parseOnly {
				require.Equal(t, tt.parseW, gotW)
			}
		})
	}
}

// OnWorkerConnected materialises one queue per worker, persists the
// state row, and is idempotent. A worker with no benched+enabled
// device gets no queue (the scheduler can't honestly score it).
func TestScheduler_OnWorkerConnected_Lifecycle(t *testing.T) {
	tests := []struct {
		name         string
		devices      []stats.Device
		deviceEnable func(workerID, deviceID string) bool
		benchedIDs   []string // device IDs that should have a benchmark seeded
		wantQueues   []string
	}{
		{
			name: "benched device produces one worker queue",
			devices: []stats.Device{
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			},
			benchedIDs: []string{"gpu:0"},
			wantQueues: []string{"worker|w1"},
		},
		{
			name: "every device benched still produces one queue",
			devices: []stats.Device{
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			},
			benchedIDs: []string{"cpu:0", "gpu:0"},
			wantQueues: []string{"worker|w1"},
		},
		{
			name: "all devices disabled: no queue",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			},
			deviceEnable: func(_, _ string) bool { return false },
			benchedIDs:   []string{"gpu:0"},
			wantQueues:   nil,
		},
		{
			name: "no benchmark on any enabled device: no queue",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			},
			benchedIDs: nil,
			wantQueues: nil,
		},
		{
			name:       "no devices at all: no queue",
			devices:    nil,
			wantQueues: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			if tt.deviceEnable != nil {
				s.SetDeviceEnabledFn(tt.deviceEnable)
			}
			for _, devID := range tt.benchedIDs {
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID:   "w1",
					DeviceID:   devID,
					DeviceName: devID,
					Throughput: map[string]float64{"q4k_matvec": 100},
					BenchedAt:  time.Now(),
				}))
			}
			w := newFakeWorker("w1", tt.devices)
			s.OnWorkerConnected(w)

			s.queueMu.RLock()
			got := make([]string, 0, len(s.devQueues))
			for name := range s.devQueues {
				got = append(got, name)
			}
			s.queueMu.RUnlock()
			require.ElementsMatch(t, tt.wantQueues, got)

			rows, err := st.ListWorkerQueueStates()
			require.NoError(t, err)
			gotRows := make([]string, 0, len(rows))
			for _, r := range rows {
				gotRows = append(gotRows, r.QueueName)
			}
			require.ElementsMatch(t, tt.wantQueues, gotRows)

			// Idempotent: reconnect must not double-create.
			s.OnWorkerConnected(w)
			s.queueMu.RLock()
			require.Equal(t, len(tt.wantQueues), len(s.devQueues))
			s.queueMu.RUnlock()
		})
	}
}

// OnWorkerDisconnected drains queued rows back to global and removes
// the worker queue + persisted state.
func TestScheduler_OnWorkerDisconnected_DrainsToGlobal(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	s.OnWorkerConnected(w)

	// Place jobs through the real path — global anchor + leased handoff
	// to the worker queue — so the drain has something to move. More rows
	// than the old 64-row drain cap: every one of them must come back.
	const total = 70
	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	for i := range total {
		placeOnWorkerQueueForTest(t, s, wq, queue.Envelope{
			Priority:    queue.PriorityMedium,
			RuntimeName: "llama-cpp",
			ModelID:     "m1",
			RequestID:   fmt.Sprintf("req-%d", i),
			Payload:     []byte("payload"),
		})
	}

	depthBefore, err := wq.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, total, depthBefore)

	s.OnWorkerDisconnected("w1")

	// Worker queue must be gone from the in-memory map and from the store.
	s.queueMu.RLock()
	_, present := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.False(t, present)

	rows, err := st.ListWorkerQueueStates()
	require.NoError(t, err)
	require.Empty(t, rows)

	// Every drained envelope must be on global now — including the rows
	// past the old cap.
	globalDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, total, globalDepth)

	msgs, err := globalQ.Peek(ctx, total)
	require.NoError(t, err)
	require.Len(t, msgs, total)
	got, err := queue.UnmarshalEnvelope(msgs[0].Body)
	require.NoError(t, err)
	require.Equal(t, "m1", got.ModelID)
}

// An in-flight (leased) row on a dead worker is charged against
// disconnectRequeueBudget: under budget it re-places on global with
// Attempts incremented and the result reverted to pending; past budget
// the result fails terminally — the cap that keeps a worker-killing job
// from cycling forever while leaving single-death redistribution intact.
func TestScheduler_DrainWorkerQueue_InflightChargesAttemptBudget(t *testing.T) {
	s, st := newTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Stage the job as the dispatcher would leave it mid-run: placed via
	// the real path, worker row leased, result row processing.
	env := queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-1", Payload: []byte("p"),
	}
	placeOnWorkerQueueForTest(t, s, wq, env)
	rows, err := wq.PeekAll(ctx, 4)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = wq.LeaseByID(ctx, rows[0].ID, dispatchLeaseDuration)
	require.NoError(t, err)
	require.NoError(t, s.results.Create("req-1"))
	require.NoError(t, s.results.Processing("req-1"))

	// First worker death: budget allows one re-placement — the envelope
	// lands back on global with the attempt recorded, result pending again.
	s.drainWorkerQueue(ctx, wq, globalQ, true)

	wqDepth, err := wq.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, wqDepth)
	msgs, err := globalQ.Peek(ctx, 4)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	requeued, err := queue.UnmarshalEnvelope(msgs[0].Body)
	require.NoError(t, err)
	require.Equal(t, uint8(1), requeued.Attempts, "re-placement must consume an attempt")
	r, err := s.GetResult("req-1")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, r.Status)

	// A job on its last budgeted attempt loses its worker again: both
	// rows dropped and the result fails terminally.
	requeued.Attempts = disconnectRequeueBudget - 1
	requeued.GlobalMsgID = string(msgs[0].ID)
	wres, err := wq.Submit(ctx, requeued)
	require.NoError(t, err)
	_, err = wq.LeaseByID(ctx, queue.MessageID(wres.ID), dispatchLeaseDuration)
	require.NoError(t, err)
	require.NoError(t, s.results.Processing("req-1"))

	s.drainWorkerQueue(ctx, wq, globalQ, true)

	gDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, gDepth)
	r, err = s.GetResult("req-1")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, r.Status)
	require.Contains(t, r.Error, "worker disconnected")
}

// A leased row whose job already carries a terminal result is dropped, not
// requeued. This is the device-loss shape: the worker ships the terminal
// error frame and only then exits for its clean restart, so the disconnect
// drain always runs moments after the result landed. Before the guard, the
// drain resubmitted the job and the restarted worker re-ran the exact
// workload that had just killed it.
func TestScheduler_DrainWorkerQueue_TerminalResultNotRequeued(t *testing.T) {
	s, st := newTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Stage the incident: job in flight (leased row), and its terminal
	// error frame already stored by the pump before the stream closed.
	env := queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-lost", Payload: []byte("p"),
	}
	placeOnWorkerQueueForTest(t, s, wq, env)
	rows, err := wq.PeekAll(ctx, 4)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = wq.LeaseByID(ctx, rows[0].ID, dispatchLeaseDuration)
	require.NoError(t, err)
	require.NoError(t, s.results.Create("req-lost"))
	require.NoError(t, s.results.Processing("req-lost"))
	require.NoError(t, s.results.Fail("req-lost", "unhandled exception: vk::Device::waitForFences: ErrorDeviceLost"))

	s.drainWorkerQueue(ctx, wq, globalQ, true)

	// Both rows gone, nothing re-placed, and the stored result untouched.
	wqDepth, err := wq.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, wqDepth)
	gDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, gDepth)
	r, err := s.GetResult("req-lost")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, r.Status)
	require.Contains(t, r.Error, "ErrorDeviceLost")
}

// OnWorkerDevicesChanged drains every pending row when the toggle
// disables every device on the worker — the worker can't host
// anything, jobs must go back to global so a peer can pick them up.
// Mirrors the disconnect-drain path without removing the queue entry,
// so re-enabling a device lets the queue receive again.
func TestScheduler_OnWorkerDevicesChanged_AllDisabledDrainsToGlobal(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	placeOnWorkerQueueForTest(t, s, wq, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-1", Payload: []byte("p"),
	})

	// Disable every device on the worker via the test enabled-fn.
	s.SetDeviceEnabledFn(func(_, _ string) bool { return false })

	s.OnWorkerDevicesChanged("w1")

	globalDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, globalDepth, "row must be back on global after all-disable toggle")

	// Worker queue stays registered (re-enabling a device should let it
	// receive again without a reconnect cycle).
	s.queueMu.RLock()
	_, present := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.True(t, present, "worker queue stays registered through a toggle")
}

// The all-disable toggle drains PENDING rows only. The worker is still
// online and its in-flight job is still streaming, so requeueing the
// leased row would dispatch a duplicate onto a peer, burn the disconnect
// budget, and eventually write a false "worker disconnected" result.
func TestScheduler_OnWorkerDevicesChanged_AllDisabledKeepsRunningJob(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// In-flight row: placed, leased, result processing.
	placeOnWorkerQueueForTest(t, s, wq, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-running", Payload: []byte("p"),
	})
	rows, err := wq.PeekAll(ctx, 4)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = wq.LeaseByID(ctx, rows[0].ID, dispatchLeaseDuration)
	require.NoError(t, err)
	require.NoError(t, s.results.Create("req-running"))
	require.NoError(t, s.results.Processing("req-running"))

	// Queued-but-never-dispatched row behind it.
	placeOnWorkerQueueForTest(t, s, wq, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-pending", Payload: []byte("p"),
	})

	s.SetDeviceEnabledFn(func(_, _ string) bool { return false })
	s.OnWorkerDevicesChanged("w1")

	// Only the pending row goes back to global.
	gmsgs, err := globalQ.Peek(ctx, 4)
	require.NoError(t, err)
	require.Len(t, gmsgs, 1, "only the pending row may return to global")
	back, err := queue.UnmarshalEnvelope(gmsgs[0].Body)
	require.NoError(t, err)
	require.Equal(t, "req-pending", back.RequestID)

	// The running job is untouched: still leased on the worker queue,
	// still processing, no attempt consumed.
	rows, err = wq.PeekAll(ctx, 4)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the leased row stays on the worker queue")
	require.True(t, rows[0].Leased, "the leased row keeps its lease")
	stillRunning, err := queue.UnmarshalEnvelope(rows[0].Body)
	require.NoError(t, err)
	require.Equal(t, "req-running", stillRunning.RequestID)
	require.Equal(t, uint8(0), stillRunning.Attempts, "a running job must not be charged an attempt")
	r, err := s.GetResult("req-running")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusProcessing, r.Status)
}

// eligibleWorker filters out workers with no usable devices (every
// device disabled). Without the filter MASS would dispatch to a worker
// whose load would immediately fail.
func TestScheduler_EligibleWorker_RequiresUsableDevice(t *testing.T) {
	s, _ := newTestScheduler(t)
	w := newFakeWorker("w1", []stats.Device{
		{ID: "gpu:0", Type: stats.DeviceTypeGPU},
	})
	require.NoError(t, s.workers.Register(w))

	// Default: device enabled, worker is eligible.
	require.True(t, s.eligibleWorker(w, queue.Envelope{}))

	// All devices disabled: not eligible.
	s.SetDeviceEnabledFn(func(_, _ string) bool { return false })
	require.False(t, s.eligibleWorker(w, queue.Envelope{}))
}

// A remote (non-loopback) worker can never load a model whose files are
// shipped by local path only (no URL) — the gateway's LocalPath points at
// MASS's host filesystem. eligibleWorker must exclude such workers so the
// job doesn't dispatch to a worker whose load is guaranteed to fail.
func TestScheduler_EligibleWorker_LoopbackOnlyFiles(t *testing.T) {
	urlFile := &workerpb.ModelFile{Url: "https://example.com/m.gguf", SizeBytes: 1}
	localFile := &workerpb.ModelFile{LocalPath: "/data/models/m.gguf", SizeBytes: 1}

	tests := []struct {
		name     string
		loopback bool
		files    []*workerpb.ModelFile
		want     bool
	}{
		{name: "remote worker, no files", loopback: false, files: nil, want: true},
		{name: "remote worker, all files have URLs", loopback: false, files: []*workerpb.ModelFile{urlFile}, want: true},
		{name: "remote worker, URL-less file", loopback: false, files: []*workerpb.ModelFile{localFile}, want: false},
		{name: "remote worker, mixed files", loopback: false, files: []*workerpb.ModelFile{urlFile, localFile}, want: false},
		{name: "remote worker, nil file entry only", loopback: false, files: []*workerpb.ModelFile{nil}, want: true},
		{name: "loopback worker, URL-less file", loopback: true, files: []*workerpb.ModelFile{localFile}, want: true},
		{name: "loopback worker, no files", loopback: true, files: nil, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
			w.SetFakeLoopback(tt.loopback)
			require.NoError(t, s.workers.Register(w))

			got := s.eligibleWorker(w, queue.Envelope{ModelID: "m", Files: tt.files})
			require.Equal(t, tt.want, got)
		})
	}
}

// memoryEligible passes through when BaseLoadBytes is 0 (unknown),
// when the model is already resident, or when free memory across the
// device set is sufficient. Rejects when free is below the request.
func TestScheduler_MemoryEligible_Branches(t *testing.T) {
	const mb = 1024 * 1024
	const gb = 1024 * mb

	tests := []struct {
		name      string
		devices   []stats.Device
		stats     []stats.DeviceStats
		resident  string // ModelID for SetFakeLoadedModels (empty = none)
		loadBytes int64
		want      bool
	}{
		{
			name:    "base=0 passes through (unknown)",
			devices: []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}},
			stats:   []stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 23 * 1024, TotalMemoryMB: 24 * 1024}},
			// loadBytes=0; free is only 1 GB, would fail if checked.
			want: true,
		},
		{
			name:      "model already resident passes through",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}},
			stats:     []stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 20 * 1024, TotalMemoryMB: 24 * 1024}},
			resident:  "m-1",
			loadBytes: 10 * gb,
			want:      true,
		},
		{
			name:      "free GPU memory ≥ load: eligible",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}},
			stats:     []stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 8 * 1024, TotalMemoryMB: 24 * 1024}},
			loadBytes: 12 * gb,
			want:      true, // 16 GB free
		},
		{
			name:      "free GPU memory < load: rejected",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}},
			stats:     []stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 20 * 1024, TotalMemoryMB: 24 * 1024}},
			loadBytes: 8 * gb,
			want:      false, // 4 GB free
		},
		{
			// Default placement picks GPUs when any are enabled — the
			// CPU's much larger free pool doesn't help. 20 GB load
			// doesn't fit in the 8 GB GPU.
			name: "GPU + CPU: default picks GPUs only (CPU ignored)",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 8 * 1024},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU, TotalMemoryMB: 64 * 1024},
			},
			stats: []stats.DeviceStats{
				{DeviceID: "gpu:0", UsedMemoryMB: 0, TotalMemoryMB: 8 * 1024},
				{DeviceID: "cpu:0", UsedMemoryMB: 16 * 1024, TotalMemoryMB: 64 * 1024},
			},
			loadBytes: 20 * gb,
			want:      false,
		},
		{
			name: "two GPUs sum: combined free fits a load no single GPU could",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 16 * 1024},
				{ID: "gpu:1", Type: stats.DeviceTypeGPU, TotalMemoryMB: 16 * 1024},
			},
			stats: []stats.DeviceStats{
				{DeviceID: "gpu:0", UsedMemoryMB: 4 * 1024, TotalMemoryMB: 16 * 1024},
				{DeviceID: "gpu:1", UsedMemoryMB: 4 * 1024, TotalMemoryMB: 16 * 1024},
			},
			loadBytes: 20 * gb,
			want:      true, // 12 + 12 = 24 GB free
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", tt.devices, time.Now())
			w.SetFakeDeviceStats(tt.stats)
			if tt.resident != "" {
				w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: tt.resident}})
			}
			require.NoError(t, s.workers.Register(w))

			env := queue.Envelope{
				ModelID:       tt.resident,
				BaseLoadBytes: tt.loadBytes,
			}
			require.Equal(t, tt.want, s.memoryEligible(w, env))
		})
	}
}

// feasibleByAnyWorker uses total hardware memory (not free) so the
// Submit-stage check distinguishes "fleet too small" from "fleet busy
// now." Even when free is 0, total is the hardware ceiling and the
// predicate accepts if any worker physically could host the load.
func TestScheduler_FeasibleByAnyWorker_UsesTotalNotFree(t *testing.T) {
	const mb = 1024 * 1024
	const gb = 1024 * mb
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}}, time.Now())
	// Worker is currently 100% used, but its hardware ceiling is 24 GB.
	w.SetFakeDeviceStats([]stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 24 * 1024, TotalMemoryMB: 24 * 1024}})
	require.NoError(t, s.workers.Register(w))

	env := queue.Envelope{RuntimeName: "llama-cpp"}

	env.BaseLoadBytes = 20 * gb
	require.True(t, s.feasibleByAnyWorker(env), "20 GB fits the hardware (24 GB total) even though 0 free now")

	env.BaseLoadBytes = 30 * gb
	require.False(t, s.feasibleByAnyWorker(env), "30 GB exceeds the hardware ceiling — operator must add capacity")

	env.BaseLoadBytes = 0
	require.True(t, s.feasibleByAnyWorker(env), "unknown load bytes pass through")
}

// Reservation lifecycle: startInflight bumps the ledger; finishInflight
// drains it. Memory eligibility reflects the reservation immediately
// (no heartbeat needed), so two cold-load decisions in fast succession
// can't both admit against the same not-yet-heartbeated slack.
func TestScheduler_MemoryReservation_BridgesHeartbeatLag(t *testing.T) {
	const mb = 1024 * 1024
	const gb = 1024 * mb
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}}, time.Now())
	w.SetFakeDeviceStats([]stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 0, TotalMemoryMB: 24 * 1024}})
	require.NoError(t, s.workers.Register(w))

	// Initially: 24 GB free, a 16 GB load fits.
	env := queue.Envelope{BaseLoadBytes: 16 * gb}
	require.True(t, s.memoryEligible(w, env))

	// Dispatch a 16 GB load → reserve 16 GB.
	s.startInflight(workerQueueName("w1"), "req-1", "m-1", "", "", 1.0, 16*gb)

	// A second 16 GB load no longer fits: 24 − 0 − 16 = 8 GB free.
	require.False(t, s.memoryEligible(w, env))

	// An 8 GB load still fits.
	envSmall := queue.Envelope{BaseLoadBytes: 8 * gb}
	require.True(t, s.memoryEligible(w, envSmall))

	// First job finishes → reservation released → 16 GB load fits again.
	s.finishInflight("req-1")
	require.True(t, s.memoryEligible(w, env))
	require.Zero(t, s.getMemoryReservation("w1"), "ledger fully drained")
}

// retryAfterLoadFailure bumps Attempts and re-submits to global until
// the configured cap, then returns false so the caller fails the job.
// Verifies the cap math (attempt 1 retries when cap=2, attempt 2 gives
// up when cap=2) and that the new envelope on global carries the
// incremented Attempts.
func TestScheduler_RetryAfterLoadFailure_Cap(t *testing.T) {
	s, st := newTestScheduler(t)
	s.cfg.LoadAttempts = 2 // first attempt + 1 retry = 2 total

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)
	require.NotNil(t, globalQ)

	// Plant an envelope on both queues simulating a first-attempt
	// dispatch that's about to fail to load.
	gres, err := globalQ.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-1", Payload: []byte("p"),
	})
	require.NoError(t, err)
	env := queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-1", GlobalMsgID: gres.ID,
		Payload: []byte("p"), Attempts: 0,
	}
	wres, err := wq.Submit(ctx, env)
	require.NoError(t, err)

	// First retry: Attempts 0+1 = 1, cap = 2 → succeeds.
	require.True(t, s.retryAfterLoadFailure(wq, queue.MessageID(wres.ID), env))

	// Worker queue is empty; global has one row with Attempts=1.
	wqDepth, err := wq.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, wqDepth)
	gDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, gDepth)
	msgs, err := globalQ.Peek(ctx, 4)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	got, err := queue.UnmarshalEnvelope(msgs[0].Body)
	require.NoError(t, err)
	require.Equal(t, uint8(1), got.Attempts, "Attempts incremented")
	require.NotEqual(t, gres.ID, got.GlobalMsgID, "new global anchor on retry")

	// Second retry: Attempts 1+1 = 2, cap = 2 → refuses.
	require.False(t, s.retryAfterLoadFailure(wq, queue.MessageID(wres.ID), got))
}

// retryAfterLoadFailure with LoadAttempts=1 (default) refuses on the
// first failure — preserves the pay-on-failure behavior operators
// get when they don't tune the setting.
func TestScheduler_RetryAfterLoadFailure_DefaultRefuses(t *testing.T) {
	s, _ := newTestScheduler(t)
	// cfg.LoadAttempts left at 0 → EffectiveLoadAttempts coerces to 1.
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	require.False(t, s.retryAfterLoadFailure(nil, "ignored", queue.Envelope{
		RequestID: "r", Attempts: 0,
	}))
}

// retryAfterLoadFailure soft-fails (returns false) when the queue
// subsystem isn't wired yet. Same shape as the cap-exhausted refusal:
// caller falls through to the failure path with the original error.
func TestScheduler_RetryAfterLoadFailure_NoGlobalQueue(t *testing.T) {
	// Build a scheduler WITHOUT InitQueue — globalQ stays nil.
	cfg := &config.Config{LoadAttempts: 3}
	s := New(cfg, zerolog.Nop(), worker.NewFleet())
	require.False(t, s.retryAfterLoadFailure(nil, "ignored", queue.Envelope{
		RequestID: "r", Attempts: 0,
	}))
}

// retryAfterLoadFailure handles the anchorless envelope: an envelope
// that somehow reached dispatch without a GlobalMsgID (legacy paths,
// test fixtures) falls back to a single-side Delete on the worker
// queue before resubmitting. The retry still succeeds, leaving the
// worker queue empty and one fresh row on global.
func TestScheduler_RetryAfterLoadFailure_AnchorlessEnvelope(t *testing.T) {
	s, st := newTestScheduler(t)
	s.cfg.LoadAttempts = 2

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Anchorless envelope: only on the worker queue, GlobalMsgID="".
	env := queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m1", RequestID: "req-anchorless", Payload: []byte("p"),
	}
	wres, err := wq.Submit(ctx, env)
	require.NoError(t, err)

	require.True(t, s.retryAfterLoadFailure(wq, queue.MessageID(wres.ID), env))

	wqDepth, err := wq.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, wqDepth)
	gDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, gDepth)
}

// retryAfterLoadFailure soft-fails when the queue rejects the row
// deletion (e.g. row already gone via a race with cleanup). Same
// "return false, let caller report" contract as the cap-exhausted
// case — important so a queue error during retry doesn't leak as a
// panic or mask the original load failure.
func TestScheduler_RetryAfterLoadFailure_DeleteError(t *testing.T) {
	s, st := newTestScheduler(t)
	s.cfg.LoadAttempts = 2

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()

	// Wrap the worker queue so DeleteBoth fails. Anchorless path also
	// covered by passing an envelope with GlobalMsgID=""
	// (forces the Delete path) and a never-submitted msgID so the
	// real queue returns no row found — which goqite tolerates as
	// nil error, so use the failing wrapper to force the error path.
	failQ := &failingQueue{QueueInterface: wq, failDeleteBoth: true, failDelete: true}

	// With GlobalMsgID set, retryAfterLoadFailure takes the DeleteBoth path.
	require.False(t, s.retryAfterLoadFailure(failQ, "any", queue.Envelope{
		Priority:    queue.PriorityMedium,
		RuntimeName: "llama-cpp",
		RequestID:   "req-deletefail",
		GlobalMsgID: "some-global",
		Attempts:    0,
	}))

	// Without GlobalMsgID, retryAfterLoadFailure takes the single-Delete path.
	require.False(t, s.retryAfterLoadFailure(failQ, "any", queue.Envelope{
		Priority:    queue.PriorityMedium,
		RuntimeName: "llama-cpp",
		RequestID:   "req-deletefail-anchorless",
		Attempts:    0,
	}))
}

// failingQueue wraps a real QueueInterface and turns selected methods
// into error returns. Used to cover the soft-fail branches in
// retryAfterLoadFailure without crafting a full mock — the real queue
// handles every method we don't override.
type failingQueue struct {
	queue.QueueInterface
	failDeleteBoth bool
	failDelete     bool
}

func (f *failingQueue) DeleteBoth(ctx context.Context, msgID queue.MessageID, other queue.QueueInterface, otherID queue.MessageID) error {
	if f.failDeleteBoth {
		return errors.New("DeleteBoth injected failure")
	}
	return f.QueueInterface.DeleteBoth(ctx, msgID, other, otherID)
}

func (f *failingQueue) Delete(ctx context.Context, msgID queue.MessageID) error {
	if f.failDelete {
		return errors.New("Delete injected failure")
	}
	return f.QueueInterface.Delete(ctx, msgID)
}

// memoryEligible: an empty deviceSet (worker registered without
// devices, or every device disabled) returns 0 free → reject any
// non-zero load. Sits adjacent to the existing branches; covers
// the "len(set) == 0" guard in freeMemoryBytes / totalMemoryBytes.
func TestScheduler_MemoryEligible_WorkerWithNoDevices(t *testing.T) {
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp", nil, time.Now())
	require.NoError(t, s.workers.Register(w))

	env := queue.Envelope{ModelID: "m1", BaseLoadBytes: 1024 * 1024}
	require.False(t, s.memoryEligible(w, env), "no-device worker can't fit anything")

	// Zero load passes through regardless.
	env.BaseLoadBytes = 0
	require.True(t, s.memoryEligible(w, env))
}

// memoryEligible with a heartbeat missing for a device: used=0
// assumed (fresh worker, no DeviceStats yet) so total counts as
// free. Important — without this defensive fallback, a worker that
// just connected (no heartbeat yet) would erroneously fail every
// memory check.
func TestScheduler_MemoryEligible_NoHeartbeatYet(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 24 * 1024}}, time.Now())
	// No SetFakeDeviceStats — heartbeat hasn't arrived.
	require.NoError(t, s.workers.Register(w))

	env := queue.Envelope{ModelID: "m1", BaseLoadBytes: 20 * gb}
	require.True(t, s.memoryEligible(w, env), "no heartbeat: total counts as free")
}

// feasibleByAnyWorker scopes to the env's runtime. A worker on a
// different runtime — even with massive memory — doesn't count.
func TestScheduler_FeasibleByAnyWorker_ScopedToRuntime(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	s, _ := newTestScheduler(t)
	wOther := worker.NewFakeStreamWorker("w-other", "other-runtime",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 96 * 1024}}, time.Now())
	require.NoError(t, s.workers.Register(wOther))

	require.False(t, s.feasibleByAnyWorker(queue.Envelope{
		RuntimeName: "llama-cpp", BaseLoadBytes: 20 * gb,
	}), "worker on other runtime doesn't count for llama-cpp submit")
}

// Reservation: zero bytes is a no-op (ledger stays empty), so the
// fast path through startInflight when BaseLoadBytes==0 doesn't
// pollute the map with empty entries. Mirrors the workerID=="" guard
// for malformed queue names.
func TestScheduler_Reservation_ZeroBytesNoOp(t *testing.T) {
	s, _ := newTestScheduler(t)
	s.startInflight(workerQueueName("w1"), "req-zero", "m1", "", "", 1.0, 0)
	require.Zero(t, s.getMemoryReservation("w1"), "0 bytes must not populate the ledger")
	require.Zero(t, s.getMemoryReservation(""), "empty workerID must not populate the ledger")
	s.finishInflight("req-zero")
	require.Zero(t, s.getMemoryReservation("w1"), "lifecycle ends clean")
}

// Reservation: two concurrent cold loads to the same worker sum
// their reservations in the ledger and each release decrements
// independently. Models the realistic case where MASS dispatches
// two distinct models to the same worker before either finishes
// loading.
func TestScheduler_Reservation_TwoConcurrentLoadsSum(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	s, _ := newTestScheduler(t)

	s.startInflight(workerQueueName("w1"), "req-a", "m-a", "", "", 1.0, 8*gb)
	s.startInflight(workerQueueName("w1"), "req-b", "m-b", "", "", 1.0, 16*gb)
	require.Equal(t, 24*gb, s.getMemoryReservation("w1"), "two reservations sum")

	s.finishInflight("req-a")
	require.Equal(t, 16*gb, s.getMemoryReservation("w1"), "release one, the other stays")

	s.finishInflight("req-b")
	require.Zero(t, s.getMemoryReservation("w1"), "release both, ledger drained")
}

// freeMemoryBytes guards against negative-free when the reservation
// ledger exceeds (total - used). Happens when MASS reserves bytes
// against a worker whose heartbeat then reports much higher used_mem
// than the reservation accounted for (worker doing non-MASS work, or
// a previous reservation lifecycle leaked). The safety floor at 0
// prevents memoryEligible from admitting against negative free.
func TestScheduler_FreeMemoryBytes_OverReservedFloorsAtZero(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 8 * 1024}}, time.Now())
	// 6 GB used → 2 GB nominally free.
	w.SetFakeDeviceStats([]stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 6 * 1024, TotalMemoryMB: 8 * 1024}})
	require.NoError(t, s.workers.Register(w))

	// Reserve 16 GB — more than the worker has total. free = 2 - 16 = -14
	// must floor at 0, not return a negative int64.
	s.startInflight(workerQueueName("w1"), "leak", "m", "", "", 1.0, 16*gb)

	require.Zero(t, s.freeMemoryBytes(w))
}

// totalMemoryBytes returns 0 when the worker has no devices, which
// in turn lets feasibleByAnyWorker / memoryEligible reject without
// dividing by zero or summing a nil set.
func TestScheduler_TotalMemoryBytes_NoDevices(t *testing.T) {
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp", nil, time.Now())
	require.NoError(t, s.workers.Register(w))
	require.Zero(t, s.totalMemoryBytes(w))
}

// Submit-time feasibility surfaces ErrNoMemoryFit through preflight
// when the gateway estimates a load no fleet member can host. End-
// to-end check that the predicate is wired into the real Submit path
// (not just unit-tested in isolation).
func TestScheduler_Submit_RejectsNoMemoryFit(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 8 * 1024}}, time.Now())
	require.NoError(t, s.workers.Register(w))

	_, err := s.Submit(context.Background(), scheduler_SubmitRequestFor("llama-cpp", "m1", 50*gb))
	require.ErrorIs(t, err, ErrNoMemoryFit)
}

// scheduler_SubmitRequestFor builds a minimal SubmitRequest for tests
// that only exercise the preflight gates (no real dispatch).
func scheduler_SubmitRequestFor(runtimeName, modelID string, loadBytes int64) SubmitRequest {
	return SubmitRequest{
		RuntimeName:   runtimeName,
		ModelID:       modelID,
		Cost:          1.0,
		CostAxis:      "q4k_matvec",
		Payload:       []byte("p"),
		BaseLoadBytes: loadBytes,
	}
}

// Recovery reattaches to "worker|*" rows from a prior process lifetime
// so their goqite rows drain (or get stolen) once capacity arrives.
func TestScheduler_RecoverPersistedQueues(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(store.DialectSQLite, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	pool := queue.NewPool(st.DB(), st.Dialect())
	results := queue.NewResultStore(st.DB(), st.Dialect())
	cfg := &config.Config{}

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	first := New(cfg, zerolog.Nop(), worker.NewFleet())
	first.InitQueue(pool, results, st)
	first.OnWorkerConnected(newFakeWorker("w1", []stats.Device{{ID: "gpu:0"}}))

	second := New(cfg, zerolog.Nop(), worker.NewFleet())
	second.InitQueue(pool, results, st)
	second.recoverPersistedQueues()

	second.queueMu.RLock()
	_, workerPresent := second.devQueues["worker|w1"]
	second.queueMu.RUnlock()
	require.True(t, workerPresent, "worker|w1 row should reattach")
}

// A MASS crash mid-flight leaves rows leased past their delivery budget
// with no process left to write their results. reapAbandonedAtStartup
// (run by Start before the dispatcher) must fail their result entries
// and delete the rows — on the global queue AND on worker queues known
// only through persisted worker_queue_state (worker never reconnected).
// Healthy pending rows survive untouched.
func TestScheduler_ReapAbandonedAtStartup(t *testing.T) {
	s, st := newTestScheduler(t)
	ctx := context.Background()

	// Global-queue casualty. A negative lease spends the delivery budget
	// with an already-expired timeout — the exact shape a crash leaves.
	require.NoError(t, s.results.Create("rid-global"))
	gres, err := s.globalQ.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-global", Payload: []byte("p"),
	})
	require.NoError(t, err)
	leasedG, err := s.globalQ.LeaseByID(ctx, queue.MessageID(gres.ID), -time.Second)
	require.NoError(t, err)
	require.NotNil(t, leasedG)

	// Worker-queue casualty reachable only via the persisted state row.
	const wqName = "worker|crashed"
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: wqName, WorkerID: "crashed", DeviceIDs: []string{"gpu:0"},
	}))
	wq := s.queuePool.Open(wqName)
	require.NoError(t, s.results.Create("rid-worker"))
	wres, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-worker", Payload: []byte("p"),
	})
	require.NoError(t, err)
	leasedW, err := wq.LeaseByID(ctx, queue.MessageID(wres.ID), -time.Second)
	require.NoError(t, err)
	require.NotNil(t, leasedW)

	// Healthy pending row: must survive the reap.
	require.NoError(t, s.results.Create("rid-live"))
	_, err = s.globalQ.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-live", Payload: []byte("p"),
	})
	require.NoError(t, err)

	s.reapAbandonedAtStartup(ctx)

	for _, rid := range []string{"rid-global", "rid-worker"} {
		res, err := s.results.Get(rid)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, queue.ResultStatusError, res.Status,
			"%s must be failed by the startup reap", rid)
	}
	live, err := s.results.Get("rid-live")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, live.Status)

	// Reaped rows are deleted outright; the live row remains.
	gRows, err := s.globalQ.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, gRows, 1, "only the live global row survives")
	wRows, err := wq.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, wRows, "abandoned worker row must be deleted")
}

// The dispatch lease keep-alive must re-extend both the worker row and
// the global anchor for as long as it runs — nothing else extends them,
// and a cold load or a long pre-first-chunk gap outlives any single
// lease window. After the (synchronous) stop, the leases must lapse so
// the rows become reachable again. Uses a short lease duration so
// expiry is observable; production passes dispatchLeaseDuration.
func TestLeaseKeepAlive_ExtendsBothRowsUntilStopped(t *testing.T) {
	s, _ := newTestScheduler(t)
	ctx := context.Background()

	wq := s.queuePool.Open("worker|ka")
	env := queue.Envelope{Priority: queue.PriorityMedium, RuntimeName: "rt", RequestID: "rid-ka", Payload: []byte("p")}
	gres, err := s.globalQ.Submit(ctx, env)
	require.NoError(t, err)
	env.GlobalMsgID = gres.ID

	const leaseDur = time.Second
	wres, leased, err := s.globalQ.LeaseAndSubmit(ctx, queue.MessageID(gres.ID), leaseDur, wq, env)
	require.NoError(t, err)
	require.True(t, leased)
	leasedMsg, err := wq.LeaseByID(ctx, queue.MessageID(wres.ID), leaseDur)
	require.NoError(t, err)
	require.NotNil(t, leasedMsg)

	stop := s.startLeaseKeepAlive(wq, queue.MessageID(wres.ID), gres.ID, leaseDur)

	peekEmpty := func(q queue.QueueInterface) bool {
		msgs, err := q.Peek(ctx, 10)
		require.NoError(t, err)
		return len(msgs) == 0
	}

	// 2.5 lease windows later both rows must still be hidden — only the
	// keep-alive can have carried them past the initial 1s lease.
	time.Sleep(2500 * time.Millisecond)
	require.True(t, peekEmpty(wq), "worker row must stay leased while the keep-alive runs")
	require.True(t, peekEmpty(s.globalQ), "global anchor must stay leased while the keep-alive runs")

	// stop is synchronous: once it returns, no further Extend can fire,
	// so both leases lapse within one window.
	stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !peekEmpty(wq) && !peekEmpty(s.globalQ) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("rows never became visible after keep-alive stop + lease expiry")
}

// --- helpers ---

func newTestScheduler(t *testing.T) (*Scheduler, *store.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(store.DialectSQLite, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	pool := queue.NewPool(st.DB(), st.Dialect())
	results := queue.NewResultStore(st.DB(), st.Dialect())
	cfg := &config.Config{}
	s := New(cfg, zerolog.Nop(), worker.NewFleet())
	s.InitQueue(pool, results, st)
	return s, st
}

// placeOnWorkerQueueForTest stages env on wq through the real placement
// path: a global durability anchor is submitted and lease-handed to the
// worker queue, exactly as placeOnWorkerQueue does. Tests must not plant
// anchorless rows directly on worker queues — every production envelope
// carries a GlobalMsgID and the drain paths rely on it.
func placeOnWorkerQueueForTest(t *testing.T, s *Scheduler, wq queue.QueueInterface, env queue.Envelope) {
	t.Helper()
	ctx := context.Background()
	gres, err := s.globalQ.Submit(ctx, env)
	require.NoError(t, err)
	env.GlobalMsgID = gres.ID
	_, leased, err := s.globalQ.LeaseAndSubmit(ctx, queue.MessageID(gres.ID), dispatchLeaseDuration, wq, env)
	require.NoError(t, err)
	require.True(t, leased)
}

// waitDispatchIdle blocks until every per-queue drain goroutine spawned by
// dispatchPass has exited, so tests can assert on state the drainers mutate
// (inflight records, released leases, queue depths). Not usable while a
// drainer is intentionally parked (e.g. a LoadModel awaiting its ack) —
// wait on the relevant channel first.
func waitDispatchIdle(t *testing.T, s *Scheduler) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.drainMu.Lock()
		n := len(s.draining)
		s.drainMu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("dispatch drainers did not go idle within 5s")
}

// newFakeWorker builds a minimal *worker.StreamWorker for tests that only
// touch the read-side getters (ID, RuntimeName, Devices, Status). Anything
// hitting the bidi stream (AssignJob, LoadModel) will fail — those paths
// belong to integration tests with a real hub. Hard-codes runtime name
// "llama-cpp" because every existing test runs against that runtime; if
// a future test needs another, plumb a parameter then.
func newFakeWorker(id string, devices []stats.Device) *worker.StreamWorker {
	return worker.NewFakeStreamWorker(id, "llama-cpp", devices, time.Now())
}
