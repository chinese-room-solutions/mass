package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// QueueSnapshot must return one section per active queue, populated with
// the unleased rows. The global section comes first; worker sections are
// sorted by queue name for deterministic UI rendering.
func TestQueueSnapshot_ListsGlobalAndWorkerQueues(t *testing.T) {
	s, st := newTestScheduler(t)

	// Two rows on global.
	for _, mid := range []string{"global-a", "global-b"} {
		_, err := s.globalQ.Submit(context.Background(), queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: mid,
			RequestID: "rid-" + mid, Payload: []byte("p"),
		})
		require.NoError(t, err)
	}

	// Two worker queues — names chosen so alphabetical sort puts a before b.
	for _, id := range []string{"a", "b"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: id, DeviceID: "gpu:0", DeviceName: "gpu:0",
			MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
		}))
		w := newFakeWorker(id, []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
		s.queueMu.RLock()
		wq := s.devQueues[workerQueueName(id)]
		s.queueMu.RUnlock()
		require.NotNil(t, wq)
		_, err := wq.Submit(context.Background(), queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-" + id,
			RequestID: "wrid-" + id, Payload: []byte("p"),
		})
		require.NoError(t, err)
	}

	sections, err := s.QueueSnapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, sections, 3)

	require.Equal(t, "global", sections[0].Name)
	require.Equal(t, "", sections[0].WorkerID)
	require.Len(t, sections[0].Rows, 2)

	require.Equal(t, workerQueueName("a"), sections[1].Name)
	require.Equal(t, "a", sections[1].WorkerID)
	require.Len(t, sections[1].Rows, 1)
	require.Equal(t, "m-a", sections[1].Rows[0].ModelID)

	require.Equal(t, workerQueueName("b"), sections[2].Name)
	require.Equal(t, "b", sections[2].WorkerID)
	require.Len(t, sections[2].Rows, 1)
	require.Equal(t, "m-b", sections[2].Rows[0].ModelID)
}

// QueuedModelFiles unions the ModelFile.Filename set across the global queue
// and every worker queue — the residency guard's view of what a QUEUED job
// still needs on disk.
func TestQueuedModelFiles_UnionsAllQueues(t *testing.T) {
	s, st := newTestScheduler(t)

	_, err := s.globalQ.Submit(context.Background(), queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "g",
		RequestID: "rid-g", Payload: []byte("p"),
		Files: []*workerpb.ModelFile{
			{Filename: "gguf/g/model.gguf"},
			{Filename: "gguf/g/mmproj.gguf"},
		},
	})
	require.NoError(t, err)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.queueMu.RLock()
	wq := s.devQueues[workerQueueName("w")]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)
	_, err = wq.Submit(context.Background(), queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "wm",
		RequestID: "rid-w", Payload: []byte("p"),
		Files: []*workerpb.ModelFile{{Filename: "onnx/whisper"}},
	})
	require.NoError(t, err)

	files, err := s.QueuedModelFiles(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"gguf/g/model.gguf":  {},
		"gguf/g/mmproj.gguf": {},
		"onnx/whisper":       {},
	}, files)
}

// No queues open yet: empty set, no error.
func TestQueuedModelFiles_Empty(t *testing.T) {
	s, _ := newTestScheduler(t)
	files, err := s.QueuedModelFiles(context.Background())
	require.NoError(t, err)
	require.Empty(t, files)
}

// Global rows: leased = "placed on a worker queue", which would show up
// under that worker's section. Snapshot must NOT double-render them here.
func TestQueueSnapshot_GlobalExcludesLeasedRows(t *testing.T) {
	s, _ := newTestScheduler(t)

	res, err := s.globalQ.Submit(context.Background(), queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-1",
		RequestID: "rid-1", Payload: []byte("p"),
	})
	require.NoError(t, err)

	// Lease the row → it disappears from the global section.
	leased, err := s.globalQ.LeaseByID(context.Background(), queue.MessageID(res.ID), 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, leased)

	sections, err := s.QueueSnapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, sections, 1)
	require.Equal(t, "global", sections[0].Name)
	require.Empty(t, sections[0].Rows, "leased global row must not appear in snapshot")
}

// Worker rows: a row is in flight when the scheduler holds an inflight
// record for its RequestID — NOT when the goqite lease is taken. The
// device-set gate briefly leases rows during retry attempts; those
// rows must stay pending in the UI. This test pins the inflight-map
// truth source.
func TestQueueSnapshot_WorkerInflightFlagDrivenByInflightTracker(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
	}))
	w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	s.queueMu.RLock()
	wq := s.devQueues[workerQueueName("w1")]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Row 1: leased but not in the inflight map → looks like a
	// gate-retry transient. Snapshot must report Inflight=false.
	res1, err := wq.Submit(context.Background(), queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-leased",
		RequestID: "rid-leased-only", Payload: []byte("p"),
	})
	require.NoError(t, err)
	leased1, err := wq.LeaseByID(context.Background(), queue.MessageID(res1.ID), time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased1)

	// Row 2: unleased on disk (goqite-pending) but the scheduler has
	// an inflight record for it (the row's lease just expired
	// mid-flight). Snapshot must report Inflight=true.
	_, err = wq.Submit(context.Background(), queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-tracked",
		RequestID: "rid-tracked", Payload: []byte("p"),
	})
	require.NoError(t, err)
	s.startInflight(workerQueueName("w1"), "rid-tracked", "m-tracked", "", 1.0, 0)

	sections, err := s.QueueSnapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, sections, 2)
	wsec := sections[1]
	require.Equal(t, workerQueueName("w1"), wsec.Name)
	require.Len(t, wsec.Rows, 2)

	byReq := map[string]bool{}
	for _, r := range wsec.Rows {
		byReq[r.RequestID] = r.Inflight
	}
	require.False(t, byReq["rid-leased-only"],
		"goqite-leased row without inflight record must NOT be reported as running")
	require.True(t, byReq["rid-tracked"],
		"row with inflight record must be reported as running")
}

// CancelQueuedRow handles four shapes: global-unleased happy path,
// worker-queue happy path (also kills the global anchor), leased row
// refused, unknown queue refused. Stage hooks build the queue state;
// assertion hooks verify outcomes specific to each branch.
func TestCancelQueuedRow(t *testing.T) {
	const requestID = "rid-1"
	tests := []struct {
		name      string
		queueName string // "global" / workerQueueName("w1") / workerQueueName("nope")
		// stage runs before CancelQueuedRow. Returns the msgID to cancel
		// plus the global msgID (for cleanup probing). Empty msgID means
		// "no row staged" — used by the unknown-queue case.
		stage    func(t *testing.T, s *Scheduler) (msgID, globalMsgID string)
		wantErr  error
		wantRows int // expected global queue depth after the call
		// checkResult: if true, the result store must show Error status
		// with errText "cancelled by operator".
		checkResult bool
		// checkFanout: if true, the queue-change callback must fire ≥1×.
		checkFanout bool
	}{
		{
			name:      "global unleased: row gone, result error, fanout fires",
			queueName: "global",
			stage: func(t *testing.T, s *Scheduler) (string, string) {
				t.Helper()
				require.NoError(t, s.results.Create(requestID))
				res, err := s.globalQ.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-g",
					RequestID: requestID, Payload: []byte("p"),
				})
				require.NoError(t, err)
				return res.ID, ""
			},
			wantRows:    0,
			checkResult: true,
			checkFanout: true,
		},
		{
			name:      "worker queue unleased: both rows gone, result error",
			queueName: workerQueueName("w1"),
			stage: func(t *testing.T, s *Scheduler) (string, string) {
				t.Helper()
				require.NoError(t, s.results.Create(requestID))
				gres, err := s.globalQ.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-w",
					RequestID: requestID, Payload: []byte("p"),
				})
				require.NoError(t, err)
				_, err = s.globalQ.LeaseByID(context.Background(), queue.MessageID(gres.ID), time.Minute)
				require.NoError(t, err)
				stageWorkerQueue(t, s, "w1")
				wres, err := s.devQueues[workerQueueName("w1")].Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-w",
					RequestID: requestID, GlobalMsgID: gres.ID, Payload: []byte("p"),
				})
				require.NoError(t, err)
				return wres.ID, gres.ID
			},
			wantRows:    0,
			checkResult: true,
		},
		{
			name:      "leased row refused with ErrRowInFlight; row preserved",
			queueName: "global",
			stage: func(t *testing.T, s *Scheduler) (string, string) {
				t.Helper()
				res, err := s.globalQ.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-1",
					RequestID: requestID, Payload: []byte("p"),
				})
				require.NoError(t, err)
				_, err = s.globalQ.LeaseByID(context.Background(), queue.MessageID(res.ID), time.Minute)
				require.NoError(t, err)
				return res.ID, res.ID // anchor == row; release on cleanup
			},
			wantErr:  ErrRowInFlight,
			wantRows: 1,
		},
		{
			name:      "unknown queue refused with ErrUnknownQueue",
			queueName: workerQueueName("nope"),
			stage:     func(*testing.T, *Scheduler) (string, string) { return "synthetic", "" },
			wantErr:   ErrUnknownQueue,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			var fanouts atomic.Int32
			s.SetQueueChangeCallback(func() { fanouts.Add(1) })

			msgID, globalMsgID := tt.stage(t, s)
			err := s.CancelQueuedRow(context.Background(), tt.queueName, msgID)

			if tt.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tt.wantErr),
					"got %v, want errors.Is(%v)", err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			// Global queue depth. For the worker-queue-success case we
			// need to release the leased anchor so Peek can see whether
			// DeleteBoth nuked it.
			if globalMsgID != "" && tt.wantErr == nil {
				require.NoError(t, s.globalQ.ReleaseLease(context.Background(), queue.MessageID(globalMsgID)))
			}
			if globalMsgID != "" && tt.wantErr != nil {
				require.NoError(t, s.globalQ.ReleaseLease(context.Background(), queue.MessageID(globalMsgID)))
			}
			rows, err := s.globalQ.Peek(context.Background(), 10)
			require.NoError(t, err)
			require.Len(t, rows, tt.wantRows, "global queue depth")

			if tt.checkResult {
				r, err := s.results.Get(requestID)
				require.NoError(t, err)
				require.NotNil(t, r)
				require.Equal(t, queue.ResultStatusError, r.Status)
				require.Equal(t, "cancelled by operator", r.Error)
			}
			if tt.checkFanout {
				require.GreaterOrEqual(t, fanouts.Load(), int32(1))
			}
		})
	}
}

// stageWorkerQueue registers a benched worker so its device queue
// materialises and is available in s.devQueues.
func stageWorkerQueue(t *testing.T, s *Scheduler, workerID string) {
	t.Helper()
	require.NoError(t, testStore(s).SaveBenchmark(store.BenchmarkRow{
		WorkerID: workerID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
	}))
	w := newFakeWorker(workerID, []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
}

// EvictQueuedRowToGlobal moves an unleased worker-queue row back to
// global, releasing its anchor so drainGlobal re-scores. Four cases:
// happy path, leased row refused, global-row refused (nowhere to
// evict to), unknown queue refused.
func TestEvictQueuedRowToGlobal(t *testing.T) {
	const requestID = "rid-evict"
	tests := []struct {
		name      string
		queueName string
		// stage returns the msgID to evict. Empty msgID is fine for the
		// unknown-queue case.
		stage   func(t *testing.T, s *Scheduler) string
		wantErr error
		// happy-path assertions: each enabled by a non-nil verifier.
		checkRowGone     bool // worker row deleted
		checkAnchorBack  bool // global anchor visible again
		checkTailDebited bool // tail_seconds back to 0
		checkResultStays bool // result entry still pending
		checkFanout      bool
	}{
		{
			name:      "happy path: row off worker queue, anchor released, tail debited, result pending",
			queueName: workerQueueName("w1"),
			stage: func(t *testing.T, s *Scheduler) string {
				t.Helper()
				st := testStore(s)
				require.NoError(t, s.results.Create(requestID))
				gres, err := s.globalQ.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-e",
					RequestID: requestID, Payload: []byte("p"),
				})
				require.NoError(t, err)
				_, err = s.globalQ.LeaseByID(context.Background(), queue.MessageID(gres.ID), time.Minute)
				require.NoError(t, err)
				stageWorkerQueue(t, s, "w1")
				wqName := workerQueueName("w1")
				require.NoError(t, st.AddTailSecondsAndSetModel(wqName, 4.0, "m-e"))
				wres, err := s.devQueues[wqName].Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-e",
					RequestID: requestID, GlobalMsgID: gres.ID, QueuedSeconds: 4.0,
					Payload: []byte("p"),
				})
				require.NoError(t, err)
				return wres.ID
			},
			checkRowGone:     true,
			checkAnchorBack:  true,
			checkTailDebited: true,
			checkResultStays: true,
			checkFanout:      true,
		},
		{
			name:      "leased row refused with ErrRowInFlight",
			queueName: workerQueueName("w1"),
			stage: func(t *testing.T, s *Scheduler) string {
				t.Helper()
				stageWorkerQueue(t, s, "w1")
				wq := s.devQueues[workerQueueName("w1")]
				res, err := wq.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-1",
					RequestID: "rid-1", GlobalMsgID: "gmid-1", Payload: []byte("p"),
				})
				require.NoError(t, err)
				_, err = wq.LeaseByID(context.Background(), queue.MessageID(res.ID), time.Minute)
				require.NoError(t, err)
				return res.ID
			},
			wantErr: ErrRowInFlight,
		},
		{
			name:      "global row refused with ErrEvictGlobalRow",
			queueName: "global",
			stage: func(t *testing.T, s *Scheduler) string {
				t.Helper()
				res, err := s.globalQ.Submit(context.Background(), queue.Envelope{
					Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m-1",
					RequestID: "rid-1", Payload: []byte("p"),
				})
				require.NoError(t, err)
				return res.ID
			},
			wantErr: ErrEvictGlobalRow,
		},
		{
			name:      "unknown queue refused with ErrUnknownQueue",
			queueName: workerQueueName("nope"),
			stage:     func(*testing.T, *Scheduler) string { return "synthetic" },
			wantErr:   ErrUnknownQueue,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			var fanouts atomic.Int32
			s.SetQueueChangeCallback(func() { fanouts.Add(1) })

			msgID := tt.stage(t, s)
			err := s.EvictQueuedRowToGlobal(context.Background(), tt.queueName, msgID)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tt.wantErr),
					"got %v, want errors.Is(%v)", err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			wqName := workerQueueName("w1")
			if tt.checkRowGone {
				wq := s.devQueues[wqName]
				wPeek, err := wq.Peek(context.Background(), 10)
				require.NoError(t, err)
				require.Empty(t, wPeek)
			}
			if tt.checkAnchorBack {
				gPeek, err := s.globalQ.Peek(context.Background(), 10)
				require.NoError(t, err)
				require.Len(t, gPeek, 1, "global anchor must become visible after evict")
			}
			if tt.checkTailDebited {
				dqState, err := testStore(s).GetWorkerQueueState(wqName)
				require.NoError(t, err)
				require.InDelta(t, 0.0, dqState.TailSeconds, 1e-9, "tail must be debited")
			}
			if tt.checkResultStays {
				r, err := s.results.Get(requestID)
				require.NoError(t, err)
				require.NotNil(t, r)
				require.Equal(t, queue.ResultStatusPending, r.Status)
			}
			if tt.checkFanout {
				require.GreaterOrEqual(t, fanouts.Load(), int32(1))
			}
		})
	}
}

// CancelRunningJob is gated by three pieces of state: whether an
// inflight record exists, whether the worker is registered, and
// whether the worker-side jobID has been attached. Each row pins one
// branch of the gate.
func TestCancelRunningJob(t *testing.T) {
	tests := []struct {
		name              string
		stageInflight     bool   // call startInflight before CancelRunningJob
		attachJobID       bool   // call attachWorkerJobID before CancelRunningJob
		registerWorker    bool   // register w1 with the fleet (otherwise ErrWorkerGone)
		requestID         string // request ID to cancel
		inflightRequestID string // request ID to use for startInflight (defaults to requestID)
		wantErr           error  // nil for success
		wantWireCancels   int    // HubCancelJob wire messages observed
		wantCancelFlag    bool   // s.isInflightCancelled after the call
	}{
		{
			name:      "no inflight record → ErrNotInflight",
			requestID: "rid-nope",
			wantErr:   ErrNotInflight,
		},
		{
			name:           "happy path: HubCancelJob fires + flag set",
			stageInflight:  true,
			attachJobID:    true,
			registerWorker: true,
			requestID:      "rid-1",
			// Wire message carries the worker-side jobID, not the request
			// ID — verified by the sender callback below.
			wantWireCancels: 1,
			wantCancelFlag:  true,
		},
		{
			name:            "cancel races attachWorkerJobID: no wire, flag set",
			stageInflight:   true,
			attachJobID:     false,
			registerWorker:  true,
			requestID:       "rid-pre",
			wantWireCancels: 0,
			wantCancelFlag:  true,
		},
		{
			name:          "worker gone: inflight stamped but never registered → ErrWorkerGone",
			stageInflight: true,
			attachJobID:   true,
			requestID:     "rid-x",
			wantErr:       ErrWorkerGone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)

			var wireCancels atomic.Int32
			if tt.registerWorker {
				w := newFakeWorker("w1", []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}})
				w.SetFakeSender(func(msg *workerpb.HubMessage) error {
					if cj := msg.GetCancelJob(); cj != nil {
						wireCancels.Add(1)
						require.Equal(t, "worker-job-"+tt.requestID, cj.GetJobId(),
							"HubCancelJob must carry the worker-side jobID")
					}
					return nil
				})
				require.NoError(t, s.workers.Register(w))
			}

			if tt.stageInflight {
				inflightID := tt.inflightRequestID
				if inflightID == "" {
					inflightID = tt.requestID
				}
				s.startInflight(workerQueueName("w1"), inflightID, "", "", 1.0, 0)
				if tt.attachJobID {
					s.attachWorkerJobID(inflightID, "worker-job-"+inflightID)
				}
			}

			err := s.CancelRunningJob(context.Background(), tt.requestID)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tt.wantErr),
					"got %v, want errors.Is(%v)", err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(tt.wantWireCancels), wireCancels.Load(),
				"wire CancelJob count")
			require.Equal(t, tt.wantCancelFlag, s.isInflightCancelled(tt.requestID),
				"isInflightCancelled flag")
		})
	}
}

// When operator-cancel races a worker-emitted error frame, the pump
// must rewrite the result message to the stable operator-facing string
// regardless of what the worker put on the wire. Guards against gateways
// surfacing raw runtime errors when the cause was operator action.
func TestPumpWorkerChunks_OperatorCancelRewritesErrText(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
	}))

	jobIDCh := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName, []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case jobIDCh <- aj.GetJobId():
			default:
			}
		}
		// HubCancelJob is fire-and-forget; nothing to capture.
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	requestID, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100,
	})
	require.NoError(t, err)

	s.dispatchPass(context.Background())
	var workerJobID string
	select {
	case workerJobID = <-jobIDCh:
	case <-time.After(2 * time.Second):
		t.Fatal("AssignJob did not fire within 2s")
	}

	// Operator cancels mid-flight. The intent must reach the inflight
	// record before the worker's terminal frame arrives, so the pump
	// observes wasCancelled=true.
	require.NoError(t, s.CancelRunningJob(context.Background(), requestID))

	// Worker emits its own error message (whatever runtime chose). The
	// pump should rewrite this to the operator-facing string.
	w.DeliverJobChunk(workerJobID, &worker.JobChunk{
		Type:    worker.JobChunkTypeError,
		ErrText: "Chat: cancelled by operator", // raw worker error
	})

	// Wait for the pump goroutine to finalise.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		res, err := s.results.Get(requestID)
		require.NoError(t, err)
		require.NotNil(t, res)
		if res.Status == queue.ResultStatusError {
			require.Equal(t, "cancelled by operator", res.Error,
				"pump must rewrite errText to the stable operator-facing string")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("result never transitioned to error within 2s")
}
