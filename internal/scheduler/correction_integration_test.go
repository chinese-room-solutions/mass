package scheduler

import (
	"context"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// dispatchOneAndDeliver submits a single job to a resident worker, drives
// one dispatch pass, captures the AssignJob worker-side id, and delivers
// the given terminal chunk — the full pumpWorkerChunks path. Returns once
// the durable result reaches a terminal state.
func dispatchOneAndDeliver(t *testing.T, terminal *worker.JobChunk) *Scheduler {
	t.Helper()
	const runtimeName, modelID = "llama-cpp", "m-1"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assignCh := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	// Resident so the device-set gate is a no-op and no load fires.
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 4, Active: 0, DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assignCh <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	requestID, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	dispatchDone := make(chan struct{})
	go func() { s.dispatchPass(context.Background()); close(dispatchDone) }()

	var jobID string
	select {
	case jobID = <-assignCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected AssignJob")
	}
	<-dispatchDone

	// Hold the job briefly so its measured wall-clock clears
	// correctionMinActualSec — otherwise observeThroughput correctly drops
	// it as overhead-dominated and no sample is recorded.
	time.Sleep(time.Duration(correctionMinActualSec*float64(time.Second)) + 30*time.Millisecond)
	w.DeliverJobChunk(jobID, terminal)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res, err := s.results.Get(requestID)
		require.NoError(t, err)
		if res != nil && (res.Status == queue.ResultStatusDone || res.Status == queue.ResultStatusError) {
			return s
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("result never reached terminal state")
	return nil
}

func correctionSampleCount(s *Scheduler, workerID, axis string) int {
	s.correctionMu.Lock()
	defer s.correctionMu.Unlock()
	return s.throughputCorrection[workerID+"|"+axis].samples
}

// An ok terminal flowing through pumpWorkerChunks feeds the correction
// loop. This is the wiring the direct observeThroughput tests can't prove.
func TestCorrection_OkTerminalFeedsLoop(t *testing.T) {
	s := dispatchOneAndDeliver(t, &worker.JobChunk{
		Type:  worker.JobChunkTypeCompleted,
		Final: []byte("ok"),
	})
	require.Equal(t, 1, correctionSampleCount(s, "w1", "q4k_matvec"),
		"a clean completion must record one correction sample")
}

// An error terminal must NOT feed the loop — its wall-clock is a failure
// latency, not a throughput signal. This guards the "clean completions
// only" contract in pumpWorkerChunks against future refactors.
func TestCorrection_ErrorTerminalDoesNotFeedLoop(t *testing.T) {
	s := dispatchOneAndDeliver(t, &worker.JobChunk{
		Type:    worker.JobChunkTypeError,
		ErrText: "boom",
	})
	require.Zero(t, correctionSampleCount(s, "w1", "q4k_matvec"),
		"a failed job must not record a correction sample")
}
