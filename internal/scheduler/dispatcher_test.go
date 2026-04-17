package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/chinese-room-solutions/mass/pkg/workerpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- test infrastructure ---

type testEnv struct {
	globalQ    queue.QueueInterface
	results    queue.ResultStoreInterface
	stateStore store.DeviceQueueStateStoreInterface
	benchStore store.BenchmarkStoreInterface
	db         *sql.DB // raw handle for goqite-row assertions in tests
	logger     zerolog.Logger
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)
	db := s.DB()

	return &testEnv{
		globalQ:    queue.NewNamed(db, "global", 1, 30*time.Second),
		results:    queue.NewResultStore(db),
		stateStore: s,
		benchStore: s,
		db:         db,
		logger:     logger,
	}
}

func (e *testEnv) submitTask(t *testing.T, fingerprint string, payload []byte) string {
	t.Helper()
	// testModelSize is large so the swap cost dominates wait differences in
	// these tests — matching real-world model swap dynamics where loading a
	// multi-gigabyte file is usually slower than burning down a short queue.
	sub, err := e.globalQ.SubmitRaw(context.Background(), queue.RequestTypeChatCompletion, payload, "direct", fingerprint, uint64(testModelSize), queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, e.results.Create(sub.ID, sub.RequestHash))
	return sub.ID
}

func (e *testEnv) registerDevices(t *testing.T, workerID string, deviceIDs []string, gflops float64) {
	t.Helper()
	for _, did := range deviceIDs {
		require.NoError(t, e.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
			QueueName: DeviceQueueName(workerID, did),
			WorkerID:  workerID,
			DeviceIDs: []string{did},
			Enabled:   true,
		}))
		require.NoError(t, e.benchStore.SaveBenchmark(store.BenchmarkRow{
			WorkerID: workerID, DeviceID: did,
			ComputeGFlops: gflops, MemoryGBs: 550,
		}))
	}
}

func (e *testEnv) newPool(t *testing.T) *modelPool {
	t.Helper()
	return newModelPool(&nilLoader{}, "test", "test-agent", e.logger, 0)
}

func (e *testEnv) newDispatcher(t *testing.T, deviceIDs []string, deviceQueues map[string]*DeviceQueueManager) *Dispatcher {
	t.Helper()
	pool := e.newPool(t)
	reg := newTestFleet(t, deviceIDs)
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: e.globalQ,
		Pool:        pool,
		Workers:     reg,
		Results:     e.results,
		StateStore:  e.stateStore,
		BenchStore:  e.benchStore,
		Logger:      e.logger,
	})
	for name, dq := range deviceQueues {
		dq.dispatcher = d
		d.Add(name, dq)
	}
	return d
}

// --- stubs ---

// nilLoader satisfies ModelLoaderInterface but never loads anything.
type nilLoader struct{}

func (n *nilLoader) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfigInterface, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return nil, nil
}

func (n *nilLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfigInterface, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return nil, nil
}

// dispatchTestWorker satisfies worker.WorkerInterface for dispatch tests.
type dispatchTestWorker struct {
	id      string
	online  bool
	devices []stats.Device
}

func (a *dispatchTestWorker) ID() string                 { return a.id }
func (a *dispatchTestWorker) Name() string               { return a.id }
func (a *dispatchTestWorker) Stats() []stats.DeviceStats { return nil }
func (a *dispatchTestWorker) Devices() []stats.Device    { return a.devices }

func (a *dispatchTestWorker) Status() worker.WorkerStatus {
	return worker.WorkerStatus{Online: a.online}
}

func (a *dispatchTestWorker) Bench(_ string) (bench.Result, error) { return bench.Result{}, nil }

func (a *dispatchTestWorker) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfigInterface, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return &stubChatModel{}, nil
}

func (a *dispatchTestWorker) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfigInterface, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return &stubEmbeddingModel{}, nil
}

// Compile-time check: dispatchTestWorker satisfies WorkerInterface.
var _ worker.WorkerInterface = (*dispatchTestWorker)(nil)

// workerSpec describes an agent and its devices for test setup.
type workerSpec struct {
	id        string
	online    bool
	deviceIDs []string
	memoryMB  int // per device, default 8000
}

func newTestFleet(t *testing.T, deviceIDs []string) *worker.Fleet {
	t.Helper()
	return newMultiWorkerFleet(t, workerSpec{id: "local", online: true, deviceIDs: deviceIDs, memoryMB: 8000})
}

func newMultiWorkerFleet(t *testing.T, workers ...workerSpec) *worker.Fleet {
	t.Helper()
	reg := worker.NewFleet()
	for _, a := range workers {
		memMB := a.memoryMB
		if memMB == 0 {
			memMB = 8000
		}
		var devs []stats.Device
		for _, d := range a.deviceIDs {
			devs = append(devs, stats.Device{ID: d, Type: "GPU", TotalMemoryMB: memMB})
		}
		require.NoError(t, reg.Register(&dispatchTestWorker{id: a.id, online: a.online, devices: devs}))
	}
	return reg
}

func makeDQM(name, workerID string, deviceIDs []string, q queue.QueueInterface, pool *modelPool, logger zerolog.Logger) *DeviceQueueManager {
	return &DeviceQueueManager{
		queueName: name, workerID: workerID, deviceIDs: deviceIDs,
		queue: q, pool: pool, logger: logger, maxConcurrent: 1,
		workerDone: make(chan struct{}, 1),
		stateStore: noopStateStore{},
	}
}

// noopStateStore satisfies DeviceQueueStateStoreInterface for tests that
// construct DeviceQueueManager directly. Methods that mutate state are
// best-effort in production (logged warnings on failure), so swallowing
// them in unit tests is correct behavior — the tests under test_env that
// care about persisted state use env.stateStore directly.
type noopStateStore struct{}

func (noopStateStore) UpsertDeviceQueueState(store.DeviceQueueState) error { return nil }
func (noopStateStore) GetDeviceQueueState(string) (store.DeviceQueueState, error) {
	return store.DeviceQueueState{}, nil
}
func (noopStateStore) ListDeviceQueueStates() ([]store.DeviceQueueState, error) { return nil, nil }
func (noopStateStore) DeleteDeviceQueueState(string) error                      { return nil }
func (noopStateStore) UpdateTail(string, string, float64) error                 { return nil }
func (noopStateStore) AddTailDifficulty(string, float64) error                  { return nil }
func (noopStateStore) UpdateLoadedHash(string, string) error                    { return nil }
func (noopStateStore) SetEnabled(string, bool) error                            { return nil }

// registerTestModel loads a stub model into the pool on a specific agent/device.
// Returns the computed fingerprint.
func registerTestModel(t *testing.T, pool *modelPool, path string, deviceIDs ...string) string {
	t.Helper()
	cfg := config.LlamaChatConfig{Path: path}
	return pool.RegisterChat(ModeStatic, "direct", "", cfg, config.PlacementConfig{}, &stubChatModel{}, "local", "local", deviceIDs...)
}

// --- tests ---

// TestDispatcher_collectCandidates_AttachesManager guards Shape B's central
// invariant: every candidate scoring receives must carry its
// DeviceQueueManager pointer, resolved while the dispatcher's lock is held.
// This is what eliminates the orphan-manager branch — without it we'd be
// back to a separate `d.deviceQueues[name]` lookup that could race against
// worker disconnect.
func TestDispatcher_collectCandidates_AttachesManager(t *testing.T) {
	env := newTestEnv(t)
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	cands := d.collectCandidates(func(_, _ string) bool { return true })
	require.Len(t, cands, 1)
	require.NotNil(t, cands[0].Manager, "candidate must carry its manager pointer")
	require.Same(t, d.Get("device:local:gpu:0"), cands[0].Manager,
		"manager pointer must point at the registered manager")
}

func TestDispatcher_AffinityRouting(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	// gpu:0 is running model "abc" (tail=5), gpu:1 is running "def" (tail=2).
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "abc", 5))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "abc"))
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:1", "def", 2))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:1", "def"))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// Task with fingerprint "abc" should route to gpu:0 (affinity).
	env.submitTask(t, "abc", []byte("task1"))
	d.dispatchOne(ctx)

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg0, "task should route to gpu:0 via affinity")

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg1, "gpu:1 should be empty")
}

// TestDispatcher_PreferShorterMismatchedQueue — when neither candidate
// matches the new fingerprint (both incur the same swap cost), the task
// goes to the lighter queue, which finishes the existing work sooner and
// gets to the new task earlier. Equal GFlops keeps the tiebreak from
// kicking in, so the result is purely the cost-model preference.
func TestDispatcher_PreferShorterMismatchedQueue(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	// Both queues hold "xyz" tasks; gpu:0 has a heavier backlog.
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "xyz", 10_000_000))
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:1", "xyz", 1_000_000))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	env.submitTask(t, "new", []byte("task-new"))
	d.dispatchOne(ctx)

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg1, "task should route to gpu:1 — same swap cost on both, lighter queue wins")

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg0, "heavier-queue gpu:0 should be skipped")
}

func TestDispatcher_GFlops_Tiebreak(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)

	// gpu:0 = 1000 GFlops, gpu:1 = 2000 GFlops.
	env.registerDevices(t, "local", []string{"gpu:0"}, 1000)
	require.NoError(t, env.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
		QueueName: "device:local:gpu:1", WorkerID: "local", DeviceIDs: []string{"gpu:1"},
		Enabled: true,
	}))
	require.NoError(t, env.benchStore.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "local", DeviceID: "gpu:1",
		ComputeGFlops: 2000, MemoryGBs: 800,
	}))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	env.submitTask(t, "brand-new", []byte("route-me"))
	d.dispatchOne(ctx)

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg0, "weaker gpu:0 should not get the task")

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg1, "stronger gpu:1 should get the task")
}

func TestDispatcher_PriorityOrdering(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	// Submit low, then critical.
	_, err := env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("low-task"), "direct", "fp1", 0, queue.PriorityLow)
	require.NoError(t, err)
	_, err = env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("critical-task"), "direct", "fp1", 0, queue.PriorityCritical)
	require.NoError(t, err)

	// First dispatch should pick critical.
	d.dispatchOne(ctx)
	msg, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	dispatched, err := queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("critical-task"), dispatched.Payload)

	// Second dispatch should pick low.
	d.dispatchOne(ctx)
	msg, err = dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	dispatched, err = queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("low-task"), dispatched.Payload)
}

func TestDispatcher_RequestIDPreserved(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	originalID := env.submitTask(t, "fp1", []byte("track-me"))
	d.dispatchOne(ctx)

	msg, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	dispatched, err := queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, originalID, dispatched.RequestID, "RequestID must survive global→device queue hop")
}

func TestDispatcher_WorkStealing_SaveDonorFromContextSwitch(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger)
	thief.loadedHash = "abc"

	donor := makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger)
	donor.loadedHash = "abc"

	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": thief,
		"device:local:gpu:1": donor,
	})

	// Enqueue "def" task on donor — would cause a context switch there.
	donorEnv := queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "def", RequestID: "orig-1", Payload: []byte("steal-me"),
	}
	_, err := dq1.SubmitEnvelope(ctx, donorEnv, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal task that would cause context switch on donor")
	require.Equal(t, "device:local:gpu:1", result.FromQueue)
	require.Equal(t, 1, result.Count)
	// Donor row was moved transactionally — donor queue empty, thief has the row.
	donorDepth, err := dq1.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, donorDepth)
	thiefDepth, err := dq0.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, thiefDepth)
}

func TestDispatcher_MultipleTasksSameFingerprint_BuildSequence(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	// gpu:0 already has "model-a" running (tail=1).
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "model-a", 1))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "model-a"))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// Submit 3 tasks all with "model-a" — all should go to gpu:0 to build the sequence.
	for i := 0; i < 3; i++ {
		env.submitTask(t, "model-a", []byte("seq-task"))
		d.dispatchOne(ctx)
	}

	for i := 0; i < 3; i++ {
		msg, err := dq0.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg, "task %d should be on gpu:0", i)
	}

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg1, "gpu:1 should have no tasks")
}

func TestDispatcher_EmptyGlobalQueue_NoOp(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	d.dispatchOne(ctx)

	msg, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg, "no tasks should appear on device queue")
}

// --- Multi-agent tests ---

func TestDispatcher_MultiWorker_AffinityAcrossAgents(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	localQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)

	// Local gpu has model "abc", remote gpu has model "def".
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 1800)
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "abc", 3))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "abc"))
	require.NoError(t, env.stateStore.UpdateTail("device:server:gpu:0", "def", 3))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:server:gpu:0", "def"))

	pool := env.newPool(t)
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ, pool, env.logger))
	d.Add("device:server:gpu:0", makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger))

	// Task for "def" should go to the server (affinity).
	env.submitTask(t, "def", []byte("remote-task"))
	d.dispatchOne(ctx)

	localMsg, err := localQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, localMsg, "local should not get 'def' task")

	remoteMsg, err := remoteQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, remoteMsg, "'def' task should route to server via affinity")
}

func TestDispatcher_MultiWorker_StrongerDeviceWins(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	weakQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	strongQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)

	// Local = 500 GFlops (weak), Server = 5000 GFlops (strong). No affinity.
	env.registerDevices(t, "local", []string{"gpu:0"}, 500)
	env.registerDevices(t, "server", []string{"gpu:0"}, 5000)

	pool := env.newPool(t)
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, weakQ, pool, env.logger))
	d.Add("device:server:gpu:0", makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, strongQ, pool, env.logger))

	env.submitTask(t, "new-model", []byte("power-task"))
	d.dispatchOne(ctx)

	weakMsg, err := weakQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, weakMsg, "weak local device should not get the task")

	strongMsg, err := strongQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, strongMsg, "strong server device should get the task")
}

func TestDispatcher_OfflineWorkerSkipped(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	offlineQ := queue.NewNamed(env.db, "device:offline-worker:gpu:0", 1, 30*time.Second)
	onlineQ := queue.NewNamed(env.db, "device:online-worker:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "offline-worker", []string{"gpu:0"}, 5000) // stronger but offline
	env.registerDevices(t, "online-worker", []string{"gpu:0"}, 1000)  // weaker but online

	pool := env.newPool(t)
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "offline-worker", online: false, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "online-worker", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:offline-worker:gpu:0", makeDQM("device:offline-worker:gpu:0", "offline-worker", []string{"gpu:0"}, offlineQ, pool, env.logger))
	d.Add("device:online-worker:gpu:0", makeDQM("device:online-worker:gpu:0", "online-worker", []string{"gpu:0"}, onlineQ, pool, env.logger))

	env.submitTask(t, "any-model", []byte("online-only"))
	d.dispatchOne(ctx)

	offMsg, err := offlineQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, offMsg, "offline worker should never receive tasks")

	onMsg, err := onlineQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, onMsg, "online worker should receive the task")
}

func TestDispatcher_DisabledQueueSkipped(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	disabledQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	enabledQ := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0"}, 5000) // stronger but disabled
	env.registerDevices(t, "local", []string{"gpu:1"}, 1000)

	// Disable gpu:0.
	require.NoError(t, env.stateStore.SetEnabled("device:local:gpu:0", false))

	pool := env.newPool(t)
	reg := newTestFleet(t, []string{"gpu:0", "gpu:1"})

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, disabledQ, pool, env.logger))
	d.Add("device:local:gpu:1", makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, enabledQ, pool, env.logger))

	env.submitTask(t, "any-model", []byte("enabled-only"))
	d.dispatchOne(ctx)

	disMsg, err := disabledQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, disMsg, "disabled queue should never receive tasks")

	enMsg, err := enabledQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, enMsg, "enabled queue should receive the task")
}

func TestDispatcher_CrossWorkerWorkStealing(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	localQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	// Local thief has model "abc" loaded, is idle.
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ, pool, env.logger)
	thief.loadedHash = "abc"

	// Remote donor has model "abc" loaded, but queued a "xyz" task (context switch).
	donor := makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger)
	donor.loadedHash = "abc"

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", thief)
	d.Add("device:server:gpu:0", donor)

	// Enqueue "xyz" on remote donor.
	_, err := remoteQ.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "xyz", RequestID: "cross-1", Payload: []byte("steal-cross-agent"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal across workers")
	require.Equal(t, "device:server:gpu:0", result.FromQueue)
}

func TestDispatcher_MultiWorker_MixedLoad(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	// 3 workers: local (2 GPUs), server1 (1 GPU), server2 (1 GPU, offline).
	localQ0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	localQ1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	srv1Q := queue.NewNamed(env.db, "device:server1:gpu:0", 1, 30*time.Second)
	srv2Q := queue.NewNamed(env.db, "device:server2:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)
	env.registerDevices(t, "server1", []string{"gpu:0"}, 3000)
	env.registerDevices(t, "server2", []string{"gpu:0"}, 5000) // strongest but offline

	// local:gpu:0 has model "alpha" loaded.
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "alpha", 2))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "alpha"))

	pool := env.newPool(t)
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "gpu:1"}},
		workerSpec{id: "server1", online: true, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "server2", online: false, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ0, pool, env.logger))
	d.Add("device:local:gpu:1", makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, localQ1, pool, env.logger))
	d.Add("device:server1:gpu:0", makeDQM("device:server1:gpu:0", "server1", []string{"gpu:0"}, srv1Q, pool, env.logger))
	d.Add("device:server2:gpu:0", makeDQM("device:server2:gpu:0", "server2", []string{"gpu:0"}, srv2Q, pool, env.logger))

	// Task 1: "alpha" → should go to local:gpu:0 (affinity).
	env.submitTask(t, "alpha", []byte("affinity-hit"))
	d.dispatchOne(ctx)

	msg, err := localQ0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg, "alpha task should route to local:gpu:0 via affinity")

	// Task 2: "beta" (no affinity) — every candidate pays the same swap cost,
	// so the deciding factor is the wait term: server1:gpu:0 has the empty
	// queue and the highest GFlops among the empty-queue candidates, so the
	// new task should land there.
	env.submitTask(t, "beta", []byte("power-pick"))
	d.dispatchOne(ctx)

	srv2Msg, err := srv2Q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, srv2Msg, "offline server2 must not receive tasks")

	srv1Msg, err := srv1Q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, srv1Msg, "beta task should route to server1:gpu:0 (empty queue, strongest among free devices)")
}

// --- Edge case tests ---

func TestDispatcher_PriorityPreservedAcrossHops(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	// Submit a critical task.
	_, err := env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("critical-payload"), "direct", "fp1", 0, queue.PriorityCritical)
	require.NoError(t, err)

	d.dispatchOne(ctx)

	msg, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	dispatched, err := queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, queue.PriorityCritical, dispatched.Priority, "priority must survive global→device queue hop")
}

func TestDispatcher_NoDevices_LeavesTaskInQueue(t *testing.T) {
	// When no devices are available the dispatcher must NOT delete the
	// message and must NOT mark its result as failed — the task simply waits
	// in goqite (under visibility timeout) until devices come online or
	// resources free up.
	env := newTestEnv(t)
	ctx := context.Background()

	pool := env.newPool(t)
	reg := worker.NewFleet() // empty — no workers
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})

	originalID := env.submitTask(t, "unreachable", []byte("waiting"))

	// Dispatch many times — task must stay put.
	for i := 0; i < 10; i++ {
		d.dispatchOne(ctx)
	}

	// Result must still be pending (not failed).
	result, err := env.results.Get(originalID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, result.Status,
		"task must remain pending — no false failures from a busy queue head")

	// The message row must still exist in goqite, even though it's invisible
	// to Receive() between dispatch ticks.
	var rowCount int
	require.NoError(t, env.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM goqite WHERE queue = ?`, "global").Scan(&rowCount))
	require.Equal(t, 1, rowCount, "task row must still be in goqite")
}

func TestDispatcher_AllDevicesSameFingerprint(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)

	// Both GPUs have same model, but gpu:1 is stronger.
	env.registerDevices(t, "local", []string{"gpu:0"}, 1000)
	require.NoError(t, env.benchStore.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "local", DeviceID: "gpu:1", ComputeGFlops: 2000, MemoryGBs: 800,
	}))
	require.NoError(t, env.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
		QueueName: "device:local:gpu:1", WorkerID: "local", DeviceIDs: []string{"gpu:1"},
		Enabled: true,
	}))

	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "same-model", 5))
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:1", "same-model", 3))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// Both tails match → swap cost is zero on both. The deciding factor is
	// (TailDifficulty + taskDifficulty) / GFlops; gpu:1 has the lighter tail
	// AND the stronger device (2000 vs 1000 GFlops), so it wins on both
	// terms — finishes existing work faster and runs the new task faster.
	env.submitTask(t, "same-model", []byte("pick-faster"))
	d.dispatchOne(ctx)

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg1, "should prefer gpu:1 (lighter queue, stronger device)")

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg0)
}

func TestDispatcher_AllWorkersOffline_TaskWaits(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// All workers offline — no candidate available.
	reg := newMultiWorkerFleet(t,
		workerSpec{id: "worker1", online: false, deviceIDs: []string{"gpu:0"}},
		workerSpec{id: "worker2", online: false, deviceIDs: []string{"gpu:0"}},
	)
	env.registerDevices(t, "worker1", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "worker2", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})

	originalID := env.submitTask(t, "any", []byte("waiting"))

	d.dispatchOne(ctx)

	// Result stays pending; the message stays in goqite (invisible until
	// the visibility timeout redelivers it for another dispatch attempt).
	result, err := env.results.Get(originalID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, result.Status)

	var rowCount int
	require.NoError(t, env.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM goqite WHERE queue = ?`, "global").Scan(&rowCount))
	require.Equal(t, 1, rowCount, "task row must still be in goqite")
}

func TestDispatcher_LoadedModelPrefersOverCold(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	// gpu:0 has model "abc" loaded, but queue is empty (no tail).
	// gpu:1 is cold (nothing loaded, nothing queued).
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "abc"))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// Task for "abc" should go to gpu:0 (model already loaded, avoids loading time).
	env.submitTask(t, "abc", []byte("warm-hit"))
	d.dispatchOne(ctx)

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg0, "task should route to gpu:0 (model already loaded)")

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg1, "cold gpu:1 should not get the task")
}

// --- Eviction and device-specific tests ---

func TestDispatcher_AvailablePlacement_SkipsOccupiedDevices(t *testing.T) {
	// gpu:0 has model "A" loaded, gpu:1 is empty.
	// selectAvailablePlacement for "B" should pick gpu:1, not gpu:0.
	env := newTestEnv(t)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: newTestFleet(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger))
	d.Add("device:local:gpu:1", makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger))

	fpB := config.LlamaChatConfig{Path: "/model-B.gguf"}.Fingerprint()
	candidate := d.selectAvailablePlacement(fpB)
	require.NotNil(t, candidate)
	require.Equal(t, "device:local:gpu:1", candidate.QueueName, "should pick empty gpu:1, not occupied gpu:0")
}

func TestDispatcher_AvailablePlacement_AllOccupied_ReturnsNil(t *testing.T) {
	// Both devices occupied with different models → no placement available.
	env := newTestEnv(t)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	fpB := registerTestModel(t, pool, "/model-B.gguf", "gpu:1")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:1", fpB))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: newTestFleet(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger))
	d.Add("device:local:gpu:1", makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger))

	fpC := config.LlamaChatConfig{Path: "/model-C.gguf"}.Fingerprint()
	candidate := d.selectAvailablePlacement(fpC)
	require.Nil(t, candidate, "all devices occupied, no placement available")
}

func TestDispatcher_AvailablePlacement_SameModel_ReusesDevice(t *testing.T) {
	// gpu:0 has model "A" loaded. Loading "A" again should reuse gpu:0.
	env := newTestEnv(t)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: newTestFleet(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger))
	d.Add("device:local:gpu:1", makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger))

	// Same fingerprint as A — should pick gpu:0 (already loaded there).
	candidate := d.selectAvailablePlacement(fpA)
	require.NotNil(t, candidate)
	require.Equal(t, "device:local:gpu:0", candidate.QueueName, "should reuse gpu:0 where model A is loaded")
}

func TestDispatcher_EvictThenPlace_MultiDevice(t *testing.T) {
	// 3 devices: gpu:0 has "A", cpu:0 has "B", remote gpu:0 is empty.
	// Loading "C" should find remote:gpu:0 (empty) without evicting anything.
	env := newTestEnv(t)
	localGpuQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(env.db, "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	fpB := registerTestModel(t, pool, "/model-B.gguf", "cpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Workers: reg,
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.Add("device:local:gpu:0", makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, pool, env.logger))
	d.Add("device:local:cpu:0", makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, pool, env.logger))
	d.Add("device:server:gpu:0", makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger))

	fpC := config.LlamaChatConfig{Path: "/model-C.gguf"}.Fingerprint()
	candidate := d.selectAvailablePlacement(fpC)
	require.NotNil(t, candidate)
	require.Equal(t, "device:server:gpu:0", candidate.QueueName, "should pick empty remote device without evicting local models")

	// Both local models should still be in pool.
	require.True(t, pool.HasChat(fpA), "model A should not be evicted")
	require.True(t, pool.HasChat(fpB), "model B should not be evicted")
}

func TestDispatcher_WorkStealing_MatchThiefModel(t *testing.T) {
	// Priority B: thief has model "abc" loaded and is idle.
	// Donor has model "xyz" loaded, but has a queued "abc" task.
	// Thief should steal the "abc" task from donor.
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger)
	thief.loadedHash = "abc"

	donor := makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger)
	donor.loadedHash = "xyz" // donor has a DIFFERENT model loaded

	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": thief,
		"device:local:gpu:1": donor,
	})

	// Enqueue "abc" task on donor — matches thief's loaded model, NOT donor's.
	// Priority A won't fire because "abc" != donor's loaded "xyz" is a mismatch,
	// but the first peeked task IS the mismatch, so priority A would steal it.
	// To isolate priority B, we make donor's first task match donor's model,
	// and put the thief-matching task second.
	_, err := dq1.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "xyz", RequestID: "donor-task", Payload: []byte("donor-keeps"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	_, err = dq1.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "abc", RequestID: "thief-match", Payload: []byte("steal-me"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal task matching thief's model (priority B)")
	require.Equal(t, "device:local:gpu:1", result.FromQueue)
	require.Equal(t, 1, result.Count)

	// Thief's queue now holds the stolen "abc" task.
	stolen, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, stolen)
	stolenEnv, err := queue.UnmarshalEnvelope(stolen.Body)
	require.NoError(t, err)
	require.Equal(t, "abc", stolenEnv.Fingerprint)

	// Donor still has its "xyz" task.
	remaining, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, remaining)
	remainingEnv, err := queue.UnmarshalEnvelope(remaining.Body)
	require.NoError(t, err)
	require.Equal(t, "xyz", remainingEnv.Fingerprint)
}

func TestDispatcher_WorkStealing_RebalanceFromLongest(t *testing.T) {
	// Priority C: thief is idle (no loaded model), both donors have work.
	// Donor with longest queue should have tasks stolen from its tail.
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	dq2 := queue.NewNamed(env.db, "device:local:gpu:2", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1", "gpu:2"}, 1800)

	pool := env.newPool(t)

	// Thief: idle, no loaded model.
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger)

	// Donor 1: 2 tasks (short queue).
	donor1 := makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger)
	donor1.loadedHash = "model-a"

	// Donor 2: 4 tasks (longest queue).
	donor2 := makeDQM("device:local:gpu:2", "local", []string{"gpu:2"}, dq2, pool, env.logger)
	donor2.loadedHash = "model-b"

	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1", "gpu:2"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": thief,
		"device:local:gpu:1": donor1,
		"device:local:gpu:2": donor2,
	})

	// Fill donor1 with 2 tasks.
	for i := range 2 {
		_, err := dq1.SubmitEnvelope(ctx, queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "model-a", RequestID: fmt.Sprintf("d1-%d", i), Payload: []byte("d1"),
		}, queue.PriorityMedium)
		require.NoError(t, err)
	}

	// Fill donor2 with 4 tasks.
	for i := range 4 {
		_, err := dq2.SubmitEnvelope(ctx, queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "model-b", RequestID: fmt.Sprintf("d2-%d", i), Payload: []byte("d2"),
		}, queue.PriorityMedium)
		require.NoError(t, err)
	}

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal from longest queue (priority C)")
	require.Equal(t, "device:local:gpu:2", result.FromQueue, "should steal from donor2 (longest)")
	require.Greater(t, result.Count, 0, "should steal at least one task")

	// Donor2 should still have at least 1 task (rebalance leaves 1 for donor).
	depth2, err := dq2.Depth(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, depth2, 1, "donor should keep at least 1 task")
}

func TestDispatcher_WorkStealing_PriorityOrder(t *testing.T) {
	// When priority A fires, priorities B and C should NOT be attempted.
	// We set up conditions where all three could fire, verify only A's result.
	env := newTestEnv(t)
	ctx := context.Background()
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(env.db, "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger)
	thief.loadedHash = "abc"

	donor := makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger)
	donor.loadedHash = "abc"

	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": thief,
		"device:local:gpu:1": donor,
	})

	// Enqueue "def" on donor — mismatched with donor's "abc" → priority A fires.
	_, err := dq1.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "def", RequestID: "prio-a", Payload: []byte("priority-a-task"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result)
	require.Equal(t, 1, result.Count)

	// The "def" row was moved into the thief's queue transactionally.
	stolen, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, stolen)
	stolenEnv, err := queue.UnmarshalEnvelope(stolen.Body)
	require.NoError(t, err)
	require.Equal(t, "def", stolenEnv.Fingerprint, "priority A should fire first")
}

// --- LoadModel placement priority tests ---

// newSchedulerForLoadTest builds a minimal Scheduler wired for LoadModel testing.
// It sets up the pool, dispatcher, device queues, and worker fleet.
func newSchedulerForLoadTest(
	t *testing.T,
	env *testEnv,
	reg *worker.Fleet,
	deviceQueues map[string]*DeviceQueueManager,
) *Scheduler {
	t.Helper()
	pool := newModelPool(&nilLoader{}, "local", "local", env.logger, 0)
	cfg := &config.Config{}
	o := &Scheduler{
		cfg:        cfg,
		saveFn:     func() {},
		apps:       make(map[string]*ManagedApp),
		appRoutes:  make(map[string]string),
		logger:     env.logger,
		workers:    reg,
		pool:       pool,
		stateStore: env.stateStore,
	}
	o.dispatcher = NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ,
		Results:     env.results,
		StateStore:  env.stateStore,
		BenchStore:  env.benchStore,
		Workers:     reg,
		Pool:        pool,
		Logger:      env.logger,
	})
	for name, dq := range deviceQueues {
		dq.dispatcher = o.dispatcher
		o.dispatcher.Add(name, dq)
	}
	return o
}

func TestLoadModel_PrefersRemoteOverEvictingLocal(t *testing.T) {
	// Scenario: local gpu:0 has model A, local cpu:0 has model B,
	// remote server has a free gpu:0.
	// Loading model C should go to the remote worker, NOT evict a local model.
	env := newTestEnv(t)
	localGpuQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(env.db, "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
		"device:local:cpu:0":  makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, nil, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load models A and B on local devices.
	cfgA := config.LlamaChatConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat(ModeStatic, "direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	cfgB := config.LlamaChatConfig{Path: "/model-B.gguf"}
	fpB := o.pool.RegisterChat(ModeStatic, "direct", "", cfgB, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "cpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	// Load model C — should go to remote, not evict A or B.
	cfgC := config.LlamaChatConfig{Path: "/model-C.gguf"}
	fpC, err := o.LoadModel(cfgC, config.PlacementConfig{}, "direct", ModeStatic)
	require.NoError(t, err)
	require.NotEmpty(t, fpC)

	// Both local models must still be in the pool.
	require.True(t, o.pool.HasChat(fpA), "local model A should not be evicted")
	require.True(t, o.pool.HasChat(fpB), "local model B should not be evicted")
	require.True(t, o.pool.HasChat(fpC), "model C should be loaded")

	// Model C should be on the remote worker.
	info, ok := o.pool.SnapshotInstance(fpC)
	require.True(t, ok)
	require.Equal(t, "server", info.WorkerID, "model C should be placed on remote worker")
}

func TestLoadModel_PrefersLocalFreeDevice(t *testing.T) {
	// Scenario: local gpu:0 has model A, local cpu:0 is free.
	// Loading model B should go to local cpu:0, not remote.
	env := newTestEnv(t)
	localGpuQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(env.db, "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(env.db, "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		workerSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
		"device:local:cpu:0":  makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, nil, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load model A on local gpu:0 only. cpu:0 is free.
	cfgA := config.LlamaChatConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat(ModeStatic, "direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	// Load model B — should go to free local cpu:0, not remote.
	cfgB := config.LlamaChatConfig{Path: "/model-B.gguf"}
	fpB, err := o.LoadModel(cfgB, config.PlacementConfig{}, "direct", ModeStatic)
	require.NoError(t, err)

	info, ok := o.pool.SnapshotInstance(fpB)
	require.True(t, ok)
	require.Equal(t, "local", info.WorkerID, "model B should be placed on local worker")
}

func TestLoadModel_EvictsLocalWhenAllDevicesOccupied(t *testing.T) {
	// Scenario: local gpu:0 has model A, no remote workers.
	// Loading model B should evict A and place B on gpu:0.
	env := newTestEnv(t)
	localGpuQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	reg := newMultiWorkerFleet(t,
		workerSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load model A on local gpu:0.
	cfgA := config.LlamaChatConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat(ModeStatic, "direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	// Load model B — no free devices, no remote workers → must evict A.
	cfgB := config.LlamaChatConfig{Path: "/model-B.gguf"}
	fpB, err := o.LoadModel(cfgB, config.PlacementConfig{}, "direct", ModeStatic)
	require.NoError(t, err)

	require.False(t, o.pool.HasChat(fpA), "model A should be evicted")
	require.True(t, o.pool.HasChat(fpB), "model B should be loaded")
}

// --- Dispatcher drain & device queue draining tests ---

func TestDispatcher_DispatchOne_DrainAll(t *testing.T) {
	// Submit 3 messages to the global queue, run the dispatcher loop via Run
	// with a short-lived context, and verify all 3 end up in the device queue.
	env := newTestEnv(t)
	dq0 := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})
	// Dispatcher is event-driven now: SubmitEnvelope on the global queue
	// signals NotifyCh, so Run wakes immediately. No interval to set.

	// Submit 3 tasks to the global queue.
	for i := 0; i < 3; i++ {
		env.submitTask(t, "fp1", []byte(fmt.Sprintf("task-%d", i)))
	}

	// Run the dispatcher for a short time — enough for one tick to drain all.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	d.Run(ctx)

	// All 3 messages should now be in the device queue.
	for i := 0; i < 3; i++ {
		msg, err := dq0.Receive(context.Background())
		require.NoError(t, err)
		require.NotNil(t, msg, "message %d should be in device queue", i)
	}

	// No more messages.
	msg, err := dq0.Receive(context.Background())
	require.NoError(t, err)
	require.Nil(t, msg, "device queue should be empty after 3 messages")

	// Global queue should also be empty.
	depth, err := env.globalQ.Depth(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, depth, "global queue should be drained")
}

func TestDeviceQueue_DrainQueue_ConcurrentExecution(t *testing.T) {
	// Put 3 same-fingerprint messages in a device queue with maxConcurrent=3.
	// Verify drainQueue processes all 3 concurrently using a barrier channel.
	env := newTestEnv(t)
	ctx := context.Background()
	deviceQ := queue.NewNamed(env.db, "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", workerID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		modelsDir: func() string { return t.TempDir() },
		logger:    env.logger,
		// Pre-set loadedHash so ensureModel short-circuits (no model switch).
		loadedHash:    "fpA",
		maxConcurrent: 3,
		wp:            workerpool.New(3),
		workerDone:    make(chan struct{}, 1),
	}
	t.Cleanup(func() { dq.wp.Close() })

	// Submit 3 messages with the same fingerprint.
	for i := 0; i < 3; i++ {
		e := queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "fpA", RequestID: fmt.Sprintf("req-%d", i),
			Payload: []byte(fmt.Sprintf("task-%d", i)),
		}
		sub, err := deviceQ.SubmitEnvelope(ctx, e, queue.PriorityMedium)
		require.NoError(t, err)
		// Create result entries so executeOne's MarkProcessing/Fail work.
		require.NoError(t, env.results.Create(fmt.Sprintf("req-%d", i), sub.RequestHash))
	}

	// processReady inspects the head, dispatches all matching-fp messages.
	dq.processReady(ctx)
	dq.wp.Wait()

	// Device queue should be empty — all 3 consumed.
	depth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "all 3 messages should be consumed from device queue")
}

func TestDeviceQueue_DrainQueue_DifferentFingerprint(t *testing.T) {
	// Put 2 messages with fp="A" then 1 with fp="B" in device queue.
	// processReady must dispatch the 2 "A" messages, then leave "B" untouched
	// at the head — no Receive, no MaxReceive bump, no visibility timer.
	// (The pool has no model "A" registered, so TryEvict fails and the model
	// switch is deferred — exactly the path that "B sits in line" relies on.)
	env := newTestEnv(t)
	ctx := context.Background()
	deviceQ := queue.NewNamed(env.db, "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", workerID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		modelsDir:     func() string { return t.TempDir() },
		logger:        env.logger,
		loadedHash:    "A",
		maxConcurrent: 2,
		wp:            workerpool.New(2),
		workerDone:    make(chan struct{}, 1),
	}
	t.Cleanup(func() { dq.wp.Close() })

	for i := 0; i < 2; i++ {
		e := queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "A", RequestID: fmt.Sprintf("a-req-%d", i),
			Payload: []byte(fmt.Sprintf("task-a-%d", i)),
		}
		sub, err := deviceQ.SubmitEnvelope(ctx, e, queue.PriorityMedium)
		require.NoError(t, err)
		require.NoError(t, env.results.Create(fmt.Sprintf("a-req-%d", i), sub.RequestHash))
	}
	eB := queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "B", RequestID: "b-req-0",
		Payload: []byte("task-b-0"),
	}
	subB, err := deviceQ.SubmitEnvelope(ctx, eB, queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, env.results.Create("b-req-0", subB.RequestHash))

	dq.processReady(ctx)
	dq.wp.Wait()

	// "B" must still be visible (Peek-only inspection, no Receive happened).
	peeked, err := deviceQ.Peek(ctx, 10)
	require.NoError(t, err)
	require.Len(t, peeked, 1, "B must remain visible — Peek does not consume")
	envB, err := queue.UnmarshalEnvelope(peeked[0].Body)
	require.NoError(t, err)
	require.Equal(t, "B", envB.Fingerprint)
}

func TestDeviceQueue_DrainQueue_ContinuousFeed(t *testing.T) {
	// Put 1 message in queue, start Run. While it's processing, add another
	// message. Verify the second message gets processed without waiting for
	// the next Run tick.
	env := newTestEnv(t)
	ctx := context.Background()
	deviceQ := queue.NewNamed(env.db, "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", workerID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		modelsDir:  func() string { return t.TempDir() },
		logger:     env.logger,
		loadedHash: "fpA", maxConcurrent: 2,
		workerDone: make(chan struct{}, 1),
	}

	// Submit the first message.
	e1 := queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "fpA", RequestID: "req-1",
		Payload: []byte("task-1"),
	}
	sub1, err := deviceQ.SubmitEnvelope(ctx, e1, queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, env.results.Create("req-1", sub1.RequestHash))

	// Start the device queue Run loop in background with a short-lived context.
	runCtx, runCancel := context.WithTimeout(ctx, 1*time.Second)
	defer runCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		dq.Run(runCtx)
	}()

	// Wait a bit for the first message to be picked up, then submit a second.
	time.Sleep(50 * time.Millisecond)

	e2 := queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "fpA", RequestID: "req-2",
		Payload: []byte("task-2"),
	}
	sub2, err := deviceQ.SubmitEnvelope(ctx, e2, queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, env.results.Create("req-2", sub2.RequestHash))

	// Wait for Run to finish (context timeout).
	<-done

	// Both messages should have been processed — queue should be empty.
	depth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "both messages should be processed")
}

func TestDeviceQueue_DrainToGlobal(t *testing.T) {
	// New semantics: each device row is paired with an existing
	// invisible-leased global row (the dispatcher set this up). DrainToGlobal
	// shortens the global lease to zero and deletes the device row, so the
	// global rows reappear at their original positions for re-dispatch — no
	// fresh `created` timestamp, no leak window.
	env := newTestEnv(t)
	ctx := context.Background()
	// Same DB for both queues so the transactional Extend+Delete spans a
	// single SQL transaction.
	globalQ := queue.NewNamed(env.db, "global", 3, 30*time.Second)
	deviceQ := queue.NewNamed(env.db, "device:local:gpu:0", 1, 30*time.Second)

	dq := &DeviceQueueManager{
		queueName:   "device:local:gpu:0",
		workerID:    "local",
		deviceIDs:   []string{"gpu:0"},
		queue:       deviceQ,
		globalQueue: globalQ,
		stateStore:  noopStateStore{},
		logger:      env.logger,
	}

	// Simulate the dispatcher's hand-off: for each task, submit to global,
	// receive (making it invisible-leased), then create a paired device row
	// stamped with the global ID.
	for i := 0; i < 3; i++ {
		e := queue.Envelope{
			Type:        queue.RequestTypeChatCompletion,
			Priority:    queue.PriorityMedium,
			Source:      "direct",
			Fingerprint: "model-a",
			RequestID:   fmt.Sprintf("req-%d", i),
			Payload:     []byte(fmt.Sprintf("task-%d", i)),
		}
		gsub, err := globalQ.SubmitEnvelope(ctx, e, queue.PriorityMedium)
		require.NoError(t, err)
		gmsg, err := globalQ.Receive(ctx)
		require.NoError(t, err)
		require.NotNil(t, gmsg)
		require.Equal(t, queue.MessageID(gsub.ID), gmsg.ID)

		e.GlobalMsgID = gsub.ID
		_, err = deviceQ.SubmitEnvelope(ctx, e, queue.PriorityMedium)
		require.NoError(t, err)
	}

	// Sanity: device queue has 3 visible rows; global has 0 visible (all leased).
	devDepth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, devDepth)
	globalDepth, err := globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, globalDepth, "global rows are leased, not visible")

	drained, err := dq.DrainToGlobal(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, drained)

	// Device queue is empty.
	devDepth, err = deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, devDepth)

	// Global rows are now visible (released) — same 3 logical tasks.
	globalDepth, err = globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, globalDepth, "released global rows must be visible to dispatcher")
}

func TestEnvelope_MarshalUnmarshal_Roundtrip(t *testing.T) {
	tests := []struct {
		name string
		env  queue.Envelope
	}{
		{
			name: "full envelope",
			env: queue.Envelope{
				Type: queue.RequestTypeChatCompletion, Priority: queue.PriorityCritical,
				Retries: 3, Source: "app:playground", Fingerprint: "abc123",
				RequestID: "req-42", Payload: []byte("hello"),
			},
		},
		{
			name: "empty optional fields",
			env: queue.Envelope{
				Type: queue.RequestTypeEmbedding, Priority: queue.PriorityLow,
				Source: "direct", Payload: []byte("data"),
			},
		},
		{
			name: "max retries",
			env: queue.Envelope{
				Type: queue.RequestTypeChatCompletion, Retries: 255,
				Source: "direct", Payload: []byte("x"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.env.Marshal()
			got, err := queue.UnmarshalEnvelope(data)
			require.NoError(t, err)
			require.Equal(t, tt.env.Type, got.Type)
			require.Equal(t, tt.env.Priority, got.Priority)
			require.Equal(t, tt.env.Retries, got.Retries)
			require.Equal(t, tt.env.Source, got.Source)
			require.Equal(t, tt.env.Fingerprint, got.Fingerprint)
			require.Equal(t, tt.env.RequestID, got.RequestID)
			require.Equal(t, tt.env.Payload, got.Payload)
		})
	}
}
