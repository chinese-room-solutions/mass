package scheduler

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/agent"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
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
		logger:     logger,
	}
}

func (e *testEnv) submitTask(t *testing.T, fingerprint string, payload []byte) string {
	t.Helper()
	sub, err := e.globalQ.SubmitRaw(context.Background(), queue.RequestTypeChatCompletion, payload, "direct", fingerprint, queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, e.results.Create(sub.ID, sub.RequestHash))
	return sub.ID
}

func (e *testEnv) registerDevices(t *testing.T, agentID string, deviceIDs []string, gflops float64) {
	t.Helper()
	for _, did := range deviceIDs {
		require.NoError(t, e.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
			QueueName: DeviceQueueName(agentID, did),
			AgentID:   agentID,
			DeviceIDs: []string{did},
		}))
		require.NoError(t, e.benchStore.SaveBenchmark(store.BenchmarkRow{
			AgentID: agentID, DeviceID: did,
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
	reg := newTestRegistry(t, deviceIDs)
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: e.globalQ,
		Pool:        pool,
		Agents:      reg,
		Results:     e.results,
		StateStore:  e.stateStore,
		BenchStore:  e.benchStore,
		Logger:      e.logger,
	})
	d.deviceQueues = deviceQueues
	return d
}

// --- stubs ---

// nilLoader satisfies ModelLoaderInterface but never loads anything.
type nilLoader struct{}

func (n *nilLoader) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfig, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return nil, nil
}

func (n *nilLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfig, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return nil, nil
}

// dispatchTestAgent satisfies agent.AgentInterface for dispatch tests.
type dispatchTestAgent struct {
	id      string
	online  bool
	devices []bench.Device
}

func (a *dispatchTestAgent) ID() string                 { return a.id }
func (a *dispatchTestAgent) Name() string               { return a.id }
func (a *dispatchTestAgent) Stats() []bench.DeviceStats { return nil }
func (a *dispatchTestAgent) Devices() []bench.Device    { return a.devices }

func (a *dispatchTestAgent) Status() agent.AgentStatus {
	return agent.AgentStatus{Online: a.online}
}

func (a *dispatchTestAgent) Bench(_ string) (bench.Result, error) { return bench.Result{}, nil }

func (a *dispatchTestAgent) LoadChatModel(_ zerolog.Logger, _ string, _ llm.ChatModelConfig, _ llm.PlacementConfig) (llm.ChatModelInterface, error) {
	return &stubChatModel{}, nil
}

func (a *dispatchTestAgent) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llm.EmbeddingModelConfig, _ llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return &stubEmbeddingModel{}, nil
}

// Compile-time check: dispatchTestAgent satisfies AgentInterface.
var _ agent.AgentInterface = (*dispatchTestAgent)(nil)

// agentSpec describes an agent and its devices for test setup.
type agentSpec struct {
	id        string
	online    bool
	deviceIDs []string
	memoryMB  int // per device, default 8000
}

func newTestRegistry(t *testing.T, deviceIDs []string) *agent.Registry {
	t.Helper()
	return newMultiAgentRegistry(t, agentSpec{id: "local", online: true, deviceIDs: deviceIDs, memoryMB: 8000})
}

func newMultiAgentRegistry(t *testing.T, agents ...agentSpec) *agent.Registry {
	t.Helper()
	reg := agent.NewRegistry()
	for _, a := range agents {
		memMB := a.memoryMB
		if memMB == 0 {
			memMB = 8000
		}
		var devs []bench.Device
		for _, d := range a.deviceIDs {
			devs = append(devs, bench.Device{ID: d, Type: "GPU", TotalMemoryMB: memMB})
		}
		require.NoError(t, reg.Register(&dispatchTestAgent{id: a.id, online: a.online, devices: devs}))
	}
	return reg
}

func makeDQM(name, agentID string, deviceIDs []string, q queue.QueueInterface, pool *modelPool, logger zerolog.Logger) *DeviceQueueManager {
	return &DeviceQueueManager{
		queueName: name, agentID: agentID, deviceIDs: deviceIDs,
		queue: q, pool: pool, logger: logger, maxConcurrent: 1,
	}
}

// registerTestModel loads a stub model into the pool on a specific agent/device.
// Returns the computed fingerprint.
func registerTestModel(t *testing.T, pool *modelPool, path string, deviceIDs ...string) string {
	t.Helper()
	cfg := config.ChatModelConfig{Path: path}
	return pool.RegisterChat("direct", "", cfg, config.PlacementConfig{}, &stubChatModel{}, "local", "local", deviceIDs...)
}

// --- tests ---

func TestDispatcher_AffinityRouting(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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

func TestDispatcher_PreferLongestTailForContextSwitch(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	// gpu:0 has long sequence (tail=10), gpu:1 has short (tail=2).
	// Both running model "xyz", new task needs model "new".
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "xyz", 10))
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:1", "xyz", 2))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// "new" model should go to gpu:0 (longest tail) — the context switch is deferred
	// furthest into the future (after 10 "xyz" tasks drain), while gpu:1's shorter
	// sequence (2 tasks) is left undisturbed.
	env.submitTask(t, "new", []byte("task-new"))
	d.dispatchOne(ctx)

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg0, "task should route to gpu:0 (longest tail, switch deferred)")

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg1, "gpu:1 (shorter tail) should be left undisturbed")
}

func TestDispatcher_GFlops_Tiebreak(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)

	// gpu:0 = 1000 GFlops, gpu:1 = 2000 GFlops.
	env.registerDevices(t, "local", []string{"gpu:0"}, 1000)
	require.NoError(t, env.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
		QueueName: "device:local:gpu:1", AgentID: "local", DeviceIDs: []string{"gpu:1"},
	}))
	require.NoError(t, env.benchStore.SaveBenchmark(store.BenchmarkRow{
		AgentID: "local", DeviceID: "gpu:1",
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	// Submit low, then critical.
	_, err = env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("low-task"), "direct", "fp1", queue.PriorityLow)
	require.NoError(t, err)
	_, err = env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("critical-task"), "direct", "fp1", queue.PriorityCritical)
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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
	_, err = dq1.SubmitEnvelope(ctx, donorEnv, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal task that would cause context switch on donor")
	require.Equal(t, "device:local:gpu:1", result.FromQueue)
	require.Len(t, result.Messages, 1)
}

func TestDispatcher_MultipleTasksSameFingerprint_BuildSequence(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
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

func TestDispatcher_MultiAgent_AffinityAcrossAgents(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)

	// Local gpu has model "abc", remote gpu has model "def".
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 1800)
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "abc", 3))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "abc"))
	require.NoError(t, env.stateStore.UpdateTail("device:server:gpu:0", "def", 3))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:server:gpu:0", "def"))

	pool := env.newPool(t)
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ, pool, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger),
	}

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

func TestDispatcher_MultiAgent_StrongerDeviceWins(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	weakQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	strongQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)

	// Local = 500 GFlops (weak), Server = 5000 GFlops (strong). No affinity.
	env.registerDevices(t, "local", []string{"gpu:0"}, 500)
	env.registerDevices(t, "server", []string{"gpu:0"}, 5000)

	pool := env.newPool(t)
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, weakQ, pool, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, strongQ, pool, env.logger),
	}

	env.submitTask(t, "new-model", []byte("power-task"))
	d.dispatchOne(ctx)

	weakMsg, err := weakQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, weakMsg, "weak local device should not get the task")

	strongMsg, err := strongQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, strongMsg, "strong server device should get the task")
}

func TestDispatcher_OfflineAgentSkipped(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	offlineQ := queue.NewNamed(s2.DB(), "device:offline-agent:gpu:0", 1, 30*time.Second)
	onlineQ := queue.NewNamed(s2.DB(), "device:online-agent:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "offline-agent", []string{"gpu:0"}, 5000) // stronger but offline
	env.registerDevices(t, "online-agent", []string{"gpu:0"}, 1000)  // weaker but online

	pool := env.newPool(t)
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "offline-agent", online: false, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "online-agent", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:offline-agent:gpu:0": makeDQM("device:offline-agent:gpu:0", "offline-agent", []string{"gpu:0"}, offlineQ, pool, env.logger),
		"device:online-agent:gpu:0":  makeDQM("device:online-agent:gpu:0", "online-agent", []string{"gpu:0"}, onlineQ, pool, env.logger),
	}

	env.submitTask(t, "any-model", []byte("online-only"))
	d.dispatchOne(ctx)

	offMsg, err := offlineQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, offMsg, "offline agent should never receive tasks")

	onMsg, err := onlineQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, onMsg, "online agent should receive the task")
}

func TestDispatcher_DisabledQueueSkipped(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	disabledQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	enabledQ := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0"}, 5000) // stronger but disabled
	env.registerDevices(t, "local", []string{"gpu:1"}, 1000)

	// Disable gpu:0.
	require.NoError(t, env.stateStore.SetEnabled("device:local:gpu:0", false))

	pool := env.newPool(t)
	reg := newTestRegistry(t, []string{"gpu:0", "gpu:1"})

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, disabledQ, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, enabledQ, pool, env.logger),
	}

	env.submitTask(t, "any-model", []byte("enabled-only"))
	d.dispatchOne(ctx)

	disMsg, err := disabledQ.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, disMsg, "disabled queue should never receive tasks")

	enMsg, err := enabledQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, enMsg, "enabled queue should receive the task")
}

func TestDispatcher_EvictSkipsDisabledDevice(t *testing.T) {
	env := newTestEnv(t)

	env.registerDevices(t, "local", []string{"gpu:0"}, 2000)
	env.registerDevices(t, "local", []string{"gpu:1"}, 1000)

	// Disable gpu:0.
	require.NoError(t, env.stateStore.SetEnabled("device:local:gpu:0", false))

	pool := env.newPool(t)
	reg := newTestRegistry(t, []string{"gpu:0", "gpu:1"})

	// Load a model on the disabled gpu:0.
	registerTestModel(t, pool, "/models/old.gguf", "gpu:0")

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, env.globalQ, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, env.globalQ, pool, env.logger),
	}

	// Try to evict for a new model — should NOT evict from disabled gpu:0.
	evicted := d.evictIdleModels("new-model-fp")
	require.False(t, evicted, "should not evict from disabled device")

	// The old model should still be in the pool.
	snap := pool.Snapshot()
	require.Len(t, snap, 1, "model on disabled device should be preserved")
}

func TestDispatcher_CrossAgentWorkStealing(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	// Local thief has model "abc" loaded, is idle.
	thief := makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ, pool, env.logger)
	thief.loadedHash = "abc"

	// Remote donor has model "abc" loaded, but queued a "xyz" task (context switch).
	donor := makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger)
	donor.loadedHash = "abc"

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0":  thief,
		"device:server:gpu:0": donor,
	}

	// Enqueue "xyz" on remote donor.
	_, err = remoteQ.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "xyz", RequestID: "cross-1", Payload: []byte("steal-cross-agent"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal across agents")
	require.Equal(t, "device:server:gpu:0", result.FromQueue)
}

func TestDispatcher_MultiAgent_MixedLoad(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	// 3 agents: local (2 GPUs), server1 (1 GPU), server2 (1 GPU, offline).
	localQ0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	localQ1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
	srv1Q := queue.NewNamed(s2.DB(), "device:server1:gpu:0", 1, 30*time.Second)
	srv2Q := queue.NewNamed(s2.DB(), "device:server2:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)
	env.registerDevices(t, "server1", []string{"gpu:0"}, 3000)
	env.registerDevices(t, "server2", []string{"gpu:0"}, 5000) // strongest but offline

	// local:gpu:0 has model "alpha" loaded.
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "alpha", 2))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", "alpha"))

	pool := env.newPool(t)
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "gpu:1"}},
		agentSpec{id: "server1", online: true, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "server2", online: false, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0":   makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localQ0, pool, env.logger),
		"device:local:gpu:1":   makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, localQ1, pool, env.logger),
		"device:server1:gpu:0": makeDQM("device:server1:gpu:0", "server1", []string{"gpu:0"}, srv1Q, pool, env.logger),
		"device:server2:gpu:0": makeDQM("device:server2:gpu:0", "server2", []string{"gpu:0"}, srv2Q, pool, env.logger),
	}

	// Task 1: "alpha" → should go to local:gpu:0 (affinity).
	env.submitTask(t, "alpha", []byte("affinity-hit"))
	d.dispatchOne(ctx)

	msg, err := localQ0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg, "alpha task should route to local:gpu:0 via affinity")

	// Task 2: "beta" (no affinity) → should go to local:gpu:0 (longest tail=2,
	// context switch deferred furthest). Among devices with tail=0 (local:gpu:1,
	// server1:gpu:0), GFlops tiebreaker applies, but local:gpu:0's longer tail wins.
	env.submitTask(t, "beta", []byte("power-pick"))
	d.dispatchOne(ctx)

	srv2Msg, err := srv2Q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, srv2Msg, "offline server2 must not receive tasks")

	local0Msg, err := localQ0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, local0Msg, "beta task should route to local:gpu:0 (longest tail, switch deferred)")
}

// --- Edge case tests ---

func TestDispatcher_PriorityPreservedAcrossHops(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})

	// Submit a critical task.
	_, err = env.globalQ.SubmitRaw(ctx, queue.RequestTypeChatCompletion, []byte("critical-payload"), "direct", "fp1", queue.PriorityCritical)
	require.NoError(t, err)

	d.dispatchOne(ctx)

	msg, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	dispatched, err := queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, queue.PriorityCritical, dispatched.Priority, "priority must survive global→device queue hop")
}

func TestDispatcher_MaxRetriesDropsTask(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// No devices registered → selectPlacement always returns nil → triggers requeue.
	pool := env.newPool(t)
	reg := agent.NewRegistry() // empty — no agents
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{}

	originalID := env.submitTask(t, "unreachable", []byte("doomed"))

	// Dispatch MaxRetries+1 times — task should be re-queued each time until dropped.
	for i := 0; i <= queue.MaxRetries; i++ {
		d.dispatchOne(ctx)
	}

	// Global queue should be empty (task dropped, not stuck looping).
	depth, err := env.globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "task should be dropped after max retries")

	// Result should be marked as failed.
	result, err := env.results.Get(originalID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, result.Status)
	require.Contains(t, result.Error, "max retries")
}

func TestDispatcher_AllDevicesSameFingerprint(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)

	// Both GPUs have same model, but gpu:1 is stronger.
	env.registerDevices(t, "local", []string{"gpu:0"}, 1000)
	require.NoError(t, env.benchStore.SaveBenchmark(store.BenchmarkRow{
		AgentID: "local", DeviceID: "gpu:1", ComputeGFlops: 2000, MemoryGBs: 800,
	}))
	require.NoError(t, env.stateStore.UpsertDeviceQueueState(store.DeviceQueueState{
		QueueName: "device:local:gpu:1", AgentID: "local", DeviceIDs: []string{"gpu:1"},
	}))

	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:0", "same-model", 5))
	require.NoError(t, env.stateStore.UpdateTail("device:local:gpu:1", "same-model", 3))

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0", "gpu:1"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, dq1, pool, env.logger),
	})

	// Task matches both — should pick the one with longer tail (gpu:0, tail=5 > gpu:1 tail=3),
	// since affinity score = weight * (tailLength+1).
	env.submitTask(t, "same-model", []byte("pick-longer"))
	d.dispatchOne(ctx)

	msg0, err := dq0.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg0, "should prefer gpu:0 (longer affinity sequence)")

	msg1, err := dq1.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, msg1)
}

func TestDispatcher_AllAgentsOffline_RequeuesWithRetry(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// All agents offline.
	reg := newMultiAgentRegistry(t,
		agentSpec{id: "agent1", online: false, deviceIDs: []string{"gpu:0"}},
		agentSpec{id: "agent2", online: false, deviceIDs: []string{"gpu:0"}},
	)
	env.registerDevices(t, "agent1", []string{"gpu:0"}, 1800)
	env.registerDevices(t, "agent2", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg, Results: env.results,
		StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{}

	originalID := env.submitTask(t, "any", []byte("waiting"))

	// First dispatch — should requeue with retries=1.
	d.dispatchOne(ctx)

	// Task should still be in global queue.
	depth, err := env.globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, depth, "task should be re-queued")

	// Verify it's not marked as failed yet.
	result, err := env.results.Get(originalID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, result.Status)
}

func TestDispatcher_LoadedModelPrefersOverCold(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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

func TestDispatcher_EvictOnlyFromTargetDevice(t *testing.T) {
	// Scenario: gpu:0 has model "A", cpu:0 has model "B".
	// Loading model "C" should evict from one device, not both.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	gpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	cpuQ := queue.NewNamed(s2.DB(), "device:local:cpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)

	pool := env.newPool(t)

	// Register models on specific devices.
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	fpB := registerTestModel(t, pool, "/model-B.gguf", "cpu:0")

	// Mark them as loaded in device queue state.
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: newTestRegistry(t, []string{"gpu:0", "cpu:0"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, gpuQ, pool, env.logger),
		"device:local:cpu:0": makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, cpuQ, pool, env.logger),
	}

	// Evict for a new model "C".
	evicted := d.evictIdleModels("fpC")
	require.True(t, evicted, "should evict one model")

	// Only one model should remain in pool.
	snapshot := pool.Snapshot()
	require.Len(t, snapshot, 1, "eviction should remove exactly one model, not both")
}

func TestDispatcher_EvictDoesNotCrossDevices(t *testing.T) {
	// Scenario: gpu:0 has model "A", cpu:0 has model "B".
	// Evicting from device:local:cpu:0 should NOT evict model "A" from gpu:0.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	gpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	cpuQ := queue.NewNamed(s2.DB(), "device:local:cpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	fpB := registerTestModel(t, pool, "/model-B.gguf", "cpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: newTestRegistry(t, []string{"gpu:0", "cpu:0"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, gpuQ, pool, env.logger),
		"device:local:cpu:0": makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, cpuQ, pool, env.logger),
	}

	d.evictIdleModels("fpC")

	// Model A on gpu:0 should survive if cpu:0 was evicted first (iteration order).
	// Or model B on cpu:0 survives if gpu:0 was evicted first.
	// Either way, exactly one model survives.
	snapshot := pool.Snapshot()
	require.Len(t, snapshot, 1, "exactly one model should survive")

	// The surviving model should still be on its original device.
	surviving := snapshot[0]
	if surviving.Fingerprint == fpA {
		require.True(t, pool.HasChat(fpA), "model A should still be in pool")
		require.False(t, pool.HasChat(fpB), "model B should be evicted")
	} else {
		require.True(t, pool.HasChat(fpB), "model B should still be in pool")
		require.False(t, pool.HasChat(fpA), "model A should be evicted")
	}
}

func TestDispatcher_AvailablePlacement_SkipsOccupiedDevices(t *testing.T) {
	// gpu:0 has model "A" loaded, gpu:1 is empty.
	// selectAvailablePlacement for "B" should pick gpu:1, not gpu:0.
	env := newTestEnv(t)
	env.registerDevices(t, "local", []string{"gpu:0", "gpu:1"}, 1800)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: newTestRegistry(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger),
	}

	fpB := config.ChatModelFingerprint(config.ChatModelConfig{Path: "/model-B.gguf"})
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
		GlobalQueue: env.globalQ, Pool: pool, Agents: newTestRegistry(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger),
	}

	fpC := config.ChatModelFingerprint(config.ChatModelConfig{Path: "/model-C.gguf"})
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
		GlobalQueue: env.globalQ, Pool: pool, Agents: newTestRegistry(t, []string{"gpu:0", "gpu:1"}),
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, nil, pool, env.logger),
		"device:local:gpu:1": makeDQM("device:local:gpu:1", "local", []string{"gpu:1"}, nil, pool, env.logger),
	}

	// Same fingerprint as A — should pick gpu:0 (already loaded there).
	candidate := d.selectAvailablePlacement(fpA)
	require.NotNil(t, candidate)
	require.Equal(t, "device:local:gpu:0", candidate.QueueName, "should reuse gpu:0 where model A is loaded")
}

func TestDispatcher_EvictThenPlace_MultiDevice(t *testing.T) {
	// 3 devices: gpu:0 has "A", cpu:0 has "B", remote gpu:0 is empty.
	// Loading "C" should find remote:gpu:0 (empty) without evicting anything.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localGpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(s2.DB(), "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	pool := env.newPool(t)
	fpA := registerTestModel(t, pool, "/model-A.gguf", "gpu:0")
	fpB := registerTestModel(t, pool, "/model-B.gguf", "cpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	d := NewDispatcher(DispatcherOpts{
		GlobalQueue: env.globalQ, Pool: pool, Agents: reg,
		Results: env.results, StateStore: env.stateStore, BenchStore: env.benchStore, Logger: env.logger,
	})
	d.deviceQueues = map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, pool, env.logger),
		"device:local:cpu:0":  makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, pool, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, pool, env.logger),
	}

	fpC := config.ChatModelFingerprint(config.ChatModelConfig{Path: "/model-C.gguf"})
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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
	_, err = dq1.SubmitEnvelope(ctx, queue.Envelope{
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
	require.Len(t, result.Messages, 1)

	// Verify it stole the "abc" task.
	stolenEnv, err := queue.UnmarshalEnvelope(result.Messages[0].Body)
	require.NoError(t, err)
	require.Equal(t, "abc", stolenEnv.Fingerprint)

	// Donor should still have its "xyz" task.
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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
	dq2 := queue.NewNamed(s2.DB(), "device:local:gpu:2", 1, 30*time.Second)
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
		_, err = dq1.SubmitEnvelope(ctx, queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "model-a", RequestID: fmt.Sprintf("d1-%d", i), Payload: []byte("d1"),
		}, queue.PriorityMedium)
		require.NoError(t, err)
	}

	// Fill donor2 with 4 tasks.
	for i := range 4 {
		_, err = dq2.SubmitEnvelope(ctx, queue.Envelope{
			Type: queue.RequestTypeChatCompletion, Source: "direct",
			Fingerprint: "model-b", RequestID: fmt.Sprintf("d2-%d", i), Payload: []byte("d2"),
		}, queue.PriorityMedium)
		require.NoError(t, err)
	}

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result, "should steal from longest queue (priority C)")
	require.Equal(t, "device:local:gpu:2", result.FromQueue, "should steal from donor2 (longest)")
	require.Greater(t, len(result.Messages), 0, "should steal at least one task")

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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	dq1 := queue.NewNamed(s2.DB(), "device:local:gpu:1", 1, 30*time.Second)
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
	_, err = dq1.SubmitEnvelope(ctx, queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "def", RequestID: "prio-a", Payload: []byte("priority-a-task"),
	}, queue.PriorityMedium)
	require.NoError(t, err)

	result := d.TrySteal(ctx, thief, env.logger)
	require.NotNil(t, result)

	// The stolen task should be the "def" from priority A.
	stolenEnv, err := queue.UnmarshalEnvelope(result.Messages[0].Body)
	require.NoError(t, err)
	require.Equal(t, "def", stolenEnv.Fingerprint, "priority A should fire first")
}

// --- LoadModel placement priority tests ---

// newSchedulerForLoadTest builds a minimal Scheduler wired for LoadModel testing.
// It sets up the pool, dispatcher, device queues, and agent registry.
func newSchedulerForLoadTest(
	t *testing.T,
	env *testEnv,
	reg *agent.Registry,
	deviceQueues map[string]*DeviceQueueManager,
) *Scheduler {
	t.Helper()
	pool := newModelPool(&nilLoader{}, "local", "local", env.logger, 0)
	cfg := &config.Config{}
	o := &Scheduler{
		cfg:          cfg,
		saveFn:       func() {},
		modules:      make(map[string]*ManagedModule),
		moduleRoutes: make(map[string]string),
		logger:       env.logger,
		agents:       reg,
		pool:         pool,
		deviceQueues: deviceQueues,
		stateStore:   env.stateStore,
	}
	o.dispatcher = NewDispatcher(DispatcherOpts{
		GlobalQueue:  env.globalQ,
		DeviceQueues: deviceQueues,
		Results:      env.results,
		StateStore:   env.stateStore,
		BenchStore:   env.benchStore,
		Agents:       reg,
		Pool:         pool,
		Logger:       env.logger,
	})
	return o
}

func TestLoadModel_PrefersRemoteOverEvictingLocal(t *testing.T) {
	// Scenario: local gpu:0 has model A, local cpu:0 has model B,
	// remote server has a free gpu:0.
	// Loading model C should go to the remote agent, NOT evict a local model.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localGpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(s2.DB(), "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
		"device:local:cpu:0":  makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, nil, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load models A and B on local devices.
	cfgA := config.ChatModelConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat("direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	cfgB := config.ChatModelConfig{Path: "/model-B.gguf"}
	fpB := o.pool.RegisterChat("direct", "", cfgB, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "cpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:cpu:0", fpB))

	// Load model C — should go to remote, not evict A or B.
	cfgC := config.ChatModelConfig{Path: "/model-C.gguf"}
	fpC, err := o.LoadModel("chat", &cfgC, nil, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	require.NotEmpty(t, fpC)

	// Both local models must still be in the pool.
	require.True(t, o.pool.HasChat(fpA), "local model A should not be evicted")
	require.True(t, o.pool.HasChat(fpB), "local model B should not be evicted")
	require.True(t, o.pool.HasChat(fpC), "model C should be loaded")

	// Model C should be on the remote agent.
	info, ok := o.pool.SnapshotInstance(fpC)
	require.True(t, ok)
	require.Equal(t, "server", info.AgentID, "model C should be placed on remote agent")
}

func TestLoadModel_PrefersLocalFreeDevice(t *testing.T) {
	// Scenario: local gpu:0 has model A, local cpu:0 is free.
	// Loading model B should go to local cpu:0, not remote.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localGpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	localCpuQ := queue.NewNamed(s2.DB(), "device:local:cpu:0", 1, 30*time.Second)
	remoteQ := queue.NewNamed(s2.DB(), "device:server:gpu:0", 1, 30*time.Second)

	env.registerDevices(t, "local", []string{"gpu:0", "cpu:0"}, 1800)
	env.registerDevices(t, "server", []string{"gpu:0"}, 3000)

	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0", "cpu:0"}},
		agentSpec{id: "server", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0":  makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
		"device:local:cpu:0":  makeDQM("device:local:cpu:0", "local", []string{"cpu:0"}, localCpuQ, nil, env.logger),
		"device:server:gpu:0": makeDQM("device:server:gpu:0", "server", []string{"gpu:0"}, remoteQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load model A on local gpu:0 only. cpu:0 is free.
	cfgA := config.ChatModelConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat("direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	// Load model B — should go to free local cpu:0, not remote.
	cfgB := config.ChatModelConfig{Path: "/model-B.gguf"}
	fpB, err := o.LoadModel("chat", &cfgB, nil, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	info, ok := o.pool.SnapshotInstance(fpB)
	require.True(t, ok)
	require.Equal(t, "local", info.AgentID, "model B should be placed on local agent")
}

func TestLoadModel_EvictsLocalWhenAllDevicesOccupied(t *testing.T) {
	// Scenario: local gpu:0 has model A, no remote agents.
	// Loading model B should evict A and place B on gpu:0.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	localGpuQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	reg := newMultiAgentRegistry(t,
		agentSpec{id: "local", online: true, deviceIDs: []string{"gpu:0"}},
	)

	dqMap := map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, localGpuQ, nil, env.logger),
	}

	o := newSchedulerForLoadTest(t, env, reg, dqMap)

	// Pre-load model A on local gpu:0.
	cfgA := config.ChatModelConfig{Path: "/model-A.gguf"}
	fpA := o.pool.RegisterChat("direct", "", cfgA, config.PlacementConfig{}, &stubChatModel{}, "local", "local", "gpu:0")
	require.NoError(t, env.stateStore.UpdateLoadedHash("device:local:gpu:0", fpA))

	// Load model B — no free devices, no remote agents → must evict A.
	cfgB := config.ChatModelConfig{Path: "/model-B.gguf"}
	fpB, err := o.LoadModel("chat", &cfgB, nil, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	require.False(t, o.pool.HasChat(fpA), "model A should be evicted")
	require.True(t, o.pool.HasChat(fpB), "model B should be loaded")
}

// --- Dispatcher drain & device queue draining tests ---

func TestDispatcher_DispatchOne_DrainAll(t *testing.T) {
	// Submit 3 messages to the global queue, run the dispatcher loop via Run
	// with a short-lived context, and verify all 3 end up in the device queue.
	env := newTestEnv(t)
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	dq0 := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)
	env.registerDevices(t, "local", []string{"gpu:0"}, 1800)

	pool := env.newPool(t)
	d := env.newDispatcher(t, []string{"gpu:0"}, map[string]*DeviceQueueManager{
		"device:local:gpu:0": makeDQM("device:local:gpu:0", "local", []string{"gpu:0"}, dq0, pool, env.logger),
	})
	// Use a very short poll interval so Run fires quickly.
	d.pollInterval = 10 * time.Millisecond

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
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	deviceQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", agentID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		benchStore: env.benchStore, modelsDir: func() string { return t.TempDir() },
		logger: env.logger,
		// Pre-set loadedHash so ensureModel short-circuits (no model switch).
		loadedHash:    "fpA",
		maxConcurrent: 3,
		wp:            workerpool.New(3),
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

	// Run drainQueue — all 3 should be processed (execution will fail since
	// no real model, but the messages will be consumed from the queue).
	dq.drainQueue(ctx)

	// Device queue should be empty — all 3 consumed.
	depth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth, "all 3 messages should be consumed from device queue")
}

func TestDeviceQueue_DrainQueue_DifferentFingerprint(t *testing.T) {
	// Put 2 messages with fp="A" then 1 with fp="B" in device queue.
	// Verify drainQueue processes the 2 "A" messages and requeues the "B" message.
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	deviceQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", agentID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		benchStore: env.benchStore, modelsDir: func() string { return t.TempDir() },
		logger: env.logger,
		// Pre-set loadedHash to "A" so ensureModel short-circuits for "A" messages.
		loadedHash:    "A",
		maxConcurrent: 2,
		wp:            workerpool.New(2),
	}
	t.Cleanup(func() { dq.wp.Close() })

	// Submit 2 "A" messages.
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

	// Submit 1 "B" message.
	eB := queue.Envelope{
		Type: queue.RequestTypeChatCompletion, Source: "direct",
		Fingerprint: "B", RequestID: "b-req-0",
		Payload: []byte("task-b-0"),
	}
	subB, err := deviceQ.SubmitEnvelope(ctx, eB, queue.PriorityMedium)
	require.NoError(t, err)
	require.NoError(t, env.results.Create("b-req-0", subB.RequestHash))

	// Run drainQueue — should process 2 "A" messages and requeue "B".
	dq.drainQueue(ctx)

	// The "B" message should still be in the queue (requeued).
	depth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, depth, "B message should be requeued, remaining in device queue")

	// Verify the remaining message has fingerprint "B".
	msg, err := deviceQ.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	remaining, err := queue.UnmarshalEnvelope(msg.Body)
	require.NoError(t, err)
	require.Equal(t, "B", remaining.Fingerprint, "remaining message should be the 'B' fingerprint")
}

func TestDeviceQueue_DrainQueue_ContinuousFeed(t *testing.T) {
	// Put 1 message in queue, start Run. While it's processing, add another
	// message. Verify the second message gets processed without waiting for
	// the next Run tick.
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	deviceQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 3, 30*time.Second)
	pool := env.newPool(t)

	dq := &DeviceQueueManager{
		queueName: "device:local:gpu:0", agentID: "local", deviceIDs: []string{"gpu:0"},
		queue: deviceQ, pool: pool, results: env.results, stateStore: env.stateStore,
		benchStore: env.benchStore, modelsDir: func() string { return t.TempDir() },
		logger:     env.logger,
		loadedHash: "fpA", maxConcurrent: 2,
		pollInterval: 10 * time.Millisecond,
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
	env := newTestEnv(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dq.db")
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	deviceQ := queue.NewNamed(s2.DB(), "device:local:gpu:0", 1, 30*time.Second)

	dq := &DeviceQueueManager{
		queueName:   "device:local:gpu:0",
		agentID:     "local",
		deviceIDs:   []string{"gpu:0"},
		queue:       deviceQ,
		globalQueue: env.globalQ,
		logger:      env.logger,
	}

	// Submit 3 tasks directly to the device queue.
	for i := 0; i < 3; i++ {
		env := queue.Envelope{
			Type:        queue.RequestTypeChatCompletion,
			Priority:    queue.PriorityMedium,
			Source:      "direct",
			Fingerprint: "model-a",
			Payload:     []byte(fmt.Sprintf("task-%d", i)),
		}
		_, err := deviceQ.SubmitEnvelope(ctx, env, queue.PriorityMedium)
		require.NoError(t, err)
	}

	// Verify device queue has 3 tasks.
	depth, err := deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, depth)

	// Drain to global.
	drained, err := dq.DrainToGlobal(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, drained)

	// Device queue should be empty.
	depth, err = deviceQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, depth)

	// Global queue should have 3 tasks.
	globalDepth, err := env.globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, globalDepth)
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
				Retries: 3, Source: "module:playground", Fingerprint: "abc123",
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
