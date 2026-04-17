package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	massapp "github.com/chinese-room-solutions/mass-sdk/app"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	llmpkg "github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubWorker implements worker.WorkerInterface for testing.
type stubWorker struct{}

func (a *stubWorker) ID() string                         { return "local" }
func (a *stubWorker) Name() string                       { return "Local" }
func (a *stubWorker) Status() worker.WorkerStatus        { return worker.WorkerStatus{Online: true} }
func (a *stubWorker) Devices() []stats.Device            { return nil }
func (a *stubWorker) Bench(string) (bench.Result, error) { return bench.Result{}, nil }
func (a *stubWorker) LoadChatModel(_ zerolog.Logger, _ string, _ llmpkg.ChatModelConfigInterface, _ llmpkg.PlacementConfig) (llmpkg.ChatModelInterface, error) {
	return nil, nil
}
func (a *stubWorker) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ llmpkg.EmbeddingModelConfigInterface, _ llmpkg.PlacementConfig) (llmpkg.EmbeddingModelInterface, error) {
	return nil, nil
}
func (a *stubWorker) Stats() []stats.DeviceStats { return nil }

func testWorkerFleet() *worker.Fleet {
	r := worker.NewFleet()
	_ = r.Register(&stubWorker{})
	return r
}

// newTestScheduler creates an Scheduler with a no-op startFn for testing.
func newTestScheduler(startDelay time.Duration, startErr error) *Scheduler {
	o := New(&config.Config{}, func() {}, zerolog.Nop(), testWorkerFleet())
	o.startFn = func(_ *Scheduler, _ *ManagedApp) error {
		if startDelay > 0 {
			time.Sleep(startDelay)
		}
		return startErr
	}
	return o
}

// addTestApp inserts a app directly into the orchestrator for testing.
func addTestApp(o *Scheduler, name string, state AppState, mode config.LaunchMode) *ManagedApp {
	mp := &ManagedApp{
		Config: &config.AppConfig{
			Name:       name,
			LaunchMode: mode,
		},
		Info: &massapp.AppInfo{
			Name:   name,
			Models: []massapp.ModelRequirement{{Name: "test-model"}},
		},
		State:   state,
		readyCh: make(chan struct{}),
	}
	o.mu.Lock()
	o.apps[name] = mp
	o.mu.Unlock()
	return mp
}

func TestEnsureRunning_AlreadyRunning(t *testing.T) {
	o := newTestScheduler(0, nil)
	mp := addTestApp(o, "mod1", StateRunning, config.LaunchModeOnDemand)
	close(mp.readyCh) // already running

	err := o.EnsureRunning(context.Background(), "mod1")
	require.NoError(t, err)
}

func TestEnsureRunning_Discovered(t *testing.T) {
	o := newTestScheduler(10*time.Millisecond, nil)
	addTestApp(o, "mod1", StateStopped, config.LaunchModeOnDemand)

	err := o.EnsureRunning(context.Background(), "mod1")
	require.NoError(t, err)

	o.mu.Lock()
	state := o.apps["mod1"].State
	o.mu.Unlock()
	require.Equal(t, StateRunning, state)
}

func TestEnsureRunning_ConcurrentCallers(t *testing.T) {
	var startCount int64
	o := New(&config.Config{}, func() {}, zerolog.Nop(), testWorkerFleet())
	o.startFn = func(_ *Scheduler, _ *ManagedApp) error {
		atomic.AddInt64(&startCount, 1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	addTestApp(o, "mod1", StateStopped, config.LaunchModeOnDemand)

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = o.EnsureRunning(context.Background(), "mod1")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "caller %d", i)
	}
	// Only one start should have been triggered.
	require.Equal(t, int64(1), atomic.LoadInt64(&startCount))
}

func TestEnsureRunning_ContextCancellation(t *testing.T) {
	// Start takes a long time — context should cancel before it finishes.
	o := newTestScheduler(5*time.Second, nil)
	addTestApp(o, "mod1", StateStopped, config.LaunchModeOnDemand)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := o.EnsureRunning(ctx, "mod1")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestEnsureRunning_StartError(t *testing.T) {
	o := newTestScheduler(0, errTestStart)
	addTestApp(o, "mod1", StateStopped, config.LaunchModeOnDemand)

	err := o.EnsureRunning(context.Background(), "mod1")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to start")
}

var errTestStart = &testError{"test start failure"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestEnsureRunning_NotFound(t *testing.T) {
	o := newTestScheduler(0, nil)

	err := o.EnsureRunning(context.Background(), "nonexistent")
	require.Error(t, err)
	require.ErrorContains(t, err, "not found")
}

func TestEnsureRunning_ErrorState(t *testing.T) {
	o := newTestScheduler(0, nil)
	addTestApp(o, "mod1", StateError, config.LaunchModeOnDemand)

	err := o.EnsureRunning(context.Background(), "mod1")
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot start on demand")
}

func TestIdleStop_FiresAfterTimeout(t *testing.T) {
	o := newTestScheduler(0, nil)
	o.cfg.AppIdleTimeout = "50ms"
	mp := addTestApp(o, "mod1", StateRunning, config.LaunchModeOnDemand)
	close(mp.readyCh)

	// Simulate request start + end to trigger the idle timer.
	o.TrackRequestStart("mod1")
	o.TrackRequestEnd("mod1")

	// Wait for idle timeout to fire.
	require.Eventually(t, func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.apps["mod1"].State == StateStopped
	}, 3*time.Second, 10*time.Millisecond)
}

func TestIdleStop_ResetByNewRequest(t *testing.T) {
	o := newTestScheduler(0, nil)
	o.cfg.AppIdleTimeout = "200ms"
	mp := addTestApp(o, "mod1", StateRunning, config.LaunchModeOnDemand)
	close(mp.readyCh)

	// First request ends → idle timer starts.
	o.TrackRequestStart("mod1")
	o.TrackRequestEnd("mod1")

	// Start another request well within the idle timeout → should cancel the timer.
	time.Sleep(50 * time.Millisecond)
	o.TrackRequestStart("mod1")

	// Wait past the original timeout — app should still be running.
	time.Sleep(300 * time.Millisecond)
	o.mu.Lock()
	state := o.apps["mod1"].State
	o.mu.Unlock()
	require.Equal(t, StateRunning, state)

	// End the second request — timer restarts.
	o.TrackRequestEnd("mod1")

	require.Eventually(t, func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.apps["mod1"].State == StateStopped
	}, 3*time.Second, 10*time.Millisecond)
}

func TestOnDemandAppsWithModels(t *testing.T) {
	o := newTestScheduler(0, nil)

	// On-demand + stopped → should be returned.
	addTestApp(o, "mod1", StateStopped, config.LaunchModeOnDemand)

	// On-demand + running → not returned (already running).
	mp2 := addTestApp(o, "mod2", StateRunning, config.LaunchModeOnDemand)
	close(mp2.readyCh)

	// Manual + stopped → not returned (not on-demand).
	addTestApp(o, "mod3", StateStopped, config.LaunchModeManual)

	// On-demand + stopped → should be returned (we start all on-demand apps).
	mp4 := addTestApp(o, "mod4", StateStopped, config.LaunchModeOnDemand)
	mp4.Info.Models = nil

	names := o.onDemandAppsWithModels()
	require.ElementsMatch(t, []string{"mod1", "mod4"}, names)
}

func TestTrackOnDemandApps(t *testing.T) {
	o := newTestScheduler(0, nil)

	mp1 := addTestApp(o, "mod1", StateRunning, config.LaunchModeOnDemand)
	close(mp1.readyCh)

	// Manual running — should not be tracked.
	mp2 := addTestApp(o, "mod2", StateRunning, config.LaunchModeManual)
	close(mp2.readyCh)

	tracked := o.trackOnDemandApps()
	require.Equal(t, []string{"mod1"}, tracked)
	require.Equal(t, int64(1), atomic.LoadInt64(&mp1.activeReqs))
	require.Equal(t, int64(0), atomic.LoadInt64(&mp2.activeReqs))
}
