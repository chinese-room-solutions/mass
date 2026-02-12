package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/llm"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// --- test stubs ---

type stubChatModel struct {
	closed int64
}

func (m *stubChatModel) Pool() llm.PredictorInterface { return nil }
func (m *stubChatModel) Close()                       { atomic.AddInt64(&m.closed, 1) }
func (m *stubChatModel) Closed() bool                 { return atomic.LoadInt64(&m.closed) > 0 }

type stubEmbeddingModel struct {
	closed int64
}

func (m *stubEmbeddingModel) Pool() llm.EmbedderInterface { return nil }
func (m *stubEmbeddingModel) Close()                      { atomic.AddInt64(&m.closed, 1) }
func (m *stubEmbeddingModel) Closed() bool                { return atomic.LoadInt64(&m.closed) > 0 }

type stubLoader struct {
	chatModel      *stubChatModel
	embedModel     *stubEmbeddingModel
	chatLoadCount  int64
	embedLoadCount int64
}

func (l *stubLoader) LoadChatModel(_ zerolog.Logger, _ string, _ config.ChatModelConfig, _ config.PlacementConfig) (llm.ChatModelInterface, error) {
	atomic.AddInt64(&l.chatLoadCount, 1)
	m := &stubChatModel{}
	l.chatModel = m
	return m, nil
}

func (l *stubLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ config.EmbeddingModelConfig, _ config.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	atomic.AddInt64(&l.embedLoadCount, 1)
	m := &stubEmbeddingModel{}
	l.embedModel = m
	return m, nil
}

// --- tests ---

func TestModelPool_GetOrLoadChat_Reuse(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	_, fp1, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	pool.Release(fp1)

	_, fp2, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	pool.Release(fp2)

	require.Equal(t, fp1, fp2)
	require.Equal(t, int64(1), atomic.LoadInt64(&loader.chatLoadCount))
}

func TestModelPool_GetOrLoadEmbedding_Reuse(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}

	_, fp1, err := pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	pool.Release(fp1)

	_, fp2, err := pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	pool.Release(fp2)

	require.Equal(t, fp1, fp2)
	require.Equal(t, int64(1), atomic.LoadInt64(&loader.embedLoadCount))
}

func TestModelPool_DifferentConfigs(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg1 := config.ChatModelConfig{Path: "/model1.gguf"}
	cfg2 := config.ChatModelConfig{Path: "/model2.gguf"}

	_, fp1, err := pool.GetOrLoadChat(cfg1, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	_, fp2, err := pool.GetOrLoadChat(cfg2, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	require.NotEqual(t, fp1, fp2)
	require.Equal(t, int64(2), atomic.LoadInt64(&loader.chatLoadCount))

	pool.Release(fp1)
	pool.Release(fp2)
}

func TestModelPool_IdleEviction_Chat(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 50*time.Millisecond)

	cfg := config.ChatModelConfig{Path: "/model.gguf"}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	// Release triggers idle timer.
	pool.Release(fp)

	// Wait for eviction.
	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.chatModels[fp]
		return !exists
	}, 3*time.Second, 10*time.Millisecond)

	require.True(t, loader.chatModel.Closed())
}

func TestModelPool_IdleEviction_Embedding(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 50*time.Millisecond)

	cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}
	_, fp, err := pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	pool.Release(fp)

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.embedModels[fp]
		return !exists
	}, 3*time.Second, 10*time.Millisecond)

	require.True(t, loader.embedModel.Closed())
}

func TestModelPool_IdleEviction_CancelledByNewRequest(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 200*time.Millisecond)

	cfg := config.ChatModelConfig{Path: "/model.gguf"}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	// Release → idle timer starts.
	pool.Release(fp)

	// Re-acquire well before timeout → cancels timer.
	time.Sleep(50 * time.Millisecond)
	_, fp2, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	require.Equal(t, fp, fp2)

	// Wait past the original timeout — should still exist.
	time.Sleep(300 * time.Millisecond)
	pool.mu.RLock()
	_, exists := pool.chatModels[fp]
	pool.mu.RUnlock()
	require.True(t, exists, "model should not be evicted while active")
	require.False(t, loader.chatModel.Closed())

	// Now release again — timer restarts.
	pool.Release(fp2)
	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.chatModels[fp]
		return !exists
	}, 3*time.Second, 10*time.Millisecond)
}

func TestModelPool_StaticModels_NotEvicted(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 50*time.Millisecond)

	chatModel := &stubChatModel{}
	cfg := config.ChatModelConfig{Path: "/user-model.gguf"}
	fp := pool.RegisterChat("direct", "", cfg, config.PlacementConfig{}, chatModel, "local", "Local")

	// Release a static (user-loaded) model — should not start idle timer.
	pool.Release(fp)

	// Wait past idle timeout — should still exist.
	time.Sleep(100 * time.Millisecond)
	pool.mu.RLock()
	_, exists := pool.chatModels[fp]
	pool.mu.RUnlock()
	require.True(t, exists)
	require.False(t, chatModel.Closed())
}

func TestModelPool_ConcurrentGetOrLoad(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}
	done := make(chan string, 5)

	for range 5 {
		go func() {
			_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
			require.NoError(t, err)
			done <- fp
		}()
	}

	fps := make([]string, 5)
	for i := range 5 {
		fps[i] = <-done
	}

	// All should get the same fingerprint.
	for _, fp := range fps {
		require.Equal(t, fps[0], fp)
	}
	// Only one load should have happened.
	require.Equal(t, int64(1), atomic.LoadInt64(&loader.chatLoadCount))

	// Release all.
	for _, fp := range fps {
		pool.Release(fp)
	}
}

func TestModelPool_CloseAll_StopsTimers(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf"}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	// Release starts idle timer (1 minute — won't fire during test).
	pool.Release(fp)

	// CloseAll should close model and stop timer.
	pool.CloseAll()

	pool.mu.RLock()
	require.Empty(t, pool.chatModels)
	pool.mu.RUnlock()
	require.True(t, loader.chatModel.Closed())
}

func TestModelPool_ZeroIdleTimeout_NoEviction(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 0)

	cfg := config.ChatModelConfig{Path: "/model.gguf"}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)

	pool.Release(fp)

	// With 0 timeout, no timer should be started.
	time.Sleep(50 * time.Millisecond)
	pool.mu.RLock()
	_, exists := pool.chatModels[fp]
	pool.mu.RUnlock()
	require.True(t, exists, "model should persist with zero idle timeout")
}

func TestModelPool_ResolverReleaseAll(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 50*time.Millisecond)

	resolver := &modelResolver{pool: pool, source: "direct"}

	// Simulate a dynamic chat request via the resolver.
	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	// Manually do what ResolveChat does for model_config path.
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	resolver.acquired = append(resolver.acquired, fp)

	// ReleaseAll should decrement and start idle timer.
	resolver.ReleaseAll()
	require.Empty(t, resolver.acquired)

	// Wait for eviction.
	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.chatModels[fp]
		return !exists
	}, 3*time.Second, 10*time.Millisecond)
}

func TestModelPool_LoadingStatus_VisibleInSnapshot(t *testing.T) {
	// Use a slow loader so we can observe the "loading" state.
	started := make(chan struct{})
	proceed := make(chan struct{})
	loader := &slowLoader{
		started: started,
		proceed: proceed,
	}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/slow-model.gguf"}

	// Start loading in background.
	go func() {
		_, _, _ = pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	}()

	// Wait until the loader is actually called.
	<-started

	// Snapshot should show a loading instance.
	snap := pool.Snapshot()
	require.Len(t, snap, 1)
	require.True(t, snap[0].Loading, "instance should be loading")
	require.Empty(t, snap[0].Config, "loading instance should have no config entries")

	// Let the load finish.
	close(proceed)

	// Wait for the instance to become ready.
	require.Eventually(t, func() bool {
		snap = pool.Snapshot()
		return len(snap) == 1 && !snap[0].Loading
	}, 3*time.Second, 10*time.Millisecond)

	require.False(t, snap[0].Loading)
}

// slowLoader blocks LoadChatModel until proceed is closed.
type slowLoader struct {
	started chan struct{} // closed when LoadChatModel begins
	proceed chan struct{} // LoadChatModel blocks until this is closed
}

func (l *slowLoader) LoadChatModel(_ zerolog.Logger, _ string, _ config.ChatModelConfig, _ config.PlacementConfig) (llm.ChatModelInterface, error) {
	close(l.started)
	<-l.proceed
	return &stubChatModel{}, nil
}

func (l *slowLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ config.EmbeddingModelConfig, _ config.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	return &stubEmbeddingModel{}, nil
}

func TestModelPool_ModuleModels_AreDynamic(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), 50*time.Millisecond)

	// Module loads a model via GetOrLoadChat — should be dynamic.
	cfg := config.ChatModelConfig{Path: "/module-model.gguf"}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "module:testmod")
	require.NoError(t, err)

	pool.mu.RLock()
	inst := pool.chatModels[fp]
	pool.mu.RUnlock()
	require.Equal(t, ModeDynamic, inst.mode)

	// Release — idle timer should start and evict.
	pool.Release(fp)

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.chatModels[fp]
		return !exists
	}, 3*time.Second, 10*time.Millisecond)

	require.True(t, loader.chatModel.Closed())
}

// slowEmbeddingLoader blocks LoadEmbeddingModel until proceed is closed.
type slowEmbeddingLoader struct {
	started chan struct{} // closed when LoadEmbeddingModel begins
	proceed chan struct{} // LoadEmbeddingModel blocks until this is closed
}

func (l *slowEmbeddingLoader) LoadChatModel(_ zerolog.Logger, _ string, _ config.ChatModelConfig, _ config.PlacementConfig) (llm.ChatModelInterface, error) {
	return &stubChatModel{}, nil
}

func (l *slowEmbeddingLoader) LoadEmbeddingModel(_ zerolog.Logger, _ string, _ config.EmbeddingModelConfig, _ config.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	close(l.started)
	<-l.proceed
	return &stubEmbeddingModel{}, nil
}

func TestModelPool_AcquireChat(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) (*modelPool, string)
		wantFound bool
	}{
		{
			name: "existing model",
			setup: func(t *testing.T) (*modelPool, string) {
				t.Helper()
				loader := &stubLoader{}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)
				cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}
				_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
				require.NoError(t, err)
				pool.Release(fp)
				return pool, fp
			},
			wantFound: true,
		},
		{
			name: "not found",
			setup: func(_ *testing.T) (*modelPool, string) {
				loader := &stubLoader{}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)
				return pool, "nonexistent-fingerprint"
			},
			wantFound: false,
		},
		{
			name: "loading model",
			setup: func(_ *testing.T) (*modelPool, string) {
				started := make(chan struct{})
				proceed := make(chan struct{})
				loader := &slowLoader{started: started, proceed: proceed}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

				cfg := config.ChatModelConfig{Path: "/slow.gguf"}
				fp := config.ChatModelFingerprint(cfg)

				go func() {
					_, _, _ = pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
				}()
				<-started

				// Ensure cleanup after test.
				t.Cleanup(func() { close(proceed) })

				return pool, fp
			},
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, fp := tt.setup(t)

			pred, retFP, ok := pool.AcquireChat(fp)
			require.Equal(t, tt.wantFound, ok)

			if tt.wantFound {
				require.NotEmpty(t, retFP)
				// stubChatModel.Pool() returns nil, which is the expected predictor.
				_ = pred

				// Verify activeReqs was incremented.
				pool.mu.RLock()
				inst := pool.chatModels[fp]
				reqs := inst.activeReqs
				pool.mu.RUnlock()
				require.Equal(t, int64(1), reqs)

				pool.Release(fp)
			} else {
				require.Empty(t, retFP)
				require.Nil(t, pred)
			}
		})
	}
}

func TestModelPool_AcquireEmbedding(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) (*modelPool, string)
		wantFound bool
	}{
		{
			name: "existing model",
			setup: func(t *testing.T) (*modelPool, string) {
				t.Helper()
				loader := &stubLoader{}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)
				cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}
				_, fp, err := pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
				require.NoError(t, err)
				pool.Release(fp)
				return pool, fp
			},
			wantFound: true,
		},
		{
			name: "not found",
			setup: func(_ *testing.T) (*modelPool, string) {
				loader := &stubLoader{}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)
				return pool, "nonexistent-fingerprint"
			},
			wantFound: false,
		},
		{
			name: "loading model",
			setup: func(_ *testing.T) (*modelPool, string) {
				started := make(chan struct{})
				proceed := make(chan struct{})
				loader := &slowEmbeddingLoader{started: started, proceed: proceed}
				pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

				cfg := config.EmbeddingModelConfig{Path: "/slow-embed.gguf"}
				fp := config.EmbeddingModelFingerprint(cfg)

				go func() {
					_, _, _ = pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
				}()
				<-started

				t.Cleanup(func() { close(proceed) })

				return pool, fp
			},
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, fp := tt.setup(t)

			embedder, retFP, ok := pool.AcquireEmbedding(fp)
			require.Equal(t, tt.wantFound, ok)

			if tt.wantFound {
				require.NotEmpty(t, retFP)
				_ = embedder

				pool.mu.RLock()
				inst := pool.embedModels[fp]
				reqs := inst.activeReqs
				pool.mu.RUnlock()
				require.Equal(t, int64(1), reqs)

				pool.Release(fp)
			} else {
				require.Empty(t, retFP)
				require.Nil(t, embedder)
			}
		})
	}
}

func TestModelPool_AcquireChat_ConcurrentSafe(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}
	_, fp, err := pool.GetOrLoadChat(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	// Release the initial acquisition from GetOrLoadChat so we start clean.
	pool.Release(fp)

	const n = 10
	done := make(chan bool, n)

	for range n {
		go func() {
			_, _, ok := pool.AcquireChat(fp)
			done <- ok
		}()
	}

	for range n {
		ok := <-done
		require.True(t, ok)
	}

	// All goroutines acquired — activeReqs should equal n.
	pool.mu.RLock()
	inst := pool.chatModels[fp]
	reqs := inst.activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(n), reqs)

	// Clean up.
	for range n {
		pool.Release(fp)
	}
}

func TestModelPool_AcquireEmbedding_ConcurrentSafe(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}
	_, fp, err := pool.GetOrLoadEmbedding(cfg, config.PlacementConfig{}, "direct")
	require.NoError(t, err)
	pool.Release(fp)

	const n = 10
	done := make(chan bool, n)

	for range n {
		go func() {
			_, _, ok := pool.AcquireEmbedding(fp)
			done <- ok
		}()
	}

	for range n {
		ok := <-done
		require.True(t, ok)
	}

	pool.mu.RLock()
	inst := pool.embedModels[fp]
	reqs := inst.activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(n), reqs)

	for range n {
		pool.Release(fp)
	}
}

// --- resolver getOrLoad tests ---

func TestResolver_GetOrLoadChat_FastPath(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}
	chatModel := &stubChatModel{}
	fp := pool.RegisterChat("direct", "", cfg, config.PlacementConfig{}, chatModel, "local", "Local")

	loadCalled := false
	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(string, *config.ChatModelConfig, *config.EmbeddingModelConfig, config.PlacementConfig, string) (string, error) {
			loadCalled = true
			return "", nil
		},
	}

	pred, retFP, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
	require.NoError(t, err)
	require.Equal(t, fp, retFP)
	require.False(t, loadCalled, "loadModel should not be called when model is already in pool")

	// stubChatModel.Pool() returns nil — that is the expected predictor.
	_ = pred

	// Verify activeReqs incremented.
	pool.mu.RLock()
	inst := pool.chatModels[fp]
	reqs := inst.activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(1), reqs)

	pool.Release(fp)
}

func TestResolver_GetOrLoadChat_LoadsViaScheduler(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	// loadModel simulates what Scheduler.LoadModel does: registers the model in
	// the pool and returns the fingerprint.
	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(modelType string, chatCfg *config.ChatModelConfig, _ *config.EmbeddingModelConfig, placement config.PlacementConfig, _ string) (string, error) {
			require.Equal(t, "chat", modelType)
			require.NotNil(t, chatCfg)
			fp := pool.RegisterChat("direct", "", *chatCfg, placement, &stubChatModel{}, "local", "Local")
			return fp, nil
		},
	}

	pred, fp, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, fp)
	_ = pred

	// Verify activeReqs incremented by AcquireChat inside getOrLoadChat.
	pool.mu.RLock()
	inst := pool.chatModels[fp]
	reqs := inst.activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(1), reqs)

	pool.Release(fp)
}

func TestResolver_GetOrLoadChat_Fallback(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	// No loadModel — nil falls back to pool.GetOrLoadChat.
	r := &modelResolver{
		pool:      pool,
		source:    "direct",
		loadModel: nil,
	}

	_, fp, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, fp)

	// The pool's loader should have been invoked.
	require.Equal(t, int64(1), atomic.LoadInt64(&loader.chatLoadCount))

	pool.Release(fp)
}

func TestResolver_GetOrLoadChat_LoadError(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	loadErr := fmt.Errorf("device unavailable")
	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(string, *config.ChatModelConfig, *config.EmbeddingModelConfig, config.PlacementConfig, string) (string, error) {
			return "", loadErr
		},
	}

	_, _, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
	require.Error(t, err)
	require.ErrorIs(t, err, loadErr)
}

func TestResolver_ReleaseAll_AfterGetOrLoad(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}

	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(_ string, chatCfg *config.ChatModelConfig, _ *config.EmbeddingModelConfig, placement config.PlacementConfig, _ string) (string, error) {
			fp := pool.RegisterChat("direct", "", *chatCfg, placement, &stubChatModel{}, "local", "Local")
			return fp, nil
		},
	}

	_, fp, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
	require.NoError(t, err)
	r.acquired = append(r.acquired, fp)

	// Verify activeReqs is 1.
	pool.mu.RLock()
	reqs := pool.chatModels[fp].activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(1), reqs)

	// ReleaseAll should bring activeReqs back to 0.
	r.ReleaseAll()
	require.Empty(t, r.acquired)

	pool.mu.RLock()
	reqs = pool.chatModels[fp].activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(0), reqs)
}

// --- resolver getOrLoadEmbedding tests ---

func TestResolver_GetOrLoadEmbedding_LoadsViaScheduler(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}

	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(modelType string, _ *config.ChatModelConfig, embedCfg *config.EmbeddingModelConfig, placement config.PlacementConfig, _ string) (string, error) {
			require.Equal(t, "embedding", modelType)
			require.NotNil(t, embedCfg)
			fp := pool.RegisterEmbedding("direct", "", *embedCfg, placement, &stubEmbeddingModel{}, "local", "Local")
			return fp, nil
		},
	}

	embedder, fp, err := r.getOrLoadEmbedding(cfg, config.PlacementConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, fp)
	_ = embedder

	pool.mu.RLock()
	reqs := pool.embedModels[fp].activeReqs
	pool.mu.RUnlock()
	require.Equal(t, int64(1), reqs)

	pool.Release(fp)
}

func TestResolver_GetOrLoadEmbedding_LoadError(t *testing.T) {
	loader := &stubLoader{}
	pool := newModelPool(loader, "local", "Local", zerolog.Nop(), time.Minute)

	cfg := config.EmbeddingModelConfig{Path: "/embed.gguf"}

	loadErr := fmt.Errorf("no VRAM available")
	r := &modelResolver{
		pool:   pool,
		source: "direct",
		loadModel: func(string, *config.ChatModelConfig, *config.EmbeddingModelConfig, config.PlacementConfig, string) (string, error) {
			return "", loadErr
		},
	}

	_, _, err := r.getOrLoadEmbedding(cfg, config.PlacementConfig{})
	require.Error(t, err)
	require.ErrorIs(t, err, loadErr)
}

// Compile-time interface checks.
var (
	_ llm.ModelLoaderInterface = (*stubLoader)(nil)
	_ llm.ModelLoaderInterface = (*slowLoader)(nil)
	_ llm.ModelLoaderInterface = (*slowEmbeddingLoader)(nil)
	_ context.Context          = context.Background()
)
