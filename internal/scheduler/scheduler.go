// Package scheduler is MASS's runtime-agnostic job dispatcher.
//
// The scheduler accepts gateway-built [queue.Envelope] jobs, picks a worker
// of the matching runtime_name (preferring those that already have the model
// loaded), and streams chunks back to the caller. Job payloads are opaque
// bytes — the scheduler never inspects them.
//
// Key responsibilities:
//   - Track connected workers via [worker.Fleet] and their per-runtime-kind
//     index.
//   - Maintain a loaded-model index built from worker heartbeats so gateways
//     can answer "where is model X loaded?" without reaching into worker
//     internals.
//   - Coordinate model load/unload through the worker's bidi stream when a
//     gateway calls EnsureModelLoaded / EvictModel.
//   - Provide a durable global queue for jobs that arrive while no suitable
//     worker has free capacity.
//
// Earlier versions of MASS shipped a llama-shaped two-level queue with
// affinity-aware placement, work stealing, and a model pool. With runtime
// concerns moved into gateways, the v2 scheduler is a much simpler beast —
// pick a free worker, stream the response. Smarter dispatch can return
// later, driven by gateway-supplied hints.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
)

// ErrNoWorker is returned when no online worker matches the requested
// runtime_name.
var ErrNoWorker = errors.New("no worker available for runtime kind")

// ErrModelNotLoaded is returned by Schedule when a job references a model_id
// that no worker reports as loaded and auto_load was not requested.
var ErrModelNotLoaded = errors.New("model not loaded on any worker of this runtime kind")

// WorkerEnabledFn reports whether the operator-controlled toggle has the
// worker enabled. Returns true (admit) when no callback is wired or when
// the worker has no persisted disable state — sane default for newly-
// connected workers.
type WorkerEnabledFn func(workerID string) bool

// Scheduler is the central orchestrator. Construct with [New], then attach
// the persistent queue subsystem with [Scheduler.InitQueue] before dispatching.
type Scheduler struct {
	cfg     *config.Config
	saveFn  func()
	logger  zerolog.Logger
	workers *worker.Fleet

	mu        sync.RWMutex
	queuePool *queue.Pool
	globalQ   queue.QueueInterface
	results   queue.ResultStoreInterface

	workerEnabledMu sync.RWMutex
	workerEnabled   WorkerEnabledFn
}

// New builds a Scheduler. Call [Scheduler.InitQueue] once a database is
// available to enable persistent job queueing.
func New(cfg *config.Config, saveFn func(), logger zerolog.Logger, workers *worker.Fleet) *Scheduler {
	return &Scheduler{
		cfg:     cfg,
		saveFn:  saveFn,
		logger:  logger.With().Str("component", "scheduler").Logger(),
		workers: workers,
	}
}

// InitQueue wires the durable queue subsystem. The provided db handle is
// used for both the goqite-backed queue and the result store.
func (s *Scheduler) InitQueue(pool *queue.Pool, results queue.ResultStoreInterface) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queuePool = pool
	s.globalQ = pool.Open("global")
	s.results = results
}

// SetWorkerEnabledFn registers the per-worker enable check. When set,
// [WorkersForRuntime] hides any worker the callback rejects, so the
// scheduler skips them in dispatch entirely.
func (s *Scheduler) SetWorkerEnabledFn(fn WorkerEnabledFn) {
	s.workerEnabledMu.Lock()
	s.workerEnabled = fn
	s.workerEnabledMu.Unlock()
}

func (s *Scheduler) isWorkerEnabled(id string) bool {
	s.workerEnabledMu.RLock()
	fn := s.workerEnabled
	s.workerEnabledMu.RUnlock()
	if fn == nil {
		return true
	}
	return fn(id)
}

// ShutdownAll is a no-op today — there are no long-lived background workers
// to drain. Kept as a stable hook for future cleanup duties.
func (s *Scheduler) ShutdownAll() {}

// --- Worker queries ---

// WorkersForRuntime returns every online, operator-enabled worker
// advertising runtimeName.
func (s *Scheduler) WorkersForRuntime(runtimeName string) []*worker.StreamWorker {
	all := s.workers.All()
	out := make([]*worker.StreamWorker, 0, len(all))
	for _, w := range all {
		sw, ok := w.(*worker.StreamWorker)
		if !ok || !sw.Status().Online {
			continue
		}
		if sw.RuntimeName() != runtimeName {
			continue
		}
		if !s.isWorkerEnabled(sw.ID()) {
			continue
		}
		out = append(out, sw)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// WorkersWithModel returns workers of runtimeName that report modelID as
// loaded. A nil/empty modelID returns all workers of the kind (any model OK).
func (s *Scheduler) WorkersWithModel(runtimeName, modelID string) []*worker.StreamWorker {
	all := s.WorkersForRuntime(runtimeName)
	if modelID == "" {
		return all
	}
	out := make([]*worker.StreamWorker, 0, len(all))
	for _, w := range all {
		for _, lm := range w.LoadedModels() {
			if lm.ModelID == modelID {
				out = append(out, w)
				break
			}
		}
	}
	return out
}

// --- Schedule ---

// ScheduleRequest captures the gateway's job submission. Callers must pre-
// load the target model via [Scheduler.EnsureModelLoaded] — Schedule itself
// never auto-loads.
type ScheduleRequest struct {
	RuntimeName     string
	ModelID         string
	Payload         []byte
	Weight          int32
	AffinityWorkers []string
}

// Schedule picks a worker and streams the worker's chunks back through ch.
// The returned channel is closed when the job completes (Success or Error).
// Returns ErrModelNotLoaded if no worker of the matching runtime_name has
// req.ModelID resident; gateways must call EnsureModelLoaded first.
//
// Today's policy is intentionally simple: pick the worker with the most
// available capacity that already has the model loaded.
//
// Cancellation is the caller's responsibility: stop reading from ch when
// the inbound context is done. The job will keep running on the worker
// until completion; terminating it requires CancelJob (not yet wired).
func (s *Scheduler) Schedule(_ context.Context, req ScheduleRequest) (<-chan *worker.JobChunk, error) {
	if req.RuntimeName == "" {
		return nil, fmt.Errorf("schedule: runtime_name required")
	}

	pool, err := s.pickWorker(req)
	if err != nil {
		return nil, err
	}

	_, ch, err := pool.AssignJob(req.ModelID, req.Payload)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("assigning job: %w", err), map[string]any{"worker_id": pool.ID(), "model_id": req.ModelID, "runtime_name": req.RuntimeName})
	}
	return ch, nil
}

// EnsureModelLoadedRequest groups the inputs for [Scheduler.EnsureModelLoaded].
type EnsureModelLoadedRequest struct {
	RuntimeName string
	ModelID     string
	Files       []*workerpb.ModelFile
	LoadHints   []byte
	Preferred   []string
	// Source identifies who triggered the load — gateway-supplied, used for
	// the Scheduler tab's caller-attribution column. Empty defaults to "direct".
	Source string
}

// EnsureModelLoaded asks MASS to make sure modelID is loaded on at least one
// worker of runtimeName. If already loaded somewhere, returns those instances.
// Otherwise picks a worker and triggers HubLoadModel.
func (s *Scheduler) EnsureModelLoaded(req EnsureModelLoadedRequest) ([]LoadedInstance, error) {
	if existing := s.WorkersWithModel(req.RuntimeName, req.ModelID); len(existing) > 0 {
		out := make([]LoadedInstance, 0, len(existing))
		for _, w := range existing {
			for _, lm := range w.LoadedModels() {
				if lm.ModelID != req.ModelID {
					continue
				}
				out = append(out, LoadedInstance{WorkerID: w.ID(), PoolSize: int32(lm.PoolSize)})
				break
			}
		}
		return out, nil
	}

	pool, err := s.pickWorker(ScheduleRequest{RuntimeName: req.RuntimeName, AffinityWorkers: req.Preferred})
	if err != nil {
		return nil, err
	}
	res, err := pool.LoadModel(worker.LoadModelRequest{
		ModelID:   req.ModelID,
		Files:     req.Files,
		LoadHints: req.LoadHints,
		Source:    req.Source,
	})
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("loading model %s: %w", req.ModelID, err), map[string]any{"worker_id": pool.ID(), "model_id": req.ModelID, "runtime_name": req.RuntimeName})
	}
	// Wake every Scheduler-tab subscriber immediately — the next heartbeat
	// would also fire NotifyLoadedChanged via the diff in ApplyHeartbeat,
	// but that's seconds away; the user clicked or a gateway warmed up
	// just now.
	s.workers.NotifyLoadedChanged(pool.ID())
	return []LoadedInstance{{WorkerID: pool.ID(), PoolSize: res.PoolSize}}, nil
}

// EvictModel asks every worker (or one specific worker, when workerID != "")
// of runtimeName that has modelID loaded to drop it. Returns the count of
// successful unloads.
func (s *Scheduler) EvictModel(runtimeName, modelID, workerID string) (int, error) {
	candidates := s.WorkersWithModel(runtimeName, modelID)
	evicted := 0
	for _, w := range candidates {
		if workerID != "" && w.ID() != workerID {
			continue
		}
		if err := w.UnloadModel(modelID); err != nil {
			s.logger.Warn().Err(err).Str("worker_id", w.ID()).Str("model_id", modelID).Msg("evict model")
			continue
		}
		evicted++
		s.workers.NotifyLoadedChanged(w.ID())
	}
	return evicted, nil
}

// LoadedInstance describes one worker's view of a loaded model.
type LoadedInstance struct {
	WorkerID string
	PoolSize int32
}

// --- Internal selection ---

func (s *Scheduler) pickWorker(req ScheduleRequest) (*worker.StreamWorker, error) {
	candidates := s.WorkersForRuntime(req.RuntimeName)
	if len(candidates) == 0 {
		return nil, ctxerr.With(fmt.Errorf("%w: %s", ErrNoWorker, req.RuntimeName), map[string]any{"runtime_name": req.RuntimeName})
	}

	// Prefer affinity workers (gateway-supplied hint, e.g. workers that
	// already have the model loaded), then workers that themselves report
	// the model loaded, then any.
	prefer := func(w *worker.StreamWorker) int {
		score := 0
		for _, a := range req.AffinityWorkers {
			if a == w.ID() {
				score += 1000
			}
		}
		if req.ModelID != "" && workerHasModel(w, req.ModelID) {
			score += 500
		}
		score += w.AvailableCapacity()
		return score
	}
	sort.Slice(candidates, func(i, j int) bool {
		return prefer(candidates[i]) > prefer(candidates[j])
	})

	if req.ModelID != "" && len(s.WorkersWithModel(req.RuntimeName, req.ModelID)) == 0 {
		return nil, ctxerr.With(fmt.Errorf("%w: %s", ErrModelNotLoaded, req.ModelID), map[string]any{"runtime_name": req.RuntimeName, "model_id": req.ModelID})
	}
	return candidates[0], nil
}

func workerHasModel(w *worker.StreamWorker, modelID string) bool {
	if modelID == "" {
		return true
	}
	for _, lm := range w.LoadedModels() {
		if lm.ModelID == modelID {
			return true
		}
	}
	return false
}

// --- Cleanup helpers (called by main on a timer) ---

// CleanupResults removes job results older than the configured TTL.
func (s *Scheduler) CleanupResults(ctx context.Context) (int64, error) {
	_ = ctx
	s.mu.RLock()
	results := s.results
	s.mu.RUnlock()
	if results == nil {
		return 0, nil
	}
	return results.Cleanup(s.cfg.EffectiveResultTTL())
}

// StartResultCleanup launches a goroutine that periodically prunes the
// queue_results table. Runs until ctx is cancelled.
func (s *Scheduler) StartResultCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s.CleanupResults(ctx); err != nil {
					s.logger.Warn().Err(err).Msg("results cleanup")
				}
			}
		}
	}()
}

// StartIdleEviction launches a goroutine that periodically evicts
// loaded models that have been idle longer than the configured TTL
// (Config.EffectiveIdleEvictionTTL). Sweep cadence is half the TTL —
// short enough that idle models clear quickly, long enough that the
// heartbeat window (Active==0 momentarily between back-to-back jobs)
// doesn't trigger spurious evictions. Runs until ctx is cancelled.
// Idle is tracked per (worker, model) via LoadedModelStatus.IdleSince,
// stamped by the worker heartbeat merge.
func (s *Scheduler) StartIdleEviction(ctx context.Context) {
	go func() {
		ttl := s.cfg.EffectiveIdleEvictionTTL()
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.evictIdleOnce(s.cfg.EffectiveIdleEvictionTTL())
			}
		}
	}()
}

// evictIdleOnce scans every online worker for loaded models whose
// IdleSince exceeds ttl and unloads them. Best-effort; per-instance
// failures are logged and the sweep continues.
func (s *Scheduler) evictIdleOnce(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	for _, w := range s.workers.All() {
		sw, ok := w.(*worker.StreamWorker)
		if !ok || !sw.Status().Online {
			continue
		}
		for _, lm := range sw.LoadedModels() {
			if lm.Active > 0 || lm.IdleSince.IsZero() || lm.IdleSince.After(cutoff) {
				continue
			}
			if _, err := s.EvictModel(sw.RuntimeName(), lm.ModelID, sw.ID()); err != nil {
				s.logger.Warn().Err(err).
					Str("worker", sw.ID()).
					Str("model_id", lm.ModelID).
					Msg("idle eviction failed")
				continue
			}
			s.logger.Info().
				Str("worker", sw.ID()).
				Str("model_id", lm.ModelID).
				Dur("idle_for", time.Since(lm.IdleSince)).
				Msg("evicted idle model")
		}
	}
}
