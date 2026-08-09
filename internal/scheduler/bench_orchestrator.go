package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
)

// The bench orchestrator keeps one measured row per (worker, current
// device set, model). It discovers triples with no valid row, runs the
// benchmark on the worker, and writes the verdict.
//
// Exclusivity is the load-bearing property: a measurement taken while
// other work shares the device is contention, not capability. Before a
// bench the orchestrator raises a per-worker dispatch gate, lets the
// running jobs drain, and clears idle residents off the target device
// set — the same machinery dispatchEnvelope uses.

// BenchModel is one catalogue model MASS should keep benched. ID is the
// gateway's own handle (what AuthorBenchPayload takes); Key is the
// store-relative cache key of the model's primary file, which is what
// model_benchmarks rows and load artifacts are keyed by.
type BenchModel struct {
	ID  string
	Key string
}

// BenchPayload is a gateway-authored benchmark request. MASS ships every
// field to the worker verbatim and never looks inside Payload or
// LoadHints. Files must be the gateway's complete artifact set — only it
// knows which companions LoadHints names.
type BenchPayload struct {
	Payload   []byte
	LoadHints []byte
	Cost      float64
	Files     []*workerpb.ModelFile
}

// BenchModelsFn lists the models of one runtime that are worth benching.
type BenchModelsFn func(ctx context.Context, runtimeName string) ([]BenchModel, error)

// BenchPayloadAuthorFn asks a runtime's gateway to author the benchmark
// request for one model. Returning an error wrapping [ErrBenchModelGone]
// means the model isn't ready yet (typically an in-flight download) and
// the orchestrator retries without spending an attempt.
type BenchPayloadAuthorFn func(ctx context.Context, runtimeName, modelID string) (BenchPayload, error)

// ErrBenchModelGone reports that the gateway has no such model right
// now. Retryable and free: a download that is still landing produces it.
var ErrBenchModelGone = errors.New("bench: gateway has no such model yet")

// Retry policy for transient bench failures: three attempts, backing off
// 30s then 5m. After the last one the error is persisted as if the
// device set were incapable, so jobs stop waiting on it.
const benchMaxAttempts = 3

var benchBackoff = [...]time.Duration{30 * time.Second, 5 * time.Minute}

// benchTask is one queued (worker, device set, model) measurement.
type benchTask struct {
	model       BenchModel
	runtimeName string
	deviceSet   string
	attempts    int
	readyAt     time.Time
}

// benchOrchestrator owns one runner goroutine per connected worker.
// Benches on a worker run one at a time, in arrival order; different
// workers are independent.
type benchOrchestrator struct {
	s *Scheduler

	mu      sync.Mutex
	ctx     context.Context
	runners map[string]*benchRunner // workerID

	models BenchModelsFn
	author BenchPayloadAuthorFn
}

// benchRunner is one worker's serial bench queue plus the goroutine that
// drains it. Owned by the orchestrator: created on the first enqueue for
// that worker, stopped on disconnect or when the orchestrator's context
// is cancelled.
type benchRunner struct {
	workerID string
	wake     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	stopped  chan struct{}

	mu      sync.Mutex
	queue   []*benchTask
	current *benchTask
}

func newBenchOrchestrator(s *Scheduler) *benchOrchestrator {
	return &benchOrchestrator{s: s, runners: make(map[string]*benchRunner)}
}

// SetBenchProviders wires the two gateway-backed callbacks the bench
// orchestrator needs: what to bench, and what request to bench it with.
// Without them the orchestrator discovers nothing and stays idle.
func (s *Scheduler) SetBenchProviders(models BenchModelsFn, author BenchPayloadAuthorFn) {
	s.bench.mu.Lock()
	s.bench.models = models
	s.bench.author = author
	s.bench.mu.Unlock()
}

// StartBenchOrchestrator gives the orchestrator its lifetime context and
// sweeps every already-connected worker for missing rows — the same work
// a worker-connect trigger does, applied to a fleet that reconnected
// before this call. Rows survive restarts, so the sweep only queues what
// is genuinely missing.
func (s *Scheduler) StartBenchOrchestrator(ctx context.Context) {
	s.bench.mu.Lock()
	s.bench.ctx = ctx
	s.bench.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.bench.stopAll()
	}()
	for _, w := range s.allStreamWorkers() {
		s.bench.sweepWorker(w)
	}
}

// RebenchModel drops whatever verdict the fleet recorded for modelKey and
// queues a fresh measurement on every eligible worker. This is the manual
// escape hatch: an incapable row is never retried automatically, so
// clearing it is the only way back once the operator has changed
// something the bench can't see (a driver, a device, the hardware).
func (s *Scheduler) RebenchModel(runtimeName, modelKey string) error {
	st := s.benchStore()
	if st == nil {
		return fmt.Errorf("rebench: store not initialised")
	}
	if err := st.DeleteModelBenchmarksByModel(modelKey); err != nil {
		return err
	}
	s.InvalidateModelBenchmarks("")
	go s.bench.queueModelEverywhere(runtimeName, modelKey)
	return nil
}

// OnModelDownloaded is the download-completion trigger: a model whose
// files just landed gets benched on every connected worker of its
// runtime, so the first job for it doesn't pay for the measurement.
func (s *Scheduler) OnModelDownloaded(runtimeName, relPath string) {
	if runtimeName == "" || relPath == "" {
		return
	}
	// The completed file may be a companion rather than the model's
	// primary artifact, so resolve through the catalogue rather than
	// assuming the relpath is itself a model key.
	s.bench.queueModelsMatching(runtimeName, relPath)
}

// OnModelRemoved forgets every measurement of the removed model and tells
// each worker holding its files to drop them. The worker skips files
// backing a loaded model on its own, so this is safe to fire eagerly.
func (s *Scheduler) OnModelRemoved(relPaths []string) {
	st := s.benchStore()
	for _, key := range relPaths {
		if st != nil {
			if err := st.DeleteModelBenchmarksByModel(key); err != nil {
				s.logger.Warn().Err(err).Str("model_key", key).Msg("deleting model benchmarks on model removal")
			}
		}
		s.bench.cancelModel(key)
	}
	s.InvalidateModelBenchmarks("")

	for _, w := range s.allStreamWorkers() {
		if err := w.DeleteCacheFiles(relPaths); err != nil {
			s.logger.Warn().Err(err).Str("worker", w.ID()).Msg("requesting cache file deletion on model removal")
		}
	}
}

// OnWorkerRemoved forgets every measurement recorded against a worker the
// operator has revoked. Called from the revoke path, not from a plain
// disconnect: a worker that merely dropped will come back and its rows
// are still valid.
func (s *Scheduler) OnWorkerRemoved(workerID string) {
	if st := s.benchStore(); st != nil {
		if err := st.DeleteModelBenchmarksByWorker(workerID); err != nil {
			s.logger.Warn().Err(err).Str("worker", workerID).Msg("deleting model benchmarks on worker removal")
		}
	}
	s.InvalidateModelBenchmarks(workerID)
}

// BenchInFlight reports the model currently being measured on workerID,
// or "" when the worker is free. The UI reads it to show why a worker is
// briefly not taking work.
func (s *Scheduler) BenchInFlight(workerID string) string {
	s.bench.mu.Lock()
	r, ok := s.bench.runners[workerID]
	s.bench.mu.Unlock()
	if !ok {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil {
		return ""
	}
	return r.current.model.Key
}

// BenchInFlightInfo is one measurement running right now: the model being
// measured and the worker it occupies.
type BenchInFlightInfo struct {
	WorkerID    string
	RuntimeName string
	ModelKey    string
}

// BenchesInFlight reports every measurement running across the fleet,
// sorted by worker id. Queued-but-not-started tasks are deliberately
// left out — only work actually occupying a worker is reported, because
// that is what the Queue tab renders.
func (s *Scheduler) BenchesInFlight() []BenchInFlightInfo {
	s.bench.mu.Lock()
	runners := make([]*benchRunner, 0, len(s.bench.runners))
	for _, r := range s.bench.runners {
		runners = append(runners, r)
	}
	s.bench.mu.Unlock()

	out := make([]BenchInFlightInfo, 0, len(runners))
	for _, r := range runners {
		r.mu.Lock()
		cur := r.current
		r.mu.Unlock()
		if cur == nil {
			continue
		}
		out = append(out, BenchInFlightInfo{
			WorkerID:    r.workerID,
			RuntimeName: cur.runtimeName,
			ModelKey:    cur.model.Key,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

// benchStore returns the store slice the orchestrator writes through, or
// nil before [Scheduler.InitQueue].
func (s *Scheduler) benchStore() ModelBenchStoreInterface {
	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return nil
	}
	mb, ok := st.(ModelBenchStoreInterface)
	if !ok {
		return nil
	}
	return mb
}

// ModelBenchStoreInterface is the persistence the bench orchestrator
// needs. Kept separate from [StateStoreInterface] so a scheduler wired
// for placement-only tests doesn't have to implement the writes.
type ModelBenchStoreInterface interface {
	SaveModelBenchmark(row store.ModelBenchmarkRow) error
	SaveModelBenchmarkError(row store.ModelBenchmarkRow) error
	DeleteModelBenchmarksByModel(modelID string) error
	DeleteModelBenchmarksByWorker(workerID string) error
}

// allStreamWorkers snapshots every registered stream worker, sorted for
// deterministic sweep order.
func (s *Scheduler) allStreamWorkers() []*worker.StreamWorker {
	if s.workers == nil {
		return nil
	}
	all := s.workers.All()
	out := make([]*worker.StreamWorker, 0, len(all))
	for _, w := range all {
		if sw, ok := w.(*worker.StreamWorker); ok && sw.Status().Online {
			out = append(out, sw)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// --- Discovery ---

// sweepWorkerAsync runs [benchOrchestrator.sweepWorker] off the caller's
// goroutine. Discovery talks to the gateway, and the callers — worker
// connect, device toggle — are latency-sensitive paths that must not
// block on it. The goroutine is bounded by the orchestrator's context
// through the RPC it makes and exits as soon as the sweep returns.
func (b *benchOrchestrator) sweepWorkerAsync(w *worker.StreamWorker) {
	if ctx, models, _ := b.providers(); ctx == nil || models == nil {
		return
	}
	go b.sweepWorker(w)
}

// sweepWorker queues a bench for every model of w's runtime that has no
// valid row on w's CURRENT predicted device set. This is the
// worker-connect and device-toggle trigger, and the startup sweep.
func (b *benchOrchestrator) sweepWorker(w *worker.StreamWorker) {
	ctx, models, _ := b.providers()
	if ctx == nil || models == nil {
		return
	}
	set := b.s.predictDeviceSet(w)
	if len(set) == 0 {
		// An empty whitelist comes back "no devices enabled" from the
		// worker: there is nothing to measure, so skip rather than
		// record a verdict the operator's next toggle invalidates.
		return
	}
	catalogue, err := models(ctx, w.RuntimeName())
	if err != nil {
		b.s.logger.Warn().Err(err).Str("worker", w.ID()).Str("runtime_name", w.RuntimeName()).Msg("bench sweep: listing models")
		return
	}
	for _, m := range catalogue {
		b.enqueueIfMissing(w, m)
	}
}

// queueModelEverywhere queues one model on every online worker of its
// runtime, regardless of whether a row exists — the manual re-bench path.
func (b *benchOrchestrator) queueModelEverywhere(runtimeName, modelKey string) {
	ctx, models, _ := b.providers()
	if ctx == nil || models == nil {
		return
	}
	catalogue, err := models(ctx, runtimeName)
	if err != nil {
		b.s.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("rebench: listing models")
		return
	}
	for _, m := range catalogue {
		if m.Key != modelKey {
			continue
		}
		for _, w := range b.s.allStreamWorkers() {
			if w.RuntimeName() != runtimeName {
				continue
			}
			if set := b.s.predictDeviceSet(w); len(set) > 0 {
				b.enqueue(w, m, deviceSetKey(set))
			}
		}
	}
}

// queueModelsMatching queues every catalogue model of runtimeName whose
// artifacts include relPath — the download-completion trigger, where the
// finished file may be the model itself or one of its companions.
func (b *benchOrchestrator) queueModelsMatching(runtimeName, relPath string) {
	ctx, models, _ := b.providers()
	if ctx == nil || models == nil {
		return
	}
	catalogue, err := models(ctx, runtimeName)
	if err != nil {
		b.s.logger.Warn().Err(err).Str("runtime_name", runtimeName).Msg("bench on download: listing models")
		return
	}
	for _, m := range catalogue {
		if m.Key != relPath {
			continue
		}
		for _, w := range b.s.allStreamWorkers() {
			if w.RuntimeName() == runtimeName {
				b.enqueueIfMissing(w, m)
			}
		}
	}
}

// enqueueIfMissing queues a bench only when the triple has no valid row.
// A row with an error counts as concluded — incapable is permanent until
// a manual re-bench or a model-file change wipes it.
func (b *benchOrchestrator) enqueueIfMissing(w *worker.StreamWorker, m BenchModel) {
	set := b.s.predictDeviceSet(w)
	if len(set) == 0 {
		return
	}
	devSet := deviceSetKey(set)
	row, ok := b.s.lookupModelBenchmark(w.ID(), devSet, m.Key)
	if (ok || row.Error != "") && b.s.modelFileUnchanged(row) {
		return
	}
	b.enqueue(w, m, devSet)
}

// enqueue appends a task to the worker's queue, starting its runner if
// this is the first one. Duplicate (model, device set) pairs collapse:
// the queued or in-flight bench already answers the question.
func (b *benchOrchestrator) enqueue(w *worker.StreamWorker, m BenchModel, devSet string) {
	b.mu.Lock()
	ctx := b.ctx
	if ctx == nil {
		b.mu.Unlock()
		return
	}
	r, ok := b.runners[w.ID()]
	if !ok {
		r = &benchRunner{
			workerID: w.ID(),
			wake:     make(chan struct{}, 1),
			stop:     make(chan struct{}),
			stopped:  make(chan struct{}),
		}
		b.runners[w.ID()] = r
		go b.run(ctx, r)
	}
	b.mu.Unlock()

	r.mu.Lock()
	dup := r.current != nil && r.current.model.Key == m.Key && r.current.deviceSet == devSet
	for _, t := range r.queue {
		if t.model.Key == m.Key && t.deviceSet == devSet {
			dup = true
			break
		}
	}
	if !dup {
		r.queue = append(r.queue, &benchTask{model: m, runtimeName: w.RuntimeName(), deviceSet: devSet})
	}
	r.mu.Unlock()
	if !dup {
		r.kick()
	}
}

// cancelModel drops every queued task for modelKey. An in-flight bench is
// left to finish — the worker owns it, and its row is deleted anyway.
func (b *benchOrchestrator) cancelModel(modelKey string) {
	b.mu.Lock()
	runners := make([]*benchRunner, 0, len(b.runners))
	for _, r := range b.runners {
		runners = append(runners, r)
	}
	b.mu.Unlock()
	for _, r := range runners {
		r.mu.Lock()
		kept := r.queue[:0]
		for _, t := range r.queue {
			if t.model.Key != modelKey {
				kept = append(kept, t)
			}
		}
		r.queue = kept
		r.mu.Unlock()
	}
}

// stopWorker tears down a worker's runner. Called on disconnect: an
// in-flight bench unblocks with ErrWorkerOffline, leaving no row, and the
// reconnect sweep re-queues it.
func (b *benchOrchestrator) stopWorker(workerID string) {
	b.mu.Lock()
	r, ok := b.runners[workerID]
	delete(b.runners, workerID)
	b.mu.Unlock()
	if ok {
		r.shutdown()
	}
}

func (b *benchOrchestrator) stopAll() {
	b.mu.Lock()
	runners := make([]*benchRunner, 0, len(b.runners))
	for _, r := range b.runners {
		runners = append(runners, r)
	}
	clear(b.runners)
	b.mu.Unlock()
	for _, r := range runners {
		r.shutdown()
	}
}

func (b *benchOrchestrator) providers() (context.Context, BenchModelsFn, BenchPayloadAuthorFn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx, b.models, b.author
}

func (r *benchRunner) kick() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *benchRunner) shutdown() {
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.stopped
}

// next pops the earliest task that is due now, or reports how long to
// wait for the next one. Returns (nil, 0, false) when the queue is empty.
func (r *benchRunner) next(now time.Time) (task *benchTask, wait time.Duration, any bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queue) == 0 {
		return nil, 0, false
	}
	soonest := time.Duration(-1)
	for i, t := range r.queue {
		if !t.readyAt.After(now) {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			r.current = t
			return t, 0, true
		}
		if d := t.readyAt.Sub(now); soonest < 0 || d < soonest {
			soonest = d
		}
	}
	return nil, soonest, true
}

func (r *benchRunner) finish() {
	r.mu.Lock()
	r.current = nil
	r.mu.Unlock()
}

// requeue puts a transiently-failed task back with its backoff applied.
func (r *benchRunner) requeue(t *benchTask, delay time.Duration) {
	t.readyAt = time.Now().Add(delay)
	r.mu.Lock()
	r.queue = append(r.queue, t)
	r.mu.Unlock()
	r.kick()
}

// --- Execution ---

// run is one worker's bench loop: pop a due task, measure, repeat. Exits
// on shutdown or context cancellation; nothing else owns this goroutine.
func (b *benchOrchestrator) run(ctx context.Context, r *benchRunner) {
	defer close(r.stopped)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		task, wait, any := r.next(time.Now())
		switch {
		case task != nil:
			// A bench occupies the worker exclusively, so the Queue tab
			// renders it as a unit of running work — tell it when one
			// starts and when it concludes.
			b.s.broadcastQueueChange()
			b.runOne(ctx, r, task)
			r.finish()
			b.s.broadcastQueueChange()
			continue
		case !any:
			wait = time.Hour // nothing queued; wait for a kick
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-r.wake:
		case <-timer.C:
		}
	}
}

// runOne measures a single triple: author the payload, hold the worker's
// dispatch gate, clear the device set, send the bench, record the answer.
func (b *benchOrchestrator) runOne(ctx context.Context, r *benchRunner, t *benchTask) {
	sw := b.s.streamWorker(r.workerID)
	if sw == nil {
		return // gone; the reconnect sweep re-queues
	}
	_, _, author := b.providers()
	if author == nil {
		return
	}

	payload, err := author(ctx, t.runtimeName, t.model.ID)
	switch {
	case errors.Is(err, ErrBenchModelGone):
		// The model isn't in the catalogue yet — almost always a
		// download still landing. Free retry: this says nothing about
		// whether the worker can run it.
		b.s.logger.Debug().Str("worker", r.workerID).Str("model", t.model.Key).Msg("bench: model not ready; retrying")
		r.requeue(t, benchBackoff[0])
		return
	case err != nil:
		b.s.logger.Warn().Err(err).Str("worker", r.workerID).Str("model", t.model.Key).Msg("bench: authoring payload")
		b.retryOrRecord(r, t, err)
		return
	case payload.Cost <= 0:
		b.recordIncapable(t, r.workerID, fmt.Errorf("gateway authored a bench payload with cost %v", payload.Cost))
		return
	}

	release, ok := b.s.holdBenchGate(ctx, sw)
	if !ok {
		// The worker stopped draining (disconnect, shutdown). No row,
		// no attempt spent: reconnect re-queues.
		return
	}
	defer release()

	if !b.clearDeviceSet(sw) {
		r.requeue(t, benchBackoff[0])
		return
	}

	res, benchErr := sw.BenchModel(ctx, worker.ModelBenchmarkRequest{
		ModelID:   t.model.Key,
		Files:     payload.Files,
		LoadHints: payload.LoadHints,
		Payload:   payload.Payload,
		Cost:      payload.Cost,
	})
	switch {
	case errors.Is(benchErr, worker.ErrBenchIncapable):
		b.recordIncapable(t, r.workerID, benchErr)
	case errors.Is(benchErr, worker.ErrWorkerOffline), errors.Is(benchErr, context.Canceled):
		// No row: the reconnect sweep re-queues from scratch.
		b.s.logger.Info().Str("worker", r.workerID).Str("model", t.model.Key).Msg("bench: worker went away mid-benchmark")
	case benchErr != nil:
		b.retryOrRecord(r, t, benchErr)
	case res.ElapsedSecs <= 0:
		b.retryOrRecord(r, t, fmt.Errorf("worker reported elapsed_secs %v", res.ElapsedSecs))
	default:
		b.recordMeasurement(t, r.workerID, payload.Cost, res)
	}
}

// clearDeviceSet evicts every idle resident occupying the device set the
// bench will load onto. A measurement taken beside another model is
// contention, not capability. Reports false when something is still
// serving traffic or an unload failed — the caller retries later.
func (b *benchOrchestrator) clearDeviceSet(sw *worker.StreamWorker) bool {
	// targetModelID is empty: the bench is not "this model's" load yet,
	// so every resident on the predicted set is a blocker.
	blockers := residentsBlockingLoad(sw, "", b.s.predictDeviceSet(sw))
	for _, lm := range blockers {
		if lm.Active > 0 || b.s.workerHasInflightForModel(sw.ID(), lm.ModelID) {
			b.s.logger.Debug().Str("worker", sw.ID()).Str("blocked_by", lm.ModelID).Msg("bench: resident still busy; deferring")
			return false
		}
	}
	for _, lm := range blockers {
		if n, err := b.s.EvictModel(sw.RuntimeName(), lm.ModelID, sw.ID()); err != nil || n == 0 {
			b.s.logger.Warn().Err(err).Str("worker", sw.ID()).Str("model_id", lm.ModelID).Msg("bench: evicting resident before benchmark")
			return false
		}
	}
	return true
}

// retryOrRecord backs a transient failure off and re-queues it, or —
// once the attempt budget is spent — records the last error as if the
// device set were incapable, so jobs stop waiting on a bench that keeps
// failing.
func (b *benchOrchestrator) retryOrRecord(r *benchRunner, t *benchTask, cause error) {
	t.attempts++
	if t.attempts >= benchMaxAttempts {
		b.recordIncapable(t, r.workerID, fmt.Errorf("%d benchmark attempts failed: %w", t.attempts, cause))
		return
	}
	delay := benchBackoff[min(t.attempts-1, len(benchBackoff)-1)]
	b.s.logger.Info().Err(cause).Str("worker", r.workerID).Str("model", t.model.Key).
		Int("attempt", t.attempts).Dur("retry_in", delay).Msg("bench: transient failure; retrying")
	r.requeue(t, delay)
}

func (b *benchOrchestrator) recordMeasurement(t *benchTask, workerID string, cost float64, res worker.ModelBenchmarkResult) {
	st := b.s.benchStore()
	if st == nil {
		return
	}
	size, mtime := b.s.modelFileIdentity(t.model.Key)
	row := store.ModelBenchmarkRow{
		WorkerID:     workerID,
		DeviceSet:    t.deviceSet,
		ModelID:      t.model.Key,
		UnitsPerSec:  cost / res.ElapsedSecs,
		GraphSecs:    res.GraphSecs,
		BaseBytes:    res.BaseBytes,
		PerSlotBytes: res.PerSlotBytes,
		ModelSize:    size,
		ModelMTime:   mtime,
	}
	if err := st.SaveModelBenchmark(row); err != nil {
		b.s.logger.Warn().Err(err).Str("worker", workerID).Str("model", t.model.Key).Msg("bench: saving measurement")
		return
	}
	b.s.logger.Info().Str("worker", workerID).Str("device_set", t.deviceSet).Str("model", t.model.Key).
		Float64("units_per_sec", row.UnitsPerSec).Float64("graph_secs", row.GraphSecs).Msg("model benchmarked")
	b.s.InvalidateModelBenchmarks(workerID)
	b.s.kick()
}

func (b *benchOrchestrator) recordIncapable(t *benchTask, workerID string, cause error) {
	st := b.s.benchStore()
	if st == nil {
		return
	}
	size, mtime := b.s.modelFileIdentity(t.model.Key)
	if err := st.SaveModelBenchmarkError(store.ModelBenchmarkRow{
		WorkerID:   workerID,
		DeviceSet:  t.deviceSet,
		ModelID:    t.model.Key,
		ModelSize:  size,
		ModelMTime: mtime,
		Error:      cause.Error(),
	}); err != nil {
		b.s.logger.Warn().Err(err).Str("worker", workerID).Str("model", t.model.Key).Msg("bench: saving incapable verdict")
		return
	}
	b.s.logger.Warn().Err(cause).Str("worker", workerID).Str("device_set", t.deviceSet).Str("model", t.model.Key).
		Msg("model benchmark concluded incapable")
	b.s.InvalidateModelBenchmarks(workerID)
	b.s.kick()
}

// modelFileIdentity reads the size and mtime of the model's primary file
// so the row records what it was measured against. Zeroes when MASS has
// no models root wired or the file can't be read — the validity check
// then trivially passes, which is the pre-existing behaviour for a
// scheduler without a models dir.
func (s *Scheduler) modelFileIdentity(modelKey string) (size, mtime int64) {
	s.workerEnabledMu.RLock()
	dir := s.modelsDir
	s.workerEnabledMu.RUnlock()
	if dir == "" || modelKey == "" {
		return 0, 0
	}
	info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(modelKey)))
	if err != nil {
		return 0, 0
	}
	return info.Size(), info.ModTime().Unix()
}

// streamWorker returns the online stream worker with this id, or nil.
func (s *Scheduler) streamWorker(workerID string) *worker.StreamWorker {
	if s.workers == nil {
		return nil
	}
	sw, ok := s.workers.Get(workerID).(*worker.StreamWorker)
	if !ok || sw == nil || !sw.Status().Online {
		return nil
	}
	return sw
}

// --- Dispatch gate ---

// benchGateDrainPoll is how often the gate re-checks whether the worker's
// in-flight jobs have finished. Short enough that a bench starts promptly
// after the last job, long enough not to spin.
const benchGateDrainPoll = 100 * time.Millisecond

// holdBenchGate stops new dispatch to sw and waits for its in-flight jobs
// to finish, so the benchmark measures an otherwise-idle device set.
// Returns a release func and true once the worker is quiet; false when
// the wait was abandoned (context cancelled, worker gone) — the caller
// must not bench in that case.
//
// The gate only blocks DISPATCH, not placement: rows may keep landing on
// the worker's queue and go out the moment the gate lifts.
func (s *Scheduler) holdBenchGate(ctx context.Context, sw *worker.StreamWorker) (release func(), ok bool) {
	workerID := sw.ID()
	s.benchGateMu.Lock()
	if _, held := s.benchGated[workerID]; held {
		s.benchGateMu.Unlock()
		return nil, false // one bench at a time per worker
	}
	s.benchGated[workerID] = struct{}{}
	s.benchGateMu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() {
			s.benchGateMu.Lock()
			delete(s.benchGated, workerID)
			s.benchGateMu.Unlock()
			s.kick() // the worker can take work again
		})
	}

	ticker := time.NewTicker(benchGateDrainPoll)
	defer ticker.Stop()
	for s.inflightCountForWorker(workerID) > 0 {
		select {
		case <-ctx.Done():
			release()
			return nil, false
		case <-ticker.C:
			if s.streamWorker(workerID) == nil {
				release()
				return nil, false
			}
		}
	}
	return release, true
}

// benchGateHeld reports whether a benchmark currently owns workerID. The
// dispatcher checks it before draining that worker's queue.
func (s *Scheduler) benchGateHeld(workerID string) bool {
	s.benchGateMu.Lock()
	defer s.benchGateMu.Unlock()
	_, held := s.benchGated[workerID]
	return held
}
