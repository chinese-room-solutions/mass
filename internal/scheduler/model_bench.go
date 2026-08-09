package scheduler

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
)

// Per-model benchmark lookup — the scheduler's only source of compute
// estimates, pool sizing, and memory figures.
//
// A row is keyed by (worker, device set, model). The model key is the
// store-relative cache key of the load's PRIMARY artifact
// ([workerpb.ModelFile.Filename]): the one identity every party already
// agrees on — the gateway stamps it on Submit and on the bench payload,
// the worker mirrors it as its cache path, and MASS holds the file at
// that path under its models root. The gateway's own model_id is
// deliberately NOT used: it folds load-time config into a hash, so the
// same file under two configs would need two benches while measuring
// (almost) the same thing.

// modelBenchEntry is one cached lookup: the row and whether it is usable
// for placement. A miss is cached as {ok: false} so repeat misses stay
// cheap while a bench is still running.
type modelBenchEntry struct {
	row store.ModelBenchmarkRow
	ok  bool
}

// modelBenchCache fronts [StateStoreInterface.GetModelBenchmark] for the
// hot scoring path (the 200ms pending-retry loop re-scores every
// unplaceable row against every candidate). Entries are dropped whenever
// a bench result lands, a model's files change, or a worker disconnects.
type modelBenchCache struct {
	mu      sync.RWMutex
	entries map[string]map[string]modelBenchEntry // workerID → "<device set>|<model key>"
}

func newModelBenchCache() *modelBenchCache {
	return &modelBenchCache{entries: make(map[string]map[string]modelBenchEntry)}
}

func (c *modelBenchCache) get(workerID, key string) (modelBenchEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[workerID][key]
	return e, ok
}

func (c *modelBenchCache) put(workerID, key string, e modelBenchEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[workerID] == nil {
		c.entries[workerID] = make(map[string]modelBenchEntry)
	}
	c.entries[workerID][key] = e
}

func (c *modelBenchCache) dropWorker(workerID string) {
	c.mu.Lock()
	delete(c.entries, workerID)
	c.mu.Unlock()
}

func (c *modelBenchCache) dropAll() {
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

// InvalidateModelBenchmarks drops every cached model-benchmark lookup for
// workerID, or the whole cache when workerID is empty. Called after a
// bench result lands, after a model's rows are deleted, and on worker
// disconnect.
func (s *Scheduler) InvalidateModelBenchmarks(workerID string) {
	if workerID == "" {
		s.modelBench.dropAll()
		return
	}
	s.modelBench.dropWorker(workerID)
}

// deviceSetKey renders a predicted device set as the row's device_set
// column: the canonical comma-joined list. [Scheduler.predictDeviceSet]
// already returns it sorted.
func deviceSetKey(set []string) string { return strings.Join(set, ",") }

// benchModelKey returns the store-relative cache key identifying the
// model a load's artifacts describe: the PRIMARY file's filename, or the
// first artifact when no role is tagged. Empty when there are no
// artifacts — such an envelope can never be matched to a bench row.
func benchModelKey(files []*workerpb.ModelFile) string {
	var first string
	for _, f := range files {
		if f == nil || f.GetFilename() == "" {
			continue
		}
		if f.GetRole() == workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY {
			return f.GetFilename()
		}
		if first == "" {
			first = f.GetFilename()
		}
	}
	return first
}

// modelBenchmark returns the row that makes w a candidate for env: the
// measurement for env's model on w's CURRENT predicted device set.
// ok is false — w is not a candidate — when w has no usable device set,
// when the bench hasn't concluded there (no row), when it concluded
// INCAPABLE (error set), when the measurement is unusable
// (units_per_sec <= 0), or when the model's file no longer matches the
// one the row was measured against. An envelope carrying no load
// artifact has an empty model key, which no row is ever written under,
// so it lands on the same "no row" answer.
func (s *Scheduler) modelBenchmark(w *worker.StreamWorker, env queue.Envelope) (store.ModelBenchmarkRow, bool) {
	set := s.predictDeviceSet(w)
	if len(set) == 0 {
		return store.ModelBenchmarkRow{}, false
	}
	row, ok := s.lookupModelBenchmark(w.ID(), deviceSetKey(set), benchModelKey(env.Files))
	if !ok || !s.modelFileUnchanged(row) {
		return store.ModelBenchmarkRow{}, false
	}
	return row, true
}

// SetModelsDir wires MASS's models root — the directory the store-
// relative model keys resolve under. Needed to check a row against the
// file it was measured on. Left empty (tests, early init) the check is
// skipped and rows are trusted.
func (s *Scheduler) SetModelsDir(dir string) {
	s.workerEnabledMu.Lock()
	s.modelsDir = dir
	s.workerEnabledMu.Unlock()
}

// modelFileUnchanged reports whether the model file behind row still has
// the size and mtime the bench measured. A changed file means the
// measurement describes different weights, so every row of that model is
// treated as absent until a re-bench lands. A file MASS can no longer
// stat counts as changed — nothing can be dispatched against it anyway.
func (s *Scheduler) modelFileUnchanged(row store.ModelBenchmarkRow) bool {
	s.workerEnabledMu.RLock()
	dir := s.modelsDir
	s.workerEnabledMu.RUnlock()
	if dir == "" || row.ModelID == "" {
		return true
	}
	info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(row.ModelID)))
	if err != nil {
		return false
	}
	return info.Size() == row.ModelSize && info.ModTime().Unix() == row.ModelMTime
}

// modelBenchConclusion reports how the benches for env's model stand
// across every worker that could run it.
//
// pending is true while at least one eligible worker might still become
// a candidate: its bench hasn't concluded, or it concluded usable (the
// job is merely blocked on memory or files right now). errText carries
// the last recorded incapable verdict, which the caller surfaces when
// nothing is pending — every eligible worker has answered "no".
//
// An ineligible worker — offline, operator-disabled, or with every
// device disabled — is not consulted: it cannot conclude, so waiting on
// it would wedge the job forever.
func (s *Scheduler) modelBenchConclusion(env queue.Envelope) (pending bool, errText string) {
	modelKey := benchModelKey(env.Files)
	if modelKey == "" {
		return true, ""
	}
	eligible := 0
	for _, w := range s.WorkersForRuntime(env.RuntimeName) {
		set := s.predictDeviceSet(w)
		if len(set) == 0 {
			continue
		}
		eligible++
		row, ok := s.lookupModelBenchmark(w.ID(), deviceSetKey(set), modelKey)
		switch {
		case ok && s.modelFileUnchanged(row):
			return true, ""
		case row.Error != "" && s.modelFileUnchanged(row):
			errText = row.Error
		default:
			return true, "" // no row yet, or a stale one: a bench is owed
		}
	}
	// No eligible worker at all: nothing has concluded, so the job waits
	// for one to appear rather than failing on an empty fleet.
	return eligible == 0, errText
}

// lookupModelBenchmark is modelBenchmark's cache-and-store half, split
// out so the bench orchestrator can ask about an explicit triple.
func (s *Scheduler) lookupModelBenchmark(workerID, devSet, modelKey string) (store.ModelBenchmarkRow, bool) {
	cacheKey := devSet + "|" + modelKey
	if e, hit := s.modelBench.get(workerID, cacheKey); hit {
		return e.row, e.ok
	}

	s.queueMu.RLock()
	st := s.store
	s.queueMu.RUnlock()
	if st == nil {
		return store.ModelBenchmarkRow{}, false
	}

	row, err := st.GetModelBenchmark(workerID, devSet, modelKey)
	e := modelBenchEntry{row: row, ok: err == nil && row.Error == "" && row.UnitsPerSec > 0}
	if err != nil {
		e.row = store.ModelBenchmarkRow{}
	}
	s.modelBench.put(workerID, cacheKey, e)
	return e.row, e.ok
}
