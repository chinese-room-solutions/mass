package worker

import (
	"fmt"
	"sync"
)

// FleetChangeKind describes what changed in the worker fleet.
type FleetChangeKind int

const (
	FleetChangeAdded FleetChangeKind = iota
	FleetChangeRemoved
	FleetChangeUpdated
)

// FleetChangeEvent describes a change in the worker fleet.
type FleetChangeEvent struct {
	Kind     FleetChangeKind
	WorkerID string
}

// FleetChangeCallback is called when the fleet changes.
type FleetChangeCallback func(evt FleetChangeEvent)

// LoadedChangedCallback fires when a worker's loaded-model set or any
// per-model PoolSize/Active counter actually moves. Distinct from the
// stats-noisy [FleetChangeCallback] so Scheduler-tab subscribers can wake
// up only on real changes instead of every heartbeat tick.
type LoadedChangedCallback func(workerID string)

// Fleet manages the set of currently known workers connected through the [Hub].
type Fleet struct {
	mu              sync.RWMutex
	workers         map[string]WorkerInterface
	order           []string // insertion order for deterministic iteration
	onChange        []FleetChangeCallback
	onLoadedChanged []LoadedChangedCallback
}

// NewFleet creates an empty worker fleet.
func NewFleet() *Fleet {
	return &Fleet{
		workers: make(map[string]WorkerInterface),
	}
}

// Register adds a worker to the fleet.
func (f *Fleet) Register(w WorkerInterface) error {
	f.mu.Lock()
	id := w.ID()
	if _, exists := f.workers[id]; exists {
		f.mu.Unlock()
		return fmt.Errorf("worker %q already registered", id)
	}
	f.workers[id] = w
	f.order = append(f.order, id)
	cbs := f.onChange
	f.mu.Unlock()

	for _, cb := range cbs {
		cb(FleetChangeEvent{Kind: FleetChangeAdded, WorkerID: id})
	}
	return nil
}

// Deregister removes a worker from the fleet.
func (f *Fleet) Deregister(id string) error {
	f.mu.Lock()
	if _, exists := f.workers[id]; !exists {
		f.mu.Unlock()
		return fmt.Errorf("worker %q not found", id)
	}
	delete(f.workers, id)
	for i, oid := range f.order {
		if oid == id {
			f.order = append(f.order[:i], f.order[i+1:]...)
			break
		}
	}
	cbs := f.onChange
	f.mu.Unlock()

	for _, cb := range cbs {
		cb(FleetChangeEvent{Kind: FleetChangeRemoved, WorkerID: id})
	}
	return nil
}

// Get returns the worker with the given ID, or nil if not found.
func (f *Fleet) Get(id string) WorkerInterface {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.workers[id]
}

// All returns all registered workers in insertion order.
func (f *Fleet) All() []WorkerInterface {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make([]WorkerInterface, 0, len(f.order))
	for _, id := range f.order {
		if w, ok := f.workers[id]; ok {
			result = append(result, w)
		}
	}
	return result
}

// SelectWorker returns the first online worker. Returns nil if none available.
func (f *Fleet) SelectWorker() WorkerInterface {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, id := range f.order {
		w := f.workers[id]
		if w.Status().Online {
			return w
		}
	}
	return nil
}

// IsLocal returns true if the worker with the given ID is the local worker.
func (f *Fleet) IsLocal(id string) bool {
	return id == "local"
}

// NotifyUpdate fires change callbacks for a worker stats update.
func (f *Fleet) NotifyUpdate(id string) {
	f.mu.RLock()
	cbs := f.onChange
	f.mu.RUnlock()
	for _, cb := range cbs {
		cb(FleetChangeEvent{Kind: FleetChangeUpdated, WorkerID: id})
	}
}

// AddChangeCallback registers a callback for fleet changes.
func (f *Fleet) AddChangeCallback(cb FleetChangeCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onChange = append(f.onChange, cb)
}

// NotifyLoadedChanged fires every registered loaded-changed callback for
// workerID. Hub calls this when ApplyHeartbeat reports a real change in
// the loaded-model set, and after a successful load/unload.
func (f *Fleet) NotifyLoadedChanged(workerID string) {
	f.mu.RLock()
	cbs := f.onLoadedChanged
	f.mu.RUnlock()
	for _, cb := range cbs {
		cb(workerID)
	}
}

// AddLoadedChangedCallback registers a callback for real changes to any
// worker's loaded-model set / pool counters.
func (f *Fleet) AddLoadedChangedCallback(cb LoadedChangedCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onLoadedChanged = append(f.onLoadedChanged, cb)
}
