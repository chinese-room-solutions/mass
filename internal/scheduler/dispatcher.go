package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
)

const (
	// globalExtendDuration is how long to extend a global message's visibility
	// after dispatching to a device queue. The device queue worker will further
	// extend it periodically during execution.
	globalExtendDuration = 60 * time.Second

	// deviceExtendDuration is how long the device queue worker extends its own
	// in-progress message's visibility on each heartbeat. Inference can run
	// well past the device queue's default visibility (30s); without this
	// extension, goqite redelivers the message and a duplicate executes in
	// parallel on the same worker pool.
	deviceExtendDuration = 60 * time.Second

	// heartbeatIntervalDefault is the production heartbeat interval used to
	// extend both the global message and the device-queue message while
	// executing. Must be well under {global,device}ExtendDuration.
	heartbeatIntervalDefault = 15 * time.Second
)

// heartbeatInterval is the live ticker interval used by heartbeatLease. It
// is a var (not a const) so tests can speed it up via heartbeatIntervalForTesting.
var heartbeatInterval = heartbeatIntervalDefault

// heartbeatIntervalForTesting overrides heartbeatInterval for the duration of
// a test. Returns the previous value so the test can restore it. Not safe to
// call concurrently with running heartbeats.
func heartbeatIntervalForTesting(d time.Duration) time.Duration {
	prev := heartbeatInterval
	heartbeatInterval = d
	return prev
}

// Dispatcher reads tasks from the global queue and routes them to device
// queues by placement scoring (model affinity, device power, VRAM fit).
//
// Owns the in-memory device-queue registry. Worker connect/disconnect
// mutates it via [Dispatcher.Add]/[Dispatcher.Remove]; reads go through
// [Dispatcher.Get], [Dispatcher.All], or [Dispatcher.collectCandidates]
// (which resolves the manager pointer under the same lock as placement
// state to avoid races with disconnect).
type Dispatcher struct {
	globalQueue queue.QueueInterface
	results     queue.ResultStoreInterface
	stateStore  store.DeviceQueueStateStoreInterface
	benchStore  store.BenchmarkStoreInterface
	workers     *worker.Fleet
	pool        *modelPool
	logger      zerolog.Logger

	// dqMu guards deviceQueues. Reads are short, writes happen on
	// worker connect/disconnect (rare); RWMutex is the right shape.
	dqMu         sync.RWMutex
	deviceQueues map[string]*DeviceQueueManager
}

// DispatcherOpts configures a new Dispatcher.
type DispatcherOpts struct {
	GlobalQueue queue.QueueInterface
	Results     queue.ResultStoreInterface
	StateStore  store.DeviceQueueStateStoreInterface
	BenchStore  store.BenchmarkStoreInterface
	Workers     *worker.Fleet
	Pool        *modelPool
	Logger      zerolog.Logger
}

// NewDispatcher creates a dispatcher with an empty registry. Callers
// populate it via [Dispatcher.Add] — this lets the dispatcher pointer be
// available for managers to attach to before they're registered.
func NewDispatcher(opts DispatcherOpts) *Dispatcher {
	return &Dispatcher{
		globalQueue:  opts.GlobalQueue,
		results:      opts.Results,
		stateStore:   opts.StateStore,
		benchStore:   opts.BenchStore,
		workers:      opts.Workers,
		pool:         opts.Pool,
		logger:       opts.Logger,
		deviceQueues: make(map[string]*DeviceQueueManager),
	}
}

// Add registers a device queue manager. Returns the existing manager and
// false if name was already registered; otherwise returns the supplied
// manager and true.
func (d *Dispatcher) Add(name string, dq *DeviceQueueManager) (*DeviceQueueManager, bool) {
	d.dqMu.Lock()
	defer d.dqMu.Unlock()
	if existing, ok := d.deviceQueues[name]; ok {
		return existing, false
	}
	d.deviceQueues[name] = dq
	return dq, true
}

// Remove drops a manager from the registry and returns it, or nil if not
// registered. Caller drains the returned manager outside the lock.
func (d *Dispatcher) Remove(name string) *DeviceQueueManager {
	d.dqMu.Lock()
	defer d.dqMu.Unlock()
	dq, ok := d.deviceQueues[name]
	if !ok {
		return nil
	}
	delete(d.deviceQueues, name)
	return dq
}

// Get returns the manager for a name, or nil if not registered.
func (d *Dispatcher) Get(name string) *DeviceQueueManager {
	d.dqMu.RLock()
	defer d.dqMu.RUnlock()
	return d.deviceQueues[name]
}

// All returns a snapshot — safe to range after the call returns even
// while the underlying map is mutated.
func (d *Dispatcher) All() []*DeviceQueueManager {
	d.dqMu.RLock()
	defer d.dqMu.RUnlock()
	out := make([]*DeviceQueueManager, 0, len(d.deviceQueues))
	for _, dq := range d.deviceQueues {
		out = append(out, dq)
	}
	return out
}

// Count returns the number of registered managers.
func (d *Dispatcher) Count() int {
	d.dqMu.RLock()
	defer d.dqMu.RUnlock()
	return len(d.deviceQueues)
}

// Run starts the dispatcher loop. Blocks until ctx is cancelled.
//
// Event-driven: Peeks the global queue head only when woken by a new
// submit (NotifyCh) or a safety tick. The head routes to the best-scoring
// enabled device queue; only sits untouched when no candidate exists (no
// online workers, all queues disabled). Peek consumes no Receive count
// and starts no visibility timer, so a stuck head doesn't burn retries.
func (d *Dispatcher) Run(ctx context.Context) {
	d.logger.Info().Msg("dispatcher started")
	defer d.logger.Info().Msg("dispatcher stopped")

	fallback := time.NewTicker(fallbackInterval)
	defer fallback.Stop()

	for {
		for ctx.Err() == nil {
			if !d.dispatchOne(ctx) {
				break
			}
		}
		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-d.globalQueue.NotifyCh():
		case <-fallback.C:
		}
	}
}

// dispatchOne peeks the global head and routes it to the best device queue.
// Returns true if a message was acted on (routed, deleted, or skipped),
// false if the queue is empty or the head can't be placed (caller waits).
func (d *Dispatcher) dispatchOne(ctx context.Context) bool {
	peeked, err := d.globalQueue.Peek(ctx, 1)
	if err != nil {
		d.logger.Error().Err(err).Msg("peeking global queue")
		return false
	}
	if len(peeked) == 0 {
		return false
	}
	head := peeked[0]

	env, err := queue.UnmarshalEnvelope(head.Body)
	if err != nil {
		// Bad envelope — consume so it doesn't block subsequent messages.
		d.logger.Error().Err(err).Str("msg_id", string(head.ID)).Msg("unmarshalling envelope at global head")
		if got, _ := d.globalQueue.ReceiveByID(ctx, head.ID); got != nil {
			if dErr := d.globalQueue.Delete(ctx, got.ID); dErr != nil {
				d.logger.Error().Err(dErr).Str("msg_id", string(got.ID)).Msg("deleting bad global envelope")
			}
		}
		return true
	}

	if env.RequestID == "" {
		env.RequestID = string(head.ID)
	}

	// Idempotency: result already done — just delete.
	if r, err := d.results.Get(env.RequestID); err == nil && r != nil &&
		(r.Status == queue.ResultStatusDone || r.Status == queue.ResultStatusError) {
		if got, _ := d.globalQueue.ReceiveByID(ctx, head.ID); got != nil {
			if dErr := d.globalQueue.Delete(ctx, got.ID); dErr != nil {
				d.logger.Error().Err(dErr).Str("msg_id", string(got.ID)).Msg("deleting completed global msg")
			}
		}
		d.logger.Debug().Str("request_id", env.RequestID).Msg("skipping head: result already completed")
		return true
	}

	bs := batchSize(env.Type, env.Payload)
	slots := int(d.pool.modelMaxConcurrent(env.Fingerprint))
	taskDifficulty := envelopeDifficulty(len(env.Payload), env.ModelSizeBytes, bs, slots, isEmbeddingBatch(env.Type))
	candidate := d.selectPlacement(env.Fingerprint, taskDifficulty, env.ModelSizeBytes)
	if candidate == nil {
		d.logger.Debug().Str("fingerprint", env.Fingerprint).Msg("no device available for head — head stays in queue")
		return false
	}

	// Commit the handoff in one transaction: lease the global row (so it
	// stays in DB for Extend/Delete/ReleaseLeaseAndDelete) AND insert the
	// device row. Atomic — crash between can't leave one without the other.
	//
	// candidate.Manager was resolved under the dispatcher lock, so it can't
	// have been deregistered. If deregister happens between LeaseAndSubmit
	// and the device queue's first Receive, the device queue's DrainToGlobal
	// releases the lease back to global for re-dispatch.
	env.GlobalMsgID = string(head.ID)
	_, leased, err := d.globalQueue.LeaseAndSubmit(ctx, head.ID, globalExtendDuration, candidate.Manager.queue, env, env.Priority)
	if err != nil {
		d.logger.Error().Err(err).Str("queue", candidate.QueueName).Msg("lease-and-submit failed")
		return false
	}
	if !leased {
		// Race-loser: another consumer leased or consumed head.ID between
		// our Peek and the transaction. Move on.
		return true
	}

	// The task is now this device queue's tail. Bump the running difficulty
	// sum and stamp the new tail fingerprint so future placement scoring
	// sees an up-to-date snapshot. Best-effort: the lease/insert already
	// committed, and the running sum self-corrects on dequeue, so a stale
	// state row only mildly distorts later score estimates until the task
	// finishes.
	if err := d.stateStore.UpdateTail(candidate.QueueName, env.Fingerprint, candidate.TailDifficulty+taskDifficulty); err != nil {
		d.logger.Warn().Err(err).Str("queue", candidate.QueueName).Msg("updating tail after dispatch")
	}

	d.logger.Debug().
		Str("fingerprint", env.Fingerprint).
		Str("device_queue", candidate.QueueName).
		Float64("tail_difficulty", candidate.TailDifficulty+taskDifficulty).
		Msg("task dispatched to device queue")
	return true
}

// selectPlacement finds the device queue with the lowest [ScoreCost] for
// this task: queue wait + exec time + (swap cost if FP doesn't match).
func (d *Dispatcher) selectPlacement(fingerprint string, taskDifficulty float64, modelSizeBytes uint64) *Candidate {
	candidates := d.collectCandidates(func(_, loadedHash string) bool { return true })
	return SelectMinCost(candidates, fingerprint, taskDifficulty, modelSizeBytes)
}

// selectAvailablePlacement is selectPlacement but excludes devices already
// running a different model. Used by LoadModel: manual loads pick an
// unoccupied (or already-matching) device.
//
// Zero difficulty/size because there's no task being enqueued — the cost
// model collapses to "lightest queue on the strongest device."
func (d *Dispatcher) selectAvailablePlacement(fingerprint string) *Candidate {
	candidates := d.collectCandidates(func(_, loadedHash string) bool {
		return loadedHash == "" || loadedHash == fingerprint
	})
	return SelectMinCost(candidates, fingerprint, 0, 0)
}

// collectCandidates returns every enabled device queue on an online worker
// that passes filter(queueName, loadedHash). Each Candidate's Manager
// pointer is resolved under the dispatcher lock so a later worker
// disconnect can't invalidate the result; state-store and benchmark-store
// I/O happen after the lock is released.
func (d *Dispatcher) collectCandidates(filter func(queueName, loadedHash string) bool) []Candidate {
	var devices []DeviceInfo
	for _, ag := range d.workers.All() {
		if !ag.Status().Online {
			continue
		}
		for _, dev := range ag.Devices() {
			devices = append(devices, DeviceInfo{
				WorkerID:      ag.ID(),
				DeviceID:      dev.ID,
				TotalMemoryMB: dev.TotalMemoryMB,
				GFlops:        d.getDeviceGFlops(ag.ID(), dev.ID),
			})
		}
	}
	if len(devices) == 0 {
		return nil
	}

	queueStates, err := d.stateStore.ListDeviceQueueStates()
	if err != nil {
		d.logger.Error().Err(err).Msg("listing device queue states")
		queueStates = nil
	}

	onlineWorkers := make(map[string]bool, len(devices))
	for _, dev := range devices {
		onlineWorkers[dev.WorkerID] = true
	}

	managers := d.All()

	var candidates []Candidate
	for _, dq := range managers {
		if !onlineWorkers[dq.workerID] {
			continue
		}
		st := d.getQueueState(queueStates, dq.queueName)
		if !st.Enabled {
			continue
		}
		if !filter(dq.queueName, st.LoadedHash) {
			continue
		}
		candidates = append(candidates, Candidate{
			WorkerID:       dq.workerID,
			DeviceIDs:      dq.deviceIDs,
			QueueName:      dq.queueName,
			GFlops:         d.getDeviceGroupGFlops(dq.workerID, dq.deviceIDs),
			TotalMemoryMB:  d.getDeviceGroupMemory(dq.workerID, dq.deviceIDs, devices),
			TailHash:       st.TailHash,
			TailDifficulty: st.TailDifficulty,
			LoadedHash:     st.LoadedHash,
			Manager:        dq,
		})
	}
	return candidates
}

// rankedQueue is a device queue annotated with its GFlops and memory for sorting.
type rankedQueue struct {
	dq     *DeviceQueueManager
	gflops float64
	memMB  int
}

// rankedQueues returns device queues sorted by GFlops descending, optionally filtered to local workers.
func (d *Dispatcher) rankedQueues(localOnly bool) []rankedQueue {
	var devices []DeviceInfo
	for _, ag := range d.workers.All() {
		if !ag.Status().Online {
			continue
		}
		if localOnly && !d.workers.IsLocal(ag.ID()) {
			continue
		}
		for _, dev := range ag.Devices() {
			devices = append(devices, DeviceInfo{
				WorkerID:      ag.ID(),
				DeviceID:      dev.ID,
				TotalMemoryMB: dev.TotalMemoryMB,
				GFlops:        d.getDeviceGFlops(ag.ID(), dev.ID),
			})
		}
	}

	queueStates, _ := d.stateStore.ListDeviceQueueStates()

	var ranked []rankedQueue
	for _, dq := range d.All() {
		if localOnly && !d.workers.IsLocal(dq.workerID) {
			continue
		}
		ag := d.workers.Get(dq.workerID)
		if ag == nil || !ag.Status().Online {
			continue
		}
		if !d.getQueueState(queueStates, dq.queueName).Enabled {
			continue
		}
		ranked = append(ranked, rankedQueue{
			dq:     dq,
			gflops: d.getDeviceGroupGFlops(dq.workerID, dq.deviceIDs),
			memMB:  d.getDeviceGroupMemory(dq.workerID, dq.deviceIDs, devices),
		})
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].gflops > ranked[j].gflops
	})
	return ranked
}

// selectBestFreePlacement finds the strongest free (unoccupied or same-model) device.
// If localOnly is true, only local worker devices are considered.
// Does NOT evict — only returns a candidate if the device is already free.
func (d *Dispatcher) selectBestFreePlacement(fingerprint string, localOnly bool) *Candidate {
	ranked := d.rankedQueues(localOnly)
	if len(ranked) == 0 {
		return nil
	}

	queueStates, _ := d.stateStore.ListDeviceQueueStates()

	for _, r := range ranked {
		st := d.getQueueState(queueStates, r.dq.queueName)

		// Already has our model — use it directly.
		if st.LoadedHash == fingerprint {
			return &Candidate{
				WorkerID:      r.dq.workerID,
				DeviceIDs:     r.dq.deviceIDs,
				QueueName:     r.dq.queueName,
				GFlops:        r.gflops,
				TotalMemoryMB: r.memMB,
				LoadedHash:    st.LoadedHash,
			}
		}

		// Device is free — use it.
		if st.LoadedHash == "" {
			return &Candidate{
				WorkerID:      r.dq.workerID,
				DeviceIDs:     r.dq.deviceIDs,
				QueueName:     r.dq.queueName,
				GFlops:        r.gflops,
				TotalMemoryMB: r.memMB,
			}
		}
	}

	return nil
}

// selectBestEvictablePlacement finds the strongest device across all workers
// that can be freed by evicting an idle model. Used as a last resort when
// no free device is available anywhere.
func (d *Dispatcher) selectBestEvictablePlacement(fingerprint string) *Candidate {
	ranked := d.rankedQueues(false)

	for _, r := range ranked {
		depth, err := r.dq.queue.Depth(context.Background())
		if err != nil || depth > 0 {
			continue
		}
		idle := d.pool.IdleInstancesOnDevice(r.dq.workerID, r.dq.deviceIDs)
		for _, inst := range idle {
			if inst.Fingerprint == fingerprint {
				continue
			}
			if d.pool.Evict(inst.Fingerprint) {
				if uErr := d.stateStore.UpdateLoadedHash(r.dq.queueName, ""); uErr != nil {
					d.logger.Warn().Err(uErr).Str("queue", r.dq.queueName).Msg("clearing loaded_hash after eviction")
				}
				d.logger.Info().
					Str("fingerprint", inst.Fingerprint).
					Str("path", inst.ModelPath).
					Str("agent", r.dq.workerID).
					Str("queue", r.dq.queueName).
					Msg("evicted idle model for best-device placement")
				return &Candidate{
					WorkerID:      r.dq.workerID,
					DeviceIDs:     r.dq.deviceIDs,
					QueueName:     r.dq.queueName,
					GFlops:        r.gflops,
					TotalMemoryMB: r.memMB,
				}
			}
		}
	}

	return nil
}

// getDeviceGFlops returns the benchmark GFlops for a device, or 0 if not benchmarked.
func (d *Dispatcher) getDeviceGFlops(workerID, deviceID string) float64 {
	row, err := d.benchStore.GetBenchmark(workerID, deviceID)
	if err != nil {
		return 0
	}
	return row.ComputeGFlops
}

// getDeviceGroupGFlops returns the minimum GFlops across a device group.
func (d *Dispatcher) getDeviceGroupGFlops(workerID string, deviceIDs []string) float64 {
	minGFlops := 0.0
	for i, did := range deviceIDs {
		g := d.getDeviceGFlops(workerID, did)
		if i == 0 || g < minGFlops {
			minGFlops = g
		}
	}
	return minGFlops
}

// getDeviceGroupMemory returns the total memory across a device group.
func (d *Dispatcher) getDeviceGroupMemory(workerID string, deviceIDs []string, devices []DeviceInfo) int {
	total := 0
	for _, did := range deviceIDs {
		for _, dev := range devices {
			if dev.WorkerID == workerID && dev.DeviceID == did {
				total += dev.TotalMemoryMB
				break
			}
		}
	}
	return total
}

// getQueueState finds the state for a queue name from a pre-fetched list.
func (d *Dispatcher) getQueueState(states []store.DeviceQueueState, queueName string) store.DeviceQueueState {
	for _, s := range states {
		if s.QueueName == queueName {
			return s
		}
	}
	return store.DeviceQueueState{Enabled: true}
}
