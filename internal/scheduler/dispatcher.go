package scheduler

import (
	"context"
	"sort"
	"time"

	"github.com/chinese-room-solutions/mass/internal/agent"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
)

const (
	// globalExtendDuration is how long to extend a global message's visibility
	// after dispatching to a device queue. The device queue worker will further
	// extend it periodically during execution.
	globalExtendDuration = 60 * time.Second

	// heartbeatInterval is how often the device queue worker extends the global
	// message while executing. Must be well under globalExtendDuration to avoid
	// premature redelivery.
	heartbeatInterval = 15 * time.Second
)

// Dispatcher reads tasks from the global queue and dispatches them to device queues
// based on placement scoring (model affinity, device power, VRAM fit).
type Dispatcher struct {
	globalQueue  queue.QueueInterface
	deviceQueues map[string]*DeviceQueueManager // queueName → manager
	results      queue.ResultStoreInterface
	stateStore   store.DeviceQueueStateStoreInterface
	benchStore   store.BenchmarkStoreInterface
	agents       *agent.Registry
	pool         *modelPool
	logger       zerolog.Logger
	pollInterval time.Duration
}

// DispatcherOpts configures a new Dispatcher.
type DispatcherOpts struct {
	GlobalQueue  queue.QueueInterface
	DeviceQueues map[string]*DeviceQueueManager
	Results      queue.ResultStoreInterface
	StateStore   store.DeviceQueueStateStoreInterface
	BenchStore   store.BenchmarkStoreInterface
	Agents       *agent.Registry
	Pool         *modelPool
	Logger       zerolog.Logger
	PollInterval time.Duration
}

// NewDispatcher creates a new dispatcher.
func NewDispatcher(opts DispatcherOpts) *Dispatcher {
	if opts.PollInterval == 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	return &Dispatcher{
		globalQueue:  opts.GlobalQueue,
		deviceQueues: opts.DeviceQueues,
		results:      opts.Results,
		stateStore:   opts.StateStore,
		benchStore:   opts.BenchStore,
		agents:       opts.Agents,
		pool:         opts.Pool,
		logger:       opts.Logger,
		pollInterval: opts.PollInterval,
	}
}

// Run starts the dispatcher loop. Blocks until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	d.logger.Info().Msg("dispatcher started")
	defer d.logger.Info().Msg("dispatcher stopped")

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Drain all available messages from the global queue each tick.
			for ctx.Err() == nil {
				if !d.dispatchOne(ctx) {
					break
				}
			}
		}
	}
}

// dispatchOne reads one task from the global queue and dispatches it to the best device queue.
// Returns true if a message was processed (successfully or not), false if the queue was empty.
func (d *Dispatcher) dispatchOne(ctx context.Context) bool {
	msg, err := d.globalQueue.Receive(ctx)
	if err != nil {
		d.logger.Error().Err(err).Msg("receiving from global queue")
		return false
	}
	if msg == nil {
		return false
	}

	env, err := queue.UnmarshalEnvelope(msg.Body)
	if err != nil {
		d.logger.Error().Err(err).Str("msg_id", string(msg.ID)).Msg("unmarshalling envelope")
		_ = d.globalQueue.Delete(ctx, msg.ID)
		return true
	}

	// Preserve the original request ID for result tracking.
	// The global queue message ID is the one used by results.Create/WaitForResult.
	if env.RequestID == "" {
		env.RequestID = string(msg.ID)
	}

	// Idempotency: if the result was already completed (e.g. redelivery after
	// a worker finished but failed to ack the global message), just delete.
	if env.RequestID != "" {
		if r, err := d.results.Get(env.RequestID); err == nil && r != nil &&
			(r.Status == queue.ResultStatusDone || r.Status == queue.ResultStatusError) {
			_ = d.globalQueue.Delete(ctx, msg.ID)
			d.logger.Debug().Str("request_id", env.RequestID).Msg("skipping redelivered message: result already completed")
			return true
		}
	}

	fp := env.Fingerprint

	// Find the best device queue.
	candidate := d.selectPlacement(fp)
	if candidate == nil {
		// No placement found — try evicting idle models to free resources.
		if d.evictIdleModels(fp) {
			candidate = d.selectPlacement(fp)
		}
	}
	if candidate == nil {
		// Still no placement — re-queue for later retry.
		_ = d.globalQueue.Delete(ctx, msg.ID)
		env.Retries++
		if int(env.Retries) >= queue.MaxRetries {
			d.logger.Error().Str("fingerprint", fp).Int("retries", int(env.Retries)).Msg("task exceeded max retries, dropping")
			if env.RequestID != "" {
				_ = d.results.Fail(env.RequestID, "exceeded max retries: no device available")
			}
			return true
		}
		d.logger.Warn().Str("fingerprint", fp).Int("retries", int(env.Retries)).Msg("no device available for task, re-queuing")
		_, _ = d.globalQueue.SubmitEnvelope(ctx, env, env.Priority)
		return true
	}

	// Submit to the winning device queue.
	dq, ok := d.deviceQueues[candidate.QueueName]
	if !ok {
		d.logger.Error().Str("queue", candidate.QueueName).Msg("device queue not found")
		_ = d.globalQueue.Delete(ctx, msg.ID)
		return true
	}

	// Stamp the global message ID so the device queue worker can ack it on completion.
	env.GlobalMsgID = string(msg.ID)

	// Submit the full envelope (including RequestID and GlobalMsgID) to the device queue.
	_, err = dq.queue.SubmitEnvelope(ctx, env, env.Priority)
	if err != nil {
		d.logger.Error().Err(err).Str("queue", candidate.QueueName).Msg("submitting to device queue")
		// Leave in global queue for retry (visibility timeout will expire and redeliver).
		return true
	}

	// Extend the global message visibility instead of deleting it.
	// The device queue worker will delete it after successful execution.
	// If the worker crashes, the visibility timeout expires and the message
	// auto-redelivers to the global queue for re-dispatch.
	if err := d.globalQueue.Extend(ctx, msg.ID, globalExtendDuration); err != nil {
		d.logger.Error().Err(err).Msg("extending global message after dispatch")
	}

	d.logger.Debug().
		Str("fingerprint", fp).
		Str("device_queue", candidate.QueueName).
		Int("tail_length", candidate.TailLength).
		Msg("task dispatched to device queue")
	return true
}

// selectPlacement finds the best device queue for the given task fingerprint.
func (d *Dispatcher) selectPlacement(fingerprint string) *Candidate {
	// Gather device info from all agents.
	var devices []DeviceInfo
	for _, ag := range d.agents.All() {
		if !ag.Status().Online {
			continue
		}
		for _, dev := range ag.Devices() {
			devices = append(devices, DeviceInfo{
				AgentID:       ag.ID(),
				DeviceID:      dev.ID,
				TotalMemoryMB: dev.TotalMemoryMB,
				GFlops:        d.getDeviceGFlops(ag.ID(), dev.ID),
			})
		}
	}

	if len(devices) == 0 {
		return nil
	}

	// Gather queue states.
	queueStates, err := d.stateStore.ListDeviceQueueStates()
	if err != nil {
		d.logger.Error().Err(err).Msg("listing device queue states")
		queueStates = nil
	}

	// Build a set of online agent IDs for filtering.
	onlineAgents := make(map[string]bool, len(devices))
	for _, dev := range devices {
		onlineAgents[dev.AgentID] = true
	}

	// Only consider device queues belonging to online agents and enabled.
	var candidates []Candidate
	for _, dq := range d.deviceQueues {
		if !onlineAgents[dq.agentID] {
			continue
		}
		st := d.getQueueState(queueStates, dq.queueName)
		if !st.Enabled {
			continue
		}
		candidates = append(candidates, Candidate{
			AgentID:       dq.agentID,
			DeviceIDs:     dq.deviceIDs,
			QueueName:     dq.queueName,
			GFlops:        d.getDeviceGroupGFlops(dq.agentID, dq.deviceIDs),
			TotalMemoryMB: d.getDeviceGroupMemory(dq.agentID, dq.deviceIDs, devices),
			TailHash:      st.TailHash,
			TailLength:    st.TailLength,
			LoadedHash:    st.LoadedHash,
		})
	}

	return SelectBestCandidate(candidates, fingerprint)
}

// selectAvailablePlacement is like selectPlacement but excludes devices that
// already have a different model loaded. Used by LoadModel to enforce one model
// per device — tasks can queue behind a loaded model, but manual loads should
// pick an unoccupied device.
func (d *Dispatcher) selectAvailablePlacement(fingerprint string) *Candidate {
	var devices []DeviceInfo
	for _, ag := range d.agents.All() {
		if !ag.Status().Online {
			continue
		}
		for _, dev := range ag.Devices() {
			devices = append(devices, DeviceInfo{
				AgentID:       ag.ID(),
				DeviceID:      dev.ID,
				TotalMemoryMB: dev.TotalMemoryMB,
				GFlops:        d.getDeviceGFlops(ag.ID(), dev.ID),
			})
		}
	}
	if len(devices) == 0 {
		return nil
	}

	onlineAgents := make(map[string]bool, len(devices))
	for _, dev := range devices {
		onlineAgents[dev.AgentID] = true
	}

	queueStates, _ := d.stateStore.ListDeviceQueueStates()

	var candidates []Candidate
	for _, dq := range d.deviceQueues {
		if !onlineAgents[dq.agentID] {
			continue
		}
		st := d.getQueueState(queueStates, dq.queueName)
		if !st.Enabled {
			continue
		}
		// Skip devices that already have a different model loaded.
		if st.LoadedHash != "" && st.LoadedHash != fingerprint {
			continue
		}
		candidates = append(candidates, Candidate{
			AgentID:       dq.agentID,
			DeviceIDs:     dq.deviceIDs,
			QueueName:     dq.queueName,
			GFlops:        d.getDeviceGroupGFlops(dq.agentID, dq.deviceIDs),
			TotalMemoryMB: d.getDeviceGroupMemory(dq.agentID, dq.deviceIDs, devices),
			TailHash:      st.TailHash,
			TailLength:    st.TailLength,
			LoadedHash:    st.LoadedHash,
		})
	}

	return SelectBestCandidate(candidates, fingerprint)
}

// evictIdleModels attempts to free ONE device by evicting its idle model.
// Only evicts a single model from a single device — the minimum needed to
// make room for the new task. Returns true if a model was evicted.
func (d *Dispatcher) evictIdleModels(taskFingerprint string) bool {
	queueStates, _ := d.stateStore.ListDeviceQueueStates()

	for _, dq := range d.deviceQueues {
		// Don't evict from disabled devices — user deliberately parked them.
		if !d.getQueueState(queueStates, dq.queueName).Enabled {
			continue
		}
		// Only evict from devices with empty queues — don't disrupt active work.
		depth, err := dq.queue.Depth(context.Background())
		if err != nil || depth > 0 {
			continue
		}

		// Find idle models specifically on this device (not other devices on same agent).
		idle := d.pool.IdleInstancesOnDevice(dq.agentID, dq.deviceIDs)
		for _, inst := range idle {
			// Don't evict the model the task needs.
			if inst.Fingerprint == taskFingerprint {
				continue
			}
			if d.pool.Evict(inst.Fingerprint) {
				// Clear the loaded hash so selectAvailablePlacement sees this device as free.
				_ = d.stateStore.UpdateLoadedHash(dq.queueName, "")
				d.logger.Info().
					Str("fingerprint", inst.Fingerprint).
					Str("path", inst.ModelPath).
					Str("agent", dq.agentID).
					Str("queue", dq.queueName).
					Msg("evicted idle model to free resources")
				return true // one eviction is enough — caller retries placement
			}
		}
	}
	return false
}

// rankedQueue is a device queue annotated with its GFlops and memory for sorting.
type rankedQueue struct {
	dq     *DeviceQueueManager
	gflops float64
	memMB  int
}

// rankedQueues returns device queues sorted by GFlops descending, optionally filtered to local agents.
func (d *Dispatcher) rankedQueues(localOnly bool) []rankedQueue {
	var devices []DeviceInfo
	for _, ag := range d.agents.All() {
		if !ag.Status().Online {
			continue
		}
		if localOnly && !d.agents.IsLocal(ag.ID()) {
			continue
		}
		for _, dev := range ag.Devices() {
			devices = append(devices, DeviceInfo{
				AgentID:       ag.ID(),
				DeviceID:      dev.ID,
				TotalMemoryMB: dev.TotalMemoryMB,
				GFlops:        d.getDeviceGFlops(ag.ID(), dev.ID),
			})
		}
	}

	queueStates, _ := d.stateStore.ListDeviceQueueStates()

	var ranked []rankedQueue
	for _, dq := range d.deviceQueues {
		if localOnly && !d.agents.IsLocal(dq.agentID) {
			continue
		}
		ag := d.agents.Get(dq.agentID)
		if ag == nil || !ag.Status().Online {
			continue
		}
		if !d.getQueueState(queueStates, dq.queueName).Enabled {
			continue
		}
		ranked = append(ranked, rankedQueue{
			dq:     dq,
			gflops: d.getDeviceGroupGFlops(dq.agentID, dq.deviceIDs),
			memMB:  d.getDeviceGroupMemory(dq.agentID, dq.deviceIDs, devices),
		})
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].gflops > ranked[j].gflops
	})
	return ranked
}

// selectBestFreePlacement finds the strongest free (unoccupied or same-model) device.
// If localOnly is true, only local agent devices are considered.
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
				AgentID:       r.dq.agentID,
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
				AgentID:       r.dq.agentID,
				DeviceIDs:     r.dq.deviceIDs,
				QueueName:     r.dq.queueName,
				GFlops:        r.gflops,
				TotalMemoryMB: r.memMB,
			}
		}
	}

	return nil
}

// selectBestEvictablePlacement finds the strongest device across all agents
// that can be freed by evicting an idle model. Used as a last resort when
// no free device is available anywhere.
func (d *Dispatcher) selectBestEvictablePlacement(fingerprint string) *Candidate {
	ranked := d.rankedQueues(false)

	for _, r := range ranked {
		depth, err := r.dq.queue.Depth(context.Background())
		if err != nil || depth > 0 {
			continue
		}
		idle := d.pool.IdleInstancesOnDevice(r.dq.agentID, r.dq.deviceIDs)
		for _, inst := range idle {
			if inst.Fingerprint == fingerprint {
				continue
			}
			if d.pool.Evict(inst.Fingerprint) {
				_ = d.stateStore.UpdateLoadedHash(r.dq.queueName, "")
				d.logger.Info().
					Str("fingerprint", inst.Fingerprint).
					Str("path", inst.ModelPath).
					Str("agent", r.dq.agentID).
					Str("queue", r.dq.queueName).
					Msg("evicted idle model for best-device placement")
				return &Candidate{
					AgentID:       r.dq.agentID,
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
func (d *Dispatcher) getDeviceGFlops(agentID, deviceID string) float64 {
	row, err := d.benchStore.GetBenchmark(agentID, deviceID)
	if err != nil {
		return 0
	}
	return row.ComputeGFlops
}

// getDeviceGroupGFlops returns the minimum GFlops across a device group.
func (d *Dispatcher) getDeviceGroupGFlops(agentID string, deviceIDs []string) float64 {
	minGFlops := 0.0
	for i, did := range deviceIDs {
		g := d.getDeviceGFlops(agentID, did)
		if i == 0 || g < minGFlops {
			minGFlops = g
		}
	}
	return minGFlops
}

// getDeviceGroupMemory returns the total memory across a device group.
func (d *Dispatcher) getDeviceGroupMemory(agentID string, deviceIDs []string, devices []DeviceInfo) int {
	total := 0
	for _, did := range deviceIDs {
		for _, dev := range devices {
			if dev.AgentID == agentID && dev.DeviceID == did {
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
