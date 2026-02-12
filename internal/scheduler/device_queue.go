package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/server"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/pkg/workerpool"
	"github.com/rs/zerolog"
)

// DeviceQueueManager manages a per-device queue and processes inference tasks.
// It ensures the correct model is loaded before executing, handles batch processing
// up to max_concurrent, and re-queues failed tasks to the global queue.
type DeviceQueueManager struct {
	agentID   string
	deviceIDs []string
	queueName string

	queue       queue.QueueInterface
	globalQueue queue.QueueInterface
	results     queue.ResultStoreInterface
	pool        *modelPool
	stateStore  store.DeviceQueueStateStoreInterface
	benchStore  store.BenchmarkStoreInterface
	dispatcher  *Dispatcher   // set after construction, used for work stealing
	loadModelFn loadModelFunc // set after construction, full scheduler load path

	logger       zerolog.Logger
	pollInterval time.Duration
	modelsDir    func() string // returns the centralized models directory path

	mu            sync.Mutex
	loadedHash    string                 // fingerprint of currently loaded model
	maxConcurrent int32                  // calculated when model is loaded
	wp            *workerpool.WorkerPool // persistent worker pool, resized on model change
}

// NewDeviceQueueManager creates a new device queue manager.
func NewDeviceQueueManager(
	agentID string,
	deviceIDs []string,
	deviceQueue queue.QueueInterface,
	globalQueue queue.QueueInterface,
	results queue.ResultStoreInterface,
	pool *modelPool,
	stateStore store.DeviceQueueStateStoreInterface,
	benchStore store.BenchmarkStoreInterface,
	modelsDir func() string,
	logger zerolog.Logger,
) *DeviceQueueManager {
	qn := DeviceQueueName(agentID, deviceIDs[0])
	if len(deviceIDs) > 1 {
		qn = DeviceGroupQueueName(agentID, deviceIDs)
	}
	return &DeviceQueueManager{
		agentID:       agentID,
		deviceIDs:     deviceIDs,
		queueName:     qn,
		queue:         deviceQueue,
		globalQueue:   globalQueue,
		results:       results,
		pool:          pool,
		stateStore:    stateStore,
		benchStore:    benchStore,
		modelsDir:     modelsDir,
		logger:        logger.With().Str("device_queue", qn).Logger(),
		pollInterval:  100 * time.Millisecond,
		maxConcurrent: 1,
	}
}

// QueueName returns the canonical queue name.
func (dq *DeviceQueueManager) QueueName() string { return dq.queueName }

// LoadedHash returns the fingerprint of the currently loaded model.
func (dq *DeviceQueueManager) LoadedHash() string {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	return dq.loadedHash
}

// Run starts the device queue processor loop. Blocks until ctx is cancelled.
//
// Each tick it pulls all available same-fingerprint messages and submits them to
// the persistent worker pool (created/resized by ensureModel). Workers from
// previous ticks may still be running — new messages start immediately if a
// slot is free, giving true continuous concurrency across ticks.
func (dq *DeviceQueueManager) Run(ctx context.Context) {
	dq.logger.Info().Msg("device queue processor started")
	defer func() {
		dq.mu.Lock()
		if dq.wp != nil {
			dq.wp.Close()
			dq.wp = nil
		}
		dq.mu.Unlock()
		dq.logger.Info().Msg("device queue processor stopped")
	}()

	ticker := time.NewTicker(dq.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dq.drainQueue(ctx)
		}
	}
}

// drainQueue pulls all available same-fingerprint messages from the device
// queue and submits them to the persistent worker pool. Does NOT wait for
// workers to finish — they keep running across ticks. Messages arriving
// between ticks are picked up on the next tick while workers are still busy.
func (dq *DeviceQueueManager) drainQueue(ctx context.Context) {
	processed := 0

	for ctx.Err() == nil {
		msg, err := dq.queue.Receive(ctx)
		if err != nil {
			dq.logger.Error().Err(err).Msg("receiving from device queue")
			break
		}
		if msg == nil {
			if processed == 0 {
				dq.trySteal(ctx)
			}
			break
		}

		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			dq.logger.Error().Err(err).Str("msg_id", string(msg.ID)).Msg("unmarshalling envelope")
			_ = dq.results.Fail(string(msg.ID), "invalid envelope: "+err.Error())
			_ = dq.queue.Delete(ctx, msg.ID)
			continue
		}

		fp := env.Fingerprint

		// If fingerprint changed, put message back and stop — the dispatcher
		// will route it to the correct device queue.
		dq.mu.Lock()
		currentFP := dq.loadedHash
		dq.mu.Unlock()
		if currentFP != "" && fp != currentFP {
			_ = dq.queue.Delete(ctx, msg.ID)
			_ = dq.queue.Requeue(ctx, msg, env.Priority)
			break
		}

		// Ensure model is loaded (creates/resizes the persistent worker pool).
		if currentFP == "" || currentFP != fp {
			dq.ensureModel(fp)
		}

		dq.mu.Lock()
		wp := dq.wp
		dq.mu.Unlock()
		if wp == nil {
			// Should not happen, but guard against it.
			dq.logger.Error().Msg("worker pool not initialized")
			break
		}

		item := &batchItem{msg: msg, env: env}
		if err := wp.Do(ctx, func(ctx context.Context) {
			dq.executeOne(ctx, item)
		}); err != nil {
			break
		}
		processed++
	}

	if processed > 0 {
		dq.mu.Lock()
		fp := dq.loadedHash
		dq.mu.Unlock()
		if fp != "" {
			dq.updateTailState(fp, processed)
		}
	}
}

// batchItem pairs a queue message with its decoded envelope.
type batchItem struct {
	msg *queue.Message
	env queue.Envelope
}

// executeOne processes a single inference request.
func (dq *DeviceQueueManager) executeOne(ctx context.Context, item *batchItem) {
	// Use the original request ID for result tracking (set by the dispatcher).
	// Falls back to the device queue message ID if not set.
	id := item.env.RequestID
	if id == "" {
		id = string(item.msg.ID)
	}

	// Start a heartbeat goroutine that periodically extends the global queue
	// message's visibility timeout. This prevents the global queue from
	// redelivering the message while we're still processing it.
	// On completion (success or failure), we stop the heartbeat and either
	// delete the global message (success) or let it expire (transient failure).
	var heartbeatCancel context.CancelFunc
	globalMsgID := item.env.GlobalMsgID
	if globalMsgID != "" {
		var heartbeatCtx context.Context
		heartbeatCtx, heartbeatCancel = context.WithCancel(ctx)
		go dq.heartbeatGlobalMsg(heartbeatCtx, queue.MessageID(globalMsgID), id)
	}

	if err := dq.results.MarkProcessing(id); err != nil {
		dq.logger.Error().Err(err).Str("request_id", id).Msg("marking result as processing")
	}

	source := item.env.Source
	if source == "" {
		source = "direct"
	}
	resolver := &modelResolver{pool: dq.pool, source: source, modelsDir: dq.modelsDir(), loadModel: dq.loadModelFn}
	defer resolver.ReleaseAll()

	srv := server.NewServer(dq.logger, resolver)

	var body []byte
	var execErr error
	switch item.env.Type {
	case queue.RequestTypeChatCompletion:
		body, execErr = srv.ExecuteChatCompletion(ctx, item.env.Payload)
	case queue.RequestTypeBatchChatCompletion:
		body, execErr = srv.ExecuteBatchChatCompletion(ctx, item.env.Payload)
	case queue.RequestTypeEmbedding:
		body, execErr = srv.ExecuteEmbedding(ctx, item.env.Payload)
	case queue.RequestTypeBatchEmbedding:
		body, execErr = srv.ExecuteBatchEmbedding(ctx, item.env.Payload)
	case queue.RequestTypeTokenize:
		body, execErr = srv.ExecuteTokenize(ctx, item.env.Payload)
	default:
		execErr = ctxerr.With(fmt.Errorf("unknown request type"), map[string]any{"type": item.env.Type})
	}

	// Stop the heartbeat before touching results/global message.
	if heartbeatCancel != nil {
		heartbeatCancel()
	}

	if execErr != nil {
		dq.logger.Error().Err(execErr).Str("request_id", id).Msg("execution failed")
		_ = dq.results.Fail(id, execErr.Error())
		// Ack the global message — the error is recorded in results, no need for redelivery.
		if globalMsgID != "" {
			_ = dq.globalQueue.Delete(ctx, queue.MessageID(globalMsgID))
		}
	} else {
		if storeErr := dq.results.Complete(id, body); storeErr != nil {
			dq.logger.Error().Err(storeErr).Str("request_id", id).Msg("storing result")
		}
		// Success: ack (delete) the global message so it won't be redelivered.
		if globalMsgID != "" {
			if err := dq.globalQueue.Delete(ctx, queue.MessageID(globalMsgID)); err != nil {
				dq.logger.Error().Err(err).Str("request_id", id).Msg("acking global message after success")
			}
		}
	}

	// Delete from device queue (lightweight routing copy).
	if delErr := dq.queue.Delete(ctx, item.msg.ID); delErr != nil {
		dq.logger.Error().Err(delErr).Str("request_id", id).Msg("deleting device queue message")
	}
}

// heartbeatGlobalMsg periodically extends the global queue message's visibility
// timeout to prevent redelivery while the task is being processed.
func (dq *DeviceQueueManager) heartbeatGlobalMsg(ctx context.Context, globalMsgID queue.MessageID, requestID string) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := dq.globalQueue.Extend(ctx, globalMsgID, globalExtendDuration); err != nil {
				// Message may have been deleted (success) or expired — either way, stop.
				dq.logger.Trace().Err(err).Str("request_id", requestID).Msg("heartbeat extend failed, stopping")
				return
			}
			dq.logger.Trace().Str("request_id", requestID).Msg("heartbeat: extended global message visibility")
		}
	}
}

// ensureModel verifies the required model is loaded on this device. If a different
// model is currently loaded, it evicts the old one. The actual model load happens
// lazily in the resolver during executeOne — this method tracks which fingerprint
// this device queue considers "current" and updates concurrency limits accordingly.
func (dq *DeviceQueueManager) ensureModel(fingerprint string) {
	dq.mu.Lock()
	if dq.loadedHash == fingerprint && dq.wp != nil {
		dq.mu.Unlock()
		return
	}
	prevHash := dq.loadedHash
	dq.mu.Unlock()

	// Evict the previous model if one was loaded. This frees VRAM so the
	// resolver's GetOrLoadChat/GetOrLoadEmbedding can load the new model.
	if prevHash != "" {
		if dq.pool.Evict(prevHash) {
			dq.logger.Info().
				Str("prev_fingerprint", prevHash).
				Str("new_fingerprint", fingerprint).
				Msg("evicted previous model for device queue model switch")
		}
	}

	// Determine max_concurrent: prefer the value from the pool's placement
	// (set by computePlacement during LoadModel), fall back to benchmark calculation.
	newMaxConcurrent := dq.pool.modelMaxConcurrent(fingerprint)
	if newMaxConcurrent <= 0 && dq.modelsDir != nil {
		newMaxConcurrent = 1
		modelPath := dq.pool.modelPath(fingerprint)
		if modelPath != "" {
			modelSize, _ := ModelFileSize(modelPath)
			// Include auxiliary files (e.g. mmproj) in VRAM estimate.
			if mmproj := dq.pool.modelMmprojPath(fingerprint); mmproj != "" {
				if s, err := ModelFileSize(mmproj); err == nil {
					modelSize += s
				}
			}
			if modelSize > 0 {
				modelSizeGB := float64(modelSize) / (1024 * 1024 * 1024)
				modelSizeMB := int(modelSize / (1024 * 1024))
				gflops := dq.benchGFlops()
				totalMB := dq.benchTotalMemoryMB()
				kvCacheMB := int(EstimateKVCacheMB(modelSize, dq.pool.modelContextSize(fingerprint), dq.pool.modelCacheType(fingerprint)))
				newMaxConcurrent = CalcMaxConcurrent(gflops, modelSizeGB, totalMB, modelSizeMB, kvCacheMB)
			}
		}
	}

	dq.mu.Lock()
	dq.loadedHash = fingerprint
	dq.maxConcurrent = newMaxConcurrent
	// Replace worker pool with correct concurrency for the new model.
	// Wait for any in-flight work from the old pool before replacing.
	if dq.wp != nil {
		dq.wp.Close()
	}
	dq.wp = workerpool.New(int(newMaxConcurrent))
	dq.mu.Unlock()

	if err := dq.stateStore.UpdateLoadedHash(dq.queueName, fingerprint); err != nil {
		dq.logger.Warn().Err(err).Msg("updating loaded hash in DB")
	}

	dq.logger.Info().
		Str("fingerprint", fingerprint).
		Int32("max_concurrent", newMaxConcurrent).
		Msg("device queue model switched")
}

// benchGFlops returns the minimum GFlops across this device queue's devices.
func (dq *DeviceQueueManager) benchGFlops() float64 {
	minGFlops := 0.0
	for i, did := range dq.deviceIDs {
		row, err := dq.benchStore.GetBenchmark(dq.agentID, did)
		if err != nil {
			continue
		}
		if i == 0 || row.ComputeGFlops < minGFlops {
			minGFlops = row.ComputeGFlops
		}
	}
	return minGFlops
}

// benchTotalMemoryMB returns the total memory across this device queue's devices.
// Falls back to benchmark memory if available.
func (dq *DeviceQueueManager) benchTotalMemoryMB() int {
	total := 0
	for _, did := range dq.deviceIDs {
		row, err := dq.benchStore.GetBenchmark(dq.agentID, did)
		if err != nil {
			continue
		}
		// MemoryGBs is stored as float64 GB in benchmarks; convert to MB.
		total += int(row.MemoryGBs * 1024)
	}
	return total
}

// updateTailState updates the tail hash and length in the DB.
func (dq *DeviceQueueManager) updateTailState(fingerprint string, batchSize int) {
	// Read current tail state.
	st, err := dq.stateStore.GetDeviceQueueState(dq.queueName)
	if err != nil {
		dq.logger.Warn().Err(err).Msg("reading tail state")
		return
	}

	var newLength int
	if st.TailHash == fingerprint {
		newLength = st.TailLength + batchSize
	} else {
		newLength = batchSize
	}

	if err := dq.stateStore.UpdateTail(dq.queueName, fingerprint, newLength); err != nil {
		dq.logger.Warn().Err(err).Msg("updating tail state")
	}
}

// DrainToGlobal moves all pending messages from this device queue back to the
// global queue. Used when disabling a device — tasks must be redistributed.
// Returns the number of drained messages.
func (dq *DeviceQueueManager) DrainToGlobal(ctx context.Context) (int, error) {
	drained := 0
	for {
		msg, err := dq.queue.Receive(ctx)
		if err != nil {
			return drained, fmt.Errorf("receiving from device queue %s: %w", dq.queueName, err)
		}
		if msg == nil {
			break
		}

		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			dq.logger.Error().Err(err).Str("msg_id", string(msg.ID)).Msg("unmarshalling envelope during drain")
			_ = dq.queue.Delete(ctx, msg.ID)
			continue
		}

		// Delete the old global message if present — the re-submit creates a fresh one.
		if env.GlobalMsgID != "" {
			_ = dq.globalQueue.Delete(ctx, queue.MessageID(env.GlobalMsgID))
			env.GlobalMsgID = ""
		}

		if _, err := dq.globalQueue.SubmitEnvelope(ctx, env, env.Priority); err != nil {
			return drained, fmt.Errorf("re-submitting to global queue: %w", err)
		}
		if err := dq.queue.Delete(ctx, msg.ID); err != nil {
			dq.logger.Error().Err(err).Msg("deleting drained message from device queue")
		}
		drained++
	}

	if drained > 0 {
		dq.logger.Info().Int("drained", drained).Msg("drained tasks to global queue")
	}
	return drained, nil
}

// trySteal attempts work stealing when the device queue and global queue are both empty.
func (dq *DeviceQueueManager) trySteal(ctx context.Context) {
	if dq.dispatcher == nil {
		return
	}

	// Only steal if global queue is also empty.
	globalDepth, err := dq.globalQueue.Depth(ctx)
	if err != nil || globalDepth > 0 {
		return // global queue has work — dispatcher will handle it
	}

	result := dq.dispatcher.TrySteal(ctx, dq, dq.logger)
	if result == nil {
		return
	}

	// Submit stolen messages to our device queue for processing.
	for _, msg := range result.Messages {
		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			dq.logger.Error().Err(err).Msg("unmarshalling stolen message")
			continue
		}
		_, err = dq.queue.SubmitEnvelope(ctx, env, env.Priority)
		if err != nil {
			dq.logger.Error().Err(err).Msg("re-submitting stolen message to device queue")
		}
	}
}
