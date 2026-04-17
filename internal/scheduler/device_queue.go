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

// DeviceQueueManager processes inference tasks on a per-device queue:
// ensures the right model is loaded, runs up to max_concurrent at a time,
// re-queues failures to global.
//
// Event-driven: Peeks the head only on reason to act (new submit, worker
// done, pool change, safety fallback). Waiting messages are never
// Receive'd — they sit untouched in goqite, immune to MaxReceive
// accounting and visibility timeouts.
type DeviceQueueManager struct {
	workerID  string
	deviceIDs []string
	queueName string

	queue       queue.QueueInterface
	globalQueue queue.QueueInterface
	results     queue.ResultStoreInterface
	pool        *modelPool
	stateStore  store.DeviceQueueStateStoreInterface
	dispatcher  *Dispatcher   // set after construction, used for work stealing
	loadModelFn loadModelFunc // set after construction, full scheduler load path

	logger    zerolog.Logger
	modelsDir func() string // returns the centralized models directory path

	// workerDone is signalled (non-blocking, capacity 1) whenever a worker
	// finishes executeOne. The Run loop selects on it to re-check the head
	// of the queue without polling.
	workerDone chan struct{}

	mu            sync.Mutex
	loadedHash    string                 // fingerprint of currently loaded model
	maxConcurrent int32                  // calculated when model is loaded
	wp            *workerpool.WorkerPool // persistent worker pool, resized on model change
	activeWorkers int                    // number of in-flight executeOne goroutines
}

// fallbackInterval is the safety-net wake-up. Real wake-ups come from
// NotifyCh and workerDone; this only matters if a notification is missed
// (tests, out-of-process submitters, future worker integration).
const fallbackInterval = 5 * time.Second

// NewDeviceQueueManager creates a new device queue manager.
func NewDeviceQueueManager(
	workerID string,
	deviceIDs []string,
	deviceQueue queue.QueueInterface,
	globalQueue queue.QueueInterface,
	results queue.ResultStoreInterface,
	pool *modelPool,
	stateStore store.DeviceQueueStateStoreInterface,
	modelsDir func() string,
	logger zerolog.Logger,
) *DeviceQueueManager {
	qn := DeviceQueueName(workerID, deviceIDs[0])
	if len(deviceIDs) > 1 {
		qn = DeviceGroupQueueName(workerID, deviceIDs)
	}
	return &DeviceQueueManager{
		workerID:      workerID,
		deviceIDs:     deviceIDs,
		queueName:     qn,
		queue:         deviceQueue,
		globalQueue:   globalQueue,
		results:       results,
		pool:          pool,
		stateStore:    stateStore,
		modelsDir:     modelsDir,
		logger:        logger.With().Str("device_queue", qn).Logger(),
		workerDone:    make(chan struct{}, 1),
		maxConcurrent: 1,
	}
}

// signalWorkerDone is a non-blocking wake. Idempotent — multiple
// completions while the loop is busy collapse to one.
func (dq *DeviceQueueManager) signalWorkerDone() {
	select {
	case dq.workerDone <- struct{}{}:
	default:
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

// ClearLoadedHash resets the in-memory loaded fingerprint after an
// external eviction (e.g. PoolEvict) so the next task triggers ensureModel.
func (dq *DeviceQueueManager) ClearLoadedHash() {
	dq.mu.Lock()
	dq.loadedHash = ""
	if dq.wp != nil {
		dq.wp.Close()
		dq.wp = nil
	}
	dq.mu.Unlock()
}

// Run starts the processor loop. Blocks until ctx is cancelled.
//
// Wakes on: new submit (NotifyCh), worker done (workerDone), pool change
// (a model evicted elsewhere may free us to load ours), or safety
// fallback. Peek doesn't consume — Receive only fires when we decide to
// execute, so waiting tasks never burn MaxReceive or visibility timers.
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

	// Subscribe to pool changes so we can react when a model elsewhere is
	// evicted (frees us to load our own next-up fingerprint, etc.).
	poolCh := make(chan struct{}, 1)
	dq.pool.AddChangeCallback(func(PoolChangeEvent) {
		select {
		case poolCh <- struct{}{}:
		default:
		}
	})

	fallback := time.NewTicker(fallbackInterval)
	defer fallback.Stop()

	for {
		// Drain everything we can right now, then wait.
		dq.processReady(ctx)
		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-dq.queue.NotifyCh():
		case <-dq.workerDone:
		case <-poolCh:
		case <-fallback.C:
		}
	}
}

// processReady inspects the head of the queue and dispatches as many
// messages as possible without violating model-switch rules or worker pool
// capacity. Returns when no further progress can be made.
func (dq *DeviceQueueManager) processReady(ctx context.Context) {
	processed := 0

	for ctx.Err() == nil {
		// Peek the next visible message — this does NOT mark it received.
		peeked, err := dq.queue.Peek(ctx, 1)
		if err != nil {
			dq.logger.Error().Err(err).Msg("peeking device queue")
			return
		}
		if len(peeked) == 0 {
			if processed == 0 {
				dq.trySteal(ctx)
			}
			return
		}

		head := peeked[0]
		env, err := queue.UnmarshalEnvelope(head.Body)
		if err != nil {
			// Bad envelope: consume and fail it so it doesn't block the queue.
			dq.logger.Error().Err(err).Str("msg_id", string(head.ID)).Msg("unmarshalling envelope at head")
			if got, _ := dq.queue.ReceiveByID(ctx, head.ID); got != nil {
				if fErr := dq.results.Fail(string(got.ID), "invalid envelope: "+err.Error()); fErr != nil {
					dq.logger.Error().Err(fErr).Str("msg_id", string(got.ID)).Msg("failing result for invalid envelope")
				}
				if dErr := dq.queue.Delete(ctx, got.ID); dErr != nil {
					dq.logger.Error().Err(dErr).Str("msg_id", string(got.ID)).Msg("deleting invalid envelope")
				}
			}
			continue
		}

		// Idempotency: if the result for this request is already done or
		// errored, the work was completed by some other path (a prior
		// execution that crashed after Complete() but before deleting this
		// device row, a concurrent steal-back, etc.). Drop the device row
		// without re-running inference. The dispatcher already does the same
		// check on the global side.
		if env.RequestID != "" {
			if r, _ := dq.results.Get(env.RequestID); r != nil &&
				(r.Status == queue.ResultStatusDone || r.Status == queue.ResultStatusError) {
				dq.logger.Debug().Str("request_id", env.RequestID).Msg("dropping device row: result already completed")
				if got, _ := dq.queue.ReceiveByID(ctx, head.ID); got != nil {
					if dErr := dq.queue.Delete(ctx, got.ID); dErr != nil {
						dq.logger.Error().Err(dErr).Str("msg_id", string(got.ID)).Msg("deleting completed device row")
					}
				}
				continue
			}
		}

		fp := env.Fingerprint

		// Decide model state.
		dq.mu.Lock()
		currentFP := dq.loadedHash
		wp := dq.wp
		active := dq.activeWorkers
		dq.mu.Unlock()

		switch {
		case currentFP == "":
			if !dq.ensureModel(fp) {
				return
			}
		case currentFP != fp:
			// Different model needed. Wait for current workers to drain;
			// then try to evict and load the new one.
			if active > 0 {
				return
			}
			if !dq.ensureModel(fp) {
				return
			}
		case wp == nil:
			// Same fingerprint but no worker pool yet (e.g. pre-warmed loadedHash
			// without an explicit ensureModel call). Initialize via ensureModel.
			if !dq.ensureModel(fp) {
				return
			}
		}

		// Re-read after possible ensureModel.
		dq.mu.Lock()
		wp = dq.wp
		dq.mu.Unlock()
		if wp == nil {
			dq.logger.Error().Msg("worker pool not initialized")
			return
		}

		// Now consume the message — we're committed to executing it.
		msg, err := dq.queue.ReceiveByID(ctx, head.ID)
		if err != nil {
			dq.logger.Error().Err(err).Str("msg_id", string(head.ID)).Msg("receiving by id at head")
			return
		}
		if msg == nil {
			// Someone else (work stealer) consumed it between our Peek and Receive.
			continue
		}

		dq.mu.Lock()
		dq.activeWorkers++
		dq.mu.Unlock()

		item := &batchItem{msg: msg, env: env}
		if err := wp.Do(ctx, func(ctx context.Context) {
			defer func() {
				dq.mu.Lock()
				dq.activeWorkers--
				dq.mu.Unlock()
				dq.signalWorkerDone()
			}()
			dq.executeOne(ctx, item)
		}); err != nil {
			// Submission failed (ctx cancelled). Roll back the active count
			// and re-queue the message at the head so we don't lose it.
			dq.mu.Lock()
			dq.activeWorkers--
			dq.mu.Unlock()
			if rqErr := dq.queue.Requeue(ctx, msg, env.Priority); rqErr != nil {
				dq.logger.Error().Err(rqErr).Msg("requeueing message after worker submit failure")
			}
			return
		}
		processed++
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

	// Heartbeat both leases (global + this device queue's own) for the
	// duration of the inference call — without it, goqite redelivers and
	// we process the same request twice. On completion the heartbeats are
	// cancelled and the messages either deleted or allowed to expire.
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	// Always extend this device queue's own lease — the worker holds it
	// until executeOne returns, which may be minutes for inference.
	go dq.heartbeatLease(heartbeatCtx, dq.queue, item.msg.ID, deviceExtendDuration, id, dq.queueName)

	globalMsgID := item.env.GlobalMsgID
	if globalMsgID != "" {
		go dq.heartbeatLease(heartbeatCtx, dq.globalQueue, queue.MessageID(globalMsgID), globalExtendDuration, id, "global")
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
		if fErr := dq.results.Fail(id, execErr.Error()); fErr != nil {
			dq.logger.Error().Err(fErr).Str("request_id", id).Msg("storing failed result")
		}
		// Ack the global message — the error is recorded in results, no need for redelivery.
		if globalMsgID != "" {
			if dErr := dq.globalQueue.Delete(ctx, queue.MessageID(globalMsgID)); dErr != nil {
				dq.logger.Error().Err(dErr).Str("msg_id", globalMsgID).Msg("acking failed-execution global msg")
			}
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

	// Subtract this task's difficulty from the queue's running sum, mirroring
	// the increment the dispatcher made on submit. Tail_hash is intentionally
	// left alone — the next enqueue overwrites it, and a stale value in an
	// emptied queue is harmless because [ScoreCost] only consults it when
	// TailDifficulty > 0 in practice (an empty tail collapses to LoadedHash).
	bs := batchSize(item.env.Type, item.env.Payload)
	slots := int(dq.pool.modelMaxConcurrent(item.env.Fingerprint))
	if diff := envelopeDifficulty(len(item.env.Payload), item.env.ModelSizeBytes, bs, slots, isEmbeddingBatch(item.env.Type)); diff > 0 {
		if err := dq.stateStore.AddTailDifficulty(dq.queueName, -diff); err != nil {
			dq.logger.Warn().Err(err).Msg("decrementing tail_difficulty after execute")
		}
	}
}

// heartbeatLease periodically extends a queue message's visibility timeout to
// prevent redelivery while the task is being processed. Used for both the
// global queue message (ack deferred until execution completes) and the
// device queue message (its own lease must also outlive the inference call,
// otherwise goqite redelivers and processReady would dispatch a duplicate).
func (dq *DeviceQueueManager) heartbeatLease(ctx context.Context, q queue.QueueInterface, msgID queue.MessageID, dur time.Duration, requestID, queueName string) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := q.Extend(ctx, msgID, dur); err != nil {
				// Message may have been deleted (success) or expired — either way, stop.
				dq.logger.Trace().Err(err).Str("request_id", requestID).Str("queue", queueName).Msg("heartbeat extend failed, stopping")
				return
			}
			dq.logger.Trace().Str("request_id", requestID).Str("queue", queueName).Msg("heartbeat: extended message visibility")
		}
	}
}

// ensureModel verifies the required model is loaded on this device. If a
// different model is currently loaded, it evicts the old one. Returns false if
// the previous model couldn't be evicted right now (still has in-flight
// requests or is otherwise busy) — caller must defer the new request.
//
// The actual model load happens lazily in the resolver during executeOne —
// this method tracks which fingerprint this device queue considers "current"
// and updates concurrency limits accordingly.
func (dq *DeviceQueueManager) ensureModel(fingerprint string) bool {
	dq.mu.Lock()
	if dq.loadedHash == fingerprint && dq.wp != nil {
		dq.mu.Unlock()
		return true
	}
	prevHash := dq.loadedHash
	dq.mu.Unlock()

	// Evict the previous model if one was loaded *and* it's a different
	// fingerprint. TryEvict atomically checks activeReqs==0 and flips
	// draining under the same lock, closing the TOCTOU window where a
	// duplicate-redelivered request could acquire the model between a prior
	// CanEvict check and this call. If prevHash matches the new fingerprint,
	// we are only here because wp was nil — no eviction is needed.
	if prevHash != "" && prevHash != fingerprint {
		// HasChat/HasEmbedding tell us whether the pool actually holds an
		// instance to evict — if not, the loadedHash was a stale label and
		// we can proceed straight to (re)building the worker pool.
		if dq.pool.HasChat(prevHash) || dq.pool.HasEmbedding(prevHash) {
			if !dq.pool.TryEvict(prevHash) {
				dq.logger.Debug().
					Str("prev_fingerprint", prevHash).
					Str("new_fingerprint", fingerprint).
					Msg("could not evict previous model — busy; deferring request")
				return false
			}
			dq.logger.Info().
				Str("prev_fingerprint", prevHash).
				Str("new_fingerprint", fingerprint).
				Msg("evicted previous model for device queue model switch")
		}
	}

	// max_concurrent comes from the pool's placement, which is overwritten
	// at load time with the value the worker reports back via its
	// LoadModelResult. A loaded model always has a known concurrency.
	newMaxConcurrent := dq.pool.modelMaxConcurrent(fingerprint)
	if newMaxConcurrent <= 0 {
		newMaxConcurrent = 1
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
	return true
}

// DrainToGlobal releases every assigned task back to the global queue at
// its original position. Used on intentional disable or worker crash.
//
// The global row already exists (left invisible-leased by the dispatcher),
// so per message: shorten its lease to zero (dispatcher sees it next tick)
// + drop the device row, atomically. No window where the task is in
// neither queue, no fresh `created` timestamp sending it to the back.
//
// Returns the number of drained messages.
func (dq *DeviceQueueManager) DrainToGlobal(ctx context.Context) (int, error) {
	drained := 0
	for {
		// Peek inspects without consuming, so the row stays put if anything
		// below this point fails partway through.
		peeked, err := dq.queue.Peek(ctx, 1)
		if err != nil {
			return drained, fmt.Errorf("peeking device queue %s: %w", dq.queueName, err)
		}
		if len(peeked) == 0 {
			break
		}
		msg := peeked[0]

		env, err := queue.UnmarshalEnvelope(msg.Body)
		if err != nil {
			// Bad envelope — there is no recoverable global counterpart.
			// Consume the device row so the loop makes progress.
			dq.logger.Error().Err(err).Str("msg_id", string(msg.ID)).Msg("unmarshalling envelope during drain")
			if dErr := dq.queue.Delete(ctx, msg.ID); dErr != nil {
				dq.logger.Error().Err(dErr).Str("msg_id", string(msg.ID)).Msg("deleting bad envelope during drain")
			}
			continue
		}

		if env.GlobalMsgID == "" {
			// Legacy or test path: no global counterpart, just drop the row.
			if dErr := dq.queue.Delete(ctx, msg.ID); dErr != nil {
				return drained, fmt.Errorf("deleting orphan device row %s: %w", msg.ID, dErr)
			}
			drained++
			continue
		}

		if err := dq.queue.ReleaseLeaseAndDelete(ctx, msg.ID, dq.globalQueue, queue.MessageID(env.GlobalMsgID)); err != nil {
			return drained, fmt.Errorf("releasing global lease for %s: %w", env.GlobalMsgID, err)
		}
		drained++
	}

	if drained > 0 {
		// Every task that was queued here is now on the global queue. The
		// device queue's tail accounting is stale by exactly that amount;
		// reset rather than try to track decrements per-message during the
		// drain (the global queue side has no per-queue tail tracking, so
		// there's no symmetric +N to make).
		if err := dq.stateStore.UpdateTail(dq.queueName, "", 0); err != nil {
			dq.logger.Warn().Err(err).Msg("resetting tail state after drain")
		}
		dq.logger.Info().Int("drained", drained).Msg("released tasks back to global queue")
	}
	return drained, nil
}

// trySteal attempts work stealing when the device queue and global queue are
// both empty. The actual donor→thief move happens transactionally inside
// Dispatcher.TrySteal — by the time it returns, stolen rows are already in
// this queue (and removed from the donor) and the queue's NotifyCh has been
// signalled, so the next Run-loop iteration will pick them up.
func (dq *DeviceQueueManager) trySteal(ctx context.Context) {
	if dq.dispatcher == nil {
		return
	}
	// Only steal if global queue is also empty — otherwise the dispatcher
	// will route something to us soon and stealing would be wasted work.
	globalDepth, err := dq.globalQueue.Depth(ctx)
	if err != nil || globalDepth > 0 {
		return
	}
	dq.dispatcher.TrySteal(ctx, dq, dq.logger)
}
