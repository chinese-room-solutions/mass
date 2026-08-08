package scheduler

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/worker"
)

// queuePeekLimit is the LIMIT bound passed to Peek/PeekAll on the operator
// UI path. The Queue tab renders every row by design — there's no
// pagination — so we pass a value large enough to be effectively
// unbounded for any realistic workload. SQLite and Postgres both accept
// this as a plain integer LIMIT.
const queuePeekLimit = math.MaxInt32

// ErrRowInFlight is returned by [Scheduler.CancelQueuedRow] when the operator
// asks to cancel a row that is no longer visible — it has been popped by the
// dispatcher and is currently streaming on a worker. Phase A doesn't support
// cancelling in-flight rows; the caller should surface this to the operator
// as a 409 Conflict.
var ErrRowInFlight = errors.New("row is in flight; not cancellable")

// ErrUnknownQueue is returned when [Scheduler.CancelQueuedRow] or
// [Scheduler.EvictQueuedRowToGlobal] is asked to touch a queue name MASS
// doesn't recognise (not "global" and not a known worker queue).
// Indicates a stale UI snapshot or a bad request.
var ErrUnknownQueue = errors.New("unknown queue")

// ErrEvictGlobalRow is returned when [Scheduler.EvictQueuedRowToGlobal] is
// called with queueName == "global". Global rows have nowhere to evict to —
// the operator wants Cancel for that. UI surfaces this as a 400.
var ErrEvictGlobalRow = errors.New("cannot evict a global-queue row; cancel it instead")

// ErrNotInflight is returned by [Scheduler.CancelRunningJob] when the
// request isn't currently in flight — either it never started, already
// finished, or the worker disconnected. UI maps this to a 404.
var ErrNotInflight = errors.New("request not in flight")

// ErrWorkerGone is returned by [Scheduler.CancelRunningJob] when the
// worker that owns the request is no longer in the fleet (disconnected
// after the cancel was issued). UI maps this to a 410 Gone — the row
// will disappear on the next disconnect-drain anyway.
var ErrWorkerGone = errors.New("worker no longer connected")

// QueueRow is one peekable row in a queue. Shaped for the Queue tab —
// captures the envelope fields an operator can reason about. Inflight
// is true when MASS's inflight tracker has a record for this row's
// RequestID — i.e. the dispatcher has called AssignJob and the worker
// pump goroutine is alive. A row whose goqite lease was briefly taken
// during a device-set-gate retry is NOT inflight; the UI shows it as
// pending and offers the cancel button.
type QueueRow struct {
	MsgID         string
	RequestID     string
	RuntimeName   string
	ModelID       string
	Source        string
	Priority      int
	QueuedSeconds float64
	PayloadBytes  int
	Inflight      bool
}

// QueueSection groups rows by their source queue. Name is the canonical
// queue name ("global" or "worker|<id>"); WorkerID is empty for global,
// populated for worker queues. DepthSeconds is the sum of every row's
// QueuedSeconds in the section — the worker's worst-case wall-clock to
// drain assuming sequential execution at the predicted per-task rates.
// Operators read it as backpressure signal at the worker-card level so
// per-row chips can stay stable per-task estimates.
type QueueSection struct {
	Name         string
	WorkerID     string
	Rows         []QueueRow
	DepthSeconds float64
}

// QueueSnapshot returns the rows on the global queue and every active
// worker queue. Global rows are pending-only (leased global anchors mean
// "currently placed on a worker", which would double-count in the UI).
// Worker rows include both pending (placed-not-yet-popped) and in-flight
// (popped by dispatcher; LoadModel-or-streaming) — the operator wants to
// see jobs that haven't completed regardless of which lifecycle stage.
//
// QueueRow.Inflight is the authoritative "is this job actually running"
// flag — it's true iff the scheduler holds an [inflightRecord] for the
// row's RequestID. A row whose goqite lease was taken briefly during a
// device-set-gate retry is NOT marked Inflight; the UI keeps showing
// it as pending with the cancel button.
func (s *Scheduler) QueueSnapshot(ctx context.Context) ([]QueueSection, error) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	devQueues := make(map[string]queue.QueueInterface, len(s.devQueues))
	maps.Copy(devQueues, s.devQueues)
	s.queueMu.RUnlock()

	inflight := s.inflightRequestIDs()

	var sections []QueueSection
	if globalQ != nil {
		// Global: pending-only. A leased global row means the job has been
		// placed onto a worker queue (durability anchor); it'll show up
		// under that worker's section.
		rows, err := peekRowsUnleased(ctx, globalQ, inflight)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("peeking global queue: %w", err), map[string]any{"queue": "global"})
		}
		sections = append(sections, QueueSection{Name: "global", Rows: rows, DepthSeconds: sumQueuedSeconds(rows)})
	}

	workerNames := make([]string, 0, len(devQueues))
	for name := range devQueues {
		workerNames = append(workerNames, name)
	}
	sort.Strings(workerNames)
	for _, name := range workerNames {
		// Worker queues: include leased rows too. The Inflight flag
		// distinguishes "actually executing" from "lease briefly taken
		// by dispatcher retry" — the operator only sees a "running"
		// badge for the former.
		rows, err := peekRowsAll(ctx, devQueues[name], inflight)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("peeking worker queue: %w", err), map[string]any{"queue": name})
		}
		workerID, _ := parseWorkerQueueName(name)
		sections = append(sections, QueueSection{
			Name:         name,
			WorkerID:     workerID,
			Rows:         rows,
			DepthSeconds: sumQueuedSeconds(rows),
		})
	}
	return sections, nil
}

// QueuedModelFiles returns the set of store-relative cache keys referenced by
// every envelope still sitting in a durable queue — global plus every worker
// queue. These are the ModelFile.Filename values MASS would ship to a worker
// if the job dispatched; they're byte-level FILE identity, opaque to MASS. A
// model-delete guard intersects this set with the doomed paths to refuse a
// delete that would strand a QUEUED job (the gateway only sees ACTIVE ones).
func (s *Scheduler) QueuedModelFiles(ctx context.Context) (map[string]struct{}, error) {
	s.queueMu.RLock()
	globalQ := s.globalQ
	queues := make([]queue.QueueInterface, 0, len(s.devQueues)+1)
	if globalQ != nil {
		queues = append(queues, globalQ)
	}
	for _, q := range s.devQueues {
		queues = append(queues, q)
	}
	s.queueMu.RUnlock()

	files := make(map[string]struct{})
	for _, q := range queues {
		msgs, err := q.PeekAll(ctx, queuePeekLimit)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("peeking queue for model files: %w", err), nil)
		}
		for _, msg := range msgs {
			env, err := queue.UnmarshalEnvelope(msg.Body)
			if err != nil {
				continue
			}
			for _, f := range env.Files {
				if f.GetFilename() != "" {
					files[f.GetFilename()] = struct{}{}
				}
			}
		}
	}
	return files, nil
}

// sumQueuedSeconds totals the per-task QueuedSeconds across a section's
// rows. Returned as the section's DepthSeconds — the worker's
// worst-case wall-clock to drain sequentially.
func sumQueuedSeconds(rows []QueueRow) float64 {
	var total float64
	for _, r := range rows {
		if r.QueuedSeconds > 0 {
			total += r.QueuedSeconds
		}
	}
	return total
}

// inflightRequestIDs returns a snapshot of every requestID currently
// tracked as in-flight. Used by [Scheduler.QueueSnapshot] to mark rows
// as running off the inflight tracker rather than the goqite lease
// flag — the lease oscillates during gate-retry cycles, the tracker
// doesn't.
func (s *Scheduler) inflightRequestIDs() map[string]struct{} {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	out := make(map[string]struct{}, len(s.inflightByRequest))
	for rid := range s.inflightByRequest {
		out[rid] = struct{}{}
	}
	return out
}

// peekRowsUnleased pulls up to queuePeekLimit unleased rows from q.
// Used for the global queue section where leased = "placed on a worker"
// and would duplicate-render the per-worker entry. Inflight is set per
// row from the supplied in-flight RequestID set.
func peekRowsUnleased(ctx context.Context, q queue.QueueInterface, inflight map[string]struct{}) ([]QueueRow, error) {
	msgs, err := q.Peek(ctx, queuePeekLimit)
	if err != nil {
		return nil, err
	}
	out := make([]QueueRow, 0, len(msgs))
	for _, msg := range msgs {
		row, ok := envelopeToRow(msg.ID, msg.Body, inflight)
		if !ok {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

// peekRowsAll pulls up to queuePeekLimit rows including leased ones.
// Used for worker-queue sections so in-flight jobs stay visible while
// the model loads or the worker streams chunks. Inflight is set per row
// from the supplied in-flight RequestID set — NOT the goqite lease, which
// oscillates during device-set-gate retries.
func peekRowsAll(ctx context.Context, q queue.QueueInterface, inflight map[string]struct{}) ([]QueueRow, error) {
	msgs, err := q.PeekAll(ctx, queuePeekLimit)
	if err != nil {
		return nil, err
	}
	rows := make([]QueueRow, 0, len(msgs))
	for _, msg := range msgs {
		row, ok := envelopeToRow(msg.ID, msg.Body, inflight)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// envelopeToRow decodes one envelope into a QueueRow. Returns (row, false)
// when the envelope is malformed — the caller skips it. Malformed rows
// shouldn't take the whole tab down; the sweeper handles them eventually.
func envelopeToRow(id queue.MessageID, body []byte, inflight map[string]struct{}) (QueueRow, bool) {
	env, err := queue.UnmarshalEnvelope(body)
	if err != nil {
		return QueueRow{}, false
	}
	_, isInflight := inflight[env.RequestID]
	return QueueRow{
		MsgID:         string(id),
		RequestID:     env.RequestID,
		RuntimeName:   env.RuntimeName,
		ModelID:       env.ModelID,
		Source:        env.Source,
		Priority:      int(env.Priority),
		QueuedSeconds: env.QueuedSeconds,
		PayloadBytes:  len(env.Payload),
		Inflight:      isInflight,
	}, true
}

// CancelQueuedRow removes the unleased row identified by (queueName, msgID)
// and writes an error result so any gateway waiting on the request sees a
// clean terminal frame.
//
// Returns:
//   - [ErrRowInFlight] when the row is no longer visible to Peek (popped by
//     the dispatcher between snapshot and cancel). The caller should surface
//     this as a 409 Conflict.
//   - [ErrUnknownQueue] when queueName isn't "global" or a known worker
//     queue (stale UI snapshot or bad request).
//
// On global cancel: the row is Deleted directly.
// On worker-queue cancel: the worker row and its global anchor go away
// together via DeleteBoth.
//
// Best-effort beyond the cancel itself: the result store update is logged-
// and-tolerated on failure (the row is already gone; reverting on a result-
// store error would be worse).
func (s *Scheduler) CancelQueuedRow(ctx context.Context, queueName, msgID string) error {
	s.queueMu.RLock()
	globalQ := s.globalQ
	q, ok := s.devQueues[queueName]
	s.queueMu.RUnlock()

	if queueName == "global" {
		if globalQ == nil {
			return fmt.Errorf("cancel: queue not initialised")
		}
		return s.cancelGlobal(ctx, globalQ, queue.MessageID(msgID))
	}
	if !ok {
		return ctxerr.With(fmt.Errorf("%w: %s", ErrUnknownQueue, queueName), map[string]any{"queue": queueName})
	}
	return s.cancelWorkerQueue(ctx, q, queue.MessageID(msgID), globalQ)
}

// cancelGlobal removes msgID from the global queue, writes an error result
// for its RequestID, and broadcasts a queue change.
func (s *Scheduler) cancelGlobal(ctx context.Context, globalQ queue.QueueInterface, msgID queue.MessageID) error {
	row, err := findUnleasedRow(ctx, globalQ, msgID)
	if err != nil {
		return err
	}
	if row == nil {
		return ErrRowInFlight
	}
	env, envErr := queue.UnmarshalEnvelope(row.Body)
	if delErr := globalQ.Delete(ctx, msgID); delErr != nil {
		return ctxerr.With(fmt.Errorf("deleting global row: %w", delErr), map[string]any{"queue": "global", "message_id": string(msgID)})
	}
	// Envelope decode failures shouldn't block the cancel itself, but we
	// can't write a result entry without a RequestID. Log and move on.
	if envErr == nil && env.RequestID != "" {
		s.failResult(env.RequestID, "cancelled by operator")
		s.dropJob(env.RequestID)
	}
	s.broadcastQueueChange()
	return nil
}

// cancelWorkerQueue removes msgID + its global anchor, writes an error
// result, and broadcasts a queue change.
func (s *Scheduler) cancelWorkerQueue(ctx context.Context, q queue.QueueInterface, msgID queue.MessageID, globalQ queue.QueueInterface) error {
	row, err := findUnleasedRow(ctx, q, msgID)
	if err != nil {
		return err
	}
	if row == nil {
		return ErrRowInFlight
	}
	env, envErr := queue.UnmarshalEnvelope(row.Body)
	if envErr != nil || env.GlobalMsgID == "" || globalQ == nil {
		// Fallback: drop only the worker row. Shouldn't happen in practice —
		// every placement carries a GlobalMsgID — but if it does, leaving
		// the worker row stuck is worse than leaving an orphan anchor (which
		// the sweeper will reap on lease expiry).
		if delErr := q.Delete(ctx, msgID); delErr != nil {
			return ctxerr.With(fmt.Errorf("deleting worker row: %w", delErr), map[string]any{"queue": q.Name(), "message_id": string(msgID)})
		}
	} else {
		if delErr := q.DeleteBoth(ctx, msgID, globalQ, queue.MessageID(env.GlobalMsgID)); delErr != nil {
			return ctxerr.With(fmt.Errorf("deleting worker+global rows: %w", delErr), map[string]any{"queue": q.Name(), "message_id": string(msgID), "global_msg_id": env.GlobalMsgID})
		}
	}
	if envErr == nil && env.RequestID != "" {
		s.failResult(env.RequestID, "cancelled by operator")
		s.dropJob(env.RequestID)
	}
	s.broadcastQueueChange()
	return nil
}

// EvictQueuedRowToGlobal moves the unleased row identified by (queueName,
// msgID) off its worker queue and releases its global durability anchor
// so the dispatcher re-scores against the current fleet. Used by the
// operator when they want a job re-placed (e.g. the worker that received
// it became overloaded or has the wrong model resident).
//
// Returns:
//   - [ErrRowInFlight] when the row is no longer visible to Peek (popped
//     by the dispatcher between snapshot and evict). Caller maps to 409.
//   - [ErrEvictGlobalRow] when queueName == "global" — global rows aren't
//     evict targets, they're already on the placement queue. Maps to 400.
//   - [ErrUnknownQueue] when queueName isn't a known worker queue.
//
// Side effects on success:
//   - The worker-queue row is deleted, the global-queue lease released
//     atomically (DeleteAndReleaseLease — same primitive used by worker-
//     disconnect drain, just operator-triggered).
//   - tail_seconds for the source worker is debited by the row's
//     QueuedSeconds — that envelope no longer sits in its tail.
//   - The dispatcher is kicked so drainGlobal re-scores immediately.
//   - Queue-change callback fires.
//
// The result entry is NOT touched: this is a re-placement, not a
// failure. The eventual terminal frame on whatever worker the
// dispatcher picks will write it.
func (s *Scheduler) EvictQueuedRowToGlobal(ctx context.Context, queueName, msgID string) error {
	if queueName == "global" {
		return ErrEvictGlobalRow
	}

	s.queueMu.RLock()
	globalQ := s.globalQ
	q, ok := s.devQueues[queueName]
	s.queueMu.RUnlock()
	if !ok {
		return ctxerr.With(fmt.Errorf("%w: %s", ErrUnknownQueue, queueName), map[string]any{"queue": queueName})
	}
	if globalQ == nil {
		return fmt.Errorf("evict: queue not initialised")
	}

	row, err := findUnleasedRow(ctx, q, queue.MessageID(msgID))
	if err != nil {
		return err
	}
	if row == nil {
		return ErrRowInFlight
	}
	env, envErr := queue.UnmarshalEnvelope(row.Body)
	if envErr != nil || env.GlobalMsgID == "" {
		// Envelope is malformed or predates the durability-anchor model.
		// Refuse rather than silently degrade — the operator can hit
		// Cancel if they want this row gone.
		return ctxerr.With(fmt.Errorf("envelope missing global anchor; cannot evict"), map[string]any{"queue": queueName, "message_id": msgID})
	}

	if err := q.DeleteAndReleaseLease(ctx, queue.MessageID(msgID), globalQ, queue.MessageID(env.GlobalMsgID)); err != nil {
		return ctxerr.With(fmt.Errorf("evicting worker row + releasing global anchor: %w", err), map[string]any{"queue": queueName, "message_id": msgID, "global_msg_id": env.GlobalMsgID})
	}
	s.debitTail(queueName, env.QueuedSeconds)
	s.kick()
	s.broadcastQueueChange()
	return nil
}

// CancelRunningJob fires HubCancelJob at the worker that's executing
// requestID. The worker's chat loop polls the cancellation token between
// sampler steps and exits with an "operator-cancelled" terminal error
// frame; the pump rewrites the result message to "cancelled by operator".
//
// Returns:
//   - [ErrNotInflight] when requestID isn't currently in flight (never
//     dispatched, already terminal, etc.). UI maps to 404.
//   - [ErrWorkerGone] when the worker has disconnected between dispatch
//     and the cancel attempt. The disconnect-drain path will reap the
//     request shortly. UI maps to 410.
//
// On success: the cancel intent is recorded immediately, HubCancelJob is
// sent fire-and-forget, the queue-change callback fires. The actual
// terminal frame arrives async via pumpWorkerChunks; from the operator's
// perspective the row stays "running" until the worker observes the
// cancel (worst case: one llama_decode round, ~10-200ms).
func (s *Scheduler) CancelRunningJob(_ context.Context, requestID string) error {
	workerID, workerJobID, ok := s.markInflightCancelled(requestID)
	if !ok {
		return ctxerr.With(fmt.Errorf("%w: %s", ErrNotInflight, requestID), map[string]any{"request_id": requestID})
	}
	if workerJobID == "" {
		// Inflight but jobID not attached yet (cancel raced the very
		// short window between startInflight and attachWorkerJobID).
		// The marker is set; when the pump observes the terminal frame
		// it'll rewrite the result. No wire signal we can usefully send.
		s.logger.Debug().Str("request_id", requestID).Msg("cancel: worker jobID not yet captured; marker set")
		s.broadcastQueueChange()
		return nil
	}
	if workerID == "" {
		return ctxerr.With(fmt.Errorf("inflight record missing workerID"), map[string]any{"request_id": requestID})
	}
	wIface := s.workers.Get(workerID)
	if wIface == nil {
		return ctxerr.With(fmt.Errorf("%w: %s", ErrWorkerGone, workerID), map[string]any{"request_id": requestID, "worker_id": workerID})
	}
	sw, ok := wIface.(*worker.StreamWorker)
	if !ok {
		return fmt.Errorf("worker %q not a StreamWorker", workerID)
	}
	if err := sw.CancelJob(workerJobID); err != nil {
		return ctxerr.With(fmt.Errorf("sending HubCancelJob: %w", err), map[string]any{"request_id": requestID, "worker_id": workerID, "worker_job_id": workerJobID})
	}
	s.broadcastQueueChange()
	return nil
}

// ErrNoResult is returned by [Scheduler.GetResult] when no result row
// exists for the request_id — either it was never submitted or its
// retention window (result TTL) has lapsed. The two are indistinguishable
// at the store level by design.
var ErrNoResult = errors.New("no result for request id")

// GetResult fetches a submitted job's durable result by request_id. Unlike
// [Scheduler.StreamChunks] (which serves the short-lived in-memory replay
// buffer), this reads the persistent result store retained for the
// configured result TTL — the basis for fetch-later async retrieval.
// Returns [ErrNoResult] when the request_id is unknown or expired.
func (s *Scheduler) GetResult(requestID string) (*queue.Result, error) {
	s.queueMu.RLock()
	results := s.results
	s.queueMu.RUnlock()
	if results == nil {
		return nil, fmt.Errorf("get_result: result store not initialised")
	}
	r, err := results.Get(requestID)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ctxerr.With(fmt.Errorf("%w: %s", ErrNoResult, requestID), map[string]any{"request_id": requestID})
	}
	return r, nil
}

// CancelByRequestID cancels a submitted job by its request_id, whether it
// is still pending (queued, not yet dispatched) or already running.
//
// Queue rows are keyed by an opaque message ID, not the request_id, so
// each pending shape is found by scanning for the matching
// [queue.Envelope.RequestID]:
//
//   - unplaced: an unleased row on the global queue;
//   - placed but not yet dispatched: an unleased row on a worker queue,
//     behind a global anchor leased for [anchorLeaseDuration] (the common
//     case — Submit places inline, so the anchor is already hidden).
//
// If neither matches, the job has already been dispatched, so we fall
// through to the mid-dispatch marker and then the in-flight cancel path.
// Leased worker rows are deliberately left to those two — a leased row is
// being dispatched or is running. Returns [ErrNotInflight] when nothing
// matches: already completed, never existed, or expired.
//
// A row can race pending->leased->inflight between the checks; that's
// benign: a row that just left a pending scan is caught by the running
// path, and one that just completed surfaces as ErrNotInflight.
func (s *Scheduler) CancelByRequestID(_ context.Context, requestID string) error {
	// Cancellation mutates durable state (deleting the pending row, writing
	// the result), so it must complete even if the caller's request context
	// is cancelled mid-operation — e.g. the gateway's DELETE handler context
	// expires while a cold-start load makes this path slow, stranding the row
	// uncancelled. Detach with a fresh context, mirroring pumpWorkerChunks'
	// finalize. The lookups are bounded by queuePeekLimit and the queue's own
	// busy_timeout, so dropping the deadline can't hang.
	ctx := context.Background()

	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()

	if globalQ != nil {
		msgID, found, err := s.findPendingGlobalRow(ctx, globalQ, requestID)
		if err != nil {
			return err
		}
		if found {
			return s.cancelGlobal(ctx, globalQ, msgID)
		}
	}
	q, msgID, found, err := s.findPendingWorkerRow(ctx, requestID)
	if err != nil {
		return err
	}
	if found {
		return s.cancelWorkerQueue(ctx, q, msgID, globalQ)
	}
	// A job that's left the pending queue but isn't yet inflight is mid-
	// dispatch (leased, loading the model). Record the cancel intent;
	// dispatchEnvelope honours it before the job reaches the worker. Without
	// this the cancel would fall through to CancelRunningJob and 404 against
	// the not-yet-created inflight record, silently losing the cancel.
	if s.requestCancelDuringDispatch(requestID) {
		s.broadcastQueueChange()
		return nil
	}
	return s.CancelRunningJob(ctx, requestID)
}

// findPendingGlobalRow scans the unleased global-queue rows for the one whose
// envelope carries requestID, returning its message ID. found=false means no
// pending row matches (it may be in flight or gone).
func (s *Scheduler) findPendingGlobalRow(ctx context.Context, globalQ queue.QueueInterface, requestID string) (queue.MessageID, bool, error) {
	msgs, err := globalQ.Peek(ctx, queuePeekLimit)
	if err != nil {
		return "", false, ctxerr.With(fmt.Errorf("peeking global queue for cancel: %w", err), map[string]any{"request_id": requestID})
	}
	for _, m := range msgs {
		env, envErr := queue.UnmarshalEnvelope(m.Body)
		if envErr != nil {
			continue
		}
		if env.RequestID == requestID {
			return m.ID, true, nil
		}
	}
	return "", false, nil
}

// findPendingWorkerRow scans every worker queue's unleased rows for the one
// whose envelope carries requestID, returning the queue it sits on and its
// message ID. found=false means no worker queue holds it pending.
func (s *Scheduler) findPendingWorkerRow(ctx context.Context, requestID string) (queue.QueueInterface, queue.MessageID, bool, error) {
	s.queueMu.RLock()
	queues := slices.Collect(maps.Values(s.devQueues))
	s.queueMu.RUnlock()

	for _, q := range queues {
		msgs, err := q.Peek(ctx, queuePeekLimit)
		if err != nil {
			return nil, "", false, ctxerr.With(fmt.Errorf("peeking worker queue for cancel: %w", err), map[string]any{"request_id": requestID, "queue": q.Name()})
		}
		for _, m := range msgs {
			env, envErr := queue.UnmarshalEnvelope(m.Body)
			if envErr != nil {
				continue
			}
			if env.RequestID == requestID {
				return q, m.ID, true, nil
			}
		}
	}
	return nil, "", false, nil
}

// findUnleasedRow looks for msgID among the queue's currently-visible
// (unleased) rows. Returns nil, nil when the row isn't visible — either
// it doesn't exist or it's been leased by the dispatcher.
func findUnleasedRow(ctx context.Context, q queue.QueueInterface, msgID queue.MessageID) (*queue.Message, error) {
	msgs, err := q.Peek(ctx, queuePeekLimit)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("peeking for cancel: %w", err), map[string]any{"queue": q.Name(), "message_id": string(msgID)})
	}
	for _, m := range msgs {
		if m.ID == msgID {
			return m, nil
		}
	}
	return nil, nil
}
