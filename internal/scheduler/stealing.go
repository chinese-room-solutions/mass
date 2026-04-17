package scheduler

import (
	"context"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/rs/zerolog"
)

// StealResult describes the outcome of a work stealing attempt — how many
// tasks were moved and from where.
type StealResult struct {
	Count     int
	FromQueue string
}

// TrySteal attempts to steal work for an idle thief queue. Each move is a
// single SQL transaction donor→thief — no in-flight window, no duplicates,
// no leaks on crash.
//
// Priorities:
//  1. Save donor a swap: tasks whose FP differs from donor's loaded model.
//  2. Match thief: tasks whose FP matches what the thief already has loaded.
//  3. Rebalance: a consecutive same-FP batch from the longest donor queue.
//
// Returns nil if no work could be stolen.
func (d *Dispatcher) TrySteal(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	// Disabled queues must not steal — the user has parked them.
	queueStates, _ := d.stateStore.ListDeviceQueueStates()
	if !d.getQueueState(queueStates, thief.queueName).Enabled {
		return nil
	}

	thiefHash := thief.LoadedHash()

	if result := d.stealMismatchedFromDonor(ctx, thief, logger); result != nil {
		return result
	}
	if thiefHash != "" {
		if result := d.stealMatchingThief(ctx, thief, thiefHash, logger); result != nil {
			return result
		}
	}
	return d.stealFromLongest(ctx, thief, logger)
}

// moveOne atomically moves one message donor→thief and rebases tail
// accounting: subtract from donor, add to thief, stamp thief's tail_hash
// with the moved task's fingerprint.
//
// Returns false if the donor row was consumed elsewhere first (race with
// another stealer or the donor's own worker).
func moveOne(ctx context.Context, donor, thief *DeviceQueueManager, env queue.Envelope, msgID queue.MessageID) (bool, error) {
	moved, err := donor.queue.MoveTo(ctx, thief.queue, msgID, env.Priority)
	if err != nil || !moved {
		return moved, err
	}

	bs := batchSize(env.Type, env.Payload)
	slots := int(donor.pool.modelMaxConcurrent(env.Fingerprint))
	diff := envelopeDifficulty(len(env.Payload), env.ModelSizeBytes, bs, slots, isEmbeddingBatch(env.Type))
	if diff > 0 {
		if dErr := donor.stateStore.AddTailDifficulty(donor.queueName, -diff); dErr != nil {
			donor.logger.Warn().Err(dErr).Msg("decrementing donor tail_difficulty after steal")
		}
	}

	// Thief inherits this task as its new tail. Read-then-write is
	// acceptable here: stealing is sequential per-thief, so no other
	// goroutine is concurrently mutating the thief's tail.
	if st, gErr := thief.stateStore.GetDeviceQueueState(thief.queueName); gErr == nil {
		if uErr := thief.stateStore.UpdateTail(thief.queueName, env.Fingerprint, st.TailDifficulty+diff); uErr != nil {
			thief.logger.Warn().Err(uErr).Msg("updating thief tail after steal")
		}
	} else {
		thief.logger.Warn().Err(gErr).Msg("reading thief tail state after steal")
	}
	return true, nil
}

// stealMismatchedFromDonor finds a donor whose next queued tasks have a different
// fingerprint than the donor's loaded model and moves them to the thief.
func (d *Dispatcher) stealMismatchedFromDonor(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	for _, donor := range d.All() {
		if donor.queueName == thief.queueName {
			continue
		}
		donorLoaded := donor.LoadedHash()
		if donorLoaded == "" {
			continue
		}

		peeked, err := donor.queue.Peek(ctx, 16)
		if err != nil || len(peeked) == 0 {
			continue
		}
		firstEnv, err := queue.UnmarshalEnvelope(peeked[0].Body)
		if err != nil || firstEnv.Fingerprint == donorLoaded {
			continue
		}

		targetFP := firstEnv.Fingerprint
		moved := 0
		for _, m := range peeked {
			env, err := queue.UnmarshalEnvelope(m.Body)
			if err != nil || env.Fingerprint != targetFP {
				break
			}
			ok, err := moveOne(ctx, donor, thief, env, m.ID)
			if err != nil {
				logger.Error().Err(err).Msg("transactional steal failed (priority A)")
				break
			}
			if !ok {
				continue
			}
			moved++
		}

		if moved > 0 {
			logger.Info().
				Str("from", donor.queueName).
				Str("fingerprint", targetFP).
				Int("count", moved).
				Msg("stole mismatched tasks from donor (priority A)")
			return &StealResult{Count: moved, FromQueue: donor.queueName}
		}
	}
	return nil
}

// stealMatchingThief finds tasks in any donor queue that match the thief's
// currently loaded model fingerprint and moves them to the thief.
func (d *Dispatcher) stealMatchingThief(ctx context.Context, thief *DeviceQueueManager, thiefHash string, logger zerolog.Logger) *StealResult {
	for _, donor := range d.All() {
		if donor.queueName == thief.queueName {
			continue
		}

		peeked, err := donor.queue.Peek(ctx, 16)
		if err != nil || len(peeked) == 0 {
			continue
		}

		moved := 0
		for _, m := range peeked {
			env, err := queue.UnmarshalEnvelope(m.Body)
			if err != nil || env.Fingerprint != thiefHash {
				continue
			}
			ok, err := moveOne(ctx, donor, thief, env, m.ID)
			if err != nil {
				logger.Error().Err(err).Msg("transactional steal failed (priority B)")
				break
			}
			if !ok {
				continue
			}
			moved++
		}

		if moved > 0 {
			logger.Info().
				Str("from", donor.queueName).
				Str("fingerprint", thiefHash).
				Int("count", moved).
				Msg("stole matching tasks for thief's model (priority B)")
			return &StealResult{Count: moved, FromQueue: donor.queueName}
		}
	}
	return nil
}

// stealFromLongest steals a consecutive same-FP batch from the donor with the
// longest queue (last resort rebalancing).
func (d *Dispatcher) stealFromLongest(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	var longestQueue *DeviceQueueManager
	var longestDepth int
	for _, donor := range d.All() {
		if donor.queueName == thief.queueName {
			continue
		}
		depth, err := donor.queue.Depth(ctx)
		if err != nil || depth <= 1 {
			continue
		}
		if depth > longestDepth {
			longestDepth = depth
			longestQueue = donor
		}
	}
	if longestQueue == nil {
		return nil
	}

	peeked, err := longestQueue.queue.Peek(ctx, longestDepth)
	if err != nil || len(peeked) == 0 {
		return nil
	}
	last := peeked[len(peeked)-1]
	lastEnv, err := queue.UnmarshalEnvelope(last.Body)
	if err != nil {
		return nil
	}
	targetFP := lastEnv.Fingerprint

	// Walk backwards to find consecutive same-FP batch from tail.
	var toSteal []*queue.Message
	for i := len(peeked) - 1; i >= 0; i-- {
		env, err := queue.UnmarshalEnvelope(peeked[i].Body)
		if err != nil || env.Fingerprint != targetFP {
			break
		}
		toSteal = append(toSteal, peeked[i])
	}
	// Leave at least one task for the donor.
	if len(toSteal) >= longestDepth {
		toSteal = toSteal[:longestDepth-1]
	}
	if len(toSteal) == 0 {
		return nil
	}

	moved := 0
	for _, m := range toSteal {
		env, err := queue.UnmarshalEnvelope(m.Body)
		if err != nil {
			logger.Error().Err(err).Msg("unmarshalling envelope during steal (priority C)")
			break
		}
		ok, err := moveOne(ctx, longestQueue, thief, env, m.ID)
		if err != nil {
			logger.Error().Err(err).Msg("transactional steal failed (priority C)")
			break
		}
		if !ok {
			continue
		}
		moved++
	}

	if moved > 0 {
		logger.Info().
			Str("from", longestQueue.queueName).
			Str("fingerprint", targetFP).
			Int("count", moved).
			Int("donor_depth", longestDepth).
			Msg("stole from longest queue for rebalancing (priority C)")
		return &StealResult{Count: moved, FromQueue: longestQueue.queueName}
	}
	return nil
}
