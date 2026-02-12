package scheduler

import (
	"context"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/rs/zerolog"
)

// StealResult describes the outcome of a work stealing attempt.
type StealResult struct {
	Messages  []*queue.Message
	FromQueue string
}

// TrySteal attempts to steal work for an idle device queue (thief).
// It checks three priorities in order:
//  1. Save donor from context switch: find a donor whose next queued tasks
//     have a different fingerprint than its loaded model. Those tasks would
//     cause a context switch on the donor anyway — steal them.
//  2. Match thief's model: find tasks in any donor queue that match the
//     thief's currently loaded model fingerprint.
//  3. Rebalance: steal from the longest donor queue (consecutive same-FP batch from tail).
//
// Returns nil if no work could be stolen.
func (d *Dispatcher) TrySteal(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	thiefHash := thief.LoadedHash()

	// Priority A: save donor from context switch.
	if result := d.stealMismatchedFromDonor(ctx, thief, logger); result != nil {
		return result
	}

	// Priority B: match thief's loaded model.
	if thiefHash != "" {
		if result := d.stealMatchingThief(ctx, thief, thiefHash, logger); result != nil {
			return result
		}
	}

	// Priority C: rebalance — steal from longest queue.
	return d.stealFromLongest(ctx, thief, logger)
}

// stealMismatchedFromDonor finds a donor whose next queued tasks have a different
// fingerprint than the donor's loaded model. Those tasks will cause a context
// switch on the donor, so stealing them is a win-win.
func (d *Dispatcher) stealMismatchedFromDonor(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	for _, donor := range d.deviceQueues {
		if donor.queueName == thief.queueName {
			continue
		}

		donorLoaded := donor.LoadedHash()
		if donorLoaded == "" {
			continue
		}

		// Peek at donor's next tasks.
		peeked, err := donor.queue.Peek(ctx, 16)
		if err != nil || len(peeked) == 0 {
			continue
		}

		// Check if the first peeked task has a different fingerprint than donor's loaded model.
		firstEnv, err := queue.UnmarshalEnvelope(peeked[0].Body)
		if err != nil {
			continue
		}
		if firstEnv.Fingerprint == donorLoaded {
			continue // donor's next task matches its model, no context switch — skip
		}

		// Found a donor about to context-switch. Steal all consecutive same-FP tasks.
		targetFP := firstEnv.Fingerprint
		var stolen []*queue.Message
		for _, m := range peeked {
			env, err := queue.UnmarshalEnvelope(m.Body)
			if err != nil || env.Fingerprint != targetFP {
				break
			}
			received, err := donor.queue.ReceiveByID(ctx, m.ID)
			if err != nil || received == nil {
				break // already consumed
			}
			stolen = append(stolen, received)
		}

		if len(stolen) > 0 {
			logger.Info().
				Str("from", donor.queueName).
				Str("fingerprint", targetFP).
				Int("count", len(stolen)).
				Msg("stole mismatched tasks from donor (priority A)")
			return &StealResult{Messages: stolen, FromQueue: donor.queueName}
		}
	}
	return nil
}

// stealMatchingThief finds tasks in any donor queue that match the thief's
// currently loaded model fingerprint.
func (d *Dispatcher) stealMatchingThief(ctx context.Context, thief *DeviceQueueManager, thiefHash string, logger zerolog.Logger) *StealResult {
	for _, donor := range d.deviceQueues {
		if donor.queueName == thief.queueName {
			continue
		}

		peeked, err := donor.queue.Peek(ctx, 16)
		if err != nil || len(peeked) == 0 {
			continue
		}

		// Look for tasks matching thief's loaded model.
		var stolen []*queue.Message
		for _, m := range peeked {
			env, err := queue.UnmarshalEnvelope(m.Body)
			if err != nil {
				continue
			}
			if env.Fingerprint != thiefHash {
				continue
			}
			received, err := donor.queue.ReceiveByID(ctx, m.ID)
			if err != nil || received == nil {
				continue
			}
			stolen = append(stolen, received)
		}

		if len(stolen) > 0 {
			logger.Info().
				Str("from", donor.queueName).
				Str("fingerprint", thiefHash).
				Int("count", len(stolen)).
				Msg("stole matching tasks for thief's model (priority B)")
			return &StealResult{Messages: stolen, FromQueue: donor.queueName}
		}
	}
	return nil
}

// stealFromLongest steals a consecutive same-FP batch from the donor with the
// longest queue. This is the last resort for rebalancing load.
func (d *Dispatcher) stealFromLongest(ctx context.Context, thief *DeviceQueueManager, logger zerolog.Logger) *StealResult {
	var longestQueue *DeviceQueueManager
	var longestDepth int

	for _, donor := range d.deviceQueues {
		if donor.queueName == thief.queueName {
			continue
		}
		depth, err := donor.queue.Depth(ctx)
		if err != nil || depth <= 1 {
			continue // nothing worth stealing (keep at least 1 for the donor)
		}
		if depth > longestDepth {
			longestDepth = depth
			longestQueue = donor
		}
	}

	if longestQueue == nil {
		return nil
	}

	// Peek and steal a consecutive same-FP batch from the tail (lowest priority, newest).
	peeked, err := longestQueue.queue.Peek(ctx, longestDepth)
	if err != nil || len(peeked) == 0 {
		return nil
	}

	// Take from the end (tail).
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

	// Don't steal everything — leave at least 1 task for the donor.
	if len(toSteal) >= longestDepth {
		toSteal = toSteal[:longestDepth-1]
	}
	if len(toSteal) == 0 {
		return nil
	}

	// Actually consume the messages.
	var stolen []*queue.Message
	for _, m := range toSteal {
		received, err := longestQueue.queue.ReceiveByID(ctx, m.ID)
		if err != nil || received == nil {
			continue
		}
		stolen = append(stolen, received)
	}

	if len(stolen) > 0 {
		logger.Info().
			Str("from", longestQueue.queueName).
			Str("fingerprint", targetFP).
			Int("count", len(stolen)).
			Int("donor_depth", longestDepth).
			Msg("stole from longest queue for rebalancing (priority C)")
		return &StealResult{Messages: stolen, FromQueue: longestQueue.queueName}
	}
	return nil
}
