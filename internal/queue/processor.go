package queue

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// ExecuteFn processes an envelope and returns serialized response bytes.
// The caller (scheduler) is responsible for model resolution and cleanup.
type ExecuteFn func(ctx context.Context, env Envelope) ([]byte, error)

// Processor drains the inference queue and dispatches requests via ExecuteFn.
type Processor struct {
	queue        QueueInterface
	results      ResultStoreInterface
	executeFn    ExecuteFn
	logger       zerolog.Logger
	pollInterval time.Duration
	ttl          time.Duration
}

// ProcessorOpts configures a new Processor.
type ProcessorOpts struct {
	Queue        QueueInterface
	Results      ResultStoreInterface
	ExecuteFn    ExecuteFn
	Logger       zerolog.Logger
	PollInterval time.Duration
	ResultTTL    time.Duration
}

// NewProcessor creates a new queue processor.
func NewProcessor(opts ProcessorOpts) *Processor {
	if opts.PollInterval == 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	return &Processor{
		queue:        opts.Queue,
		results:      opts.Results,
		executeFn:    opts.ExecuteFn,
		logger:       opts.Logger,
		pollInterval: opts.PollInterval,
		ttl:          opts.ResultTTL,
	}
}

// Run starts the processor loop. Blocks until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) {
	p.logger.Info().Msg("queue processor started")
	defer p.logger.Info().Msg("queue processor stopped")

	// Start TTL cleanup goroutine.
	go p.cleanupLoop(ctx)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.processOne(ctx)
		}
	}
}

func (p *Processor) processOne(ctx context.Context) {
	msg, err := p.queue.Receive(ctx)
	if err != nil {
		p.logger.Error().Err(err).Msg("receiving from queue")
		return
	}
	if msg == nil {
		return
	}

	id := string(msg.ID)
	p.logger.Trace().Str("request_id", id).Msg("processing queued request")

	if err := p.results.MarkProcessing(id); err != nil {
		p.logger.Error().Err(err).Str("request_id", id).Msg("marking result as processing")
	}

	envelope, err := UnmarshalEnvelope(msg.Body)
	if err != nil {
		p.logger.Error().Err(err).Str("request_id", id).Msg("unmarshalling envelope")
		_ = p.results.Fail(id, "invalid envelope: "+err.Error())
		_ = p.queue.Delete(ctx, msg.ID)
		return
	}

	body, procErr := p.executeFn(ctx, envelope)
	if procErr != nil {
		p.logger.Error().Err(procErr).Str("request_id", id).Msg("processing request")
		_ = p.results.Fail(id, procErr.Error())
	} else {
		if storeErr := p.results.Complete(id, body); storeErr != nil {
			p.logger.Error().Err(storeErr).Str("request_id", id).Msg("storing result")
		}
	}

	if delErr := p.queue.Delete(ctx, msg.ID); delErr != nil {
		p.logger.Error().Err(delErr).Str("request_id", id).Msg("deleting processed message")
	}
}

// RunCleanupOnly starts only the TTL cleanup loop without processing queue messages.
// Used when queue processing is handled by per-device processors and only result
// cleanup is needed from the legacy processor.
func (p *Processor) RunCleanupOnly(ctx context.Context) {
	p.cleanupLoop(ctx)
}

func (p *Processor) cleanupLoop(ctx context.Context) {
	if p.ttl == 0 {
		return
	}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := p.results.Cleanup(p.ttl)
			if err != nil {
				p.logger.Error().Err(err).Msg("cleaning up expired results")
			} else if n > 0 {
				p.logger.Info().Int64("removed", n).Msg("cleaned up expired results")
			}
		}
	}
}
