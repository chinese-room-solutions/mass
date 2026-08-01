package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/worker"
)

// streamBufferMaxBytes is the per-job ring-buffer byte cap. On overflow
// the oldest chunk is dropped. The result store keeps the final body, so
// a reconnecting gateway never loses the *terminal* frame — only the
// partial history past the cap.
const streamBufferMaxBytes = 10 << 20 // 10 MiB

// SequencedChunk is one worker chunk decorated with its monotonic per-job
// sequence number. Gateways pass the last seen seq back via
// [Scheduler.StreamChunks] to resume after a disconnect.
type SequencedChunk struct {
	Seq   uint64
	Chunk *worker.JobChunk
}

// jobBuffer is the per-job in-memory ring buffer fronting one inflight
// Submit. Producers (the dispatcher's pumpWorkerChunks goroutine) call
// Append; consumers (StreamChunks RPC handlers) call Attach to get a
// channel of replayed + live chunks. After the terminal frame the buffer
// stays alive for [Scheduler.streamReplayTTL] so a reconnecting gateway
// can pick up its result.
//
// Producer invariant: store the durable result (completeResult/failResult)
// BEFORE appending the terminal frame. Attached consumers close on the
// terminal frame and immediately read the result — the gateway's
// wait-for-result path — so a publish-first ordering hands them a
// still-processing row.
//
// Drop-oldest semantics: when streamBufferMaxChunks or streamBufferMaxBytes
// is exceeded, the oldest chunk is discarded. A consumer attaching with a
// resume_seq below the buffer's current floor sees a synthetic error
// chunk so the gateway surfaces it instead of silently swallowing data.
type jobBuffer struct {
	// createdAt is stamped once at construction and never written again,
	// so the age reaper reads it without taking mu.
	createdAt time.Time

	mu sync.Mutex

	chunks     []SequencedChunk
	bytes      int
	nextSeq    uint64 // next seq to assign on Append
	floor      uint64 // oldest seq currently retained (>= floor == in buffer)
	terminal   bool
	terminalAt time.Time

	// notify is replaced on every Append + terminal flip. Consumers grab
	// the current notify under the lock during a wait turn; closing it
	// wakes every consumer cheaply (no per-attach channel bookkeeping).
	notify chan struct{}
}

func newJobBuffer() *jobBuffer {
	return &jobBuffer{createdAt: time.Now(), notify: make(chan struct{})}
}

// Append records one chunk under the next sequence number, dropping
// oldest entries when either ring cap is exceeded. Returns the assigned
// seq so the producer can log/forward it if needed.
//
// Terminal chunks (completed, error) flip the terminal flag; subsequent
// Appends are accepted but the typical producer stops after the terminal
// frame anyway.
func (b *jobBuffer) Append(c *worker.JobChunk) uint64 {
	if c == nil {
		return 0
	}
	b.mu.Lock()
	seq := b.nextSeq
	b.nextSeq++
	b.chunks = append(b.chunks, SequencedChunk{Seq: seq, Chunk: c})
	b.bytes += chunkSize(c)
	b.enforceCap()
	terminal := c.Type == worker.JobChunkTypeCompleted || c.Type == worker.JobChunkTypeError
	if terminal && !b.terminal {
		b.terminal = true
		b.terminalAt = time.Now()
	}
	prev := b.notify
	b.notify = make(chan struct{})
	b.mu.Unlock()
	close(prev)
	return seq
}

// enforceCap drops oldest chunks until the byte budget fits. Caller
// holds b.mu.
func (b *jobBuffer) enforceCap() {
	for len(b.chunks) > 0 && b.bytes > streamBufferMaxBytes {
		dropped := b.chunks[0]
		b.bytes -= chunkSize(dropped.Chunk)
		b.chunks = b.chunks[1:]
		b.floor = dropped.Seq + 1
	}
}

// terminalReached reports whether the producer has appended a Completed
// or Error frame and (optionally) how long ago.
func (b *jobBuffer) terminalReached() (bool, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.terminal, b.terminalAt
}

// Attach starts a per-call goroutine that replays buffered chunks with
// seq >= resumeSeq, then pumps live chunks until either the terminal
// frame has been delivered or ctx is cancelled. The returned channel is
// closed on completion.
//
// When resumeSeq is below the buffer's current floor (the oldest seq
// still retained), Attach delivers a synthetic Error chunk noting the
// gap and closes the channel. This surfaces the data loss to the
// gateway instead of silently advancing past it.
func (b *jobBuffer) Attach(ctx context.Context, resumeSeq uint64) <-chan SequencedChunk {
	out := make(chan SequencedChunk, 16)
	go b.pump(ctx, resumeSeq, out)
	return out
}

func (b *jobBuffer) pump(ctx context.Context, resumeSeq uint64, out chan<- SequencedChunk) {
	defer close(out)

	cursor := resumeSeq
	for {
		b.mu.Lock()
		if cursor < b.floor {
			// Gateway asked for data older than what we still keep. Surface
			// the gap so callers don't silently skip past dropped chunks.
			gap := &worker.JobChunk{
				Type:    worker.JobChunkTypeError,
				ErrText: "stream replay buffer overflowed past requested seq",
			}
			b.mu.Unlock()
			select {
			case out <- SequencedChunk{Seq: cursor, Chunk: gap}:
			case <-ctx.Done():
			}
			return
		}
		// Copy out everything at-or-after cursor, then take a snapshot of
		// the current notify channel and the terminal flag so we can
		// wait correctly after sending.
		var batch []SequencedChunk
		for _, sc := range b.chunks {
			if sc.Seq < cursor {
				continue
			}
			batch = append(batch, sc)
		}
		terminal := b.terminal
		notify := b.notify
		b.mu.Unlock()

		for _, sc := range batch {
			select {
			case out <- sc:
				cursor = sc.Seq + 1
			case <-ctx.Done():
				return
			}
		}
		if terminal {
			return
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return
		}
	}
}

func chunkSize(c *worker.JobChunk) int {
	if c == nil {
		return 0
	}
	return len(c.Chunk) + len(c.Final) + len(c.ErrText) + len(c.Note)
}
