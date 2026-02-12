package web

import (
	"sync"
)

// SystemLogBuffer is a thread-safe ring buffer that stores MASS system log
// lines and notifies listeners when new lines arrive.
type SystemLogBuffer struct {
	mu        sync.Mutex
	lines     []string
	pos       int
	full      bool
	listeners []chan string
}

// NewSystemLogBuffer creates a buffer that holds up to size lines.
func NewSystemLogBuffer(size int) *SystemLogBuffer {
	return &SystemLogBuffer{lines: make([]string, size)}
}

// Add stores a line and notifies all listeners.
func (b *SystemLogBuffer) Add(line string) {
	b.mu.Lock()
	b.lines[b.pos] = line
	b.pos++
	if b.pos >= len(b.lines) {
		b.pos = 0
		b.full = true
	}
	// Copy listeners to avoid holding lock during send.
	listeners := make([]chan string, len(b.listeners))
	copy(listeners, b.listeners)
	b.mu.Unlock()

	for _, ch := range listeners {
		select {
		case ch <- line:
		default: // drop if slow
		}
	}
}

// Lines returns all buffered lines in chronological order.
func (b *SystemLogBuffer) Lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.full {
		out := make([]string, b.pos)
		copy(out, b.lines[:b.pos])
		return out
	}
	out := make([]string, len(b.lines))
	copy(out, b.lines[b.pos:])
	copy(out[len(b.lines)-b.pos:], b.lines[:b.pos])
	return out
}

// Subscribe returns a channel that receives new log lines.
func (b *SystemLogBuffer) Subscribe() chan string {
	ch := make(chan string, 64)
	b.mu.Lock()
	b.listeners = append(b.listeners, ch)
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes and closes a listener channel.
func (b *SystemLogBuffer) Unsubscribe(ch chan string) {
	b.mu.Lock()
	for i, c := range b.listeners {
		if c == ch {
			b.listeners = append(b.listeners[:i], b.listeners[i+1:]...)
			break
		}
	}
	b.mu.Unlock()
	close(ch)
}

// Write implements io.Writer so SystemLogBuffer can be used as a zerolog output target.
// Each Write call is treated as one log line (zerolog writes one JSON object per call).
func (b *SystemLogBuffer) Write(p []byte) (int, error) {
	line := string(p)
	// Trim trailing newline that zerolog adds.
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if line != "" {
		b.Add(line)
	}
	return len(p), nil
}
