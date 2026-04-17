package scheduler

import (
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// newTestDispatcher returns a dispatcher with no fleet/store/queue wired —
// only the registry portion is exercised. Anything that would touch the
// other fields will nil-panic, which is the intended fail-fast for tests
// of the registry surface.
func newTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	return NewDispatcher(DispatcherOpts{Logger: zerolog.Nop()})
}

// stubManager creates a *DeviceQueueManager just sufficient to be stored
// in the registry — its other fields are not touched.
func stubManager(name string) *DeviceQueueManager {
	return &DeviceQueueManager{queueName: name}
}

func TestDispatcherRegistry_AddGetRemove(t *testing.T) {
	d := newTestDispatcher(t)

	require.Nil(t, d.Get("missing"), "Get on empty registry must return nil")
	require.Equal(t, 0, d.Count())
	require.Empty(t, d.All())

	dq := stubManager("device:local:gpu:0")
	got, added := d.Add("device:local:gpu:0", dq)
	require.True(t, added, "first Add must succeed")
	require.Same(t, dq, got)
	require.Equal(t, 1, d.Count())

	require.Same(t, dq, d.Get("device:local:gpu:0"))
	require.Len(t, d.All(), 1)

	// Re-Add must not overwrite.
	other := stubManager("device:local:gpu:0")
	got, added = d.Add("device:local:gpu:0", other)
	require.False(t, added, "second Add must report not-added")
	require.Same(t, dq, got, "Add must return the existing manager on conflict")
	require.Same(t, dq, d.Get("device:local:gpu:0"), "registry must keep the original")

	removed := d.Remove("device:local:gpu:0")
	require.Same(t, dq, removed)
	require.Nil(t, d.Get("device:local:gpu:0"))
	require.Equal(t, 0, d.Count())

	require.Nil(t, d.Remove("device:local:gpu:0"), "second Remove returns nil")
}

// TestDispatcherRegistry_AllReturnsSnapshot verifies that mutating the
// registry after All() does not affect the previously returned slice — the
// slice is a snapshot, safe to range over without holding the lock.
func TestDispatcherRegistry_AllReturnsSnapshot(t *testing.T) {
	d := newTestDispatcher(t)
	a := stubManager("a")
	b := stubManager("b")
	d.Add("a", a)
	d.Add("b", b)

	snapshot := d.All()
	require.Len(t, snapshot, 2)

	d.Remove("a")
	require.Len(t, snapshot, 2, "snapshot must not reflect later mutations")
	require.Equal(t, 1, d.Count(), "live count reflects the removal")
}

// TestDispatcherRegistry_ConcurrentSafety exercises the lock under racy
// reads + writes. Run with -race to catch any unprotected access. The test
// asserts only the final state (Count == 0); the goal is the race detector,
// not the bookkeeping.
func TestDispatcherRegistry_ConcurrentSafety(t *testing.T) {
	d := newTestDispatcher(t)
	const workers = 8
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(wid int) {
			defer wg.Done()
			for i := range iters {
				name := stubName(wid, i)
				dq := stubManager(name)
				d.Add(name, dq)
				_ = d.Get(name)
				_ = d.All()
				_ = d.Count()
				d.Remove(name)
			}
		}(w)
	}
	wg.Wait()

	require.Equal(t, 0, d.Count(), "every Add must have a matching Remove")
}

func stubName(workerID, iter int) string {
	return "device:w" + itoa(workerID) + ":i" + itoa(iter)
}

// itoa avoids strconv import noise in this small file.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
