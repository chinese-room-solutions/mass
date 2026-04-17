package worker

import (
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
)

// ReconcileAll asks every connected stream worker to drop cache files that
// aren't in the canonical set. Non-stream workers (none today) are skipped.
// Errors per worker are logged but don't abort the sweep — a transient
// disconnect on one worker should not stop reconciliation on the rest.
func (f *Fleet) ReconcileAll(canonical map[string]struct{}) {
	for _, w := range f.All() {
		sw, ok := w.(*StreamWorker)
		if !ok {
			continue
		}
		if !sw.Status().Online {
			continue
		}
		if _, err := sw.Reconcile(canonical); err != nil {
			sw.logger.Warn().Err(err).Msg("cache reconcile send failed")
		}
	}
}

// CacheFiles returns the worker's most recently reported cache file list
// (forward-slash relative paths under the worker's modelsDir). Empty for
// loopback workers or workers that haven't sent a heartbeat yet.
func (a *StreamWorker) CacheFiles() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.cacheFiles) == 0 {
		return nil
	}
	out := make([]string, len(a.cacheFiles))
	copy(out, a.cacheFiles)
	return out
}

// Reconcile diffs the worker's last-reported cache against canonical
// (the live MASS file set) and asks the worker to drop anything stale.
// Returns the number of filenames requested. Returns 0 with no error if
// there's no diff, or before the first heartbeat reports a cache.
func (a *StreamWorker) Reconcile(canonical map[string]struct{}) (int, error) {
	cache := a.CacheFiles()
	if len(cache) == 0 {
		return 0, nil
	}
	var stale []string
	for _, rel := range cache {
		if _, ok := canonical[rel]; !ok {
			stale = append(stale, rel)
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}

	a.sendMu.Lock()
	err := a.sender.Send(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_DeleteCacheFiles{DeleteCacheFiles: &workerpb.HubDeleteCacheFiles{
			Filenames: stale,
		}},
	})
	a.sendMu.Unlock()
	if err != nil {
		return 0, err
	}
	a.logger.Info().Int("files", len(stale)).Msg("sent cache reconcile delete")
	return len(stale), nil
}
