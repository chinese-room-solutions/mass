package web

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/chinese-room-solutions/mass-sdk/registry"
)

// Index-level compatibility: the registry index is the only authority on which
// worker version pairs with which runtime and MASS version. Nothing is compiled
// into the binaries beyond the wire protocol version, so an index edit changes
// the verdict on the next refresh.

// CheckWorkerCompat is the worker hub's Register-time compat gate, injected
// into the hub at wiring time (see [Handler.WorkerCompatCheck] usage in
// cmd/mass). The index is read from the local cache only — Register must never
// block on the network — and every missing input (no cached index, no matching
// row, a dev/non-semver version) accepts with a warning so dev builds and
// out-of-band installs stay usable. A non-nil error rejects the registration
// and its message reaches the worker's own log.
func (h *Handler) CheckWorkerCompat(runtimeName, workerVersion string) error {
	log := h.logger.With().Str("runtime_name", runtimeName).Str("worker_version", workerVersion).Logger()
	accept := func(reason string) error {
		log.Warn().Str("reason", reason).Msg("registering worker without an index compat check")
		return nil
	}

	if h.runtimes == nil {
		return accept("no runtimes manager")
	}
	mf, err := h.runtimes.Get(runtimeName)
	if err != nil {
		return accept(fmt.Sprintf("runtime %s is not installed", runtimeName))
	}
	idx, err := h.cachedIndex()
	if err != nil {
		if errors.Is(err, registry.ErrNoCache) {
			return accept("no cached registry index")
		}
		return accept(fmt.Sprintf("cached registry index unusable: %v", err))
	}

	reason, err := checkWorkerIndexCompat(idx, runtimeName, workerVersion, mf.Version, h.version)
	if err != nil {
		return err
	}
	if reason != "" {
		return accept(reason)
	}
	return nil
}

// cachedIndex loads the registry index from the on-disk cache without any
// network I/O. Returns registry.ErrNoCache when nothing was ever fetched.
func (h *Handler) cachedIndex() (*registry.Index, error) {
	client, err := h.registryClient()
	if err != nil {
		return nil, err
	}
	return client.CachedIndex()
}

// checkWorkerIndexCompat decides whether a worker of workerVersion may pair
// with the installed runtimeName runtime. It returns an error to reject, or a
// non-empty reason when an input made the check inconclusive (accept). When
// several worker packages list the same version for the runtime, one admitting
// row is enough — the worker doesn't say which package it came from.
func checkWorkerIndexCompat(idx *registry.Index, runtimeName, workerVersion, runtimeVersion, massVersion string) (string, error) {
	wv, err := semver.NewVersion(workerVersion)
	if err != nil {
		return fmt.Sprintf("worker version %q is not semver", workerVersion), nil
	}
	rows := workerVersionRows(idx, runtimeName, wv)
	if len(rows) == 0 {
		return fmt.Sprintf("registry index has no %s worker version %s", runtimeName, workerVersion), nil
	}

	var firstErr error
	for _, row := range rows {
		reason, err := checkWorkerRow(row, runtimeName, workerVersion, runtimeVersion, massVersion)
		if err == nil {
			return reason, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return "", firstErr
}

// checkWorkerRow checks one index row's ranges against the installed runtime
// and this server. Inconclusive halves (unparseable range, non-semver
// counterpart) are reported as reasons and skipped: the index is data, and a
// data error must not brick registration.
func checkWorkerRow(row *registry.Version, runtimeName, workerVersion, runtimeVersion, massVersion string) (string, error) {
	var reasons []string
	checks := []struct {
		what      string
		rangeExpr string
		version   string
	}{
		{"runtime", row.Runtime, runtimeVersion},
		{"MASS", row.Mass, massVersion},
	}
	for _, c := range checks {
		ok, reason := admits(c.rangeExpr, c.version)
		switch {
		case reason != "":
			reasons = append(reasons, fmt.Sprintf("%s range: %s", c.what, reason))
		case !ok:
			return "", fmt.Errorf("%s worker %s requires %s %s, but the installed %s is %s",
				runtimeName, workerVersion, c.what, c.rangeExpr, c.what, c.version)
		}
	}
	return strings.Join(reasons, "; "), nil
}

// admits reports whether rangeExpr covers version. An empty range is
// unconstrained (admits, no reason). An unparseable range or a non-semver
// version is inconclusive: ok is false and a reason is returned, and callers
// treat that as accept-with-warning rather than a verdict.
func admits(rangeExpr, version string) (bool, string) {
	if rangeExpr == "" {
		return true, ""
	}
	constraint, err := semver.NewConstraint(rangeExpr)
	if err != nil {
		return false, fmt.Sprintf("%q is not a valid semver constraint", rangeExpr)
	}
	v, err := semver.NewVersion(version)
	if err != nil {
		return false, fmt.Sprintf("installed version %q is not semver", version)
	}
	return constraint.Check(v), ""
}

// workerVersionRows returns every worker-package version row joined to
// runtimeName whose version equals want.
func workerVersionRows(idx *registry.Index, runtimeName string, want *semver.Version) []*registry.Version {
	var out []*registry.Version
	for _, pkg := range idx.WorkerPackagesFor(runtimeName) {
		for i := range pkg.Versions {
			v, err := semver.NewVersion(pkg.Versions[i].Version)
			if err != nil || !v.Equal(want) {
				continue
			}
			out = append(out, &pkg.Versions[i])
		}
	}
	return out
}

// workerPairing is one connected worker's runtime pairing and reported version,
// the fleet-side input to the pre-upgrade check.
type workerPairing struct {
	RuntimeName string
	Version     string
}

// fleetPairings snapshots every connected worker's runtime_name + version for
// the pre-upgrade flag. Returns nil when no fleet is wired.
func (h *Handler) fleetPairings() []workerPairing {
	if h.workers == nil {
		return nil
	}
	all := h.workers.All()
	out := make([]workerPairing, 0, len(all))
	for _, wkr := range all {
		out = append(out, workerPairing{RuntimeName: wkr.RuntimeName(), Version: wkr.Version()})
	}
	return out
}

// countIncompatibleWorkers reports how many connected workers paired with
// runtimeName the index says a runtime upgrade to candidateVersion would strand
// — their version's row exists and its runtime range excludes the candidate.
// Workers the index can't speak for (unknown version, no row, unparseable
// range) are not counted: the same leniency Register applies, so the flag never
// claims a breakage the hub wouldn't enforce.
func countIncompatibleWorkers(idx *registry.Index, workers []workerPairing, runtimeName, candidateVersion string) int {
	count := 0
	for _, w := range workers {
		if w.RuntimeName != runtimeName {
			continue
		}
		wv, err := semver.NewVersion(w.Version)
		if err != nil {
			continue
		}
		rows := workerVersionRows(idx, runtimeName, wv)
		if len(rows) == 0 {
			continue
		}
		stranded := true
		for _, row := range rows {
			if ok, reason := admits(row.Runtime, candidateVersion); ok || reason != "" {
				stranded = false
				break
			}
		}
		if stranded {
			count++
		}
	}
	return count
}
