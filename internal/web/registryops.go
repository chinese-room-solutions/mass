package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/Masterminds/semver/v3"
	"github.com/chinese-room-solutions/mass-sdk/registry"
	"github.com/chinese-room-solutions/mass/internal/audit"
)

// Package-registry operations shared by the dashboard's /api/runtimes/registry
// handlers and the public mass.v1.Mass Connect API. Like the other ops files
// (runtimeops.go, workerops.go), these are transport-neutral: they return the
// ops sentinels (see ops.go) and own their audit-log calls, so every transport
// maps failures to its own status codes without re-deciding.

// unresolvableMassVersion is the version handed to the resolver when MASS's own
// version is a non-semver dev placeholder (e.g. "dev"). It satisfies any
// min_mass range, matching the plan's decision to treat an unparseable server
// version as "new enough". The debug log about this is emitted once.
const unresolvableMassVersion = "9999.0.0"

var logDevVersionOnce sync.Once

// Sentinels for the /setup/worker-bin resolution path.
var (
	// errRuntimeNotInstalled is returned when worker-bin resolution is asked
	// for a runtime the MASS host does not have installed (its version is the
	// join key into the worker's compatible range).
	errRuntimeNotInstalled = errors.New("runtime not installed")
	// errAmbiguousBackend is returned when no backend was requested but the
	// index has worker artifacts for more than one backend on the os/arch.
	errAmbiguousBackend = errors.New("multiple backends available; specify one")
	// errAmbiguousWorker is returned when no worker package was requested but
	// more than one worker package resolves for the runtime + os/arch. The
	// candidate package names are returned alongside so the caller can list them.
	errAmbiguousWorker = errors.New("multiple worker packages available; specify one")
)

// PackageVersionView is a trimmed registry version: its semver plus whether the
// index has an artifact for the MASS server's own platform.
type PackageVersionView struct {
	Version     string
	HasArtifact bool
}

// PackageView is a trimmed registry package for the search listing.
type PackageView struct {
	Name        string
	Kind        string
	RuntimeName string
	DisplayName string
	Description string
	Versions    []PackageVersionView
	// Installable is the version an install would actually fetch here: the
	// newest one with an artifact for this platform whose mass range covers
	// this server. Empty when none resolves, and only ever set for runtime
	// packages — a worker's version additionally depends on the runtime it
	// pairs with, which a package listing has no way to know.
	Installable string
}

// RegistrySearchResult is the neutral view of a registry search: the matching
// packages plus whether the index was served stale (registry unreachable, a
// cached copy used instead).
type RegistrySearchResult struct {
	Packages []PackageView
	Stale    bool
}

// registryClient builds an index client pointed at the configured registry URL,
// caching the index under {dataDir}/registry-cache/index/. Phase 3a adds
// artifact caching under the same registry-cache/ root.
func (h *Handler) registryClient() (*registry.Client, error) {
	if h.cfg == nil {
		return nil, fmt.Errorf("%w: config", ErrOpUnavailable)
	}
	return registry.NewClient(h.cfg.EffectiveRegistryURL(), h.registryIndexCacheDir()), nil
}

// registryCacheDir is the root for all registry caches ({dataDir}/registry-cache).
func (h *Handler) registryCacheDir() string {
	return filepath.Join(h.dataDir, "registry-cache")
}

// registryIndexCacheDir is where the fetched index.yml + etag are cached.
func (h *Handler) registryIndexCacheDir() string {
	return filepath.Join(h.registryCacheDir(), "index")
}

// serverPlatform is the runtime platform key for the MASS host (os/arch).
func serverPlatform() registry.Platform {
	return registry.RuntimePlatform(runtime.GOOS, runtime.GOARCH)
}

// searchPackages fetches the index and returns the packages matching the given
// filters as neutral views. Fetch failures map to ErrOpRegistry.
func (h *Handler) searchPackages(ctx context.Context, kind, query, runtimeName string) (RegistrySearchResult, error) {
	client, err := h.registryClient()
	if err != nil {
		return RegistrySearchResult{}, err
	}
	res, err := client.Fetch(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("fetching registry index")
		return RegistrySearchResult{}, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}

	// Runtime artifacts are keyed os/arch; worker artifacts os/arch/backend.
	// has_artifact means "installable on this server's platform": for runtimes
	// that is an exact os/arch key, for workers any backend on the os/arch.
	runtimeKey := serverPlatform().Key()
	workerPrefix := runtimeKey + "/"
	matches := res.Index.Search(registry.SearchOptions{
		Kind:        registry.Kind(kind),
		Query:       query,
		RuntimeName: runtimeName,
	})
	views := make([]PackageView, 0, len(matches))
	for _, pkg := range matches {
		versions := make([]PackageVersionView, 0, len(pkg.Versions))
		for _, v := range pkg.Versions {
			has := versionHasArtifact(pkg.Kind, v.Artifacts, runtimeKey, workerPrefix)
			versions = append(versions, PackageVersionView{Version: v.Version, HasArtifact: has})
		}
		views = append(views, PackageView{
			Name:        pkg.Name,
			Kind:        string(pkg.Kind),
			RuntimeName: pkg.RuntimeName,
			DisplayName: pkg.DisplayName,
			Description: pkg.Description,
			Versions:    versions,
			Installable: h.installableRuntimeVersion(res.Index, pkg),
		})
	}
	return RegistrySearchResult{Packages: views, Stale: res.Stale}, nil
}

// installableRuntimeVersion is the version installing pkg here would fetch, or
// "" when nothing resolves. It answers for runtime packages only: the same
// resolver installRuntimeFromRegistry uses, so a listing never advertises a
// version the install would not produce. Non-runtime kinds return "" — a
// worker's choice also depends on the runtime version it pairs with, which the
// listing does not carry.
func (h *Handler) installableRuntimeVersion(idx *registry.Index, pkg registry.Package) string {
	if pkg.Kind != registryRuntimeKind {
		return ""
	}
	resolved, err := idx.ResolveRuntime(pkg.Name, serverPlatform(), h.massVersionForResolve())
	if err != nil {
		return ""
	}
	return resolved.Version.Version
}

// versionHasArtifact reports whether a package version has an artifact
// installable on the server's platform. Runtime artifacts are keyed exactly
// os/arch; worker artifacts os/arch/backend, so any backend on the os/arch
// counts (backend selection happens at install time). Theme artifacts are CSS
// and carry the single platform-independent "any" key.
func versionHasArtifact(kind registry.Kind, artifacts map[string]registry.Artifact, runtimeKey, workerPrefix string) bool {
	switch kind {
	case registry.KindWorker:
		for key := range artifacts {
			if strings.HasPrefix(key, workerPrefix) {
				return true
			}
		}
		return false
	case registryThemeKind:
		_, has := artifacts[themeArtifactKeyAny]
		return has
	}
	_, has := artifacts[runtimeKey]
	return has
}

// installRuntimeFromRegistry resolves a runtime package for the server's
// platform + MASS version, downloads and verifies its artifact into a temp file
// under the data dir, installs it via the runtimes manager, and cleans up. When
// version is non-empty, only that version is considered. Audit-logs the install
// keyed by "name@version".
func (h *Handler) installRuntimeFromRegistry(ctx context.Context, name, version, actor string) (RuntimeInfo, error) {
	if h.runtimes == nil {
		return RuntimeInfo{}, fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	if name == "" {
		return RuntimeInfo{}, fmt.Errorf("%w: package name is required", ErrOpInvalid)
	}

	client, err := h.registryClient()
	if err != nil {
		return RuntimeInfo{}, err
	}
	res, err := client.Fetch(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("fetching registry index")
		return RuntimeInfo{}, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}

	resolved, err := res.Index.ResolveRuntime(name, serverPlatform(), h.massVersionForResolve())
	if err != nil {
		if errors.Is(err, registry.ErrNotResolved) {
			return RuntimeInfo{}, fmt.Errorf("%w: %w", ErrOpNotFound, err)
		}
		return RuntimeInfo{}, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}
	// An explicit version pins the choice; the resolver already picked the
	// newest, so reject a mismatch rather than silently installing another.
	if version != "" && resolved.Version.Version != version {
		return RuntimeInfo{}, fmt.Errorf("%w: %s has no version %q with an artifact for %s",
			ErrOpNotFound, name, version, serverPlatform().Key())
	}

	target := fmt.Sprintf("%s@%s", name, resolved.Version.Version)

	tmpDir := filepath.Join(h.dataDir, "registry-cache", "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return RuntimeInfo{}, fmt.Errorf("%w: creating download dir: %v", ErrOpRegistry, err)
	}
	destPath := filepath.Join(tmpDir, name+".mass")
	defer func() { _ = os.Remove(destPath) }()

	if err := registry.Download(ctx, nil, resolved.Artifact, destPath); err != nil {
		h.logger.Warn().Err(err).Str("package", target).Msg("downloading runtime artifact")
		audit.Log(h.logger, "runtime.installed", target, audit.OutcomeError).
			Str("actor", actor).Str("source", "registry").Str("error", err.Error()).Msg("")
		return RuntimeInfo{}, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}

	mf, err := h.runtimes.InstallFromPath(destPath)
	if err != nil {
		audit.Log(h.logger, "runtime.installed", target, audit.OutcomeError).
			Str("actor", actor).Str("source", "registry").Str("error", err.Error()).Msg("")
		return RuntimeInfo{}, err
	}

	audit.Log(h.logger, "runtime.installed", target, audit.OutcomeOK).
		Str("actor", actor).Str("source", "registry").Msg("")
	return RuntimeInfo{
		RuntimeName: mf.RuntimeName,
		Version:     mf.Version,
		DisplayName: mf.DisplayName,
		Description: mf.Description,
		AutoStart:   mf.AutoStart,
		Running:     h.runtimes.IsRunning(mf.RuntimeName),
	}, nil
}

// resolveWorkerArtifact resolves the newest worker installer artifact for the
// installed runtime named runtimeName on os/arch. The installed runtime's
// version is the join key into each worker version's runtime range, and MASS's
// own version into each version's mass range.
//
// Package selection: when workerPkg is empty the worker packages joined to the
// runtime that actually resolve for this os/arch (any backend) are collected;
// exactly one ⇒ used; more than one ⇒ errAmbiguousWorker (with the candidate
// package names returned); none ⇒ registry.ErrNotResolved. When workerPkg is set
// it must name a worker package for this runtime, else an error.
//
// Backend selection (within the chosen package): when backend is empty the
// distinct backends with an artifact for os/arch across the package's
// compatible versions are collected; exactly one ⇒ used; more than one ⇒
// errAmbiguousBackend (with the list returned); none ⇒ registry.ErrNotResolved.
// When backend is set it is used directly.
//
// Returns the resolved artifact and, on errAmbiguousBackend / errAmbiguousWorker,
// the candidate backend / package-name list.
func (h *Handler) resolveWorkerArtifact(ctx context.Context, runtimeName, goos, goarch, backend, workerPkg string) (*registry.Resolved, []string, error) {
	if h.runtimes == nil {
		return nil, nil, fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	mf, err := h.runtimes.Get(runtimeName)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %s", errRuntimeNotInstalled, runtimeName)
	}

	client, err := h.registryClient()
	if err != nil {
		return nil, nil, err
	}
	res, err := client.Fetch(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("fetching registry index")
		return nil, nil, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}
	idx := res.Index
	massVersion := h.massVersionForResolve()

	pkgName, candidates, err := h.chooseWorkerPackage(idx, runtimeName, goos, goarch, workerPkg, mf.Version, massVersion)
	if err != nil {
		return nil, candidates, err
	}

	if backend == "" {
		backends := compatibleBackends(idx, pkgName, mf.Version, massVersion, goos, goarch)
		switch len(backends) {
		case 0:
			return nil, nil, fmt.Errorf("%w: no worker for %s on %s/%s (runtime %s)",
				registry.ErrNotResolved, pkgName, goos, goarch, mf.Version)
		case 1:
			backend = backends[0]
		default:
			return nil, backends, errAmbiguousBackend
		}
	}

	resolved, err := idx.ResolveWorker(pkgName,
		registry.WorkerPlatform(goos, goarch, backend), mf.Version, massVersion)
	if err != nil {
		return nil, nil, err
	}
	return resolved, nil, nil
}

// chooseWorkerPackage picks the worker package to install for runtimeName on
// os/arch. When workerPkg is set it is validated to be a worker package joined
// to the runtime and returned. When empty, the worker packages that actually
// resolve for the os/arch (any backend) given the installed runtime + MASS
// versions are collected: exactly one ⇒ its name; several ⇒ errAmbiguousWorker
// with the sorted candidate names; none ⇒ registry.ErrNotResolved.
func (h *Handler) chooseWorkerPackage(idx *registry.Index, runtimeName, goos, goarch, workerPkg, runtimeVersion, massVersion string) (string, []string, error) {
	if workerPkg != "" {
		pkg := idx.FindPackage(workerPkg)
		if pkg == nil || pkg.Kind != registry.KindWorker || pkg.RuntimeName != runtimeName {
			return "", nil, fmt.Errorf("%w: %q is not a worker package for runtime %s",
				registry.ErrNotResolved, workerPkg, runtimeName)
		}
		return workerPkg, nil, nil
	}

	var resolvable []string
	for _, pkg := range idx.WorkerPackagesFor(runtimeName) {
		if len(compatibleBackends(idx, pkg.Name, runtimeVersion, massVersion, goos, goarch)) > 0 {
			resolvable = append(resolvable, pkg.Name)
		}
	}
	switch len(resolvable) {
	case 0:
		return "", nil, fmt.Errorf("%w: no worker package for %s on %s/%s (runtime %s)",
			registry.ErrNotResolved, runtimeName, goos, goarch, runtimeVersion)
	case 1:
		return resolvable[0], nil, nil
	default:
		sort.Strings(resolvable)
		return "", resolvable, errAmbiguousWorker
	}
}

// compatibleBackends returns the distinct, sorted backends that have a worker
// artifact for os/arch among the versions of worker package pkgName compatible
// with the installed runtime + MASS versions.
func compatibleBackends(idx *registry.Index, pkgName, runtimeVersion, massVersion, goos, goarch string) []string {
	workers, err := idx.CompatibleWorkers(pkgNameToRuntime(idx, pkgName), runtimeVersion, massVersion)
	if err != nil {
		return nil
	}
	prefix := goos + "/" + goarch + "/"
	seen := make(map[string]bool)
	var out []string
	for _, cw := range workers {
		if cw.Package.Name != pkgName {
			continue
		}
		for key := range cw.Version.Artifacts {
			if b, ok := strings.CutPrefix(key, prefix); ok && b != "" && !seen[b] {
				seen[b] = true
				out = append(out, b)
			}
		}
	}
	sort.Strings(out)
	return out
}

// pkgNameToRuntime returns the runtime_name of the named package, or "" if not
// found. compatibleBackends uses it to scope CompatibleWorkers to the right
// runtime before filtering by package.
func pkgNameToRuntime(idx *registry.Index, pkgName string) string {
	if p := idx.FindPackage(pkgName); p != nil {
		return p.RuntimeName
	}
	return ""
}

// WorkerOption is one worker package the operator can pick for a runtime: its
// package name, human display name, and the distinct backends its resolvable
// versions advertise across all os/arch keys (a union for display — the setup
// dialog does not know the target host's platform).
type WorkerOption struct {
	Name        string
	DisplayName string
	Backends    []string
}

// workerOptionsFor lists the worker packages resolvable for the installed
// runtime named runtimeName — one WorkerOption per package whose versions
// satisfy the installed runtime + MASS versions. Backends is the sorted, distinct
// set of backends across ALL os/arch artifact keys of those versions (a union,
// since the target host platform is unknown at dialog time). Transport-neutral,
// like the other ops funcs; the dashboard UI calls it to populate the setup
// dialog.
func (h *Handler) workerOptionsFor(ctx context.Context, runtimeName string) ([]WorkerOption, error) {
	if h.runtimes == nil {
		return nil, fmt.Errorf("%w: runtimes manager", ErrOpUnavailable)
	}
	mf, err := h.runtimes.Get(runtimeName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errRuntimeNotInstalled, runtimeName)
	}

	client, err := h.registryClient()
	if err != nil {
		return nil, err
	}
	res, err := client.Fetch(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("fetching registry index")
		return nil, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}
	idx := res.Index

	workers, err := idx.CompatibleWorkers(runtimeName, mf.Version, h.massVersionForResolve())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpRegistry, err)
	}

	// Group compatible versions by package, unioning backends across every
	// artifact key (os/arch/backend) of each version.
	backendsByPkg := make(map[string]map[string]bool)
	var order []string
	for _, cw := range workers {
		set, seen := backendsByPkg[cw.Package.Name]
		if !seen {
			set = make(map[string]bool)
			backendsByPkg[cw.Package.Name] = set
			order = append(order, cw.Package.Name)
		}
		for key := range cw.Version.Artifacts {
			if parts := strings.Split(key, "/"); len(parts) == 3 && parts[2] != "" {
				set[parts[2]] = true
			}
		}
	}

	sort.Strings(order)
	out := make([]WorkerOption, 0, len(order))
	for _, name := range order {
		backends := make([]string, 0, len(backendsByPkg[name]))
		for b := range backendsByPkg[name] {
			backends = append(backends, b)
		}
		sort.Strings(backends)
		pkg := idx.FindPackage(name)
		display := name
		if pkg != nil && pkg.DisplayName != "" {
			display = pkg.DisplayName
		}
		out = append(out, WorkerOption{Name: name, DisplayName: display, Backends: backends})
	}
	return out, nil
}

// workerCompat is one connected worker's runtime pairing and declared
// compatible range, the inputs to the pre-upgrade fleet check.
type workerCompat struct {
	RuntimeName string
	Compatible  string
}

// fleetCompat snapshots every connected worker's runtime_name + compatible
// range for the pre-upgrade flag. Returns nil when no fleet is wired.
func (h *Handler) fleetCompat() []workerCompat {
	if h.workers == nil {
		return nil
	}
	all := h.workers.All()
	out := make([]workerCompat, 0, len(all))
	for _, wkr := range all {
		out = append(out, workerCompat{RuntimeName: wkr.RuntimeName(), Compatible: wkr.Compatible()})
	}
	return out
}

// isNewerVersion reports whether candidate is strictly newer than installed by
// semver. Either side unparseable ⇒ false (no reliable "newer" claim, so no
// upgrade flag).
func isNewerVersion(candidate, installed string) bool {
	c, err := semver.NewVersion(candidate)
	if err != nil {
		return false
	}
	i, err := semver.NewVersion(installed)
	if err != nil {
		return false
	}
	return c.GreaterThan(i)
}

// countIncompatibleWorkers reports how many connected workers paired with
// runtimeName declare a compatible range that excludes candidateVersion — the
// workers a runtime upgrade to candidateVersion would strand. It is a plain
// semver computation over the fleet, table-tested independently of any
// transport.
//
// A worker with an empty range counts as incompatible: the hub now requires
// every worker to declare a compatible range at handshake, so a connected
// worker can't have an empty one — an empty value here means something is off,
// and flagging it is the safe, operator-actionable call. A worker whose range
// fails to parse is counted for the same reason: an unparseable range can't be
// shown to cover the new version. candidateVersion must be valid semver (it
// comes from the validated index); an unparseable candidate yields an error so
// the caller can skip the flag rather than miscount.
func countIncompatibleWorkers(workers []workerCompat, runtimeName, candidateVersion string) (int, error) {
	candidate, err := semver.NewVersion(candidateVersion)
	if err != nil {
		return 0, fmt.Errorf("parsing candidate version %q: %w", candidateVersion, err)
	}
	count := 0
	for _, wc := range workers {
		if wc.RuntimeName != runtimeName {
			continue
		}
		if wc.Compatible == "" {
			count++
			continue
		}
		constraint, err := semver.NewConstraint(wc.Compatible)
		if err != nil || !constraint.Check(candidate) {
			count++
		}
	}
	return count, nil
}

// massVersionForResolve returns MASS's own version for the min_mass check, or a
// permissive placeholder when the build version is a non-semver dev string
// (e.g. "dev") — in which case min_mass is treated as satisfied. Logs the
// substitution once at debug.
func (h *Handler) massVersionForResolve() string {
	if _, err := semver.NewVersion(h.version); err == nil {
		return h.version
	}
	logDevVersionOnce.Do(func() {
		h.logger.Debug().Str("version", h.version).
			Msg("MASS version is not semver; treating registry min_mass as satisfied")
	})
	return unresolvableMassVersion
}
