package resolver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/chinese-room-solutions/mass/internal/installer"
	"github.com/rs/zerolog"
)

// Dependency declares a required app with a semver constraint and source.
type Dependency struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`          // semver constraint, e.g. ">=0.5.0", "^1.2.0"
	Source  string `yaml:"source,omitempty"` // e.g. "github:owner/repo"
}

// ResolvedApp is the result of dependency resolution for a single app.
type ResolvedApp struct {
	Name    string          `yaml:"name"`
	Version *semver.Version `yaml:"-"`
	Source  string          `yaml:"source"`
	// Installed is true if a matching version was already on disk.
	Installed bool `yaml:"-"`
}

// Resolver performs dependency resolution: it builds a dependency graph,
// checks installed versions, queries providers for missing ones, and
// returns the full set of resolved apps.
type Resolver struct {
	installDir string
	factory    ProviderFactoryInterface
	logger     zerolog.Logger
}

// NewResolver creates a Resolver.
// installDir is the apps root directory (e.g. {dataDir}/apps).
func NewResolver(installDir string, factory ProviderFactoryInterface, logger zerolog.Logger) *Resolver {
	return &Resolver{
		installDir: installDir,
		factory:    factory,
		logger:     logger,
	}
}

// Resolve takes a set of top-level dependencies and returns the full resolved
// set (including transitive dependencies). It checks installed versions first
// and only queries providers when no installed version satisfies a constraint.
func (r *Resolver) Resolve(ctx context.Context, deps []Dependency) ([]ResolvedApp, error) {
	// Build the full dependency graph (including transitive deps).
	graph, err := r.buildGraph(ctx, deps)
	if err != nil {
		return nil, fmt.Errorf("building dependency graph: %w", err)
	}

	// Topological sort to detect cycles and determine install order.
	order, err := topoSort(graph)
	if err != nil {
		return nil, err
	}

	// Merge constraints for each app from all dependants AND the
	// top-level deps list (which acts as an implicit root node).
	merged, err := mergeConstraints(graph, deps)
	if err != nil {
		return nil, err
	}

	// For each app (in install order), pick the best version.
	var result []ResolvedApp
	for _, name := range order {
		mc := merged[name]
		resolved, err := r.resolveOne(ctx, name, mc)
		if err != nil {
			return nil, fmt.Errorf("resolving %s: %w", name, err)
		}
		result = append(result, resolved)
	}

	return result, nil
}

// Install downloads and extracts all uninstalled resolved apps.
func (r *Resolver) Install(ctx context.Context, apps []ResolvedApp) error {
	for _, m := range apps {
		if m.Installed {
			r.logger.Debug().Str("app", m.Name).Str("version", m.Version.String()).Msg("already installed")
			continue
		}

		if err := r.installOne(ctx, m); err != nil {
			return fmt.Errorf("installing %s@%s: %w", m.Name, m.Version, err)
		}
	}
	return nil
}

// mergedConstraint holds all constraints for a single app and the source
// to fetch it from.
type mergedConstraint struct {
	constraints []*semver.Constraints
	rawVersions []string // original constraint strings for error messages
	source      string
}

// graphNode represents a app in the dependency graph.
type graphNode struct {
	name   string
	deps   []Dependency
	source string
}

// buildGraph recursively resolves the dependency tree. It reads app.yml
// from installed apps (or queries providers) to discover transitive deps.
func (r *Resolver) buildGraph(_ context.Context, rootDeps []Dependency) (map[string]*graphNode, error) {
	graph := make(map[string]*graphNode)
	return graph, r.walkDeps(rootDeps, graph)
}

func (r *Resolver) walkDeps(deps []Dependency, graph map[string]*graphNode) error {
	for _, dep := range deps {
		if _, exists := graph[dep.Name]; exists {
			continue // already visited
		}

		node := &graphNode{
			name:   dep.Name,
			source: dep.Source,
		}
		graph[dep.Name] = node

		// Try to read transitive deps from an installed version.
		transDeps := r.readInstalledDeps(dep.Name)
		if transDeps == nil && dep.Source != "" {
			// App not installed — we'll resolve its deps after install.
			// For now, add a node with no children. Transitive deps of
			// uninstalled apps will be discovered during install.
			continue
		}

		node.deps = transDeps
		if err := r.walkDeps(transDeps, graph); err != nil {
			return err
		}
	}
	return nil
}

// readInstalledDeps reads app.yml from all installed versions of the named
// app and returns dependencies from the latest version. Returns nil if none
// are installed.
func (r *Resolver) readInstalledDeps(name string) []Dependency {
	appDir := filepath.Join(r.installDir, name)
	entries, err := os.ReadDir(appDir)
	if err != nil {
		return nil
	}

	var latestMeta *installer.AppMetadata
	var latestVer *semver.Version

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		v, err := semver.NewVersion(e.Name())
		if err != nil {
			continue
		}
		meta, err := installer.ReadMetadataFromDir(filepath.Join(appDir, e.Name()))
		if err != nil {
			continue
		}
		if latestVer == nil || v.GreaterThan(latestVer) {
			latestVer = v
			latestMeta = meta
		}
	}

	if latestMeta == nil {
		return nil
	}

	var deps []Dependency
	for _, d := range latestMeta.Dependencies {
		deps = append(deps, Dependency{
			Name:    d.Name,
			Version: d.Version,
			Source:  d.Source,
		})
	}
	return deps
}

// topoSort returns app names in topological order (dependencies first).
func topoSort(graph map[string]*graphNode) ([]string, error) {
	const (
		white = 0 // unvisited
		gray  = 1 // in progress
		black = 2 // done
	)

	color := make(map[string]int, len(graph))
	var order []string

	var visit func(name string) error
	visit = func(name string) error {
		switch color[name] {
		case gray:
			return fmt.Errorf("dependency cycle detected involving %q", name)
		case black:
			return nil
		}
		color[name] = gray

		if node, ok := graph[name]; ok {
			for _, dep := range node.deps {
				if err := visit(dep.Name); err != nil {
					return err
				}
			}
		}

		color[name] = black
		order = append(order, name)
		return nil
	}

	// Sort keys for deterministic output.
	names := make([]string, 0, len(graph))
	for name := range graph {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// mergeConstraints collects all version constraints for each app across
// the entire graph plus the top-level dependency list (which acts as an
// implicit root node).
func mergeConstraints(graph map[string]*graphNode, rootDeps []Dependency) (map[string]*mergedConstraint, error) {
	result := make(map[string]*mergedConstraint)

	// Ensure every node has an entry.
	for name, node := range graph {
		if _, ok := result[name]; !ok {
			result[name] = &mergedConstraint{source: node.source}
		}
	}

	// addDep applies a single dependency's constraint to the merged set.
	addDep := func(dep Dependency) error {
		mc, ok := result[dep.Name]
		if !ok {
			mc = &mergedConstraint{}
			result[dep.Name] = mc
		}
		if dep.Version != "" {
			c, err := semver.NewConstraint(dep.Version)
			if err != nil {
				return fmt.Errorf("invalid constraint %q for %s: %w", dep.Version, dep.Name, err)
			}
			mc.constraints = append(mc.constraints, c)
			mc.rawVersions = append(mc.rawVersions, dep.Version)
		}
		if mc.source == "" && dep.Source != "" {
			mc.source = dep.Source
		}
		return nil
	}

	// Collect constraints from top-level deps.
	for _, dep := range rootDeps {
		if err := addDep(dep); err != nil {
			return nil, err
		}
	}

	// Collect constraints from all graph nodes' dependants.
	for _, node := range graph {
		for _, dep := range node.deps {
			if err := addDep(dep); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// resolveOne picks the best version for a single app.
func (r *Resolver) resolveOne(ctx context.Context, name string, mc *mergedConstraint) (ResolvedApp, error) {
	// 1. Check installed versions.
	installed := r.listInstalledVersions(name)
	if v := pickBest(installed, mc.constraints); v != nil {
		return ResolvedApp{
			Name:      name,
			Version:   v,
			Source:    mc.source,
			Installed: true,
		}, nil
	}

	// 2. No installed version fits — query the provider.
	if mc.source == "" {
		return ResolvedApp{}, fmt.Errorf("no installed version satisfies constraints (%s) and no source configured",
			strings.Join(mc.rawVersions, ", "))
	}

	provider, err := r.factory.ProviderFor(mc.source)
	if err != nil {
		return ResolvedApp{}, err
	}

	available, err := provider.ListVersions(ctx, name)
	if err != nil {
		return ResolvedApp{}, fmt.Errorf("listing remote versions: %w", err)
	}

	v := pickBest(available, mc.constraints)
	if v == nil {
		return ResolvedApp{}, fmt.Errorf("no remote version satisfies constraints (%s), available: %s",
			strings.Join(mc.rawVersions, ", "), versionList(available))
	}

	return ResolvedApp{
		Name:      name,
		Version:   v,
		Source:    mc.source,
		Installed: false,
	}, nil
}

// listInstalledVersions scans apps/{name}/ for version subdirectories.
func (r *Resolver) listInstalledVersions(name string) []*semver.Version {
	appDir := filepath.Join(r.installDir, name)
	entries, err := os.ReadDir(appDir)
	if err != nil {
		return nil
	}

	var versions []*semver.Version
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		v, err := semver.NewVersion(e.Name())
		if err != nil {
			continue
		}
		// Verify app.yml exists in this version dir.
		if _, err := installer.ReadMetadataFromDir(filepath.Join(appDir, e.Name())); err != nil {
			continue
		}
		versions = append(versions, v)
	}
	return versions
}

// pickBest returns the highest version that satisfies all constraints.
// Returns nil if no version matches.
func pickBest(versions []*semver.Version, constraints []*semver.Constraints) *semver.Version {
	if len(versions) == 0 {
		return nil
	}

	// Sort descending (highest first).
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].GreaterThan(versions[j])
	})

	for _, v := range versions {
		if matchesAll(v, constraints) {
			return v
		}
	}
	return nil
}

func matchesAll(v *semver.Version, constraints []*semver.Constraints) bool {
	for _, c := range constraints {
		if !c.Check(v) {
			return false
		}
	}
	return true
}

// installOne downloads and extracts a single app version.
func (r *Resolver) installOne(ctx context.Context, m ResolvedApp) error {
	provider, err := r.factory.ProviderFor(m.Source)
	if err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp("", "mass-module-*.mass")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()          //nolint:errcheck
	defer os.Remove(tmpPath) //nolint:errcheck

	r.logger.Info().
		Str("app", m.Name).
		Str("version", m.Version.String()).
		Str("source", m.Source).
		Msg("downloading app")

	if err := provider.Download(ctx, m.Name, m.Version, tmpPath); err != nil {
		return err
	}

	// Extract to apps/{name}/{version}/
	destDir := filepath.Join(r.installDir, m.Name, m.Version.String())
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("creating install dir: %w", err)
	}

	if err := installer.ExtractZip(tmpPath, destDir); err != nil {
		if rmErr := os.RemoveAll(destDir); rmErr != nil {
			r.logger.Warn().Err(rmErr).Str("dest", destDir).Msg("cleaning up after failed extract")
		}
		return fmt.Errorf("extracting: %w", err)
	}

	r.logger.Info().
		Str("app", m.Name).
		Str("version", m.Version.String()).
		Str("dir", destDir).
		Msg("app installed")

	return nil
}

func versionList(versions []*semver.Version) string {
	if len(versions) == 0 {
		return "(none)"
	}
	s := make([]string, len(versions))
	for i, v := range versions {
		s[i] = v.String()
	}
	return strings.Join(s, ", ")
}
