package resolver

import (
	"context"

	"github.com/Masterminds/semver/v3"
)

// ProviderInterface abstracts a module source that can list available versions
// and download specific versions. Implementations include GitHub Releases,
// HTTP index, or other registries.
type ProviderInterface interface {
	// ListVersions returns all available versions for the given module.
	ListVersions(ctx context.Context, module string) ([]*semver.Version, error)

	// Download fetches a .mass package for the given module version and writes
	// it to the file at dstPath.
	Download(ctx context.Context, module string, version *semver.Version, dstPath string) error
}

// ProviderFactoryInterface creates providers from a source string (e.g.
// "github:owner/repo"). This lets the resolver support multiple provider
// types without hardcoding any.
type ProviderFactoryInterface interface {
	// ProviderFor returns a ProviderInterface for the given source string
	// and the canonical module identifier within that provider.
	// For example, source "github:owner/repo" returns a GitHub provider
	// configured for owner/repo.
	ProviderFor(source string) (ProviderInterface, error)
}
