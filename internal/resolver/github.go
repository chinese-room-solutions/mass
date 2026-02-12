package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/chinese-room-solutions/mass/pkg/download"
)

// GitHubProvider implements ProviderInterface using GitHub Releases API.
type GitHubProvider struct {
	Owner  string
	Repo   string
	Client *http.Client
	Token  string // optional, for private repos
}

// githubRelease is the subset of the GitHub Release API response we need.
type githubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []githubAsset `json:"assets"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// ListVersions queries the GitHub Releases API and returns all valid semver
// versions for the repository.
func (p *GitHubProvider) ListVersions(ctx context.Context, _ string) ([]*semver.Version, error) {
	releases, err := p.listReleases(ctx)
	if err != nil {
		return nil, err
	}

	var versions []*semver.Version
	for _, r := range releases {
		tag := strings.TrimPrefix(r.TagName, "v")
		v, err := semver.NewVersion(tag)
		if err != nil {
			continue // skip non-semver tags
		}
		versions = append(versions, v)
	}
	return versions, nil
}

// Download finds the platform-matching asset for the given version and
// downloads it to dstPath.
func (p *GitHubProvider) Download(ctx context.Context, _ string, version *semver.Version, dstPath string) error {
	tag := "v" + version.String()
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/tags/%s", p.Owner, p.Repo, tag)

	var release githubRelease
	if err := p.doJSON(ctx, apiURL, &release); err != nil {
		return fmt.Errorf("fetching release %s: %w", tag, err)
	}

	downloadURL, err := findPlatformAsset(release.Assets)
	if err != nil {
		return fmt.Errorf("release %s: %w", tag, err)
	}

	return p.downloadFile(ctx, downloadURL, dstPath)
}

func (p *GitHubProvider) listReleases(ctx context.Context) ([]githubRelease, error) {
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases?per_page=100", p.Owner, p.Repo)

	var releases []githubRelease
	if err := p.doJSON(ctx, apiURL, &releases); err != nil {
		return nil, fmt.Errorf("listing releases for %s/%s: %w", p.Owner, p.Repo, err)
	}
	return releases, nil
}

func (p *GitHubProvider) doJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if p.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.Token)
	}

	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func (p *GitHubProvider) downloadFile(ctx context.Context, url, dstPath string) error {
	opts := []download.Option{
		download.WithMaxRetries(3),
		download.WithResume(false),
	}
	if p.Token != "" {
		opts = append(opts, download.WithHeaders(http.Header{
			"Authorization": {"Bearer " + p.Token},
		}))
	}

	mgr := download.NewManager(p.Client)
	return mgr.Download(ctx, url, dstPath, opts...)
}

// findPlatformAsset finds a release asset matching the current OS and arch.
// Asset names should contain "{os}_{arch}" and end with ".mass" or ".zip".
func findPlatformAsset(assets []githubAsset) (string, error) {
	suffix := fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
	for _, a := range assets {
		lower := strings.ToLower(a.Name)
		if strings.Contains(lower, strings.ToLower(suffix)) &&
			(strings.HasSuffix(lower, ".mass") || strings.HasSuffix(lower, ".zip")) {
			return a.BrowserDownloadURL, nil
		}
	}
	return "", fmt.Errorf("no compatible asset for %s/%s", runtime.GOOS, runtime.GOARCH)
}

// GitHubProviderFactory creates GitHubProvider instances from source strings.
type GitHubProviderFactory struct {
	Client *http.Client
	Token  string // global token for all GitHub requests
}

// ProviderFor parses a "github:owner/repo" source string and returns a
// configured GitHubProvider.
func (f *GitHubProviderFactory) ProviderFor(source string) (ProviderInterface, error) {
	if !strings.HasPrefix(source, "github:") {
		return nil, fmt.Errorf("unsupported source %q: only github: prefix is supported", source)
	}

	ref := strings.TrimPrefix(source, "github:")
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid github source %q: expected github:owner/repo", source)
	}

	return &GitHubProvider{
		Owner:  parts[0],
		Repo:   parts[1],
		Client: f.Client,
		Token:  f.Token,
	}, nil
}
