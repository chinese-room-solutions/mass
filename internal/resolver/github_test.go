package resolver

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGitHubProviderFactoryValid(t *testing.T) {
	f := &GitHubProviderFactory{}

	p, err := f.ProviderFor("github:owner/repo")
	require.NoError(t, err)
	require.NotNil(t, p)

	gp := p.(*GitHubProvider)
	require.Equal(t, "owner", gp.Owner)
	require.Equal(t, "repo", gp.Repo)
}

func TestGitHubProviderFactoryInvalid(t *testing.T) {
	f := &GitHubProviderFactory{}

	tests := []struct {
		name   string
		source string
	}{
		{"no prefix", "owner/repo"},
		{"empty owner", "github:/repo"},
		{"empty repo", "github:owner/"},
		{"no slash", "github:owner"},
		{"http prefix", "http://example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.ProviderFor(tt.source)
			require.Error(t, err)
		})
	}
}

func TestGitHubProviderFactoryWithToken(t *testing.T) {
	f := &GitHubProviderFactory{Token: "ghp_testtoken123"}

	p, err := f.ProviderFor("github:owner/repo")
	require.NoError(t, err)

	gp := p.(*GitHubProvider)
	require.Equal(t, "ghp_testtoken123", gp.Token)
}

func TestFindPlatformAsset(t *testing.T) {
	suffix := runtime.GOOS + "_" + runtime.GOARCH
	assets := []githubAsset{
		{Name: "app_linux_amd64.mass", BrowserDownloadURL: "https://example.com/linux_amd64"},
		{Name: "app_windows_amd64.mass", BrowserDownloadURL: "https://example.com/windows_amd64"},
		{Name: "app_darwin_arm64.mass", BrowserDownloadURL: "https://example.com/darwin_arm64"},
		{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums"},
	}

	url, err := findPlatformAsset(assets)
	require.NoError(t, err)
	require.Contains(t, url, suffix)
}

func TestFindPlatformAssetNotFound(t *testing.T) {
	assets := []githubAsset{
		{Name: "app_fakeos_fakearch.mass", BrowserDownloadURL: "https://example.com/fake"},
	}

	_, err := findPlatformAsset(assets)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no compatible asset")
}

func TestFindPlatformAssetZipExtension(t *testing.T) {
	suffix := runtime.GOOS + "_" + runtime.GOARCH
	assets := []githubAsset{
		{Name: "app_" + suffix + ".zip", BrowserDownloadURL: "https://example.com/zip"},
	}

	url, err := findPlatformAsset(assets)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/zip", url)
}
