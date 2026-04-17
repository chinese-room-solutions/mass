package installer

import (
	"archive/tar"
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/Masterminds/semver/v3"
	"github.com/rs/zerolog"

	"github.com/chinese-room-solutions/mass/pkg/download"
)

// AppInfo mirrors the registry API response for a single app.
type AppInfo struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Versions    []VersionInfo `json:"versions"`
}

// VersionInfo mirrors the registry API response for a app version.
type VersionInfo struct {
	Version   string         `json:"version"`
	Platforms []PlatformInfo `json:"platforms"`
}

// PlatformInfo mirrors the registry API response for a platform build.
type PlatformInfo struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

// Installer downloads and extracts apps from a registry.
type Installer struct {
	RegistryURL string
	InstallDir  string
	Logger      zerolog.Logger
	Client      *http.Client
}

// NewInstaller creates an Installer. If installDir is empty, it defaults to
// ~/.config/mass/apps.
func NewInstaller(registryURL, installDir string, logger zerolog.Logger) *Installer {
	if installDir == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			home, _ := os.UserHomeDir()
			if home == "" {
				home = "."
			}
			cacheDir = home
		}
		installDir = filepath.Join(cacheDir, "mass", "apps")
	}
	return &Installer{
		RegistryURL: strings.TrimRight(registryURL, "/"),
		InstallDir:  installDir,
		Logger:      logger,
		Client:      &http.Client{},
	}
}

// Resolve returns the command for the app, installing it first if needed.
func (inst *Installer) Resolve(ctx context.Context, name, version string) (string, error) {
	// Check if already installed.
	dir := filepath.Join(inst.InstallDir, name, version)
	meta, err := ReadMetadataFromDir(dir)
	if err == nil && meta.Command != "" {
		inst.Logger.Debug().
			Str("app", name).Str("version", version).Str("command", meta.Command).
			Msg("app already installed")
		return meta.Command, nil
	}

	return inst.Install(ctx, name, version)
}

// Install downloads and extracts a app, returning its command.
func (inst *Installer) Install(ctx context.Context, name, version string) (string, error) {
	inst.Logger.Info().
		Str("app", name).Str("version", version).
		Str("os", runtime.GOOS).Str("arch", runtime.GOARCH).
		Msg("downloading app from registry")

	dlURL := fmt.Sprintf("%s/api/v1/apps/%s/%s/download?os=%s&arch=%s",
		inst.RegistryURL, name, version, runtime.GOOS, runtime.GOARCH)

	tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("mass-module-%s-%s.zip", name, version))
	defer os.Remove(tmpPath) //nolint:errcheck // best-effort cleanup

	mgr := download.NewManager(inst.Client)
	if err := mgr.Download(ctx, dlURL, tmpPath, download.WithMaxRetries(3)); err != nil {
		return "", ctxerr.With(fmt.Errorf("downloading app %s@%s: %w", name, version, err), map[string]any{"app": name, "version": version, "url": dlURL})
	}

	// Extract to install directory.
	destDir := filepath.Join(inst.InstallDir, name, version)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", fmt.Errorf("creating install directory: %w", err)
	}

	if err := ExtractZip(tmpPath, destDir); err != nil {
		if rmErr := os.RemoveAll(destDir); rmErr != nil {
			inst.Logger.Warn().Err(rmErr).Str("dest", destDir).Msg("cleaning up after failed extract")
		}
		return "", ctxerr.With(fmt.Errorf("extracting app: %w", err), map[string]any{"app": name, "version": version, "dest": destDir})
	}

	meta, err := ReadMetadataFromDir(destDir)
	if err != nil {
		return "", ctxerr.With(fmt.Errorf("reading metadata after extraction: %w", err), map[string]any{"app": name, "version": version, "dest": destDir})
	}

	inst.Logger.Info().
		Str("app", name).Str("version", version).Str("command", meta.Command).
		Msg("app installed")

	return meta.Command, nil
}

// List queries the registry for all available apps.
func (inst *Installer) List(ctx context.Context) ([]AppInfo, error) {
	url := inst.RegistryURL + "/api/v1/apps"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := inst.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying registry: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort cleanup

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned %d", resp.StatusCode)
	}

	var apps []AppInfo
	if err := json.NewDecoder(resp.Body).Decode(&apps); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return apps, nil
}

// modelExtensions lists file extensions treated as model files during installation.
var modelExtensions = []string{".gguf"}

// relocateModels moves model files from srcDir to destDir.
// Returns the number of files moved.
func relocateModels(srcDir, destDir string, logger zerolog.Logger) (int, error) {
	var moved int
	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		lower := strings.ToLower(d.Name())
		isModel := false
		for _, ext := range modelExtensions {
			if strings.HasSuffix(lower, ext) {
				isModel = true
				break
			}
		}
		if !isModel {
			return nil
		}

		if err := os.MkdirAll(destDir, 0755); err != nil {
			return fmt.Errorf("creating models dir: %w", err)
		}

		dst := filepath.Join(destDir, d.Name())
		if err := os.Rename(path, dst); err != nil {
			return fmt.Errorf("moving %s: %w", d.Name(), err)
		}
		moved++
		return nil
	})
	if err != nil {
		return moved, err
	}

	// Remove empty models subdirectories left behind after relocation.
	// Failure is expected when the dir still has content — that's fine, leave it.
	modelsSubDir := filepath.Join(srcDir, "models")
	if info, err := os.Stat(modelsSubDir); err == nil && info.IsDir() {
		if rmErr := os.Remove(modelsSubDir); rmErr != nil {
			logger.Debug().Err(rmErr).Str("path", modelsSubDir).Msg("models subdir not empty, leaving in place")
		}
	}

	return moved, nil
}

// ExtractArchive detects the archive format and extracts to the destination directory.
func ExtractArchive(src, dest string) error {
	format, err := detectFormat(src)
	if err != nil {
		return fmt.Errorf("detecting format: %w", err)
	}
	if format == formatTar {
		return ExtractTar(src, dest)
	}
	return ExtractZip(src, dest)
}

// ExtractZip extracts a zip file to the destination directory.
func ExtractZip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck // read-only close

	for _, f := range r.File {
		target := filepath.Join(dest, f.Name)

		// Guard against zip slip.
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in zip: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return fmt.Errorf("creating directory %s: %w", f.Name, err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}

		outFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close() //nolint:errcheck // best-effort cleanup on error path
			return err
		}

		_, copyErr := io.Copy(outFile, rc)
		if rcErr := rc.Close(); rcErr != nil && copyErr == nil {
			copyErr = rcErr
		}
		if closeErr := outFile.Close(); closeErr != nil && copyErr == nil {
			copyErr = closeErr
		}
		if copyErr != nil {
			return copyErr
		}
	}
	return nil
}

// ExtractTar extracts a tar file to the destination directory.
func ExtractTar(src, dest string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only close

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}

		target := filepath.Join(dest, hdr.Name)

		// Guard against path traversal.
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in tar: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return fmt.Errorf("creating directory %s: %w", hdr.Name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			outFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(outFile, tr)
			if closeErr := outFile.Close(); closeErr != nil && copyErr == nil {
				copyErr = closeErr
			}
			if copyErr != nil {
				return copyErr
			}
		}
	}
	return nil
}

// InstallFromArchive installs a app from a local .mass archive (zip or tar).
// Returns the resolved command and parsed metadata.
func (inst *Installer) InstallFromArchive(archivePath string) (string, *AppMetadata, error) {
	// Read and validate metadata.
	meta, err := ReadMetadataFromArchive(archivePath)
	if err != nil {
		return "", nil, ctxerr.With(fmt.Errorf("reading metadata: %w", err), map[string]any{"archive": archivePath})
	}
	if err := ValidateMetadata(meta); err != nil {
		return "", nil, ctxerr.With(fmt.Errorf("invalid package: %w", err), map[string]any{"archive": archivePath, "app": meta.Name})
	}
	if err := ExecutableExistsInArchive(archivePath, meta.Command); err != nil {
		return "", nil, err
	}

	// Check dependencies.
	if len(meta.Dependencies) > 0 {
		if err := inst.CheckDependencies(meta.Dependencies); err != nil {
			return "", nil, err
		}
	}

	// Extract to install directory: apps/{name}/{version}/
	destDir := filepath.Join(inst.InstallDir, meta.Name, meta.Version)

	// Remove existing installation of the same version if upgrading.
	// On Windows, file locks from recently killed processes may linger briefly,
	// so we retry a few times before giving up.
	if _, err := os.Stat(destDir); err == nil {
		inst.Logger.Info().Str("app", meta.Name).Str("version", meta.Version).Msg("removing existing version for upgrade")
		if err := removeWithRetry(destDir, 5, 500*time.Millisecond); err != nil {
			return "", nil, ctxerr.With(fmt.Errorf("removing existing installation: %w", err), map[string]any{"app": meta.Name, "version": meta.Version, "dest": destDir})
		}
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", nil, fmt.Errorf("creating install directory: %w", err)
	}

	if err := ExtractArchive(archivePath, destDir); err != nil {
		if rmErr := os.RemoveAll(destDir); rmErr != nil {
			inst.Logger.Warn().Err(rmErr).Str("dest", destDir).Msg("cleaning up after failed archive extract")
		}
		return "", nil, ctxerr.With(fmt.Errorf("extracting archive: %w", err), map[string]any{"archive": archivePath, "app": meta.Name, "dest": destDir})
	}

	// Move model files (.gguf) to the centralized models directory.
	// InstallDir is {dataDir}/apps, so models dir is {dataDir}/models/{name}.
	dataDir := filepath.Dir(inst.InstallDir)
	modelsDir := filepath.Join(dataDir, "models", meta.Name)
	if n, err := relocateModels(destDir, modelsDir, inst.Logger); err != nil {
		inst.Logger.Warn().Err(err).Msg("failed to relocate model files")
	} else if n > 0 {
		inst.Logger.Info().Int("count", n).Str("dest", modelsDir).Msg("relocated model files")
	}

	inst.Logger.Info().
		Str("app", meta.Name).
		Str("version", meta.Version).
		Str("command", meta.Command).
		Msg("app installed from archive")

	return meta.Command, meta, nil
}

// removeWithRetry attempts os.RemoveAll up to maxAttempts times, waiting delay
// between attempts. On Windows, recently killed processes may hold file locks
// briefly, causing RemoveAll to fail with "Access is denied".
func removeWithRetry(path string, maxAttempts int, delay time.Duration) error {
	var err error
	for i := range maxAttempts {
		if err = os.RemoveAll(path); err == nil {
			return nil
		}
		if i < maxAttempts-1 {
			time.Sleep(delay)
		}
	}
	return err
}

// InstallFromURL downloads a .mass archive from a URL and installs it.
func (inst *Installer) InstallFromURL(ctx context.Context, dlURL string) (string, *AppMetadata, error) {
	inst.Logger.Info().Str("url", dlURL).Msg("downloading app archive")

	tmpPath := filepath.Join(os.TempDir(), "mass-module-url.mass")
	defer func() {
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			inst.Logger.Debug().Err(rmErr).Str("path", tmpPath).Msg("cleaning up temp archive")
		}
	}()

	// Remove stale temp from a previous attempt so the manager doesn't skip it.
	if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
		inst.Logger.Debug().Err(rmErr).Str("path", tmpPath).Msg("removing stale temp archive")
	}

	mgr := download.NewManager(inst.Client)
	if err := mgr.Download(ctx, dlURL, tmpPath, download.WithMaxRetries(3)); err != nil {
		return "", nil, ctxerr.With(fmt.Errorf("downloading archive: %w", err), map[string]any{"url": dlURL})
	}

	return inst.InstallFromArchive(tmpPath)
}

// InstallFromGitHub installs a app from a GitHub release.
// ref is "owner/repo" or "owner/repo@version".
func (inst *Installer) InstallFromGitHub(ctx context.Context, ref string) (string, *AppMetadata, error) {
	owner, repo, version, err := parseGitHubRef(ref)
	if err != nil {
		return "", nil, err
	}

	dlURL, err := inst.resolveGitHubAssetURL(ctx, owner, repo, version)
	if err != nil {
		return "", nil, err
	}

	inst.Logger.Info().
		Str("owner", owner).Str("repo", repo).Str("url", dlURL).
		Msg("found GitHub release asset")

	return inst.InstallFromURL(ctx, dlURL)
}

// resolveGitHubAssetURL queries the GitHub Releases API and returns the
// download URL for the platform-matching asset.
func (inst *Installer) resolveGitHubAssetURL(ctx context.Context, owner, repo, version string) (string, error) {
	var apiURL string
	if version == "" {
		apiURL = fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", owner, repo)
	} else {
		apiURL = fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/tags/v%s", owner, repo, version)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := inst.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("querying GitHub: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort cleanup

	if resp.StatusCode != http.StatusOK {
		return "", ctxerr.With(fmt.Errorf("GitHub API returned %d for %s/%s", resp.StatusCode, owner, repo), map[string]any{"owner": owner, "repo": repo, "status": resp.StatusCode})
	}

	var release struct {
		Assets []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", fmt.Errorf("parsing GitHub response: %w", err)
	}

	suffix := fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
	for _, asset := range release.Assets {
		lower := strings.ToLower(asset.Name)
		if strings.Contains(lower, strings.ToLower(suffix)) &&
			(strings.HasSuffix(lower, ".mass") || strings.HasSuffix(lower, ".zip")) {
			return asset.BrowserDownloadURL, nil
		}
	}

	return "", ctxerr.With(fmt.Errorf("no compatible asset found for %s/%s (looking for %s)", runtime.GOOS, runtime.GOARCH, suffix), map[string]any{"owner": owner, "repo": repo, "os": runtime.GOOS, "arch": runtime.GOARCH})
}

// ListInstalled returns metadata for all installed apps.
// Scans the two-level directory layout: apps/{name}/{version}/.
func (inst *Installer) ListInstalled() ([]AppMetadata, error) {
	var result []AppMetadata

	appNames, err := os.ReadDir(inst.InstallDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading install dir: %w", err)
	}

	for _, nameEntry := range appNames {
		if !nameEntry.IsDir() {
			continue
		}
		nameDir := filepath.Join(inst.InstallDir, nameEntry.Name())

		// Try two-level layout first: apps/{name}/{version}/app.yml
		versionEntries, err := os.ReadDir(nameDir)
		if err != nil {
			continue
		}

		foundVersioned := false
		for _, vEntry := range versionEntries {
			if !vEntry.IsDir() {
				continue
			}
			meta, err := ReadMetadataFromDir(filepath.Join(nameDir, vEntry.Name()))
			if err != nil {
				continue
			}
			result = append(result, *meta)
			foundVersioned = true
		}

		// Fallback: legacy flat layout apps/{name}/app.yml
		if !foundVersioned {
			meta, err := ReadMetadataFromDir(nameDir)
			if err != nil {
				continue
			}
			result = append(result, *meta)
		}
	}

	return result, nil
}

// CheckDependencies verifies that all required apps are installed and
// satisfy their semver constraints.
func (inst *Installer) CheckDependencies(deps []Dependency) error {
	installed, err := inst.ListInstalled()
	if err != nil {
		return fmt.Errorf("checking installed apps: %w", err)
	}

	// Build map of name -> list of installed versions.
	installedVersions := make(map[string][]string)
	for _, p := range installed {
		installedVersions[p.Name] = append(installedVersions[p.Name], p.Version)
	}

	var problems []string
	for _, dep := range deps {
		versions, ok := installedVersions[dep.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%q %s (not installed)", dep.Name, dep.Version))
			continue
		}

		if dep.Version == "" {
			continue // any version is fine
		}

		constraint, err := semver.NewConstraint(dep.Version)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%q has invalid constraint %q: %v", dep.Name, dep.Version, err))
			continue
		}

		satisfied := false
		for _, vs := range versions {
			v, err := semver.NewVersion(vs)
			if err != nil {
				continue
			}
			if constraint.Check(v) {
				satisfied = true
				break
			}
		}
		if !satisfied {
			problems = append(problems, fmt.Sprintf("%q %s (installed: %s, none match)", dep.Name, dep.Version, strings.Join(versions, ", ")))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("unsatisfied dependencies: %s", strings.Join(problems, "; "))
	}
	return nil
}

// parseGitHubRef parses "owner/repo" or "owner/repo@version".
func parseGitHubRef(ref string) (owner, repo, version string, err error) {
	// Split off @version if present.
	parts := strings.SplitN(ref, "@", 2)
	if len(parts) == 2 {
		version = parts[1]
	}
	ownerRepo := parts[0]

	slash := strings.SplitN(ownerRepo, "/", 2)
	if len(slash) != 2 || slash[0] == "" || slash[1] == "" {
		return "", "", "", fmt.Errorf("invalid GitHub reference %q: expected owner/repo[@version]", ref)
	}

	return slash[0], slash[1], version, nil
}
