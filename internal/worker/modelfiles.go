package worker

import (
	"net/url"
	"path/filepath"
	"strings"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
)

// buildLocalModelFile constructs a ModelFile for a file under the MASS
// host's models directory.
//
// Loopback worker: shared in place via LocalPath — worker loads directly.
// Remote worker: a URL against the MASS file-fetch endpoint.
//
// absPath must be under modelsDir; otherwise the returned URL points at
// the absolute path, which the worker rejects as unreachable.
func buildLocalModelFile(role workerpb.ModelFileRole, absPath, modelsDir, baseURL string, loopback bool) *workerpb.ModelFile {
	rel := relativeModelPath(absPath, modelsDir)
	if loopback {
		return &workerpb.ModelFile{
			Role:      role,
			Filename:  filepath.ToSlash(rel),
			LocalPath: absPath,
		}
	}
	base := strings.TrimRight(baseURL, "/")
	// url.PathEscape preserves slashes when joined; we want a path with
	// per-segment escaping.
	segments := strings.Split(filepath.ToSlash(rel), "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return &workerpb.ModelFile{
		Role:     role,
		Url:      base + "/api/v1/models/fetch/" + strings.Join(segments, "/"),
		Filename: filepath.ToSlash(rel),
	}
}

// relativeModelPath returns absPath relativized against modelsDir using forward
// slashes. If absPath is empty, already relative, or escapes modelsDir, the
// original input is returned (the worker will refuse to fetch it).
func relativeModelPath(absPath, modelsDir string) string {
	if absPath == "" || modelsDir == "" {
		return absPath
	}
	rel, err := filepath.Rel(modelsDir, absPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return absPath
	}
	return filepath.ToSlash(rel)
}
