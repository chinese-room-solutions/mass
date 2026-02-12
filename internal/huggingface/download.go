package huggingface

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/chinese-room-solutions/mass/pkg/download"
)

// hfTempSuffix is the temp file prefix used by HuggingFace model downloads.
// Must match what TempFilePath produces so pause/cancel handlers find the right file.
const hfTempSuffix = ".downloading"

// Download fetches a GGUF file from HuggingFace and saves it locally.
// The file is saved to {destDir}/{sanitized_repo_id}/{filename}.
// progressFn is called periodically with bytes downloaded and total bytes.
//
// Downloads are resumable: on context cancellation the partial file is
// preserved and reused on the next call. On completion, the temp file is
// renamed to the final path atomically.
func Download(ctx context.Context, repoID, filename, destDir string, progressFn func(downloaded, total int64)) (string, error) {
	sanitized := SanitizeRepoID(repoID)
	destPath := filepath.Join(destDir, sanitized, filename)

	downloadURL := "https://huggingface.co/" + repoID + "/resolve/main/" + url.PathEscape(filename)

	opts := []download.Option{
		download.WithResume(true),
		download.WithMaxRetries(3),
		download.WithTempSuffix(hfTempSuffix),
	}
	if progressFn != nil {
		opts = append(opts, download.WithProgress(progressFn))
	}

	mgr := download.NewManager(nil)
	if err := mgr.Download(ctx, downloadURL, destPath, opts...); err != nil {
		return "", err
	}
	return destPath, nil
}

// TempFilePath returns the path to the temporary download file for a given
// repo and filename. Used by the cancel handler to clean up.
func TempFilePath(repoID, filename, destDir string) string {
	sanitized := SanitizeRepoID(repoID)
	destPath := filepath.Join(destDir, sanitized, filename)
	return download.TempFilePath(destPath, hfTempSuffix)
}
