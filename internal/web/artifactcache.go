package web

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/chinese-room-solutions/mass-sdk/registry"
)

// Content-addressed artifact cache backing the /setup/worker-bin proxy. Files
// live at {dataDir}/registry-cache/artifacts/<sha256>, keyed by the index's
// pinned digest. Because the key is the digest, an operator can pre-drop files
// there for an air-gapped LAN and MASS serves them without ever reaching the
// network (the layout is documented in the README).

// artifactCache serializes downloads per digest so concurrent requests for the
// same artifact neither double-download nor observe a partial file: the first
// caller downloads (via registry.Download, which writes to a temp file and
// atomically renames on a verified digest), the rest wait on the same in-flight
// entry and then read the finished file.
type artifactCache struct {
	dir string

	mu       sync.Mutex
	inflight map[string]*artifactFetch
}

type artifactFetch struct {
	done chan struct{}
	err  error
}

func newArtifactCache(dir string) *artifactCache {
	return &artifactCache{dir: dir, inflight: make(map[string]*artifactFetch)}
}

// path returns the on-disk cache path for a digest.
func (c *artifactCache) path(sha256 string) string {
	return filepath.Join(c.dir, sha256)
}

// ensure guarantees the artifact identified by its index entry is present in the
// cache and returns its path. A present file (already cached, or pre-dropped for
// an air-gapped LAN) is returned immediately. Otherwise exactly one caller
// downloads it while concurrent callers for the same digest wait. ctx cancels
// the download; a cancelled waiter returns ctx.Err() without affecting the
// in-flight download owned by the first caller.
func (c *artifactCache) ensure(ctx context.Context, artifact registry.Artifact) (string, error) {
	if artifact.SHA256 == "" {
		return "", fmt.Errorf("artifact has no sha256")
	}
	dest := c.path(artifact.SHA256)

	if _, err := os.Stat(dest); err == nil {
		return dest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat cached artifact: %w", err)
	}

	c.mu.Lock()
	if f, ok := c.inflight[artifact.SHA256]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			if f.err != nil {
				return "", f.err
			}
			return dest, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	f := &artifactFetch{done: make(chan struct{})}
	c.inflight[artifact.SHA256] = f
	c.mu.Unlock()

	err := c.download(ctx, artifact, dest)

	c.mu.Lock()
	delete(c.inflight, artifact.SHA256)
	c.mu.Unlock()
	f.err = err
	close(f.done)

	if err != nil {
		return "", err
	}
	return dest, nil
}

// download fetches the artifact into the cache dir. registry.Download verifies
// the sha256 and refuses placeholder ("TBD") digests, so the cache never holds
// an unverified or unreleased file.
func (c *artifactCache) download(ctx context.Context, artifact registry.Artifact, dest string) error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return fmt.Errorf("creating artifact cache dir: %w", err)
	}
	// Pass a nil http client so Download uses one without a total timeout and
	// relies on ctx (installer binaries can be large).
	if err := registry.Download(ctx, nil, artifact, dest); err != nil {
		return fmt.Errorf("downloading artifact: %w", err)
	}
	return nil
}
