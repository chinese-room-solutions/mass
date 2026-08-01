// Package modelscan walks MASS's models directory and reports the set of
// store-relative cache keys currently on disk. It is the canonical-set
// provider for worker cache reconciliation: the diff between what a worker
// reports caching and what MASS still has locally.
//
// The paths are OPAQUE. The first segment is runtime-owned (llama-cpp uses
// gguf/…, ONNX/vLLM/diffusion runtimes choose their own subtree); MASS never
// parses or special-cases it. Every regular file becomes one forward-slash
// relative-path key; directory-shaped models surface as their constituent
// files, which reconciliation matches by path prefix.
package modelscan

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/rs/zerolog"
)

// tempPrefix marks in-flight download temp files (pkg/download writes
// ".downloading-<base>"); they are not yet real cache entries and must never
// be handed to reconciliation as canonical.
const tempPrefix = ".downloading"

// errNotDir is returned when the models dir path exists but is not a
// directory — a corrupt store, treated as unreadable so reconcile skips.
var errNotDir = errors.New("models dir is not a directory")

// Scanner memoizes a walk of the models directory for a short TTL so the
// per-heartbeat reconcile loop doesn't stat the tree on every tick. Results
// are safe to reuse across workers; the set is rebuilt lazily once the TTL
// lapses (or Invalidate is called after a local mutation).
type Scanner struct {
	modelsDir string
	ttl       time.Duration
	logger    zerolog.Logger

	mu       sync.Mutex
	cached   map[string]struct{}
	cachedAt time.Time
}

// New builds a Scanner over modelsDir with the given memoization TTL.
func New(modelsDir string, ttl time.Duration, logger zerolog.Logger) *Scanner {
	return &Scanner{
		modelsDir: modelsDir,
		ttl:       ttl,
		logger:    logger.With().Str("component", "modelscan").Logger(),
	}
}

// Set returns the canonical set of store-relative keys, walking the tree at
// most once per TTL. On a walk error it returns an empty set: callers treat
// empty as "unknown" and must NOT reconcile, so a transiently unreadable
// store can never trigger a mass cache delete on workers.
func (s *Scanner) Set() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && time.Since(s.cachedAt) < s.ttl {
		return s.cached
	}
	set, err := walk(s.modelsDir)
	if err != nil {
		s.logger.Warn().Err(err).Str("dir", s.modelsDir).Msg("walking models dir; skipping reconcile")
		return map[string]struct{}{}
	}
	s.cached = set
	s.cachedAt = time.Now()
	return set
}

// Invalidate drops the memoized set so the next Set() re-walks. Call it after
// a local mutation (RemoveLocal / ImportLocal / download completion) to
// converge faster than the TTL.
func (s *Scanner) Invalidate() {
	s.mu.Lock()
	s.cached = nil
	s.mu.Unlock()
}

// walk collects every regular file under modelsDir as a forward-slash path
// relative to modelsDir, skipping in-flight download temp files. A missing
// models dir yields an empty set (no models installed yet), not an error.
func walk(modelsDir string) (map[string]struct{}, error) {
	set := make(map[string]struct{})
	err := filepath.WalkDir(modelsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && path == modelsDir {
				return filepath.SkipAll
			}
			return err
		}
		// The root must be a directory. A regular-file models dir is a
		// corrupt store, not "one file cached" — treat it as unreadable so
		// reconcile skips rather than reaping every worker's caches.
		if path == modelsDir {
			if !d.IsDir() {
				return ctxerr.With(fmt.Errorf("scanning: %w", errNotDir), map[string]any{"models_dir": modelsDir})
			}
			return nil
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if strings.HasPrefix(d.Name(), tempPrefix) {
			return nil
		}
		rel, err := filepath.Rel(modelsDir, path)
		if err != nil {
			return err
		}
		set[filepath.ToSlash(rel)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, ctxerr.With(err, map[string]any{"models_dir": modelsDir})
	}
	return set, nil
}
