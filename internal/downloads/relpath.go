package downloads

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/KernelPryanic/ctxerr"
)

// ErrInvalidRelPath is returned when a gateway-supplied models-dir-relative
// path is empty, absolute, or escapes the models directory. RPC boundaries
// match on it to map the failure to InvalidArgument.
var ErrInvalidRelPath = errors.New("invalid rel_path")

// ValidateRelPath rejects any gateway-supplied rel_path that is not a plain
// forward-slash path strictly inside models_dir. Canonical rel paths (e.g.
// "gguf/owner-model-Q4_K_M.gguf") never contain backslashes, colons, or dot
// segments, so anything of the sort is treated as hostile: joined into
// modelsDir unchecked it could read, overwrite, or delete files anywhere on
// the host. Applied at every boundary where a gateway-supplied relative path
// enters (DownloadFiles, ImportLocal, RemoveLocal).
func ValidateRelPath(rel string) error {
	if strings.TrimSpace(rel) == "" {
		return ctxerr.With(fmt.Errorf("%w: empty", ErrInvalidRelPath), nil)
	}
	// Backslashes and colons never appear in canonical rel paths and enable
	// Windows-specific escapes (`..\`, `C:file`, drive-absolute paths), so
	// reject them on every platform for deterministic behaviour.
	if strings.ContainsAny(rel, `\:`) {
		return ctxerr.With(fmt.Errorf("%w: contains path separator or drive characters", ErrInvalidRelPath), map[string]any{"rel_path": rel})
	}
	for _, seg := range strings.Split(rel, "/") {
		switch seg {
		case "":
			return ctxerr.With(fmt.Errorf("%w: absolute or empty path segment", ErrInvalidRelPath), map[string]any{"rel_path": rel})
		case ".", "..":
			return ctxerr.With(fmt.Errorf("%w: dot path segment", ErrInvalidRelPath), map[string]any{"rel_path": rel})
		}
	}
	// Defense in depth: the platform's own locality check (reserved device
	// names on Windows, etc.).
	if !filepath.IsLocal(filepath.FromSlash(rel)) {
		return ctxerr.With(fmt.Errorf("%w: not local", ErrInvalidRelPath), map[string]any{"rel_path": rel})
	}
	return nil
}
