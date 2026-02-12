package web

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"

	webpkg "github.com/chinese-room-solutions/mass/web"
)

// publicFS returns the HTTP filesystem for static assets.
// If MASS_PUBLIC_DIR is set, serves from that directory (dev mode).
// Otherwise serves from the files embedded at build time.
func publicFS() (http.FileSystem, error) {
	if dir := os.Getenv("MASS_PUBLIC_DIR"); dir != "" {
		return http.Dir(dir), nil
	}
	sub, err := fs.Sub(webpkg.Public, "public")
	if err != nil {
		return nil, fmt.Errorf("extracting embedded public dir: %w", err)
	}
	return http.FS(sub), nil
}
