// Package web provides embedded static assets for the MASS web UI.
package web

import "embed"

//go:embed public
var Public embed.FS
