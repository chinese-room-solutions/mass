package worker

import (
	"path/filepath"
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/stretchr/testify/require"
)

func TestBuildLocalModelFile(t *testing.T) {
	modelsDir := filepath.Join("/data", "models")
	tests := []struct {
		name      string
		role      workerpb.ModelFileRole
		abs       string
		baseURL   string
		loopback  bool
		wantURL   string
		wantName  string
		wantLocal string
	}{
		{
			name:     "remote: simple GGUF under models dir",
			role:     workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY,
			abs:      filepath.Join(modelsDir, "publisher", "repo", "model.gguf"),
			baseURL:  "https://mass:3455",
			wantURL:  "https://mass:3455/api/v1/models/fetch/publisher/repo/model.gguf",
			wantName: "publisher/repo/model.gguf",
		},
		{
			name:     "remote: trailing slash on baseURL",
			role:     workerpb.ModelFileRole_MODEL_FILE_ROLE_PROJECTOR,
			abs:      filepath.Join(modelsDir, "p", "r", "mmproj.gguf"),
			baseURL:  "http://mass/",
			wantURL:  "http://mass/api/v1/models/fetch/p/r/mmproj.gguf",
			wantName: "p/r/mmproj.gguf",
		},
		{
			name:     "remote: name with spaces gets percent-escaped",
			role:     workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY,
			abs:      filepath.Join(modelsDir, "pub", "model name.gguf"),
			baseURL:  "http://mass",
			wantURL:  "http://mass/api/v1/models/fetch/pub/model%20name.gguf",
			wantName: "pub/model name.gguf",
		},
		{
			name:      "loopback: shares absolute path, no URL",
			role:      workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY,
			abs:       filepath.Join(modelsDir, "publisher", "repo", "model.gguf"),
			baseURL:   "https://mass:3455",
			loopback:  true,
			wantName:  "publisher/repo/model.gguf",
			wantLocal: filepath.Join(modelsDir, "publisher", "repo", "model.gguf"),
		},
		{
			name:      "loopback: projector",
			role:      workerpb.ModelFileRole_MODEL_FILE_ROLE_PROJECTOR,
			abs:       filepath.Join(modelsDir, "p", "r", "mmproj.gguf"),
			baseURL:   "http://mass/",
			loopback:  true,
			wantName:  "p/r/mmproj.gguf",
			wantLocal: filepath.Join(modelsDir, "p", "r", "mmproj.gguf"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildLocalModelFile(tt.role, tt.abs, modelsDir, tt.baseURL, tt.loopback)
			require.Equal(t, tt.role, got.Role)
			require.Equal(t, tt.wantURL, got.Url)
			require.Equal(t, tt.wantName, got.Filename)
			require.Equal(t, tt.wantLocal, got.LocalPath)
		})
	}
}

func TestIsLoopbackPeer(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{"empty", "", false},
		{"ipv4 loopback with port", "127.0.0.1:54321", true},
		{"ipv4 loopback no port", "127.0.0.1", true},
		{"ipv6 loopback with port", "[::1]:54321", true},
		{"ipv6 loopback no port", "::1", true},
		{"localhost name with port", "localhost:54321", true},
		{"localhost name no port", "localhost", true},
		{"public IPv4", "10.0.0.5:1234", false},
		{"hostname", "worker.example.com:1234", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isLoopbackPeer(tt.addr))
		})
	}
}
