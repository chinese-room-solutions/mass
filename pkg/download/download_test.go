package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManager_Download(t *testing.T) {
	content := "Hello, Download Manager!"

	tests := []struct {
		name      string
		handler   http.HandlerFunc
		opts      []Option
		wantErr   bool
		wantBody  string
		setupDest func(t *testing.T, destPath string) // pre-populate dest
	}{
		{
			name: "basic download",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
				_, _ = w.Write([]byte(content))
			},
			wantBody: content,
		},
		{
			name: "already downloaded skips fetch",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				t.Fatal("server should not be called")
			},
			setupDest: func(t *testing.T, destPath string) {
				t.Helper()
				require.NoError(t, os.MkdirAll(filepath.Dir(destPath), 0755))
				require.NoError(t, os.WriteFile(destPath, []byte(content), 0644))
			},
			wantBody: content,
		},
		{
			name: "404 returns error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "not found", http.StatusNotFound)
			},
			opts:    []Option{WithMaxRetries(0)},
			wantErr: true,
		},
		{
			name: "500 retries then fails",
			handler: func() http.HandlerFunc {
				var calls atomic.Int32
				return func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					http.Error(w, "oops", http.StatusInternalServerError)
				}
			}(),
			opts:    []Option{WithMaxRetries(2), WithBaseDelay(time.Millisecond)},
			wantErr: true,
		},
		{
			name: "500 then success on retry",
			handler: func() http.HandlerFunc {
				var calls atomic.Int32
				return func(w http.ResponseWriter, _ *http.Request) {
					if calls.Add(1) == 1 {
						http.Error(w, "oops", http.StatusInternalServerError)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
					_, _ = w.Write([]byte(content))
				}
			}(),
			opts:     []Option{WithMaxRetries(2), WithBaseDelay(time.Millisecond)},
			wantBody: content,
		},
		{
			name: "context cancelled returns error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Block until the client context is done.
				<-r.Context().Done()
			},
			opts:    []Option{WithMaxRetries(0)},
			wantErr: true,
		},
		{
			name: "custom headers are sent",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer tok123" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte(content))
			},
			opts: []Option{
				WithMaxRetries(0),
				WithHeaders(http.Header{"Authorization": {"Bearer tok123"}}),
			},
			wantBody: content,
		},
		{
			name: "progress callback is called",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
				_, _ = w.Write([]byte(content))
			},
			opts: []Option{
				WithMaxRetries(0),
				WithProgress(func(downloaded, total int64) {
					// Just verify it doesn't panic; values checked below.
				}),
			},
			wantBody: content,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			dir := t.TempDir()
			destPath := filepath.Join(dir, "output", "file.bin")

			if tt.setupDest != nil {
				tt.setupDest(t, destPath)
			}

			ctx := context.Background()
			if tt.name == "context cancelled returns error" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}

			mgr := NewManager(srv.Client())
			err := mgr.Download(ctx, srv.URL+"/file.bin", destPath, tt.opts...)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got, err := os.ReadFile(destPath)
			require.NoError(t, err)
			require.Equal(t, tt.wantBody, string(got))
		})
	}
}

func TestManager_Resume(t *testing.T) {
	fullContent := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		if rangeHdr != "" {
			var start int64
			_, _ = fmt.Sscanf(rangeHdr, "bytes=%d-", &start)
			if start >= int64(len(fullContent)) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", int64(len(fullContent))-start))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(fullContent)-1, len(fullContent)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(fullContent[start:]))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fullContent)))
		_, _ = w.Write([]byte(fullContent))
	}))
	defer srv.Close()

	dir := t.TempDir()
	destPath := filepath.Join(dir, "file.bin")
	tempPath := TempFilePath(destPath, ".downloading")

	// Simulate a partial download.
	require.NoError(t, os.WriteFile(tempPath, []byte(fullContent[:10]), 0644))

	mgr := NewManager(srv.Client())
	err := mgr.Download(context.Background(), srv.URL+"/file.bin", destPath,
		WithResume(true),
		WithMaxRetries(0),
	)
	require.NoError(t, err)

	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, fullContent, string(got))

	// Temp file should be gone (renamed to dest).
	_, err = os.Stat(tempPath)
	require.True(t, os.IsNotExist(err))
}

func TestManager_ProgressValues(t *testing.T) {
	content := "0123456789"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
		_, _ = w.Write([]byte(content))
	}))
	defer srv.Close()

	var lastDownloaded, lastTotal int64
	mgr := NewManager(srv.Client())
	err := mgr.Download(context.Background(), srv.URL+"/f", filepath.Join(t.TempDir(), "f"),
		WithMaxRetries(0),
		WithProgress(func(downloaded, total int64) {
			lastDownloaded = downloaded
			lastTotal = total
		}),
	)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), lastDownloaded)
	require.Equal(t, int64(len(content)), lastTotal)
}

func TestTempFilePath(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name   string
		dest   string
		suffix string
		want   string
	}{
		{"default suffix", filepath.Join(dir, "file.bin"), ".downloading", filepath.Join(dir, ".downloading-file.bin")},
		{"empty suffix", filepath.Join(dir, "file.bin"), "", filepath.Join(dir, ".downloading-file.bin")},
		{"custom suffix", filepath.Join(dir, "file.bin"), ".dl", filepath.Join(dir, ".dl-file.bin")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TempFilePath(tt.dest, tt.suffix)
			require.Equal(t, tt.want, got)
		})
	}
}
