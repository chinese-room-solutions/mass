package download

import (
	"context"
	"errors"
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

// withStallTimeout shortens the stall watchdog. Test-only: production has
// no reason to tune it, so it stays off the package's Option surface.
func withStallTimeout(d time.Duration) Option {
	return func(o *options) { o.stallTimeout = d }
}

// A server that sends headers and then goes silent must not hang the
// download forever — no HTTP client timeout covers a body that started and
// then stopped.
func TestManager_StalledTransferAborts(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release) // runs first: never leave the handler parked

	done := make(chan error, 1)
	go func() {
		mgr := NewManager(srv.Client())
		done <- mgr.Download(context.Background(), srv.URL+"/f", filepath.Join(t.TempDir(), "f"),
			WithMaxRetries(0), withStallTimeout(100*time.Millisecond))
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrStalled)
	case <-time.After(10 * time.Second):
		t.Fatal("stalled download never aborted")
	}
}

// A local filesystem failure must surface on the first attempt: retrying it
// costs a full re-download and ends in the same error.
func TestManager_LocalFileErrorIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte("payload"))
	}))
	defer srv.Close()

	destPath := filepath.Join(t.TempDir(), "file.bin")
	// Occupy the temp path with a directory so the temp-file open fails.
	require.NoError(t, os.MkdirAll(TempFilePath(destPath, ".downloading"), 0755))

	mgr := NewManager(srv.Client())
	err := mgr.Download(context.Background(), srv.URL+"/file.bin", destPath,
		WithMaxRetries(3), WithBaseDelay(time.Millisecond))
	require.ErrorIs(t, err, ErrWriteFile)
	require.Equal(t, int32(1), calls.Load(), "local filesystem failures must not re-download")
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"local file", fmt.Errorf("%w: opening temp file: %w", ErrWriteFile, os.ErrPermission), false}, //nolint:errorlint // wrapping sentinel
		{"4xx", fmt.Errorf("%w: 404", ErrHTTPClientError), false},
		{"5xx", fmt.Errorf("%w: 503", ErrHTTPServerError), true},
		{"stalled", fmt.Errorf("%w: no data", ErrStalled), true},
		{"network", errors.New("connection reset by peer"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isRetryable(tt.err))
		})
	}
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		name    string
		base    time.Duration
		attempt int
		wantMin time.Duration
		wantMax time.Duration
	}{
		{"first attempt", time.Second, 0, 750 * time.Millisecond, 1250 * time.Millisecond},
		{"doubles", time.Second, 2, 3 * time.Second, 5 * time.Second},
		{"capped", time.Second, 9, 22500 * time.Millisecond, 37500 * time.Millisecond},
		{"huge base is capped", time.Hour, 0, 22500 * time.Millisecond, 37500 * time.Millisecond},
		{"overflowing attempt is capped", time.Duration(1) << 40, maxRetriesLimit, 22500 * time.Millisecond, 37500 * time.Millisecond},
		{"zero base", 0, 5, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 50 { // jittered: sample it
				got := retryDelay(tt.base, tt.attempt)
				require.GreaterOrEqual(t, got, tt.wantMin)
				require.LessOrEqual(t, got, tt.wantMax)
			}
		})
	}
}

func TestWithMaxRetries_Clamps(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"negative", -5, 0},
		{"zero", 0, 0},
		{"in range", 4, 4},
		{"over limit", 1 << 40, maxRetriesLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := defaultOptions()
			WithMaxRetries(tt.n)(&o)
			require.Equal(t, tt.want, o.maxRetries)
		})
	}
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
