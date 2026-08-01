// Package download provides a unified file download manager with resumable
// downloads, progress reporting, retries with exponential backoff, and
// optional authentication headers.
package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/KernelPryanic/ctxerr"
)

// ManagerInterface abstracts file downloading so callers can swap
// implementations (e.g. for testing).
type ManagerInterface interface {
	// Download fetches url into destPath. Options control resume, progress,
	// retries, and auth headers.
	Download(ctx context.Context, url, destPath string, opts ...Option) error
}

// ProgressFunc is called periodically with bytes downloaded and total bytes.
// total may be -1 if the server did not provide Content-Length.
type ProgressFunc func(downloaded, total int64)

// Manager is the default implementation of ManagerInterface.
type Manager struct {
	client *http.Client
}

// NewManager creates a Manager. If client is nil, one with connection and
// response-header timeouts is used — but no total Client.Timeout: model
// files are arbitrarily large, so bounding the whole transfer would kill
// legitimate long downloads. Mid-body stalls are caught by the stall
// watchdog instead (see stallTimeout).
func NewManager(client *http.Client) *Manager {
	if client == nil {
		client = &http.Client{Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		}}
	}
	return &Manager{client: client}
}

const (
	// maxRetryDelay caps the exponential backoff. Uncapped, a handful of
	// doublings puts the next attempt hours out — and past ~62 of them the
	// shift overflows time.Duration into a negative (immediate) delay.
	maxRetryDelay = 30 * time.Second
	// maxRetriesLimit clamps WithMaxRetries. Ten attempts against a
	// 30s-capped backoff is already minutes of retrying.
	maxRetriesLimit = 10
)

// stallTimeout is how long a transfer may deliver no bytes at all before
// it's abandoned. Generous — a busy origin can legitimately pause mid-body,
// and a resumed retry re-reads nothing — but a full minute of silence on an
// open connection is a dead transfer no HTTP client timeout would catch.
const stallTimeout = time.Minute

// options holds per-request configuration.
type options struct {
	headers      http.Header
	progressFn   ProgressFunc
	resume       bool
	maxRetries   int
	baseDelay    time.Duration
	stallTimeout time.Duration
	tempSuffix   string // suffix for the temp file (default ".downloading")
}

func defaultOptions() options {
	return options{
		headers:      make(http.Header),
		maxRetries:   3,
		baseDelay:    time.Second,
		stallTimeout: stallTimeout,
		tempSuffix:   ".downloading",
	}
}

// Option configures a single download request.
type Option func(*options)

// WithHeaders sets extra HTTP headers (e.g. Authorization).
func WithHeaders(h http.Header) Option {
	return func(o *options) {
		for k, vs := range h {
			for _, v := range vs {
				o.headers.Add(k, v)
			}
		}
	}
}

// WithProgress sets a progress callback.
func WithProgress(fn ProgressFunc) Option {
	return func(o *options) { o.progressFn = fn }
}

// WithResume enables resumable download via HTTP Range headers.
// When enabled, a partial temp file is preserved on interruption and
// reused on the next call.
func WithResume(enable bool) Option {
	return func(o *options) { o.resume = enable }
}

// WithMaxRetries sets the maximum number of retry attempts on transient
// errors (connection reset, timeout, 5xx). 0 means no retries; n is clamped
// to [0, maxRetriesLimit].
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = min(max(n, 0), maxRetriesLimit) }
}

// WithBaseDelay sets the base delay for exponential backoff between
// retries. Negative values are treated as zero.
func WithBaseDelay(d time.Duration) Option {
	return func(o *options) { o.baseDelay = max(d, 0) }
}

// WithTempSuffix overrides the temp file suffix (default ".downloading").
func WithTempSuffix(s string) Option {
	return func(o *options) { o.tempSuffix = s }
}

// sentinel errors for classification. ErrWriteFile covers every local
// filesystem failure — creating, writing, closing and renaming the temp
// file — because none of them are worth a retry: a full disk or a wrong
// permission costs a complete re-download to surface the same error.
var (
	ErrHTTPClientError = errors.New("HTTP client error (4xx)")
	ErrHTTPServerError = errors.New("HTTP server error (5xx)")
	ErrWriteFile       = errors.New("writing file")
	ErrStalled         = errors.New("transfer stalled")
)

// Download fetches url and saves the result to destPath atomically
// (write to temp, rename on success).
func (m *Manager) Download(ctx context.Context, url, destPath string, opts ...Option) error {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	base := filepath.Base(destPath)
	tempPath := filepath.Join(dir, o.tempSuffix+"-"+base)

	// Already downloaded — report full progress and return.
	if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
		if o.progressFn != nil {
			o.progressFn(info.Size(), info.Size())
		}
		return nil
	}

	var lastErr error
	attempts := 1 + o.maxRetries
	for attempt := range attempts {
		lastErr = m.doDownload(ctx, url, destPath, tempPath, &o)
		if lastErr == nil {
			return nil
		}
		// Don't retry on context cancellation or non-retryable errors.
		if ctx.Err() != nil {
			return lastErr
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if attempt < attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay(o.baseDelay, attempt)):
			}
		}
	}
	return lastErr
}

// doDownload performs a single download attempt.
func (m *Manager) doDownload(ctx context.Context, url, destPath, tempPath string, o *options) error {
	var resumeOffset int64
	if o.resume {
		if info, err := os.Stat(tempPath); err == nil {
			resumeOffset = info.Size()
		}
	}

	// Watchdog against a server that accepts the request and then goes
	// silent: no HTTP client timeout covers a body that has started but
	// stopped, so cancel this attempt's context when no bytes arrive for
	// o.stallTimeout. Rearmed on every read below.
	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()
	var stalled atomic.Bool
	watchdog := time.AfterFunc(o.stallTimeout, func() {
		stalled.Store(true)
		cancelReq()
	})
	defer watchdog.Stop()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	for k, vs := range o.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if resumeOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeOffset))
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return ctxerr.With(fmt.Errorf("HTTP request failed: %w", err), map[string]any{"url": url})
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort cleanup

	resumeOffset, resp, err = m.handleResumeResponse(reqCtx, resp, url, o, resumeOffset)
	if err != nil {
		return err
	}

	var total int64
	if resp.StatusCode == http.StatusPartialContent {
		total = resumeOffset + resp.ContentLength
	} else {
		total = resp.ContentLength // may be -1
	}

	var f *os.File
	if resumeOffset > 0 && resp.StatusCode == http.StatusPartialContent {
		f, err = os.OpenFile(tempPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		f, err = os.Create(tempPath)
		resumeOffset = 0
	}
	if err != nil {
		return ctxerr.With(fmt.Errorf("%w: opening temp file: %w", ErrWriteFile, err), //nolint:errorlint // wrapping sentinel
			map[string]any{"path": tempPath})
	}
	defer f.Close() //nolint:errcheck // safety net

	downloaded := resumeOffset
	buf := make([]byte, 64*1024)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			watchdog.Reset(o.stallTimeout)
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				return fmt.Errorf("%w: %w", ErrWriteFile, wErr) //nolint:errorlint // wrapping sentinel
			}
			downloaded += int64(n)
			if o.progressFn != nil {
				o.progressFn(downloaded, total)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if stalled.Load() {
				return ctxerr.With(fmt.Errorf("%w: no data for %s", ErrStalled, o.stallTimeout),
					map[string]any{"url": url, "downloaded": downloaded})
			}
			return ctxerr.With(fmt.Errorf("reading response: %w", readErr), map[string]any{"url": url})
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("%w: closing temp file: %w", ErrWriteFile, err) //nolint:errorlint // wrapping sentinel
	}

	if err := os.Rename(tempPath, destPath); err != nil {
		return ctxerr.With(fmt.Errorf("%w: finalizing download: %w", ErrWriteFile, err), //nolint:errorlint // wrapping sentinel
			map[string]any{"temp": tempPath, "dest": destPath})
	}

	return nil
}

// handleResumeResponse deals with the various HTTP status codes when resuming.
// Returns the adjusted resumeOffset, the response to read from, and any error.
func (m *Manager) handleResumeResponse(ctx context.Context, resp *http.Response, url string, o *options, resumeOffset int64) (int64, *http.Response, error) {
	switch resp.StatusCode {
	case http.StatusPartialContent:
		return resumeOffset, resp, nil

	case http.StatusOK:
		// Server doesn't support Range or fresh download.
		return 0, resp, nil

	case http.StatusRequestedRangeNotSatisfiable:
		// Partial file invalid — restart fresh.
		// Body close failure here is a network-level issue we can't recover
		// from anyway; the next request will surface the real error.
		if cErr := resp.Body.Close(); cErr != nil {
			return 0, nil, ctxerr.With(fmt.Errorf("closing partial-content response: %w", cErr), map[string]any{"url": url})
		}
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return 0, nil, fmt.Errorf("creating request: %w", err)
		}
		for k, vs := range o.headers {
			for _, v := range vs {
				req2.Header.Add(k, v)
			}
		}
		resp2, err := m.client.Do(req2)
		if err != nil {
			return 0, nil, ctxerr.With(fmt.Errorf("HTTP request failed: %w", err), map[string]any{"url": url})
		}
		if resp2.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp2.Body, 1024))
			if cErr := resp2.Body.Close(); cErr != nil {
				return 0, nil, ctxerr.With(fmt.Errorf("%w: %d: %s (body-close: %v)", httpSentinel(resp2.StatusCode), resp2.StatusCode, string(body), cErr), map[string]any{"url": url, "status": resp2.StatusCode})
			}
			return 0, nil, ctxerr.With(fmt.Errorf("%w: %d: %s", httpSentinel(resp2.StatusCode), resp2.StatusCode, string(body)), map[string]any{"url": url, "status": resp2.StatusCode})
		}
		return 0, resp2, nil

	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, nil, ctxerr.With(fmt.Errorf("%w: %d: %s", httpSentinel(resp.StatusCode), resp.StatusCode, string(body)), map[string]any{"url": url, "status": resp.StatusCode})
	}
}

// TempFilePath returns the temp file path for a given destPath and suffix.
// Useful for cleanup/cancel handlers.
func TempFilePath(destPath, tempSuffix string) string {
	if tempSuffix == "" {
		tempSuffix = ".downloading"
	}
	dir := filepath.Dir(destPath)
	base := filepath.Base(destPath)
	return filepath.Join(dir, tempSuffix+"-"+base)
}

// httpSentinel returns the appropriate sentinel error for the HTTP status code.
func httpSentinel(statusCode int) error {
	if statusCode >= 500 {
		return ErrHTTPServerError
	}
	return ErrHTTPClientError
}

// retryDelay returns the backoff before retry number attempt (0-based):
// base doubled per attempt, capped at maxRetryDelay, with ±25% jitter so
// concurrent downloads don't retry in lockstep.
func retryDelay(base time.Duration, attempt int) time.Duration {
	delay := base
	for range attempt {
		if delay >= maxRetryDelay {
			break
		}
		delay *= 2 // can't overflow: bounded by 2 * maxRetryDelay
	}
	delay = min(delay, maxRetryDelay)
	spread := delay / 4
	if spread <= 0 {
		return delay
	}
	return delay - spread + time.Duration(rand.Int64N(int64(2*spread)))
}

// isRetryable returns true for transient errors that may succeed on retry.
func isRetryable(err error) bool {
	if errors.Is(err, ErrWriteFile) || errors.Is(err, ErrHTTPClientError) {
		return false // local filesystem errors and 4xx are not transient
	}
	// Retry on HTTP 5xx and network errors (connection reset, timeout, etc.).
	// Context cancellations are handled before this is called.
	return true
}
