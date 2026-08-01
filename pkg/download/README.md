# pkg/download

HTTP file downloader with resume, progress, and retry — used for model
weights, app installers, and anything else MASS pulls from the network.

## Features

- **Atomic writes**: data streams to `<dir>/.downloading-<name>`, then
  `os.Rename` on success. Partial writes never appear at the final path.
- **Resume**: `WithResume(true)` sends a `Range` header on restart and
  appends to the existing temp file. Falls back cleanly if the server
  sends `200 OK` (no range support) or `416` (range not satisfiable).
- **Retries with exponential backoff**: configurable max attempts (clamped
  to 10) and base delay, doubling per attempt up to 30s with ±25% jitter.
  4xx responses and local filesystem errors are non-retryable — a retry
  would re-download the whole file to hit the same error.
- **Stall detection**: connect/TLS/response-header timeouts on the default
  client, plus a watchdog that abandons an attempt after a minute with no
  bytes at all. There is deliberately no total `Client.Timeout` — model
  files are arbitrarily large.
- **Progress callbacks**: `WithProgress(fn)` delivers `(downloaded, total)`
  periodically. `total` is `-1` when the server omits `Content-Length`.
- **Custom headers**: `WithHeaders(...)` — e.g. `Authorization: Bearer …`
  for HuggingFace tokens.

## Usage

```go
m := download.NewManager(nil) // nil -> client with connect/header timeouts
err := m.Download(ctx, url, dest,
    download.WithResume(true),
    download.WithMaxRetries(5),
    download.WithProgress(func(dl, total int64) {
        // update UI
    }),
    download.WithHeaders(http.Header{"Authorization": {"Bearer " + token}}),
)
```

`download.TempFilePath(dest, suffix)` returns the temp filename — useful
for cancel/cleanup handlers that want to drop an in-flight download.

## Sentinel errors

- `ErrHTTPClientError` — 4xx responses (non-retryable).
- `ErrHTTPServerError` — 5xx responses (retryable).
- `ErrWriteFile` — local disk failure (non-retryable).
- `ErrStalled` — the transfer delivered no bytes for a minute (retryable).

Use `errors.Is` to classify.
