# pkg/llm

Public types and interfaces for model inference in MASS.

This package defines the *contract* — request/response types, model
interfaces, config structs — that is shared between MASS and external
consumers (mass-worker, mass-module SDK). Runtime implementations live
in the worker that uses them: `mass-worker-llama` ships `internal/llama`
for llama.cpp today; future runtimes (ONNX, vLLM, remote APIs) would
ship as new `mass-worker-<runtime>` binaries.

## Interfaces

| Interface                         | Purpose                                                 |
| --------------------------------- | ------------------------------------------------------- |
| `ChatModelInterface`              | Loaded chat model; yields a `PredictorInterface` pool.  |
| `EmbeddingModelInterface`         | Loaded embedding model; yields an `EmbedderInterface`.  |
| `ModelLoaderInterface`            | Loads a model for a given kind and runtime-specific config. |
| `PredictorInterface`              | `Submit` / `SubmitStream` / `Tokenize` on a model pool. |
| `EmbedderInterface`               | `Embed` / `EmbedBatch` on a model pool.                 |

## Config shape: one interface, many runtimes

`ModelConfigInterface` is the common contract for any runtime-specific
model config — it exposes `Runtime()`, `Kind()`, `Fingerprint()`, and
`Validate()`. Marker sub-interfaces discriminate at compile time:

- `ChatModelConfigInterface` — e.g. `LlamaChatConfig`
- `EmbeddingModelConfigInterface` — e.g. `LlamaEmbeddingConfig`

Each runtime owns its own concrete config type under this package, so
the scheduler can dispatch on the interface without knowing any
runtime name.

## Config split: identity vs placement

Configuration is split into two structs on purpose:

- **Identity** — runtime-specific structs like `LlamaChatConfig`:
  fields that determine *what* the model is (path, context size, chat
  template, …). Two configs with matching identity fields share a
  loaded instance.
- **Placement** — `PlacementConfig`: scheduler-decided fields for
  *how/where* the model runs (GPU layers, tensor split, thread count).
  Excluded from the fingerprint.

## Fingerprinting

`cfg.Fingerprint()` returns a 16-char hex SHA-256 over the identity
fields, including the runtime name and kind. The scheduler uses these
to pool model instances by effective identity.

> **Note on terminology.** "Runtime" = the inference engine (llama.cpp,
> ONNX Runtime, vLLM, ...). The hardware target *within* a runtime
> (CPU vs CUDA vs Metal vs ROCm vs DirectML) is selected by build tags
> and runtime detection, not by the config interface.

## Usage

External binaries construct a loader from a runtime package
(e.g. `llama.NewLoader()`), then pass the resulting
`ModelLoaderInterface` into whatever orchestration layer they have.
Inside MASS the same interfaces flow through `internal/scheduler` and
the model pool.
