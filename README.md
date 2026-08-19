# MASS — Modular AI Scheduling Service

[![CI](https://github.com/chinese-room-solutions/mass/actions/workflows/ci.yml/badge.svg)](https://github.com/chinese-room-solutions/mass/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/chinese-room-solutions/mass)](https://github.com/chinese-room-solutions/mass/releases/latest)
[![License: FSL-1.1-ALv2](https://img.shields.io/badge/License-FSL--1.1--ALv2-blue.svg)](LICENSE.md)

MASS is a **pure-Go orchestrator** for AI inference. It does no inference itself — instead it manages a fleet of compute, schedules work onto it, and exposes that capacity through pluggable runtimes. Every inference-specific concern (which model formats are understood, which API is spoken, how a model is loaded and run) lives outside MASS in **installable runtime gateways**, so the same orchestrator drives llama.cpp today and vLLM, a TTS engine, or a diffusion model tomorrow — without a single code change.

*AI-powered applications made easy.*

## Architecture

<img src="./assets/MASS Architecture.png" alt="MASS architecture" width="820"/>

**MASS** is the coordinator. It holds the durable job queues, scores and places each job onto a suitable worker, streams results back, and serves the operator dashboard. It deals only in *opaque jobs* — bytes it routes but never interprets.

**Runtime gateways** are the adapters that teach MASS a single inference ecosystem. Each one is an installable `.mass` package (one per runtime kind: llama-cpp, vLLM, TTS, …) that MASS launches as a [go-plugin](https://github.com/hashicorp/go-plugin) subprocess. A gateway owns everything format-specific that MASS deliberately doesn't:

- it **hosts the public API** for its runtime (OpenAI-compatible and/or its own typed endpoints), which MASS proxies at `/mass.<runtime>.*`;
- it **recognises the models** — walks the models directory, parses the files it understands, and builds the catalogue MASS renders. MASS owns the bytes on disk and does all the copying and fetching; the gateway only *plans* what an install should contain and *parses* what's already there;
- it **defines what a "job" is** — it translates an incoming API request into an opaque payload, submits it to MASS's scheduler, and streams the result back to the caller. Only the gateway and its workers understand those payload bytes.

This is why a new runtime is just a new gateway: it brings its own format knowledge and API, and MASS keeps scheduling unchanged.

**Workers** are the muscle. Each is a runtime-specific binary that connects into MASS over a long-lived bidirectional gRPC stream, advertises its hardware (and benchmarks itself), then holds models resident in GPU/CPU memory and executes the opaque payloads its matching gateway produced. Workers are where the heavy inference toolchains (CUDA, ROCm, Metal, Vulkan) and the model weights actually live; many can join one MASS to form a fleet.

**Apps** are ordinary client programs. For inference they call a gateway's API (proxied through MASS at `/mass.<runtime>.*`) and get streamed responses back, exactly as if talking to any inference server. For management — installing the model an app needs, listing what's available — they call MASS's own versioned RPC API. See **Contracts** below.

**DB** (SQLite by default, or Postgres) persists the job queues and completed results, so work survives restarts and result streams can resume after a reconnect.

The key property: **MASS knows nothing about models or inference.** Format parsing, API shapes, and execution are all pushed to the edges — gateways and workers — leaving a small, stable orchestrator in the middle that only has to schedule opaque work well.

## Contracts

MASS deliberately exposes its surfaces along the same seams as its architecture. Each one has a different audience, and that audience decides its shape — which is why there is no single "MASS API".

- **MASS management API** (`mass.v1`) — the public, **versioned** RPC contract for managing MASS itself: installing models, listing what's available, and (over time) the broader control surface that lets apps and AI agents drive the orchestrator. It is a [Connect](https://connectrpc.com/) service, so the *same* methods are reachable as gRPC *and* plain HTTP/JSON. This is the stable entry point external programs build against. Model operations here follow one rule: the **runtime decides** (which files are one model, their names, which is a companion) and **MASS executes** the byte movement against its store — so MASS never has to understand a model format, and a gateway never touches the store.

- **Runtime gateway API** (`mass.<runtime>.v1`) — each gateway's own **versioned** API for inference, which MASS proxies opaquely at `/mass.<runtime>.*` (typed API) and `/mass.<runtime>/*` (the gateway's plain HTTP routes, e.g. an OpenAI-compatible shim). MASS routes the bytes but never interprets them, so a gateway is free to expose whatever it likes (its typed API, an OpenAI-compatible shim, or both). Apps doing inference talk here.

- **Internal plugin & worker contracts** — MASS↔gateway (go-plugin) and MASS↔worker (a long-lived gRPC stream carrying gateway-encoded payloads) are private wiring between components, not for external callers.

- **Operator dashboard endpoints** — the live dashboard talks to MASS over a private set of REST + Server-Sent-Events endpoints. These exist because the browser UI (Datastar) needs streaming HTML/SSE that a request/response RPC can't provide. They are an implementation detail of the bundled UI, ship in lockstep with it, and are **not** a public contract — external programs should use the versioned management API instead.

The dividing line: **versioned RPC for everything external; the dashboard's REST/SSE stays private to the UI.** The management-API and internal protos live in [`mass-proto`](https://github.com/chinese-room-solutions/mass-proto); each gateway's own API proto ships in that gateway's repo.

## Features

- **Pure-Go core** — the heavy inference toolchains (CUDA, ROCm, Metal, Vulkan) are confined to the workers. The only CGO anywhere is the desktop window's system webview (Linux/macOS); `make build-headless` builds a fully static, CGO-free server binary.
- **Pluggable runtimes** — install, start, stop, and uninstall `.mass` gateway packages live from the dashboard; no rebuild, no restart of MASS.
- **Latency-aware scheduling** — jobs are placed by predicted wall-clock cost (queue depth + model load time), models load on demand, failed placements retry on another worker, and result streams survive a worker reconnecting mid-job.
- **Fleet control** — enable/disable individual workers or devices, with new workers benchmarked automatically so the scheduler knows their throughput.
- **Operator dashboard** — live view (Datastar + Shoelace + Tailwind) of runtimes, models, the scheduler, queues, and workers, with Prometheus metrics, bearer-token auth, and optional TLS for API and worker traffic.

## Install

On macOS and Linux, one line fetches the installer and runs the wizard:

```sh
curl -fsSL https://raw.githubusercontent.com/chinese-room-solutions/mass/main/install.sh | sh
```

A curl-fetched file carries no quarantine flag, so macOS skips the Gatekeeper
"Open Anyway" dance described below.

Or download the installer for your OS from the
[releases page](https://github.com/chinese-room-solutions/mass/releases/latest)
and run it — a short terminal wizard that stages the app and a launcher.

## Quick start

To build from source instead:

Building MASS needs **Go 1.26+** (plus `bun` or `npx` for the Tailwind CSS step). The desktop build also needs the system webview's dev packages on Linux (`gtk+-3.0`, `webkit2gtk-4.1`) and the Xcode command-line tools on macOS — Windows needs neither. Inference backend toolchains are needed only for the separate worker binaries, not for MASS.

```bash
git clone https://github.com/chinese-room-solutions/mass.git
cd mass
make run          # build web assets + binary, then start bin/mass
```

MASS starts as a desktop app: a native window over the dashboard, plus a tray icon (minimizing folds to the tray, Quit closes the window). The window is a thin client — the backend runs as a separate daemon the window attaches to, starting one on demand if none is running. A daemon started this way retires itself after 10 seconds without clients (a running daemon holds the executable open, so an update cannot replace it until it retires); on a server, run `mass serve` for a permanent one with no window — or `make build-headless` for a CGO-free static binary with no GUI at all — and open the dashboard in a browser instead.

MASS loads its config from the user config dir (e.g. `~/.config/mass/config.yml`), writing defaults on first run. Then, in the dashboard at `http://localhost:3455`:

1. **Runtimes** → install a gateway package (e.g. [`mass-runtime-gateway-llama-cpp`](https://github.com/chinese-room-solutions/mass-runtime-gateway-llama-cpp)) and start it.
2. **Workers** → **Add worker** (or `mass workers install-local` for the MASS host itself). MASS serves the [worker](https://github.com/chinese-room-solutions/mass-worker-llama-cpp) installer and prints the download-and-run commands for each GPU machine — see **Joining workers** below.
3. **Models** → install or import a model. Models must enter through MASS: they land in the hub's store, and workers fetch what they run from there — a model dropped on a worker's own disk is invisible to the fleet.
4. Send inference at the gateway's proxied endpoint. MASS strips the `/mass.<runtime>` prefix and forwards the rest — `/mass.llama-cpp/v1/...` reaches the gateway's OpenAI-compatible shim at `/v1/...`, while `/mass.llama-cpp.v1/...` selects its typed API (`Chat`, `Jobs/{id}`, ...):
   ```bash
   curl -X POST http://localhost:3455/mass.llama-cpp/v1/chat/completions \
     -H 'Content-Type: application/json' \
     -d '{"model":"my-model","messages":[{"role":"user","content":"hi"}]}'
   ```

### Running a downloaded release on macOS

The macOS release ships an ad-hoc-signed `MASS.app`. It is **not notarized** by Apple
(notarization needs a paid Apple Developer account), so on first launch macOS shows:

> "Apple could not verify 'MASS' is free of malware…"

This is expected for any non-notarized app — it is a Gatekeeper warning, not a problem
with the build. Approve it **once** and it never appears again for that copy:

- **Recommended:** right-click (or Control-click) `MASS.app` → **Open** → **Open** in the dialog.
- Or: try to open it, then go to **System Settings → Privacy & Security** and click **Open Anyway**.
- Or, from the terminal, clear the quarantine flag:
  ```bash
  xattr -dr com.apple.quarantine /Applications/MASS.app
  ```

After approving once, MASS launches normally by double-click.

## Command-line management

The `mass` binary is also a client for its own `mass.v1` management API, so the whole control surface is scriptable without the dashboard. A leading subcommand runs the CLI (`mass serve` runs the daemon; `mass -version` prints the build). A verb aimed at the local address starts the daemon on demand when none is running:

```bash
mass status                                   # orchestrator health
mass runtimes search                          # gateway packages in the registry
mass runtimes install mass-runtime-gateway-llama-cpp   # install (and start) one
mass runtimes list                            # installed gateways
mass runtimes start llama-cpp                 # bring a gateway up
mass models import-remote --runtime llama-cpp --repo owner/model --file q4.gguf
mass workers list                             # fleet state + device IDs
mass queue list                               # inspect queued/running jobs
mass queue cancel --help                      # synopsis + detail for any command
```

`runtimes install` takes the registry **package** name; `start`/`stop`/`uninstall`
and every `--runtime` flag take the **runtime** name that package declares
(`mass-runtime-gateway-llama-cpp` → `llama-cpp`). Both columns show in `runtimes search`.

The verb groups mirror the dashboard tabs: `status`, `models`, `runtimes`, `workers`, `scheduler`, and `queue`. Shared flags on every command that reaches the server: `--addr` (target URL; defaults to `$MASS_ADDR`, else the local config), `--token` (`$MASS_AUTH_TOKEN`), `--json` (raw protojson — use this when parsing), and `--timeout`. Errors map to exit codes `0`/`1`/`2` (ok/error/usage).

### Agent skill

For the full verb reference and common workflows, point your agent at the [`mass-cli` skill](skills/mass-cli/SKILL.md).

The skill is a plain Markdown instruction file, tied to no particular agent, and it ships inside the binary — so it documents the verbs your build actually has, with no checkout required:

```bash
mass skill                    # print it (pipe it wherever you like)
mass skill install <dir>      # write it to <dir>/mass-cli/SKILL.md
```

`<dir>` is whatever directory your agent discovers skills in — there is no default, and no server is needed, so this works on a fresh install. Reinstall after upgrading MASS: an old copy describes verbs that may have moved.

## Build commands

Run through the `Makefile` (works on Linux, macOS, and Windows under Git Bash / MSYS2).

| Command | Description |
|---------|-------------|
| `make build` | Web assets + Go build → `bin/mass` |
| `make build-headless` | Server build: no window/tray, CGO-free static binary |
| `make build-web` | Web assets only (templ + Tailwind) |
| `make run` | Build and run |
| `make package` | Self-extracting installer (app + setup wizard) → `dist/` |
| `make bundle-macos` | macOS-only: build an ad-hoc-signed `bin/MASS.app` |
| `make test` / `make unittest` | Tests with `-race` / with `-short` |
| `make lint` / `make vulncheck` | golangci-lint / govulncheck |
| `make fmt` / `make tidy` / `make clean` | format / `go mod tidy` / remove `bin/` |

## Configuration

A single YAML file in the user config dir (`%APPDATA%/mass/config.yml` on Windows, `~/.config/mass/config.yml` on Linux). Defaults are written on first run; most keys are also editable live in the **Settings** tab.

```yaml
listen_addr: "127.0.0.1:3455"  # also serves gRPC over the same port; non-loopback binds require auth_token
data_dir: ""                # root for runtimes, models, DB (platform default if empty)
theme: dark                 # dark | light
result_ttl: "24h"           # how long completed job results are kept
idle_eviction_ttl: "30s"    # idle time before a loaded model is unloaded
stream_replay_ttl: "30s"    # how long a finished job's stream chunks stay replayable
load_attempts: 1            # placement attempts before failing (1 = no retry)
db_dialect: ""              # "" = SQLite (default), or "postgres"
db_dsn: ""                  # Postgres DSN when db_dialect: postgres
logger:
  level: info               # trace | debug | info | warn | error
tls:
  enabled: false
  cert_file: ""             # PEM with cert + key; enables TLS for API + workers
```

The **auth token** isn't stored here — set it in the Settings tab (persisted hashed in the DB; empty = no auth). `MASS_AUTH_TOKEN` overrides it. By default MASS serves plaintext h2c, fine for localhost and trusted networks.

## Joining workers

MASS hands out a download command plus a run command that installs the matching worker on another machine. Get them from `mass workers join-command` (or the **Add worker** button on the Workers tab):

```bash
# 1. Download the installer (uname picks the right OS/arch build):
curl -fsSL "http://<mass-host>:3455/setup/worker-bin/llama-cpp?os=$(uname -s)&arch=$(uname -m)" -o mass-worker-setup && chmod +x mass-worker-setup
# 2. Run it — an interactive wizard walks you through scope/dirs/options:
./mass-worker-setup --mass-url http://<mass-host>:3455 --token <TOKEN>
```

On Windows, download with `irm` (arch hardcoded to `AMD64`) and run `.\mass-worker-setup.exe`. `--mass-url` and `--token` are just prefilled defaults for the wizard; nothing is forced. The `/setup/*` endpoints are unauthenticated (the join token rides only in the pasted command line), and the worker-bin path accepts uname-style `os`/`arch` values (`Linux`/`Darwin`/`Windows`, `x86_64`/`aarch64`, …).

### Air-gapped artifact cache

MASS proxies and caches worker installers content-addressed by the index's sha256, under:

```
<data_dir>/registry-cache/artifacts/<sha256>
```

For a LAN with no registry access, drop the installer files there named by their sha256 (from the index) ahead of time; MASS serves them straight from the cache and never reaches the network.

## License

[FSL-1.1-ALv2](LICENSE.md) — source-available: use, modify, and redistribute
freely for anything except a competing product or service; each release
converts to Apache-2.0 two years after publication.
