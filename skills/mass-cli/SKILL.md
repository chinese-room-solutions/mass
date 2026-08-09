---
name: mass-cli
description: Manage a MASS instance — runtimes, models, workers, scheduler, queue — from the command line via the mass.v1 API.
---

# mass CLI

The `mass` binary doubles as a client for its own `mass.v1` management API. A
leading non-flag argument (`mass status`) runs the CLI; `mass -headless` /
`mass -version` still boot the server.

## Connection & auth

- `--addr <url>` — base URL. Default: `$MASS_ADDR`, else the local config's
  listen address (scheme follows its TLS setting), else `http://127.0.0.1:3455`.
- `--token <t>` — bearer token. Default: `$MASS_AUTH_TOKEN`. Optional on
  loopback; required when the server binds a routable address.
- `--json` — emit the raw protojson response. **Always pass `--json` when
  parsing output**; the default table format is for humans and unstable.
- `--timeout <dur>` — request timeout (default `60s`; `workers benchmark`
  defaults `10m`).

Every request carries `X-Mass-Actor: cli` for audit attribution.

## Verb reference

| Command | RPC | Example |
|---------|-----|---------|
| `mass status` | GetStatus | `mass status --json` |
| `mass models list [--runtime R]` | ListModels | `mass models list --json` |
| `mass models import-local --runtime R --path P` | ImportLocalModel | `mass models import-local --runtime llama-cpp --path /srv/m.gguf` |
| `mass models import-remote --runtime R --repo O/M --file F` | ImportRemoteModel | `mass models import-remote --runtime llama-cpp --repo Qwen/Qwen2.5-0.5B-Instruct-GGUF --file qwen2.5-0.5b-instruct-q4_k_m.gguf` |
| `mass models delete --runtime R --id ID` | DeleteModel | `mass models delete --runtime llama-cpp --id abc` |
| `mass runtimes list` | ListRuntimes | `mass runtimes list --json` |
| `mass runtimes search [QUERY] [--kind K] [--runtime R]` | SearchPackages | `mass runtimes search llama --kind runtime` |
| `mass runtimes install NAME[@version]` | InstallRuntimeFromRegistry | `mass runtimes install mass-runtime-gateway-llama-cpp` |
| `mass runtimes install --path pkg.mass` | InstallRuntime | `mass runtimes install --path /srv/llama.mass` |
| `mass runtimes uninstall NAME` | UninstallRuntime | `mass runtimes uninstall llama-cpp` |
| `mass runtimes start NAME` | StartRuntime | `mass runtimes start llama-cpp` |
| `mass runtimes stop NAME` | StopRuntime | `mass runtimes stop llama-cpp` |
| `mass runtimes auto-start NAME --enabled=BOOL` | SetRuntimeAutoStart | `mass runtimes auto-start llama-cpp --enabled=true` |
| `mass workers list` | ListWorkers | `mass workers list --json` |
| `mass workers install-local [--runtime R] [--scope user\|system] [--name N]` | InstallLocalWorker | `mass workers install-local` |
| `mass workers join-command [--runtime R] [--worker W] [--backend B] [--mass-url U] [--ttl D]` | CreateJoinToken | `mass workers join-command --ttl 30m` |
| `mass workers enable ID` / `disable ID` | SetWorkerEnabled | `mass workers disable w1` |
| `mass workers device enable WORKER DEVICE` / `disable ...` | SetWorkerDeviceEnabled | `mass workers device disable w1 gpu0` |
| `mass workers benchmark [--workers a,b] [--devices x,y]` | BenchmarkWorkers | `mass workers benchmark --timeout 15m` |
| `mass scheduler list` | ListInstances | `mass scheduler list --json` |
| `mass scheduler evict --worker W --model M` | EvictInstance | `mass scheduler evict --worker w1 --model m1` |
| `mass queue list` | GetQueue | `mass queue list --json` |
| `mass queue cancel --queue Q --msg-id ID` | CancelQueuedJob | `mass queue cancel --queue global --msg-id 01H...` |
| `mass queue cancel-running --request-id ID` | CancelRunningJob | `mass queue cancel-running --request-id r1` |
| `mass queue evict --queue Q --msg-id ID` | EvictQueuedJob | `mass queue evict --queue global --msg-id 01H...` |
| `mass skill [show]` / `install DIR` | — (local) | `mass skill install ~/.claude/skills` |

Positional-name verbs accept flags after the name (`mass runtimes start llama-cpp --json`).

`runtimes install NAME` takes the **package** name from the registry
(`mass-runtime-gateway-llama-cpp`); `start` / `stop` / `uninstall` and every
`--runtime` flag take the **runtime** name the package declares (`llama-cpp`).
Both appear in `mass runtimes search`. Installing also starts the gateway and
sets auto-start.

## Example JSON output

`mass runtimes list --json` (truncated):

```json
{
  "runtimes": [
    {
      "runtime_name": "llama-cpp",
      "version": "1.0.0",
      "running": true,
      "auto_start": false
    }
  ]
}
```

Repeated fields that are empty are omitted entirely, so an idle server may
return `{}`.

## Workflows

**Bring up inference:**
```
mass runtimes search --kind runtime --json          # available gateway packages
mass runtimes install mass-runtime-gateway-llama-cpp  # installs and starts it
mass workers install-local                          # a worker on this host
mass models import-remote --runtime llama-cpp --repo owner/model --file q4.gguf
mass models list --runtime llama-cpp --json         # the id to send as "model"
```

`models import-remote` returns as soon as the import is planned — the bytes
download in the background, and the model appears in `models list` when done.

**Diagnose a stuck job:**
```
mass status --json                        # queued/running counts
mass queue list --json                    # find the offending msg_id / request_id
mass queue cancel --queue global --msg-id <id>          # not yet started
mass queue cancel-running --request-id <id>             # in flight
```

**Manage capacity:**
```
mass workers list --json                  # who is online, device IDs
mass workers device disable <worker> <device>          # drain a GPU
mass workers benchmark --workers <worker> --timeout 15m # re-measure throughput
```

## Exit codes

- `0` — success
- `1` — RPC/runtime error (`--json` prints `{"error":...,"code":...}` with the
  Connect code, e.g. `not_found`)
- `2` — usage error (unknown verb, missing required flag), help to stderr

Benchmarking loads a model on every selected device and can take minutes —
raise `--timeout` accordingly.
