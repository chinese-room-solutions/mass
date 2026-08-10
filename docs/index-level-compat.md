# Index-level compatibility + contract versioning

Approved 2026-08-10. Replaces per-binary semver compat with two layers:

1. **Distribution compat** — semver ranges live only in the registry indexes.
   Editable after release, validated by CI, single source of truth.
2. **Wire safety** — a small integer protocol version negotiated at every
   service boundary. Compiled in, bumped only on breaking wire changes,
   rejects cleanly at connect time. Works offline and for out-of-band builds.

Everything between the two disappears: the worker's compiled
`kCompatibleRuntimes`, the `compatible` handshake field, the frozen
exact-equality `GatewayAPIVersion`, Grimoire's unread `grimoire:` field.

## Why

The 2026-08-10 worker-bin 404 came from four hand-synced copies of one fact.
Index-only compat removes the copies instead of guarding them. The protocol
version replaces the one run-time check that removal would lose (hub Register),
and extends the same protection to gateway Init and Grimoire kernel load,
which today have none that works.

## Protocol negotiation (all three boundaries)

The caller sends the list of protocol versions it speaks, the callee picks the
highest it also supports and echoes it, no intersection = clean rejection with
both lists in the error. Lists, not exact ints, so one binary can span a
breaking boundary and pairs never have to ship in lockstep again.

| Boundary | Carrier | Start at |
|---|---|---|
| worker → hub | `WorkerRegistration.protocol_versions` (new, `repeated int32`), replaces `compatible` (reserve the field number) | `{1}` |
| MASS → gateway Init | `InitRequest.supported_protocols` + `InitResponse.protocol`, replace the exact `gateway_api_version` ints (reserve) | `{1}` |
| Grimoire kernel load | `protocol: 1` in `*.kernel.yaml`, checked against the core's supported set at load; missing or unsupported = kernel skipped with a clear log (same path as a bad manifest) | `{1}` |

Go constants live in hand-written files in mass-proto `gen/go`
(`gateway/version.go` reworked, `worker/version.go` new) with a shared
negotiate helper. The C++ worker hardcodes its list — two ends of one
contract, and a drift is exactly what the negotiation detects.

## Semver compat at Register (index-driven)

Hub Register, after protocol negotiation: load the cached registry index
(cache-only, never the network), find worker rows joined by the installed
runtime's `runtime_name`, match the row whose `version` equals the worker's
reported version, check its `runtime:` range against the installed gateway
version and its `mass:` range against the server version. No cache, no row,
or a non-semver worker version → accept with a warning log (dev builds,
out-of-band installs). An index edit changes the verdict on the next index
refresh — that is the point.

## Changes by repo

**mass-proto** — proto changes above, regenerate `gen/go` with the pinned
plugins, version consts + negotiate helper. Python gen stays untouched
(already stale, out of scope).

**mass-sdk** — `Version.Grimoire` field (`grimoire` yaml tag) so the range
finally parses. Compat-aware resolve usable by Grimoire (semver-newest,
range-enforced, artifact-present — reuse `versionSatisfies`/`resolve`
internals). Cache-only index load on the client for the Register path.
`cmd/registry-validate`: validates an index.yml — schema_version, unique
names, semver versions ascending, ranges parse, artifacts carry url +
hex-or-TBD sha256; `--verify-artifacts` downloads and hashes. Used by both
registry CIs.

**mass** — hub Register rework (protocol + index check, `compatible` gone),
gateway Init negotiation, drop the now-dead range plumbing
(`hub.go` handshake validation, `registryops` worker-compat duplication where
it hand-rolls what the SDK resolver now owns).

**mass-runtime-gateway-llama-cpp** — the response side of Init negotiation:
`protocol.Negotiate` over the request's `supported_protocols`, echo the pick,
refuse on empty intersection. Manifest version stays owner-managed.

**mass-worker-llama-cpp** — delete `kCompatibleRuntimes`, send
`protocol_versions`, handle rejection. CI: pin the mass-proto checkout to a
ref instead of the default branch.

**grimoire** — kernel + theme installs go through the SDK compat resolver
(kills positional-newest and the ignored ranges), kernel loader checks
`protocol:`, embedded builtin kernels get the field.

**grimoire-kernel-go / -python / -yaegi** — `protocol: 1` in the manifests,
packer test updated.

**mass-registry / grimoire-registry** — CI running `registry-validate`
(checkout mass-sdk, run from source; digest verification on
workflow_dispatch + weekly schedule, URL/schema checks on every push).
README additions: the skew-window statement (a worker floats within a
gateway minor) and the pair-publishing ordering rule currently living in
commit `87c95aa`'s message.

## Out of scope (follow-ups)

Theme installed-version record (upgrade detection), GUI pinning the version
it displays, python gen version, go-plugin handshake literal dedup.

## Rollout

Waves of two agents, never two in one repo: (1) mass-proto + mass-sdk,
(2) mass + mass-worker-llama-cpp, (3) grimoire + registries, kernel repos
alongside. One commit per repo, local only, no tags — owner tags and does
the pin sweep at release (proto `gen/go/v0.3.0`, sdk `v0.2.0` expected).
Cross-repo builds during development resolve through an untracked `go.work`
in the workspace parent dir — never committed, no `replace` directives in
any go.mod. Verification per repo: `make lint`, `make test`, plus a live
XDG-isolated smoke of Register (worker accept + protocol-mismatch reject)
before the final report.
