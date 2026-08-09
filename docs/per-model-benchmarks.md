# Per-model benchmarks

Replace all proxy-based estimation with one measured row per
(worker, device set, model). A device set is what one load occupies — the
worker's rule is fixed (all enabled GPUs, else CPU; `predictDeviceSet`
mirrors it), so it's a single device in the common case and a canonical
sorted list under tensor split. Throughput of a split load isn't
decomposable per device, so the set is the measurable unit.
Principle: as simple as possible — flexibility later.

## What goes away

| Mechanism | Today | Fate |
|---|---|---|
| `throughput_corrections` (mass.db) | live EWMA per (worker, axis), corrects proxy bias | **deleted** — table, `store/throughput_correction.go`, scheduler update path |
| `.calibration-cache` (worker models dir) | frozen graph_secs + slot deltas, drives auto slot ceiling | **deleted** — worker self-calibration removed, measurement code reused by the bench |
| Cost axes | `CostAxis` on jobs, per-axis `throughput` map in proto, axis-fallback path | **deleted** — Cost becomes a scalar in model-native units |
| `device_benchmarks` (mass.db) | per-axis generic throughput, feeds estimates | **demoted** — one matmul FLOPS number per device, display only, never consulted by the scheduler |
| Envelope `BaseLoadBytes`/`PerSlotBytes` | gateway-predicted memory hints per Submit | **deleted** — the memory gate reads measured bytes from the row |

## The row

`model_benchmarks` in mass.db (both dialects, edited into `000001_init` — repo is unreleased, no new migration files):

```sql
CREATE TABLE model_benchmarks (
    worker_id      TEXT NOT NULL,
    device_set     TEXT NOT NULL,  -- canonical sorted device-id list, e.g. "gpu:0"
    model_id       TEXT NOT NULL,
    units_per_sec  REAL NOT NULL,  -- payload cost / measured wall time
    graph_secs     REAL NOT NULL,  -- one full-ubatch decode on this hardware
    base_bytes     INTEGER NOT NULL,
    per_slot_bytes INTEGER NOT NULL,
    model_size     INTEGER NOT NULL,
    model_mtime    INTEGER NOT NULL,
    error          TEXT,           -- NULL = usable; set = incapable, measurements zero
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (worker_id, device_set, model_id)
);
```

- Lookup at placement: the row for the worker's *current* predicted device
  set. Row with `error IS NULL` ⇔ schedulable there. Row with `error` set ⇔
  this device set can't run this model — never re-benched automatically.
  No row ⇔ bench hasn't concluded (running or retrying; that state lives in
  the scheduler, not the table).
- Rows for non-current device sets are kept — toggling devices back reuses
  the old row, no re-bench. Only the current set is ever benched; MASS can't
  target a non-predicted set anyway (the worker owns placement).
- Validity: stored `model_size`/`model_mtime` vs current. Mismatch → all of
  the model's rows treated as absent → re-bench.
- Cleanup: delete rows on model removal and worker removal (plus worker-side
  file deletion on model removal). Stale device-set rows die with them.
- Memory bytes mirror the existing gate's granularity (aggregate scalars).
  Per-device split is a later flexibility.

## Bench flow

Trigger: a (worker, current device set, model) triple with no valid row,
discovered

- when a model download completes (`downloads` manager) → bench on every
  connected worker of the matching runtime,
- when a worker connects → bench every model missing a row for its current
  device set,
- when the device whitelist changes → bench every model missing a row for
  the new predicted set (old rows kept),
- on model file change or manual re-bench in the UI.

Mechanics — one new RPC pair in worker.proto:

- `HubModelBenchmark { model_id, files, load_hints, payload, cost }` — files
  travel exactly as in `HubLoadModel`; the worker **keeps them** afterward
  (catalog mirror — this is the distribution change). `payload`/`cost` are
  gateway-authored: a representative request for the model's kind (chat vs
  embed), sized to run seconds not minutes, with known total cost in the
  model's own units.
- Worker: load pool-1 → run payload (wall-time it) → time the calibration
  graph → grow one extra slot to measure per-slot delta → read base bytes →
  unload → `WorkerModelBenchmarkResult { elapsed_secs, graph_secs,
  base_bytes, per_slot_bytes }`. Reuses the existing `time_calibration_graph`
  and headroom-delta code, repackaged.
- MASS writes the row (`units_per_sec = cost / elapsed_secs`).

Exclusivity: the scheduler dispatches nothing to a worker while its bench RPC
is in flight (per-worker gate). Benches on one worker run sequentially; jobs
for already-benched pairs dispatch between benches. Other workers unaffected.

Before benching, MASS clears the target device set of idle residents (the
existing device-set gate machinery) — measured on an empty device, an
allocation failure is a genuine capability verdict, not contention.

Failure handling — the result carries a worker-classified kind:

- **incapable** (allocation/OOM, model too large for the set): no retry.
  MASS writes the row with `error` set — this set can't run this model,
  permanently, until manual re-bench or a model-file change wipes it.
- **transient** (anything else — crash, I/O, unclassifiable): retry with
  backoff, capped (3 attempts, 30s → 5m). After the cap, persist as
  incapable with the last error.

Worker dies mid-bench → no row → re-benches on reconnect. Jobs whose model
has *concluded* everywhere with no usable row fail immediately with the
bench error; jobs wait only while at least one bench is still pending.

Gateway contract: one new mass.v1 call, `AuthorBenchPayload(model) →
{ payload, load_hints, cost }`. The gateway owns what "representative" means
per model kind; MASS never interprets the payload.

## Scheduler changes

- Placement: a worker is a candidate for a job only with a valid row for
  (its current device set, the job's model). No candidate anywhere → job
  waits in the global queue (bench is already running or retrying; the job
  does not trigger anything).
- Estimates: `Cost / units_per_sec`, from the row. No correction factor, no
  axis lookup. Same job → same estimate.
- Pool size: MASS computes `clamp(floor(budget / graph_secs), 1, cap)` and
  sends it as the load's `max_concurrent`. `budget` (2.5s) and `cap` (16)
  move from `ctx_pool.hpp` to MASS config. The worker's VRAM headroom gate
  still bounds actual growth at load.
- Memory gate: `base_bytes + n·per_slot_bytes` from the row.
- `dispatchEnvelope`'s load-on-demand path is unchanged — files are already
  on the worker, so it degenerates to a warm load.

## Per-repo summary

- **mass-proto**: add `HubModelBenchmark`/`WorkerModelBenchmarkResult` (the
  result carries measurements or a classified failure: incapable vs
  transient); drop
  `CostAxis`; `WorkerBenchmarkDevice.throughput` map → single `flops` double;
  drop envelope memory-hint fields. Regenerate pinned.
- **mass**: table + store; bench orchestration (triggers, exclusivity gate,
  retry, invalidation); estimation/pool/memory-gate rewiring; delete EWMA and
  axis paths; UI: manual re-bench, bench-in-progress state, FLOPS display.
- **mass-worker-llama-cpp**: implement the bench RPC (reuse calibration
  code); delete `.calibration-cache` and auto-ceiling; honor pushed
  `max_concurrent`; keep files after bench; delete files on model removal.
- **mass-runtime-gateway-llama-cpp**: `AuthorBenchPayload` for chat and
  embed kinds; stop emitting axis/memory hints on Submit.

Breaking changes throughout are fine — nothing is released. Old
`.calibration-cache` files and the two dropped tables are deleted, not
migrated.

## Out of scope (flexibility later)

Per-device memory rows, drift detection/TTL re-bench, bench-payload tuning
per quantization, opt-out of catalog mirroring, re-keyed EWMA.
