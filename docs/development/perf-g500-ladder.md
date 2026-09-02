# Bounded billion-edge scale ladder (#736)

This is the M5 ([#735](https://github.com/DecisionNerd/graphforge/issues/735))
**root gate**: a versioned, **bounded-memory** Graph500-parameter (`edgefactor=16`
undirected Kronecker/R-MAT) scale ladder that measures the *first real
bottleneck* on the published Rust facade before any billion-edge certification
([#745](https://github.com/DecisionNerd/graphforge/issues/745)).

It is a thin in-tree **reference client** under the
[Scale Evaluation](../reference/scale-evaluation.md) contract — **not**
Official-track and **not** TEPS. It does **not** itself claim one billion live
edges.

| Claim | Status |
|---|---|
| Official Graph500 submission | No |
| `track: official` in evidence JSON | Must not be set (`track` is `null`) |
| Pinned `github.com/graph500/graph500` generator | No — bounded bench-local Kronecker in the test file |
| Graph500 BFS kernel / harmonic-mean TEPS | Non-goal (`teps` is `null`) |
| One-billion-live-edge product certification | No — that is #745 |
| Engineering green (generate → ingest → reopen → GSI → `LIMIT 1000`) | Yes, through one resumable `GraphConstructionSession` + `execute` |

## What is new versus the #710 SCALE-20 client

[perf-g500-scale20.md](perf-g500-scale20.md) retains **every** raw tuple in
memory (a `Vec` of all `2^scale * 16` attempts) and sorts/deduplicates in RAM.
That is fine at SCALE-20 but allocates ~8.6 GiB of raw tuples at SCALE-26.

This ladder replaces that with **external sort + spill + k-way merge**:

1. Generate Kronecker attempts into a fixed `buffer_edges` buffer.
2. When full, sort the buffer and **spill a run file** to disk (no in-buffer
   dedup, so every duplicate is counted at merge).
3. K-way merge the sorted runs, emitting each unique undirected pair once.

Peak resident edges never exceed `buffer_edges`, **independent of total edge
count**. Nodes and merged live edges are appended as bounded Arrow chunks to
one disk-owned construction session. Its opaque UUID is fsynced before append,
so an interrupted rung resumes the same session; publication performs exactly
one `CURRENT` transition after all chunks are sealed.

## Counts always reconcile

Every attempted rung proves:

```
raw_attempts == live_unique_edges + self_loops_rejected + duplicates_rejected
```

`raw_attempts` (`2^scale * 16`) can therefore **never** be reported as live
persisted edges. SCALE-26 produces `1,073,741,824` raw attempts; the live
persisted count after self-loop and duplicate policy may fall **below** one
billion, and the evidence records both distinctly.

## The versioned profile

The ladder, seed, initiator, policy, envelope, metrics, and exact invocation
live in a single committed profile,
[`crates/graphforge-api/tests/fixtures/scale_g500_ladder.v1.json`](../../crates/graphforge-api/tests/fixtures/scale_g500_ladder.v1.json)
(`schema: graphforge-billion-edge-ladder/1`). The runner reads it; do not
hard-code rungs elsewhere.

| Field | Value |
|---|---|
| Rungs | S10 (CI), S20, S22, S24, S25, S26 (provisioned) |
| Seed / initiator | `1` / `A,B,C,D = 0.57, 0.19, 0.19, 0.05` |
| Policy | undirected, drop self-loops, drop duplicates, canonical `(lo,hi)` |
| Host capacity | **128 GiB** peak RSS, **1 TiB** local NVMe (declared **Linux cloud** SKU) |
| Wall-clock fail-safe | **4 h** (`timeout_s: 14400`) end-to-end on that SKU — provisional #745 budget, not a laptop or “overnight is fine” product claim |

Before any provisioned rung, complete the read-only/no-spend checks from the
[certification runbook](g500-certification.md): subscription and role, provider
registration state, regional SKU restrictions and quota, current on-demand
price, protected environment, and unique runner-label availability. Provider
registration, quota requests, runner-token generation, provisioning, and
dispatch require recorded resource and spend authorization.

The declared certification filesystem is the filesystem the Rust process
actually uses. Configure process `TMPDIR` and GitHub `RUNNER_TEMP` on the same
local-NVMe XFS mount and verify their resolved filesystem/device identity before
dispatch. Merely attaching NVMe or checking a different runner directory is not
capacity evidence. Absolute paths stay in the private provisioning log and are
excluded from sanitized evidence.

The **S10** rung runs in normal CI and deliberately sets `buffer_edges` below
its own edge count, so the spill/merge path is exercised on every run. S20–S26
are `#[ignore]` and opt-in on **provisioned Linux cloud / evidence hosts**
only. Developer laptops (including macOS Air-class machines) must not be used
as #745 certification SUTs; local runs are dry-runs at best.

Record the full provider/region/SKU and immutable Linux image version for every
provisioned observation. Use on-demand capacity with a unique ephemeral runner;
an eviction-prone Spot/low-priority host cannot provide controlled cancellation
or wall-time evidence. A five-hour infrastructure TTL bounds billing and
cleanup, while the product certification must still finish within 14,400
seconds.

## First-fail ladder

`run_ladder` walks the provisioned rungs in increasing scale and, after each
phase (`generate`, `ingest`, `reopen`, `query`), compares peak RSS / disk /
elapsed time against the envelope. On the **first** violation it records the
failing phase and `error_class` (`oom` | `disk_exhaustion` | `timeout`) and
stops — no larger rung is attempted and no SCALE-26 pass is claimed.

> RSS fidelity: peak RSS is read from `/proc/self/status` `VmHWM` on Linux
> (a true high-water mark) and falls back to sampled `ps` RSS otherwise (an
> *instantaneous lower bound*, not a peak). Evidence records `rss_source`
> (`vmhwm` | `ps_sampled`) so a `ps_sampled` value is read as a floor. Run
> provisioned certification rungs on **Linux**.

Provisioned runs must set `GF_G500_LADDER_WORKSPACE` to a stable run directory.
If omitted, the runner derives `workspace/<rung>` beside
`GF_G500_LADDER_JOURNAL_OUT`; it refuses a provisioned rung when neither path is
available. The fsynced opaque construction-session UUID and spill/project state
therefore survive process replacement and are reused on re-entry.

> Ingest-phase attribution: construction writes bounded node and edge Arrow
> windows into immutable Parquet shards and retains only bounded merge/probe
> state; accumulated topology remains disk-owned. While ingest runs, the atomic journal is
> refreshed every two seconds with the current subphase, edge-chunk index,
> anonymous/file RSS, and aggregate topology rewrite counters. It deliberately
> does not recursively walk the active project. Disk attribution comes from
> storage-owned counters and exact descriptors at completed phase boundaries. An
> `oom` with `first_failing_phase: "ingest"` therefore remains an upstream
> construction failure, not a generator-memory regression. Each completed
> ingest phase records elapsed time, RSS, disk bytes, shard count, input rows,
> batches, writes, and authentication reads so 1x/2x/4x observations can test
> bounded memory and linear topology work directly.

The scale client has one authoritative construction boundary: 65,536 rows,
matching `GraphConstructionBudgets::default().max_batch_rows`. Each full Arrow
batch is submitted as one durable construction chunk. There is no smaller
8,192-row subdivision and no larger outer edge buffer. This matters because a
chunk intentionally owns authenticated artifacts, intent/receipt/checkpoint
updates, and file/directory durability barriers; choosing a smaller Arrow
window multiplies that fixed durability work without improving the session's
bounded-memory guarantee. Ingest evidence records the configured rows per
chunk, submitted chunks, artifact and synchronization counts, append elapsed
time, reconciliation elapsed time, and combined seal/publication elapsed time.
Fresh staging reports append time and a null reconciliation time; published
re-entry reports reconciliation time and a null append time. The immutable
artifact count is storage-owned evidence reconstructed, for legacy
checkpoints, from the authenticated receipt chain. Deterministic 1x/2x/4x
tests require exact chunk counts and bounded peak windows while aggregate
merge work remains linear in accepted rows.

Query-phase ordered-LIMIT evidence must distinguish complete candidate
examination from materialization amplification. For the canonical fixed-hop
destination-UUID query, record expansion chunks/candidates/projected columns,
edge and node Parquet calls/rows, and v4 ordinal ranges/coalesced calls/bytes/
peak charged buffer/per-record seeks plus session revalidation calls/bytes.
Generation authentication is charged once per execution session and must scale
linearly with retained immutable artifacts, never expansion chunks multiplied
by artifact count. Edge and node Parquet calls are exactly
zero, per-record seeks are exactly zero, ordered source and imported results
are byte-identical, and 1x/2x/4x ordinal work remains a linear constant factor
of candidates examined. A bounded RSS result does not pass if logical reads
grow as expansion chunks multiplied by graph size.

## Commands

Always-on CI (SCALE-10 smoke + all reconciliation / determinism / bounded /
first-fail unit tests):

```bash
cargo test -p graphforge-api --test scale_g500_ladder
```

Provisioned full ladder (long; isolate the target dir; **Linux cloud scale-host**
matching the declared SKU — not a developer laptop for #745 evidence):

```bash
GF_G500_LADDER_MAX_SCALE=25 CARGO_TARGET_DIR=/tmp/cargo-g500-ladder make bench-g500-ladder
# override the evidence path:
GF_G500_LADDER_MAX_SCALE=25 GF_G500_LADDER_EVIDENCE_OUT=build/g500-ladder-evidence.json make bench-g500-ladder
```

`GF_G500_LADDER_MAX_SCALE` is mandatory and must name a provisioned rung in the
profile. It is an authorization ceiling, not a performance hint: selecting 25
runs S20, S22, S24, and S25, and excludes S26 even when every smaller rung
passes.

Default evidence path: `docs/development/g500-ladder-evidence.json`.
The runner also atomically updates `build/g500-ladder-journal.json` before and
after every phase and after every rung. Retrieve the journal after an OOM,
ENOSPC, timeout, or operator safety stop; `completed_rungs` remain valid, while
`active_rung`, `active_phase`, and `run_state` describe the interrupted work and
must not be presented as a pass. On Linux, each journal observation separates
process `VmHWM`, current RSS, anonymous RSS, and file-backed RSS so filesystem
cache is not mistaken for GraphForge heap growth.

## Evidence JSON

One object per attempted rung (schema
`graphforge-billion-edge-ladder-evidence/1`), carrying:

- `counts`: `raw_attempts`, `self_loops_rejected`, `duplicates_rejected`,
  `live_unique_edges`, and the reconciliation identity.
- `persisted`: reopened `node_count` / `edge_count` (must equal
  `live_unique_edges`).
- `first_failing_phase`, `error_class`, `pass`, `reconciles`.
- `input_fingerprint` (deterministic SHA-256 of the sorted live edge set),
  `rss_peak_bytes`, `disk_used_bytes`, `wall_time_s`, per-phase `steps`.
- `machine_envelope` (128 GiB / 1 TiB / 4 h fail-safe), `sut`, `generator`.
- `track` and `teps` are always `null`.

Wall-clock and RSS numbers are hardware-specific observations, never CI
millisecond gates. For #745, `sut` must name the cloud SKU; laptop SUTs are
rejected as certification evidence.

### Disk attribution and S26 admission

The versioned `graphforge-g500-ladder-qualification/3` companion document is
produced and validated by
`graphforge_bench.progressive_storage_qualification`. Every observed
rung has exactly one row for canonical node topology, canonical edge topology,
properties, UUID/surrogate indexes, adjacency/CSR, catalogs/manifests,
construction staging/spill, portable package, and clean import. Native file
identity deduplicates content-addressed/shared objects. Logical bytes,
filesystem allocation, current retained allocation, and full-lifecycle
transient peak remain separate quantities.

Artifact rows are local ownership views and may refer to the same physical CAS
object. Their allocated/current columns therefore are not summed to obtain the
workspace footprint. `totals.current_retained_bytes` is the independently
reconciled native-identity union across all simultaneously retained owners.
The authoritative-project ratio uses a separate source-project native-identity
union captured at the stable source boundary. It includes the project controls,
every retained generation, and every CAS object not yet removed by an exact GC
receipt. It is therefore bounded below by the selected-generation snapshot and
above by the independent workspace union, but it does not include construction
staging, the portable package, drills, or the clean imported project.

The lifecycle peak is not reconstructed by adding category peaks or directory
sizes. Storage owns a reference-counted union keyed by authenticated native
`(volume, file-id)` identities. Append, shaping, encoding, CAS publication,
portable export, private import materialization, clean publication, corruption
drills, and interrupted-import cleanup install or remove exact owners. The
high-water mark advances at each transition, so aliases count once and files
that did not coexist are never added together. Certification emits
`storage_owned_active_identity_union` provenance only from this tracker.

The project owner includes `FORMAT`, `CURRENT`, and every authenticated
generation still installed in the bounded generation namespace, including
checkpoint branches and generations not yet reclaimed; publication does not
discard an old generation from accounting merely because `CURRENT` advanced.
It also includes the CAS lifecycle control and every sealed CAS object, including
objects not referenced by the current generation. Only an exact explicit GC
receipt may remove those identities. The bounded CAS inventory runs at retained
phase boundaries, never during active ingest. Portable writers record native allocation as files
are written, synchronized, published, or removed; they never rediscover a
large export with a recursive post-write directory pass, and measurement
failure is a typed operation failure rather than a zero observation.
Portable import success additionally carries an identity-safe cleanup receipt;
the materialization owner is removed only after authenticated deletion and
parent-directory synchronization complete. Cleanup failure is a typed import
failure and its still-owned identities remain in lifecycle evidence.

The same document carries a closed nine-phase inventory: append/merge, seal
authentication, shape consumption/reauthentication, encode plus post-write
authentication, publication preauthentication, CAS install, hydration
verification, synchronization, and recovery reauthentication. Raw bytes,
calls, blocks, objects, and fsyncs reconcile exactly before ratios are derived.
Fixed-run merge bytes and calls come from the same instrumented readers and
writers: `merge_read_operations` and `merge_write_operations` count actual
non-empty storage submissions, while block counters remain a separate transfer
granularity metric. Shape-phase totals add the disjoint fixed-run and Parquet
byte/call counters exactly once.
Each phase declares whether it was applicable. Every ordinary lifecycle phase
must contain source-owned activity; a zero row is rejected. Recovery may be
non-applicable only for an uninterrupted run, while the deterministic durable
crash matrix separately proves nonzero recovery bytes and calls whenever an
interrupted intent is accepted.
The deterministic full-lifecycle 1x/2x/4x ladder executes source construction,
CSR, reopen/query, export/verify, clean import/reopen/query, corruption,
cancellation, resource-limit, and interrupted-finalization drills. It validates
the qualification phase inventory and bounds bytes, calls, objects, blocks, and
fsyncs per phase rather than relying only on aggregate I/O.
Node canonical cost uses reopened live nodes; edge canonical, authoritative
project, and lifecycle peak costs use reopened live edges. Ratios preserve raw
integer numerators and denominators; rounded decimals are not evidence.

Provider and qualification artifacts never expose generation UUIDs. Generation
agreement and source/import distinctness are checked while the generations are
lifetime-pinned, then emitted only as required-true authenticated proof fields.
Before either artifact is written, a recursive sanitizer rejects raw UUID
strings, absolute host paths, credentials, secrets, tokens, and provider
machine, volume, or resource identifiers. Storage snapshots likewise omit the
generation UUID and native file-identity map.

At least two ordered adjacent rungs are required. The S26 rate must be no lower
than both the newest observed peak ratio and every positive adjacent-rung slope.
The validator independently recomputes separate projected canonical-node and
canonical-edge allocation plus the lifecycle peak,
volume headroom, and the admit/refuse decision. A single successful rung is not
a projection, and insufficient reserved headroom always refuses SCALE-26.

Build and validate a companion document from two adjacent complete progressive
rung documents with:

```bash
make -C benchmarks progressive-storage-qualification \
  LOW_RUNG=build/s20-rung.json \
  HIGH_RUNG=build/s22-rung.json \
  EVIDENCE=build/g500-ladder-qualification.json \
  COMMIT=$(git rev-parse HEAD) \
  IMAGE_DIGEST=registry.fly.io/graphforge-bench@sha256:<64-hex-digest> \
  LOW_RESULT_SHA256=<independently-recorded-s20-result-sha256> \
  HIGH_RESULT_SHA256=<independently-recorded-s22-result-sha256> \
  VOLUME_BYTES=536870912000 \
  RESERVED_HEADROOM_BYTES=53687091200
```

## CI placement

Per the [Scale Evaluation](../reference/scale-evaluation.md) contract, large
Graph500 runs **must not** be wired into normal GitHub Actions. Only the S10 CI
rung runs in `test.yml`. SCALE-20+ / #745 certification must run on an
**approved dedicated Linux cloud evidence job** (or equivalent provisioned host)
tracked with #745 — not on developer laptops.

The target-live SCALE-26 lifecycle, protected dispatch policy, typed phase
journal, and sanitized evidence contract are defined in the
[billion-live-edge certification runbook](g500-certification.md). The ladder is
preflight evidence only; it cannot substitute for that exact-SHA lifecycle.
