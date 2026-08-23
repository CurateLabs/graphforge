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
| Engineering green (generate → ingest → reopen → GSI → `LIMIT 1000`) | Yes, on `GraphForge::publish_bulk_*` + `execute` |

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
count**. Live edges stream straight into `publish_bulk_edges` during the merge,
so ingest is bounded too.

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
phase (`generate`, `ingest`, `reopen`, `node_count`, `edge_count`, `one_hop`,
`two_hop`), compares peak RSS / disk /
elapsed time against the envelope. On the **first** violation it records the
failing phase and `error_class` (`oom` | `disk_exhaustion` | `timeout` | `execution_failure` |
`result_mismatch`) and
stops — no larger rung is attempted and no SCALE-26 pass is claimed.

> RSS fidelity: peak RSS is read from `/proc/self/status` `VmHWM` on Linux
> (a true high-water mark) and falls back to sampled `ps` RSS otherwise (an
> *instantaneous lower bound*, not a peak). Evidence records `rss_source`
> (`vmhwm` | `ps_sampled`) so a `ps_sampled` value is read as a floor. Run
> provisioned certification rungs on **Linux**.

> Ingest-phase attribution: `publish_bulk_nodes` / `publish_bulk_edges` retain
> request-sized normalization, identity, endpoint, writer, delta, and receipt
> state. Existing fixed-schema Parquet is copied through bounded 64K-row batches,
> and writer reopen reads only final row-group surrogate tails; neither operation
> materializes accumulated topology. While ingest runs, the atomic journal is
> refreshed every two seconds with the current subphase, edge-chunk index,
> anonymous/file RSS, disk usage, and aggregate topology rewrite counters. An
> `oom` with `first_failing_phase: "ingest"` therefore remains an upstream
> publication failure, not a generator-memory regression. Append-only linear-I/O
> construction is tracked separately by #901 and remains required before the
> billion-edge close gate.

### S20 Fly baseline and interpretation

The exact-merge `eccb6e06726d05cdef9e5242cad885be80565eee` S20 attempt ran on
a Fly performance Machine with 2 vCPUs, 4 GiB RAM, and an attached NVMe volume.
Ingest completed at approximately **688 MB peak RSS**. Reopen/recount later
reached approximately **3.33 GB RSS**, and the kernel killed the process during
fixed-hop query execution at approximately **3.80 GB anonymous RSS** (exit
137). The volume was only **52% used**. These observations diagnose a
GraphForge reopen/query execution-memory defect, not disk exhaustion, Fly page
cache, or bounded-generator growth. They are a historical failing baseline,
not a current pass or a universal memory requirement; S20 must be rerun at the
fix's exact merge SHA before this gate can be called green.

The journal now publishes a separate atomic `running` and completed/failed
boundary for node count, edge count, one-hop, and two-hop execution. Every
normal completion includes both high-water RSS and current Linux process-memory
components. A process-level OOM or `SIGKILL` cannot execute Rust cleanup code;
in that case the last durable `running` boundary identifies the interrupted
phase, while the external Machine event/exit status supplies the typed `oom`.
The journal deliberately leaves `error_class` null rather than inventing a
cause when no typed in-process failure was observed.

## Commands

Always-on CI (SCALE-10 smoke + all reconciliation / determinism / bounded /
first-fail unit tests):

```bash
cargo test -p graphforge-api --test scale_g500_ladder
```

Provisioned S20 full lifecycle (the work root must not already exist):

```bash
GF_G500_S20_WORK_ROOT=/mounted-work/s20 \
GF_G500_S20_EVIDENCE_OUT=/mounted-work/s20-evidence.json \
GF_G500_CERT_JOURNAL_OUT=/mounted-work/s20-journal.json \
GF_G500_S20_EXPECTED_SHA="$(git rev-parse HEAD)" \
make bench-g500-s20-lifecycle
```

This is distinct from the first-fail ladder entry. It uses the versioned S20
profile values and runs all 17 integrated lifecycle phases: source generation,
ingest, CSR, reopen and queries; portable export and full verification; import
into a previously absent destination; imported reopen and equivalent queries;
and the four bounded negative drills. Its evidence is not a pass unless every
phase is present and successful and the source/import fingerprints match.

The one-hop and two-hop observations are rooted at the deterministic published
node for Graph500 vertex 15, then apply global `ORDER BY ... LIMIT 1000` within
that neighborhood. Vertex 15 is present at every configured rung and gives the
pinned S10 seed a non-empty one-hop and two-hop probe without selecting the
generator's pathological highest-degree vertex. This is an ordinary
parameterized Rust-facade query, not a benchmark-only execution path. The root
bound is part of the workload contract: an unrooted two-hop TopK must enumerate
the complete graph's two-hop path result to preserve exact ordering, making
runtime proportional to path cardinality rather than providing a bounded
neighborhood traversal signal.

### Disposable Fly S20 controller

The checked-in Fly harness is
[`scripts/fly-g500-s20.py`](../../scripts/fly-g500-s20.py), with its immutable
runtime image under
[`containers/fly-g500-s20/`](../../containers/fly-g500-s20/). Build and push
the image for the final clean commit as Linux/amd64. A Fly registry namespace
requires its empty disposable app to exist before the push; execution accepts
only that exact empty app and owns its final destruction. Resolve the
**platform-child** manifest digest after pushing (an OCI index digest is
rejected):

```bash
SHA="$(git rev-parse HEAD)"
APP="gf-s20-${SHA%????????????????????????????????}"
flyctl apps create "$APP" --org personal
docker buildx build --platform linux/amd64 --provenance=false --push \
  -f containers/fly-g500-s20/Dockerfile \
  -t "registry.fly.io/${APP}:${SHA}" .
docker buildx imagetools inspect --raw "registry.fly.io/${APP}:${SHA}"
```

Run the controller without `--execute` against the resolved child digest before
execution. This creates no Machine or volume. It fetches the current official
Fly pricing page, extracts the fixed `dfw` performance-2x/4GB and volume rates,
and refuses a projected 4h30 maximum that exceeds the approved $10 ceiling. A
$1 reserve covers unpriced registry/rootfs/network variance. The controller
fixes 2 performance CPUs, 4096 MiB RAM, one 50 GB volume, no services, restart
`no`, auto-destroy, and a 16,200-second hard controller deadline.

```bash
python3 scripts/fly-g500-s20.py \
  --expected-sha "$SHA" \
  --image "registry.fly.io/${APP}@sha256:<linux-amd64-child>" \
  --org personal --app-name "$APP" \
  --machine-name "${APP}-machine" --volume-name gf_s20_volume
```

Only after inspecting that dry-run, add `--execute --confirm-disposable`. Live
execution additionally requires the exact clean checkout, re-resolves the child
manifest, keeps the Fly token only in process memory, retrieves and validates
the journal/evidence, acknowledges retrieval, and destroys and verifies absence
of the Machine, volume, and app in `finally`. Do not use pricing fixtures with
execution; `--pricing-html` and `--manifest-json` exist only for deterministic
dry-run tests.

During execution the controller prints sanitized JSON progress: every completed
phase, the next phase start, and a heartbeat once per minute. It also writes
each valid journal prefix to `--journal-out`, so an operator stop or timeout
does not discard completed evidence. Phase-aware operational ceilings stop a
stalled or pathologically broad phase with `phase_timeout` before it can consume
the entire 4h30 outer envelope; a journaled product failure stops immediately
with `phase_failed` and its recorded failure code. These ceilings only prevent
runaway spend and silence. They do not turn a partial lifecycle into a pass:
success still requires the exact 17-phase journal and equivalent source/import
evidence described above. The dry-run plan prints the complete timeout table.

Ingest and clean import each have a 90-minute ceiling. A Fly S20 ingest remained
responsive and made progress but crossed the former 60-minute boundary by 18
seconds, with no OOM or disk failure; the Machine, volume, and app then tore down
cleanly. The additional allowance accounts for empirically observed shared-host
storage-I/O variance. It does not weaken correctness: the 4h30 hard run limit,
all other phase ceilings, typed failure handling, exact phase sequence, and
source/import equivalence requirements remain unchanged.

```bash
python3 scripts/ci/test-fly-g500-s20.py
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
after every phase—including separate count and hop phases—and after every rung.
Retrieve the journal after an OOM,
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

The `reopen`, `node_count`, `edge_count`, `cypher_limit_1hop`, and
`cypher_limit_2hop` steps each carry `rss_peak_bytes` plus `process_memory`
(`vmrss_bytes`, `rss_anon_bytes`, and `rss_file_bytes` on Linux), rather than a
single aggregate query observation.

Each hop step also carries aggregate-only `operators` evidence. Expansion
records input batches/rows, generated candidates, emitted rows, selective
edge/node scan rows, and maximum concurrent reads per edge binding. Ordered
operators record the physical TopK row bound, output rows/batches, actual spill
count/bytes, and their post-execution memory gauge. Query-level DataFusion
memory-pool reservations are sampled before execution and after all operator
streams have been dropped; equality proves reservation quiescence. These are
engine metrics, not estimated heap sizes. Global `ORDER BY` remains a semantic
barrier: terminal cancellation is never pushed through the sort, while
`SortExec: TopK(fetch=N)` bounds retained sort state to the requested limit.

Wall-clock and RSS numbers are hardware-specific observations, never CI
millisecond gates. For #745, `sut` must name the cloud SKU; laptop SUTs are
rejected as certification evidence.

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
