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
count**. The required #932 implementation will decode fixed 1 MiB blocks into
Arrow batches of at most 65,536 rows and submit them to one storage-owned
construction session. That session must write immutable runs, perform one
bounded final merge and private seal, and publish `CURRENT` exactly once. The
current executable uses `EdgeSink` pair callbacks and repeated public
publication, advertises `legacy-repeated-publication/refused`, and is
intentionally refused before a Fly app, volume, or Machine can be created. It
is diagnostic code, not valid S20 evidence.

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

> RSS fidelity: provisioned evidence samples cgroup `memory.current` and
> `memory.peak` plus `/proc/self/smaps_rollup` RSS, anonymous, and file-backed
> values at every phase/window boundary. `VmHWM` is retained only as a run-level
> backstop; it is never reused as a phase peak. Lower rungs must demonstrate a
> phase-local plateau. Continued material RSS growth with edge count is an
> architecture failure, not permission to select a larger Machine.

> Current legacy ingest attribution: `publish_bulk_nodes` / `publish_bulk_edges`
> retain
> request-sized normalization, identity, endpoint, writer, delta, and receipt
> state. Existing fixed-schema Parquet is copied through bounded 64K-row batches,
> and writer reopen reads only final row-group surrogate tails; neither operation
> materializes accumulated topology. While ingest runs, the atomic journal is
> refreshed every two seconds with the current subphase, edge-chunk index,
> anonymous/file RSS, disk usage, and aggregate topology rewrite counters. An
> `oom` with `first_failing_phase: "ingest"` therefore remains an upstream
> publication failure, not a generator-memory regression. Append-only linear-I/O
> construction is tracked by #932 and remains required before the paid S20
> gate. These counters must never be relabeled as block/batch evidence.

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

This command currently builds the deliberately unadvertised legacy executable.
The dry-run controller must reject it because the Dockerfile has no S20 runtime,
measurement, construction, or revision contract labels. #932 owns adding those
labels only after its image build executes the real contract regression. Do not
add labels or synthesize positive counters to bypass admission.

Before the S20 dry run, produce its qualification artifact with
[`scripts/produce-fly-s20-qualification.py`](../../scripts/produce-fly-s20-qualification.py).
The producer accepts a smallest-first JSON list of candidate Machines; every
candidate includes `name`, `cpus`, `memory_mb`, and the conservative maximum
cost of one observation as `observation_max_usd`. Its executable adapter owns
one disposable full-lifecycle observation at a time and receives the requested
scale, exact source/image/platform/region/Machine/volume binding, and evidence
path through `GF_QUALIFICATION_*`. The adapter must return exact-bound but
untrusted `graphforge-fly-s20-qualification-observation/1` child evidence,
including its reported cost and proof that its Machine, volume, and app are
absent. The producer independently checks every binding and never treats stdout
as evidence.

The producer admits the exact clean source and immutable image contract before
calling the adapter, so the current unadvertised image invokes no adapter and
creates no remote resource. It runs adjacent S18 and S19 with identical budgets
on one candidate. Escalation is bounded and allowed only after a typed
`memory_headroom_exceeded` result with verified cleanup; arbitrary timeout,
CPU, disk, or I/O failure is an architectural/environmental failure rather than
a reason to buy more RAM. After a successful pair, measured RSS must prove that
every skipped smaller candidate lacked the required headroom. The observations
share a four-hour product deadline, with an idempotent cleanup action given at
most ten additional minutes after every attempt. Qualification and S20 share
the approved $10 ceiling, including a $1 reserve. Before every attempted rung,
the controller irrevocably accumulates that candidate's configured maximum
exposure; zero or understated child-reported cost cannot restore budget. The
final artifact binds the controller-owned rate snapshot, reservation and
completion timestamps, per-attempt reservations, reserved total, and separately
reported total. S20 compute, volume, reserve, and the qualification's reserved
exposure must fit the same cumulative $10 ceiling.

```bash
python3 scripts/produce-fly-s20-qualification.py \
  --expected-sha "$SHA" \
  --image "registry.fly.io/${APP}@sha256:<linux-amd64-child>" \
  --region dfw --volume-gb 100 \
  --candidates-json build/fly-machine-candidates.json \
  --observation-command scripts/private/run-disposable-qualification \
  --evidence-out build/s20-fly-qualification.json
```

The adapter and candidate file are operator inputs, not committed evidence or
credentials. The adapter must clean up independently on every pass, refusal,
timeout, and interruption; a missing cleanup proof stops escalation. The
producer converts handled `SIGHUP`/`SIGTERM`, `KeyboardInterrupt`, and
`SystemExit` into a bounded idempotent cleanup attempt before propagating the
termination. No userspace process can run cleanup after `SIGKILL`, kernel panic,
or host loss, so the adapter must also use deterministic disposable names that
the next operator invocation can discover and remove. The producer itself
contains no Fly create operation and cannot convert a failed admission into a
launch.

Then run the S20 controller without `--execute` against the resolved child
digest. This creates no Machine or volume. Supply the producer's sanitized
qualification artifact from the two lower rungs under identical budgets. It
binds the fixed region and immutable image digest, records
phase-local cgroup/smaps and raw block/batch/shard/topology-row I/O observations,
and contains an S20 physical-storage projection. Ingest and import counters must
be nonzero, syscall counts must remain sub-row, and block/batch/shard density
must remain linear across adjacent S18/S19 rows. These gates run before
`execute()` and therefore before paid resources. The checked-in
[`fly-s20-qualification.schema.json`](fly-s20-qualification.schema.json)
defines that input. The controller refuses an RSS growth curve that does not
plateau, chooses the smallest listed performance Machine with both 25% and 512
MiB RSS headroom, and requires the exact qualification-bound volume size to
contain the projected physical peak with 25% headroom. It uses that same size
for S20 rather than relabeling the observation onto a different volume. 128 GiB
and Fly's 500 GB volume limit are refusal ceilings, not defaults.

The final S20 evidence must repeat the three computed gates as
`rss_plateau=pass`, `disk_headroom=pass`, and `construction_io=pass`; the
controller recomputes the S20 phase consistency and refuses missing or
hand-declared substitutes. The current Rust executable emits neither this gate
set nor the required construction contract, which is another intentional
prelaunch incompatibility until #932 wires the actual storage counters.

The current official rate for the selected Machine and derived volume is priced
for the four-hour product clock plus bounded cleanup. The total, including a $1
unpriced reserve, must remain at or below the approved $10 ceiling. The Machine
has no service, restart policy `no`, auto-destroy enabled, one attached volume,
and the recorded fixed region/digest. The historical 2-vCPU/4-GiB/50-GB run is
useful qualification input, never a universal resource requirement.

```bash
python3 scripts/fly-g500-s20.py \
  --expected-sha "$SHA" \
  --image "registry.fly.io/${APP}@sha256:<linux-amd64-child>" \
  --org personal --app-name "$APP" \
  --machine-name "${APP}-machine" --volume-name gf_s20_volume \
  --qualification-evidence build/s20-fly-qualification.json
```

Only after inspecting that dry-run, add `--execute --confirm-disposable`. Live
execution additionally requires the exact clean checkout, re-resolves the child
manifest, keeps the Fly token only in process memory, retrieves and validates
the journal/evidence, acknowledges retrieval, and destroys and verifies absence
of the Machine, volume, and app in `finally`. Do not use pricing fixtures with
execution; `--pricing-html` and `--manifest-json` exist only for deterministic
dry-run tests.

Before any Fly resource is created, the controller reads the pinned child image
configuration and requires Linux/AMD64, the exact source revision, the
phase-measurement contract, and the storage-owned construction-session contract.
The current legacy `EdgeSink`/repeated-publication executable deliberately does
not advertise that contract and is refused before paid launch. A Docker label is
evidence only when the image build itself runs the contract regression; it must
never be added merely to bypass this admission gate.

Admission and the full lifecycle share one 14,400-second product clock; the
S20 subprocess receives only the time remaining after admission. During
execution the controller prints sanitized JSON progress: every completed
phase, the next phase start, and a heartbeat once per minute. It also writes
each valid journal prefix to `--journal-out`, so an operator stop or timeout
does not discard completed evidence. Phase-aware operational ceilings stop a
stalled or pathologically broad phase with `phase_timeout` before it can consume
the entire four-hour outer envelope; a journaled product failure stops
immediately with `phase_failed` and its recorded failure code. These ceilings
only prevent runaway spend and silence. They do not turn a partial lifecycle into a pass:
success still requires the exact 17-phase journal and equivalent source/import
evidence described above. The dry-run plan prints the complete timeout table.

The retrieved final artifact is written to `--evidence-out` **before** pass
validation. Consequently, an incomplete or non-pass artifact remains available
after the disposable app is destroyed instead of being lost with its volume.
All locally preserved JSON is bounded and redacts credential-shaped keys. On a
controller failure, `--diagnostic-out` (default `s20-diagnostic.json`) also
records the controller error plus an allowlisted Machine state and at most 20
exit/status events captured before teardown. Raw Fly logs, environment values,
network addresses, and unrecognized event fields are never retained.

Phase timeouts are safety stops, not performance allowances. Ingest/import
limits must be interpreted with observed block throughput and storage-owned
counters; they do not rationalize record-at-a-time or repeated-publication
work. Teardown independently attempts the exact Machine, volume, and app under
one ten-minute deadline and reports every unresolved ID.

```bash
python3 scripts/ci/test-produce-fly-s20-qualification.py
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
  `wall_time_s`, and per-phase/window boundary observations.
- `machine_envelope` (128 GiB / 1 TiB / 4 h fail-safe), `sut`, `generator`.
- `track` and `teps` are always `null`.

Every phase/window carries cgroup current-before/peak/current-after; smaps RSS,
anonymous, and file values before/after; `/proc/self/io` byte and syscall
deltas; storage sequential bytes, 1 MiB blocks, Arrow batches and maximum batch
rows, shards/row groups, random seeks, fsyncs, and construction/index counters.
Filesystem boundaries use `statvfs` capacity/free/available values and explicit
allocated project/spill/package/import counters. A recursive project-tree `du`
watchdog is prohibited because its work scales with shard count and perturbs
the measured I/O.

The final evidence binds the actual selected Machine, CPUs, memory, volume,
region, immutable child digest, no-service/restart settings, passing disk and
RSS-plateau gates, and exactly one `CURRENT` transition for source construction
and one for clean import. It must prove source export, full verification, import
into an absent destination, imported reopen, counts/queries, and equivalent
project/authority fingerprints. Partial evidence remains a diagnosis, not a
pass.

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
