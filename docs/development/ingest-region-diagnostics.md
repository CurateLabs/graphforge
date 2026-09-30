# Ingest region diagnostics

Stock `gf --json --diagnostics import-session ...` receipts include
`region_diagnostics`. Ordinary commands leave optional lifecycle I/O, storage
read counters, construction diagnostics, and region timing disabled; an absent
observation is unavailable, never a measured zero. The explicit
`storage-attribution` command and `--allocation-diagnostics` also request their
collectors. Required receipt identities, allocation accounting, recovery
authority, and durability barriers run independently of these observers.
Import operation timings are also optional: their getter returns `None` without
capture and commands omit them or return `null`. Required progress elapsed time
and cancellation checks retain their normal clocks. Custom property-scan
execution metrics are collected only when requested; an explicit execution-demand
capture enables them while resource reservations and decoder limits remain active
for every query.

No test feature or custom engine build is needed. Rust callers request lifecycle
and storage read measurements with `graphforge_api::LifecycleIoCapture::install()`.
Keep that guard alive around the operation, and read
`graphforge_api::lifecycle_io_snapshot()` before dropping it. The snapshot is
`None` without a requested, valid capture. Each operation owns its counters;
worker jobs and deferred readers carry the originating capture and nested guards
restore the previous one. Region timing has its own explicit
`graphforge_api::concurrency_attribution::RegionCapture::start` guard;
finish it after its nested guards have dropped. Captures are thread-bound,
non-durable, and contain static region names and counters, never source paths or
row payloads. A later status call measures that call; it does not replay prior work.

Each region reports `inclusive` measurements and a `residual` after subtracting
its immediate children. Sum residual **wall** times to reconcile with the root.
Do not sum inclusive rows. Parent and child regions deliberately overlap.
Misordered guards invalidate the capture (`complete: false`, no rows), rather
than returning plausible measurements assigned to the wrong stage. There are at
most 256 distinct paths and 16 nesting levels; exceeding either invalidates it.

## Units and boundaries

- `process_cpu_ns / wall_ns` is effective **cores**, not CPU-busy fraction,
  throughput speedup, or serialized fraction. It is the time-weighted amount of
  process CPU execution, at Linux `/proc`'s 10 ms CPU resolution. It includes all
  process threads, including unrelated embedding-host work. Captured region
  paths describe the calling thread; worker scopes are not assigned duplicate
  process CPU. The explicitly captured phase `snapshot()` also contains
  inclusive totals and must not be summed across overlapping phases.
- `thread_running_ns` measures calling-thread execution; `thread_runnable_ns`
  measures time waiting to run. They do not measure pool occupancy.
- `thread_sleeping_ns` measures completed non-runnable sleep, including
  uninterruptible blocking. `thread_uninterruptible_ns` is a **subset**;
  `thread_iowait_ns` is a further I/O-wait subset. Never add these three values.
  None of them measures whole-process inactivity or PSI `some`.
- `fsync` and `lock_wait` paths identify observed directory/membership-sync and
  blocking file-lock calls. Their scheduler counters separate running, runnable
  delay, and sleeping inside that call. They do not classify all process I/O:
  automatic cache-writer barriers, worker-thread syscalls, mutexes, and other
  uninstrumented operations remain in their enclosing region/residual.
- `thread_unknown_ns` is elapsed time remaining after available running,
  runnable, and sleeping observations. Missing counters stay `null`, and their
  time remains unknown. A negative counter difference or inconsistent boundary
  sample yields `null`, not a clamped claim of zero. Sampling is sequential:
  `sampling_uncertainty_ns` bounds the read windows, in addition to CPU tick
  quantization. Residual uncertainty adds parent and child windows.

Scheduler delay and sleep counters require Linux scheduler statistics enabled
by the operator (`kernel.sched_schedstats=1`). The library never changes this
setting. Linux kernels that lack a counter, or platforms without these proc
interfaces, report that counter unavailable. The sleep/block accounting and
units follow [Linux scheduler accounting](https://github.com/torvalds/linux/blob/master/kernel/sched/stats.c)
and [scheduler debug output](https://github.com/torvalds/linux/blob/master/kernel/sched/debug.c).

## Complete ingest and useful work

The certification runner adds `graphforge-workflow-timing/1` around the complete
five-command ingest workflow: begin, both source registrations, validate, commit.
It includes child startup, facade open, command handling, and command cleanup.
It reports runner CPU and reaped child CPU separately from the same workflow
boundary. These counters are shared with any unrelated work in that runner, so
use a dedicated runner process. The outer lifecycle storage observation happens
after this boundary, matching existing phase-wall semantics.

`lifecycle_runtime` reports the workflow CPU/wall, disjoint command-root totals,
and their wall/CPU residuals. The residual includes work outside the import
command handler, such as startup and facade opening. CPU residuals are signed
because separate proc samples can differ by a tick. Within a command,
`resume_import`, `register_parquet`, `stage+seal`, `open_construction`, `seal`, and
`commit/publish` retain their own explicit residuals. Existing operation timings
remain the five disjoint construction-call observations; their boundaries are
narrower than command scopes.

`commit/publish` is decomposed into sequential children so publication can be
budgeted inside complete ingest (#1481): `prepare_encoding` (reopening the
encoded inventory and reclaiming superseded payloads), `publication_authentication`
(inventory control, parent generation, manifest, retained-artifact and route
authority checks), `cas_install` (appending authenticated graph objects to the
content-addressed store), `publication_intent` (the durable intent record),
`generation_commit` (staging the generation and committing `CURRENT`),
`publication_receipt` (authenticating the published target and recording the
receipt), `hydration` (materializing the reader workspace) and
`read_authority` (runtime catalog, property inventory, ordinal handle and
adjacency provider). The residual of `commit/publish` is the visibility swap
plus uninstrumented time between those children. Boundaries to keep in mind
when reading the numbers: `cas_install` opens before the route-table authority
read (a few kilobytes) that the append needs, so that read counts as install;
an idempotent replay of an already-published session skips publication and
reports `hydration` and `read_authority` as siblings of `publication_receipt`.
For a fresh publication, reader preparation runs against the durable candidate
inside `generation_commit`, immediately before `CURRENT`: the receipts nest
`hydration` and `read_authority` under `generation_commit`, and a preparation
failure leaves `CURRENT` unchanged (fail-closed) instead of committing a
generation that cannot be hydrated. Candidate verification before that callback
(the durable manifest and lease authentication) remains in `generation_commit`'s
residual. Adjacency CSR encoding runs
inside `stage+seal/seal/canonical_encoding/adjacency_encoding`, not inside
publish; reconcile publication against that region rather than assuming CSR
cost lands in `commit`. Measured attribution on the integrated tree is recorded in
[`evidence/publication-attribution-1481.md`](https://github.com/CurateLabs/graphforge/blob/29a7b34ebe441a85ffb9274164d58aaeeb68dc8a/docs/development/evidence/publication-attribution-1481.md).

Registration reports successfully owned bytes (Arrow registration reports rows),
append reports accepted rows, and successful new shaping/encoding reports the
completed graph's node/edge counts. Reused artifacts do not claim new useful work.
Rates use that stage's own wall time. These are different populations; do not add
registration, append, shape, and encoding counts together.

A single receipt cannot establish speedup; its matched speedup is unavailable.
For two controlled worker-count runs, the comparison entrypoint is:

```bash
PYTHONPATH=benchmarks/harness python3 -m graphforge_bench.region_diagnostics \
  baseline.json candidate.json --scope import_command/stage+seal/seal/shaping --unit edges
```

Each file wraps the unmodified receipt as `receipt` plus `provenance` containing
`build`, `input`, `host`, `cache`, `resource_policy`, `workers`, and `processes`.
The experiment must provide truthful identities and configured worker counts;
the tool does not infer them from CPU use or pool capacity. It requires identical
provenance and useful work, one process in both runs, and one versus N workers.
It reports wall speedup separately from each run's CPU/wall. Stock construction
currently has no public per-stage worker override; use the existing controlled
worker experiment harness when supplying such pairs. N independent processes
cannot satisfy this comparison.

## Calibration

Deterministic tests cover nesting, guard misuse, unavailable counters, old/new
kernel field names, units, and exact residual reconciliation. Timing calibration
runs separately on a quiet host so competing tests cannot supply process CPU:

```bash
cargo build -p graphforge-storage --release --example region_controls
# With scheduler statistics enabled by the operator; select one allowed CPU:
taskset -c CPU target/release/examples/region_controls
```

The example asserts approximately one effective core for CPU work, at least
150 ms of observed sleep in a known 200 ms delay, and over 100 ms of runnable
scheduler delay for two CPU-bound threads pinned to one CPU. It fails when the
required observation is unavailable; it never substitutes a slow fsync for the
fixed-delay control. Restore the host's original scheduler-statistics setting
after an experiment.

The stock release example passed on 2026-09-19 with one allowed CPU selected
and scheduler statistics enabled for the experiment (then restored):

| Control | Wall | Process CPU/wall | Calling-thread observation |
| --- | ---: | ---: | --- |
| CPU loop | 400.17 ms | 0.950 cores | 384.48 ms running |
| Known 200 ms wait | 200.48 ms | 0.000 cores | 200.05 ms sleeping |
| Two busy threads on one CPU | 400.18 ms | 0.975 cores | 203.82 ms runnable delay |

These are calibration observations, not ingest performance thresholds. In the
last row process CPU includes both threads; the scheduler row observes only the
capturing thread.

## Written and hashed bytes and barriers

Stock `gf --json --diagnostics import-session validate --session-uuid UUID` stages and seals
sources. Its outcome and measured region are `stage+seal`; `validate` remains
the CLI command. `status` uses the same outcome after sealing. The persisted
`ImportPhase::Validated` state and its existing binding labels remain readable;
they describe the durable lifecycle state rather than the measured phase.

New receipts use `graphforge-region-diagnostics/2`. The certification reader
and schema also accept `/1` because the retained encoding-lane receipt fixture
and historical ladder receipts are consumers of that contract. Old receipts
keep their original names and have no invented byte or barrier observations.

Every region's `inclusive` and `residual` measurements include:

- `written_bytes`: Linux `/proc/self/io` `wchar` differences, bytes accepted by
  write syscalls on all process threads. This includes control and payload
  writes and any other writes in the process, including pipes. It is neither
  disk writeback nor the logical length of newly published objects. It is
  unavailable (`null`) when the kernel counter cannot be read.
- `hashed_bytes`: input bytes supplied to instrumented SHA-256 sites in storage,
  API, core canonical identities, and ontology compilation/composition.
  Repeated input counts repeatedly; finalization padding does not count as
  payload. The ordinary digest implementation and digest bytes are unchanged.
- `hash_elapsed_ns`: the sum of SHA update/finalize elapsed intervals across
  process threads, excluding counter updates. Concurrent intervals overlap;
  this is accumulated hashing effort, not a disjoint critical-path region or
  process CPU time. Instrumentation has overhead, so it is not a speedup proof.
- `fsync_calls` and `fsync_elapsed_ns`: attempted file/data/directory barriers
  and their accumulated elapsed intervals, including failed calls. They cover
  observed storage/API file barriers, stable-directory barriers and cache-writer
  barriers. They are not inferred from the number of named `fsync` scopes.

Counters activate only during explicit region capture and never reset another
capture's totals. Region boundary differences include workers even though
worker region trees are not captured. Like process CPU, these totals require
one purpose per process: simultaneous unrelated tasks contaminate attribution.
The `io_scope` field states their shared process scope. Sum disjoint residuals,
not inclusive parents and children; unknown or inconsistent differences stay
`null`. Useful-work counters remain a separate population.

`source_read` covers iterator decode/canonicalization calls; `normalization`
covers bounded normalization windows. Appends are split into `append_nodes`
and `append_edges`; `manifest_persistence` covers each full manifest rewrite
including its barrier and install. Seal, encoding, hydration and commit retain
their existing boundaries. Reader setup and uninstrumented work stay in the
reported residual; do not call the entire residual hashing or source reading.

The baseline S22 input identities are `nodes.parquet`
(`bcbcbea526e61ceb63f6006ee5f56de6bb4f74cffdd68dc6eff6d230d3897f06`)
and `edges.parquet`
(`1c0ff75485f75e904cbd59b6f5d42da1d8b1af6ddac59ee6c495a4948a428d13`).
They represent 4,194,304 input nodes and 67,108,864 input edges.

For a fresh successful S22 construction, independently reconcile the leaf
`import_command/stage+seal/seal/shaping/shape_routing/artifact_authentication`
against the retained `receipt-NNNNNNNNNNNNNNNNNNNN.json` chunk receipts under
`.graphforge-construction/`. Require contiguous accepted sequences starting at
zero and reconcile receipt rows by kind with the input node/edge counts. That
leaf authenticates each chunk's Parquet artifact once: its `hashed_bytes` must
equal the sum of `parquet.bytes`, with zero-byte tolerance, and its call count
must equal the number of accepted chunk receipts. Normal publication retains
these small receipts, so collect them after measurement. Original source file
sizes and aggregate application reads are different populations; the latter
also includes metadata reads outside this leaf.

To reproduce S22, use those digest-pinned inputs and the baseline host. Build an ordinary release CLI with an isolated
`CARGO_TARGET_DIR`, record the source revision and binary/input SHA-256 digests,
then finish all builds before measuring. Set `TMPDIR` and the new project to
native storage on the process-root volume. Check for compiler/benchmark
processes and low load, establish a quiet window, and sample process names/load
throughout the five-command begin/register-nodes/register-edges/validate/commit
workflow under `runexec --no-container --cores 0-15` on the baseline host.
A contended run cannot establish the priority decision.

Keep each command's JSON receipt and the runexec output outside `docs/`; attach
them to #1623 or its PR. The snapshot lives at `receipt.region_diagnostics`.
Report wall/CPU, CPU divided by wall, written/hashed bytes and barrier counts
per region, along with the parent-minus-immediate-children residual. Budget
shares use the S22 edge count divided by 1,000,000 edges/s. Compare accumulated
hash/barrier effort with the disjoint construction regions and disclose its
scope when recording #735's conditional priority decision. Reopen and recount
the published project before claiming a successful ingest.

The deterministic worker-thread counter and residual regression runs with:

```bash
cargo test -p graphforge-storage --test region_work
cargo test -p graphforge-cli --test portable import_operation_timings_survive_separate_cli_processes
```
