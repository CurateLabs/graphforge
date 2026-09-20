# Ingest region diagnostics

Stock `gf --json import-session ...` receipts include `region_diagnostics`.
No test feature or custom engine build is needed. Rust callers can capture the
same tree with `graphforge_api::concurrency_attribution::RegionCapture::start`;
finish the capture after its nested guards have dropped. Captures are thread-bound,
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
  process CPU. The legacy process-global `snapshot()` also contains inclusive
  totals and must not be summed across overlapping phases.
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
`resume_import`, `register_parquet`, `validate`, `open_construction`, `seal`, and
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
receipt), `hydration` (materializing the published workspace) and
`read_authority` (runtime catalog, property inventory, ordinal handle and
adjacency provider). The residual of `commit/publish` is the visibility swap
plus uninstrumented time between those children. Adjacency CSR encoding runs
inside `validate/seal/canonical_encoding/adjacency_encoding`, not inside
publish; reconcile publication against that region rather than assuming CSR
cost lands in `commit`. Measured attribution on the integrated tree is recorded in
[`evidence/publication-attribution-1481.md`](evidence/publication-attribution-1481.md).

Registration reports successfully owned bytes (Arrow registration reports rows),
append reports accepted rows, and successful new shaping/encoding reports the
completed graph's node/edge counts. Reused artifacts do not claim new useful work.
Rates use that stage's own wall time. These are different populations; do not add
registration, append, shape, and encoding counts together.

A single receipt cannot establish speedup; its matched speedup is unavailable.
For two controlled worker-count runs, the comparison entrypoint is:

```bash
PYTHONPATH=benchmarks/harness python3 -m graphforge_bench.region_diagnostics \
  baseline.json candidate.json --scope import_command/validate/seal/shaping --unit edges
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
