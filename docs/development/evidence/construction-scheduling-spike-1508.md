# Construction scheduling and cancellation spike (#1508)

**Status:** runnable experiment, correctness evidence, and repeated measurements
for the scheduling mechanism. Follows
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md)
§3.4 and §5. This is evidence for the #1509 ADR, not a migration decision.
**Baseline revision:** `82a30cb6` (`origin/main` after #1564, #1506 and
#1507 landed; the branch was created at `3b514cd8`).
**Pins:** tokio 1.53.1, datafusion 54.1.0, rayon 1.12.0 (rayon-core 1.13.0).

## Predeclared experiment (before measurement)

| Item | Choice |
| --- | --- |
| Mechanism | Scheduling and cancellation of finish-time fixed-width partition loads (`partition_load::consume_in_partition_order`, the only multi-threaded construction scheduler; protocol §3.4) |
| Contract every candidate must keep | Loads off the coordinator; `consume` on the calling thread in partition index order, no `Send` required; at most `workers` partitions materialized (in flight + ready + being consumed); first error stops dispatch; return only after every started load has returned (joined) |
| Candidates | `rayon` (`ThreadPool::in_place_scope`, per-call or shared pool); `tokio-blocking` (`spawn_blocking`, per-call current-thread runtime with `max_blocking_threads(workers)`, or a shared multi-thread runtime); `datafusion-spawned` (`datafusion::common::runtime::SpawnedTask::spawn_blocking`); `tokio-inline` (loads as futures under `buffered(workers)`, no spawn: negative control); DataFusion operators `CoalescePartitionsExec` and `SortPreservingMergeExec` as probes |
| Control | Production pool (`baseline`), same call site, same bytes |
| Selection | `FixedRangePartitioner::with_load_scheduler`, `cfg(test)` only; `rayon` is a storage dev-dependency only. Production builds are unchanged. |
| Correctness gates | Byte-identical output and identical relabelled evidence to production at 1/2/3 workers; window bound held; cancellation and errors at distinct stages leave nothing running and nothing published; same leftover set as production after a cancelled real finish |
| Measurements | Wall, process CPU, effective cores, peak concurrent loads, distinct load threads; ≥5 rotated repetitions per candidate; median and range; no threshold invented |
| Out of scope | Changing the production default; the serial row-partition path; complete-ingest S18/S20 through publication (the #1509 integrated experiment); any claim about #1387 |

## What was built

- `crates/graphforge-storage/src/graph_construction/partition_load/scheduling_spike.rs`:
  the four candidate adapters behind one `run(scheduler, …)`.
- `…/scheduling_spike/tests.rs`: contract, cancellation, error, panic, shutdown and
  nested-runtime tests per candidate; library-behaviour tests; DataFusion
  operator probes; the real `FixedRangePartitioner` finish under every
  candidate; and the `#[ignore]`d measurement report.
- `partition_shaping.rs`: the load closure moved into
  `FixedRangePartitioner::schedule_loads` (production path unchanged) with a
  `cfg(test)` branch that dispatches to a forced spike scheduler. The #1507
  external-spill selection sits inside that one load body, so every
  scheduler loads a partition the same way.

### How to run

```bash
# Correctness (debug; TMPDIR must be on an admitted filesystem for real_finish)
TMPDIR=<ext4 dir> cargo test -p graphforge-storage --lib partition_load

# Measurements: alone, release, quiet host
TMPDIR=<ext4 dir> cargo test --release -p graphforge-storage --lib \
  scheduling_spike::tests::report -- --ignored --nocapture --test-threads=1
```

Knobs: `GF_1508_REPETITIONS` (5), `GF_1508_SORT_ELEMENTS` (2^19),
`GF_1508_RECORDS` (2^22), `GF_1508_PARTITIONS` (64).

## Findings verified from code and tests

Each row cites the test that demonstrates it. "All" means every candidate in
the row's test, including the baseline.

| # | Finding | Evidence |
| --- | --- | --- |
| F1 | All four off-coordinator schedulers keep index-ordered consumption and hold the materialization window exactly at `min(workers, partitions)` under a slow head partition. | `every_parallel_candidate_consumes_in_order_and_holds_the_window` |
| F2 | Rayon, `spawn_blocking` and `SpawnedTask` run loads concurrently on non-coordinator threads (observed, not inferred from the API). | `parallel_candidates_overlap_loads_on_distinct_threads` |
| F3 | An async API is not parallelism: `buffered(n)` over load futures on the coordinator runs one load at a time, all on the calling thread, while still materializing up to `n` results. | `inline_async_loads_run_one_at_a_time_on_the_coordinator` |
| F4 | The real finish publishes byte-identical output with identical relabelled evidence under every candidate at 1, 2 and 3 workers. | `real_finish::every_candidate_publishes_identical_bytes_and_evidence` |
| F5 | A cancelled real finish publishes no output and no output temporary under every candidate, and leaves exactly the production leftover set (sealed, receipted partition spills owned by recovery). | `real_finish::cancelling_a_real_finish_publishes_nothing_under_every_candidate` |
| F6 | Cancellation at three stages (before the first consume, while the head load runs, mid-consume), a load error, and a consume error with loads in flight all return the right error only after every load has exited, with nothing left materialized. | `cancellation_*`, `load_error_surfaces_at_its_partition_and_stops_dispatch`, `consume_error_returns_only_after_in_flight_loads_exit` |
| F7 | **Dropping or aborting a started Tokio blocking task does not stop it**, and DataFusion's abort-on-drop `SpawnedTask` does not either (its docs: aborted "only if it hasn't started yet"). A Tokio/DataFusion scheduler that returns on error without joining lets loads keep reading spills the caller is about to delete. The adapters must drain every handle; neither library does it for them. Rayon's `in_place_scope` joins by construction. | `dropping_or_aborting_a_started_blocking_task_does_not_stop_it`; drain in `scheduling_spike::drive` |
| F8 | Tokio and DataFusion tasks are `'static`. The production load borrows the session directory and job list, so the adapter must duplicate the directory descriptor (`ConstructionDirectory::try_clone`) and copy the job list into an `Arc` closure. Rayon's scope borrows like `std::thread::scope`. | `FixedRangePartitioner::schedule_loads` |
| F9 | Blocking on a runtime from inside a runtime panics (`Cannot start a runtime from within a runtime`). A Tokio-driven scheduler inside storage must refuse or hop threads when its caller is already async (the API's `GraphForge::block_on` hops to a scoped thread for this reason). Thread-based schedulers do not care. | `blocking_on_a_runtime_from_inside_a_runtime_panics`, `tokio_candidates_refuse_to_nest_and_thread_candidates_do_not_care`; `graphforge-api/src/runtime_ownership.rs` |
| F10 | A panicking load becomes a structured `partition load panicked` error under Tokio and DataFusion (`JoinError::is_panic`) and under Rayon only with an explicit `catch_unwind` in the adapter; without it, Rayon resumes the panic at scope end, after the coordinator has waited for a result that never arrives. | `a_panicking_load_is_a_structured_error_under_every_parallel_candidate` (the production pool is included since #1564) |
| F11 | **Production defect found and fixed separately (#1564, PR #1565):** before the fix, a load that panicked while another worker was alive hung `consume_in_partition_order` forever. The panicking worker's `LiveWorker` guard decremented `live_workers` but neither set `stop` nor delivered its index. The coordinator waited for that index while `live_workers > 0`, and the surviving worker waited for window capacity. A panicking `consume` hung too, because `stop` was only set on the coordinator's normal exit. Reproduced during this spike: no return within 5 s. The fix is the same containment F10 needs for Rayon (`catch_unwind` per load, plus a drop guard that sets `stop`), so it does not favour any library. This branch includes the fix, and the report's `load_panic` section shows the production pool returning a structured error. | #1564 regression tests in `partition_load/tests.rs`; report `load_panic` |
| F12 | Shared admission across concurrent imports: two concurrent imports on the production pool run up to 4 loads (2 private threads each). A shared Rayon pool of 2, or a shared Tokio runtime with `max_blocking_threads(2)`, bounds both imports together at 2. | `a_shared_runtime_or_pool_bounds_loads_across_concurrent_imports`; report `concurrent_imports` |
| F13 | The API's instance runtime (`resource_policy::build_tokio_runtime`) sets `worker_threads` but leaves `max_blocking_threads` at Tokio's default of 512, and query operators (for example `PropertyOverlayExec` decode) already use `spawn_blocking` on it. Construction loads spawned there get **no** CPU admission, and capping that pool to admit construction would also cap query decode. Shared admission through Tokio would need a dedicated blocking budget, for example a semaphore, not the existing runtime. The instance's Rayon `ComputePool` (`graphforge-exec/src/compute_pool.rs`, sized by `compute_threads`, `catch_unwind` around `install`) is the existing process-level CPU budget. | code references |
| F14 | DataFusion's operator scheduling cannot host the ordered bounded pool. `CoalescePartitionsExec` starts every input partition at `execute()` into a channel sized to the input partition count, before anything is consumed: no window below the partition count, and completion (not index) order. `SortPreservingMergeExec` restores order but materializes every input before its first row. Its inputs run on spawned tasks only on a multi-thread runtime (`spawn_buffered` checks the runtime flavor); on a current-thread runtime they are polled inline on the caller. DataFusion's scheduling facilities below the operators are Tokio `JoinSet`/`SpawnedTask`/mpsc channels, which the `datafusion-spawned` candidate exercises directly. | `datafusion_operators::coalesce_partitions_starts_every_input_without_a_window`, `datafusion_operators::sort_preserving_merge_materializes_every_input_and_parallelizes_by_runtime_flavor`; `datafusion-physical-plan-54.1.0/src/{coalesce_partitions.rs,common.rs,stream.rs}` |
| F15 | Cancellation latency is set by the coordinator's wait policy, not by the library. Production polls the caller's callback only in `consume`, so a cancel that arrives while the head partition loads is seen only after that load finishes (the workers' `stop` flag is set only when the coordinator stops). Every candidate that polls while waiting (Rayon via `recv_timeout`, Tokio/DataFusion via `select!` on a 1 ms timer) returns after roughly one stop-poll interval of the in-flight loads. A `Condvar::wait_timeout` poll in the production pool would do the same. | report `cancel_latency`; `cancellation_while_the_head_load_runs_returns_only_after_it_exits` |

## Measurements

One run of the report above, alone on a quiet host (1-minute load 0.42 at
start; no other build, ingest or test process), 2026-09-23 21:43–21:47Z.
Host: AMD Ryzen 7 3800X, 16 logical CPUs, `powersave` governor, Linux
7.0.0-30, `TMPDIR` on ext4. Release build, `debug_assertions` off, default
knobs (5 repetitions, candidate order rotated each repetition). Measured code
is this change's Rust source on `origin/main` `82a30cb6` (built before the
squash as local commit `bba0f7f7`); only this document, its data file and the
Bazel lock and drift metadata changed afterwards. Raw observations, one JSON object per line:
[`construction-scheduling-spike-1508.jsonl`](construction-scheduling-spike-1508.jsonl)
(sha256 `963ceec032c35bee6483a429d8aaf643489e0ee0224d53bbd346116e89eae1f5`).

Cells are median (min–max) over 5 repetitions. Effective cores is process
CPU time over wall time for the measured region (`concurrency_attribution`).
CPU time has 10 ms granularity.

**1. CPU-bound loads** (32 partitions, each a sort of 2^19 generated `u64`
values; `consume` does nothing). Every run produced checksum
`0000963f9dfa6f34`.

| Workers | Candidate | Wall ms | CPU ms | Effective cores | Peak concurrent loads | Distinct load threads |
| --- | --- | --- | --- | --- | --- | --- |
| 2 | baseline | 158.7 (154.0–162.3) | 310 (300–320) | 1.93 (1.91–2.02) | 2 | 2 |
| 2 | rayon | 154.1 (152.2–170.2) | 310 (310–310) | 2.01 (1.82–2.04) | 2 | 2 |
| 2 | tokio-blocking | 152.9 (152.8–159.7) | 300 (300–310) | 1.96 (1.94–1.96) | 2 | 2 |
| 2 | datafusion-spawned | 153.6 (152.6–161.1) | 310 (300–320) | 1.99 (1.93–2.03) | 2 | 2 |
| 2 | tokio-inline | 302.6 (290.0–306.0) | 300 (290–310) | 1.01 (0.99–1.02) | 1 | 1 |
| 4 | baseline | 88.1 (82.7–90.1) | 330 (310–340) | 3.75 (3.66–3.88) | 4 | 4 |
| 4 | rayon | 88.7 (79.3–89.2) | 340 (310–340) | 3.83 (3.81–3.91) | 4 | 4 |
| 4 | tokio-blocking | 89.3 (79.7–92.0) | 330 (310–330) | 3.69 (3.59–3.89) | 4 | 4 |
| 4 | datafusion-spawned | 89.3 (86.1–93.6) | 330 (320–340) | 3.69 (3.63–3.72) | 4 | 4 |
| 4 | tokio-inline | 281.1 (279.4–305.3) | 280 (280–300) | 1.00 (0.98–1.00) | 1 | 1 |

The four off-coordinator schedulers deliver the same on-CPU parallelism,
close to `workers`, with overlapping ranges. The inline async control stays at
one core regardless of `buffered(n)` (F3).

**2. Real fixed-partition finish** (`FixedRangePartitioner` over 2^22 routed
identity records, 64 requested partitions; the measured region is the whole
finish, including the coordinator's ordered writes). Every run published the
same output, sha256 `13eb02e16e5b3e995f77399ac25fc0f0d8e7c34c8576bba7f0b316dfe73dd767`.

| Workers | Candidate | Wall ms | CPU ms | Effective cores |
| --- | --- | --- | --- | --- |
| 2 | baseline | 411.8 (403.1–420.4) | 410 (390–430) | 1.02 (0.95–1.02) |
| 2 | rayon | 410.2 (402.7–416.6) | 410 (380–420) | 0.98 (0.94–1.01) |
| 2 | tokio-blocking | 409.4 (405.1–411.9) | 400 (400–420) | 0.99 (0.98–1.02) |
| 2 | datafusion-spawned | 421.0 (404.7–428.7) | 410 (400–420) | 0.98 (0.95–1.01) |
| 2 | tokio-inline | 560.7 (543.0–577.2) | 370 (360–380) | 0.66 (0.66–0.66) |
| 4 | baseline | 301.5 (295.3–342.0) | 410 (400–430) | 1.33 (1.26–1.38) |
| 4 | rayon | 302.2 (288.8–323.5) | 410 (400–440) | 1.36 (1.33–1.42) |
| 4 | tokio-blocking | 316.1 (300.9–318.5) | 410 (410–420) | 1.33 (1.29–1.36) |
| 4 | datafusion-spawned | 311.4 (290.0–320.9) | 410 (390–420) | 1.32 (1.22–1.45) |
| 4 | tokio-inline | 534.1 (499.8–552.7) | 380 (350–390) | 0.70 (0.69–0.72) |

At this size the finish is bound by the serial ordered consume, not by loads:
about one effective core at 2 workers and 1.3 at 4 for every off-coordinator
scheduler, with overlapping ranges. The scheduler library does not change
the real finish here. Only the inline control, which serializes loads with
the consume, is slower (about 1.4–1.8x). Larger partitions or a cheaper
consume would shift the balance; this run does not measure that.

**3. Cancellation latency while the head load runs** (8 partitions, 2
workers; the head load spins for up to 500 ms, checking its `stop` flag on
every iteration; cancel arrives 50 ms after the head load starts; latency is from
the cancel to the return). Every run returned `construction cancelled` with
every load joined.

| Candidate | Latency ms |
| --- | --- |
| baseline | 450.0 (450.0–450.0) |
| rayon | 0.7 (0.6–0.7) |
| tokio-blocking | 1.4 (1.4–1.5) |
| datafusion-spawned | 1.4 (1.4–1.5) |

The production pool returns only after the head load completes its full
bound (F15). The polling candidates return within about one to two poll
intervals (the 1 ms `CANCEL_POLL`).

**4. Two concurrent imports** (each: 16 partitions, sorts of 2^17 values, 2
workers; the private-pool baseline against one pool or runtime of 2 shared by
both imports).

| Candidate | Wall ms (both imports) | Effective cores | Peak concurrent loads |
| --- | --- | --- | --- |
| baseline (private pools) | 19.4 (18.2–21.0) | 4.13 (3.45–4.40) | 4 |
| rayon shared pool | 35.6 (35.4–35.9) | 1.97 (1.95–2.26) | 2 |
| tokio-blocking shared runtime | 35.5 (35.4–36.1) | 1.97 (1.66–1.98) | 2 |
| datafusion-spawned shared runtime | 35.5 (35.3–35.8) | 1.97 (1.96–1.98) | 2 |

Shared admission does what it says (F12): it caps both imports at the
shared budget, trading wall time for a bounded CPU footprint. The private
pools use twice the cores. Which is right is a policy question for #1509.

**5. A panicking load with the other worker alive.** The baseline (with
#1564), `rayon`, `tokio-blocking` and `datafusion-spawned` all returned
`partition load panicked`. `tokio-inline` has no containment: the panic
propagates to the caller (the report records `Err("panic")` from its own
`catch_unwind`), which is the unbounded behaviour F3's control accepts by
running loads on the caller.

## Maintenance and correctness tradeoff (protocol §5.6)

Code sizes are non-comment, non-blank lines at this revision (measured).
Adapters are spike-quality: no production diagnostics, no evidence plumbing.

| Design | Scheduler code | What the library provides | What the adapter still owns | New risks |
| --- | --- | --- | --- | --- |
| Production pool | 120 lines before #1564 (`State`/`Shared`/`worker`/`consume_in_partition_order`); #1564 adds panic containment and a stop guard | `std::thread::scope` joins | Window, reorder buffer, stop flag, error ordering, live-worker accounting, panic containment | Cancellation is noticed only in `consume` (F15) |
| Rayon | 62 lines (`rayon_ordered`) | Scoped borrowing and joined return (`in_place_scope`); pool threads; optional shared pool = process admission | Window, reorder buffer, stop flag, `catch_unwind`, cancel polling | Blocking file I/O inside a shared Rayon pool occupies CPU workers that analytics (`ComputePool`) would also use; a coordinator that is itself a pool worker blocking on `recv` could starve the pool (not exercised) |
| Tokio `spawn_blocking` | 98 lines (`tokio_ordered` + `drive`) | Blocking threads, `JoinError` panic containment, timer for cancel polling | Window, ordered handle queue, stop flag, **explicit drain** (F7), **`'static` ownership** (F8), **nested-runtime refusal** (F9), runtime construction per call or a dedicated blocking budget (F13) | Detached loads if the drain is ever skipped; per-call runtime construction; async context coupling |
| DataFusion `SpawnedTask` | shares Tokio's 98 lines | As Tokio, plus abort-on-drop for tasks not yet started | As Tokio | As Tokio; abort-on-drop can suggest a cancellation it does not provide for started blocking work (F7) |
| DataFusion operators | n/a | Partition streams, order via merge | n/a | No window, completion order or whole-input materialization (F14): inapplicable to this contract |
| Tokio inline | 28 lines | Nothing parallel | Everything | Serial CPU (F3) |

Dependency and build notes (estimates marked): Rayon is already in the lockfile
through `graphforge-exec`, so a production adoption adds an edge, not a crate
(measured: `Cargo.lock` gains one dependency line). Tokio and DataFusion are
already normal storage dependencies. Upgrade exposure (estimate): Rayon's
scope API has been stable across 1.x; Tokio blocking-pool and `JoinHandle`
semantics are stable 1.x API; `SpawnedTask` moved between DataFusion crates
before (it is re-exported at `datafusion::common::runtime` in 54, not
`datafusion::common_runtime`), which a direct dependency would inherit.

## Bounded conclusions for #1509

These hold only for the tested design (ordered, windowed, joined partition
loads at finish time) on this host and pins.

- **DataFusion operator scheduling:** inapplicable to this contract (F14),
  shown by runnable probes rather than inferred. DataFusion's lower-level
  scheduling is Tokio and is evaluated as such.
- **Tokio / DataFusion `SpawnedTask`:** runnable and correct once the adapter
  adds joined drain, `'static` ownership and nested-runtime refusal. It delivers
  real parallel CPU (F2, measurements). It adds three invariants the thread
  designs get from `std::thread::scope` or `rayon::scope` for free. The existing
  instance runtime cannot serve as CPU admission (F13).
- **Rayon:** runnable, correct, and the smallest adapter with library-joined
  return. It is the only candidate with an existing process-level CPU budget
  (`ComputePool`) to share.
- **Production pool:** correct for everything tested once #1564 is in.
  The remaining gap is the cancellation latency in F15, which has an in-place
  fix (poll while waiting) that needs no library.

Any adopt/retain/hybrid recommendation belongs to the #1509 ADR together with
the sorting (#1506) and spill (#1507) results and complete-ingest evidence.

## Follow-ups

- F11 production defect: fixed by #1564 (PR #1565), which blocks this issue.
- F15 cancellation latency in the production pool: an input for the #1509
  decision, not filed as a defect. Cancellation is correct, only late by at
  most one head-partition load.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-23 | Scheduling/cancellation spike, findings F1–F15, measurements, raw data. |
