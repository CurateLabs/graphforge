# Integrated construction reuse experiment (#1509)

> **Retired (#1582).** The integrated harness this document evidences
> (`construction_integrated_spike_tests.rs`, `from_env`,
> `recorded_budget_override` and its call at session open) was removed from
> `crates/`, along with the mechanism spikes it combined (#1506, #1507, #1508).
> The code was last present on `main` at commit `d2c52a87`; rebuild from git
> history and this evidence if a trigger in ADR 0046 fires.

Parent: [#1504](https://github.com/CurateLabs/graphforge/issues/1504). Follows the
binding protocol in
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
Mechanism evidence: sorting and partitioning
([#1506](shape-sort-partition-spike-1506.md)), spill and memory
([#1507](spill-memory-pool-1507.md)), scheduling and cancellation
([#1508](construction-scheduling-spike-1508.md)).
Decision: [ADR 0046](../../adr/0046-construction-reuse-decisions.md).

## Predeclared experiment (recorded before any timed run)

### Why a new integrated measurement

The mechanism spikes measured on `3b514cd8` and `82a30cb6`. Since then #1562
(PR #1571) changed shaping: durable finish stages now retire partition runs as
their outputs complete, and the transient peak fell from 449 to 316 B/edge at
S20. No spike number is the integrated baseline. Both sides of every comparison
here run on the same tree, from one binary.

No complete ingest has run a library scheduler before this experiment. #1508's
schedulers were `cfg(test)` only, and its real-finish measurement stopped at
the shaped output.

### Designs under test

| Design | Sorting | Partitioning | Over-budget partition | Finish-time loads |
| --- | --- | --- | --- | --- |
| Production | `sort_unstable` | Recorded splitters | Refused | Production pool |
| Scheduler alone: `rayon` | same | same | Refused | Rayon `in_place_scope` |
| Scheduler alone: `tokio` | same | same | Refused | Tokio `spawn_blocking` |
| Hybrid: `hybrid` | same | same | DataFusion external sort under a pool of the recorded budget | Production pool |
| **Integrated candidate: `hybrid-rayon`** | same | same | same as `hybrid` | Rayon `in_place_scope` |

Every other mechanism candidate is excluded because its own spike already
recorded a disposition with evidence. In-memory Arrow and DataFusion sorts
were retained by #1506. Hash, round-robin and formula partitioning are
inapplicable by #1506's reviewed proofs. DataFusion operator scheduling is
inapplicable by #1508 F14. `tokio::fs` is inapplicable by #1507.

The Tokio-driven schedulers combined with the hybrid are not a measured mode.
The correctness tests below show that combination cannot run: the external
partition streams its merge on the coordinator with `Runtime::block_on`, and a
Tokio scheduler's coordinator is already inside `block_on`. That was found by
the correctness suite before measurement.

### Selection

One release `gf`, built with
`cargo build --release -p graphforge-cli -p graphforge-storage --features graphforge-storage/test-support`.
Every selector is compiled only under `cfg(test)` or `test-support`. Unset
selectors leave the production path unchanged, and invalid values fail closed.

| Variable | Effect |
| --- | --- |
| `GF_SHAPE_LOAD_SCHEDULER` | Selects a #1508 scheduler at the real `schedule_loads` call site: `baseline`, `rayon`, `tokio-blocking`, `datafusion-spawned` or `tokio-inline` |
| `GF_SHAPE_LOAD_WORKERS` | Forces the finish-time worker count |
| `GF_SHAPE_MAX_PARTITION_BYTES` | Replaces the recorded `max_partition_bytes` budget at session open. It is recorded in the checkpoint, so a resume must present the same value |
| `GF_SHAPE_SPILL_SPIKE=datafusion` | The #1507 hybrid, unchanged |

### Budget

At the default 256 MiB budget no Graph500 partition at S18 or S20 is refused,
so the hybrid would never take its external path. The hybrid modes therefore
record a smaller budget. The budget is chosen by this rule, before any timed
run:

1. Run the `hybrid` mode once at S18, untimed, at each of 8, 4, 2 and 1 MiB
   and 512 KiB.
2. Choose the largest budget at which the ingest completes and at least 1% of
   S18's fixed-width partitions take the external path. #1507 counted 1,298
   fixed-width partitions at S18.
3. If no candidate meets both conditions, choose the smallest budget that
   completes, and say so.

The same budget is used at S20. The `baseline`, `rayon` and `tokio` modes keep
the default budget, which is the production contract. The comparison is
therefore production against a design that processes the refused partitions
externally. It is not two designs under one budget, because under the small
budget production refuses.

### Budget selected (calibration, untimed)

Run once per candidate at S18 before any timed run. Raw records:
[`calibration-s18.jsonl`](construction-reuse-integrated-1509/calibration-s18.jsonl).

| Recorded budget | Ingest | External fixed-width partitions | Share of about 1,298 | Largest external partition input |
| --- | --- | ---: | ---: | ---: |
| 8 MiB | Completed | 0 | 0% | none |
| 4 MiB | Completed | 0 | 0% | none |
| 2 MiB | Completed | 2 | 0.15% | 2.89 MB |
| **1 MiB** | Completed | **119** | **9.2%** | 2.89 MB |
| 512 KiB | Completed | 768 | 59% | 2.89 MB |

The rule selects **1 MiB**: the largest candidate at which at least 1% of
S18's fixed-width partitions take the external path. Every external partition
also spilled at these budgets, because the pool equals the budget and each
external partition exceeds it by construction.

### Measurement

Driver: [`construction-reuse-integrated-1509/measure.py`](construction-reuse-integrated-1509/measure.py),
derived from the #1507 driver.

- **Input:** Graph500, edge factor 16, seed `13907095936298285200`. Ladder
  profile's five `import-session` commands into a fresh project.
- **Scales and modes:** S18 and S20. Modes `baseline`, `rayon`, `tokio`,
  `hybrid`, `hybrid-rayon`. Three rounds per scale, mode order rotated each
  round.
- **Tighter-memory stress case:** `hybrid-rayon` with a 64 KiB pool at S18,
  three runs.
- **Per run:** page cache dropped, then 12 consecutive QUIET samples 5 s apart.
  The host guard is sampled every second during the run. A run that saw BUSY is
  retained under `contended/`, excluded and retried.
- **Timed:** all five `import-session` commands. Reported: wall, process CPU
  (`wait4`), validate and commit wall, peak RSS, `ru_oublock`, host
  `nr_dirtied` and `nr_written` deltas.
- **Untimed after each run:** reopen by `gf query`, node and edge recounts and
  the one-hop and two-hop profile queries, recording each `result_sha256`.
- **Instrumented, untimed:** one run of `hybrid-rayon` at each scale, and one
  of the stress case, with per-partition external-sort metrics.
- **Report:** median and observed range. No significance claim at n=3. No
  speedup threshold is set, here or after measuring (protocol §5.3).

### Correctness gates

1. Every accepted run's query answers equal `baseline`'s at the same scale.
2. GraphForge-accounted rows, chunks, write bytes and fsyncs are reported per
   mode. A difference is reported, not averaged away.
3. The correctness suite below passes on the measured tree.
4. No run leaves DataFusion runs in its scratch directory.

## Correctness evidence

`crates/graphforge-storage/src/construction_integrated_spike_tests.rs` runs
every case as a complete construction (shape, encode, publish, reopen, adjacency
hydration) in a fresh process, through the #1507 child body. The fixture is
#1507's: 1,024 nodes and 8,192 edges, half of them pointing at one hub. The
tight recorded budget is 16 KiB and reaches the child through
`GF_SHAPE_MAX_PARTITION_BYTES`, so the budget override is itself under test.
Every child runs under a 300 s bound, so a scheduler that hangs fails its case.

```bash
TMPDIR=<ext4 dir> cargo test -p graphforge-storage --lib integrated_ -- --nocapture
```

| Case | Outcome |
| --- | --- |
| Production path, default budget (control) | Publishes; the reopened adjacency index holds 8,192 edges |
| Production path, 16 KiB recorded budget | Refused: `exceeds recorded budget`; nothing published |
| Each scheduler alone (`baseline`, `rayon`, `tokio-blocking`, `datafusion-spawned`) at 1, 2 and 3 workers | Byte-identical to the control (shaped and encoded fingerprint), no temps |
| Hybrid with the production pool or Rayon, at 1, 2 and 3 workers | Byte-identical to the control; one partition external and spilling; no DataFusion runs left |
| Hybrid with `tokio-blocking` or `datafusion-spawned`, at 1, 2 and 3 workers | **Cannot run.** The child panics with `Cannot start a runtime from within a runtime`. Nothing is published and no runs are left |
| Integrated candidate, spill payload byte flipped | Refused by the #1507 guard |
| Integrated candidate, spill run truncated | Refused (`failed to fill whole buffer`) |
| Integrated candidate, 4 KiB disk quota | Refused (`exceeded the allowable limit`) |
| Integrated candidate, 1 KiB pool | Refused (`Not enough memory to continue external sort`) |
| Invalid scheduler, zero workers, non-numeric budget | Refused with a structured error before anything is published |
| Zero recorded budget | Refused by budget validation (`invalid construction budgets`) |
| Integrated candidate, cancelled after an external sort spilled, then retried | Cancel returns a structured error with no temps or runs; the retry publishes the control's bytes |
| Integrated candidate, process aborted after spilling (exit by signal), then resumed | Resumes to the control's bytes |
| Integrated candidate, exit at `shape.partition_output.after_install` (exit 86), then resumed | Resumes to the control's bytes |
| Either crash, resumed with a different recorded budget | Refused: `checkpoint authority or resume parameters changed` |

In every failing case the recorded outcome shows no published generation and
no construction temporaries.

**Known positive for the override.** With the override call removed from
session open, all three integrated tests fail. The tight-budget control
publishes instead of refusing, and the non-numeric budget is accepted.

**Why the Tokio schedulers cannot host the hybrid.** The #1507 adapter hands the
coordinator an external partition whose sorted output is still streaming. The
coordinator pulls it with `Runtime::block_on(stream.next())` in
`ExternalPartition::for_each_record`. The #1508 Tokio and DataFusion adapters
run the coordinator's `consume` inside their own `block_on`. Tokio forbids
entering a runtime from a thread already driving one, so the process panics.
The failure is a panic, not a structured error. Making the pair work needs one
of these adapter changes, none of which exists today:

- stream the merge on a separate thread and hand batches over a channel;
- drive the external stream on the scheduler's runtime instead of its own;
- or make `consume` asynchronous.

Each adds an ownership boundary that the Rayon and production pools do not
need, because their coordinator is a plain thread.

## Measurements

**Setup.** Binary `gf` SHA-256 `bc324b03…d601c`, generator
`4556db3b…6da1`, both built at `9d4c9cba` (this branch on `origin/main`
`0c91dc4c`). Host OVHC-AGENCY: 16 logical CPUs, 125 GiB RAM, ext4 on mdadm
RAID 1, no cgroup limit. Inputs: S18 nodes `44c9dfd9…` and edges `f112ccbe…`,
the same S18 inputs as #1481 and #1507; S20 hashes are in
[`main/manifest.json`](construction-reuse-integrated-1509/main/manifest.json).
Runs took place 2026-09-24 13:45–15:03 UTC. All 30 main observations and all 3
stress observations were accepted on their first attempt; no run saw a busy
host. Raw data: [`main/`](construction-reuse-integrated-1509/main/) and
[`stress/`](construction-reuse-integrated-1509/stress/), with validate and
commit receipts per run and a `SHA256SUMS` over the directory.

The first attempt stopped after one run on the driver's query `TMPDIR` defect
(changelog). Its one observation is kept as
[`main/aborted-first-attempt.jsonl`](construction-reuse-integrated-1509/main/aborted-first-attempt.jsonl)
and is not in any figure.

Medians, with the observed range in brackets. Deltas are against `baseline` at
the same scale. The stress row ran as a separate invocation immediately after
the main runs, with the same binary, inputs and quiet rules, and is compared
with the main S18 baseline.

| Scale / mode | Ingest wall (s) | Δ wall | Ingest CPU (s) | Δ CPU | Validate wall (s) | Validate peak RSS (MiB) | Host writeback (GB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S18 baseline | 31.4 [30.9–31.5] | — | 28.7 [28.2–28.8] | — | 28.3 | 232 [227–233] | 3.29 |
| S18 rayon | 31.3 [31.2–31.3] | −0.3% | 28.6 [28.4–28.6] | −0.4% | 28.2 | 251 [231–252] | 3.28 |
| S18 tokio | 31.3 [31.1–31.4] | −0.3% | 28.7 [28.7–28.7] | +0.2% | 28.2 | 237 [232–239] | 3.28 |
| S18 hybrid | 32.6 [32.1–32.7] | +3.7% | 30.0 [29.9–30.1] | +4.5% | 29.3 | 260 [258–266] | 3.57 |
| S18 hybrid-rayon | 32.2 [31.9–32.5] | +2.7% | 30.0 [29.5–30.1] | +4.6% | 29.2 | 285 [267–300] | 3.58 |
| S18 hybrid-rayon, 64 KiB pool | 36.4 [36.1–36.4] | +15.9% | 35.7 [35.7–35.8] | +24.6% | 33.2 | 259 [257–260] | 4.19 |
| S20 baseline | 128.0 [127.1–129.2] | — | 118.0 [117.8–118.8] | — | 116.0 | 308 [294–309] | 13.25 |
| S20 rayon | 127.7 [127.6–128.1] | −0.2% | 118.0 [117.9–118.1] | −0.0% | 115.3 | 305 [269–320] | 13.25 |
| S20 tokio | 127.4 [126.9–127.5] | −0.4% | 118.1 [117.7–118.6] | +0.0% | 115.5 | 286 [283–296] | 13.25 |
| S20 hybrid | 133.9 [132.5–134.0] | +4.6% | 130.1 [129.0–130.5] | +10.2% | 122.0 | 362 [361–387] | 17.31 |
| S20 hybrid-rayon | 134.1 [133.8–134.7] | +4.7% | 130.0 [130.0–130.5] | +10.2% | 121.9 | 368 [360–374] | 17.31 |

**Instrumented runs** (untimed, one per case):

| Case | External partitions | Input wire bytes | Spill runs | Spilled bytes | Byte amplification | Peak pool of limit | Peak simultaneous run files | Sort + merge wall (s) |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| S18 hybrid-rayon, 1 MiB | 119 (all spilled) | 148.5 MB | 620 | 291.8 MB | 1.97× | 1,048,402 of 1,048,576 B | 7 | 1.84 + 0.67 |
| S20 hybrid-rayon, 1 MiB | 256 (all spilled) | 1,107.3 MB | 5,380 | 4,050.6 MB | 3.66× | 1,048,402 of 1,048,576 B | 21 | 19.40 + 5.49 |
| S18 hybrid-rayon, 64 KiB pool | 119 (all spilled) | 148.5 MB | 11,974 | 878.3 MB | 5.92× | 65,488 of 65,536 B | 117 | 7.33 + 1.05 |

No run, timed or instrumented, left a DataFusion run file behind.

### What the numbers show

- **Correctness held in every mode.** At each scale, every accepted run's node
  count, edge count, one-hop and two-hop `result_sha256` equal `baseline`'s.
  Every commit receipt reports the same GraphForge-accounted evidence: at S18
  4,456,448 rows, 68 chunks, 9,310 fsyncs and 3,039,429,943 write bytes; at
  S20 17,825,792 rows, 272 chunks, 37,695 fsyncs and 12,272,356,363 write
  bytes. The stress runs match S18's.
- **The scheduler alone does not change complete ingest.** Rayon and Tokio
  land within 0.4% of baseline on wall and CPU at both scales, inside the
  observed ranges. At the production worker count of two, finish-time loads
  are not what bounds ingest; #1508 found the real finish bound by the ordered
  consume. Library scheduling adds nothing here and removes nothing wrong.
- **The integrated candidate is correct and costs time, CPU and memory.**
  Processing about 7–9% of fixed-width partitions externally (119 at S18, 256
  at S20) under a 1 MiB pool costs +2.7–4.7% wall and +4.5–10.2% CPU against
  production, which keeps those partitions in memory under its default budget. The 64 KiB pool raises that to
  +15.9% wall and +24.6% CPU at S18.
- **Rayon does not change the hybrid.** `hybrid` and `hybrid-rayon` overlap on
  wall and CPU at both scales.
- **A smaller budget did not buy a smaller process.** Validate peak RSS rose
  in the hybrid modes (+28 to +60 MiB), although their recorded budget is 1 MiB
  against production's 256 MiB. The partitions production keeps resident here
  are at most 8.4 MB of input, so the default budget was never what sized the
  process. The DataFusion runtime, batches and spill buffers sit outside the
  pool, as #1507 found.
- **Spill bytes reach storage and GraphForge cannot see them.** Host writeback
  grew by 0.28 GB at S18 and 4.06 GB at S20, and by 0.90 GB under the 64 KiB
  pool. None of it appears in GraphForge's I/O evidence or allocation ledger.

## Bounded conclusions for the ADR

These hold for the tested designs, pins, host and scales only.

- **Integrated candidate (hybrid + Rayon): correct, not adopted as a unit.**
  It is the one combination that runs end to end. Its only benefit is turning
  a refusal into a publication, and no production workload is known to hit the
  refusal. Its costs are measured above.
- **Hybrid external sort: designated design for over-budget partitions,
  pending a maintainer decision on the refusal contract.** Its adapter
  obligations from #1507 stand, plus one from this experiment: it requires a
  thread-based scheduler.
- **Scheduling: retain the production pool.** Rayon is equivalent at complete
  ingest and remains the designated alternative if a shared CPU budget or
  nested lanes are needed. Tokio and DataFusion `SpawnedTask` are rejected for
  construction CPU scheduling: equivalent alone, unable to host the hybrid,
  and carrying the extra invariants #1508 recorded.
- **Tokio-driven scheduler with the hybrid: incompatible as built**, with the
  three adapter routes listed under correctness evidence. None is justified by
  a measured benefit.

## Maintenance assessment (protocol §5.6)

Code sizes are non-blank, non-comment lines at this branch's head (measured).
Everything else is marked as an estimate.

| Alternative | Custom code it deletes | Adapter code it adds | What GraphForge still owns | New risks |
| --- | --- | --- | --- | --- |
| Production (retained) | none | none | Pool (`partition_load.rs`, 188 lines with #1564's panic containment), splitters, budgets, spills, receipts, recovery | Cancellation noticed only in `consume` (#1508 F15) |
| Rayon scheduler alone | *Estimate:* the pool's window, reorder buffer and worker loop, about 120 lines | `rayon_ordered`, 62 lines, plus `contain_panic` (3) and cancel polling | Window, reorder buffer, stop flag, panic containment | A shared Rayon pool would put blocking file I/O on CPU workers that analytics also uses; not exercised |
| Tokio scheduler alone | Same *estimate* | `tokio_ordered` (27) and `drive` (69), plus nested-runtime refusal (8) | As Rayon, plus explicit drain (F7) and `'static` ownership of the load (F8) | Detached loads if the drain is skipped; cannot host the hybrid (this experiment) |
| Hybrid external sort | none: it adds capability for refused partitions | `spill_spike.rs`, 730 lines, and the `LoadedPartition` split in `partition_shaping.rs` | Splitters, segments, receipts, recovery, total admission, the integrity guard, run-directory ownership, a shared disk limit, evidence accounting (#1507) | Unvalidated spill reads; orphaned runs after a crash; DataFusion API churn per major (*estimate:* a small touch per upgrade) |
| Integrated candidate (hybrid + Rayon) | The Rayon row's *estimate* | Both adapters: about 800 lines | Everything in both rows | Both rows' risks; the thread-scheduler requirement couples the two choices |
| This experiment's selectors | none | `from_env` (29), `recorded_budget_override` (7), the `schedule_loads` branch, and a storage `rayon` dependency | — | None in production: all compiled only under `cfg(test)` or `test-support` |

Dependency and build: every candidate uses crates already in the workspace
graph. Rayon became a direct storage dependency for this experiment; it was
already built for `graphforge-exec`, so the build compiles no new crate
(`Cargo.lock` gains one edge).

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-24 | Predeclared experiment, before any timed run. |
| 2026-09-24 | Budget calibration at S18 under the predeclared rule selected 1 MiB, before any timed run. |
| 2026-09-24 | Main S18/S20 measurement and S18 stress case completed; results, instrumented runs and conclusions recorded. |
| 2026-09-24 | Driver fix after the first timed run: the untimed reopen query inherited a tmpfs `TMPDIR` and failed with a cross-device rename. It now uses the ingest's ext4 `TMPDIR`. The one completed observation was set aside and the measurement restarted from the beginning. Timed commands are unchanged. |
