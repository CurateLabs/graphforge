# Integrated construction reuse experiment (#1509)

Parent: [#1504](https://github.com/CurateLabs/graphforge/issues/1504). Follows the
binding protocol in
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
Mechanism evidence: sorting and partitioning
([#1506](shape-sort-partition-spike-1506.md)), spill and memory
([#1507](spill-memory-pool-1507.md)), scheduling and cancellation
([#1508](construction-scheduling-spike-1508.md)).

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

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-24 | Predeclared experiment, before any timed run. |
| 2026-09-24 | Budget calibration at S18 under the predeclared rule selected 1 MiB, before any timed run. |
| 2026-09-24 | Driver fix after the first timed run: the untimed reopen query inherited a tmpfs `TMPDIR` and failed with a cross-device rename. It now uses the ingest's ext4 `TMPDIR`. The one completed observation was set aside and the measurement restarted from the beginning. Timed commands are unchanged. |
