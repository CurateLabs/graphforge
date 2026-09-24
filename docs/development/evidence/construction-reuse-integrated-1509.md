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

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-24 | Predeclared experiment, before any timed run. |
