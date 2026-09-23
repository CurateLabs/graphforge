# Construction spill, buffering, and memory-pool reuse (#1507)

Parent: [#1504](https://github.com/CurateLabs/graphforge/issues/1504). Follows the
binding protocol in
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
Sibling mechanisms: sorting/partitioning (#1506), scheduling/cancellation
(#1508). The integrated ADR is #1509.

## Predeclared experiment (recorded before measurement)

| Item | Choice |
| --- | --- |
| Mechanism | Spill and memory admission for fixed-width shaping partitions |
| Candidate | DataFusion 54.1 external `SortExec` with a `GreedyMemoryPool` and a `DiskManager` (Arrow IPC spill runs, multi-level merge) |
| Hybrid | `GF_SHAPE_SPILL_SPIKE=datafusion`: partitions the baseline admits stay resident; a partition the recorded `max_partition_bytes` would refuse is sorted externally under a pool of that budget |
| Overhead probe | `GF_SHAPE_SPILL_SPIKE=datafusion-always`: every fixed-width partition goes external |
| Retained | Recorded splitters, routing, GraphForge's sealed spill segments and receipts, publication, recovery, the balance assertion, cancellation callback |
| Selection | Environment only; compiled under `cfg(test)` / `feature = "test-support"`; invalid values fail closed |
| Out of scope | Row (Arrow property) partitions, production default, splitter authority, scheduling changes (#1508) |

### Measurement protocol

- **Binary:** one release `gf` built with `graphforge-storage/test-support`; modes
  differ only by environment. Commit and binary SHA-256 recorded with results.
- **Input:** Graph500 S18 and S20, edge factor 16, seed
  `13907095936298285200`, the ladder profile's five `import-session`
  commands (begin, two `register-parquet`, validate, commit) into a fresh
  project per observation. Input SHA-256 recorded.
- **Modes:** `baseline`; `external` (`datafusion-always`, pool = recorded
  budget, 256 MiB default — operator cost without memory pressure);
  `external-tight` (`datafusion-always`, pool 64 KiB — forced spill in every
  partition, the tighter-memory stress case).
- **Repetitions:** three alternating triples per scale, mode order rotated per
  triple. A run starts after 12 consecutive QUIET samples (5 s apart) from
  the host guard and is sampled each second during execution; a run that saw
  BUSY is retained under `contended/`, excluded, and retried.
- **Metrics:** complete-ingest wall; process-tree CPU (user+sys via
  `wait4`); validate wall/CPU; peak RSS of the largest `gf` process; 512-byte
  blocks written (`ru_oublock`); host-wide `nr_dirtied`/`nr_written` page
  deltas (page cache vs storage writes). One separate instrumented run per
  external mode and scale records per-partition pool peak, spill count,
  spilled bytes/rows, peak spill disk bytes/files, and sort/merge wall; it is
  excluded from timing.
- **Report:** median and observed range; no significance claim from n=3; no
  invented speedup threshold. Published row counts must match baseline.
- **Correctness gate:** the tests below, plus the full storage library suite
  with every fixed partition external.

The driver is [`spill-memory-pool-1507/measure.py`](spill-memory-pool-1507/measure.py).

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-23 | Predeclared experiment and measurement protocol. |
