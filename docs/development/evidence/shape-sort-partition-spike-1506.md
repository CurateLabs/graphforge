# Construction sort/partition reuse spike (#1506)

**Status:** first runnable slice — Arrow fixed-partition sort behind
`GF_SHAPE_SORT_SPIKE`. Follows
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
**Baseline revision:** recorded per PR / evidence JSON when measurements land.

## Predeclared experiment (before measurement)

| Item | Choice |
| --- | --- |
| Mechanism | Sorting (fixed-width partition materialization) |
| Candidate | Arrow `sort_to_indices` + `take` on `FixedSizeBinary` width N |
| Hybrid retain | Compact detail offset sort stays baseline (no padded round-trip) |
| Control | Unset / `GF_SHAPE_SORT_SPIKE=baseline` → `sort_unstable` |
| Selection | Env only; compiled under `cfg(test)` / `feature = "test-support"` |
| Correctness gate | Byte-identical sorted fixed partitions vs baseline; invalid mode fails closed |
| Performance gate | None invented yet — S18/S20 quiet-host pairs required before adopt/retain advice |
| Out of scope for this slice | DataFusion `SortExec`, replacing recorded UUID splitters, production default |

## Partitioning authority (incompatibility note)

Recorded sampled UUID splitters (`PartitionPlan`) are format authority. A
high-bit / hash repartition that ignores recorded splitters is **inapplicable**
as a drop-in for durable shaping plans (UUIDv7 timestamp clustering; see
`partition.rs` module docs and
`balance_assertion_refuses_formula_splitters_over_a_uuid_v7_ingest`).
Execution-only library repartition of an already planned partition remains an
open candidate for later slices — it must not rewrite splitter authority.

## How to run correctness

```bash
cargo test -p graphforge-storage --lib \
  graph_construction::partition_records::sort_spike
```

Arrow mode is exercised inside those unit tests via a process-local env lock.

## Measurement (not yet recorded)

Quiet-host paired S18–S20 complete-ingest observations comparing baseline vs
`GF_SHAPE_SORT_SPIKE=arrow` under equal CPU/memory envelopes, plus a
skew/over-budget partition case, remain required by #1506 before any adopt
recommendation. This Cloud Agent host cannot admit durable projects
(`GF_UNSUPPORTED_FILESYSTEM`); host measurements need an admitted ext4/xfs/btrfs
root.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-20 | Arrow fixed-sort spike + unit equivalence; partitioning authority note. |
