# Construction sort/partition reuse spike (#1506)

> **Integrated follow-up (#1509):** this mechanism was combined with the other
> spikes' candidates and measured through complete ingest, publication, reopen
> and queries on a tree containing #1562. See
> [`construction-reuse-integrated-1509.md`](construction-reuse-integrated-1509.md).

**Status:** complete for #1506. This follows
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
**Measured revision:** `6bf02962` (branch `spike/1506-sort-partition-evaluation`
on `origin/main` `3b514cd8`), with `arrow` 58.4.0 and `datafusion` 54.1.0.
**Raw evidence:** [`sort-partition-spike-1506/`](sort-partition-spike-1506/).

These results cover the tested mechanisms, designs and envelope only: sorting a
materialized construction partition, and routing records to partitions. They
are not a verdict on DataFusion or Arrow in general. Spill authority is covered
by #1507, scheduling by #1508, and the integrated design and ADR by #1509.

## Candidates and what ran

`GF_SHAPE_SORT_SPIKE` selects who sorts a partition inside the real loaders
(`load_fixed_partition` and `RowRangePartitioner::load_partition`). The code is
compiled only under `cfg(test)` or `feature = "test-support"`. It is unset by
default, and an invalid value fails closed.

| Candidate | Where it ran | Disposition |
| --- | --- | --- |
| Arrow `sort_to_indices`: fixed-width partitions (`arrow-fixed`, `arrow`) | Real loader; storage suite; S18/S20 ingest; kernel bench | Runnable. **Retain the baseline** |
| Arrow `sort_to_indices` over a zero-copy `LargeBinary` view of compact details (`arrow`) | Same | Runnable. **Retain** |
| Arrow `sort_to_indices` on the row-partition key column (`arrow`) | Same | Runnable. **Retain** |
| DataFusion `SortExec`, in memory (`datafusion`) | Same | Runnable. **Retain** |
| DataFusion `SortExec` with a `FairSpillPool` and a caller-owned `DiskManager` directory (external sort) | Real sealed spill segments read with the real reader; unit tests; kernel bench | Runnable. **Hybrid candidate** for over-budget partitions only (see below) |
| DataFusion `RepartitionExec(Hash)` + per-partition `SortExec` + `SortPreservingMergeExec` | Unit test; kernel bench | Runnable. **Inapplicable** as routing authority (proof 2) |
| DataFusion range partitioning | none exists | **Inapplicable** (proof 1) |
| DataFusion `RoundRobinBatch` | n/a | **Inapplicable** (proof 3) |
| Arrow split-point routing kernel | none exists | **Inapplicable** (proof 4) |
| Arrow `lexsort_to_indices` | Same code path as `sort_to_indices` | Covered by the `arrow` mode (proof 5) |
| High-bit UUID formula partitioner | Existing test | **Inapplicable** (proof 6) |

Each candidate returns a permutation, and the GraphForge representation is then
reordered by it. That keeps compact details as offsets over their wire bytes:
nothing is padded back to 272 or 304 bytes. Timing did not select any candidate.
Every disposition follows from correctness, memory admission and maintenance
evidence, with no speedup threshold invented after measuring (protocol §5.3).

## Correctness evidence

- **Whole storage suite in every mode.** `cargo test -p graphforge-storage --lib`
  passed 1,296 tests (0 failed, 6 ignored) under `baseline`, `arrow-fixed`,
  `arrow` and `datafusion`. That covers construction, shaping, recovery,
  cancellation, schedule-independence and failpoint tests
  ([`storage-lib-by-mode.txt`](sort-partition-spike-1506/storage-lib-by-mode.txt)).
- **Byte-identical publication across modes.**
  `same_input_twice_produces_identical_digests` prints its digests. Run once per
  mode, the digests were identical in all four modes: 64 partitions, all 5
  shaped runs and all 28 encoded artifacts
  ([`cross-mode-digests.txt`](sort-partition-spike-1506/cross-mode-digests.txt)).
  The shaped digests equal the #1441 values (for example, edge details
  `f663cd84…`, endpoints `14a3c60b…`).
- **Graph500 ingest answers.** All 18 S18/S20 ingests, across 3 modes and 3
  rounds, published and reopened. Their recount, one-hop and two-hop
  `result_sha256` values are identical within each scale. Arrow IPC file bytes
  and published Parquet file hashes differ even between two baseline runs,
  because every run creates a fresh project. Those hashes are therefore not a
  cross-mode signal, and the receipts' result digests are used instead.
- **Selector preflight.** The frozen `gf` refused `GF_SHAPE_SORT_SPIKE=bogus`
  with `invalid shape sort experiment mode`. With `datafusion` selected it
  completed an S10 ingest. This proves the selector was compiled into the binary.
- **Unit tests** (`partition_records/sort_spike.rs` and
  `sort_partition_spike/tests.rs`):
  - Every mode matches baseline bytes on hub-duplicated fixed records and on
    compact details with duplicate UUIDs.
  - A compact detail stays compact after sorting.
  - Row-order permutations match.
  - Invalid modes fail closed.
  - The DataFusion path refuses to nest inside a Tokio runtime.
  - The external-sort and hash-partition tests are described in their sections.

## Measurements

The host has 16 logical CPUs, 125 GiB RAM and an ext4 md RAID root. Before every
timed run the driver required 60 s of continuous QUIET, and it recorded QUIET
again after each run. All 18 ingest runs were quiet before and after. The
frozen binaries and input hashes are in
[`host-state.txt`](sort-partition-spike-1506/ingest-pairs/host-state.txt) and
[`inputs.sha256`](sort-partition-spike-1506/ingest-pairs/inputs.sha256).

### Complete ingest (headline)

The headline is protocol §5.1's quiet-host pairs, run with
[`run-ingest-pairs.sh`](sort-partition-spike-1506/run-ingest-pairs.sh):

- One `gf` built with `-p graphforge-cli -p graphforge-storage --features
  graphforge-storage/test-support`.
- Ladder-profile inputs: Graph500, edge factor 16, seed `13907095936298285200`.
- Three rounds per mode at each scale, with the mode order rotated each round.
- Page cache dropped before each run.
- Timed region: `import-session validate` plus `commit`. Shaping runs inside
  `validate`.

| Scale | Mode | Median wall s [range] | Δ wall | Median CPU s | Δ CPU | Median peak RSS |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| S18 | baseline | 30.22 [30.16–30.33] | — | 28.14 | — | 231 MiB |
| S18 | arrow | 30.47 [30.28–30.60] | +0.8% | 28.30 | +0.6% | 252 MiB (+21) |
| S18 | datafusion | 30.39 [30.35–30.54] | +0.6% | 28.52 | +1.4% | 257 MiB (+26) |
| S20 | baseline | 124.04 [124.03–124.11] | — | 116.15 | — | 288 MiB |
| S20 | arrow | 125.37 [123.84–125.50] | +1.1% | 117.44 | +1.1% | 322 MiB (+34) |
| S20 | datafusion | 124.62 [124.08–125.04] | +0.5% | 117.94 | +1.5% | 333 MiB (+45) |

With N=3 per mode the wall differences are at most about 1% and partly
overlapping, so they are not a demonstrated slowdown. Nothing is faster. The
RSS increase is consistent in every run, because the candidates hold a second
copy of the partition that the in-place baseline never holds. Partition sorting
is a small share of ingest, so kernel ratios do not carry over to ingest time.

### Kernel measurements (supporting)

These come from [`examples/sort_partition_spike.rs`](../../../crates/graphforge-storage/examples/sort_partition_spike.rs):

- 5 rotated rounds per workload.
- A counting global allocator measures the transient heap peak.
- Inputs are regenerated outside the timed region.
- Every output digest is checked against the baseline.

Full data is in [`kernel.json`](sort-partition-spike-1506/kernel.json).
Workloads use 1,048,576 records per partition. That is 64 times the 16,384
planning target, which makes the kernels visible.

| Workload | Baseline median | Arrow | DataFusion | Candidate transient heap |
| --- | ---: | ---: | ---: | --- |
| identities (26 B) | 119.0 ms | ×2.04 | ×2.29 | 58 / 64 MiB vs 0 |
| resolved endpoints (25 B) | 119.5 ms | ×2.01 | ×2.24 | 57 / 62 MiB vs 0 |
| power-law endpoints (33 B) | 101.7 ms | ×2.78 | ×3.28 | 65 / 78 MiB vs 0 |
| compact node details | 197.2 ms | ×1.95 | ×2.19 | 28 / 49 MiB vs 0 |
| row-partition key order | 90.0 ms | ×1.90 | ×2.02 | 32 / 36 MiB vs 20 |
| identities, Arrow kernel only (no adapter copies) | 120.2 ms | ×1.85 | — | 32 MiB vs 0 |

The kernel-only row separates the adapter copies from Arrow's comparator.
`sort_to_indices` alone is 1.85× slower than `sort_unstable` on these records,
so the representation boundary is not the main cost.

## Skew, hub and over-budget partitions

- **Hub-heavy data that fits the budget.** The power-law endpoints workload
  (hubs own most rows) sorts correctly in every mode. The existing
  `hub_heavy_endpoints_over_balanced_keys_are_accepted` test also passes in
  every mode.
- **A single hub key larger than the budget.** Endpoints are keyed by node UUID,
  so no splitter set can divide one hub's run of records. Adaptive partitioning
  cannot fix this. The kernel workload used 4,194,304 records (132 MiB of
  endpoints) against a 32 MiB budget:
  - **Current contract: safe refusal.** `load_fixed_partition` refused in 15 µs,
    before allocating anything: `partition materialization requires 138412032
    bytes, exceeds recorded budget 33554432`.
  - **Candidate: success within the budget.** DataFusion's external sort, with
    the pool set to that same 32 MiB, sorted the partition in a median 917 ms.
    It made 12 spills (139 MB written), its pool reservation peaked at
    33,259,520 of 33,554,432 bytes, and heap peaked at 31.3 MiB. Its output
    digest equals an in-memory sort with the budget raised to fit, which took a
    median 630 ms at a 133 MiB heap peak, so the external sort is 1.45× slower.
    Library scratch is empty afterwards.
- **A pool too small for one batch.** This fails with a structured `Resources
  exhausted` error, emits no partial output, and leaves the scratch directory
  empty (`external_sort_refuses_when_the_pool_cannot_hold_one_batch`).
- **A disagreeing record count.** The external sort refuses when the routed
  count does not match what it read
  (`external_sort_rejects_a_count_that_disagrees_with_the_routed_count`).

The external sort was not wired into complete ingest. The coordinator consumes
fully materialized partitions (rule R4 in `load_fixed_partition`), so success
here is demonstrated at the partition level, not through publication. Complete
ingest still refuses such a partition. That is the declared contract, covered by
`recorded_partition_refusal_survives_reopen_with_no_completed_shape`.

## Partitioning: incompatibility proofs

A separate agent reviewed these proofs read-only against the version-matched
sources. It confirmed all six and found no correctness bug in the experiment
code ([`independent-review.md`](sort-partition-spike-1506/independent-review.md)).

1. **No range partitioning in DataFusion 54.1.** `Partitioning` has only
   `RoundRobinBatch`, `Hash` and `UnknownPartitioning`
   (`datafusion-physical-expr-54.1.0/src/partitioning.rs:114`). The only range
   mention is a `TODO support RangePartition` comment
   (`datafusion-physical-plan-54.1.0/src/sorts/sort.rs:1144`).
2. **Hash partitions break the property shaping relies on.** Shaping relies on
   `concat(sorted partition 0..P-1) == global order`. The runnable experiment
   (`hash_partitions_lose_the_concatenation_order_that_recorded_range_splitters_keep`,
   256 partitions in the kernel bench) shows hash partitions violate it:
   - Restoring the order needs a global `SortPreservingMergeExec`, which is the
     merge #1456 removed.
   - The DataFusion pipeline took 4.89× the time of recorded range splitters
     (1,389 vs 284 ms over 4 Mi keys), and its heap peak was 260 vs 158 MiB.
   - Hash assignment is `SeededRandomState::with_seed(0)` over foldhash
     (`repartition/mod.rs:576`). foldhash disclaims output stability across
     versions or platforms, so it cannot be a recorded, resumable format
     parameter the way recorded splitters are.
3. **`RoundRobinBatch` is not key ownership.** It assigns batches by arrival
   order, not by key.
4. **Arrow has no split-point routing kernel.** Arrow 58.4 has no kernel that
   routes rows by split points. `arrow_ord::partition` only finds runs of equal
   values in input that is already sorted.
5. **Arrow `lexsort_to_indices` is not a separate candidate.** For a single key
   it delegates to `sort_to_indices` (`arrow-ord-58.4.0/src/sort.rs:939`).
   DataFusion's single-batch `SortExec` reaches the same kernel.
6. **High-bit UUID formulas are inapplicable.** UUIDv7 prefixes are timestamps
   (`partition.rs` module docs). The existing test
   `balance_assertion_refuses_formula_splitters_over_a_uuid_v7_ingest` asserts
   the resulting collapse and its refusal.

## Interaction with construction contracts

- **Recorded UUID splitters.** Unchanged. Every sort candidate runs after
  routing, inside one partition, and the partition plan and its recorded
  splitters are not touched.
- **Surrogate assignment and endpoint resolution.** Unchanged. They consume the
  shaped runs, which are byte-identical in every mode (all 5 shaped digests).
- **Canonical publication (ADR 0038).** The 28 encoded artifacts are
  byte-identical across modes. Transient intermediates were not required to
  match. They do anyway, because the candidates return the same total order.
- **External sort.**
  - It produces the same order, so an integration could write the same shaped
    run. That requires a streaming consumer in place of the materialized
    partition. Row-group boundaries would remain a function of the admitted
    record count, which is known before the sort.
  - The library spill files are transient scratch. They are not receipts or
    checkpoints, and giving them ownership, disk limits, corruption detection
    and recovery behaviour belongs to #1507.

## Maintenance and correctness tradeoff

These are estimates from this prototype's diff; they are not measured:

| Candidate | Custom code it could delete | Adapter it adds | Memory admission | Assessment |
| --- | --- | --- | --- | --- |
| Arrow `sort_to_indices` | About 10 lines. The baseline is `sort_unstable` plus a 4-line offset comparator, which is already standard-library reuse | About 150 lines: array views, a permutation gather, a contiguity check, offset widening | Adds 2.2× the partition's bytes (fixed) or 28 B per record (details), which `admit_materialization` does not charge today | **Retain.** No maintenance gain; costs memory and time |
| DataFusion `SortExec`, in memory | Same | About 60 lines more: plan, runtime, a nested-runtime guard | Same, plus an index column and a key copy | **Retain.** Same kernel with plan and runtime overhead per partition |
| DataFusion external sort | None: it would add a path for partitions the budget currently refuses | About 250 lines: spill-segment stream, peak pool, scratch directory, order and count checks | The pool bounds the operator's reservation (peak ≤ limit observed), not total process memory. Heap peaked at 31.3 MiB against 32 MiB here | **Hybrid candidate**, for over-budget partitions only. It turns refusal into bounded success at 1.45× cost. It depends on #1507 (spill authority) and #1509 (a streaming consumer through publication). It changes the declared refusal contract, so adoption needs an explicit maintainer decision |
| Hash repartition + merge | None | Would reintroduce a global merge | Heap peak 1.6× that of range routing | **Inapplicable** (proofs 1–4) |

Dependency and upgrade risk: all candidates use crates the project already
depends on. The DataFusion paths add plan/API surface (`LexOrdering`,
`StreamingTableExec`, `MemoryPool`), which changes across major versions.

Revisit if any of these happen:

- DataFusion gains caller-supplied range partitioning.
- Arrow gains a split-point routing kernel.
- Partition sorting becomes a measurable share of ingest.
- The maintainers decide over-budget hub partitions must succeed rather than
  refuse. That is the case where the external-sort hybrid becomes the lead
  candidate for #1509.

## Reproduce

```bash
# Correctness in every mode (TMPDIR must be on ext4/xfs/btrfs)
for m in baseline arrow-fixed arrow datafusion; do
  GF_SHAPE_SORT_SPIKE=$m cargo test -p graphforge-storage --lib
done
# Kernel measurements
cargo run --release -p graphforge-storage --features test-support \
  --example sort_partition_spike -- <scratch-dir> 5
# Complete-ingest pairs (quiet host; frozen binaries in <bin-dir>)
docs/development/evidence/sort-partition-spike-1506/run-ingest-pairs.sh <out> <bin-dir> 3 18 20
```

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-20 | Arrow fixed-sort spike and unit equivalence; partitioning authority note (#1511). |
| 2026-09-23 | #1511's `arrow` mode renamed `arrow-fixed`. `arrow` now also covers compact details and row partitions. Added the `datafusion` mode, the external-sort and hash-repartition experiments, the kernel bench, S18/S20 ingest pairs, independent review of the proofs, and dispositions. |
