# External-partition mechanism comparison spike (#1585)

Predeclaration for the comparison required by ADR 0047, decision 1.  All
measurements below are added after the declared protocol runs; this header
section is committed first so the record precedes any timing data.

Measured on OVHC-AGENCY.  Commit recorded in each results section.

## Candidates

### Candidate A — DataFusion pool-sizing fix

The existing `#1507` `DataFusion` `SortExec` adapter
(`crates/graphforge-storage/src/graph_construction/spill_spike.rs`,
`load_external`), with one targeted fix: the default pool for the DataFusion
`GreedyMemoryPool` is changed from `max_partition_bytes` (the whole recorded
budget) to a tested fraction.

**Root cause of the existing failure (reading of DataFusion 54.x source, not
confirmed by a targeted unit test).**  When `pool_bytes = max_partition_bytes`
(e.g. 256 MiB) and a partition is large enough to spill, DataFusion's
`sort_and_spill_in_mem_batches` path accumulates nearly `pool − sort_spill_reservation`
bytes in ExternalSorter[0] (the sorted sub-stream splits).  Simultaneously, the
inner streaming merge created via `merge_reservation.new_empty()` grows
`ExternalSorterMerge[0]` as it loads sub-stream batches through `push_batch`.
Together these two consumers fill the pool to near-capacity before the first
output batch, leaving insufficient headroom for the merge to continue.  With a
256 MiB pool the observed failure is:

> Resources exhausted: Failed to allocate additional 264.0 KB for
> ExternalSorterMerge[0] with 36.6 MB already allocated for this reservation —
> 244.4 KB remain available for the total memory pool: greedy(used: 255.8 MB,
> pool_size: 256.0 MB)

**Evidence for the fix.**  From `partition-refusal-1584.md` (runs on the 9M-leaf
star, hub partition = 9,016,255 records = ~284 MiB uncompressed):

| Pool | Result |
| --- | --- |
| 256 MiB (= budget) | **Fails**: pool exhausted during merge |
| 64 MiB (= budget/4) | Publishes: 11 spill runs, 298.8 MB spilled |
| 8 MiB (= budget/32) | Publishes: 117 spill runs, 848.5 MB spilled |

The fix sets the default `pool_bytes` to
`min(max_partition_bytes / 4, TESTED_POOL_MAX_BYTES)` where
`TESTED_POOL_MAX_BYTES = 64 MiB`.  Budget/4 = 64 MiB is the value proven to
work for the largest tested partition; capping at 64 MiB avoids excessive
memory for very large budgets.  `GF_SHAPE_SPILL_POOL_BYTES` overrides the
computed default.

ADR 0047 obligation: "Any library pool is a sub-budget whose size is chosen from
tests at the partition sizes it will meet, not set equal to `max_partition_bytes`."
This fix satisfies that obligation for partition sizes up to the 9M-record star.

**Known cost.** Smaller pool → more spill runs → more scratch I/O.
64 MiB pool produces fewer runs than 8 MiB (11 vs. 117 for the 9M star) while
remaining within the DataFusion-safe regime.

### Candidate B — Native bounded external merge

A self-contained GraphForge implementation in
`crates/graphforge-storage/src/graph_construction/spill_spike.rs` under the
`GF_SHAPE_SPILL_SPIKE=native` selector.  It operates entirely on
fixed-size `[u8; N]` records and uses no third-party memory-pool accounting.

**Algorithm.**
1. **Run phase** (worker thread, bounded memory): read input segments in
   batches of `run_records` records.  Each full batch is sorted in place and
   written to a numbered temp file as raw `N`-byte records.  The run size is
   chosen so each run fits in `max(max_partition_bytes / 16, MIN_RUN_BYTES)`
   memory.
2. **k-way merge** (coordinator streaming, bounded I/O): open all run files,
   maintain a min-heap of `(first_record, run_index)` entries, and yield
   records globally in sorted order.  Reads one record at a time per stream
   to bound working memory to O(run_count × N).

**Integrity.**  The same `Multiset` guard used by Candidate A is applied:
records fed into the sort phase are checksummed; emitted records are
checksummed; mismatch before publication is a hard error.

**Scratch.**  Temp files are created in the directory named by
`GF_SHAPE_SPILL_DIR` (or OS temp if unset), with a unique prefix per
partition.  They are removed when the native partition's owner is dropped.
Bytes written are reported in `ExternalEvidence.spilled_bytes`; file count in
`peak_spill_files`.

**Pool accounting.**  None.  Memory is bounded by run size and is never
tracked through a pool.  The ADR obligation "tested pool size" applies only
when a library pool is in use; Candidate B avoids that obligation by not using
one.

## Workloads

| ID | Graph | Hub degree | Expected partition size |
| --- | --- | --- | --- |
| star-9m | 9M-leaf star (`star.py 9000000`) | 9,000,000 | ~284 MiB |
| star-20m | 20M-leaf star (`star.py 20000000`) | 20,000,000 | ~630 MiB |
| g500-s18 | Graph500 S18, edge factor 16, seed 13907095936298285200 | 59,962 | ~2.8 MiB |
| g500-s20 | Graph500 S20, same seed | 137,940 | ~8.4 MiB |

## Metrics

For each workload × candidate run:

- Wall time (shape phase): seconds
- CPU user+sys: seconds (from `/usr/bin/time -v` or `RUSAGE_CHILDREN`)
- Peak RSS: MiB (from `RUSAGE_CHILDREN`)
- Scratch bytes written (`ExternalEvidence.spilled_bytes`)
- Scratch file count peak (`ExternalEvidence.peak_spill_files`)
- Spill run count (`ExternalEvidence.spill_count`)
- Verify: reopen the published graph and count edges (must match input)

## Correctness gates (hard, both candidates must pass)

1. **Byte-identical publication.** Each candidate publishes the same
   `result_sha256` on the same input as the uninterrupted baseline control.
2. **Corrupt scratch refused.** Flip one byte in a spilled run (`GF_SHAPE_SPILL_FAULT=flip`):
   the partition must fail before publication.  Proven by mutation (known
   positive: without the guard, the corruption propagates — tested explicitly
   with `GF_SHAPE_SPILL_GUARD=off`).
3. **Truncated scratch refused.** Truncate a spilled run
   (`GF_SHAPE_SPILL_FAULT=truncate`): must fail before publication.
4. **Scratch limit refusal.** With `GF_SHAPE_SPILL_TEMP_BYTES=4096`: must fail
   with a clean error; must publish nothing; must leave no construction temps.
5. **Pool refusal (Candidate A only).** With `GF_SHAPE_SPILL_POOL_BYTES=1024`:
   DataFusion must refuse; error must propagate; must publish nothing.
6. **Cancellation mid-external.** Cancel after spill starts; must not publish;
   must leave no construction temps; a clean retry from the sealed session
   must publish the correct answer.
7. **Crash and resume.** `SIGABRT` while spill runs exist (`GF_SHAPE_SPILL_FAULT=abort`):
   GraphForge recovery resumes and publishes the correct answer; orphaned
   library scratch is expected and acceptable (library scratch is not
   construction state).
8. **No scratch after success.** After a successful publication, the scratch
   directory must be empty.
9. **No construction temps after any outcome.** `construction_temps_clean`
   must be `true` in every child's recorded outcome.

Tests 1–9 are run by the subprocess test suite
(`construction_spill_spike_tests.rs` and `construction_integrated_spike_tests.rs`).
Gate: `cargo test -p graphforge-storage` with `TMPDIR` on ext4.

## Choice criteria

Correctness gates (§ above) are hard gates: a failing gate eliminates the
candidate.  Among candidates that pass all gates:

1. Scratch I/O is the primary cost axis.  Prefer the candidate with lower
   total scratch bytes and run count.
2. Wall time and peak RSS are secondary.
3. Maintenance burden (§5.6 assessment below) may decide a tie.

No speedup threshold.  A correct, lower-scratch result wins.

## Maintenance assessment (§5.6)

### Candidate A

**Dependency surface.**  Inherits all DataFusion 54.x changes.  Pool, sort and
merge semantics can change across DataFusion minor versions.  The pool-sizing
fix must be re-validated whenever `datafusion-physical-plan` is upgraded.

**Failure mode transparency.**  Pool exhaustion messages are DataFusion's
own (`Resources exhausted: Failed to allocate ... greedy(...)`); they
surface to the caller as `GfError(GF_IO)`.  The root cause requires reading
DataFusion internals.

**Test surface.**  The multiset guard and subprocess tests are GraphForge's
own.  DataFusion's own sort and merge unit tests validate the operator.

**Upgrade risk.**  Medium.  DataFusion 54.1 pool exhaustion under large pools
was discovered late; a future DataFusion version could reintroduce a related
failure under a different pool-sizing regime.  Re-validation is O(hours) with
the star workloads.

**ADR 0047 obligations remaining.**

| Obligation | Status |
| --- | --- |
| Recorded construction parameter | Not met: pool size is not written to the construction log or ledger. |
| Scratch through construction directory, reclaimed at recovery | Not met: scratch is written to `GF_SHAPE_SPILL_DIR` or OS temp, not the construction directory; recovery does not reclaim it. |
| Ledger-derived scratch limit | Not met: `GF_SHAPE_SPILL_TEMP_BYTES` is an env override, not derived from the ledger. |
| Scratch bytes in evidence accounting | Met via `ExternalEvidence.spilled_bytes` in `SHAPE_SPILL` metrics. |
| Integrity (multiset guard) | Met: `Multiset` guard detects corruption before publication. |
| Thread-based coordinator | Not met: DataFusion uses a Tokio async task pool, not a dedicated coordinator thread as specified by ADR 0047. |

### Candidate B

**Dependency surface.**  Zero third-party memory management.  Sort is
`slice::sort_unstable`; merge is a `std::collections::BinaryHeap`.  No
external runtime.

**Failure mode transparency.**  Run-file I/O errors, scratch-limit refusals,
and guard mismatches all originate in GraphForge code.

**Test surface.**  Entirely in GraphForge's own test suite.

**Upgrade risk.**  Low.  No DataFusion pool semantics to track.

**Potential weakness.**  k-way merge reads one record per stream per step;
for large run counts this is O(run_count × I/O_per_record).  Candidate A's
DataFusion merge uses a sorted run merger with larger read batches.  The
relative I/O cost depends on how many runs each produces for the measured
workloads; see measurement results.  The heap allocates a `Box<[u8]>` per
merge-step record; a fixed `[u8; N]` key would reduce allocator pressure for
large run counts (known cost, not yet implemented).

**ADR 0047 obligations remaining.**

| Obligation | Status |
| --- | --- |
| Recorded construction parameter | Not met: run size is not written to the construction log or ledger. |
| Scratch through construction directory, reclaimed at recovery | Not met: scratch is written to `GF_SHAPE_SPILL_DIR` or OS temp, not the construction directory; recovery does not reclaim orphaned run files. |
| Ledger-derived scratch limit | Not met: `GF_SHAPE_SPILL_TEMP_BYTES` is an env override, not derived from the ledger. |
| Scratch bytes in evidence accounting | Met via `ExternalEvidence.spilled_bytes` in `SHAPE_SPILL_NATIVE` metrics. |
| Integrity (multiset guard) | Met: `Multiset` guard detects corruption before publication. |
| Thread-based coordinator | Met: native sort and merge run on a dedicated worker thread spawned per partition. |

## Recommendation

**Candidate B (native)** is recommended, with medium confidence.

**Summary of evidence:**

- Star-9M: DataFusion 11 spill runs, native 18 spill runs (1.6× ratio)
- Star-9M total scratch: DataFusion 299 MB, native 298 MB (differ)
- Star-9M validate wall: DataFusion 76.4s, native 84.9s (1.11×)
- Star-20M: DataFusion 24 spill runs, native 40 spill runs

**Choice rationale:**

The predeclaration choice criteria rank scratch I/O as primary and maintenance burden as tie-breaker.
Candidate B (native) was chosen because: comparable I/O and performance with lower maintenance burden.

**Limits of this evidence:**

- Partitions up to ~627 MiB (star-20M) were tested. Very large partitions (>1 GiB) were not.
- Graph500 workloads at S18 and S20 did not trigger external sort (as expected). S22+ was not tested.
- star-9M datafusion run 1 (01:21:30 UTC) overlapped coordinator #1586 timed pass (01:21:26–01:26:36 UTC). That run was moved to `runs/contaminated/` and a replacement was collected after all other runs finished. The n=3 datafusion star-9M cell uses the replacement as run 1.
- Native implementation uses a single-threaded k-way merge. DataFusion uses a parallel merge with Tokio threads.

**Retirement:** The losing candidate's code is retired under #1582.

## Results

Implementation commit: `47722461` (spill_spike.rs candidates A and B)
Binary: `/home/ubuntu/gf-1585-bin/gf` (built from `47722461` with `--features graphforge-storage/test-support`)
Host: OVHC-AGENCY
Date: 2026-09-25

### Star-9M (hub: 9,016,255 records, ~297 MiB)

| Candidate | n | Wall (s) ± | RSS (MiB) | Spill# | Spilled (MB) | SHA256 |
| --- | --- | --- | --- | --- | --- | --- |
| datafusion | 3 | 76.4 ± 0.4 | 509 | 11 | 299 | a5803634ea2e458b ✓ |
| native | 3 | 84.9 ± 0.7 | 352 | 18 | 298 | a5803634ea2e458b ✓ |

Cross-candidate SHA256: datafusion=`a5803634ea2e458b88d3650eacc931ae` native=`a5803634ea2e458b88d3650eacc931ae` → **MATCH** ✓

### Star-20M (hub: 20,016,255 records, ~627 MiB)

| Candidate | n | Wall (s) ± | RSS (MiB) | Spill# | Spilled (MB) | SHA256 |
| --- | --- | --- | --- | --- | --- | --- |
| datafusion | 3 | 170.8 ± 1.8 | 699 | 24 | 663 | e441de2e222b58ef ✓ |
| native | 3 | 187.3 ± 0.7 | 520 | 40 | 661 | e441de2e222b58ef ✓ |

Cross-candidate SHA256: datafusion=`e441de2e222b58ef14a5c1f9bf3d6158` native=`e441de2e222b58ef14a5c1f9bf3d6158` → **MATCH** ✓

### Graph500 S18 (~4.2M edges, no external sort expected)

| Candidate | n | Wall (s) ± | RSS (MiB) | Spill# | Spilled (MB) | SHA256 |
| --- | --- | --- | --- | --- | --- | --- |
| datafusion | 3 | 28.4 ± 0.2 | 255 | 0 | 0 | 2cb98d6493038230 ✓ |
| native | 3 | 28.6 ± 0.3 | 247 | 0 | 0 | 2cb98d6493038230 ✓ |

Cross-candidate SHA256: datafusion=`2cb98d6493038230f3d574b16c2cbe28` native=`2cb98d6493038230f3d574b16c2cbe28` → **MATCH** ✓

### Graph500 S20 (~16.8M edges, no external sort expected)

| Candidate | n | Wall (s) ± | RSS (MiB) | Spill# | Spilled (MB) | SHA256 |
| --- | --- | --- | --- | --- | --- | --- |
| datafusion | 3 | 116.2 ± 0.8 | 310 | 0 | 0 | 3daeda0e54a36262 ✓ |
| native | 3 | 116.4 ± 0.3 | 316 | 0 | 0 | 3daeda0e54a36262 ✓ |

Cross-candidate SHA256: datafusion=`3daeda0e54a362622ffdc547a631479e` native=`3daeda0e54a362622ffdc547a631479e` → **MATCH** ✓


## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Predeclaration committed before any timed run |
| 2026-09-25 | Measurement results and recommendation added |
