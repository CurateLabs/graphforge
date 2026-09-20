# Construction reuse inventory and comparison protocol (#1505 / #1504)

**Status:** recorded before mechanism spikes (#1506–#1508) and the integrated ADR (#1509).  
**Baseline revision:** `eeda14670594951f571e77ffdae44c2d5ff2ae3a` (`origin/main` at branch creation; includes #1503 adaptive partition materialization bound).  
**Parent epic:** [#1504](https://github.com/CurateLabs/graphforge/issues/1504).

This document satisfies #1505. Spikes must cite it and must not invent speedup thresholds after measuring. Later protocol edits require an explicit changelog entry in this file explaining why the contract changed.

## 1. Dependency baseline (version-grounded)

Exact resolved versions from `Cargo.lock` at the baseline revision:

| Crate | Workspace pin | Locked version | Role for #1504 |
| --- | --- | --- | --- |
| `arrow` / `arrow-data` / `parquet` | `58` | `58.4.0` | Batches, kernels (`take`, `concat_batches`), Parquet I/O |
| `datafusion` / `datafusion-datasource` | `54` | `54.1.0` | Query/exec/catalog elsewhere; encode seam spike only on construction (#1465) |
| `tokio` | `1` (`full`) | `1.53.1` | Async runtime (API/bindings); construction path is primarily sync |
| `rayon` | `1.10` (via `graphforge-exec`) | `1.12.0` | Parallel CPU in **exec**, not construction shaping workers |

Primary upstream references (version-matched major lines):

- Arrow compute kernels / `RecordBatch`: [arrow-rs 58.x](https://docs.rs/arrow/58.4.0/arrow/)
- DataFusion physical plans, `SortExec`, memory pool, spill: [datafusion 54.x](https://docs.rs/datafusion/54.1.0/datafusion/)
- Tokio tasks/channels/cancellation: [tokio 1.53](https://docs.rs/tokio/1.53.1/tokio/)
- Rayon scoped pools: [rayon 1.12](https://docs.rs/rayon/1.12.0/rayon/)

Existing reuse that is **not** a #1504 verdict:

- Arrow is already the construction data plane (`RecordBatch`, Parquet writers/readers, `arrow::compute::take` / `concat_batches` / `take_record_batch`).
- DataFusion owns query/catalog/`parquet_scan` surfaces under `graphforge-storage`; construction shaping does **not** run through DataFusion operators today.
- [#1465](https://github.com/CurateLabs/graphforge/issues/1465) proved a narrow encode `DataSinkExec` seam (`docs/development/evidence/encode-datafusion-seam-1465.md`). It did **not** exercise `SortExec`, repartition, external spill, or shaping. Its slower/neutral encode timing must not reject shaping/sort/spill/scheduling candidates.

## 2. Pipeline map (where mechanisms sit)

Construction lifecycle (storage-owned; bindings remain thin):

1. **Intake / stage** — accept Arrow batches; UUID-sort windows; write staged fixed/row runs and chunk receipts (`intake.rs`).
2. **Partition plan** — sample staged identity domain; record monotone UUID splitters in shape intent (`partition.rs`, `shape.rs`).
3. **Route / spill** — range-route records into per-partition spill files (`partition_shaping.rs`); seal spills with batched directory barriers.
4. **Load / sort / publish shape** — bounded workers load+sort fixed partitions; coordinator consumes in partition index order and publishes shaped runs (`partition_load.rs`, `partition_shaping.rs`, `shape.rs`); surrogate assignment and endpoint resolution on shaped outputs.
5. **Encode / publish** — Parquet encode, CAS install, receipts, supersession (`graph_construction_encoding*`, `encoding_publication*`, `supersession.rs`).
6. **Recovery / controls** — authenticated control temps, shape/encoding intents, crash boundaries (`controls.rs`, `recovery.rs`, `construction_directory.rs`).

Hard product contracts that candidates must preserve: graph identities, duplicate policy, recorded UUID splitters, surrogate/endpoint mappings, properties, ADR 0038 publication where required, CAS/receipt integrity, structured errors, atomic publication (no partial published graph on failure).

## 3. Mechanism inventory

### 3.1 Sorting

| Mechanism | Location | Role | Invariants | Tests / evidence | Existing library reuse |
| --- | --- | --- | --- | --- | --- |
| Intake UUID window sort | `intake.rs` (`uuid_sorted_batch`, `sort_unstable_by_key`) | Strict UUID order inside accepted Arrow windows before staging | Fail-closed if row artifact is not strictly UUID sorted | `intake/tests.rs` (`row_artifact_retains_…_uuid_sorted`) | Arrow `take` to reorder columns; **not** Arrow `lexsort` / DataFusion `SortExec` |
| Fixed partition in-memory sort | `partition_records.rs` (`PartitionRecords::sort`), load path in `partition_shaping.rs` | Sort each range partition independently so concat is global order | Strict sort; compact detail sort moves offsets, not padded arrays | `partition_records/tests.rs`, `partition_shaping/tests.rs` (wire-byte preservation) | Custom `sort_unstable` / offset comparator |
| Staged identity / endpoint / detail sorts | `intake.rs` fixed-run writers | Produce sorted staged runs | Strictly sorted runs validated on recovery | `recovery.rs` / `shape.rs` `validate_sorted_run` | Custom |
| Encode row-index / inventory sorts | encode path (#1465 notes) | Bounded output-batch and metadata-path ordering | Not skew-sensitive DataFusion sorts | #1465 evidence | Custom; DF seam does not add `SortExec` |

**Maintenance:** sorting logic is spread across intake, partition records, and recovery validators. The former external merge tree is **removed** (`partition_shaping.rs` module docs; `shape/tests.rs` asserts no merge levels). `GraphConstructionBudgets::merge_fan_in` remains a recorded budget field (default 32) even though the fan-in merge tree is gone—treat as format/budget residue, not an active merge scheduler.

### 3.2 Partitioning

| Mechanism | Location | Role | Invariants | Tests / evidence | Existing library reuse |
| --- | --- | --- | --- | --- | --- |
| Sampled range `PartitionPlan` | `partition.rs` | Monotone UUID range partitions; splitters recorded before route | Splitters strictly increasing; concat(partitions) == global sort; count is recorded format param (not host-derived); UUIDv7 high-bit formulas are forbidden | `partition/tests.rs` (monotone assignment, balance), collapsed-splitter refusal in session tests | Custom quantile sample (`SAMPLE_POINTS_PER_PARTITION=64`) |
| Adaptive cut / balance | `partition.rs` (`PartitionBalance`, `BALANCE_TOLERANCE=4`, `MIN_ROWS_PER_PARTITION`) | Bound skew; refuse collapsed plans | Balance only meaningful above min mean rows | Hub-heavy acceptance vs collapsed refusal tests | Custom |
| Materialization admission | `partition.rs` `admit_materialization`, budget `max_partition_bytes` (default 256 MiB) | Fail closed before allocating retained partition + sort buffers | Exceeding recorded budget errors; resume preserves authority (#1503 lineage) | Budget exceed / resume tests in `graph_construction/tests.rs` | Custom byte accounting |
| Family routing | `partition_shaping.rs` (`FixedRangePartitioner`, `RowRangePartitioner`, `PartitionFamily`) | Route identities/details/endpoints/rows into named spills | Node-keyed families may use node-specific plans when joint UUID bands skew (Graph500) | `partition_shaping/tests.rs`, shape routing comments in `shape.rs` | Arrow batches for row spills |
| Durable layout vs execution partitions | shaped `*.run` / Parquet catalogs vs in-memory partition jobs | Execution partitions are transient spills; durable shaped artifacts are authenticated | Library repartition ≠ publication layout | Shape/recovery authentication tests | N/A |

### 3.3 Spill, buffering, memory accounting

| Mechanism | Location | Role | Invariants | Tests / evidence | Existing library reuse |
| --- | --- | --- | --- | --- | --- |
| Partition spill files | `partition_shaping.rs` (`SpillWriter`, `FixedSpillWriter`, `RowSpill`, `fixed_spill_name` / `row_spill_name`) | Transient per-partition scratch during shaping | Owned temps; cleanup on failure; not durable checkpoints | Shape cleanup / crash recovery; `shape.partition_spill.*` failpoints | Custom file writers + Parquet for rows |
| Batched seal directory barriers | `controls.rs` `SealDirectoryBatch`; seal path in shaping | One directory sync per seal batch, not per spill (#1452) | Barrier accounting in evidence | `seal_batches_report_one_directory_barrier_per_seal_not_per_spill` | Custom |
| Staging / catalog admission budgets | `GraphConstructionBudgets`; `catalog.rs` | Bound batch rows/bytes, chunks, schema groups, catalog decoded/identifier bytes | Fail closed on exhaustion | Catalog streaming budget tests | Custom; Arrow batch size capped by budget |
| Construction evidence / allocation ledger | `io_evidence.rs`, coordinator in `partition_load.rs` | Account reads/writes/peaks; workers never touch shared ledger | Observed peak ≠ reservation bound; merge of worker counters is commutative | `partition_load` merge totality tests | Custom |
| Encode DF memory pool (spike only) | #1465 seam | `GreedyMemoryPool` reserves input batch estimate | Does **not** cover writer/compression/runtime; not total admitted memory | #1465 evidence | DataFusion pool (experimental path) |
| Control/artifact temps | `controls.rs` `control_temp` / `artifact_temp`; recovery cleanup | Authenticated temp install + reclaim | Descriptor authentication; no orphan published state | Controls/recovery tests | Custom filesystem capabilities |

**Separation rule for spikes:** a DataFusion/Arrow spill file is transient execution scratch unless GraphForge wraps it with the same ownership, cleanup, disk limits, corruption detection, and recovery authority as current spills. It is never automatically a durable checkpoint or receipt substitute.

### 3.4 Scheduling and cancellation

| Mechanism | Location | Role | Invariants | Tests / evidence | Existing library reuse |
| --- | --- | --- | --- | --- | --- |
| Partition load worker pool | `partition_load.rs` `consume_in_partition_order`, `PARTITION_LOAD_WORKERS=2` | Bounded `std::thread` scope; load+sort off coordinator; consume in index order | Worker count is **scheduling only**—must not change output bytes or merged evidence; at most `min(workers, partitions)` materialized | Schedule-independence tests; forced `with_load_workers` | **Custom** scoped threads — **not Rayon, not Tokio** |
| Row partition serial path | `partition_shaping.rs` | Arrow row partitions load serially | Same publication order | Row shaping tests | Arrow |
| Cancellation callback | `reject_cancelled`; encode/shape/`uuid_membership` polls; `AtomicBool` stop flag for workers | Coordinator owns `FnMut() -> bool` (no `Send` required); workers poll stop between records | Cancelled work publishes nothing; temps cleaned | Detail cancel/retry; encode cancel; #1465 sink cancel handshake | Custom callbacks (+ Tokio current-thread only in #1465 spike worker) |
| Process CPU admission / concurrent import | API resource policy + session exclusive lock | Construction session coordination file exclusively locked | Concurrent same-process open fail closed | Session lock tests in `graph_construction/tests.rs` | Custom |

**Rayon baseline:** Rayon is the repo’s parallel CPU facility in `graphforge-exec`, not the construction shaping scheduler. Comparisons that claim “replace Rayon in construction” are category errors unless a candidate first replaces the custom `partition_load` pool. Tokio presence in dependencies is not evidence that construction CPU work is async-parallel.

### 3.5 Buffering and related coordination

| Mechanism | Location | Notes |
| --- | --- | --- |
| Arrow staging windows | budgets `max_batch_rows` / `max_batch_bytes` | Input buffering before spill |
| Writer / compression buffers | Parquet `ArrowWriter` paths | Partially unaccounted by DF pool in #1465 |
| Completed-result retention | coordinator consume window | Bound by worker window; backpressure is implicit (window fill) |
| Supersession / reclaim | `supersession.rs` | Post-encode payload retirement; cancellable reclaim |

## 4. Candidate matrix (pre-measurement)

Applicability is provisional pending spikes. “Inapplicable” requires an independently reviewed correctness/API proof in the spike issue—not speculation or #1465 encode timing.

| Mechanism | Current | Arrow candidates | DataFusion candidates | Tokio candidates | Hybrid sketches | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Sorting | Custom `sort_unstable` + Arrow `take` reorder | `lexsort` / `sort_to_indices` + `take`; batch sort kernels | `SortExec` / `SortPreservingMergeExec` | N/A (CPU) | Kernel sort inside current partition load workers | Must preserve strict UUID order + packed detail offset sort semantics |
| Partitioning | Recorded sampled range splitters + family routing | Hash/range repartition helpers (if any) for **execution only** | `RepartitionExec`, input partitioning | N/A | Keep recorded splitters; optionally DF-execute per partition | **Must not** replace recorded splitter authority with host-hash or high-bit UUID formulas |
| Spill / memory | Custom spill files + budgets + evidence ledger | IPC/spill file formats as scratch | `DiskManager`, `MemoryPool` (`Greedy`/`Fair`), operator spill | Async FS wrappers (suspect for CPU path) | DF pool for operator peak + GF ledger for total admission + GF-owned spill dirs | Library spill ≠ recovery checkpoint |
| Scheduling | Custom 2-thread ordered consume | N/A | DF task scheduling / work tables | `spawn_blocking`, streams, mpsc byte-bounded channels | Tokio for I/O wait + keep ordered consume; or Rayon scoped pool with same ordered consume contract | Async API ≠ parallel CPU; nested runtimes forbidden without proof |
| Cancellation | Coordinator callback + stop flag | N/A | DF cancellation tokens / `AbortOnDrop` patterns | `CancellationToken`, cooperative cancel | Map GF callback ↔ DF/Tokio cancel at stage boundaries | Must match stage granularity tests |
| Memory admission | GF budgets + evidence; optional DF pool on spike | Buffer pooling | Memory pool reservations | N/A | Dual accounting: pool ⊆ process ⊆ cgroup | Distinguish reservation, RSS, page cache (#1465 lesson) |

## 5. Comparison protocol (binding for #1506–#1509)

Recorded **before** spike results. Spikes may add detail but may not silently replace these rules.

### 5.1 Workloads

| Class | Intent | Minimum representation |
| --- | --- | --- |
| Empty / tiny | Startup and refuse paths | 0–few nodes/edges |
| Normal | Common ingest | Small deterministic fixtures already in construction tests |
| Duplicate-heavy | Duplicate policy + sort stability | Fixture with controlled duplicate UUIDs |
| Skewed / hub | Power-law hubs; balance and over-budget partitions | Hub-heavy endpoint fixtures; partitions exceeding `max_partition_bytes` |
| Variable-width / nested properties | Row spill + property columns | Property-bearing batches |
| Spill-forcing | Forced memory pressure | Constrained `max_partition_bytes` / cgroup / pool ceiling that forces spill or refuse |
| Graph500 quiet-host pairs | Fair performance | **S18 and S20** repeated paired A/B; at least one larger **or** tighter-memory stress case. Exact supported scale stated per experiment. **Do not claim S26 qualification.** |

Input identities (generator, scale, edge factor, seed), binary revision, and configuration must be preserved with raw observations.

### 5.2 Resource envelopes

Equalize across A/B modes unless a candidate’s documented contract differs:

- Logical CPUs pinned and recorded (example from #1465: 16 logical CPUs).
- Memory: process-tree cgroup limit and/or explicit pool/budget ceilings; report **reservation bound**, **observed RSS**, and **page cache** separately.
- Storage: same durable root class constraints as product admission; do not weaken descriptor authentication for benchmarks.
- Durability: same fsync/publication barriers unless an experiment explicitly measures a stated durability reduction (default: no reduction).

### 5.3 Repetitions and uncertainty

- Prefer alternating matched pairs on a quiet host (`QUIET` before/after; retain and exclude contended runs).
- At least **three** accepted complete-ingest observations per mode for headline medians at S18–S20, unless a spike documents why a different count is required.
- Report median and observed range; do not over-claim significance from tiny N.
- No universal speedup threshold. A candidate may be recommended on maintenance/correctness with an explicit quantified performance cost only with maintainer acceptance if it changes an existing requirement (#1387 floor and related gates remain separate).

### 5.4 Metrics

Collect end-to-end and phase where instruments exist:

- Wall time and process CPU-seconds (framework authority per `docs/development/benchmarking.md` when used as gates).
- Effective concurrency / on-CPU evidence for scheduling claims (not API shape alone).
- Peak RSS, reservation bounds, copies, logical I/O, scratch file counts/bytes, spill amplification, fsync/barrier counts.
- Cancellation latency to joined return; recovery cost for interrupted vs uninterrupted imports.
- Published row counts must match for successful runs.

### 5.5 Correctness gates (hard)

Every runnable candidate must show, for its exercised surface:

1. Identities, duplicate policy, surrogate/endpoint mappings, and properties remain valid.
2. Recorded UUID splitters / partition authority unchanged unless the experiment’s written contract says otherwise **and** publication still meets ADR 0038 where required.
3. CAS/receipt integrity and structured errors preserved.
4. Failure publishes **no** partial graph; reopen/recovery matches uninterrupted success or safe refuse.
5. Forced worker counts/schedules do not change durable output bytes where schedule-independence is claimed.
6. Cancellation at distinct stages cleans temps and returns joined errors.
7. Malformed/truncated/corrupt spills, disk exhaustion, and admission refusal behave as declared (spike-scoped).

### 5.6 Maintenance assessment rubric

For each alternative, record (estimates explicitly marked):

- Custom code/tests deleted vs adapter code added
- Ownership boundary clarity (who owns spill files, memory, cancel, evidence)
- Invariant duplication risk
- Dependency/API stability and upgrade effort
- Diagnostics/debugging cost
- Portability / build / binary size impact
- Required upstream changes

Code-line reduction alone is insufficient.

### 5.7 Decision vocabulary for later ADR (#1509)

Per mechanism: **adopt** / **retain** / **hybrid**, with evidence, drawbacks, confidence, limitations, and revisit triggers. Production migration is separately tracked; completing evaluation ≠ shipping migration ≠ meeting #1387.

## 6. Sequencing and non-goals

- #1505 (this doc) blocks #1506–#1508.
- #1506–#1508 block #1509.
- All five block epic #1504.
- Non-goals: mandatory wholesale engine replacement; rewriting graph semantics/public APIs; distributed execution; weakening durability to win a benchmark; implementing every recommended production migration inside the evaluation.

## 7. Changelog

| Date | Change |
| --- | --- |
| 2026-09-20 | Initial inventory and protocol for #1505 against baseline `eeda1467`. |
