---
title: "ADR 0046: Construction keeps its own sorting, partitioning and admission; library reuse is bounded to named hybrids"
adr: "0046"
status: "Accepted"
date: "2026-09-24"
superseded_by: null
---

# ADR 0046: Construction keeps its own sorting, partitioning and admission; library reuse is bounded to named hybrids

**Status:** Accepted

**Implementation:** No production change. Follow-ups: #1581 (cancellation repair), #1582 (retire the experiment code).

**Build target:** v0.6.0

**Related:** ADR 0013 (threat model), ADR 0038 (determinism at the publication
boundary), ADR 0045 (ingest authentication regime); epic #1504 and its children
#1505 (protocol), #1506 (sorting and partitioning), #1507 (spill and memory),
#1508 (scheduling and cancellation), #1509 (integrated experiment and this
record); #1448 (shaping parallelism), #1387 (ingest floor).

## Later decision: ADR 0047

The maintainers took the two decisions this record reserved.
[ADR 0047](0047-over-budget-partitions-and-instance-cpu-budget.md) decides that
an over-budget fixed-width partition is processed externally instead of refusing
the ingest, which replaces this record's "retain refusal" row. It also decides
that one CPU budget per instance is shared by queries and construction. Every
other decision below stands. The record is otherwise unchanged from its
acceptance.

## Context

GraphForge builds a graph with its own construction machinery. Intake sorts
UUID windows. Shaping routes records into range partitions cut at recorded
UUID splitters, loads and sorts each partition, and concatenates the sorted
partitions in index order. A bounded pool of two scoped threads loads
partitions off the coordinator, which consumes them in index order. A
partition whose materialization would exceed the recorded `max_partition_bytes`
budget is refused before anything is allocated. Arrow is already the data
plane. DataFusion and Tokio are already dependencies, used by query and the
API rather than by construction.

#1504 asked whether DataFusion, Arrow and Tokio facilities should replace that
machinery, with neither answer assumed. #1505 recorded the inventory and a
binding comparison protocol before anything was measured: no invented speedup
threshold, correctness as a hard gate, and maintenance weighed with explicit
estimates. Three spikes then evaluated one mechanism each, and #1509 combined
the credible survivors in one binary and ran them through complete ingest,
publication, reopen and queries.

The evidence behind this record:

- `docs/development/construction-reuse-inventory-protocol-1505.md`
- `docs/development/evidence/shape-sort-partition-spike-1506.md`
- `docs/development/evidence/spill-memory-pool-1507.md`
- `docs/development/evidence/construction-scheduling-spike-1508.md`
- `docs/development/evidence/construction-reuse-integrated-1509.md`

## Decision

Construction keeps its own sorting, partitioning, memory admission, scheduling
and cancellation. No mechanism is migrated to a library by this record. Two
bounded hybrids are recorded as designated designs with named triggers, and
one small in-place repair is scheduled.

Every conclusion below is bounded to the tested mechanism, design, pins
(`arrow` 58.4.0, `datafusion` 54.1.0, `tokio` 1.53.1, `rayon` 1.12.0), host
(16 logical CPUs, ext4 on md RAID 1) and scales (S18 and S20). None is a
verdict on these libraries in general, or on all library reuse.

### Decision matrix

| Mechanism | Decision | Evidence | Confidence | Revisit when |
| --- | --- | --- | --- | --- |
| Partition sorting | **Retain** `sort_unstable` with the offset comparator | #1506: Arrow and DataFusion sorts publish identical bytes; complete ingest within about 1% and not faster; +21–45 MiB RSS; kernel 1.9–3.3× slower; no custom code removed | High | Partition sorting becomes a measurable share of ingest |
| Partitioning | **Retain** recorded range splitters and family routing | #1506: six independently reviewed incompatibility proofs; DataFusion 54 has no range partitioning; hash partitions break concatenation order and are not a recordable format parameter | High | DataFusion gains caller-supplied range partitioning, or Arrow a split-point routing kernel |
| Over-budget partitions | **Retain refusal** as the contract. **Designated hybrid:** DataFusion external `SortExec` for refused partitions only | #1507 and #1509: byte-identical publication for partitions production refuses; with a 1 MiB recorded budget, where 9.2% of S18 and 7.0% of S20 fixed-width partitions go external, it costs +2.7–4.7% wall, +4.5–10.2% CPU and +28–60 MiB validate RSS against production, and its spill writes are invisible to GraphForge evidence | Medium | A production workload hits the partition-budget refusal, or the maintainers decide over-budget partitions must succeed |
| Spill files as recovery authority | **Never** | #1507: DataFusion runs are unvalidated, unsynced, path-based and invisible to recovery and the allocation ledger | High | DataFusion adds validated spill reads and a caller-supplied file factory |
| Memory admission | **Retain** GraphForge budgets; a DataFusion pool may only be a sub-budget inside them | #1507: RSS grew while the pool held 64 KiB; the pool counts only explicit reservations | High | — |
| Finish-time load scheduling | **Retain** the production pool. **Designated alternative:** Rayon `in_place_scope` | #1508 and #1509: Rayon, Tokio and DataFusion `SpawnedTask` deliver the same parallel CPU as the pool (#1508); at complete ingest Rayon and Tokio are within 0.4% of production on wall and CPU at S18 and S20 | Medium | A process-wide CPU admission policy is decided, or #1448 needs nested or shared lanes |
| Tokio or DataFusion as construction CPU scheduler | **Reject** for this contract | #1508 F7–F9, F13, F14; #1509: cannot host the external-sort hybrid (nested runtime panic) | High | Construction's coordinator becomes asynchronous end to end |
| Cancellation | **Retain** the coordinator callback; **repair** in place to poll while waiting | #1508 F15: 450 ms against 0.7–1.4 ms for polling candidates; a library is not needed for the repair | High | — |
| Integrated candidate (hybrid + Rayon) | **Not adopted as a unit** | #1509: correct through publish, reopen and query; costs match the hybrid row, and Rayon does not change them | Medium | Either component's trigger fires |

### Why retain is the answer where it is

Retaining custom code is a decision this record has to justify, not a default.
For each retained mechanism the evidence shows one of three things:

- **The library is incompatible with a product contract.** Recorded splitters
  are durable format authority, and no DataFusion 54 or Arrow 58 partitioner
  can reproduce them.
  Tokio-driven scheduling cannot host a coordinator that itself blocks on a
  runtime.
- **The library does the same work at more cost and deletes nothing.** The
  in-memory sorts replace about ten lines of standard-library code with an
  estimated 150–210 lines of adapter, and hold a second copy of each partition.
- **The library is correct and costs the same, but removes nothing that is
  wrong.** Rayon schedules the finish-time loads as well as the production
  pool, within 0.4% at complete ingest. Swapping it in would replace an
  estimated 120 of the pool's 188 lines (the window, reorder buffer and worker
  loop) with a 62-line adapter that still needs its own panic containment and
  cancel polling. That is a lateral move until something needs what Rayon
  adds: a shared process-level budget through `ComputePool`, or nested scopes.

### Maintainer decisions this record does not make

Both change an existing requirement, so the #1505 protocol reserves them for
the maintainers:

1. **Must an over-budget partition succeed rather than refuse?** Today a
   single hub whose records exceed `max_partition_bytes` refuses the ingest,
   safely and before allocating. The designated hybrid turns that refusal into
   a byte-identical publication at a measured cost. Adopting it changes the
   declared refusal contract.
2. **Should concurrent imports share one CPU budget?** #1508 F12 shows a shared
   Rayon pool or Tokio runtime caps two concurrent imports at the shared
   budget, trading wall time for a bounded CPU footprint. The API runtime's
   blocking pool gives construction no admission today (F13). Choosing shared
   admission is a resource-policy decision, and Rayon's `ComputePool` is the
   existing facility it would use.

Until either is decided, production behaviour is unchanged.

### Adapter obligations for the designated hybrid

If decision 1 is taken, the hybrid may ship only with what the library does
not supply. The first five are named by #1507 and the last by #1509:

- a run directory created through the construction directory, not a path;
- removal of that directory at recovery, because orphans survive a crash;
- one `DiskManager` shared across workers, with a disk limit derived from the
  allocation ledger;
- run bytes accounted in construction evidence;
- the record-multiset integrity guard, because DataFusion reads spill runs
  back with validation disabled and no checksum;
- a thread-based scheduler, because the streaming merge blocks on its own
  runtime (#1509).

### Staged migration and rollback, for any future adoption

No migration is accepted here. If a trigger fires, the sequence is:

1. Land the adapter behind a recorded construction parameter, never a host
   property, so resume validates it like any budget.
2. Keep the refusing path as the default until the parameter is recorded in a
   release; the old behaviour is the rollback.
3. Gate adoption on the #1505 protocol at S18 and S20 against the integrated
   baseline, plus the crash, corruption and cancellation suites unweakened.

## Consequences

- Production construction is unchanged by this record.
- The experiment code stays test-support only until #1582 retires the
  selectors, the spike modules and the storage `rayon` dependency together.
  The evidence documents remain. A trigger that fires can rebuild the hybrid
  from git history and the evidence.
- #1448 builds partition-level shaping on the retained production pool,
  `consume_in_partition_order`. That pool is scoped `std::thread`, not Rayon.
  The phrase "the existing Rayon arrangement" in #1448's retain path should
  read as this pool; Rayon today is `graphforge-exec`'s `ComputePool`, which
  construction does not use. If
  #1448 needs nested or shared lanes, Rayon `in_place_scope` is the designated
  alternative, and choosing it is part of #1448's own adoption gate.
- The cancellation latency repair (#1508 F15) is #1581.

## Open under #1504

These remain evaluation questions, not implementation follow-ups:

- **The explicit-exchange, owned-artifact pipeline** recorded as a hypothesis
  on 2026-09-21. It was not prototyped or measured. The spikes bear on it
  without settling it: DataFusion has no range exchange (#1506 proof 1),
  its operator scheduling has no bounded window below the partition count
  (#1508 F14), and its spill files cannot carry recovery authority (#1507). A
  test of the hypothesis needs its own experiment and, for its admission
  manager, manifests and publication protocol, their own records.
- **Row (Arrow property) partitions** under the hybrid. Not exercised.
- **Spill compression** for the 4.7–5.4× spill amplification. Not exercised.
- **Metadata and offset faults** in DataFusion spill runs. Not safe to test
  with validation disabled.
- **Scales above S20** and the S26 billion-edge round trip. Not claimed.

## Not claimed

- That these libraries are unsuitable in general, or for other workloads.
- That custom construction code is best practice.
- That any result here meets the #1387 ingest floor or qualifies S26.
- That encode-only evidence (#1465) settles any mechanism above.

## Evidence

| Evidence | What it establishes |
| --- | --- |
| `docs/development/construction-reuse-inventory-protocol-1505.md` | Inventory, candidate matrix and the binding comparison protocol, recorded before any measurement |
| `docs/development/evidence/shape-sort-partition-spike-1506.md` | Sorting and partitioning: runnable candidates, six reviewed incompatibility proofs, S18/S20 ingest pairs, kernel measurements |
| `docs/development/evidence/spill-memory-pool-1507.md` | External sort and memory pool under forced pressure; library properties independently reviewed; failure and crash cases |
| `docs/development/evidence/construction-scheduling-spike-1508.md` | Scheduling and cancellation: findings F1–F15, on-CPU evidence, repeated measurements |
| `docs/development/evidence/construction-reuse-integrated-1509.md` | The combined designs through complete ingest, publication, reopen and queries at S18 and S20; correctness and failure suite; maintenance assessment |
