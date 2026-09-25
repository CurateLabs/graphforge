# Instance construction CPU budget (#1586)

Implements decision 2 of
[ADR 0047](../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md):
one CPU budget per instance, with construction held below it so queries keep a
share while imports run.

## What the code does

- `ConstructionCpuAdmission` (`graphforge-storage`, `graph_construction/cpu_admission.rs`)
  is a counting limit on parallel construction lanes, with cancellable waits and
  leases released on drop.
- Each `GraphForge` instance builds one, sized
  `construction_cpu_limit = compute_threads - construction_cpu_reserve`. When
  `compute_threads` is 1, the limit is 1. Every construction session the
  instance opens is attached to it.
- **Finish-time partition loads** lease their worker count from it. A partial
  grant runs fewer workers and publishes the same bytes and evidence.
- **Import normalization** leases its lanes for each flush on the calling
  thread, then maps at most that many batches at once on the compute pool. No
  pool worker ever blocks waiting for a lane.
- `construction_cpu_reserve` is a resource-policy field. It must be at least
  one and below `compute_threads`. Diagnostics report the reserve, the limit,
  the lanes in use and the peak.

### How this realises "all work draws from the budget"

Query kernels are not admission-gated. They keep running on the instance's
`compute_threads` pool, because each kernel splits its work into
`compute_threads` chunks. Letting a kernel's width vary with load would change
its chunking, and with it the order of floating-point reductions. The budget is
therefore shared from the construction side. Construction, across every
concurrent import, never uses more than `compute_threads - reserve` lanes.
Normalization runs on the query pool, so at least `reserve` of the pool's
threads stay free for queries. Finish-time loads run on their own threads and
count against the same limit.

## Predeclared measurement (recorded before any timed run)

The harness is `crates/graphforge-api/src/import_session/cpu_budget_report.rs`,
an ignored test run in a release build. For each configuration, a fresh
instance opens one project that holds a committed base graph (400,000 edges).
It runs a PageRank `rank` workload alone, then again while two imports of
2,000,000 edges each validate concurrently on the same instance. The imports are
aborted, so the project never changes.

| Setting | Values |
| --- | --- |
| Compute threads | 4, pinned to CPUs 0–3; 8, pinned to CPUs 0–7 |
| Configurations | `unbounded` (a 64-lane admission, how construction behaved before #1586); reserve 1; reserve `T/4`; reserve `T/2` (deduplicated) |
| Rounds | 3, configuration order rotated each round |
| Host | Quiet host required before each run and checked after it |
| Metrics | Query p50, p95 and max, alone and during imports; each import's validate wall time; admission peak and limit |

**How the default reserve is chosen.**
- The default stays `max(1, compute_threads / 4)` only if, at 8 compute threads,
  reserve 2 gives a lower median of the per-round query p95 during imports than
  reserve 1, with non-overlapping round ranges.
- Otherwise the default becomes 1, the smallest reserve ADR 0047 allows, which
  costs imports least.
- The import-time cost of each reserve relative to `unbounded` is reported. It
  is not a gate.

**Correctness checks.**
- In every bounded run, the admission peak is at most the limit.
- The `unbounded` peak shows how many lanes two concurrent imports ask for.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Implementation and predeclared measurement, before any timed run. |
| 2026-09-25 | Harness fix before any valid run: the first attempt panicked at setup because its edge IDs were not UUIDv7 and its imports reused the base graph's node IDs. Each input now has its own UUIDv7 identity space. Nothing was measured; the protocol is unchanged. |
