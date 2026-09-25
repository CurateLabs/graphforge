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

## Results

Two clean passes, 2026-09-25: 4 compute threads on CPUs 0–3 from 02:22:51 to
02:27:54 UTC, then 8 compute threads on CPUs 0–7 from 02:28:56 to 02:35:26 UTC.
Both started after 60 s of sustained quiet with no peer measurement process
running, and the host was quiet after each. The measured test binary's SHA-256
and commit are in `construction-cpu-budget-1586/binary.sha256`. Raw
observations: `construction-cpu-budget-1586/observations.jsonl`, one line per
configuration and round.

Medians of the three rounds, with the observed range. "During" is the p95 of
the PageRank workload while two imports validated concurrently, about 105–114
queries per observation. "Import" is the slower of the two imports' validate
wall times.

| Compute threads | Configuration | Limit | Peak lanes | Query p95 alone (ms) | Query p50 during (ms) | Query p95 during (ms) | Import (s) |
| ---: | --- | ---: | ---: | ---: | ---: | --- | --- |
| 4 | unbounded | 64 | 8 | 259 | 262 | 306 [298–316] | 29.9 [29.7–30.2] |
| 4 | reserve 1 | 3 | 3 | 251 | 268 | 315 [294–322] | 30.3 [30.2–30.4] |
| 4 | reserve 2 | 2 | 2 | 256 | 265 | 295 [288–300] | 30.2 [29.7–30.5] |
| 8 | unbounded | 64 | 8 | 264 | 259 | 304 [298–312] | 29.2 [28.9–29.5] |
| 8 | reserve 1 | 7 | 7 | 275 | 265 | 315 [304–322] | 29.3 [29.0–29.3] |
| 8 | reserve 2 | 6 | 6 | 264 | 262 | 307 [306–313] | 29.3 [28.8–29.5] |
| 8 | reserve 4 | 4 | 4 | 249 | 258 | 302 [302–311] | 28.8 [28.7–28.9] |

### What the numbers show

- **The limit holds and binds.** In every bounded run the peak equals the
  limit. Two concurrent imports ask for 8 lanes, which the unbounded peak shows.
- **The reserve changed nothing measurable here.** Query p95 during imports
  overlaps across every configuration at both thread counts. Every import took
  between 28.7 s and 30.5 s, whether the imports shared 2 lanes or 8. On this
  workload, construction lanes are not what bounds either queries or
  imports. Each import validates in about 30 s, whatever its lane count.
- **So this measurement does not show the reserve protecting queries.** It
  shows the budget holding at no measured cost. Its value is as a bound for
  work that does contend, such as #1448's partition-level shaping lanes, which
  will draw from the same admission.

### Default reserve

By the predeclared rule: at 8 compute threads, reserve 2's median query p95
during imports (307 ms, range 306–313) is not below reserve 1's (315 ms, range
304–322) with non-overlapping ranges. **The default reserve is therefore 1**,
the smallest ADR 0047 allows. `default_construction_cpu_reserve` returns 1, and
the limit defaults to `compute_threads - 1`.

### Discarded attempts

Three attempts before the clean passes are kept under
`construction-cpu-budget-1586/discarded/` and excluded from every figure (see
changelog):

- a setup panic, before anything was measured;
- two drivers running at once;
- a pass that overlapped a #1585 ingest.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Implementation and predeclared measurement, before any timed run. |
| 2026-09-25 | Harness fix before any valid run: the first attempt panicked at setup because its edge IDs were not UUIDv7 and its imports reused the base graph's node IDs. Each input now has its own UUIDv7 identity space. Nothing was measured; the protocol is unchanged. |
| 2026-09-25 | Discarded attempt: a second driver left over from an earlier start ran its 8-thread pass at the same time as the new driver's 4-thread pass, on overlapping CPUs, and the driver's peer check used the wrong process name (`graphforge_api` instead of `graphforge_api-`). Both processes were killed after two observations each; those observations are kept as contaminated and excluded. The driver now holds a lock so only one can run. |
| 2026-09-25 | Clean 4- and 8-thread passes (02:22–02:35 UTC); results and the default reserve recorded. |
| 2026-09-25 | Discarded attempt: the 4-thread pass (01:21:26–01:26:36 UTC) overlapped a #1585 star-graph ingest on the same host that started four seconds later; the quiet guard cannot see either process. The pass is kept as contaminated and excluded. Timed runs from the two efforts are now serialized: #1586 runs only after #1585 reports its timed runs complete. |
