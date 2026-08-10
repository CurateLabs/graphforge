# Embedded Execution Resource Policy (#337)

GraphForge exposes one Rust-owned, per-instance
[`ExecutionResourcePolicy`](../../crates/graphforge-api/src/resource_policy.rs)
on [`GraphForgeOptions::resource`](../../crates/graphforge-api/src/write_modes.rs).
The facade normalizes and validates the policy **before** creating the long-lived
Tokio runtime or any DataFusion execution session.

## What it covers

| Knob | Default (Explicit) | Applied to |
|---|---|---|
| `tokio_worker_threads` | `2` | Facade multi-thread Tokio runtime |
| `target_partitions` | `2` | DataFusion `SessionConfig` |
| `batch_size` | `8192` | DataFusion `SessionConfig`; analyst Arrow shaping / property enrichment (#341) |
| `memory_budget_bytes` | `512 MiB` | DataFusion `RuntimeEnv` memory pool |
| `spill` | disabled | Optional absolute spill directory + byte cap |
| `io_concurrency` | `2` | Reserved I/O concurrency budget |
| `max_concurrent_heavy_queries` | `64` | Instance-owned admission semaphore |
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #535 Jaccard similarity; #504 clustering coefficient; #515 triangles; #506 Degree; #501 betweenness; #518 Components) |
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #507 eigenvector) |

Defaults preserve pre-#337 fixed two-worker / two-partition behavior.

## Modes

- **Explicit** — caller-supplied knobs (omitted fields use the defaults above).
- **Automatic** — derive a bounded configuration from
  `std::thread::available_parallelism()`:
  - ≤2 logical CPUs → serial/minimal (`1` worker, `1` partition)
  - otherwise → `ceil(cpus/2)` clamped to `[1, 8]`

Automatic records the selected values on
[`NormalizedResourcePolicy`](../../crates/graphforge-api/src/resource_policy.rs)
(`observed_logical_cpus`, workers, partitions). Timing remains hardware-specific
evidence, never a CI threshold.

## Validation (fail closed)

Normalization returns structured [`GfError::Validation`](../../crates/graphforge-core)
(or `GF_RESOURCE_LIMIT` for admission) when:

- thread counts are outside `1..=256`
- combined Tokio + partition concurrency exceeds the machine-relative budget
  (`min(max(4, 2×cpus), 512)`)
- reserved `io_concurrency` / `compute_threads` exceed
  `max(tokio_workers, observed_cpus)`
- spill is enabled without an absolute, non-symlink directory
- spill directory/max_bytes are set while spill is disabled
- memory / batch / heavy-query bounds are out of range

Invalid settings never partially construct a runtime or authoritative spill
state.

## Application

1. `GraphForgeOptions::validate` → `(options, NormalizedResourcePolicy)`
2. Facade stores the normalized policy + `HeavyQueryAdmission`
3. `build_runtime(&policy)` sets Tokio worker threads
4. Every DataFusion `ExecutionSession` receives a
   [`SessionResourceConfig`](../../crates/graphforge-exec/src/lib.rs) with
   partitions, batch size, memory, optional spill, and `io_concurrency`
5. Heavy ops (`run_query`, streams construction, `rank`, `similar`,
   `analyze_embedding`) take an admission permit
6. Query-facing Parquet scans (`GraphForgeParquetExec`, #339) defer file I/O to
   `ExecutionPlan::execute`, emit batches sized from `batch_size`, declare
   natural file fragments, and acquire the session `io_concurrency` semaphore
   before opening each fragment

Resources are **instance-owned**, not process-global. `compute_threads` sizes a
private Rayon pool on each `GraphForge` instance. Exact cosine KNN / similarity
(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), exact
Jaccard node similarity (#535), local clustering coefficient (#504), triangle
ranking (#515), Degree (#506), betweenness Brandes source searches (#501), and
`cluster(by="components")` (#518) may partition independent work across that
pool above documented crossovers; work never uses Rayon's process-global pool.
Cosine dot products retain serial coordinate order, PageRank keeps canonical
contribution order with serial dangling/delta reductions, Jaccard retains
serial candidate order per source, clustering coefficient merges node-range
scores in canonical dense-ordinal order, triangles merge node-owned counts by
ascending dense ordinal, Degree merges node chunks in dense ordinal order,
betweenness reduces per-source dependency arrays in canonical source order,
Components merges worker-local forests in canonical source-range order, and
Node2Vec skip-gram training stays serial, so fingerprints match the one-thread
path.
(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
eigenvector destination updates (#507) may partition independent work across
that pool above documented crossovers; work never uses Rayon's process-global
pool. Cosine dot products retain serial coordinate order, PageRank keeps
canonical contribution order with serial dangling/delta reductions, eigenvector
keeps per-destination incoming contribution order with serial norm/convergence
reductions, and Node2Vec skip-gram training stays serial, so fingerprints match
the one-thread path.

## Parallel cosine KNN (#342)

Exact, all-score, and filtered cosine partition **independent source rows**
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated multiply-adds (`sources × candidates × dimensions`, or the filtered
  candidate total × dimensions) are at least
  `COSINE_PARALLEL_CROSSOVER_OPS` (`32_768`) in `graphforge-exec`. That
  threshold is the smallest power-of-two multiply-add count at/above the measured
  win boundary on the M4 agent host (4 vCPU, adversarial fixture, 4 private
  workers, release build): ~16k ops still tax serial, ~36k ops first clear win,
  ≥65k ops ≥2×. Smaller workloads stay serial. Worker-local checkpoint counters
  avoid shared atomic contention on the multiply-add path.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Inner floating-point reductions
stay serial per source→target pair (no parallel dot products, no approximate
similarity). Worker outputs merge in canonical source order so schemas, row
order, scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel Jaccard node similarity (#535)

Exact and filtered `similar(by="node_similarity")` Jaccard partition
**independent source rows** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated source-degree candidate probes (source neighborhood size × candidate
  count, summed over non-empty sources) are at least
  `JACCARD_PARALLEL_CROSSOVER_OPS` (`16_384`) in `graphforge-exec`. That
  threshold is the smallest power-of-two probe count below the first stable win
  boundary on the M4 agent host (4 vCPU, adversarial set fixture, 4 private
  workers, release build): ≤4.3k probes are noise-dominated, ~17k probes first
  stable win (~0.65× serial), ≥48k probes ≥2×. Smaller workloads stay serial.
  Worker-local checkpoint counters avoid shared atomic contention on the
  candidate path.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Each worker preserves the existing
serial behavior for candidate validation, duplicate suppression, set
intersection, top-k ordering, and tie-breaking. Worker outputs merge in
canonical source order so schemas, row order, scores, ties, and fingerprints
match the one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel PageRank (#343)

PageRank destination updates run on the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `PAGERANK_PARALLEL_CROSSOVER_EDGES` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial source-scatter path runs with no pool scheduling tax. Parallel work is
destination-owned: each worker owns a contiguous dense-ordinal destination
range and applies inbound contributions in the same canonical source/edge order
as serial scatter. Dangling-mass and L1 convergence reductions stay serial so
IEEE accumulation order matches the accepted oracle. Schemas, row order,
scores, iteration counts, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel clustering coefficient (#504)

Local clustering coefficient partitions **independent dense-ordinal node
ranges** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated local neighbor-pair probes are at least
  `CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK` (`32_768`) in
  `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial node-order path runs with no pool scheduling tax. Each worker computes
complete node-local triangle/wedge scores using the same canonical directed
simple adjacency and the same per-node integer arithmetic as the one-thread
path. Worker outputs merge by ascending node range, so schemas, row order,
scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations. The crossover is a conservative
structural cutoff for embedded CPU work; timing remains hardware-specific
evidence, not a universal scale claim.

## Parallel triangles (#515)

`rank(by="triangles")` normalizes the selected graph to the same simple
undirected neighbor lists as the serial path, then partitions **node-owned
triangle counts** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected node count is at least `TRIANGLES_PARALLEL_CROSSOVER_NODES` (`256`)
  in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
original serial nested-neighbor path runs with no pool scheduling tax. Parallel
workers own contiguous dense-ordinal node ranges, keep each node's neighbor-pair
scan serial, and return worker-local score vectors. Results merge in ascending
range order, so schemas, row order, counts, and fingerprints match the
one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel Degree (#506)

Normalized degree scores partition **independent dense node ordinals** across
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected node count is at least
  `DEGREE_PARALLEL_CROSSOVER_NODES` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Workers emit `(uuid, score)` rows
for contiguous ordinal ranges; the sink merges them in ascending node order so
schemas, row order, scores, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.
## Parallel eigenvector (#507)

`rank(by="eigenvector")` shifted power-iteration destination updates may run on
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `EIGENVECTOR_PARALLEL_CROSSOVER_EDGES` (`8_192`) in `graphforge-exec`.

Release-mode local evidence on the 4-worker M4 agent host showed regular graphs
that converge during warm-up avoid the pool, while irregular non-converged
fixtures crossed over by ~8K selected adjacency entries:
`8_689` edges `1.57ms → 0.87ms`, `24_440` edges `2.89ms → 1.43ms`,
`65_505` edges `9.90ms → 4.25ms`, and `130_544` edges
`14.56ms → 7.26ms` (one thread vs four private workers; hardware-specific
timing, not a CI gate).

Below that crossover, or when the policy provides one compute thread, the
serial source-scatter path runs with no pool scheduling tax. Above the
crossover, the first two required power iterations also stay serial; if the
workload converges there, no inbound CSR or worker scheduling tax is paid.
Remaining non-converged work is destination-owned: each worker owns a
contiguous dense-ordinal destination range and applies inbound contributions
after the implicit identity term in the same canonical source/edge order as
serial scatter. L2 normalization and component-wise convergence checks stay
serial, so scores, iteration counts, and fingerprints match the one-thread
result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel Node2Vec walk generation (#344)

Walk-corpus construction partitions **independent `(start ordinal, walk
ordinal)` tasks** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated transitions (`starts × walks_per_node × walk_length`) are at least
  `NODE2VEC_WALK_PARALLEL_CROSSOVER` (`256`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Worker-local token counts merge
by canonical node ordinal with checked addition. Training order and arithmetic
are unchanged, so schemas, row order, metadata, and fingerprints match the
one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel betweenness Brandes BFS (#501)

`rank(by="betweenness")` partitions independent Brandes **source** searches
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated source-search work
  (`selected_nodes × (selected_nodes + selected_adjacency_entries)`) is at least
  `BETWEENNESS_PARALLEL_CROSSOVER_WORK` (`65_536`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial source loop runs with no pool scheduling tax. The crossover is the
smallest power-of-two work estimate at/above the measured win boundary on the
M4 agent host (4 vCPU, directed chord fixture, 4 private workers, release
build): sub-32k source-search work remains noise dominated, while fixtures above
64k first amortize pool scheduling.

Each worker runs a complete serial Brandes BFS/dependency pass for every source
it owns; no individual BFS frontier, predecessor list, or dependency array is
parallelized. Worker outputs carry one dependency contribution array per source.
The caller replays cooperative checkpoints and accumulates those arrays in
ascending source ordinal order, matching the one-thread floating-point addition
order. Worker loops use cancellation checks rather than shared checkpoint
mutation; cancellation and limit failures remain structured. Schemas, row order,
scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel Components (#518)

`cluster(by="components")` partitions **independent source-node adjacency scans**
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected direction-expanded adjacency entries are at least
  `COMPONENTS_PARALLEL_CROSSOVER_EDGES` (`16_384`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the serial
union-find path runs with no pool scheduling or worker-local map setup cost. The
crossover is the measured M4 boundary where chunk-local union-find begins to
amortize Rayon scheduling and deterministic merge overhead on multi-component
fixtures.

Parallel work is source-range-owned: each worker builds a local min-root
union-find forest for the edges whose source ordinal falls in its chunk, then the
main thread merges those local links by ascending source range. Final component
IDs are still assigned by canonical node order, so schemas, row order, labels,
and fingerprints match the one-thread result at `1`/`2`/`4`/`8`/automatic
configurations. Cancellation remains cooperative both before worker launch and
inside chunk scans.

## Observability

`GraphForge::resource_policy()` and `GraphForge::resource_diagnostics()` expose
safe aggregates: mode, workers, partitions, batch size, memory, spill flag,
I/O / compute budgets, heavy-query limit and available permits, and observed
CPUs. Diagnostics never include query text, parameters, UUIDs, properties,
graph contents, or local project paths.

## Bindings

Python and Node keep existing construction defaults via
`..Default::default()` / `resource: defaults.resource`. No new public binding
knobs are required for #337.

## Thread-parity matrix

The M4 harness executes contract cells `threads-1` / `2` / `4` / `8` /
`threads-automatic` through the public Rust facade under this policy:

```bash
export CARGO_TARGET_DIR=/tmp/gf-m4-337-target
cargo test -p graphforge-api --test m4_entry_baseline \
  thread_parity_matrix_executes_under_resource_policy -- --nocapture
make m4-entry-matrix-check
```

Configurations that exceed the host concurrency budget are recorded
`unavailable` with a reason; results are never fabricated. Executed cells must
share schemas, ordering, fingerprints, structured error codes, and LIMIT row
counts.

## Related docs

- [M4 Entry Baseline](m4-entry-baseline.md)
- [Scale Limits](../reference/scale-limits.md)
- Contract: [`tests/contracts/m4-entry-matrix.json`](../../tests/contracts/m4-entry-matrix.json)
