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
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks) |

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
(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and connected
components (#518) may partition independent work across that pool above
documented crossovers; work never uses Rayon's process-global pool. Cosine dot
products retain serial coordinate order, PageRank keeps canonical contribution
order with serial dangling/delta reductions, Node2Vec skip-gram training stays
serial, and components assigns final community IDs in canonical node order, so
fingerprints match the one-thread path.

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

## Parallel components (#518)

`cluster(by="components")` partitions independent source-node adjacency scans
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `COMPONENTS_PARALLEL_CROSSOVER_EDGES` (`16_384`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the serial
union-find path runs with no pool scheduling tax. Parallel workers build
worker-local union-find parents over deterministic source chunks; the kernel then
merges those local parents into one global union-find and assigns public
community IDs serially in canonical node order. Schemas, row order, community ID
assignment, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

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
