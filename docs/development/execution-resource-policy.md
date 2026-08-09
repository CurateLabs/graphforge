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
| `batch_size` | `8192` | DataFusion `SessionConfig` |
| `memory_budget_bytes` | `512 MiB` | DataFusion `RuntimeEnv` memory pool |
| `spill` | disabled | Optional absolute spill directory + byte cap |
| `io_concurrency` | `2` | Reserved I/O concurrency budget |
| `max_concurrent_heavy_queries` | `64` | Instance-owned admission semaphore |
| `compute_threads` | `2` | Reserved future private CPU-pool budget |

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
   partitions, batch size, memory, and optional spill
5. Heavy ops (`run_query`, streams construction, `rank`, `similar`,
   `analyze_embedding`) take an admission permit

Resources are **instance-owned**, not process-global. `compute_threads` is a
structural reserve for a future private Rayon pool; this issue does **not**
parallelize graph algorithms.

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
