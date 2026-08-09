# M4 Entry Baseline (#334)

**Contract:** [`tests/contracts/m4-entry-matrix.json`](../tests/contracts/m4-entry-matrix.json)  
**Harness:** `cargo test -p graphforge-api --test m4_entry_baseline`  
**Owner issues:** entry #334 · resource-policy parity #337 · public persistence #338 · exit #345 · epic #335  
**Resource policy:** [Embedded Execution Resource Policy](execution-resource-policy.md)

This is the accepted **before/after** evidence source for every M4 implementation
issue. Later PRs cite this gate for structural before/after comparisons.
Wall-clock numbers are hardware-specific observations, never CI pass/fail
thresholds.

## What this gate measures

Public Rust facade workloads under the **default Explicit** two-worker /
two-partition resource policy (preserving pre-#337 semantics), plus executed
thread-parity cells when the machine budget allows:

| Workload id | Class | Surface |
|---|---|---|
| `fixed-hop-limit` | fixed-hop `LIMIT` | `GraphForge::execute` |
| `scan-count` | full scan / count | `GraphForge::execute` |
| `aggregate-top-n` | aggregate / top-N | `GraphForge::execute` |
| `pagerank` | iterative algorithm | `GraphForge::rank` |
| `exact-cosine-knn` | exact vector search | `GraphForge::similar` |
| `node2vec` | embedding workload | `GraphForge::analyze_embedding` |

## Configurations

| Configuration | Status | Notes |
|---|---|---|
| Default Explicit two-worker / two-partition | **supported** | `GraphForgeOptions::default().resource` |
| Requested `1` / `2` / `4` / `8` / `automatic` | **supported via #337** | Executed by the harness when within budget; otherwise recorded `unavailable` |

#337 owns proving schema / ordering / fingerprint / errors / cancellation /
resource-limit parity across those cells under
[`ExecutionResourcePolicy`](execution-resource-policy.md).

## Matrices

### Short CI (required)

```bash
make m4-entry-matrix-check
cargo test -p graphforge-api --test m4_entry_baseline -- --nocapture
```

Pass/fail uses structural gates, determinism, and thread-parity fingerprints.
Timing and peak RSS are printed as observations. Absolute millisecond
thresholds, sleeps, retries, and ignored correctness assertions are forbidden.

### Large manual / scheduled

```bash
make bench-m4-entry
# optional evidence file:
GF_M4_ENTRY_EVIDENCE_OUT=build/m4-entry-evidence.json make bench-m4-entry
```

Reuses documented 1M/10M-edge and LiveJournal paths from the fixed-hop benches;
CI must not download external fixtures.

```bash
make bench-fixed-hop-limit
GF_LIVEJOURNAL_PROJECT=/path/to/project make bench-fixed-hop-livejournal
```

## Metrics

**Structural gates:** output rows, schema fields, result fingerprint,
I/O / demand counters where applicable, configuration classification.

**Hardware-specific observations:** wall time, peak RSS (`VmHWM` on Linux),
CPU model, logical CPUs, OS, memory, accelerator identity when present.

**May be unavailable:** spill bytes (not yet universally instrumented), observed
DataFusion target partitions, thread-parity cells over the host budget.

## Honest bottlenecks retained

The entry baseline intentionally records the current implementation, including
eager Parquet materialization, single-partition custom plans, serial algorithm
kernels, and CSR-to-map conversion where still observable. Optimizations belong
to later M4 issues.

## Discovery evidence (not a public baseline)

The lower-level **~8M-node / ~128M-edge** local scale report is classified as
`discovery_not_public_facade_baseline`. It does not traverse the current public
snapshot path. Public-product claims at that scale require #338 and public-facade
reruns (#345).

## Citation for M4 implementation issues

Before/after evidence source:

- Contract: `tests/contracts/m4-entry-matrix.json` (`graphforge-m4-entry-matrix/1`)
- Docs: this page, [`execution-resource-policy.md`](execution-resource-policy.md),
  and [`benchmarks/m4_entry_baseline.md`](../benchmarks/m4_entry_baseline.md)
- Harness: `crates/graphforge-api/tests/m4_entry_baseline.rs`

Every M4 child (#336–#344) should compare against this gate’s structural
contract. #345 reruns it as exit evidence on the final tree.
