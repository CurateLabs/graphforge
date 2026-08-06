# Bazel migration Cargo/Blacksmith baseline (freeze)

Accepted Blacksmith + Cargo CI baseline for later [#1](https://github.com/CurateLabs/graphforge/issues/1)
performance comparison ([#5](https://github.com/CurateLabs/graphforge/issues/5)).
Owned by [#12](https://github.com/CurateLabs/graphforge/issues/12).

Companion ledger: [bazel-migration-ledger.md](bazel-migration-ledger.md).

## Freeze metadata

| Field | Value |
| --- | --- |
| Baseline freeze date (UTC) | 2026-08-06 |
| Inventory/document SHA | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| Runner family | Blacksmith (`test.yml` jobs) |
| Build system | Cargo (+ maturin / napi packaging) |
| Sample size | 5 successful full-matrix PR runs |
| Metric | Job wall time seconds from GitHub Actions job `started_at`/`completed_at` |

## Accepted sample runs

| Run | SHA | Title | URL |
| --- | --- | --- | --- |
| `31065788285` | `f8d6ee50fa1185b4b7ffda62a8caedf650429500` | fix(explain): side-effect-free write planning (#354) | https://github.com/CurateLabs/graphforge/actions/runs/31065788285 |
| `31065484762` | `0c28132251cdacb288e2ee80a556c03bc2dad9ae` | fix(bdd): bulk add_nodes operation_uuid readback (#355) | https://github.com/CurateLabs/graphforge/actions/runs/31065484762 |
| `31065189044` | `11cc05b894b68b36db8f48395842021f7f4b5ffa` | fix(recipes): neighbourhood hop-bound and schema contracts (#356) | https://github.com/CurateLabs/graphforge/actions/runs/31065189044 |
| `31064495388` | `2c16a425440319c84b539e2625826d221b9fd9e1` | fix(node): preserve TypeError for binding coercion (#357) | https://github.com/CurateLabs/graphforge/actions/runs/31064495388 |
| `31058201474` | `b3cb50c9e8953d26a6cd3c34ac27b56f608dd4ff` | fix(search): align find empty and vector-query contracts (#352) | https://github.com/CurateLabs/graphforge/actions/runs/31058201474 |

## Per-job wall times (seconds)

| Job | `f8d6ee50` | `0c281322` | `11cc05b8` | `2c16a425` | `b3cb50c9` | **p50** |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust Quality | 42 | 42 | 41 | 44 | 57 | **42** |
| Rust Tests | 327 | 308 | 293 | 356 | 386 | **327** |
| Python Binding | 184 | 166 | 177 | 170 | 246 | **177** |
| Node Binding | 120 | 120 | 132 | 121 | 143 | **121** |
| Windows graphforge-storage Locks | 142 | 135 | 133 | 141 | 147 | **141** |
| Concurrency Matrix | 108 | 103 | 110 | 133 | 132 | **110** |

## Compute proxy

GitHub Actions does not expose a single “CPU-seconds” field for all jobs here.
For #1’s “total build compute” comparison, use the **sum of the six job wall times**
above as the accepted Cargo/Blacksmith compute proxy for a representative PR run
(jobs may overlap in wall-clock calendar time; the sum still tracks compile/test work).

| Run SHA | Sum of six job walls (s) |
| --- | ---: |
| `f8d6ee50fa11` | 923 |
| `0c28132251cd` | 874 |
| `11cc05b894b6` | 886 |
| `2c16a4254403` | 965 |
| `b3cb50c9e895` | 1111 |
| **p50** | **923** |

## How #5 must compare

Against this baseline, on paired representative runs (≥10 pairs per #1):

- Warm PR build/test **p50** ≥ 30% faster than the Cargo path job set above
  (primary: `Rust Tests` + binding jobs as defined in the #5 measurement plan).
- Total build compute proxy ≥ 25% lower than the p50 sum above.
- Cold p50 regression ≤ 10% vs cold Cargo/Blacksmith measurements recorded in #5
  (cold Cargo sticky-disk warm starts are **not** cold; #5 must define cold protocol).

## Explicit non-claims

- This document does **not** claim Bazel modeling, remote-cache hits, or cutover.
- Org-admin Blacksmith **Bazel Build Caching** enablement remains a #5 dependency.
- Docs-only PR runs (path-skipped Rust/bindings) are **excluded** from this sample.

