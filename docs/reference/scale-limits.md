# GraphForge Scale Limits

**Last updated:** 2026-08-09

GraphForge is designed for research and notebook workflows on
[GSI](graph-scale-index.md) Levels **01–06** (`XS`–`MD`, V &lt; 10M). This
document describes practical limits on the v0.5.0 Rust core, distinguishes
between query types, and explains why **edge count matters more than node
count** for most operations. Profile concrete datasets with a full Graph Scale
Index (for example `GD-06-MD-D00`). Do not compare wall-clock numbers across
machines without matching hardware and graph layout.

With DataFusion over Parquet, large-graph work is **disk-limited** (RAM for
working sets). Escalation past Levels 01–06 (Graph500 SCALE ≥ 24 / GSI `07`+)
is a **spec + external harness** track — see
[Scale Evaluation](scale-evaluation.md) (Official Graph500 + Derived density
matrix + harness contract) and the [LDBC full suite](../guide/datasets/ldbc.md)
— not normal GraphForge CI.

## Rust 0.5.0 Fixed-Hop LIMIT Contract

Fixed one- and multi-hop patterns use the adjacency provider in every ontology
mode, for typed and wildcard relationships, whether the persistent index is a
hit, miss, or still absent. `ExpandExec` streams input batches and accepts
DataFusion's physical `fetch`. Chained hops additionally receive a query-scoped
soft batch goal through a fail-closed physical-plan whitelist. This removes
eager round-robin buffering below selective filters and cancels upstream reads
when terminal demand is met. Hard limits still do not cross filters or
relationship uniqueness; ordering, aggregation, and `DISTINCT` remain blocking
and consume their complete semantic input.

For canonical dense node files, filtered hydration proves from Parquet
row-group and page metadata that `node_id = row ordinal + 1`, then selects the
exact requested rows. Scattered destination ids therefore remain
neighborhood-proportional as the node table grows. Deleted/gapped or
index-less files retain the conservative predicate reader with post-read key
validation.

The CI gate executes through the public `GraphForge` facade on deterministic
graphs whose edge count differs by 10x. It requires the larger graph to
materialize no more than 3x as many edge or node rows for the same `LIMIT 1000`,
with zero full edge reads on an adjacency hit. Wall-clock is reported but not
gated. The release command is:

| Deterministic graph | One-hop `LIMIT 1000` | Two-hop `LIMIT 1000` | Materialized edge rows |
|---:|---:|---:|---:|
| 1M edges | 9.76 ms | 13.08 ms | 1,000 / 1,288 |
| 10M edges | 113.39 ms | 111.35 ms | 1,000 / 1,288 |

These are warmed Apple Silicon development measurements; use them to verify
shape, not as a cross-machine service-level objective.

On LiveJournal (4.0M nodes / 34.7M edges), the release build measured
66.3 ms for one hop and 90.3 ms for two hops at `LIMIT 1000`, with no full node
or edge reads and no read starting after cancellation. One-hop selected and
scanned 968 node rows, down from 3,080,458; two-hop selected and scanned 1,946,
down from 5,964,042. No derived metadata is built or refreshed, and project
storage size is unchanged.

```bash
make bench-fixed-hop-limit

GF_LIVEJOURNAL_PROJECT=/path/to/cached/project \
  make bench-fixed-hop-livejournal
```

See [Traversal Scaling](https://github.com/CurateLabs/graphforge/blob/main/benchmarks/traversal_scaling.md)
for the fixed-hop and variable-length benchmark methodology.

## M4 Embedded Performance Entry Gate

M4 before/after performance work uses the versioned entry contract in
[`tests/contracts/m4-entry-matrix.json`](../../tests/contracts/m4-entry-matrix.json)
and the public-facade harness documented in
[M4 Entry Baseline](../development/m4-entry-baseline.md). The short CI matrix
gates on structural correctness under the default Explicit two-worker resource
policy; thread configurations `1`/`2`/`4`/`8`/automatic are executed under
[Embedded Execution Resource Policy](../development/execution-resource-policy.md)
(#337) when the machine budget allows. Lower-level 8M-node/128M-edge reports
remain discovery evidence until #338 proves the public path.

```bash
make m4-entry-matrix-check
cargo test -p graphforge-api --test m4_entry_baseline
make bench-m4-entry
```

---

## Why Edge Count, Not Node Count

Framing scale as “N million nodes” is misleading: the real ceiling depends on
what you are doing and how many **edges** you have. Full-scan aggregations and
global sorts are edge- or cardinality-bound; LIMIT-respecting traversal is not.

Prefer the fixed-hop LIMIT contract above for interactive notebook work. Treat
full-scan aggregations and unconstrained `ORDER BY` as separate, tighter
ceilings.

---

## Structural Approach (v0.5.0)

| Concern | v0.5.0 approach |
|---|---|
| Edge counting | Columnar `COUNT(*)` on edge facts / Parquet |
| Top-N ordering | DataFusion top-N physical node |
| Bulk ingest | Parquet write via Arrow RecordBatch |
| Neighborhood expansion | Derived CSR adjacency index under `indexes/adjacency/` |
| Memory layout | Compact columnar Parquet, not per-edge Python objects |

---

## Practical Size Guidance

| Use case | Guidance |
|----------|----------|
| Interactive traversal (`LIMIT`) | Prefer fixed-hop patterns; measured through tens of millions of edges on the release benches above |
| Full-scan aggregation | Expect edge-count binding; validate on your hardware |
| Global `ORDER BY` | Prefer top-N / `LIMIT` forms |
| Project sharing | Parquet project directory — reopen through `GraphForge(path)` |

---

## Release load matrix coverage

The [standardized release load matrix](../development/release-load-matrix.md)
exercises public Rust, Python, and Node surfaces across synthetic size and
density classes. It proves **correctness and operational envelope** inside the
small-to-medium posture above — load, lifecycle ops, cleanup, and reopen — not
the fixed-hop LIMIT wall-clock numbers in this document.

Authoritative size, density, and topology IDs live in
`tests/contracts/load-dataset-taxonomy.json`. Summary mapping:

| Scale-limits claim | What the matrix proves | Scenario classes |
|---|---|---|
| Small-to-medium notebook graphs | Facades load complete fixtures and finish inside per-size resource bounds (RSS, persisted/temporary bytes, hang timeout) | XS–XL (all datasets) |
| Edge count matters more than node count | At similar node counts, dense fixtures raise live-edge cardinality; sparse fixtures keep edges low while topologies vary | Sparse vs dense pairs at each size |
| Neighborhood / adjacency-shaped work | Hub-heavy and path-heavy sparse graphs stress uneven degree and path structure without claiming LIMIT latency | `*-sparse-hub`, `*-sparse-path` |
| Edge-heavy / denser workloads | Dense clustered and cyclic graphs maximize edges for the size class (ops correctness, not aggregation SLOs) | `*-dense-clustered`, `*-dense-cyclic` |
| Project sharing via reopen | Every case closes and reopens the project with fail-closed reopen equivalence | All 144 cases |
| Fixed-hop `LIMIT` shape and benches | **Out of scope** for this matrix — use the LIMIT contract and benches above | Separate release benches |

| Size | Node band (taxonomy) | Sparse datasets | Dense datasets |
|------|----------------------|-----------------|----------------|
| XS | 16–31 | disconnected, path-heavy | clustered, cyclic |
| S | 64–127 | hub-heavy, path-heavy | clustered, cyclic |
| M | 256–511 | disconnected, hub-heavy | clustered, cyclic |
| L | 1024–2047 | clustered | cyclic |
| XL | 4096–8191 | hub-heavy | clustered |

Accepted same-SHA case results land on
[Release Load Matrix Results](load-matrix-results.md) once CI produces the
artifact. Until then that page stays an explicit pending placeholder.

---

## Further Reading

- [Graph Scale Index (GSI)](graph-scale-index.md) — size axis (node band + density)
- [Scale Evaluation](scale-evaluation.md) — Official Graph500 + Derived density matrix; harness contract
- [LDBC full suite](../guide/datasets/ldbc.md) — SNB / Graphalytics / FinBench / SPB (spec; execution external)
- [Install footprint](../guide/installation.md#install-footprint) — download and on-disk package sizes for Python/Node (not query scale)
- [Release Load Matrix Results](load-matrix-results.md) — evidence landing for accepted matrix runs
- [Standardized Release Load Matrix](../development/release-load-matrix.md) — contracts, executor, reproduce
- [GitHub Releases](https://github.com/CurateLabs/graphforge/releases) — release notes
- [Architecture Overview](../book/architecture/overview.md) — Rust core design and DataFusion execution model
