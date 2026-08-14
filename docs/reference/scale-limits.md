# GraphForge Scale Limits

**Last updated:** 2026-08-09

GraphForge is designed for research and notebook workflows on
[GSI](graph-scale-index.md) Levels **01–06** (`XS`–`MD`, V &lt; 10M). This
document describes practical limits on the v0.5.0 Rust core, distinguishes
between query types, and explains why **edge count matters more than node
count** for most operations. Profile concrete datasets with a full Graph Scale
Index (for example `GD-06-MD-D00`) via [`profile_gsi`](api.md#profile_gsi--graphscaleindexprofile)
or the [GSI reference](graph-scale-index.md). Do not compare wall-clock numbers
across machines without matching hardware and graph layout.

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
(#337) when the machine budget allows.

### Graph persistence envelopes

| Path | What it stores | Open behavior | Size guidance |
|---|---|---|---|
| Legacy `graph`/`snapshot` (Arrow IPC) | Whole workspace bytes in one participant | Hydrates every file into a private workspace | Historical envelope: 1 GiB/file and 2 GiB total. Still readable. Do not raise these constants. |
| File-backed `graph`/`files` + generation `graph/` tree | Canonical inventory participant; graph files remain on disk | Validates inventory; read-only opens may pin the generation tree; writers materialize file-by-file | No universal GiB ceiling. Public reopen past the legacy 2 GiB snapshot envelope is proven by oversize file-backed evidence (#338 / #345). Full 8M/128M densified public-facade reruns remain optional scale-host measurements under local resource stops — not a CI product max. CI uses a small multi-file fixture. |

New publications use the file-backed path. Portable interchange currently returns a
structured unsupported error for file-backed trees (copy the project directory
instead); legacy snapshot generations remain portable.

Public persistence past the legacy 2 GiB snapshot envelope is proven by the
ignored oversize fixture in `file_backed_graph_generation` (sparse padding beside
a queryable graph; checked-in evidence:
[`file-backed-oversize-evidence.json`](../development/file-backed-oversize-evidence.json)).
That is not a universal size ceiling and does not download 8M/128M data in CI.

```bash
make m4-entry-matrix-check
cargo test -p graphforge-api --test m4_entry_baseline
cargo test -p graphforge-api --test file_backed_graph_generation
make bench-m4-entry
# Optional large-class persistence proof (ignored; local only):
GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT=build/file-backed-oversize-evidence.json \
  cargo test -p graphforge-api --test file_backed_graph_generation \
  oversize_file_backed_generation_exceeds_legacy_snapshot_envelope -- --ignored --nocapture
```

## Adjacency index build (#336)

Derived CSR adjacency construction streams projected Parquet batches
(`edge_id` / `src_id` / `dst_id`, plus `rel_type_name` for exploratory files)
instead of concatenating each typed edge file into one Arrow `RecordBatch`.
That removes the observed **134,217,727-edge** ceiling caused by concatenating
`FixedSizeBinary(16)` UUID columns into a single contiguous buffer
(2 GiB / 16 bytes).

Peak build memory is governed by an explicit chunk/spill policy
(`AdjacencyBuildOptions`: `chunk_rows`, `batch_size`, optional
`memory_budget_bytes`, `spill_dir`, `spill_max_bytes`), not by total edge
count. Sorted runs spill under the unpublished stage (or a configured absolute
spill directory from the #337 resource policy) and are removed on success,
failure, or cancellation. Manifest-last publication is unchanged: a cancelled
or failed build cannot publish a fresh-looking partial index.

Deterministic CI covers multi-row-group streaming without UUID projection,
tiny-`chunk_rows` golden CSR equality against `csr_from_entries`, and
cancel/spill cleanup. A full **>200M-edge** public-path index build remains an
M4 scale / scheduled evidence run (map to the #334 harness after hardware and
spill configuration are recorded). Do **not** read the former 134M Arrow
boundary as a GraphForge maximum graph size.

| Claim | Status |
|---|---|
| No full-file UUID concat during adjacency build/validate/inspect | Covered by CI streaming seam |
| CSR bytes match scan-build semantics under spill | Covered by tiny-chunk golden tests |
| Cancel/failure leaves prior index or absent/stale | Covered by cancel + spill-cap tests |
| >200M edges indexes on a supported machine | Accepted disposition: pending scale-host / scheduled evidence (#345). Not a product claim on agent hosts. |

Manual/scheduled 8M/128M reproduction (not CI): build or point at the measured
fixture, publish through `GraphForge`, reopen, and record RSS/storage/fingerprint
via the #334 evidence emitter.

## CSR-native execution (#340)

Persisted-index **hits** no longer expand the validated base CSR into
`HashMap<u64, Vec<(edge_id, neighbor_id)>>` for traversal or analyst
projection. Execution keeps:

- directed CSR with checked O(1) row lookup over offsets + parallel
  edge/neighbor columns;
- undirected views as an out+in CSR pair merged **per accessed row**
  (out-before-in on equal `edge_id`), without a full merged hash map;
- delta overlays as a bounded replacement map over only keys touched by the
  delta chain — the complete valid base CSR is retained, not recopied.

Scan-build / missing / stale / corrupt index paths still use the historical
hash-map oracle (or rebuild then serve CSR-native). Structural counters on
`Adjacency` (`backing()`, `base_csr_entries_expanded()`, `overlay_row_count()`)
assert zero base-CSR expansion on a fresh hit. Analyst export builds a
selection-bounded flat CSR of `AlgorithmEdge` entries rather than per-node
heap vectors for every graph edge.

| Claim | Status |
|---|---|
| Fresh index hit: no O(E) HashMap / per-node Vec expansion | Covered by unit structural counter + parity vs scan |
| Out / in / undirected / typed / wildcard semantics preserved | Covered by adjacency + persistent provider tests |
| Bounded delta overlay without full base copy | Covered by storage overlay parity tests |
| Selected-subgraph projection bounded by selection | Covered by export path iterating selected node ids |
| Peak RSS / cold-warm first-use on #334 fixtures | Hardware-specific observation only; recorded in [`m4-exit-evidence.json`](../development/m4-exit-evidence.json). Never a CI pass/fail gate. |
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
