# Execution Model

**Status:** v0.5.0 — Rust core shipped**
**Last Updated:** 2026-07-27

> **Implementation status legend** (used across architecture docs): **Shipped** = implemented and
> tested on `main`; **Partially built** = some paths real, others stubbed; **Designed** = specified,
> not yet a complete public capability. For v0.5.0: the Cypher pipeline (including CREATE,
> variable-length traversal, OPTIONAL MATCH, and UNWIND), the analyst verbs
> (`rank`/`cluster`/`paths`/`analyze`/`similar`/`find`), and knowledge-layer provenance /
> confidence / epistemic records (knowledge-layer + epistemic) are **Shipped**. Per-algorithm catalog detail lives in
> [Algorithm Verbs](algorithms.md).
---

## Overview

GraphForge uses **DataFusion as the primary execution backbone**. DataFusion provides the
logical/physical planning pipeline, optimizer, table providers, and Arrow-native batch
execution. GraphForge extends it with custom physical plan nodes for operations that require
graph-native semantics.

The v0.5.0 unified API exposes seven entry points — `execute`, `rank`, `cluster`, `paths`,
`analyze`, `similar`, and `find` — all producing Arrow Tables. `execute` travels the full
Cypher compiler pipeline (`graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`); the analyst verbs bypass the
parser and planner but converge on the same DataFusion execution layer. All seven entry points
are **Shipped** (see the per-algorithm status in [Algorithm Verbs](algorithms.md)). Their
graph-native execution is a **graph-layer** capability under
[ADR 0005](../../adr/0005-layered-architecture.md). A workbench or knowledge workflow may prepare
their inputs and consume their results, but the graph layer never depends upward on either layer.
Polars is used as a **storage-layer companion** for IO and sinks (CSV, JSON, Parquet, IPC).
It is not the semantic owner of query execution.

---

## Why DataFusion

DataFusion exposes the extension points GraphForge needs:

- Custom `LogicalPlan` nodes — graph operators slot in alongside relational operators
- Custom `ExecutionPlan` nodes — variable-length path expansion and provenance-aware joins as physical operators
- Custom `TableProvider` — Parquet as a pluggable scan source
- Custom optimizer rules — predicate pushdown and label-constraint hoisting
- Custom `QueryPlanner` — GraphForge IR is lowered to DataFusion's logical plan rather than parsed SQL

DataFusion's Arrow-native execution means results already arrive as `RecordBatch` streams —
no additional conversion is needed before returning through the thin Python or Node bindings
(or the native Rust facade).

---

## Execution Pipeline

```text
GraphPlan (Graph IR)
      ↓
┌────────────────────────────┐
│  graphforge-rel: relational lower  │
│  Simple ops → LogicalPlan  │
│  Graph ops → custom nodes  │
└────────────────────────────┘
      ↓
┌────────────────────────────┐
│  DataFusion Analyzer       │
│  DataFusion Optimizer      │
│  (predicate pushdown, etc) │
└────────────────────────────┘
      ↓
┌────────────────────────────┐
│  DataFusion Physical Plan  │
│  gf custom PhysicalNodes   │
└────────────────────────────┘
      ↓
  SendableRecordBatchStream   ← Arrow columnar batches
      ↓
  graphforge-bindings-py / graphforge-bindings-node / Rust API
```

---

## Custom Graph Execution Nodes

GraphForge registers custom `ExecutionPlan` implementations with DataFusion for operators
that cannot be faithfully expressed as relational algebra:

| Node | Description |
| -------------------- | -------------------------------------------------------------------------- |
| `VarLenExpand` | Iterative or recursive expansion for `*min..max` path patterns |
| `OptionalMatch` | Left-join semantics with Cypher null-shaping (distinct from SQL LEFT JOIN) |
| `PathUnique` | Path isomorphism/homomorphism enforcement |
| `ProvenanceSemijoin` | Semijoin with confidence propagation |
| `OntologyInfer` | Transitive/symmetric closure materialization |
| `GraphMerge` | Partial MERGE upsert (standalone new node or referenced-endpoint relationship) with write-path locking |

All other operators (scan, filter, project, aggregate, sort, limit) run through standard
DataFusion physical nodes.

---

## Adjacency-Backed Execution

Traversal is fundamentally adjacency-oriented. GraphForge maintains a derived, rebuildable
**adjacency index** (CSR, surrogate-keyed) under `indexes/adjacency/`
([ADR 0004](../../adr/0004-adjacency-index.md), [storage.md](storage.md) §Derived Indexes). Both
execution paths consume it through a single `AdjacencyProvider` abstraction:

- **Cypher traversal path** — `ExpandExec` / `VarLenExpandExec` ask the provider for a
  node's neighbors instead of rebuilding adjacency from a full edge scan per query.
- **Analyst-verb path** — `export_adjacency` is a thin adapter from the same provider to the
  `AdjacencyGraph` consumed by the `AlgorithmBackend` trait.

| Node | Description |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `VarLenExpandExec` | Iterative BFS over the `AdjacencyProvider` for `*min..max` patterns (replaces the per-query in-memory adjacency build) |
| `ExpandExec` | Adjacency-backed single-hop expansion: chosen at lowering time when the provider reports a `hit` for a typed relation (any direction; undirected wraps in `DISTINCT`, mirroring the join path's union+distinct). Exploratory single-hop and uncovered patterns keep the DataFusion join chain. |

`ExpandExec` receives exact physical column demand through projections, filters,
sorts, and limits; unknown or multi-input physical operators are conservative
materialization barriers. A destination-identity-only hop carries the CSR
neighbor `node_id` directly and resolves `node_uuid` through the facade's exact
generation-pinned v4 ordinal authority. It opens neither edge topology nor
destination-node Parquet. Lookup batches are sorted and deduplicated within the
operator batch, coalesced by the v4 reader, and restored to adjacency order.
There is no graph-sized identity map and no path-based rediscovery of mutable
authority. The facade authenticates and pins the selected immutable generation
once when it creates an execution session. Every hop in that session then reads
through the retained authenticated file handles; it does not repeat a complete
artifact-name/stamp walk for every bounded expansion chunk. A later session
revalidates the names and identities again, so a planted replacement cannot be
adopted as authority.

When relationship or destination-node data is required, filtered readers decode
only the demanded canonical columns plus their join key. Legacy node layouts and
typed wildcard unions retain their normalization path before result shaping.
Aggregate diagnostics report projected columns/chunks/rows and v4 ranges,
coalesced calls, bytes, peak charged buffers, and forbidden per-record seeks;
session revalidation calls/bytes are accounted exactly once rather than hidden
from lookup evidence. Diagnostics never report identities or paths. Global `ORDER BY ... LIMIT` still examines
the complete unordered candidate stream and does not use invalid early
cancellation.

Ordinary streaming result sinks retain the same aggregate evidence through the
terminal stream boundary. `gf --json query` emits `graphforge-result-sink/2`
with nested `graphforge-query-evidence/1`: named hop reader, logical-row,
projection, identity-byte, TopK/spill, memory-release, and operator-RSS fields.
The receipt also includes the SHA-256 of the atomically published result and an
optional `scalar_u64` only for an exact one-row, one-`UInt64` result. Evidence is
content-free: it contains no graph values, identities, paths, or provider names.

**Selection is a planner choice, not an IR change.** The Graph IR is unchanged: variable-length
traversal is still encoded on `Expand { …, min_hops, max_hops }`. A lowering rule selects an
adjacency-backed physical node when the provider covers the relation type + direction, and falls
back to the DataFusion hash-join path otherwise. Both paths produce identical results; only
speed differs.

The index is **optional and never authoritative**: a stale or missing index
falls back to scan-and-build with identical output. The pinned v0.5
project-generation UUID and graph source fingerprint detect staleness; the
committed Parquet graph participant is always the source of truth.

Fresh index hits serve a **CSR-native** adjacency view (#340): validated
offsets with parallel edge/neighbor columns and O(1) row lookup. Undirected
requests keep separate out/in CSRs and merge per accessed row (out before in
on equal `edge_id`) without materializing a full merged hash map. Delta
overlays attach a bounded replacement map over touched keys only — they do
not copy the complete valid base CSR. Scan-built fallback retains the
historical hash-map representation for oracle parity. Analyst
`export_adjacency` projects selected nodes into a flat CSR of algorithm
edges rather than duplicating the full graph into per-node heap vectors.

`explain` annotates each traversal node with `adjacency=hit | miss | building`, so plan
inspection shows whether the accelerator was used (`ExecutionSession::explain_physical`
renders the physical plan without executing it). One `PersistentAdjacencyProvider` lives per
session (= per query) with an interior per-`(relation, direction)` cache, so multi-expand
queries load each CSR once; lifting the provider to the long-lived facade for cross-query
caching is a planned follow-on.

---

## Analyst Verbs

`rank()`, `cluster()`, `paths()`, `analyze()`, `similar()`, and `find()` bypass the Cypher
parser and planner entirely. They accept structured arguments, build their own DataFusion
sub-plans, and return Arrow Tables through the same result contract as `execute()`.

| Method | Bypasses | Uses | Returns |
| ----------------------------- | --------------- | ----------------------------------------------------------- | -------------------------------------------- |
| `rank(label, by=…)` | Parser, planner | Adjacency export → algorithm dispatch → DataFusion project | node properties + `score: Float64` |
| `cluster(label, by=…)` | Parser, planner | Adjacency export → community algorithm → DataFusion project | node properties + `community_id: Int64` |
| `paths(source, target, by=…)` | Parser, planner | Adjacency export → path algorithm → DataFusion project | `source_uuid`, `target_uuid`, `cost`, `path` |
| `analyze(label, by=…)` | Parser, planner | Adjacency export → structural analysis → DataFusion project | varies by algorithm |
| `similar(label, by=…)` | Parser, planner | Adjacency export → similarity → DataFusion project | `node1_id`, `node2_id`, `similarity` |
| `find(query, vector=…)` | Parser, planner | FTS / vector index → RRF fusion → DataFusion project | node properties + `score` + `matched_on` |

Key design facts:

- **No separate result type.** All analyst verbs return Arrow Tables exactly as `execute()` does.
- **`write_property` is opt-in mutation.** Only `rank()` and `cluster()` support it; all other analyst verbs are read-only.
- **`find()` indexes text lazily.** A missing/stale default text index is built once from observed string properties. Vector search selects a complete, compatible embedding-space generation; it never fabricates missing vectors. Substantially stale vector spaces refresh successfully first or fail unless explicitly forced under the [embedding publication contract](embedding-v1.md#source-fingerprint-and-freshness).
- **All paths converge at DataFusion.** Adjacency export and scoring stages produce Arrow data that DataFusion projects and returns as `RecordBatch` streams.

### Embedding publication and search

Embedding production, publication, retrieval, and optional reranking are four
separate stages:

```text
analyst-verb/local/callback/remote/caller batch
        │ produces complete UUID + vector Arrow data
        ▼
private generation ── validate against committed source snapshot
        │ data + manifest, then atomic active-generation swap
        ▼
compatible visible space ── exact vector / text / rrf@1 retrieval
        │ optional explicit bounded reranker
        ▼
canonical Arrow search result
```

Publication never mutates graph properties or an algorithm result. Retrieval checks
durable compatibility and freshness before candidate work. One private build
per space and one coalesced newest pending request prevent refresh storms;
project-wide producer concurrency is bounded. Cancellation or failure exposes
neither partial generations nor partial result rows. Proactive refresh runs
only while a process is open; reopen reconstructs freshness from durable graph
and space metadata. Provider work is explicit and optional, and reranking
cannot silently change canonical `rrf@1` behavior.

### Reproducible algorithm execution

> **Status: implemented across the v0.5.0 algorithm, knowledge, and epistemic layers.**

Algorithm dispatch produces two logically separate values:

1. a canonical Arrow result owned by the selected algorithm contract; and
2. a neutral invocation descriptor containing the algorithm and contract version, resolved
   projection fingerprint, normalized selectors and options, deterministic controls, and
   resource limits.

The descriptor contains no evidence, assertion, reasoning, or belief state. It is sufficient
to identify what the graph layer actually computed. The knowledge layer persists it with a durable
`algorithm_run_uuid` and lineage links. The epistemic layer resolves a point-in-time belief state to an
immutable UUID projection before the invocation, then attach assertions and reasoning to the
run after it completes.

```text
Epistemic belief query (optional)
        │ resolves UUID projection / explicit parameter mapping
        ▼
algorithm graph algorithm ──► canonical Arrow result
        │
        └── neutral invocation descriptor
                    │
                    ▼
           knowledge-layer provenance run record
                    │
                    ▼
      epistemic assertions / evidence / reasoning
```

The same resolved graph input produces the same algorithm result whether or not knowledge/epistemic
capabilities are present. Consequently:

- graph algorithms do not query knowledge tables;
- `algorithm_run_uuid` is knowledge/provenance identity, not graph topology identity;
- confidence, provenance, assertion status, and valid time are not conditionally appended to
  algorithm result schemas; and
- bitemporal and competing-hypothesis selectors are resolved above the algorithm API rather
  than added to algorithm options.

Resolution pins one committed generation, reconstructs the mandatory transaction-time
snapshot, resolves status/supersession/hypothesis ambiguity, and only then applies an optional
half-open valid-time filter. The resulting graph-only projection is fingerprinted before analyst verbs
dispatch. The neutral knowledge-layer run is durable before the epistemic layer attachment is attempted, so an
attachment failure never rolls back a successful algorithm run.

See [Algorithm Verbs](algorithms.md#invocation-and-knowledge-layer-boundary) for the normative
algorithm boundary and [ADR 0006](../../adr/0006-epistemic-model.md) for preservation and
bitemporal semantics.

Full algorithm catalog: [Algorithm Verbs](algorithms.md).

---

## Arrow Result Contract

Data-bearing execution results — Cypher `execute`, analyst verbs, and other
tabular algorithm or inspection tables — are returned as **Arrow RecordBatch
streams**. Control, metadata, lifecycle, explanation, and construction surfaces
may return scalars, collections, unit, or handles instead; see
[Arrow as the Data Contract](overview.md#arrow-as-the-data-contract). The schema
for query results carries GraphForge metadata:

```rust
fn result_schema(fields: Vec<Field>, query_id: &str, ontology_ver: &str) -> Schema {
    let mut meta = HashMap::new();
    meta.insert("graphforge.ir_version".into(), "1.0.0".into());
    meta.insert("graphforge.ontology_version".into(), ontology_ver.into());
    meta.insert("graphforge.query_id".into(), query_id.into());
    meta.insert("graphforge.result_kind".into(), "query_result".into());
    Schema::new_with_metadata(fields, meta)
}
```

Topology node schema (the shipped layout — see `crates/graphforge-storage/src/schemas.rs`):

```rust
// topology/nodes.parquet — identity + labels only (the GRAPH LAYER hot path)
let node_schema = Schema::new(vec![
    Field::new("node_uuid",  DataType::FixedSizeBinary(16), false), // UUIDv7 — canonical identity
    Field::new("node_id",    DataType::UInt64,  false),             // local surrogate — join key
    Field::new("type_id",    DataType::UInt32,  false),             // immutable primary label
    Field::new("type_ids",   DataType::List(Arc::new(Field::new(
        "item", DataType::UInt32, false
    ))), false),                                                   // complete label set
    Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
]);
```

> **Layer note (ADR 0005).** The topology node row holds _only_ identity and type. Property values
> live in `properties/ENTITY_TYPE.parquet` (graph layer, warm path). **Confidence, provenance, and
> valid-time are knowledge-layer concerns** ([ADR 0005](../../adr/0005-layered-architecture.md)) — they
> are recorded in the knowledge layer and attached to graph objects by `node_uuid`/`edge_uuid`, not
> as columns on topology tables. Bitemporal valid-time lives on epistemic _assertions_
> ([ADR 0006](../../adr/0006-epistemic-model.md)), not on raw nodes. (Earlier drafts of this doc showed
> `props_json`/`confidence`/`provenance_id`/`valid_*_ts` on the node row; that pre-refactor schema
> has been superseded by the topology/properties split and the layer boundary.)

The shipped typed edge schema contains neither engine-owned `confidence` nor
`provenance_uuid`. A domain property named `confidence` remains an ordinary
graph property with no special execution semantics. Provenance and knowledge
records use the independent `graphforge-provenance`/`graphforge-knowledge` ledgers owned by
[ADR 0012](../../adr/0012-knowledge-domain-ownership.md); no historical project is
imported or converted.

---

## Memory Model

Arrow is used as the internal execution currency:

- **Columnar RecordBatch** — the unit of data flowing between operators
- **C Data Interface** — zero-copy in-process sharing across Rust and Python
- **C Stream Interface** — batch readers for streaming results
- **Arrow IPC** — serialized stream for cross-process use (Node today; future UniFFI consumers)

Query-result files use the same demand-driven `RecordBatch` stream. Parquet and
Arrow IPC sinks request one batch only after the preceding batch has been
accepted, enforce configured batch and Parquet row-group limits, and publish a
sibling temporary file atomically only after writer finalization and sync.
Receipts report rows, batches, bytes, elapsed time, and completion phase;
cancellation or execution/writer failure reports bounded progress and never
publishes a partial destination.

File order is the query's result order. An explicit Cypher `ORDER BY` therefore
produces deterministic row order; without an ordering clause, neither the sink
nor the file format adds an ordering guarantee.

Swift and Kotlin bindings are deferred to v0.5.1; when they ship, they will consume Arrow IPC
bytes over UniFFI without becoming semantic owners.

---

## Provenance and confidence

Graph execution emits a neutral mutation/inference receipt containing operation
semantics and affected UUIDs. It does not construct domain rows or open
knowledge tables. `graphforge-api` converts the receipt into `graphforge-provenance` events and
lineage, validates any `graphforge-knowledge` confidence/assertion references, and
publishes all participants through one generic `graphforge-storage` generation.

Confidence is immutable knowledge metadata. It is not a Cypher,
traversal, rank, cluster, paths, analyze, similar, or find option and never
changes their graph-native schemas or results. epistemic selected-belief projection
is completed before neutral algorithm dispatch; algorithm executors remain
knowledge-independent.

Schema registries, version compatibility, same-transaction reference
validation, and forbidden dependency directions are normative in
[ADR 0012](../../adr/0012-knowledge-domain-ownership.md).

---

## Error Handling

| Layer | Tool | Approach |
| --------------- | -------------------------- | ----------------------------------------------------------------------------------------------- |
| Public Rust API | `thiserror` | Typed error enums: `ParseError`, `BindError`, `OntologyError`, `StorageError`, `ExecutionError` |
| Rust internals | `anyhow` | Contextual error accumulation |
| Python binding | `pyo3` exception mapping | Thin projection: each Rust error category → distinct Python exception class |
| Node binding | `napi-rs` error conversion | Thin projection: `Error` with `code`, `message`, and structured `details` |

---

## References

- [Architecture Overview](overview.md) — workspace layout and unified API
- [Algorithm Verbs](algorithms.md) — full algorithm catalog
- [AST & Planning](ast-and-planning.md) — compiler pipeline and Graph IR
- [Storage](storage.md) — StorageProvider trait, Parquet provider
- [ADR 0001: Rust Core](../../adr/0001-rust-core.md) — DataFusion and Arrow choice rationale
