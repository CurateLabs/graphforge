# ADR 0001: Rust Core

**Date:** 2026-05-25
**Status:** Accepted
**Build target:** v0.5.0

---

## Context

GraphForge is a **Knowledge Analysis Workbench**. Analysts need a single semantic core that:

1. **Returns language-independent results** — downstream tools must not depend on a language-specific object model
2. **Performs well on graph-native work** — variable-length path expansion, provenance tracking, and large-graph algorithms benefit from native code
3. **Stays extensible** — custom operators, storage providers, and planner rules must be first-class extension points

Those requirements drive a **Rust core** with thin multi-language bindings: a first-class Rust crate API, Python and Node bindings that return Arrow (table / IPC), and Swift and Kotlin bindings via UniFFI. The compiler pipeline, ontology model, and Graph IR are Rust artifacts; bindings translate results and never own semantics.

The public API is seven analyst-intent verbs on a single `forge` instance (`execute`, `rank`, `cluster`, `paths`, `analyze`, `similar`, `find`) with a unified Arrow result contract.

---

## Decision

GraphForge is a **Rust core** with multi-language bindings. Rust owns behavior; Python, Node, and UniFFI surfaces are thin projections of `graphforge-api`.

---

## Architecture Summary

### Parser: Recursive Descent + Pratt expression parser

**Chosen over LALRPOP, pest, and nom.**

The parser is a hand-written recursive-descent clause parser with a Pratt expression parser
subroutine — the architecture used by Neo4j's production Cypher parser, PostgreSQL, SQLite,
and virtually every other production SQL/query-language parser. It is conflict-free by
construction.

See [ADR 0002](0002-lr1-grammar.md) for the full rationale: LALRPOP was evaluated first,
but structural shift/reduce conflicts proved unresolvable within LALRPOP's state-merging
model. The hand-written parser resolves this permanently and is directly auditable against
the openCypher BNF.

- **LALRPOP** — rejected because LALRPOP's state-merging produces structural conflicts in the full openCypher grammar that cannot be resolved without compromising correctness — see ADR 0002
- **pest** — rejected: PEG-based, different parsing model, cannot reuse the hand-written `Tok` lexer
- **nom** — rejected: parser-combinator library optimised for streaming/binary parsing; not a natural fit for a large declarative grammar

### Execution: DataFusion + custom graph operators

**Chosen over Polars-as-executor and a fully custom executor.**

DataFusion exposes exactly the extension points GraphForge needs:

- Custom `LogicalPlan` and `ExecutionPlan` nodes for graph-native operators
- Custom `TableProvider` for Parquet (and future providers)
- Custom optimizer rules for predicate pushdown and label-constraint hoisting
- A `QueryPlanner` hook so GraphForge IR is lowered directly rather than parsing SQL

Polars is an excellent dataframe engine but does not expose these extension points. A fully custom executor would require implementing scan interfaces, optimizer rules, batch streams, and partitioning from scratch — all of which DataFusion already provides.

Polars is retained as a **storage-layer companion** for IO/sinks (Parquet, CSV, JSON/NDJSON, IPC) and as an optional convenience layer in the Python binding. It is not the semantic owner of query execution.

### Wire contract: Arrow

Arrow provides:

- A stable, language-independent columnar memory format
- Zero-copy in-process exchange via the C Data Interface
- Python exchange via the PyCapsule Interface (no hard PyArrow dependency)
- Node consumption via Arrow IPC and `tableFromIPC` in Apache Arrow JS

Arrow schema metadata carries GraphForge-specific annotations (`ir_version`, `ontology_version`, `query_id`, `provenance_policy`) that survive IPC and Parquet round-trips.

The AST is not the cross-language contract. The **Graph IR** (semver-versioned) and the **Arrow result schema** are the stable boundaries.

### Storage: Parquet

**Parquet** is the sole storage provider for the v0.5.0 Rust core. Parquet file metadata carries GraphForge provenance annotations. The `StorageProvider` trait is designed for extension, but additional backends (SQLite, DuckDB) are out of scope for this release.

### User-Facing API

Seven analyst-intent verbs on a single `forge` instance:

| Method | What it does | Returns |
|---|---|---|
| `forge.execute(cypher)` | Run an openCypher query | Arrow Table |
| `forge.rank(label, by=...)` | Score every node (centrality, structural, link prediction) | Arrow Table + `score` |
| `forge.cluster(label, by=...)` | Assign community membership | Arrow Table + `community_id` |
| `forge.paths(source, target, by=...)` | Find paths, compute flow, or traverse | Arrow Table + `cost`, `path` |
| `forge.analyze(label, by=...)` | Spanning trees, DAG analysis, coloring, matching, embeddings | Arrow Table (varies) |
| `forge.similar(label, by=...)` | Pairwise node similarity | Arrow Table + `similarity` |
| `forge.find(query, ...)` | Search/vector/hybrid search with lazy indexing | Arrow Table + `score` + `matched_on` |

`rank()`, `cluster()`, and `paths()` accept `write_property` for opt-in graph mutation. `find()` indexes lazily on first call; `forge.index()` is available for explicit control.

Full algorithm catalog: [docs/book/architecture/algorithms.md](../book/architecture/algorithms.md)

### Bindings

| Language | Mechanism | Result type |
|---|---|---|
| Python | PyO3 + maturin | `pyarrow.Table` or `RecordBatchReader` |
| Node | napi-rs | Arrow IPC `Buffer` → `tableFromIPC(buf)` |
| Swift | UniFFI | Arrow IPC `Data` → `GraphForgeResult` |
| Kotlin | UniFFI | Arrow IPC `ByteArray` → `GraphForgeResult` |

### Ontology: runtime-loadable

The ontology is a runtime-loadable metadata model, not baked into Rust types at compile time. Authoring format is YAML or JSON (Serde); execution format is Arrow tables compiled at load time; persistence format is Parquet. Progressive modes are defined in [ADR 0003](0003-progressive-ontology.md).

---

## Quality bar

These requirements remain the acceptance bar for the Rust core and its bindings:

| Gate | Requirement |
|---|---|
| Parser parity | Existing corpus + syntax goldens pass against recursive-descent + Pratt parser |
| openCypher conformance | Agreed TCK subset passes end-to-end |
| Ontology runtime | Load/validate/migrate round-trips pass |
| Data contract | Arrow/Parquet/IPC round-trips pass |
| Provider baseline | Parquet provider passes core semantics |
| Binding baseline | Python, Node, Swift, and Kotlin can execute queries and consume Arrow results |
| Observability | `explain`, query IDs, provenance IDs, structured errors available |

---

## Consequences

### Positive

- First-class Rust crate API, Python, Node, Swift, and Kotlin bindings sharing a single semantic core
- Arrow as a stable, language-independent result contract eliminates per-language serialization
- DataFusion's optimizer and execution framework accelerate complex query handling without building a full executor from scratch
- Pluggable storage providers enable future backends without changing query semantics
- The ontology as a runtime-loadable model enables schema evolution without recompilation

### Negative / Risks

- Native and binding surfaces must stay in lockstep; bindings must never become a fallback engine
- The parity corpus and differential testing infrastructure are ongoing maintenance costs
- DataFusion's extension API surface is stable but evolving; custom node implementations must track upstream changes
- The quality bar is strict by design; shipping pressure must not lower it

### Mitigations

- Thin-binding rule: Rust owns semantics; bindings only translate requests and Arrow results
- Shared corpus and TCK gates across facade and binding lanes
- Explicit crate boundaries (see [ADR 0005](0005-layered-architecture.md) and [ADR 0012](0012-m20-domain-ownership.md))

---

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| Polars as primary executor | Does not expose compiler extension points (custom plan nodes, optimizer rules, table providers) needed for graph-native semantics |
| Fully custom Rust executor | Front-loads the highest-risk work (scan interfaces, optimizer, batch streams) that DataFusion already provides |
| Language-native engines per binding | Cannot deliver one semantic core; path expansion and provenance tracking diverge per language |
| DuckDB as semantic owner | DuckDB is an excellent accelerator but its extension model is not designed for owning a graph IR and custom operator semantics |

---

## References

- [Architecture Overview](../book/architecture/overview.md)
- [AST & Planning](../book/architecture/ast-and-planning.md)
- [Execution Model](../book/architecture/execution-model.md)
- [Storage Architecture](../book/architecture/storage.md)
- [ADR 0005: Layered Architecture](0005-layered-architecture.md) — the graph/knowledge/workbench layer model and crate-boundary rule that organises the crates listed above
