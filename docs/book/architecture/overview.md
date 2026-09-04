# GraphForge Architecture Overview

**Status:** v0.5.0 — Rust core shipped
**Last Updated:** 2026-08-14

> **Implementation status legend** (used across architecture docs): **Shipped** = implemented and
> tested on `main`; **Partially built** = some paths real, others stubbed; **Designed** = specified,
> not yet a complete public capability; **Deferred** = intentionally after v0.5.0 (for example
> Swift/Kotlin bindings). For v0.5.0: the Cypher pipeline
> (`graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`), analyst verbs, Parquet project storage, thin Python/Node
> bindings, and the knowledge layer (immutable provenance ledger + epistemic model) are **Shipped**.
---

## Executive Summary

GraphForge is a **Knowledge Analysis Workbench** — not a graph database or a graph analytics engine.
It optimizes for analyst workflows that begin with uncertainty, discover structure over time,
and progressively formalize that structure into ontology, workflows, and repeatable analysis.

For an explicit whole-system comparison with a database-centered analytics platform, including
the tradeoffs between canonical Arrow results and GDS `stream`/`stats`/`mutate`/`write` modes,
see [GraphForge v0.5 and Neo4j with Graph Data Science](graphforge-vs-neo4j-gds.md).

The project (not the graph) is the primary unit of work:

```
Project = Knowledge Graph + Documents + Provenance + Embeddings + Workflows + Artifacts + Sync State
```

Behavior lives in a **Rust core**. Python and Node are thin bindings over `graphforge-api` — never fallback
engines. v0.5.0 exposes a unified API and a compiler pipeline (DataFusion-backed execution, Arrow as
the stable in-memory and FFI contract, Parquet for durable graph data).

The normative pre-v1 geometry, CRS, Arrow layout, and ownership boundary is
defined in [Canonical spatial values](spatial-values.md).
---

## Architecture Principles

1. **Arrow is the data-plane wire contract** — Cypher, analyst verbs, and other tabular/data-bearing results cross language boundaries as Arrow RecordBatch streams; no GraphForge-specific buffer protocol for those results
2. **GraphForge owns the semantics** — the Cypher compiler, ontology, and Graph IR live in GraphForge-owned Rust crates; no storage provider or binding becomes the semantic owner
3. **DataFusion is the execution backbone** — GraphForge extends DataFusion with custom graph operators rather than writing a full executor from scratch
4. **Scoped result contract** — data-returning operations use Arrow in and Arrow out; control, metadata, lifecycle, explanation, and construction surfaces may return scalars, collections, unit, or construction handles (not binding-owned graph result objects)
5. **Correctness over performance** — strict openCypher TCK compliance remains the primary constraint
6. **Ontology is progressive, not required** — GraphForge supports three modes: `exploratory` (no ontology required, all labels accepted), `advisory` (ontology present, violations are warnings), and `strict` (ontology enforced, violations are errors). Exploratory analysis is a first-class workflow. See [ADR 0003](../../adr/0003-progressive-ontology.md).
7. **Three layers, clean boundaries** — graph concerns, knowledge concerns, and workbench concerns are separated; the graph layer stays graph-native and never absorbs the others. See the next section and [ADR 0005](../../adr/0005-layered-architecture.md).
8. **Preserve the evolution of understanding** — GraphForge records not just the current state of knowledge but how it evolved: competing hypotheses, superseded conclusions, evidence, and reasoning are preserved, never destroyed. See [ADR 0006](../../adr/0006-epistemic-model.md).

---

## Layered Architecture

GraphForge is a **knowledge analysis workbench**, not just a graph engine. Its architecture
separates three layers with strict boundaries ([ADR 0005](../../adr/0005-layered-architecture.md)).
Lower layers never depend on higher ones, and — critically — **the graph layer never absorbs
knowledge or workbench concerns**.

```text
┌───────────────────────────────────────────────────────────────────────┐
│  WORKBENCH LAYER                                                        │
│  forge.rank / cluster / paths / analyze / similar / find · search ·     │
│  workflows / recipes · exploration · project portability               │
│  — consumes the layers below; holds NO graph-semantic state             │
├───────────────────────────────────────────────────────────────────────┤
│  KNOWLEDGE LAYER                                                        │
│  provenance · confidence · evidence · ontology-inference lineage ·      │
│  epistemic assertions + status + supersession + valid-time (ADR 0006)   │
│  — attaches to graph objects BY UUID REFERENCE ONLY                     │
├───────────────────────────────────────────────────────────────────────┤
│  GRAPH LAYER                                                            │
│  nodes · edges · properties · traversal · pattern matching ·            │
│  graph algorithms · adjacency index (ADR 0004)                          │
│  — graph-native; surrogate-keyed execution; UUID identity              │
│  — stores NO knowledge or workbench semantics                          │
└───────────────────────────────────────────────────────────────────────┘
```

| Layer | Owns | Where it lives |
| ------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Graph** | Nodes, edges, properties, traversal, pattern matching, graph algorithms, adjacency | `graphforge-cypher`, `graphforge-ir`, `graphforge-rel`, `graphforge-exec`, `graphforge-storage` (`topology/`, `properties/`, `indexes/adjacency/`) |
| **Knowledge** | Provenance, confidence, evidence, epistemic assertions/status/supersession/valid-time | `graphforge-provenance` + `graphforge-knowledge`; `provenance/`, `knowledge/` |
| **Workbench** | Analyst verbs, hybrid search, workflows, exploration, project envelope | `graphforge-api`, bindings, search modules |

**Boundary rule:** knowledge attaches to the graph by UUID reference, never by embedding columns on
graph tables. Cypher/traversal/algorithms read only the graph layer, so the presence or absence of
knowledge data never changes a graph-native query result (a tested invariant). This keeps the
traversal hot path lean and preserves the lightweight-embedded model.

Embedding computation and embedding publication are also separate boundaries.
`analyze(..., by=<embedding>)` is read-only and returns Arrow; an explicit
find/index operation may publish that complete result, local/custom/provider output,
or caller-supplied vectors as an atomic, versioned search-space generation.
Display names never define compatibility, remote inference is optional, and
search never reads knowledge state. The normative identity, freshness, refresh,
provider, tokenizer, privacy, and reranking rules are in the
[embedding v1 contract](embedding-v1.md#embedding-space-publication).

The project (not the graph) is the unit of work, and the layers map onto the project envelope:

```
Project = Graph (topology + properties)          ← graph layer
        + Knowledge (provenance + confidence + evidence + epistemic assertions)  ← knowledge layer
        + Workbench assets (documents + embeddings + indexes + workflows + artifacts)  ← workbench layer
        + Sync State
```

---

## High-Level Architecture

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                               GraphForge API (graphforge-api)                             │
│   Thin bindings: Python (PyO3/maturin)  ·  Node (napi-rs)                         │
│   Swift/Kotlin: deferred to v0.5.1                                                │
│                                                                                   │
│  forge.execute(…)  forge.rank(…)   forge.cluster(…)  forge.paths(…)              │
│  forge.analyze(…)  forge.similar(…)  forge.find(…)                               │
└──────────────────────────────────────────────────────────────────────────────────┘
          │                        │                  │                  │
          ▼                        └──────────────────┴──────────────────┘
┌──────────────────┐                                  │
│   Cypher Path    │                                  ▼
│                  │              ┌──────────────────────────────────┐
│  RD+Pratt parser │              │   Analyst Verbs                  │
│  (graphforge-cypher)     │              │   rank / cluster / paths /       │
│       ↓          │              │   analyze / similar / find       │
│  Binder +        │              │   — bypass parser/planner        │
│  ontology        │              │   Export adjacency or index      │
│       ↓          │              │   Dispatch algorithm or search   │
│  Graph IR        │              │   Produce scored Arrow batches   │
│  (graphforge-ir)         │              │                                  │
│       ↓          │              │                                  │
│  Relational      │              │                                  │
│  lowering        │              │                                  │
│  (graphforge-rel)        │              │                                  │
│       ↓          │              │                                  │
│  DataFusion      │◄─────────────────────────────────┘
│  (graphforge-exec)       │         (all paths converge to DataFusion)
│       ↓          │
│  Arrow batches   │
└──────────────────┘
          │
          ▼
┌──────────────────────────────────────┐
│          Storage (graphforge-storage)         │
│              Parquet + JSON           │
└──────────────────────────────────────┘
```

---

## Crate dependencies and durable values

The diagram above shows execution flow, not Cargo dependencies. Today storage
also depends on IR and ontology: `IrLiteral`, tagged runtime IDs and the runtime
catalog's persistence schema live in IR, and storage consumes ontology and
composition adapters. `IrVersion` annotates results; it is not a project-open
compatibility gate.

[ADR 0025](../../adr/0025-storage-value-contract.md) chooses a compiler-independent
`graphforge-value` crate above core for shared values, checked tagged IDs and
Arrow/catalog codecs. That extraction is **Designed**, tracked by #1011 and
#1012; the crate is not yet present. Compiler plans remain in IR, storage owns
project admission and physical persistence, and existing encodings must be
preserved. The [project compatibility policy](project-format-compatibility.md)
remains separate from compiler-plan versioning.

## Rust Workspace Layout

```
crates/
  graphforge-api/              # public Rust facade (lifecycle, Cypher, verbs, knowledge)
  graphforge-core/             # shared identities, values, options, and facade errors
  graphforge-ast/              # AST + spans + syntax diagnostics
  graphforge-cypher/           # hand-written lexer + recursive-descent/Pratt parser
  graphforge-ontology/         # runtime ontology model, validation, migration
  graphforge-ir/               # graph IR + serde DTOs
  graphforge-rel/              # graph IR → relational lowering
  graphforge-plan/             # DataFusion logical extension nodes and mutation specifications
  graphforge-exec/             # execution session, algorithms, search, result streaming
  graphforge-storage/          # project generations, Arrow schemas, and Parquet storage
  graphforge-io/               # bounded, atomic Parquet and Arrow IPC result sinks
  graphforge-provenance/       # knowledge-layer provenance events + lineage domain
  graphforge-knowledge/        # knowledge-layer immutable + epistemic record domains
  graphforge-bindings-py/      # thin PyO3 + maturin Python binding
  graphforge-bindings-node/    # thin napi-rs Node binding
  graphforge-cli/              # command-line interface
```

Feature flags: `default = ["datafusion", "parquet"]`. Optional: `polars`, `python`, `node`.
Swift/Kotlin UniFFI bindings are deferred to v0.5.1.

---

## Three Internal Representations

The Cypher compiler maintains three distinct representations — they are not interchangeable:

| Representation | Purpose | Stability |
| --------------------------- | ------------------------------------------------------- | -------------------------------- |
| **AST** | Syntax-faithful, span-rich, close to Cypher source text | Internal only — no API guarantee |
| **Graph IR** | Semantic and graph-native; the stable plan contract | Semver-versioned |
| **DataFusion logical plan** | Relational/physical execution | DataFusion's own contract |

The AST is not the cross-language compatibility surface. The stable boundary is the Graph IR
envelope and the Arrow result contract.

See [AST & Planning](ast-and-planning.md) for the full compiler pipeline.

---

## Arrow as the Data Contract

**Data-plane** results — Cypher `execute` / streaming sinks, analyst verbs
(`rank` / `cluster` / `paths` / `analyze` / `similar` / `find`), tabular
inspection such as `schema()`, bulk-construction receipts, and other
data-bearing algorithm or knowledge tables — cross language boundaries as
**Arrow RecordBatch streams**. Arrow provides:

- A stable, language-independent columnar memory format
- Zero-copy in-process exchange via the C Data Interface
- Python interchange via the PyCapsule Interface (no hard PyArrow dependency required)
- Node consumption via Arrow IPC and `tableFromIPC` in Apache Arrow JS

Arrow schema metadata carries GraphForge-specific annotations:

```
graphforge.ir_version = "1.0.0"
graphforge.ontology_version = "core-2026.05"
graphforge.result_kind = "node_table"
graphforge.confidence_policy = "conservative_minmax"
graphforge.query_id = "01J..."
```

These annotations survive IPC serialization and Parquet round-trips, which is why Arrow is
the correct contract for tabular results rather than a Polars or Python-specific result type.

The unused string-column `graphforge_api::RecordBatch` interim type is removed
for v0.6.0. Query results continue to expose Arrow batches through
`ExecutionResult`; Rust table consumers use `arrow::record_batch::RecordBatch`.
Metadata lists, counts, and explanations retain their public collection,
scalar, and string return types.

### Control and construction plane (intentional non-Arrow returns)

Not every public method is a tabular data operation. The Rust facade
(`graphforge-api`) intentionally returns non-Arrow values for control,
metadata, lifecycle, explanation, and construction. Python and Node mirror the
same categories as thin projections — they do **not** execute graph logic or
rebuild tabular engine results into binding-owned objects.

| Category | Typical returns | Examples (Rust → Python / Node) |
| -------- | --------------- | -------------------------------- |
| **Metadata / inspection** | string collections, scalars | `labels()` / `relationship_types()` → `Vec<String>` / `list[str]`; `node_count()` → `u64` / `int` |
| **Explanation** | plain text | `explain()` → `String` / `str` |
| **Lifecycle / control** | unit (`()` / `None`) | `index(...)`, `load_ontology(...)`, `adopt_ontology(...)`, `clear_ontology(...)`, `execute_to_parquet(...)`, embedding publish helpers |
| **Construction handles** | instance-bound handles | `add_node(...)` → `NodeHandle`; `add_edge(...)` → `EdgeHandle` |

**Construction handles vs metadata/control:** a `NodeHandle` / `EdgeHandle` is a
Rust-owned, instance-bound identity token (stable UUID plus label or relationship
metadata) so callers can wire subsequent construction or selectors. It is not a
tabular query result and not a binding-side graph object model. Metadata and
control returns (`Vec<String>`, `u64`, `String`, `()`) answer inspection,
planning, or lifecycle questions without columnar payloads. Bulk construction
and Cypher/analyst paths remain on the Arrow data plane (including Arrow
receipts for atomic bulk publish).

---

## Multi-Language Bindings

| Language | Mechanism | Crate / Package | Result contract | v0.5.0 |
| ---------- | ---------------- | ----------------------------------------- | -------------------------------------------- | -------- |
| **Rust** | Native crate API | `graphforge-api` / `graphforge-core` | Arrow for data-bearing results; scalars / collections / unit / handles elsewhere | **Shipped** |
| **Python** | PyO3 + maturin | `graphforge-bindings-py` | `pyarrow.Table` (or reader) for tabular results; same non-Arrow categories as Rust | **Shipped** (thin) |
| **Node** | napi-rs | `graphforge-bindings-node` | Arrow IPC `Buffer` → `tableFromIPC(buf)` for tabular results; same non-Arrow categories as Rust | **Shipped** (thin) |
| **Swift** | UniFFI (planned) | deferred | Arrow IPC (data plane) | **Deferred** (v0.5.1) |
| **Kotlin** | UniFFI (planned) | deferred | Arrow IPC (data plane) | **Deferred** (v0.5.1) |

The architectural rule: **never let a binding become the semantic owner**. Bindings project
requests and results; the Rust core owns Cypher, verbs, storage, and knowledge semantics.
Bindings never reshape tabular engine results into binding-owned row/object graphs.
See [ADR 0001](../../adr/0001-rust-core.md).

---

## v0.5.0 correctness bar

Shipped v0.5.0 expects these surfaces to stay green on `main`:

| Gate | Requirement |
| ---------------------- | --------------------------------------------------------------------- |
| Parser / compiler | RD+Pratt parse → bind → Graph IR → relational lowering → DataFusion |
| OpenCypher conformance | Authoritative TCK corpus passes |
| Ontology runtime | Load/validate round-trips for progressive modes |
| Data contract | Arrow/Parquet/IPC round-trips pass |
| Storage | Parquet project generations with atomic publication / recovery |
| Bindings | Thin Python and Node projections; tabular results stay Arrow |
| Knowledge | knowledge ledger + epistemic records attach by UUID without changing graph results |
---

## References

- [AST & Planning](ast-and-planning.md) — recursive-descent/Pratt parser, three-tier IR, compiler pipeline
- [GraphForge v0.5 and Neo4j GDS](graphforge-vs-neo4j-gds.md) — whole-system positioning, current limitations, and result-lifecycle tradeoffs
- [Algorithm Verbs](algorithms.md) — full algorithm catalog across rank/cluster/paths/analyze/similar
- [Execution Model](execution-model.md) — DataFusion integration, custom graph operators, Arrow result streams
- [Storage](storage.md) — Project generations, Arrow schemas, and Parquet storage
- [ADR Index](../../adr/README.md) — contiguous decision log (`0001`–`0025`)
- [ADR 0001: Rust Core](../../adr/0001-rust-core.md) — Rust core and binding strategy
- [ADR 0002: RD+Pratt Parser](../../adr/0002-lr1-grammar.md) — Parser algorithm decision
- [ADR 0003: Progressive Ontology](../../adr/0003-progressive-ontology.md) — exploration-first ontology modes
- [ADR 0004: Adjacency Index](../../adr/0004-adjacency-index.md) — graph-layer derived traversal accelerator
- [ADR 0005: Layered Architecture](../../adr/0005-layered-architecture.md) — graph / knowledge / workbench boundaries
- [ADR 0006: Epistemic Model](../../adr/0006-epistemic-model.md) — preserving the evolution of understanding
- [ADR 0012: knowledge/epistemic Domain Ownership](../../adr/0012-knowledge-domain-ownership.md) —
  crate dependency boundaries, table ownership, schema evolution, and
  cross-domain validation
- [ADR 0013: Project Generations](../../adr/0013-project-generation-protocol.md) — durable project-generation protocol
- [ADR 0014: Workspace Checkpoints](../../adr/0014-workspace-checkpoints.md) — complete-workspace checkpoints and revert
- [ADR 0015: Embedded Write Modes](../../adr/0015-embedded-write-modes.md) — single, queued, and optimistic project writes
- [ADR 0016: Repository integration and deployment configuration](../../adr/0016-repository-integration-and-deployment-configuration.md) — tracked definitions, local data, CLI, skills, and IaC ownership boundaries
- [ADR 0017: One version across core and adapters](../../adr/0017-unified-release-version.md) — one public version for the Rust core, bindings, CLI, and skills release set
- [ADR 0018: Acknowledged durability and isolation](../../adr/0018-acknowledged-durability-isolation.md) — acknowledgement boundary, filesystem scope, and isolation honesty
- [Concurrency and recovery](concurrency-recovery.md) — architecture narrative for write modes, recovery, and the durability matrix
- [Roadmap](../../releases/roadmap.md) — Milestones and timeline
