# GraphForge Architecture Overview

This is a technical reference for developers and integrators. You do not need
the engine architecture to analyze your first dataset. Start with
[Your first research project](../../guide/first-research-project.md) or
[Your first graph](../../guide/quickstart.md).

**Scope:** v0.6.0 architecture. Package availability and release qualification
are described in [Installation](../../guide/installation.md).

> **Implementation status legend:** **Implemented** means present in the source;
> **Partially built** means some paths remain incomplete; **Designed** means
> specified but not a complete public capability; **Deferred** means outside the
> current release scope. Source implementation does not establish publication
> or usability qualification for a release candidate.

## Executive Summary

GraphForge is a **Knowledge Analysis Workbench** — not a graph database or a graph analytics engine.
It optimizes for analyst workflows that begin with uncertainty, discover structure over time,
and progressively formalize that structure into ontology, workflows, and repeatable analysis.

A Project groups graph data with optional research assets. The architecture
organizes these concerns as follows:

```
Project = Knowledge Graph + Documents + Provenance + Embeddings + Workflows + Artifacts + Sync State
```

The implemented Core [research workspace capabilities](research-workspaces.md)
include Slices, independent Branches, immutable Versions, Forks, and selective
Proposals. They are optional: ordinary graph construction and queries do not
require them. [Keep research history](../../guide/research-journey.md) introduces
a retained Version through a runnable example. The
[analyst research experience](../../engineering/analyst-ux.md) also defines
outcomes for associated interfaces such as XYG and the Hub; their qualification
is separate from Core implementation.

Behavior lives in a **Rust core**. Python and Node are thin bindings over `graphforge-api` — never fallback
engines. GraphForge exposes a unified API and a compiler pipeline (DataFusion-backed execution, Arrow as
the stable in-memory and FFI contract, Parquet for durable graph data).

Within the Rust facade, `query_execution` owns query binding/execution, streaming,
parameter validation, and result sinks. `result_shaping` owns public Arrow column
selection and per-query schema metadata. These private modules preserve the
`GraphForge` public methods. `workspace_hydration` owns authenticated workspace
materialization and generation read authority. `graph_publication` owns mutation
publication, reconciliation, and in-memory reset; `runtime_ownership` owns runtime
lifetime, synchronous execution, and query admission. A streamed query retains
its workspace until the stream and its file descriptors are dropped.

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

| Layer         | Owns                                                                                  | Where it lives                                                                                                                                     |
| ------------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Graph**     | Nodes, edges, properties, traversal, pattern matching, graph algorithms, adjacency    | `graphforge-cypher`, `graphforge-ir`, `graphforge-rel`, `graphforge-exec`, `graphforge-storage` (`topology/`, `properties/`, `indexes/adjacency/`) |
| **Knowledge** | Provenance, confidence, evidence, epistemic assertions/status/supersession/valid-time | `graphforge-provenance` + `graphforge-knowledge`; `provenance/`, `knowledge/`                                                                      |
| **Workbench** | Analyst verbs, hybrid search, workflows, exploration, project envelope                | `graphforge-api`, bindings, search modules                                                                                                         |

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
│   Swift/Kotlin: outside this release                                             │
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

### Cargo feature profiles

`graphforge-api` keeps the complete facade enabled by default. Its opt-in surface
features are `knowledge`, `provenance`, `search`, `portable`, `discovery`, and
`research`; knowledge implies provenance, and research implies knowledge and
portable package support. A no-default API build
retains graph construction, Cypher execution, traversal, and the TCK while
omitting those extension modules. The CLI and native binding crates explicitly
enable the full feature set so their public behavior does not depend on Cargo's
default-feature unification.

`graphforge-exec` separates the rank and clustering registries into
`algorithms-core` and `algorithms-extended`. The core profile registers Degree,
PageRank, connected components, and every path algorithm. Extended rank and
clustering algorithms remain enabled by default for existing consumers. A
core-only executor returns the normal unavailable-algorithm error for entries
that are not registered in that profile. CI builds and tests both the lean API
and core-only executor profiles.

Feature gates control Rust API compilation and algorithm registration; they do
not change graph storage formats or the Cypher execution pipeline.

The diagram above shows execution flow, not Cargo dependencies. Storage
consumes the shared value crate and still has IR and ontology dependencies;
execution-flow diagrams do not describe every crate dependency. `IrVersion`
annotates results; it is not a project-open compatibility gate.

[ADR 0025](../../adr/0025-storage-value-contract.md) chooses a compiler-independent
`graphforge-value` crate above core for shared values, checked tagged IDs and
catalog representations. That crate is present and consumed by storage. Compiler
plans remain in IR, storage owns project admission and physical persistence, and
existing encodings must be preserved. The [project compatibility policy](project-format-compatibility.md)
remains separate from compiler-plan versioning.

## Rust Workspace Layout

```
crates/
  graphforge-api/              # public Rust facade (lifecycle, Cypher, verbs, knowledge)
  graphforge-core/             # shared identities, options, and facade errors
  graphforge-value/            # shared literals, tagged IDs, and catalog values
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

Swift/Kotlin UniFFI bindings are outside this release.

---

## Three Internal Representations

The Cypher compiler maintains three distinct representations — they are not interchangeable:

| Representation              | Purpose                                                 | Stability                        |
| --------------------------- | ------------------------------------------------------- | -------------------------------- |
| **AST**                     | Syntax-faithful, span-rich, close to Cypher source text | Internal only — no API guarantee |
| **Graph IR**                | Semantic and graph-native; the stable plan contract     | Semver-versioned                 |
| **DataFusion logical plan** | Relational/physical execution                           | DataFusion's own contract        |

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

| Category                  | Typical returns             | Examples (Rust → Python / Node)                                                                                                        |
| ------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| **Metadata / inspection** | string collections, scalars | `labels()` / `relationship_types()` → `Vec<String>` / `list[str]`; `node_count()` → `u64` / `int`                                      |
| **Explanation**           | plain text                  | `explain()` → `String` / `str`                                                                                                         |
| **Lifecycle / control**   | unit (`()` / `None`)        | `index(...)`, `load_ontology(...)`, `adopt_ontology(...)`, `clear_ontology(...)`, `execute_to_parquet(...)`, embedding publish helpers |
| **Construction handles**  | instance-bound handles      | `add_node(...)` → `NodeHandle`; `add_edge(...)` → `EdgeHandle`                                                                         |

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

| Language   | Mechanism        | Crate / Package                      | Result contract                                                                                 | Status                 |
| ---------- | ---------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------- | ---------------------- |
| **Rust**   | Native crate API | `graphforge-api` / `graphforge-core` | Arrow for data-bearing results; scalars / collections / unit / handles elsewhere                | **Implemented**        |
| **Python** | PyO3 + maturin   | `graphforge-bindings-py`             | `pyarrow.Table` (or reader) for tabular results; same non-Arrow categories as Rust              | **Implemented** (thin) |
| **Node**   | napi-rs          | `graphforge-bindings-node`           | Arrow IPC `Buffer` → `tableFromIPC(buf)` for tabular results; same non-Arrow categories as Rust | **Implemented** (thin) |
| **Swift**  | UniFFI (planned) | deferred                             | Arrow IPC (data plane)                                                                          | **Deferred**           |
| **Kotlin** | UniFFI (planned) | deferred                             | Arrow IPC (data plane)                                                                          | **Deferred**           |

The architectural rule: **never let a binding become the semantic owner**. Bindings project
requests and results; the Rust core owns Cypher, verbs, storage, and knowledge semantics.
Bindings never reshape tabular engine results into binding-owned row/object graphs.
See [ADR 0001](../../adr/0001-rust-core.md).

---

## Correctness bar

These surfaces must stay green on `main`:

| Gate                   | Requirement                                                                        |
| ---------------------- | ---------------------------------------------------------------------------------- |
| Parser / compiler      | RD+Pratt parse → bind → Graph IR → relational lowering → DataFusion                |
| OpenCypher conformance | Authoritative TCK corpus passes                                                    |
| Ontology runtime       | Load/validate round-trips for progressive modes                                    |
| Data contract          | Arrow/Parquet/IPC round-trips pass                                                 |
| Storage                | Parquet project generations with atomic publication / recovery                     |
| Bindings               | Thin Python and Node projections; tabular results stay Arrow                       |
| Knowledge              | knowledge ledger + epistemic records attach by UUID without changing graph results |

---

## References

- [AST & Planning](ast-and-planning.md) — recursive-descent/Pratt parser, three-tier IR, compiler pipeline
- [Algorithm Verbs](algorithms.md) — full algorithm catalog across rank/cluster/paths/analyze/similar
- [Execution Model](execution-model.md) — DataFusion integration, custom graph operators, Arrow result streams
- [Storage](storage.md) — Project generations, Arrow schemas, and Parquet storage
- [ADR Index](../../adr/README.md) — architecture decisions and their status
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
- [ADR 0018: Acknowledged durability and isolation](../../adr/0018-acknowledged-durability-isolation.md) — acknowledgement boundary, filesystem scope, and isolation honesty
- [ADR 0036: The GraphForge release version contract](../../adr/0036-release-version-contract.md) — one public version, spelled per ecosystem, for the Rust core, bindings, CLI, and skills release set
- [Concurrency and recovery](concurrency-recovery.md) — architecture narrative for write modes, recovery, and the durability matrix
- [Roadmap](../../releases/roadmap.md) — Milestones and timeline
