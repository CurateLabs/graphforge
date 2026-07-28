# GraphForge v0.5 Architecture Reference

**Status:** Architecture Baseline — decision record  
**Last Updated:** 2026-05-31  
**Milestone:** v0.5.0: Architecture Baseline

This document is the authoritative reference for architectural decisions made before and during the v0.5 refactor. Treat this document as the primary constraint document for v0.5 architecture. When an implementation decision conflicts with something here, update this document first, then update the implementation.

**Product positioning:** GraphForge is a **Knowledge Analysis Workbench** — not a graph database and not merely a graph analytics engine. The graph is one asset inside a larger analytical project. The workbench is designed for analysts who build knowledge from uncertain, incomplete, or heterogeneous data over time.

---

## 1. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          User Surfaces                               │
│   Python (PyO3, thin)  ·  Node (napi-rs, thin)  ·  Swift/Kotlin deferred (v0.5.1) │
│   forge.execute()  forge.rank()  forge.cluster()  forge.find()      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
         ┌─────────────────┴──────────────────────────────┐
         │  Cypher / GQL Path                              │  Analyst Verbs Path
         ▼                                                 │
   ┌─────────────┐                                         │  rank / cluster / paths /
   │   Parser    │  (gf-cypher)                            │  analyze / similar / find
   │ RD + Pratt  │                                         │  — bypass parser/binder
   └──────┬──────┘                                         │  — export adjacency or index
          ▼                                                 │  — dispatch algorithm backend
   ┌─────────────┐                                         │  — return scored Arrow batches
   │     AST     │  (gf-ast)                               │
   │  spans +    │                                         │
   │  syntax     │                                         │
   └──────┬──────┘                                         │
          ▼                                                 │
   ┌─────────────┐                                         │
   │   Binder    │  (gf-ir)                                │
   │  ontology   │  Resolves names → TypeId/PropId         │
   │  resolution │  Variable scope validation              │
   └──────┬──────┘                                         │
          ▼                                                 │
   ┌─────────────┐                                         │
   │  Graph IR   │  (gf-ir)  ← stable semantic contract   │
   │  GraphPlan  │  DataFusion-independent                 │
   │  + ExprArena│                                         │
   └──────┬──────┘                                         │
          ▼                                                 │
   ┌─────────────┐                                         │
   │  Relational │  (gf-rel)                               │
   │  Lowering   │  GraphOp → DataFusion LogicalPlan        │
   └──────┬──────┘                                         │
          ▼                                                 │
   ┌──────────────────────────────────────────────────────┐│
   │              DataFusion Execution Engine              ││
   │   LogicalPlan → Optimizer → PhysicalPlan → Execute   ││
   │   Custom operators: VarLenExpand, OptionalMatch,      ││
   │   PathUnique, ProvenanceSemijoin, OntologyInfer,      ││
   │   GraphMerge, TypedEdgeScan (NEW)                     ││
   └──────────────────────────────────────────────────────┘│
          │◄────────────────────────────────────────────────┘
          ▼
   ┌─────────────┐
   │Arrow Batches│  UUID columns in every output
   └──────┬──────┘
          ▼
   ┌──────────────────────────────────────────────────────┐
   │                  Storage Provider                     │
   │  topology/  ·  properties/  ·  documents/            │
   │  embeddings/  ·  provenance/  ·  indexes/             │
   └──────────────────────────────────────────────────────┘
```

---

## 1.5. Layering — Graph / Knowledge / Workbench

GraphForge is organised into three layers with strict boundaries
([ADR 0005](../../adr/0005-layered-architecture.md)):

- **Graph layer** — nodes, edges, properties, traversal, pattern matching, graph algorithms, the
  adjacency index. Lives in `topology/` + `properties/` + `indexes/adjacency/`. Graph-native and
  surrogate-keyed; stores no knowledge or workbench semantics.
- **Knowledge layer** — provenance, confidence, evidence, ontology-inference lineage, and the
  epistemic model (assertions, status, supersession, reasoning, bitemporal valid-time;
  [ADR 0006](../../adr/0006-epistemic-model.md)). Lives in `provenance/` + `knowledge/`. Attaches to
  graph objects **by UUID reference only**.
- **Workbench layer** — the analyst verbs, hybrid search, workflows, exploration, project envelope.
  Consumes the layers below and returns Arrow; holds no graph-semantic state.

**Boundary rule:** knowledge attaches by UUID reference, never by embedding columns on graph tables.
Graph-native query results never depend on knowledge-layer data (a tested invariant). This is what
keeps the traversal hot path lean and the model lightweight-embedded. The directory structure below
maps directly onto these layers.

---

## 2. Project-Centric Directory Structure

GraphForge is a **project-centric knowledge analysis workbench**. The graph is one asset inside a larger analytical project — not the whole project.

### Capability-Based Structure

Folders are capability modules. An absent folder means that capability is not yet enabled. Projects start simple and grow capabilities over time.

| Template | topology | properties | documents | provenance | embeddings | indexes | workflows | artifacts | sync |
| ----------------------------- | -------- | ---------- | --------- | ---------- | ---------- | ------- | --------- | --------- | ---- |
| **Minimal** | yes | yes | — | — | — | — | — | — | — |
| **Exploratory investigation** | yes | yes | yes | yes | — | — | — | — | — |
| **Full workbench** | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| **Published canonical** | yes | yes | — | yes | — | — | — | — | — |

### Full Directory Tree

```
my-investigation/
├── graphforge.yaml          # project manifest (version, name, ontology path, ontology_mode, ir_version)
├── ontology.yaml            # domain ontology (optional — see Section 3.5)
│
├── topology/                # hot path — identity + type only, no properties
│   ├── nodes.parquet        # node_uuid, node_id, primary type_id, type_ids(List<UInt32>), timestamps
│   └── edges/               # typed edge tables — one file per relation type
│       ├── WORKS_AT.parquet    # edge_uuid, src_uuid, dst_uuid, edge_id(UInt64), src_id(UInt64), dst_id(UInt64), created_at, confidence, provenance_uuid
│       ├── OWNS.parquet
│       ├── MENTIONS.parquet
│       └── LOCATED_IN.parquet
│
├── properties/              # warm path — columnar property storage
│   ├── Person.parquet       # node_uuid, name, email, age, ...
│   ├── Organization.parquet # node_uuid, org_name, country, ...
│   └── Document.parquet     # doc_uuid, title, date, content_hash, ...
│
├── documents/               # raw source documents
│   ├── {doc_uuid}.pdf
│   └── {doc_uuid}.json
│
├── embeddings/              # vector stores, one subdirectory per model
│   ├── text-embedding-3/
│   │   └── {embedding_uuid}.parquet  # entity_uuid, vector FIXED_SIZE_LIST(Float32, N)
│   └── custom-model/
│
├── provenance/              # provenance events + lineage graph
│   ├── events.parquet       # provenance_uuid, kind, source_ref, analyst_id, created_at
│   └── lineage.parquet      # parent_provenance_uuid, child_provenance_uuid
│
├── indexes/                 # full-text search indexes (Tantivy)
│   └── Person/
│       └── tantivy/
│
├── workflows/               # saved analysis pipelines (serialised GraphPlan JSON)
│   └── {workflow_uuid}.json
│
├── artifacts/               # outputs — ranked lists, clusters, exported reports
│   ├── {artifact_uuid}_rank.parquet
│   └── {artifact_uuid}_cluster.parquet
│
└── sync/                    # changeset-based collaboration (post-v0.5)
    ├── local/               # outbound changeset queue
    ├── inbound/             # received changesets
    ├── checkpoints/         # last-synced state per remote
    └── merge_history/       # reconciliation log
```

Also present in exploratory mode:

```
├── topology/
│   └── edges/
│       └── _exploratory.parquet  # catch-all for unknown relation types
└── topology/
    └── runtime_catalog.parquet   # RuntimeCatalog: observed labels/types/properties
```

### Key Design Decisions

- **Typed edge tables** (`topology/edges/TYPENAME.parquet`) — each relation type has its own Parquet file. DataFusion can scan `WORKS_AT.parquet` directly without filtering a unified edges table. Critical for 100M+ edge graphs. In exploratory mode, unknown relation types fall back to `_exploratory.parquet`.
- **Topology/properties split** — `topology/` holds only identity + type; `properties/` holds columns. Enables column pruning: a graph traversal never reads property data unless the query explicitly projects it.
- **Typed property files** — one Parquet per entity type. Schema evolution is per-type. Adding a column to `Person.parquet` does not affect `Organization.parquet`.
- **Progressive ontology** — `ontology.yaml` is optional. See Section 3.5.
- **Capability modules** — folders are capabilities. Absent folders are not-yet-enabled, not misconfigured. A project can start as `topology/ + properties/` and add `provenance/`, `embeddings/`, etc. incrementally.

---

## 3. Ontology Model

The ontology is already implemented in `gf-ontology` (ontology runtime). This section documents how it integrates with the new storage architecture.

The `ontology.yaml` file in the project root is the schema authority. It drives:

- **The binder** — resolves label strings to TypeId/PropId integers
- **The relational lowering** — tells the planner which typed edge file to scan for a given relation type
- **The storage provider** — maps entity type names to `properties/TYPENAME.parquet` paths
- **Validation** — load-time validation of the graph against declared constraints

The ontology is **not** a database schema enforced on write. It is a semantic description that enables optimized planning and validation. Data that violates the ontology can still be stored; the planner and validator will flag violations.

**The ontology is optional.** See Section 3.5 for progressive ontology modes.

---

## 3.5. Progressive Ontology

GraphForge supports three ontology modes to accommodate the full analyst spectrum — from exploratory discovery to production-grade validated graphs. See [ADR 0003](../../adr/0003-progressive-ontology.md) for the full rationale and design.

### Three modes

| Mode | When | Binder behaviour | Violations |
| ------------- | --------------------------------------------- | ----------------------------------------------------- | -------------------------- |
| `exploratory` | No ontology.yaml present | Creates RuntimeTypeId for unknowns via RuntimeCatalog | None — all labels accepted |
| `advisory` | ontology.yaml present, mode unset or explicit | Warns on unknowns; adds to RuntimeCatalog | Warnings only |
| `strict` | `ontology_mode: strict` in graphforge.yaml | BindError on any unknown | Hard errors |

### `graphforge.yaml` manifest

The project manifest includes `ontology_mode` and an optional `ontology` path:

```yaml
# Exploratory project — no ontology required
project_uuid: "0195f3a2-..."
name: "Investigation Alpha"
version: "1"
ontology_mode: exploratory
ontology: null
ir_version: "0.1.0"
graphforge_version: "0.5.0"
capabilities: # absent = false; only list enabled capabilities
  topology: true
  properties: true
  documents: true
  provenance: true
```

```yaml
# Advisory project — ontology present but non-enforcing
project_uuid: "0195f3a3-..."
name: "Entity Graph v2"
version: "1"
ontology_mode: advisory # default when ontology.yaml present
ontology: "./ontology.yaml"
```

```yaml
# Strict project — fully validated
project_uuid: "0195f3a4-..."
name: "Production Knowledge Base"
version: "1"
ontology_mode: strict # must be explicitly declared
ontology: "./ontology.yaml"
```

### RuntimeCatalog

The RuntimeCatalog is an auto-growing registry present in all modes:

```
RuntimeCatalog
├── entity_types      label → RuntimeTypeId (UInt32, session-local)
├── relation_types    name  → RuntimeTypeId
├── property_names    name  → RuntimePropId
├── statistics        observation counts, first/last-seen timestamps
└── inference_hints   suggested ontology entries (for export)
```

Persisted to `topology/runtime_catalog.parquet`. Can be exported to `ontology.yaml` via `forge.suggest_ontology()`.

### Storage in exploratory mode

Typed edge tables require predefined relation type names. In exploratory mode:

- Unknown relation types write to `topology/edges/_exploratory.parquet` (includes `rel_type_name: Utf8` column)
- Known types (from RuntimeCatalog once promoted) use typed edge tables
- A maintenance operation can reorganise rows into typed files after the ontology is formalised

### Binder open-world resolution (exploratory/advisory modes)

```
bind(label: &str) → TypeId:
1. Check OntologyHandle (if present): return TypeId if found
2. Check RuntimeCatalog: return existing RuntimeTypeId if previously seen
3. New label: RuntimeCatalog::intern(label) → new RuntimeTypeId
4. Return RuntimeTypeId for use in GraphOp
```

In strict mode, step 3 is replaced by `BindError::UnknownLabel`.

### IR envelope

`GraphPlan.ontology_version` is optional:

```rust
GraphPlan {
    ir_version: IrVersion,
    dialect: String,
    ontology_version: Option<OntologyVersion>,  // None in exploratory mode
    ontology_mode: OntologyMode,                // exploratory | advisory | strict
    ops: Vec<GraphOp>,
    exprs: ExprArena,
}
```

### Migration path

```
exploratory → advisory → strict
```

Transitions are always the analyst's choice. Moving to a stricter mode never deletes data — it only adds validation.

---

## 4. Graph IR Model

The Graph IR (`gf-ir`) is the **stable semantic contract** between the compiler and the execution engine. It is deliberately DataFusion-independent — the IR can be serialized, stored, inspected, and replayed without DataFusion being present.

### Operators

| Operator | Description |
| ---------------------------------------------- | --------------------------------------------------------- |
| `NodeScan { var, ty }` | Scan all nodes of a type (or all types if `ty` is `None`) |
| `TypedEdgeScan { var, rel_ty }` | **NEW** — scan a specific typed edge table directly |
| `Expand { src, edge, dst, rel_ty, dir, hops }` | Traverse edges from a bound node |
| `Filter { predicate }` | Apply a boolean expression |
| `Project { items, distinct }` | Select and alias output columns |
| `Aggregate { group_by, aggs }` | Grouping aggregations |
| `Sort { keys }` | ORDER BY |
| `Limit / Skip` | Pagination |
| `Optional { child }` | OPTIONAL MATCH |
| `Union { all, inputs }` | UNION / UNION ALL |
| `Unwind { list_expr, alias }` | List expansion |
| `With { items, where_predicate }` | Pipeline stage |
| `Create / Merge { pattern }` | Write operations (relational lowering) |

`TypedEdgeScan` is the key addition for typed edge table storage — the binder emits this operator when a pattern specifies a relation type, bypassing a full edges scan.

> **Adjacency note (ADR 0004).** The graph-native adjacency index introduces **no new IR
> operator**. Variable-length traversal remains encoded on `Expand { …, min_hops, max_hops }`.
> Whether a traversal is executed via the adjacency index or a DataFusion hash join is a
> _lowering/planner_ choice with identical semantics — not part of this stable semantic
> contract. See [Execution Model](execution-model.md) §Adjacency-Backed Execution.

### IR Envelope

```rust
GraphPlan {
    ir_version: IrVersion,          // semver, current = 0.1.0
    dialect: String,                // "opencypher-9" | "gql-1"
    ontology_version: OntologyVersion,  // checksum of ontology used at bind time
    feature_flags: Vec<String>,
    ops: Vec<GraphOp>,
    exprs: ExprArena,
}
```

---

## 5. Identity Model

### Decision: UUIDv7 as Canonical Identity

**Every first-class object in GraphForge has a globally unique immutable identifier.**

The chosen format is **UUIDv7** (RFC 9562):

- Time-ordered: monotonically increasing within a millisecond window
- Globally unique without coordination
- 128-bit: fits in Arrow `FixedSizeBinary(16)`
- Suitable for Parquet row-group statistics (time-ordering enables range pruning)
- Supports offline generation on mobile devices, air-gapped systems, disconnected analysts

### Objects Requiring UUID Identity

Every object in the following categories receives a UUID at creation time and retains it for its entire lifecycle, regardless of where it moves or how the project is restructured:

| Object | UUID column name | Notes |
| --------------------- | ----------------- | --------------------------------- |
| Node (entity) | `node_uuid` | Canonical entity identity |
| Edge (relationship) | `edge_uuid` | Canonical relationship identity |
| Document | `doc_uuid` | Source document |
| Observation | `obs_uuid` | A recorded observation |
| Assertion | `assertion_uuid` | A derived or inferred claim |
| Provenance event | `provenance_uuid` | Lineage record |
| Analyst / User | `analyst_uuid` | Contributor identity |
| Project | `project_uuid` | Project-level identity |
| Workflow | `workflow_uuid` | Saved analysis pipeline |
| Embedding | `embedding_uuid` | Vector artifact |
| Source reference | `source_uuid` | External system / document source |
| Ranking output row | `rank_uuid` | Reproducible result reference |
| Clustering output row | `cluster_uuid` | Reproducible result reference |
| Generated artifact | `artifact_uuid` | Any analytical output |

### UUID + Surrogate Key Pattern

UUIDs are the **canonical stable identity**. Integer surrogate keys are a **per-session execution optimization**.

```
topology/nodes.parquet columns:
  node_uuid   BINARY(16)   -- UUIDv7, globally unique, immutable
  node_id     UInt64       -- local surrogate, assigned at load time
  type_id     UInt32       -- immutable primary label for property routing
  type_ids    List<UInt32> -- authoritative complete label set
  created_at  Timestamp
  updated_at  Timestamp

topology/edges/WORKS_AT.parquet columns:
  edge_uuid         BINARY(16)   -- UUIDv7
  src_uuid          BINARY(16)   -- references node_uuid
  dst_uuid          BINARY(16)   -- references node_uuid
  edge_id           UInt64       -- local surrogate
  src_id            UInt64       -- local surrogate (for DataFusion joins)
  dst_id            UInt64       -- local surrogate
  created_at        Timestamp
  confidence        Float64
  provenance_uuid   BINARY(16)
```

The relational lowering layer maps `node_uuid → node_id` once at scan time, then all DataFusion joins use integer surrogates. Results are projected back to UUIDs before returning to the user.

### Architectural Principle

```
Entity identity is globally unique.
Storage location is temporary.
Ownership is temporary.
Projects are temporary.
Identity is permanent.
```

---

## 6. Provenance Model (Knowledge Layer)

> **Layer + status (ADR 0005 / ADR 0006).** Provenance is a **knowledge-layer** concern, not a
> graph-layer one. Graph objects may carry UUID references; events and lineage live in
> `gf-provenance` / `gf-knowledge` and are produced/interpreted there — never as graph semantics.
> **Status:** knowledge-layer immutable ledger and epistemic append-only epistemic interpretation are **Shipped** for
> v0.5.0 (see [knowledge-ledger.md](knowledge-ledger.md) and
> [ADR 0006](../../adr/0006-epistemic-model.md)). The illustrative parquet schemas below remain a
> conceptual map; the authoritative machine-readable contracts are the generated knowledge/epistemic schema
> inventories under `docs/reference/`.

Every fact (node, edge, assertion, ranking output) carries a `provenance_uuid` that references an entry in `provenance/events.parquet`.

```
provenance/events.parquet:
  provenance_uuid   BINARY(16)    -- UUID of this provenance event
  kind              Utf8           -- "ingestion" | "inference" | "assertion" | "merge"
  source_ref        BINARY(16)    -- UUID of source document or system
  analyst_uuid      BINARY(16)    -- UUID of the analyst who created this
  rule_id           Utf8          -- ontology inference rule ID (nullable)
  confidence        Float64       -- confidence score for this event
  query_id          Utf8          -- UUID of the query that produced this fact
  created_at        Timestamp

provenance/lineage.parquet:
  parent_uuid       BINARY(16)    -- provenance event that contributed to this one
  child_uuid        BINARY(16)    -- downstream provenance event
  role              Utf8          -- "input" | "derived" | "merged"
  weight            Float64       -- contribution weight
```

The `conservative_min` confidence policy: confidence of a derived fact = min(confidence of inputs).

---

## 7. Storage Architecture

### Three Tiers

**Tier 1: Topology** (hot path — small files, frequently scanned)

The topology layer holds only identity and type. Graph traversal reads only this tier.

```
topology/nodes.parquet          -- all nodes, UUID + type + timestamps
topology/edges/TYPENAME.parquet -- one file per relation type
```

**Tier 2: Properties** (warm path — larger files, column-pruned)

Property access requires a join from topology UUID to property file.

```
properties/ENTITY_TYPE.parquet  -- one file per entity type, UUID + all properties
```

**Tier 3: Analytical Artifacts** (cold path — written once, read rarely)

```
documents/        raw source files
embeddings/       vector stores
provenance/       lineage records
indexes/          FTS indexes
workflows/        saved pipelines
artifacts/        ranked/clustered outputs
```

### Performance Implications by Scale

| Scale | Critical optimizations |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1M–10M edges | Current design sufficient. Typed edge tables provide modest gains. |
| 10M–100M edges | Typed edge tables become important — avoid scanning all edges to find one type. Topology/properties split reduces traversal memory. |
| 100M–1B edges | Typed edge tables are mandatory. Partition by UUID prefix (first byte) for parallel scans. Surrogate IDs critical (avoid 128-bit joins in hot path). Consider time-range partitioning for temporal queries. |
| 1B+ edges | Out of scope for v0.5. Would require distributed execution (DataFusion Ballista or similar). |

---

## 8. v0.5 capability layers

<a id="8-revised-milestone-plan"></a>

v0.5.0 ships as one product surface. The adjacency index ([ADR 0004](../../adr/0004-adjacency-index.md))
is a first-class derived layer that both Cypher variable-length expand and analyst verbs consume.
The table below names **capabilities**, not delivery staging labels.

| Capability | Status | Notes |
| --- | --- | --- |
| Architecture baseline | Shipped | UUIDv7 identity, typed edges, topology/properties split, progressive ontology (this document). |
| Graph IR | Shipped | `GraphOp` set incl. `TypedEdgeScan`; `Expand { min_hops, max_hops }` encodes variable-length. |
| Relational lowering | Shipped | `TypedEdgeScan` → typed-edge `TableProvider`; UUID→surrogate mapping; `VarLenExpandNode` logical stub. |
| Execution baseline | Shipped | Custom physical nodes incl. VarLenExpand. |
| Adjacency index | Shipped | Shared derived `AdjacencyProvider` + CSR under `indexes/adjacency/`. **No IR change.** See ADR 0004. |
| Bindings (Python + Node) | Shipped | Thin bindings; no adjacency dependency. |
| Conformance hardening | Shipped | openCypher TCK, fuzzing, and semantic conformance gates. |
| Analyst verbs | Shipped | `rank`, `cluster`, `similar`, `paths`, and `analyze` return stable Arrow over the shared adjacency provider. |
| Find / index | Shipped | Tantivy text, vector, and hybrid search under `indexes/`. |
| Knowledge layer | Shipped | Neutral algorithm invocation descriptors, projection/result fingerprints, durable `algorithm_run_uuid` without changing graph-native computation or Arrow schemas. |
| Epistemic model | Shipped | Point-in-time / competing-hypothesis projections before algorithm dispatch; assertions, evidence, and reasoning after dispatch. |
| v0.5.0 publication close | Open | Human-authorized final close after graph/knowledge boundary and release aggregate gates. |
| Swift / Kotlin bindings | Deferred (v0.5.1) | Shared UniFFI layer; intentionally follows the v0.5.0 release. |

The algorithm invocation boundary is normative in
[Algorithm Verbs](algorithms.md#invocation-and-knowledge-layer-boundary) and
[Execution Model](execution-model.md#reproducible-algorithm-execution). The analyst-verb path
receives only a resolved graph-native projection. The knowledge layer persists the neutral run
descriptor; the epistemic layer resolves belief projections before dispatch and attaches
interpretations afterward. Swift/Kotlin bindings remain planned for v0.5.1.

### Why adjacency is its own layer

Adjacency is the root dependency of all five analyst verbs and already exists ephemerally inside
VarLenExpand (`build_adjacency`). Building one shared derived index prevents a second throwaway
adjacency reader and keeps Graph IR and relational lowering unchanged. See ADR 0004.

### Capability dependency sketch

```
Execution
   ├──> Bindings (Python/Node)     (parallel)
   ├──> Conformance                (parallel)
   ├──> Find / index               (parallel)
   └──> Adjacency index ──> Analyst verbs ──> Knowledge layer
                                              │
                                              ▼
                                         Epistemic model
                                              │
                                              ▼
                                         v0.5.0 publication close
                                              │
                                              ▼
                                         Swift/Kotlin (v0.5.1)
```


---

## 9. Project format stance

v0.5.0 defines the supported project/container contract. GraphForge does not
import, migrate, or reinterpret unrecognized layouts. Unsupported roots fail
closed with `GF_UNSUPPORTED_PROJECT_FORMAT` before mutation. See
[Pre-v1 project compatibility](project-format-compatibility.md).

---

## 10. Performance Analysis

### Current Roadmap (flat node_facts/edge_facts)

```
MATCH (a:Person)-[:WORKS_AT]->(b:Organization)
→ Scan all edges → Filter type = WORKS_AT → Join with nodes
```

At 100M edges: scanning all edges to find one type wastes I/O. No row-group pruning possible on type column when all types are mixed.

### Revised Roadmap (typed edge tables)

```
MATCH (a:Person)-[:WORKS_AT]->(b:Organization)
→ TypedEdgeScan(WORKS_AT) → read WORKS_AT.parquet only → Join with topology
```

At 100M edges: if 1% of edges are WORKS_AT, we read 1M rows instead of 100M. 100x I/O reduction.

### UUID Impact

UUID overhead: 16 bytes vs 4 bytes per ID = 4x per ID column. Mitigated by:

- Surrogate integers for join columns (only UUID at API surface and for identity)
- Arrow dictionary encoding for repeated values
- Parquet binary column compression (UUIDs compress well with ZSTD)

Net additional storage: ~15–20% for UUID columns vs integer-only design. Acceptable given the merge/federation benefits.

---

## 11. Risks and Tradeoffs

| Decision | Benefit | Risk | Mitigation |
| ---------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| **UUIDv7 identity** | Merge-safe, offline-safe, globally unique | 16 bytes vs 4 bytes; slower integer joins | Surrogate integer IDs for execution joins; UUID only at API surface |
| **Typed edge tables** | Direct scans, no type filter, partition stats | More files, schema evolution per-type | Ontology drives file layout; `_exploratory.parquet` catch-all in exploratory mode |
| **Topology/properties split** | Column pruning, less traversal memory | Extra join for property access | DataFusion handles this as a hash jo; modern CPUs handle it cheaply |
| **Project-centric structure** | Multi-asset, future-proof, federation-ready | More complex than single graph file | graphforge.yaml manifest makes structure discoverable |
| **Conservative min confidence** | Simple, predictable, conservative | May undervalue high-quality inferences | Pluggable confidence policies; conservative_min is the safe default |
| **Progressive ontology (3 modes)** | Exploratory analysts can start immediately; structured projects get strict validation | Binder/storage provider complexity; advisory warning surface needed | Sensible defaults; RuntimeCatalog handles the exploratory→strict upgrade path |

---

## 12. Collaboration Architecture (post-v0.5)

Four concepts support multi-analyst collaboration. They are distinct and should remain so.

### Changesets — unit of change

A **changeset** is the atomic unit of knowledge contribution. It carries a UUIDv7, analyst UUID, timestamp, and contains one or more mutations (node/edge additions, property changes, assertions) plus their provenance.

### Sync — movement of changesets

**Sync** moves changesets between projects. It is transport-layer: local filesystem copy, HTTP push/pull, or air-gapped USB transfer.

```
sync/
├── local/          # outbound changeset queue
├── inbound/        # received changesets pending merge
├── checkpoints/    # last-synced state per remote
└── merge_history/  # reconciliation log
```

### Merge — semantic reconciliation

**Merge** reconciles overlapping knowledge. It operates on UUID identity and entity resolution, not file diffs. Provenance is preserved through merge; confidence scoring resolves conflicts. Merge is deterministic given the same input changesets regardless of arrival order.

### Federation — trust and policy boundary

**Federation** governs who can sync, what can sync, redaction rules, trust policies, audit requirements, and federated learning policies. Federation is not sync — it is the policy layer above sync.

### Readiness assessment (v0.5)

| Concept | Ready now | Post-v0.5 |
| -------------- | ------------------------------------------ | ------------------------------------------------ |
| **Changeset** | UUIDv7 identity, provenance model | Formal changeset envelope, signing |
| **Sync** | Project directory portable (zip and share) | Protocol, remotes, incremental transfer |
| **Merge** | UUID union of topology files | Entity resolution, confidence merge, conflict UI |
| **Federation** | — | Auth, trust policy, redaction rules, audit |

The foundational decisions (UUIDv7 identity, project-centric structure, typed edge tables, provenance model) make all four concepts architecturally possible. No structural changes will be needed — only new crates/services layered on top.

---

## References

- [Architecture Overview](overview.md)
- [AST & Planning](ast-and-planning.md)
- [Storage Architecture](storage.md)
- [Execution Model](execution-model.md)
- [Algorithms](algorithms.md)
- RFC 9562 — UUIDv7 specification
- GraphAr project — concepts evaluated but not adopted as dependency
