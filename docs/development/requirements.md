# Lightweight openCypher-Compatible Graph Engine

## Requirements Document (Draft)

**Last Updated:** 2026-06-07

> **Note on scope evolution.** GraphForge has reoriented to its true north: a lightweight, embedded,
> local-first **knowledge analysis workbench**. The v0.5.0 release ships the *basic-but-complete*
> embodiment of that vision — the graph engine **plus** a real knowledge layer (provenance,
> confidence, evidence) and epistemic integrity (preserving the evolution of understanding). The
> requirements below add the layering, knowledge-layer, epistemic-integrity, and explicit
> lightweight-embedded invariants that this reorientation makes first-class. See
> [ADR 0005](../adr/0005-layered-architecture.md) and [ADR 0006](../adr/0006-epistemic-model.md).

---

## 1. Purpose

This document defines the functional and non-functional requirements for a **lightweight, embedded, openCypher-compatible graph engine** designed specifically for **research, investigative, and analytical workflows** in Python-centric data science and machine learning environments.

**This project implements a declared subset of the openCypher specification, validated via the openCypher Technology Compatibility Kit (TCK), rather than claiming full language coverage.**

The system is intentionally scoped to support **graph materialization and graph analytics as intermediate analytical steps**, not as a long-lived production database. It provides a standardized, portable, and semantically correct way to work with graphs during information extraction, investigation, and exploratory analysis.

---

## 2. Standards & Compatibility

### 2.1 openCypher Alignment

The engine MUST:
- Parse and validate queries using the openCypher grammar
- Follow openCypher semantic rules for pattern matching, filtering, and projection
- Maintain compatibility with the openCypher Technology Compatibility Kit (TCK) for supported features

The engine MUST NOT:
- Introduce proprietary Cypher extensions in the core language
- Silently accept unsupported syntax or semantics

### 2.2 TCK Compliance Model

- The project SHALL define a clear **TCK feature coverage matrix**
- Each openCypher feature SHALL be explicitly categorized as:
  - Supported
  - Unsupported (with defined failure behavior)
- Unsupported features MUST:
  - Fail deterministically
  - Produce clear, descriptive, spec-aligned errors

---

## 3. Design Principles

- Embedded-first (no server or daemon)
- Local-first (single-node execution)
- Graph-native execution (no relational joins)
- Spec-driven correctness over performance
- Deterministic and reproducible results
- Inspectable storage and execution behavior
- Python-first developer experience

The design philosophy mirrors SQLite: minimal operational overhead, stable APIs, and replaceable internals.

---

## 4. Intended Usage & Scope

### 4.1 What This Project Is

This system is a **knowledge analysis workbench** for:
- Beginning with uncertainty and discovering structure over time (exploration-first)
- Materializing extracted entities and relationships into a property graph
- Iteratively refining and revising that graph
- Querying and analyzing graph structure using openCypher
- Executing graph algorithms via compiled backends (NetworkX, igraph, native)
- Performing hybrid retrieval (full-text search, vector similarity) over graph properties
- Progressively formalizing discovered structure into ontology and repeatable workflows

It is designed to live *inside* Python workflows such as:
- Notebooks
- Research scripts
- Agentic or LLM-driven pipelines
- Investigative and OSINT analysis workflows

### 4.2 What This Project Is Not

This system is explicitly NOT:
- A raw-data ingestion platform (it curates *analytical knowledge*, not ingestion pipelines)
- An information extraction system
- A production graph database
- A graph-serving backend
- A distributed or multi-tenant service

> **Clarification (scope reconciliation).** "Not an ingestion platform" means GraphForge does not own
> raw-source ETL or entity extraction. It does **not** mean GraphForge is conclusion-only: curating
> the *analytical knowledge* around a graph — provenance, confidence, evidence, competing hypotheses,
> and the evolution of understanding — **is** in scope and is the knowledge layer (§19, ADR 0005/0006).

---

## 5. Canonical Workflow Pattern (Scoped)

This project operates exclusively as an **intermediate analytical layer**.

### Upstream Context (Out of Scope)

The following steps are assumed to occur outside this system:

1. Data ingestion from structured or unstructured sources
2. Entity and relationship extraction (including probabilistic or noisy outputs)

No assumptions are made about how entities or relationships are produced.

### Core Responsibility (In Scope)

#### 3. Graph Materialization

The system MUST support:
- Creation of nodes and relationships from extracted data
- Iterative updates, corrections, and revisions
- Durable but disposable graph persistence
- Multiple experimental or competing graph states

Graphs at this stage may be incomplete, inconsistent, or exploratory.

#### 4. Graph Exploration & Analytics

The system MUST support:
- Pattern matching using openCypher
- Structural exploration of graphs
- Identification of:
  - Variations
  - Outliers
  - Structural anomalies
  - Unexpected relationships

This phase is explicitly analytical and investigative.

### Downstream Context (Out of Scope)

The system does NOT handle:
- Final data curation or validation
- Long-term systems of record
- Production database serving
- Feature stores or ML model hosting

---

## 6. Data Model Requirements

### 6.1 Nodes

- Each node MUST have:
  - A unique internal identifier
  - Zero or more labels
  - Zero or more properties
- Node identity MUST be stable within a transaction

### 6.2 Relationships

- Each relationship MUST have:
  - A unique internal identifier
  - A source node
  - A destination node
  - Exactly one relationship type
  - Directionality
  - Zero or more properties

### 6.3 Properties

Properties MUST support openCypher value types:
- Integer
- Float
- Boolean
- String
- Null
- List
- Map

Null propagation and comparison semantics MUST follow the openCypher specification.

---

## 7. Data Models, Schemas, and Ontologies

### 7.1 Purpose

The system MUST support optional **data models** (also referred to as ontologies or schemas) that provide semantic structure over nodes and relationships without imposing rigid database-style schemas.

These models are intended to:
- Standardize meaning across investigative and extraction workflows
- Improve consistency in graph materialization
- Enable validation and tooling in Python-based environments
- Remain flexible enough for exploratory and probabilistic data

### 7.2 Compatibility Requirements

Data models MUST be expressible in formats compatible with:
- Pydantic models
- JSON Schema (draft-agnostic, best-effort)

This ensures interoperability with:
- Python data validation tooling
- LLM extraction pipelines
- External schema and ontology tooling

### 7.3 Scope of Enforcement

Data models:
- MUST be **optional**
- MUST NOT be required to create or query graphs
- MUST NOT prevent insertion of incomplete or uncertain data by default

Schema enforcement SHOULD be:
- Advisory rather than mandatory
- Configurable by the user (e.g. strict vs permissive modes)

### 7.4 Modeling Capabilities

Data models SHOULD be able to define:

- Node types (conceptual classes)
- Relationship types
- Allowed properties and value types
- Optional vs required properties
- Inheritance or specialization (where supported by the model format)

These models MAY be used to:
- Validate extracted entities and relationships
- Annotate nodes and relationships with semantic meaning
- Assist in query formulation and interpretation

### 7.5 Relationship to Cypher Semantics

Data models MUST:
- Remain orthogonal to openCypher semantics
- NOT alter Cypher query meaning or execution results
- Provide metadata and validation layers only

Cypher queries MUST operate on graph data regardless of whether a data model is present.

---

## 7. Query Language Requirements

### 7.1 Supported Constructs

The engine supports full openCypher as validated by the openCypher TCK (3,885/3,885 scenarios passing):

- MATCH, OPTIONAL MATCH (nodes, relationships, directionality, variable-length paths)
- WHERE (boolean logic, comparisons, property access, list predicates)
- RETURN, WITH (expressions, aliases, multiple projections, DISTINCT)
- CREATE, SET, REMOVE, DELETE, MERGE
- UNWIND
- ORDER BY, SKIP, LIMIT
- Pattern comprehension
- Temporal types (date, datetime, duration)
- All standard openCypher functions

### 7.2 Not Supported

The following are explicitly not planned:

- CALL procedures — graph algorithms use `db.gds.*` Python methods instead
- Graph-level DDL (CREATE INDEX, DROP, schema constraints)
- Stored procedures / user-defined functions via Cypher

Unsupported syntax MUST fail deterministically with clear error messages.

---

## 8. Execution Engine Requirements

The execution engine MUST:
- Operate on graph-native primitives
- Use adjacency-based traversal
- Implement operators for:
  - Node scanning
  - Relationship expansion
  - Filtering
  - Projection
  - Limiting
- Preserve openCypher semantics throughout execution

Query planning MAY be rule-based; cost-based planning is out of scope.

---

## 9. Storage Engine Requirements

### 9.1 Implementation Approach

The storage layer SHALL use **SQLite** as the persistence backend.

**Rationale:**
- SQLite provides ACID transactions, WAL mode, and crash recovery out-of-the-box
- Zero operational overhead (embedded, single-file, zero-config)
- Battle-tested durability (20+ years, billions of deployments)
- Cross-platform compatibility with no external dependencies
- Aligns with "mirrors SQLite" design philosophy
- Allows focus on openCypher execution rather than storage implementation

See `docs/storage-architecture-analysis.md` for detailed analysis.

### 9.2 Storage Requirements

The storage layer MUST:
- Be durable across crashes (SQLite WAL mode)
- Support atomic commits (SQLite transactions)
- Use WAL journaling (SQLite `PRAGMA journal_mode=WAL`)
- Support snapshot isolation for readers (SQLite WAL mode)
- Store adjacency lists explicitly (graph-specific schema design)
- Preserve stable internal IDs (application-managed ID generation)

The storage engine MUST remain opaque to Cypher semantics.

---

## 10. Concurrency Model

- Single writer at a time
- Multiple concurrent readers
- Readers MUST see only committed state
- Writers MUST not observe partial writes

---

## 11. Python API Requirements

GraphForge exposes three independent API surfaces:

```python
# Cypher surface — openCypher query engine
rows = db.execute("""
MATCH (n:Person)
WHERE n.age > 30
RETURN n.name
LIMIT 5
""")

# Algorithm surface — compiled graph algorithms
db.gds.pagerank(write_property="pr")
scores = db.gds.triangle_count()          # stream mode, no mutation
top10 = db.execute("MATCH (n) RETURN n ORDER BY n.pr DESC LIMIT 10")

# Search surface — hybrid retrieval
nodes = db.search.fts("Alice land transaction")
nodes = db.search.hybrid("Alice Nguyen", embedding_vector, top_k=10)
db.search.index_all(node_label="Person", properties=["name", "bio"])

# Recipes — composable helper functions
from graphforge.recipes import neighbourhood
context = neighbourhood(db, "alice", hops=2)
```

API guarantees:
- Synchronous execution
- Deterministic results
- Typed exceptions for parse, validation, and execution errors
- Reusable database handle
- All three surfaces share a single storage layer (in-memory or SQLite)

---

## 12. Testing & Validation

### 12.1 openCypher TCK Integration

- The openCypher TCK MUST be integrated into continuous integration (CI)
- TCK tests MUST be runnable in an automated, reproducible manner
- Each TCK test MUST be explicitly classified as:
  - **Pass** (fully supported and compliant)
  - **Skip** (feature intentionally unsupported)
  - **Expected Failure** (known limitation, documented)

A public **TCK Coverage Matrix** MUST be maintained and versioned with the codebase.

### 12.2 Regression Testing

- All supported openCypher features MUST have regression tests
- Regression tests MUST ensure semantic stability across releases
- Storage durability MUST be tested across process restarts

---

## 13. Non-Functional Requirements

### Performance

- Correctness prioritized over throughput
- Target scale (best effort):
  - ~10^6 nodes
  - ~10^7 relationships

### Portability

- macOS, Linux, Windows
- Python 3.10 or newer

### Observability

- Inspectable query plans
- Configurable debug logging
- Documented storage layout

---

## 14. Explicit Non-Goals

This system is NOT intended to:
- Fully implement the openCypher language
- Replace production graph databases
- Serve as a long-running graph service
- Achieve full TCK coverage in v1
- Support high-concurrency OLTP workloads
- Introduce Cypher dialect fragmentation
- Add Cypher syntax extensions for algorithms or search — `db.gds` and `db.search` operate outside the Cypher executor and do not modify openCypher semantics

---

## 15. Success Criteria (v1)

The project is successful if:
- A user can materialize a graph from extracted entities and relationships
- Execute valid openCypher MATCH queries within the declared feature set
- Pass the corresponding subset of the openCypher TCK
- Persist graphs across restarts
- Use the system entirely embedded, without external services

---

## 16. Comparison to Existing Approaches

This project intentionally occupies a middle ground between in-memory graph libraries and production-scale graph databases. The following comparisons clarify why neither "just using NetworkX" nor running an external graph database fully satisfies the intended use cases.

---

### 16.1 Comparison: Using NetworkX Alone

**What NetworkX Provides**

NetworkX is an excellent Python library for:
- Graph algorithms (centrality, clustering, paths)
- Rapid prototyping
- In-memory graph manipulation

However, NetworkX is explicitly **not a graph engine**. It lacks several capabilities that are critical for investigative and analytical workflows at scale.

**Limitations of NetworkX for These Use Cases**

- No durable storage (graphs must be serialized manually)
- No standardized query language
- No declarative pattern matching
- No snapshot isolation or transactional semantics
- No schema or semantic enforcement
- Poor reproducibility across sessions without custom glue code

As a result, NetworkX graphs tend to become:
- Ephemeral
- Ad-hoc
- Difficult to share or reproduce
- Tightly coupled to specific scripts or notebooks

**How This Project Differs**

This system complements NetworkX rather than replacing it:
- Provides durable, inspectable graph storage
- Supports declarative pattern matching via openCypher
- Enforces consistent graph semantics
- Enables reproducible analytical workflows

In v0.4.0, GraphForge adds `db.gds.*` methods that dispatch to NetworkX or igraph as computational backends. NetworkX transitions from a pure downstream consumer to a pluggable algorithm backend — one option in the backend priority stack (native → igraph → NetworkX). The `to_networkx()` + `set_node_properties()` pattern remains the escape hatch for algorithms not in the `db.gds` catalog.

---

### 16.2 Comparison: External Graph Databases (Neo4j, Memgraph, etc.)

**What External Graph Databases Provide**

Production graph databases excel at:
- Long-lived, authoritative graph storage
- High-performance traversals
- Concurrent multi-user access
- Operational robustness

They are optimized for **serving applications**, not exploratory analysis.

**Limitations in Research & Investigative Contexts**

For Python-based research workflows, external graph databases introduce significant friction:

- Require separate processes or services
- Impose operational overhead (installation, configuration, lifecycle)
- Break notebook-local execution models
- Encourage premature schema and data-model finalization
- Make iterative or disposable graphs costly to manage

These systems assume that the graph is:
- Clean
- Stable
- Long-lived
- Worth operational investment

This assumption does not hold during information extraction or investigative analysis.

**How This Project Differs**

This system:
- Runs entirely embedded within Python
- Requires no external services
- Encourages iterative, revisable graph construction
- Treats graphs as analytical artifacts, not systems of record
- Optimizes for semantic correctness and reproducibility over throughput

Export to production graph databases is explicitly supported *after* analytical refinement.

---

### 16.3 Summary Comparison

| Dimension | NetworkX | External Graph DBs | This Project |
|--------|----------|-------------------|--------------|
| Execution Model | In-memory | External service | Embedded |
| Durability | None (manual) | Persistent | Persistent |
| Query Language | None | Cypher | openCypher |
| Graph Semantics | Weakly enforced | Strong | Strong |
| Iterative Analysis | Excellent | Poor | Excellent |
| Operational Overhead | Minimal | High | Minimal |
| Notebook-Friendly | Yes | No | Yes |
| Production Serving | No | Yes | No |

This project exists specifically to fill the analytical gap between these two extremes.

---

## 17. Cypher Support Clarification (Non-Normative)

To avoid ambiguity, the following clarifications apply:

- openCypher compatibility refers to **semantic correctness for supported features**, not total language coverage
- Unsupported clauses and expressions MUST fail explicitly and deterministically
- The absence of a feature does not imply partial or degraded semantics for supported features

Users should expect:
- Strong semantic guarantees within the supported subset
- Clear error messages for unsupported syntax
- Gradual, explicit expansion of Cypher coverage over time

---

## 18. Why This Exists (README Excerpt)

### The Problem

Modern data science, machine learning, and investigative workflows increasingly rely on **entities and relationships** extracted from messy, probabilistic sources: text, tables, OCR, logs, and LLM outputs. While these workflows naturally produce **graph-shaped data**, practitioners are forced into poor tooling choices:

- In-memory graph libraries (e.g. NetworkX) that lack durability, semantics, and declarative querying
- Production graph databases that impose operational overhead, rigidity, and premature commitment

As a result, graph-based analysis during research and investigation is often ad-hoc, non-reproducible, and tightly coupled to one-off scripts.

---

### The Gap

There is a missing middle layer between:

- **Ephemeral in-memory graphs** used for algorithms
- **Production graph databases** used as systems of record

This gap is where most investigative and information-extraction work actually happens.

Researchers and ML engineers need a way to:
- Materialize extracted entities and relationships into a graph
- Iteratively revise and explore that graph
- Ask declarative, pattern-based questions
- Do all of this **inside Python**, without running external services

---

### The Idea

This project provides a **lightweight, embedded, openCypher-compatible graph engine** designed specifically for that gap.

It is:
- Embedded and local-first (no server)
- Graph-native (adjacency-based execution)
- Declarative (openCypher subset)
- Durable but disposable
- Designed for analytical, not operational, workloads

Rather than replacing production graph databases, it acts as a **graph workbench**:

- Build and revise graphs during extraction and investigation
- Explore structure, patterns, and anomalies
- Export refined results into systems like Neo4j or Memgraph *after* analysis

---

### What This Is (and Is Not)

**This is:**
- A standardized, portable environment for graph-based analysis
- A Cypher-compatible execution engine for research workflows
- A bridge between extraction pipelines and production systems

**This is not:**
- A production graph database
- A high-concurrency graph service
- A replacement for Neo4j, Memgraph, or TigerGraph

---

### Why openCypher

openCypher provides a widely understood, declarative way to reason about graphs.

By aligning with the openCypher specification and validating behavior with the openCypher TCK (for supported features), this project ensures:
- Semantic correctness
- Portability of queries
- Low friction when moving results to production systems

---

### Philosophy

> We are not building a database for applications.
> We are building a graph execution environment for thinking.

---

## 19. Layered Architecture & Boundaries

See [ADR 0005](../adr/0005-layered-architecture.md).

- The system MUST be organised into three layers with strict boundaries: **graph**
  (nodes/edges/properties/traversal/algorithms/adjacency), **knowledge** (provenance, confidence,
  evidence, epistemic assertions), and **workbench** (analyst verbs, search, workflows, exploration).
- The graph layer MUST NOT store knowledge or workbench semantics. Knowledge MUST attach to graph
  objects **by UUID reference only**, never as embedded columns on topology tables.
- Graph-native query results (Cypher, traversal, algorithms) MUST NOT depend on the presence or
  absence of knowledge-layer data. This MUST be enforced by a boundary regression test.
- The workbench layer MUST hold no persisted graph-semantic state; verbs are functions over the
  graph + knowledge layers.

## 20. Knowledge Layer Requirements

See [ADR 0005](../adr/0005-layered-architecture.md). **(v0.5.0)**

- Provenance events MUST be recorded for derived facts (not left NULL) via the shipped
  `graphforge-provenance` write path during execute/CREATE.
- Confidence MUST be computed and propagated through execution per a **pluggable policy**, default
  `conservative_min` (a derived fact's confidence = min of its inputs).
- Evidence MUST remain attached to assertions/observations regardless of later conclusions; refuting
  a claim MUST NOT detach its supporting evidence.

## 21. Epistemic Integrity Requirements

See [ADR 0006](../adr/0006-epistemic-model.md). **(v0.5.0 — Full scope)**

- The system MUST preserve the **evolution** of understanding, not merely the current state.
- Competing hypotheses about the same question MAY coexist (shared `hypothesis_group`); the system
  MUST NOT collapse knowledge to a single surviving interpretation by construction.
- Rejection or supersession of a claim MUST preserve the prior assertion, its evidence, and its
  reasoning (preservation-over-deletion). No destructive overwrite/delete path may exist for
  epistemic records; retraction is recorded, not deleted.
- The system MUST capture reasoning (why a conclusion was reached / why an alternative was rejected).
- Epistemic state MUST be queryable, including **point-in-time belief** ("what did we believe, when,
  and why did it change?") via a **bitemporal valid-time** model (assertion-time + transaction-time).
- Bitemporal querying MUST be capability-gated and MAY be off by default, with a documented
  assertion-time-only fallback if footprint/complexity is a concern on a given project.

## 22. Lightweight-Embedded Invariants (Normative)

These constraints are part of the product vision and MUST hold for every feature:

- **Zero-configuration:** `pip install` / `cargo add` then immediate use; no setup step required.
- **Embedded:** runs in-process; no server, daemon, or background service.
- **Local-first:** single-node execution; no network or cloud dependency required to operate.
- **Single-user by default:** no multi-user/multi-tenant requirement (single-writer, multi-reader).
- **Infrastructure-free:** no database administration, no external services required.
- **Portable:** a project is a self-contained directory (or single file) that can be zipped and
  shared; all capabilities (graph, knowledge, workbench) persist as local Parquet/files.
- Any new capability MUST be evaluated against these invariants; a capability that compromises them
  MUST be capability-gated and off by default, with a lighter fallback documented.

---

## 23. Future Considerations (Non-Binding)

- Native compiled algorithm backend (Rust) for `db.gds`
- sqlite-vec ANN vector search backend for `db.search`
- `EXPLAIN` / `PROFILE` query plan output
- Thread-safe `GraphForge` instances (connection pooling)
- Operator streaming pipeline (row-at-a-time, bounded memory)
- Large graph support (> 20M edges)

