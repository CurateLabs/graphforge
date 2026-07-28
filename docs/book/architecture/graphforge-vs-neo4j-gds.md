# GraphForge v0.5 and Neo4j with Graph Data Science

**Status:** Product and architecture comparison
**Last Updated:** 2026-07-27

---

## Purpose and status vocabulary

GraphForge and Neo4j Graph Data Science (GDS) overlap, but they organize the work around
different primary objects. Neo4j starts with an operational graph database and brings analytics
to it. GraphForge v0.5 starts with a portable analytical project and embeds query, search, and
analysis inside it.

This is a comparison of the complete systems, not an algorithm checklist. It is also a
description of current tradeoffs, not a claim that GraphForge is a smaller or faster Neo4j.

GraphForge capabilities are labeled as follows (same legend as other architecture docs):

- **Shipped:** implemented and tested on `main`.
- **Partially built:** some paths real, others stubbed.
- **Designed:** specified in architecture documents but not yet a complete public capability.
- **Deferred:** intentionally after v0.5.0 (for example Swift/Kotlin bindings).
- **Possible direction:** useful design space, not a roadmap commitment.

Neo4j descriptions refer to the current official
[Neo4j GDS manual](https://neo4j.com/docs/graph-data-science/current/).

## Different centers of gravity

```text
Neo4j + GDS                              GraphForge

applications                             documents + graph + ontology
     │                                               │
     ▼                                               ▼
transactional graph database              portable project directory
     │                                               │
     ▼                                               ▼
named in-memory GDS projection             embedded Rust engine
     │                                      ├─ openCypher
     ▼                                      ├─ analyst algorithms
algorithm / model                           ├─ text and vector search
     │                                      └─ Arrow execution
     ▼                                               │
stream / stats / mutate / write                     ▼
                                           Arrow results + versioned assets
```

The **database** is the durable center of Neo4j. The **project** is the intended durable center
of GraphForge:

```text
Project = Graph + Documents + Ontology + Provenance + Embeddings + Workflows + Artifacts
```

Only part of that GraphForge project equation is a full product surface in every deployment.
Graph topology and properties, Parquet storage, the Rust query engine, native algorithm
execution, search, embedding generations, and the knowledge/epistemic knowledge layer (provenance,
confidence, epistemic interpretation) are **Shipped** for v0.5.0. Broader workbench concerns
such as multi-user changeset sync and mature workflow/artifact productization remain designed
or future work. Their presence in the project layout must not be read as current feature
parity with a mature server platform.

## Whole-system comparison

| Dimension | GraphForge v0.5 | Neo4j with GDS |
| ------------------------- | -------------------------------------------------- | ------------------------------------------------------------ |
| Primary unit | Portable analytical project | Long-running graph database |
| Deployment | Embedded Rust engine with Python and Node bindings | Neo4j server plus GDS plugin, or managed Aura |
| Durable graph storage | Columnar Parquet project files | Native transactional property-graph store |
| Analytical representation | Rust-owned selected adjacency/projection | Named specialized in-memory graph catalog |
| Query surface | Embedded openCypher-compatible `execute()` | Mature Cypher server |
| Algorithm surface | Analyst verbs (`rank`/`cluster`/`paths`/`analyze`/`similar`) + `find` returning Arrow | Algorithm-specific Cypher procedures |
| Search | Rust-owned text, vector, and hybrid retrieval | Database indexes, vector indexes, and GDS-derived properties |
| Public identity | Stable UUIDs; numeric surrogates stay internal | Database entities and GDS node identifiers |
| Result boundary | Canonical Arrow tables | `stream`, `stats`, `mutate`, and `write` modes |
| Collaboration model | Local project today; changeset sync is future work | Shared server, users, roles, clustering, and administration |
| Transactional workload | Not a mature multi-user OLTP database | Core product strength |
| Offline/local use | Natural operating mode | Possible, but requires a database runtime |
| Operational maturity | Embedded Rust core with thin Python/Node bindings | Mature database and analytics platform |

## Storage proximity and analytical projection

Native algorithms remove an external system boundary, but they do not make graph analysis
free of projection or memory costs.

GraphForge stores durable topology and properties in Parquet. Columnar storage is effective for
portable, typed analytical data, but traversal algorithms need an adjacency-oriented
representation. GraphForge therefore resolves selectors, builds or reads its derived adjacency
state, maps public UUIDs to execution surrogates, runs the Rust kernel, and maps results back to
UUIDs. The [adjacency-index ADR](../../adr/0004-adjacency-index.md) describes that derived state.

GDS similarly does not run its algorithms directly over Neo4j's transactional representation.
It loads a selected projection into a specialized
[in-memory graph catalog](https://neo4j.com/docs/graph-data-science/current/introduction/).
Named projections can be reused by multiple algorithms and remain until dropped or until their
hosting instance stops.

### GraphForge strengths

- No network export into a separate Python, Spark, or analytics service.
- One implementation owns selector resolution, UUID mapping, cancellation, limits, errors, and
  result shaping.
- The project can be opened in-process by a local application without database administration.
- Parquet and Arrow make project data and results portable across analytical tools.

### GraphForge weaknesses

- Parquet is not itself a traversal-optimized graph store; adjacency preparation remains real
  work and consumes memory.
- Derived adjacency and search state require explicit freshness contracts.
- v0.5 does not offer a user-managed, reusable analytical graph catalog comparable to GDS.
- The new Rust implementations have less production history and large-graph tuning than GDS.

### GDS strengths

- A mature, optimized analytical graph representation designed for repeated server-side runs.
- Named projections can be filtered, transformed, reused, monitored, and shared within the
  GDS lifecycle.
- Strong parallel execution and operational tooling for large managed deployments.

### GDS weaknesses

- The stored graph and analytical projection are distinct states with separate memory and
  lifecycle costs.
- Database changes do not automatically become changes in an existing projection.
- Installation, compatibility, memory sizing, users, and server or Aura administration are
  part of operating the system.

## Results are different product models

The difference between canonical Arrow tables and the four GDS execution modes is not just API
syntax. It determines where analytical state lives and how one computation feeds the next.

### GraphForge: canonical Arrow tables

**Shipped/active v0.5 contract:** `execute`, `rank`, `cluster`, `paths`, `analyze`, `similar`,
and `find` expose typed Arrow results. The Rust engine owns their semantics, and Python and Node
are thin consumers of the same columnar result. See the [execution model](execution-model.md)
and [API reference](../../reference/api.md).

This gives GraphForge one language-neutral boundary:

```text
query / algorithm / search
             │
             ▼
schema + Arrow RecordBatch stream
             │
     ┌───────┼─────────┬──────────┐
     ▼       ▼         ▼          ▼
  Python    Node    DataFusion   Parquet/IPC
```

The advantages are:

- One schema contract across query, algorithms, search, and language bindings.
- Direct composition with pandas, Polars, DataFusion, Arrow IPC, and Parquet.
- Stable UUID identity in public output rather than exposed execution surrogates.
- No algorithm-specific result object or driver-owned conversion model.
- A computation can remain read-only; returning data does not silently persist it.

GraphForge also distinguishes a returned result from a published asset. For example, an algorithm
embedding algorithm returns a canonical UUID/vector table. Publishing that complete table as an
embedding-space-space generation is a separate, validated, atomic operation. Publication does not
mutate the original result or graph properties. See
[embedding-space publication](embedding-v1.md#embedding-space-publication).

The current contract also has meaningful limitations:

- There is no general **stats-only** execution mode. A caller interested only in a summary may
  still pay to produce or consume a detailed result before aggregating it.
- There is no general equivalent of GDS **mutate**: algorithms cannot add temporary properties
  or relationships to a reusable analytical projection and immediately chain another algorithm
  over that changed projection.
- Only `rank` and `cluster` expose opt-in graph-property write-back. There is no universal
  algorithm **write** mode.
- Arrow makes tabular composition easy, but a caller or future workflow layer must explicitly
  join an Arrow result back into a graph input when the next algorithm needs it.
- A canonical table does not by itself provide a server-side result catalog, shared model
  catalog, access control, job management, or result retention policy.
- Although execution is batch-streamed internally, some binding-level APIs expose a completed
  table or IPC payload. Very large results therefore still require careful memory accounting.

These are deliberate simplifications for v0.5, but they are not universally superior to GDS's
stateful analytical workspace.

### GDS: `stream`, `stats`, `mutate`, and `write`

GDS exposes algorithms through four
[execution modes](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/):

| Mode | Result destination | Best fit | Principal tradeoff |
| -------- | --------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `stream` | Rows returned to the caller | Inspection, top-N selection, application consumption | Large results cross the client boundary |
| `stats` | Summary row only | Cost, distribution, and quality inspection without full output | Detailed result is unavailable |
| `mutate` | Property or relationship added to the in-memory projected graph | Chaining algorithms without database write-back | State exists only in the projection and consumes catalog memory |
| `write` | Property, label, or relationship persisted to Neo4j | Repeated database queries and durable operational use | Database transaction/write cost; an existing projection does not automatically see the write |

This is a stronger built-in lifecycle for server-side algorithm composition. A PageRank result
can be mutated into a projected graph and consumed by another algorithm without round-tripping
through an application. A final result can then be written to the database.

The cost is a more stateful model. Users must know whether a value lives in a streamed result,
the GDS projection, the model catalog, or the database. The projected graph can diverge from the
database, consumes separate memory, and has its own ownership and lifetime. GDS documentation
also notes that a database `write` must be reprojected before an existing analytical graph can
use the new value.

### Results-model tradeoff

| Question | GraphForge Arrow contract | GDS execution modes |
| -------------------------------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Where does a read result live? | Caller-owned Arrow data | Client rows or a summary |
| Can algorithms chain without client orchestration? | Limited today | Yes, through `mutate` |
| Is persistence implicit? | No; publication/write-back is explicit and limited | Explicit `write` mode |
| Is summary-only work first class? | No general mode today | Yes, `stats` |
| Is interoperability first class? | Yes, through Arrow | Primarily Cypher/driver APIs; Arrow import/export is available in GDS Enterprise |
| Is analytical state centrally managed? | No general catalog today | Yes, graph and model catalogs |
| Is stale state possible? | Yes, in derived indexes/assets; guarded by generation and fingerprint contracts | Yes, between the database and named projections |
| Is schema uniform across operations? | One Arrow-based family of typed schemas | Output depends on algorithm and execution mode |

Neither model dominates every workflow. GraphForge favors explicit, portable dataflow. GDS
favors a managed, stateful server-side analytical workspace.

## Query, algorithms, search, and ML

Neo4j has a much more mature Cypher implementation, optimizer, transactional engine, procedure
ecosystem, and operational history. GDS provides a broad catalog of parallel algorithms and
machine-learning pipelines. Its GraphSAGE workflow, for example, can train a model, retain it in
a model catalog, and apply it to compatible graphs.

GraphForge v0.5 owns openCypher-compatible compilation, native Rust analyst verbs, and hybrid
search inside one embedded engine. Its analyze embedding contracts favor deterministic,
invocation-local computation and canonical Arrow output. This is simpler to package and
reproduce, but it is not yet a substitute for GDS model management, pipeline tuning, or
large-server execution.

Algorithm-name overlap therefore does not establish product parity. The meaningful GraphForge
value is that queries, algorithms, and search share the same project identity, errors, resource
controls, and Arrow boundary without requiring an external analysis runtime.

## Project scope, knowledge, and provenance

GraphForge's intended project boundary includes documents, ontology, evidence, provenance,
embeddings, workflows, and artifacts in addition to graph topology. Its layered architecture
keeps graph algorithms independent of knowledge state: a belief status or evidence record cannot
silently change a graph-native algorithm result. Higher layers may resolve an explicit graph
projection before execution and record claims about the result afterward.

Knowledge-layer provenance and epistemic records and resolved dispatch are shipped v0.5 contracts.
This is not a claim that v0.5 ships a complete investigation or collaboration platform.
Neo4j applications can model these concepts in a graph or
integrate external document and workflow systems, but GDS does not prescribe GraphForge's
Epistemic boundary. Conversely, GraphForge does not currently match Neo4j's mature database
security, transactions, administration, and shared-service behavior.

## When each approach fits

### Prefer GraphForge when

- The analytical project must be local, portable, offline, or embedded in another product.
- Arrow, Parquet, dataframe, or multi-language interoperability is a primary requirement.
- Reproducible selectors, UUID identity, deterministic contracts, and explicit publication are
  more important than a shared graph server.
- Query, graph analysis, text search, and vector search should ship as one application component.
- Operating a database service and a separate analytics environment would be disproportionate.

### Prefer Neo4j with GDS when

- The graph is a shared operational system of record.
- Concurrent transactions, authentication, authorization, clustering, backup, and monitoring
  are required.
- Large analytical projections and parallel algorithms need mature server-side optimization.
- Analysts need graph/model catalogs, `mutate` pipelines, ML training, and database write-back.
- The organization already operates Neo4j and benefits from its ecosystem.

### Use both when

- GraphForge is the portable investigative or offline workspace while Neo4j is the shared
  operational graph.
- Arrow or Parquet snapshots move bounded analytical projections into GraphForge and curated
  outcomes are published back through an explicit integration.
- Local reproducibility and server-scale collaboration are both requirements, with ownership and
  freshness rules defined at the boundary.

Using both is not automatically beneficial. It reintroduces synchronization, identity mapping,
access control, and partial-failure concerns that an embedded-only workflow avoids.

## Possible future directions for GraphForge

The following would close specific workflow gaps without requiring GraphForge to imitate every
Neo4j server feature. They are **possibilities, not v0.5 commitments**:

- A stats execution option that can avoid full result publication when a kernel supports
  bounded summaries.
- Lazy `RecordBatchReader` APIs consistently exposed across bindings.
- A workflow layer that passes typed Arrow outputs into subsequent projections explicitly.
- Versioned analytical projections or immutable derived-property overlays.
- General result publication policies with compatibility, provenance, and atomicity contracts.
- Materialized analytical artifacts with retention and reproducibility metadata.
- Shared project services only where local project locking and changesets are insufficient.

Arrow should remain the public interoperability contract unless a future architecture decision
changes it. A stats result can still be an Arrow table; a projection overlay can still be stored
as UUID-keyed Arrow/Parquet data. The open question is lifecycle and ownership, not whether
GraphForge needs a bespoke binary result format.

## Bottom line

Neo4j plus GDS brings mature analytics to an operational graph database. GraphForge brings
query, native analysis, and search to a portable analytical project.

GraphForge v0.5 is strongest as an embedded, Arrow-native, local-first engine with explicit
identity and reproducibility contracts. It is weakest where a mature database platform is
expected: concurrent transactional workloads, shared administration, model and graph catalogs,
cluster operations, and years of large-scale optimization.

Native algorithms are essential to GraphForge because they keep the analytical loop inside the
project. They do not, by themselves, make GraphForge equivalent to Neo4j with GDS. The larger
product thesis is a different unit of ownership: the complete analytical project rather than the
database server.

## References

### GraphForge

- [Architecture overview](overview.md)
- [Execution model](execution-model.md)
- [Storage architecture](storage.md)
- [Algorithm verbs](algorithms.md)
- [Embedding v1 contract](embedding-v1.md)
- [ADR 0004: Adjacency index](../../adr/0004-adjacency-index.md)
- [ADR 0005: Layered architecture](../../adr/0005-layered-architecture.md)

### Neo4j

- [GDS introduction and graph catalog](https://neo4j.com/docs/graph-data-science/current/introduction/)
- [Running algorithms and execution modes](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/)
- [Graph management](https://neo4j.com/docs/graph-data-science/current/management-ops/)
- [GDS installation and deployment](https://neo4j.com/docs/graph-data-science/current/installation/)
- [Node embeddings](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/)
