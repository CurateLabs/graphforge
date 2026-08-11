# ADR 0005: Layered Architecture — Graph / Knowledge / Workbench

**Status:** Accepted
**Date:** 2026-06-07
**Build target:** v0.5.0
**Related:** ADR 0001 (Rust Core), ADR 0003 (Progressive Ontology), ADR 0004 (Adjacency Index), ADR 0006 (Epistemic Model), ADR 0012 (knowledge/epistemic Domain Ownership)

---

## Context

GraphForge is positioned throughout its documentation as a **lightweight, embedded, local-first
knowledge analysis workbench** — not a graph database, not a server, not a distributed system. The
graph is the load-bearing core, but it is **one asset inside a larger analytical project**, not the
whole product.

That positioning appears in the requirements and architecture overview. Without an explicit
**architectural decision**, there is no named boundary between:

- the **graph** (nodes, edges, properties, traversal, Cypher), and
- the **knowledge** an analyst accretes around the graph (provenance, confidence, evidence,
  epistemic status), and
- the **workbench** experience the analyst works through (the analyst verbs, search, workflows,
  exploration).

Two concrete failure modes motivate the boundary:

1. **Schema drift toward the graph.** Fusing `props_json`, `confidence`, `provenance_id`, and
   valid-time columns into one node-fact row mixes graph, knowledge, and temporal concerns.
   Topology must stay identity + type only; properties and knowledge live elsewhere
   (`crates/graphforge-storage/src/schemas.rs`).

2. **Knowledge concerns with no home.** Provenance, confidence, evidence, and the evolution of
   an analyst's understanding are not graph concerns. Without a knowledge layer they drift as
   unowned columns on graph tables and bloated hot paths.

Without explicit layering, every new capability risks becoming more columns on the graph —
contaminating graph semantics and eroding the lightweight model.

This ADR establishes the layering and, critically, the **boundary rule** that keeps the graph
graph-native.

---

## Decision

GraphForge is organised into **three layers**. Each owns distinct concerns, and lower layers never
depend on higher ones.

```
┌──────────────────────────────────────────────────────────────────┐
│  WORKBENCH LAYER                                                   │
│  Analyst experience: forge.rank / cluster / paths / analyze /      │
│  similar / find · hybrid search · workflows / recipes ·            │
│  exploration · project portability                                 │
│  — consumes the layers below; holds NO graph-semantic state        │
└───────────────────────────────┬──────────────────────────────────┘
                                 │ consumes (Arrow + UUID references)
┌───────────────────────────────┴──────────────────────────────────┐
│  KNOWLEDGE LAYER                                                   │
│  provenance · confidence · evidence · ontology-inference lineage · │
│  epistemic assertions + status + supersession + valid-time         │
│  (ADR 0006)                                                        │
│  — attaches to graph objects BY UUID REFERENCE ONLY                │
└───────────────────────────────┬──────────────────────────────────┘
                                 │ references (UUID), never embeds
┌───────────────────────────────┴──────────────────────────────────┐
│  GRAPH LAYER                                                       │
│  nodes · edges · properties · traversal · pattern matching ·       │
│  graph algorithms · adjacency (ADR 0004)                           │
│  — graph-native; surrogate-keyed execution; UUID identity          │
│  — stores NO knowledge or workbench semantics                      │
└────────────────────────────────────────────────────────────────────┘
```

### Layer responsibilities

| Layer | Owns | Crates / storage (today) | Must NOT hold |
|---|---|---|---|
| **Graph** | Nodes, edges, properties, traversal, pattern matching, graph algorithms, adjacency index | `graphforge-cypher`, `graphforge-ir`, `graphforge-rel`, `graphforge-exec`, `graphforge-storage` (`topology/`, `properties/`, `indexes/adjacency/`) | confidence semantics, epistemic status, evidence, reasoning |
| **Knowledge** | Provenance events + lineage, confidence + policy, evidence links, ontology-inference lineage, epistemic assertions/status/supersession/valid-time | `graphforge-provenance` + `graphforge-knowledge` (**Shipped**); `provenance/`, `knowledge/` Parquet | graph topology, traversal logic, Cypher semantics |
| **Workbench** | Analyst verbs, hybrid search, workflows/recipes, exploration, project envelope | `graphforge-api`, bindings, search modules | graph-semantic state, persisted knowledge logic |

### The boundary rule (load-bearing)

> **Knowledge attaches to the graph by UUID reference, never by embedding.**

- The graph layer stores identity (`*_uuid`), type, topology, and properties. It does **not** store
  what a fact *means epistemically*, how confident we are, what evidence supports it, or how belief
  evolved.
- The knowledge layer stores those concerns in its **own tables**, each row referencing graph
  objects by their `*_uuid`. A node carries a `node_uuid`; a confidence score, a provenance event,
  or an epistemic assertion about that node lives in the knowledge layer and points back via
  `node_uuid`.
- The workbench layer **consumes** both lower layers and returns Arrow. It holds no persisted
  graph-semantic state; a verb is a function over the graph + knowledge layers, not a new place to
  store graph meaning.

### Consequence for graph-native execution

Cypher execution, traversal, and graph algorithms operate **only** on the graph layer. The presence
or absence of knowledge-layer data (provenance, confidence, epistemic history) **must never change
the result of a graph-native query**. This is testable and is enforced as a boundary regression test
(see ADR 0006): a graph with full epistemic history returns identical Cypher results to the same
graph without it.

---

## "Which layer does X belong to?" checklist

When adding a capability, ask in order:

1. **Is it about nodes/edges/properties/traversal/pattern-matching/graph-algorithms?** → Graph
   layer. Keep it surrogate-keyed and graph-native. Do not add knowledge columns.
2. **Is it about where a fact came from, how confident we are, what supports it, or how the
   analyst's understanding evolved?** → Knowledge layer. Store it in its own table, referencing
   graph objects by UUID. Never as a column on a topology table.
3. **Is it about how the analyst *works* — a verb, a search, a workflow, an exploration, a
   result shape?** → Workbench layer. Implement it as a consumer of the layers below; persist no
   graph semantics.

If a capability seems to span layers, split it: the graph part stays in the graph layer, the
meaning/evidence part goes to the knowledge layer, the experience part goes to the workbench.

---

## Consequences

### Positive

- **Graph stays lean and fast.** The hot traversal path reads only `topology/` (and the adjacency
  index), never knowledge columns. This protects the lightweight-embedded model.
- **Knowledge becomes a real subsystem**, not unowned columns — the home for provenance,
  confidence, and the epistemic model (ADR 0006).
- **Clear extension story.** Future capabilities have an obvious home, reducing the risk of
  graph-semantic contamination.
- **Testable boundary.** "Knowledge never changes graph results" is a concrete regression test.

### Negative / Risks

- More tables and a join (graph ↔ knowledge by UUID) where previously a column would have sufficed.
  Mitigated by: knowledge tables are capability-gated and optional; the graph hot path never reads
  them; UUID joins are bounded and local.
- A discipline cost: contributors must classify each capability by layer. Mitigated by the checklist
  above and the boundary regression test.

### Lightweight-embedded impact

None negative. The layering is conceptual + storage-organisational; it introduces no server, no
service, no infrastructure. Knowledge and workbench layers are local Parquet + in-process code, the
same as the graph layer. If anything, keeping knowledge out of the graph hot path *protects*
embedded performance.

---

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| **No explicit layering (status quo)** | Knowledge concerns drift onto graph tables as unowned columns; graph semantics get contaminated; the hot path bloats. The two concrete symptoms above are the result. |
| **Two layers (graph + everything-else)** | Conflates "what an analyst believes" (knowledge) with "how an analyst works" (workbench). Workbench verbs would end up owning persistence logic that belongs to the knowledge layer. |
| **Knowledge as graph properties** | Embeds confidence/provenance/epistemic status as node/edge columns. Violates the boundary rule, slows traversal, and makes the graph collapse to current state (kills the epistemic model). |

---

## References

- [Architecture Overview](../book/architecture/overview.md) — layer diagram and per-layer sections
- [Architecture Refactor v0.5](../book/architecture/refactor-v0.5.md) — layering subsection, §6 knowledge layer
- [Storage Architecture](../book/architecture/storage.md) — `provenance/` and `knowledge/` tables
- [ADR 0001: Rust Core](0001-rust-core.md) — crate layout
- [ADR 0004: Adjacency Index](0004-adjacency-index.md) — a graph-layer derived accelerator
- [ADR 0006: Epistemic Model](0006-epistemic-model.md) — the knowledge-layer epistemic extension
- [ADR 0012: knowledge/epistemic Domain Ownership](0012-knowledge-domain-ownership.md) — crate DAG,
  table ownership, schema registries, evolution, and reference validation
