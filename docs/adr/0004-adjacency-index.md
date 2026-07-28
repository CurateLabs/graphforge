# ADR 0004: Graph-Native Adjacency Index

**Date:** 2026-06-07
**Status:** Accepted
**Build target:** v0.5.0

---

## Context

GraphForge is a **Knowledge Analysis Workbench**. Its core workloads — neighborhood
discovery, variable-length traversal, reachability, path finding, and graph algorithms —
are fundamentally *adjacency-oriented*. The v0.5 storage architecture
([refactor-v0.5.md](../book/architecture/refactor-v0.5.md), [storage.md](../book/architecture/storage.md))
makes analytical scans efficient via typed edge tables, but graph traversal still resolves
adjacency through relational execution patterns.

Two independent paths need the same adjacency view:

1. **VarLenExpand (Cypher traversal).** Without a shared index, `build_adjacency()` would
   rebuild a `HashMap<u64, Vec<(edge_id, neighbor_id)>>` **from a full typed-edge scan on
   every query**. The BFS (`bfs_emit`) then walks that map. Cost is paid per query and scales
   with total edges, not with the neighborhood actually visited.

2. **Analyst verbs.** `rank`, `cluster`, `paths`, `analyze`, and `similar` need the *same*
   typed edge tables as an `AdjacencyGraph` for the `AlgorithmBackend` trait
   (`export_adjacency(label, via, directed, weight)`).

Without a shared derived index, Cypher traversal and analyst verbs would each own a
**divergent adjacency reader**. Unifying them behind one rebuildable `AdjacencyProvider`
consolidates scope rather than adding it.

The key question is not "can GraphForge ship without an adjacency index?" (it can). It is
whether a single index reduces net complexity across both consumers — it does.

---

## Decision

Introduce a **graph-native adjacency index** as a derived execution artifact, and route both
the Cypher traversal path and the analyst-verb path through a single `AdjacencyProvider`
abstraction.

### Canonical vs derived

- **Parquet topology remains the sole source of truth.** A graph is fully valid and queryable
  with **no** adjacency artifacts present.
- The adjacency index is **derived, rebuildable, versioned, and optional**. It is an execution
  accelerator, never a correctness dependency.
- A stale or missing index MUST never produce incorrect results — only slower ones. The
  provider detects staleness (topology generation mismatch) and falls back to scan-and-build.

### Surrogate-only

Adjacency structures operate **exclusively** on `node_id` / `edge_id` / `src_id` / `dst_id`
(UInt64 surrogates). UUIDs are public identity and are resolved only at the API boundary, as
they already are throughout execution. This matches the existing `build_adjacency` contract.

### One provider, no duplication

Both consumers obtain adjacency through one `AdjacencyProvider`:

- VarLenExpand stops calling its private `build_adjacency()` and asks the provider.
- `export_adjacency` becomes a thin adapter from the provider to `AdjacencyGraph`,
  not a second Parquet reader.

After this decision there is exactly **one** adjacency implementation in the codebase.

### No Graph IR change

This is deliberately a **storage + execution** concern, invisible to the semver-versioned
Graph IR. No new `GraphOp` is introduced. Variable-length traversal continues to be encoded
on the existing `Expand { …, min_hops, max_hops }` operator. Whether a traversal uses the
adjacency index or a DataFusion hash join is a *lowering/planner* choice with identical
semantics — selected by a lowering rule, not by the IR.

Physical accelerator operators for the Cypher path (`ShortestPathExec`, `ReachabilityExec`)
are explicitly **out of scope** for v0.5 — they are physical-only and deferred until a Cypher
shortest-path surface needs them. The analyst verbs already dispatch to algorithm backends.

### Storage location: `indexes/adjacency/`

The index lives under `indexes/`, **not** `topology/`. Rationale:

1. `topology/` is defined as the canonical, hot-path, identity-and-type-only tier. Derived data
   there would erode the "topology is the source of truth" invariant.
2. `indexes/` is already the established capability module for derived, rebuildable acceleration
   structures (Tantivy FTS lives there). Adjacency is the same *kind* of artifact.
3. Capability-module semantics fit exactly: absent `indexes/adjacency/` = build in-memory
   on-demand (today's behavior); present = load from disk. Never required.

On-disk layout (full schema in [storage.md](../book/architecture/storage.md) §Derived Indexes):

```
indexes/
└── adjacency/
    ├── index_manifest.parquet       # topology_generation, built_at, relation_types[], directions[]
    ├── <REL_TYPE>.out.csr           # CSR: offsets (UInt64) + targets (struct{edge_id, neighbor_id})
    ├── <REL_TYPE>.in.csr            # reverse direction
    └── _all.out.csr                 # union across relation types
```

CSR (compressed sparse row) is keyed by surrogate `node_id`. Build = stream typed-edge files
once, sort by `src_id`, write offsets/targets. Staleness = `topology_generation` counter
bumped on every topology write; provider compares and rebuilds (or builds in memory) on
mismatch.

---

## Consequences

### Positive

- **Net complexity reduction.** Cypher traversal and analyst verbs share one adjacency
  provider instead of two divergent builders.
- **Neighborhood-proportional traversal.** Variable-length expansion cost scales with the
  visited frontier + neighborhood, independent of total edge count — the headline success
  criterion.
- **No per-query full edge scan.** VarLenExpand does not rebuild adjacency from a full scan on
  every call when a fresh index is present.
- **Shared foundation for analyst verbs.** `rank`, `cluster`, `paths`, `analyze`, and
  `similar` reuse the same adjacency layer as Cypher traversal.
- **IR stability preserved.** No change to the shipped, versioned Graph IR — only additive
  execution/storage work plus one lowering rule.

### Negative / Risks

- One new capability module + CSR format + manifest + versioning logic to maintain (bounded;
  mirrors the existing Tantivy precedent under `indexes/`).
- A new lowering rule adds planner surface (additive, isolated).
- Adjacency-backed and join-backed paths must provably agree — requires a differential
  correctness corpus in CI.

### Mitigations

- **Differential correctness corpus** (adjacency vs DataFusion join) enforced in CI.
- **Staleness fallback**: a stale/missing index falls back to scan-and-build with identical
  results — the index can only affect speed, never correctness.
- **Incremental rebuild deferred to v0.5.1** with the gap logged; v0.5.0 ships full-rebuild +
  lazy/explicit build, which is sufficient for the success criteria.
- Scope discipline: no IR operators; no physical Cypher accelerator operators in v0.5.

---

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| **Defer to a later release** | Analyst verbs would ship a standalone `export_adjacency` while VarLenExpand kept a per-query full-scan builder, leaving two divergent adjacency paths plus a later consolidation project — strictly more total work than doing it once. |
| **New IR operators (`ShortestPathExec`, `ReachabilityExec` as `GraphOp`s)** | Leaks physical execution strategy into the stable semantic contract, violating the layering ADR 0001 establishes. Adjacency is a lowering/execution choice. |
| **Store adjacency under `topology/adjacency/`** | Erodes the "topology is identity+type only, and canonical" invariant. `indexes/` already owns derived, rebuildable accelerators. |
| **Require the index (make it canonical)** | Breaks the rebuildable/optional principle and the offline/merge story; a graph must remain valid with no derived artifacts. |

---

## References

- [Architecture Refactor v0.5](../book/architecture/refactor-v0.5.md) — §4 Graph IR, §7 Storage
- [Storage Architecture](../book/architecture/storage.md) — §Derived Indexes (adjacency layout)
- [Execution Model](../book/architecture/execution-model.md) — adjacency-backed execution
- [ADR 0001: Rust Core](0001-rust-core.md) — names variable-length path expansion as a Rust-core driver
