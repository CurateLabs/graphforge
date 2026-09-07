# ADR 0028: One transaction owns graph mutation effects

**Status:** Proposed
**Date:** 2026-09-07
**Related:** #1010; ADRs 0025 and 0026

## Context

Cypher's statement driver buffers writes, stages one rewrite, and assembles
counters and receipts. Its facade separately persists the observed runtime
catalog and publishes a generation. Analyst write-back duplicates catalog
interning, property staging, receipt construction, snapshot capture and rollback.
A property-write staging failure and an error after CURRENT publication must not
be handled as the same transaction outcome.

This is a correctness-first durable mutation boundary. Existing public Arrow
results, receipt meanings, checked identities and durable formats must remain
compatible. Clause order, pending-write visibility and pure-append adjacency
segments must also survive the extraction.

## Options

1. Share only the API publication wrapper. Small, but leaves duplicated staging,
   catalog, counters and receipt invariants; it does not satisfy #1010.
2. Extract a supported execution-owned mutation transaction and supply an API
   publication adapter. Reuses existing owners without a reverse dependency.
3. Move all transaction policy into storage. This would pull execution receipts,
   runtime observation policy and API domain publication into the wrong layer.

## Decision

Choose option 2, without adding a crate or a durable transaction format.

`graphforge-exec::mutation` owns a `MutationTransaction`: the working catalog
snapshot, neutral effect/counter accumulation, staged rewrite, and commit/abort
state. Both entry points use its property-write recording and receipt builder.
The Cypher statement context retains only clause/frontier evaluation and
pending-write visibility; its buffered topology/property operations feed this
same transaction. A property-only analyst transaction does not open a topology
writer or scan all label memberships merely to use the shared machinery.

Catalog interning occurs against a transaction-owned working catalog. Cypher's
binder receives that catalog before mutation binding; analyst property
interning uses the same owner. The catalog's persisted batch joins the graph
rewrite, rather than being written independently after a successful Cypher
commit. The live catalog is installed according to the final publication
outcome. Ordinary read-query observation policy is unchanged.

The API supplies a small publication adapter using its existing generation and
domain-participant methods. It does not call private executor phase functions.
The adapter captures the authoritative parent generation, publishes the neutral
receipt, restores or reconciles the workspace when required, and installs the
resulting property/ordinal authority. Transaction orchestration, rather than
each verb, invokes these operations. Standalone execution retains its existing
local staged-commit behavior through the same core without API publication.

The shared transaction owns the complete sequence:

1. Acquire existing write admission and establish parent/catalog state.
2. Bind or validate against the working catalog; accumulate graph operations,
   existing property-counter semantics and deterministically ordered neutral effects.
3. Stage graph and catalog changes together. Reuse existing RewriteBatch and
   topology/UUID-index commit primitives, including adjacency delta behavior.
4. Commit the local rewrite, then publish through the API adapter when present.
5. On success, install the catalog and refresh/invalidate retained graph
   resources through one completion path. Preserve selected-resource authority.
6. On any error, including a local rewrite commit failure, use the same abort
   path. Restore the parent workspace/catalog only while the parent remains
   authoritative. If publication already advanced CURRENT, preserve committed
   data and reconcile the live catalog/resource authority; never undo a
   published generation because a later operation returned an error.

Every GraphForge facade, including in-memory construction, initializes a
project generation before executing writes. Its ordinary publishing operations
therefore have an authoritative parent, including the first write to an empty
graph. Standalone exec targets and unpublished transaction workspaces can lack
an authoritative snapshot of their current contents. Those paths require a
private disk-backed rollback copy of the admitted workspace, with validated
file inventory and streaming/reflink materialization, before any local commit.
They must never silently run without rollback merely because CURRENT is absent.
This fallback does not build a whole-graph Arrow byte envelope in memory.

Rollback uses the authoritative committed generation where available; analyst
write-back no longer captures a whole-workspace Arrow snapshot for this purpose.
Errors determining publication state fail closed; they do not authorize a
speculative restoration. Existing project publication remains the durable
atomicity boundary, rather than a second commit protocol in exec.

## Validation and consequences

Baseline tests first pin repeated SET, CREATE-then-SET and no-op counter behavior.
A shared Rust-facade test runs equivalent Cypher SET and real rank/cluster
write-back on matching seeded graphs. It compares normalized receipt fields,
existing property counters (including repeated SET and CREATE-then-SET), catalog entries, stored values and generation effects.
Receipt capture stays test-internal; no new public facade result is introduced.

The same matrix injects failures during staging/local commit, before CURRENT,
and after CURRENT. It checks pre-publication rollback of both data and catalog,
post-publication preservation, resource invalidation and durable reopen. Include
an existing nonempty graph, no-op/empty write-back, repeated SETs, mixed Cypher
CREATE/SET/REMOVE/DELETE/MERGE, and semantic composition routing. Existing query
parity and bounded cache tests remain required.

This is one concern under #1010. Implement the shared state/staging core first,
route both entry points through it, then delete the duplicated catalog/receipt/
rollback paths only after the common acceptance matrix passes. The API remains
the owner of generation/domain publication and exec remains independent of API.
