---
title: "ADR 0058: Initial builds run on a bulk builder derived from the published generation"
adr: "0058"
status: "Accepted"
date: "2026-10-07"
superseded_by: null
revisit_when: "An initial build must exceed the in-memory budget before the scratch path lands, a published artifact stops being a projection of the three ranked inputs, or a chunk-API initial build needs the same speedup"
---

# ADR 0058: Initial builds run on a bulk builder derived from the published generation

**Status:** Accepted

**Implementation:** #1883 (slice of epic #1881).

**Related:**
- ADR 0013 (project generation protocol; the `CURRENT` swap is unchanged)
- ADR 0038 (determinism at the publication boundary; property 1 is amended below)
- ADR 0046 (construction keeps its own sorting and partitioning)
- ADR 0047 (one construction CPU budget)
- ADR 0056 (shaping stays serial within stages; this record removes shaping for initial builds instead)
- ADR 0057 (node-index endpoint resolution, which the builder keeps)
- #1387 (ingest floor), #1881 (epic)

## Context

An initial import stages every row, shapes it by routing it through range
partitions, sorts it, resolves endpoints, and only then encodes the published
generation. At Graph500 S22 the published project is 55 B/edge, while
construction writes about 600 B/edge of intermediates, performs about 270,000
fsyncs, and runs at 1.1–1.25 effective cores on a 16-thread host (#1881,
measured). Each removal measured under #1387 stayed below its 10% adoption gate
because the cost is spread over every stage (ADR 0056).

For an initial build the published artifacts are projections of three things:

1. the node UUIDs in sorted order, where `node_id` is a node's rank;
2. the edge UUIDs in sorted order, where `edge_id` is an edge's rank;
3. `(src_id, dst_id)` per edge.

Nodes Parquet, the ordinal v4 blocks, the node surrogates, the edge tables, the
runtime catalog, and both CSR directions all follow from them. The stages
between the input and those three arrays exist to compute them out of core.

## Decision

An initial build computes the three arrays once, in memory, with dense `u32`
ids, and emits every canonical artifact from them. It produces exactly the
artifact inventory the staged encoder produces, so the publisher cannot tell
the builds apart.

### Passes

0. **Plan.** Row counts and tasks come from the source footers. The plan
   decides, from the sources' columns, whether a kind carries properties.
1. **Nodes.** Row groups decode in parallel. Each batch passes the same intake
   normalization an append gets. The builder checks sortedness (sorted input
   skips the sort), sorts otherwise, rejects duplicates, and ranks.
2. **Edges.** Row groups decode in parallel. Endpoints resolve through an
   open-addressing index from node UUID to rank (a refinement of ADR 0057's
   index). Edges sort and rank the same way. A missing endpoint, an endpoint
   that names an edge, an edge UUID that equals a node UUID, and a repeated edge
   UUID are refused.
3. **Emit.** Catalog, node and edge Parquet windows, property overlays, the
   UUID membership index, the v4 ordinal artifacts and the adjacency CSR are
   built from the ranked arrays. Windows and CSR shards are independent once
   ranks exist and encode in parallel. Each entry of a CSR direction is
   `key << 32 | edge_id`; sorting those orders every node's list by `edge_id`.
   Shard boundaries follow the streamed writer's rule, so shard bytes match.
4. **Publish.** The existing publication path installs the artifacts into the
   CAS and swaps `CURRENT` (ADR 0013). It is unchanged.

Published bytes are hashed (SHA-256 and XXH64) from the bytes about to be
written. Intermediates are never synced or hashed. Routing is explicit and
decided at plan time: property-bearing and typed-ontology input take the same
passes, through the staged encoder's own per-batch property and ownership
functions.

### Scope

- The builder serves `import-session` initial builds (Parquet and Arrow IPC
  sources). Appends keep the staged path, as do initial builds made through the
  chunk API (`GraphConstructionSession::append_*`), until the builder takes
  those inputs.
- The builder is in-memory. An initial build whose estimated peak memory
  exceeds the plan-time budget takes the staged path instead, which holds a
  fixed window of memory and produces the same bytes. The route is chosen once,
  by the first `validate`, from the footers and the process's cgroup-aware
  memory headroom (three fifths of it), and written to the import manifest
  (`build_route`); every later `validate`, in any process, reads it back. A
  refused, cancelled or killed bulk attempt therefore cannot be re-routed to the
  staged path by a change in free memory. It is never a retry. The scratch path of #1881 replaces
  this routing for large inputs, and must land before the ladder slice.
- Node and edge counts are limited to 2^32 - 2 by the dense ids. A larger input
  is refused with a resource-limit error.

## Restart instead of resume (amends ADR 0038 property 1)

ADR 0038 property 1 said a resumed run produces the same graph as an
uninterrupted one. For an initial build the builder stages nothing, so there is
nothing to resume. A crash, cancellation or error discards the build's scratch,
and the next `validate` reruns from the registered sources. Property 1 becomes:
*a rerun after a crash produces the same graph as an uninterrupted run.* The
rerun reuses nothing from the interrupted attempt except encoded objects that
the checkpoint already pins. Properties 2 and 3 are unchanged, and publication
atomicity and the preservation of the prior `CURRENT` are untouched. A session
whose earlier binary began staging, or whose project already has a generation,
keeps the staged path and its resume semantics.

## Consequences

**Enables.** Complete ingest scales with the number of cores instead of with the
serial shaping spine. The CPU the staged path spends on routing, sorting and
spill file lifecycle is removed from initial builds.

**Costs.**
- Memory: every task decodes straight into its slice of the final columns, so
  the resident columns are the 28 bytes per edge the ranked graph needs (UUID,
  two endpoint ranks, relation id), and only one CSR direction's 8-byte entries
  are resident at a time. Measured peak RSS fits `671 MB + 24.2 B/edge + 72.8
  B/node` for property-free input (Graph500 S18-S24 and two S22 node sets with
  fewer edges; worst measured/fitted ratio 1.14): S22 peaks at 2.3 GiB and S24
  at 7.8 GiB. The planner uses `768 MiB + 26 B/edge + 76 B/node` plus a 25%
  margin, so S22 plans for 3.6 GB, S24 for 11.3 GB, S25 for 21.6 GB and S26 for
  42.3 GB. A property-bearing kind retains its decoded batches and two copies of
  them: six times their uncompressed footer bytes, measured at 5.5. A source
  whose UUIDs arrive unsorted also needs an order array and one gathered
  column at a time (about 20 B/edge more) while it sorts.
- The session checkpoint records an empty shape for a builder session. The
  inventory's `shape_*` digests therefore differ from a staged build of the
  same input; they are session authority, not published bytes.
- Published artifacts are byte-identical to the staged build's for the same
  input and session clock, with one exception: `ordinal-v4-receipt.json`
  carries a random rebuild nonce (named by ADR 0038), in both builds.

**Does not change.** Intake refusals, the publication protocol, the format of
any published artifact, appends, and the chunk API.
