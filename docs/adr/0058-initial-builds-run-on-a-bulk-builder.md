---
title: "ADR 0058: Initial builds run on a bulk builder derived from the published generation"
adr: "0058"
status: "Accepted"
date: "2026-10-07"
superseded_by: null
revisit_when: "Node identity tables must go out of core, a published artifact stops being a projection of the three ranked inputs, or a chunk-API initial build needs the same speedup"
---

# ADR 0058: Initial builds run on a bulk builder derived from the published generation

**Status:** Accepted

**Implementation:** #1883, #1900 and #1916 (slices of epic #1881).

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
- An initial build whose estimated peak memory fits the plan-time budget keeps
  everything resident. One that does not runs the same passes through scratch
  files (below), so peak memory stays inside the budget. The budget is three
  fifths of the process's cgroup-aware memory headroom, or the bytes in
  `GF_BULK_BUILD_MEMORY_BUDGET_BYTES`. Routing is a function of the footers and
  the budget, never of the data, and the bytes are the same on every route.
- The staged path remains for appends, chunk-API sessions, sessions an earlier
  binary began staging, and `node_tables_exceed_budget`: the identity tables,
  labels, endpoint index and degree arrays need a conservative 56 bytes per
  node alongside the fixed workspace. Property payload size does not contribute
  to this identity-table footprint. Out-of-core node identities remain open
  under #1881. The historical `edge_properties_exceed_budget` manifest reason
  remains readable but new builds do not select it.
- The route is chosen once, by the first `validate`, and written to the import
  manifest (`build_route`); every later `validate`, in any process, reads it
  back. A refused, cancelled or killed bulk attempt therefore cannot be
  re-routed to the staged path by a change in free memory. It is never a
  retry. Whether the bulk route then runs in memory or on scratch is decided
  again on each attempt from the live budget; either produces the same bytes.
  If that budget can no longer hold the bulk route's node tables or minimum scratch workspace, the attempt returns a resource-limit refusal before loading
  data. It keeps the bulk route and can retry when the budget is sufficient.

## Scratch route

When the estimate exceeds the budget:

- Pass 2 decodes the edges once, resolves endpoints through the node index and
  scatters a 28-byte record (UUID, source rank, target rank, relation id) into
  edge-UUID range partitions. Exact, non-null Parquet row-group bounds feed a
  histogram over the stated UUID span; otherwise boundaries come from a sample
  of up to 64 evenly spread tasks. These are initial estimates. The scatter
  also observes each partition’s actual UUID bounds. An oversized partition
  streams once into radix children at the first byte where its observed bounds
  differ, skipping any shared prefix. Oversized children repeat that step;
  leaves appear in UUID order and each fits its worker’s reservation. Equal
  bounds on an oversized range prove a duplicate identity. The source is never
  reread for refinement, and a chain has at most sixteen radix steps.
  Consecutive small child ranges coalesce through one streaming output, so
  bookkeeping follows the number of bounded partitions rather than the radix
  fanout. Already-fitting initial ranges keep their original scratch files.
- Pass 3 builds the partitions in order, several at a time. Sorting a partition
  ranks its edges (the first `edge_id` is the number of earlier edges plus
  one). It checks identities, writes its canonical edge files, keeps its sorted
  UUIDs for the membership index, and scatters its adjacency entries once into
  node-range partitions bounded by exact node degrees. A node larger than a
  partition spans consecutive partitions split by its increasing edge occurrence ordinal, so a hub
  cannot force all of its adjacency into one resident partition.
  Canonical edge files cover fixed windows of `edge_id`s that can straddle two
  partitions; the rows after a partition's last whole window carry to the next,
  one partition at a time.
- The adjacency pass sorts each node-range partition in order and feeds the
  union into one canonical shard carry. Covering relation groups encode from
  that same carry, one encoder at a time. Other relation entries append to
  ordered CRC-protected scratch spools through one capped block buffer. After
  the union finishes, one relation spool at a time reuses the same carry and
  encoder. The published greedy shard cuts are unchanged; unfinished shards
  no longer retain edge-bearing memory for every relation simultaneously.
- Scratch has no fsync and no SHA-256. Every block carries a CRC32C that the
  reader checks. The base scatter writes and reads each record once. Adaptive
  edge refinement adds one write/read of the affected records per radix level.
  Each usable non-covering relation adds one sequential write/read of its CSR
  entries (at most another 32 bytes per edge across both directions, plus block
  headers). Every successful written block is consumed once; the report names
  refinement and spool bytes separately. Scratch is deleted on completion,
  on error, and when a session is opened for recovery, and a rerun starts from
  the sources.
- Partition count and the partitions in flight come from the budget. A memory
  gate grants reservations in partition order, so the bytes in flight never
  exceed the gate and a waiting partition is not starved. The fixed footprint
  reserves 192 MiB for runtime/allocator overhead, 256 MiB for one bounded
  canonical CSR carry and Arrow IPC encoder, and a reusable minimum 64 MiB
  decoding/scatter/sort working set. The remaining budget expands that working
  set; the planner does not invent headroom after exhausting the budget.
  Relation metadata and published artifact inventories remain proportional to
  the number of output groups and files; edge-bearing CSR workspace is fixed.

- Node and edge counts are limited to 2^32 - 2 by the dense ids. A larger input
  is refused with a resource-limit error.

## Bounded property scratch

Property-bearing kinds on the scratch route keep no decoded source batches.
Every admitted batch, including a bare schema in a property-bearing kind, enters
an exact-schema group identified by the existing normalized schema digest.
Sorted Arrow IPC runs preserve full UUID order, field order, field metadata,
values and nulls. Two-way merges bound decoded fan-in; physical transport frames
have a small target size, with one admitted wide row allowed its own frame.
Scratch frames have length bounds and CRC32C, without hashing or fsync.

Catalog observation streams groups in digest order and rows in UUID order,
using the same per-row interning operations as the resident route. Overlay
encoding preserves the existing logical `max_batch_rows` windows. A disposable
window spool records payload while a compact owner/active-field inventory finds
all non-null fields for each owner across that whole window. A second scan
projects into owner/route spools. Those spools stream in owner/route order through
the shared 4 MiB greedy fragment splitter and existing Parquet encoder. Physical
frame boundaries therefore cannot affect published cuts or ordinals. Selected
pieces are copied using Arrow take before being retained; slices never serve as
memory-accounting boundaries. A binary accumulator limits retained batch headers
and concatenation copies while assembling one canonical fragment.

The property workspace includes admitted canonical Arrow buffers, two decoded merge frames,
IPC payloads, selection/concatenation transients and one canonical fragment and
encoder. It reuses the CSR workspace between stages, rather than reserving both
at once. Transport admission is based on actual retained Arrow array memory, including
nested children and dictionary buffers, rather than compressed source bytes or
logical fragment charge. A budget below the fixed workspace is refused before
decoding. Property scratch reads and writes are reported separately; catalog,
window and projected-row scans all count. Cancellation and recovery discard this
transport using the same restart policy as edge and CSR scratch.

Registered-source decoding precedes this transport. Parquet dictionary/page
expansion and the normalizer's row maps need their own bounded physical batching
and admission (#1918); this decision does not claim that normalized-row transport
alone bounds every source decoder. Available IPC footer/body expansion and schema
metadata reservations are checked before creating eager source readers. Property
traffic and fixed reservations remain distinct from measured process RSS.

**Alternatives.** Reusing the staged range partitioner would retain partition
writers, SHA receipts and fsyncs and would inherit its sorted-chunk and skew
constraints. Re-reading registered sources for each window would amplify input
I/O with graph size. Arrow row conversion is not an exact transport: hidden
children of null lists or temporal structs can affect the existing fragment
charge. IPC preserves those children. This is an internal reversible storage
choice; no public API or published format changes.

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
  42.3 GB. The resident route retains a property-bearing kind's decoded batches and two
  copies of them: six times their uncompressed footer bytes, measured at 5.5.
  The scratch route replaces those retained payloads with bounded property runs. A source
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
