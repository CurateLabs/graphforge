---
title: "ADR 0057: Initial builds resolve edge endpoints by a node-surrogate index instead of two endpoint sorts"
adr: "0057"
status: "Proposed"
date: "2026-10-05"
superseded_by: null
revisit_when: "The tracer's laned probe cost exceeds a third of the endpoint work it removes at S20, an append onto a large base needs the same saving, or new-node count outgrows the recorded index budget on a supported workload"
---

# ADR 0057: Initial builds resolve edge endpoints by a node-surrogate index instead of two endpoint sorts

**Status:** Proposed

**Related:**
- ADR 0038 (determinism at the publication boundary)
- ADR 0046 (construction keeps its own sorting and partitioning)
- ADR 0047 (over-budget partitions; one CPU budget)
- ADR 0056 (shaping stays serial within stages)
- #1387 (ingest floor), whose evidence comments are 5996434589, 5998525033 and 5998673917

## Context

Construction writes about 676 B/edge of intermediates at Graph500 S20 to publish
18 B/edge of edge Parquet. Endpoint data is the largest family (#1387, measured
on `dc50e13c`):

| endpoint step | record | B/edge written |
| --- | --- | ---: |
| intake: two staged endpoint records per edge, sorted per chunk | 33 B (node UUID, edge UUID, role) | 66 |
| shaping: routed by node UUID (`node_plan`) | 33 B | 66 |
| `Endpoints` stage: sorted per partition into `staged-endpoints.run` | 33 B | 66 |
| `ResolvedRouted`: merge join against `shaped-identities.run`, re-routed by edge UUID | 25 B (edge UUID, role, node surrogate) | 50 |
| `Resolved`: sorted per partition into `shaped-edge-endpoints.run` | 25 B | 50 |
| **total** | | **≈298** |

All of this exists to compute one join: node UUID to node surrogate. Two facts
make the join cheap without it:

- **A new node's surrogate is a dense rank.** `assign_surrogates` gives each new
  node `base_max_node` + its 1-based rank among new nodes in UUID order
  (`graph_construction/shape.rs` `assign_surrogates`). The sorted array of new
  node UUIDs, at 16 B per node, is therefore the whole map. Index *i* means
  surrogate `base_max_node + i + 1`.
- **Edge details already carry both endpoint UUIDs** (`detail[16..48]`,
  `graph_construction/intake.rs`). `encode_edges` already decodes them while it
  streams edges in edge-UUID order (`graph_construction_encoding.rs`
  `encode_edges`). Its only other endpoint input is `shaped-edge-endpoints.run`.

Nothing outside `encode_edges` reads the resolved endpoints. The endpoint
family otherwise appears only in stage plumbing, recovery name and width
rules, receipt checks and pinned test counts. None of these reach published
bytes. Adjacency encoding reads the encoded edge Parquet, not endpoint runs.

An adversarial review of the lookup design found constraints that this record
adopts as obligations:

- **Base appends.** The base UUID index authenticates one block per probe, with
  no cache. Endpoint-ordered windows sweep it once. Edge-ordered probes would
  scatter across it, so base I/O becomes windows × index size.
- **Memory.** The index would be the first resident structure that grows with
  the graph: 1 GiB at S26 (64M nodes) against the 4 GiB ladder envelope.
  Nothing admits it today.
- **CPU.** The `encode_edges` loop is serial. At S26, 2.1 billion serial binary
  searches into a 1 GiB array would cost far more than the floor allows.
- **Validation timing.** A dangling endpoint fails during shaping today, before
  the shape is recorded complete. Failing in encoding would record a shape that
  can never encode.
- **Benchmark bias.** Graph500 UUIDs are a constant prefix plus a sequential
  index, which flatters any distribution-sensitive search.

## Options

1. **Fuse the one-consumer rewrites, keeping the sort join.** Resolution
   consumes endpoint partitions directly, and encoding consumes resolved
   partitions directly. This removes 116 B/edge of writes. Estimated at most
   ~9% of S20 complete ingest. It changes the stage contract but keeps the
   sorts, both endpoint routings and the hub-keyed partitions.
2. **Index resolution for initial builds; sort join otherwise.** This record's
   choice, below. It removes the endpoint family on the path the ladder and
   first imports take, and keeps today's bounded behaviour where the index
   cannot apply.
3. **Resolve at append time.** Seal and rank nodes before any edge is staged,
   and write pre-resolved edges. This removes the most work, but it changes the
   public import-session contract: nodes must be registered before edges. That
   needs its own product decision, and it does not cover appends either.

## Decision (proposed)

### Which path a construction takes

A construction resolves endpoints by **node index** when both hold:

- **No base graph:** `parent_topology_generation == 0`.
- **The index fits its budget:** new node count × 16 B ≤ a new recorded budget,
  `GraphConstructionBudgets::max_node_index_bytes`. Its default is chosen from
  tracer evidence, and 0 disables the path.

Otherwise it uses today's **sort join**. The choice is a recorded construction
parameter, so a resume validates it like any budget (ADR 0046's migration
rule). Admission and lane counts never affect published bytes (ADR 0047).

### Intake

Intake stops writing endpoint runs for every construction. The sort-join
path derives its endpoint family from edge details at routing time, two records
per edge exactly as intake builds them today, so one intake format serves both
paths. This removes the per-chunk endpoint sort and 66 B/edge of staged bytes
everywhere.

### Index path

- **Build.** The index is the sorted array of new node UUIDs. It is captured
  while node-kind identity records are streamed after `Assigned`, and dropped
  before adjacency encoding.
- **Validate.** Before the shape is recorded complete, a laned membership pass
  over `shaped-edge-details.run` probes every endpoint UUID. A miss refuses
  with today's "endpoint UUID lacks node surrogate" error, still inside
  shaping.
- **Resolve.** `encode_edges` resolves source and target by batched, laned
  probes into the same index, using leases from the ADR 0047 construction CPU
  admission. Probes use plain binary search, optionally behind the recorded
  `node_plan` splitters as a first level. The cost must not depend on UUID
  distribution.
- **Stages removed.** `Endpoints`, `ResolvedRouted` and `Resolved` do not
  occur on this path. Their outputs, failpoints, recovery names and receipt
  requirements are removed or made path-conditional. `FORMAT_VERSION` is
  bumped; older in-flight sessions refuse at open, as any format bump does
  before v1.0.

### Invariants

- **Published bytes are byte-identical to the sort-join path:** same
  surrogates, same edge order, same Parquet. This is proven by digest equality
  on both paths for the same input, at S18 and S20, with sequential Graph500
  UUIDs and with random v4 and v7 UUIDs.
- Crash, corruption and cancellation suites run unweakened. Resume
  re-derives the index from the sealed shape, so encoding needs no new resume
  authority.

## Consequences

- Initial builds drop about 298 B/edge of endpoint writes, plus two global
  sorts, one merge join and three durable stages. **Estimated, not measured:**
  about 20–25% of S20 complete ingest, net of probe cost. The adoption gate is
  predeclared in the slice plan below.
- Hub-keyed endpoint partitions disappear on the index path, which is the case
  ADR 0047's external sorting was built for. External partitions remain for
  identities and on the sort-join path.
- Construction gains its first graph-sized resident structure, bounded by a
  recorded budget, with a fallback above it.
- Appends keep today's cost. Their saving needs a base-index design that
  probes in edge order without per-probe block reads. That is a separate
  decision.
- This does not close #1387's 5.5× gap. It is the largest single reduction the
  evidence supports.

## Slice plan

1. **Tracer (measurement, not merged).** In a branch, build the index after
   `Assigned` and resolve in `encode_edges` by laned batched probes. Also keep
   the sort-join output, and assert surrogate equality per edge. Measure probe
   cost, serial and laned, at S18 and S20, with sequential and random UUIDs.
   *Go only if* laned probe cost is at most a third of the endpoint work it
   replaces (an estimated ≈24 s at S20, from region attribution and bytes) and equality holds
   everywhere.
   **Result (2026-10-05): go.** The method and receipts are on #1387, from
   tracer source at `628faab8` plus a 118-line patch, run against a frame-pointer
   release build.
   - **Equality held.** Index resolution matched the sort join for every one of
     33.5M lookups, at 1 and 8 lanes. Inputs were Graph500 S20 with its
     sequential UUIDs, and the same graph remapped to random UUIDv7 keys
     (GraphForge requires v7; a v4 remap is refused at intake). Every run
     reopened 16,777,216 edges.
   - **Cost:**

     | | 1 lane | 8 lanes |
     | --- | ---: | ---: |
     | probe wall, sequential UUIDs | 17.30 s | 2.33 s |
     | probe wall, random v7 UUIDs | 15.95 s | 2.42 s |
     | index build | 0.45–0.47 s | 0.45–0.47 s |

   - **Against the go criterion.** Two laned passes (validation and
     resolution) plus two builds total about 5.7 s. The ceiling is ≈8.9 s, a
     third of the estimated endpoint work.
   - **Caveats.** All four runs were on a contended host, load 1.0–3.8,
     because no quiet window opened in two hours. Serial binary search costs
     about 500 ns per lookup, so the lanes, or a cache-friendlier layout
     (Eytzinger, or the splitters as a first level), are required, not
     optional.
2. **Slice A: index path.** Recorded `endpoint_resolution` choice and
   `max_node_index_bytes`; the laned validation pass in shaping; resolution in
   `encode_edges`; path-conditional stages; format bump. Proven by
   cross-path digest equality, crash and resume tests at each new failpoint,
   and a cancellation test inside the probe lanes.
3. **Slice B: intake without endpoint runs.** The sort-join path derives
   endpoints from edge details. Proven by appends onto a base graph with
   unchanged published digests, and by the hub/star external-partition tests
   (ADR 0047) on the sort-join path.
4. **Adoption gate (predeclared).** S20 and S22 complete ingest improves by at
   least 15% in four ABBA pairs with every pair the same sign. Published
   digests are identical. VmHWM stays within the ladder envelope, and its
   growth between adjacent rungs stays under the ladder's 10% plateau rule.
   A miss leaves this record Proposed with the measurement attached, and the
   code does not merge.

## Open questions

- **Probe layout.** Entity UUIDs must be v7 (`GF_BULK_VALIDATION(invalid_uuid)`
  refuses others), so keys cluster by creation time. Binary search cost was
  the same for sequential and random v7 keys in the tracer. Whether a
  splitter-indexed first level or an Eytzinger layout earns its code is a
  Slice A measurement.
- **Default for `max_node_index_bytes`.** It must fit the 4 GiB ladder envelope
  at S26 (1 GiB) with the construction's existing peak, about 0.5 GB at S20.
- **A base-append index.** A design that sweeps the base index in edge order
  (for example a per-window block cache keyed by UUID range) would extend the
  saving to appends.
