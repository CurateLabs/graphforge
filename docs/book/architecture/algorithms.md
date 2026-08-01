# Algorithm Verbs

**Status:** Rust Core — shipped analyst-verb catalog
**Last Updated:** 2026-07-24

The frozen, knowledge-neutral identity used for recorded algorithm execution is
defined by the
[neutral invocation descriptor v1](m18-invocation-descriptor-v1.md). Direct
and descriptor dispatch share the same Rust executor; graph and algorithm
crates never open knowledge tables.

---

## Overview

GraphForge exposes graph algorithms through five **analyst-intent verbs** that bypass the
Cypher compiler entirely. All return Arrow Tables; none require writing Cypher.

```
forge.rank(label, by=...)             →  node scores          (centrality, structural)
forge.cluster(label, by=...)          →  community membership  (partitioning, components)
forge.paths(source=None, target=None, by=...) → path results   (shortest paths, flows, reachability)
forge.analyze(label, by=...)          →  graph-level metrics   (planarity, coloring, matching, DAG)
forge.similar(label, by=...)          →  pairwise similarity   (topology, vector)
```

The algorithm surface is explicitly designed for parity with:

- **OCG** — pure-Rust openCypher engine (31+ algorithms via `AlgorithmEngine` trait)
- **Neo4j GDS** — production graph data science library (60 algorithms across 7 categories)
- **igraph** — the de-facto standard Python graph analysis library

Every algorithm that exists in any of these three libraries has a home in one of the five
verbs. Algorithms with no upstream analog (genealogy-specific traversals, provenance-aware
semijoins) are added as needed without breaking the verb design.

---

## Rust Algorithm Dispatch

Production algorithms register one `RustAlgorithm` handler per typed catalog value in
`graphforge-exec`. Dispatch, limits, cancellation, and Arrow result shaping are shared across the
five verbs. igraph and NetworkX are optional development parity oracles only: they are not
runtime backends, fallbacks, packaging dependencies, or recovery paths.

### Invocation and knowledge-layer boundary

> **Status: implemented across the v0.5.0 algorithm, knowledge, and epistemic layers.**

Analyst algorithms are graph-layer computations. An invocation consumes only graph-native
topology, properties, selectors, typed options, and—when an algorithm needs a partition or
other derived input—an explicit UUID-keyed mapping. The algorithm path never reads provenance,
evidence, assertions, belief status, valid time, or any other knowledge-layer table. This is
the downward-only dependency required by
[ADR 0005](../../adr/0005-layered-architecture.md) and preserves the graph/knowledge boundary
gate.

Every completed invocation must be reproducible from a neutral descriptor containing:

- the algorithm catalog value and contract version;
- a fingerprint of the resolved graph projection;
- normalized label, relationship, source, target, weight, and property selectors;
- normalized algorithm options, including `k`, seed, walk length, or partition source when
  applicable; and
- applied resource limits.

The descriptor is an execution value, not a new result column. The analyst-verb layer owns its deterministic
construction and the canonical Arrow result. The knowledge layer persists that descriptor, its projection
fingerprint, and a durable `algorithm_run_uuid` in provenance/knowledge storage. The epistemic layer may link
assertions, evidence, and preserved reasoning to that run UUID. Neither layer changes the
algorithm result schema or makes confidence affect an algorithm unless a future, explicitly named
algorithm contract says so.

Descriptor, projection, result, and derived-record bytes use the frozen
[canonical fingerprint v1 contract](canonical-fingerprints-v1.md). Algorithm
contracts establish result order before encoding; fingerprinting never reorders
rows.

For a point-in-time or competing-hypothesis analysis, the epistemic layer resolves the requested belief state
_before_ dispatch into an immutable UUID projection or parameter mapping. The analyst-verb path runs against
that resolved input exactly as it would against an equivalent graph-native input. The epistemic layer then
records claims about the result _after_ dispatch. `as_of`, assertion status, hypothesis group,
confidence policy, and provenance selectors therefore do not belong in algorithm option structs.

The public resolved path supports all five analyst-verb families. Transaction time is mandatory and
resolved first; optional valid time is applied afterward. The neutral knowledge-layer run is published
before its epistemic interpretation attachment, and attachment failure is separately reportable and
retryable without algorithm redispatch.

This separation deliberately rejects three tempting alternatives:

- graph algorithms querying knowledge tables, which would invert the layer dependency;
- conditional provenance or confidence columns, which would make one catalog value expose
  multiple Arrow schemas; and
- materializing a belief state by destructively rewriting graph topology, which would violate
  the preservation model in [ADR 0006](../../adr/0006-epistemic-model.md).

---

## `forge.rank()` — Node Scoring

Returns an Arrow Table: all node properties + `score: Float64`.

`write_property` is opt-; default is read-only.

### Implemented PageRank contract

`by="pagerank"` is a Rust-owned unweighted PageRank implementation. It selects nodes by
`label`; `via=None` includes every relationship type, while a named `via` includes only that
type. Directed mode is the default and follows outgoing adjacency. Undirected mode exports
both endpoint directions. Parallel edges contribute independently and self-loops remain in
the transition distribution.

The implementation uses damping `0.85`, uniform initial scores, uniform teleportation, and
uniform redistribution of dangling-node mass. Iteration stops when the L1 score delta is at
most `selected_node_count * 1e-10`. Rows and floating-point accumulation follow stable
topology order, so identical graph state and options produce identical results. Disconnected
graphs retain every selected node and normalized mass; an empty selection returns a typed
zero-row table.

The stable public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then the selected nodes' materialized property columns with Arrow NULLs for
missing values. No execution surrogate is exposed. `write_property` atomically persists the
score only after successful execution; omitting it never mutates the graph.

PageRank uses the shared hard caps of 10,000,000 selected nodes, 100,000,000 selected
adjacency entries, 10,000,000 output rows, and 10,000 iteration checkpoints, plus cooperative
cancellation. Invalid labels or relationship selectors, unavailable catalog values, limit
violations, cancellation, adjacency/storage failures, and shaping/write-back failures remain
structured Rust errors. Python and Node only translate arguments and Arrow IPC around this
same handler; neither binding contains an algorithm or fallback.

### Implemented betweenness contract

`by="betweenness"` is a Rust-owned, exact, unweighted Brandes node-betweenness
implementation. Node and relationship selection matches the shared rank path:
`label` selects nodes, `via=None` includes every relationship type, a named
`via` filters to that type, directed mode follows outgoing adjacency, and
undirected mode exports both endpoint directions. Parallel adjacency entries
represent distinct shortest-path choices. Self-loops cannot shorten a route and
therefore add no dependency contribution.

For `n > 2` selected nodes, every accumulated dependency score is multiplied by
`1 / ((n - 1) * (n - 2))`; selections of at most two nodes return zero scores.
Consequently the middle node of a directed three-node chain scores `0.5`, while
the same chain in undirected mode scores `1.0`. Unreachable pairs contribute
zero, disconnected selections retain every selected node, and an empty
selection returns a typed zero-row table. Stable topology and adjacency order
make both shortest-path accumulation and output deterministic.

The public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node-property columns with Arrow NULLs for
missing values. Numeric execution surrogates never escape. `write_property` is
opt-in and atomically persists all scores only after successful execution.

Betweenness uses the shared hard caps of 10,000,000 selected nodes, 100,000,000
selected adjacency entries, 10,000,000 output rows, and 10,000 cooperative
iteration checkpoints, including checkpoints within edge-heavy traversal and
dependency loops. Invalid selectors or unavailable catalog values, resource
limits, cancellation, non-finite path multiplicity or dependencies, and
adjacency/storage/shaping/write-back failures remain structured Rust errors.
Python and Node only translate arguments and Arrow IPC around this handler; they
contain no algorithm backend, fallback, packaging dependency, or recovery path.

### Implemented closeness contract

`by="closeness"` is a Rust-owned exact unweighted outward-closeness
implementation. It performs one BFS per selected source. `label` and `via`
select the shared adjacency; directed mode follows outgoing edges, consistently
with GraphForge's other rank algorithms, while undirected mode exports both
endpoint directions. Parallel entries and self-loops cannot reduce an
unweighted shortest-path distance and therefore do not change scores.

For `N` selected nodes, let `r` be the number of other selected nodes reachable
from a source and `S` the sum of their finite shortest-path distances. GraphForge
uses the disconnected-graph correction of Wasserman and Faust:
`r² / ((N - 1) * S)` when `N > 1`, `r > 0`, and `S > 0`; otherwise the score is
`0.0`. This reduces to `(N - 1) / S` for a connected selection. A directed
three-node chain scores `[2/3, 1/2, 0]`; the undirected chain scores
`[2/3, 1, 2/3]`. Unreachable nodes remain rows with finite scores, and an empty
selection returns a typed zero-row table.

Stable topology and adjacency order make BFS and output deterministic. The
public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Numeric execution surrogates never escape. `write_property` atomically
persists every score only after successful execution and is otherwise read-only.

Closeness uses the shared hard caps of 10,000,000 selected nodes, 100,000,000
selected adjacency entries, 10,000,000 output rows, and 10,000 cooperative
iteration checkpoints, including edge-heavy BFS checkpoints. Invalid selectors
or unavailable catalog values, resource limits, cancellation, non-finite scores,
and adjacency/storage/shaping/write-back failures remain structured Rust errors.
Python and Node only translate arguments and Arrow IPC around this handler; they
contain no algorithm backend, fallback, packaging dependency, or recovery path.

### Implemented harmonic closeness contract

`by="harmonic_closeness"` is a Rust-owned exact normalized unweighted outward
harmonic-closeness implementation. It performs one BFS per selected source.
`label` and `via` select the shared adjacency; directed mode follows outgoing
edges, consistently with GraphForge's other rank algorithms, while undirected
mode exports both endpoint directions. Parallel entries and self-loops cannot
reduce an unweighted shortest-path distance and therefore do not change scores.

For `N` selected nodes, GraphForge sums `1 / distance(u, v)` over every reachable
selected node `v != u`, then divides by `N - 1`. Unreachable nodes contribute
zero, and selections with at most one node score `0.0`. A directed three-node
chain scores `[3/4, 1/2, 0]`; the undirected chain scores `[3/4, 1, 3/4]`.
Disconnected nodes remain rows with finite scores, and an empty selection
returns a typed zero-row table.

Stable topology and adjacency order make BFS and output deterministic. The
public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Numeric execution surrogates never escape. `write_property` atomically
persists every score only after successful execution and is otherwise read-only.

Harmonic closeness uses the shared hard caps of 10,000,000 selected nodes,
100,000,000 selected adjacency entries, 10,000,000 output rows, and 10,000
cooperative iteration checkpoints, including edge-heavy BFS checkpoints.
Invalid selectors or unavailable catalog values, resource limits, cancellation,
non-finite scores, and adjacency/storage/shaping/write-back failures remain
structured Rust errors. Python and Node only translate arguments and Arrow IPC
around this handler; they contain no algorithm backend, fallback, packaging
dependency, or recovery path.

### Implemented eigenvector contract

`by="eigenvector"` is a Rust-owned deterministic unweighted incoming-neighbor
eigenvector-centrality implementation. It applies shifted power iteration to
`Aᵀ + I`: every node retains its current score and receives the current score of
each selected predecessor. The identity shift preserves eigenvectors while
separating the dominant eigenvalue and avoiding periodic oscillation.

For `N` selected nodes, every node starts at `1 / N`. Each iteration accumulates
in stable topology/adjacency order and then L2-normalizes the complete selected
score vector. Execution stops once every component changes by at most `1e-7`,
after at least two iterations, or returns the last normalized iterate after 20
iterations. Directed mode accumulates from incoming selected edges. Undirected
mode exports both endpoint directions. `via` filters relationship type.
Parallel entries contribute independently; a stored self-loop contributes once
in addition to the implicit identity term.

Disconnected components share one global L2 norm and remain deterministic from
the uniform start. An edgeless `N`-node selection scores every node
`1 / sqrt(N)`, a singleton scores `1.0`, and an empty selection returns a typed
zero-row table. The public schema is non-null
`node_uuid: FixedSizeBinary(16)`, non-null `score: Float64`, then materialized
node properties with Arrow NULLs for missing values. Numeric execution
surrogates never escape. `write_property` atomically persists every score only
after successful execution and is otherwise read-only.

Eigenvector uses the shared hard caps of 10,000,000 selected nodes, 100,000,000
selected adjacency entries, 10,000,000 output rows, and 10,000 cooperative
iteration checkpoints, including edge-heavy checkpoints inside every power
iteration. Invalid selectors or unavailable catalog values, resource limits,
cancellation, non-finite scores or norms, and adjacency/storage/shaping/
write-back failures remain structured Rust errors. Python and Node only
translate arguments and Arrow IPC around this handler; they contain no algorithm
backend, fallback, packaging dependency, or recovery path.

### Implemented ArticleRank contract

`by="article_rank"` is a Rust-owned deterministic, unweighted ArticleRank
implementation. Every selected node starts with score and propagation delta
`alpha = 1 - 0.85 = 0.15`. The selected graph's average outgoing degree is
`sum(outdegree) / N`. On each propagation round, a source with positive degree
sends its delta over every selected adjacency entry as
`delta / (outdegree + average_outdegree)`. Each target multiplies its incoming
message sum by `0.85`, adds that new delta to its accumulated score, and uses it
for the next round. Dangling nodes send nothing and dangling mass is not
redistributed.

Execution stops when every new delta is at most `1e-7` or after 20 propagation
rounds, returning the last accumulated scores at the cap. Directed mode sends
over selected outgoing edges and therefore scores incoming influence;
undirected mode exports both endpoint directions. `via` filters relationships
before degree, average-degree, and score computation. Parallel adjacency entries
contribute independently, and stored self-loops follow the shared adjacency
adapter's directed/undirected export contract.

Disconnected components share only the graph-wide average-degree denominator.
An edgeless non-empty selection scores every node `0.15`; a singleton does the
same, and an empty selection returns the typed zero-row table. The public schema
is non-null `node_uuid: FixedSizeBinary(16)`, non-null `score: Float64`, then
materialized node properties with Arrow NULLs for missing values. Numeric
execution surrogates never escape. `write_property` atomically persists every
score only after successful execution and is otherwise read-only.

ArticleRank uses the shared hard caps of 10,000,000 selected nodes, 100,000,000
selected adjacency entries, 10,000,000 output rows, and 10,000 cooperative
iteration checkpoints, including edge-heavy checkpoints during propagation.
Invalid selectors or unavailable catalog values, resource limits, cancellation,
non-finite scores, and adjacency/storage/shaping/write-back failures remain
structured Rust errors. Python and Node only translate arguments and Arrow IPC
around this handler; they contain no algorithm backend, fallback, packaging
dependency, or recovery path.

### Implemented HITS hub contract

`by="hits_hub"` is a Rust-owned deterministic, unweighted HITS hub-score
implementation. Authority and hub vectors start at `1.0`. Each of 20 fixed
iterations first replaces every authority with the sum of predecessor hub
scores and globally L2-normalizes the complete authority vector, then replaces
every hub with the sum of successor authority scores and globally L2-normalizes
the complete hub vector. The final hub component is returned; the rank
surface exposes no tolerance or iteration option.

A zero phase norm leaves the complete vector at zero, so isolated and entirely
edgeless selections never produce non-finite scores. Directed mode uses selected
outgoing adjacency for the standard recurrence. Undirected mode exports both
endpoint directions. `via` filters relationships before both phases. Parallel
adjacency entries contribute independently, and stored self-loops follow the
shared adjacency adapter's directed/undirected export contract. Disconnected
components share global norms; isolated nodes score `0.0`, an edgeless selection
scores every node `0.0`, a self-loop singleton scores `1.0`, and an empty
selection returns the typed zero-row table.

The public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Numeric execution surrogates never escape. `write_property` atomically
persists every score only after successful execution and is otherwise
read-only.

HITS hub uses the shared hard caps of 10,000,000 selected nodes, 100,000,000
selected adjacency entries, 10,000,000 output rows, and 10,000 cooperative
iteration checkpoints, including edge-heavy checkpoints in both phases.
Invalid selectors or unavailable catalog values, resource limits, cancellation,
non-finite scores or norms, and adjacency/storage/shaping/write-back failures
remain structured Rust errors. Python and Node only translate arguments and
Arrow IPC around this handler; they contain no algorithm backend, fallback,
packaging dependency, or recovery path.

### Implemented HITS authority contract

`by="hits_authority"` is the authority projection of the same Rust-owned,
deterministic, unweighted HITS recurrence used by `hits_hub`. Authority and hub
vectors start at `1.0`. Each of 20 fixed iterations first replaces every
authority with the sum of predecessor hub scores and globally L2-normalizes the
complete authority vector, then replaces every hub with the sum of successor
authority scores and globally L2-normalizes the complete hub vector. The final
authority component is returned. A zero phase norm remains all zero.

With `directed=True`, authority flows along selected outgoing adjacency from
each predecessor to its target. With `directed=False`, the shared adapter
exports both endpoint directions. `via` filters before either phase. Parallel
edges contribute independently, stored self-loops follow the shared adapter
contract, disconnected components share global norms, isolated nodes score
`0.0`, an edgeless selection is all zero, and a self-loop singleton scores
`1.0`. Empty selections return the same typed zero-row table.

The public schema, UUID-only identity and order, materialized properties,
atomic opt-in `write_property`, shared node/edge/output/iteration limits,
edge-heavy cancellation checkpoints, finite-score validation, and structured
Rust errors are identical to the HITS hub contract. Python and Node only
translate arguments and Arrow IPC; neither contains an algorithm backend,
fallback, packaging dependency, or recovery path.

### Implemented CELF contract

`by="celf"` is a Rust-owned Cost-Effective Lazy Forward influence-maximization
implementation over the Independent Cascade model. Because the public `rank()`
surface has no seed-set-size option, GraphForge selects the complete node set. A
node's `score` is the marginal expected spread when CELF selects that node; the
scores therefore sum to the selected node count. Candidates begin with singleton
spread, stale gains are recomputed lazily against the current seed set, and equal
gains are resolved by the smallest public node UUID. Rows remain in stable topology
order rather than CELF selection order.

Expected spread uses 100 fixed live-edge simulations with activation probability
`0.1` and simulation identifiers beginning at zero. A SplitMix64-derived decision
is keyed by the simulation identifier, public source-node UUID, and public edge
UUID. Reusing those same live-edge samples for every candidate makes repeated
execution on unchanged graph state bit-for-bit deterministic without exposing an
execution surrogate.

Directed mode propagates over selected outgoing adjacency. Undirected mode exports
both endpoint directions, and `via` filters relationships before simulation.
Parallel edges are independent activation opportunities. A self-loop cannot
activate a new node. Disconnected components contribute additively; every node in
an edgeless selection scores `1.0`, as does a singleton, while an empty selection
returns the typed zero-row table.

The public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. `write_property` atomically persists every marginal score only after
successful execution and is otherwise read-only. CELF uses the shared selected
node, adjacency-entry, output-row, and iteration limits, with cooperative
cancellation before spread simulations and during edge-heavy propagation.
Unavailable selectors, limit or cancellation events, invalid or non-finite
marginal spread, and adjacency/storage/shaping/write-back failures remain
structured Rust errors. Python and Node only translate arguments and Arrow IPC;
neither contains an algorithm backend, fallback, packaging dependency, or recovery
path.

### Implemented clustering coefficient contract

`by="clustering_coefficient"` is a Rust-owned, deterministic, unweighted local
clustering coefficient. `local_clustering_coefficient` is an accepted input alias
that parses to the same canonical enum owner; schema metadata always reports
`clustering_coefficient`.

Directed mode uses Fagiolo's coefficient. After removing self-loops and collapsing
parallel arcs in the same direction, let `A` be the Boolean adjacency matrix,
`k_tot` the sum of a node's in- and out-degrees, and `k_recip` its reciprocal
degree. The score is
`(A + A^T)^3[i,i] / (2 * (k_tot * (k_tot - 1) - 2 * k_recip))`.
Undirected mode uses the same computation after the shared adapter exports both
endpoint directions, reducing to classic local transitivity `2T / (k * (k - 1))`.
A zero denominator yields `0.0`, including isolated and degree-one nodes.

`via` filters relationships before the simple adjacency is built. Reciprocal arcs
remain distinct directions, but duplicate same-direction arcs do not increase the
score and self-loops never participate in degree or triangles. Disconnected
selections retain every node. Stable topology order drives adjacency construction,
neighbor-pair accumulation, and output; an empty selection returns the typed
zero-row table.

The public schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. `write_property` atomically persists all scores only after successful
execution and is otherwise read-only. Shared selected-node, adjacency-entry,
output-row, iteration, and cancellation limits apply, including edge-heavy
neighbor-pair checkpoints. Invalid selectors, unavailable catalog values, limits,
cancellation, non-finite or out-of-range counts, and adjacency/storage/shaping/
write-back failures remain structured Rust errors. Python and Node only translate
arguments and Arrow IPC; neither contains an algorithm backend, fallback,
packaging dependency, or recovery path.

### Implemented triangle count contract

`by="triangles"` is a Rust-owned, deterministic, unweighted count of the
distinct triangles containing each selected node. A triangle is an unordered
three-node clique: every pair has at least one selected relationship, and each
clique contributes `1.0` to each of its three nodes. The public score remains
`Float64` even though every finite count is integral.

Triangle count intentionally ignores relationship direction. The rank-wide
`directed` option is accepted in both modes, but the handler symmetrizes the
selected topology and therefore produces identical scores. This is not a
directed-motif count. `via` filters relationship types before symmetrization;
parallel and reciprocal arcs then collapse to one undirected adjacency, and
self-loops never participate. No relationship weight or random seed applies.

Disconnected selections retain every node, isolated and degree-one nodes score
`0.0`, and an empty selection returns the typed zero-row table. Stable topology
and neighbor order make repeated results deterministic. The public schema is
non-null `node_uuid: FixedSizeBinary(16)`, non-null `score: Float64`, then
materialized node properties with Arrow NULLs for missing values. Internal
execution surrogates never escape.

`write_property` is opt-in and atomically persists all returned scores only after
successful rank execution. Shared selected-node, adjacency-entry, output-row,
iteration, and cancellation limits apply, including checkpoints during edge
normalization and neighbor-pair enumeration. Invalid selectors, unavailable
catalog values, limits, cancellation, out-of-range counts, and adjacency/storage/
shaping/write-back failures remain structured Rust errors. Python and Node only
translate arguments and Arrow IPC; neither contains an algorithm backend,
fallback, packaging dependency, or recovery path.

### Implemented k-core contract

`by="k_core"` is the Rust-owned, deterministic core number for every selected
node. A node's score is the largest integer `k` for which it belongs to a maximal
induced subgraph whose minimum degree is at least `k`. Scores use the public
`Float64` rank field while remaining exact integral values.

K-core intentionally uses corresponding undirected simple adjacency. The
rank-wide `directed` option is accepted, but both modes symmetrize the selected
topology and produce identical scores. `via` filters relationship types before
symmetrization. Parallel and reciprocal relationships then collapse to one
undirected adjacency, self-loops are ignored, and relationship weights do not
apply.

Stable node and neighbor order drive deterministic Batagelj-Zaversnik-style
peeling. Disconnected components peel independently, isolated nodes score `0.0`,
an edgeless selection retains every selected node at `0.0`, and an empty
selection returns the typed zero-row table. The public schema is non-null
`node_uuid: FixedSizeBinary(16)`, non-null `score: Float64`, then materialized
node properties with Arrow NULLs for missing values; execution surrogates remain
internal.

`write_property` is opt-in and atomically persists every returned core number
only after successful rank execution. Shared selected-node, adjacency-entry,
output-row, iteration, and cancellation limits apply, with batched checkpoints
during edge normalization, heap peeling, and neighbor updates. Invalid selectors,
unavailable catalog values, limits, cancellation, numeric-range failures, and
adjacency/storage/shaping/write-back failures remain structured Rust errors.
Python and Node only translate arguments and Arrow IPC; neither contains an
algorithm backend, fallback, packaging dependency, or recovery path.

### Implemented preferential-attachment aggregate contract

`by="preferential_attachment"` is the Rust-owned, deterministic missing-link
aggregate for every selected node. Its pairwise primitive is
`PA(u, v) = degree(u) * degree(v)`. Rank returns
`score(u) = sum(PA(u, v))` over all `v != u` for which the selected topology has
no link from `u` to `v`. Existing links and self-pairs are excluded. This
aggregation is why the public result has one row per node rather than one row per
candidate pair.

In directed mode, distinct outgoing neighbors define degree and missing outgoing
arcs define candidates. In undirected mode, the adapter supplies the
corresponding symmetric simple graph. `via` filters before normalization;
same-direction parallel relationships collapse, reciprocal relationships remain
distinct only in directed mode, self-loops are ignored, and relationship weights
do not apply. Missing-link candidates cross component boundaries, so disconnected
components still contribute to one another. Isolated nodes, complete graphs,
edgeless graphs, singleton selections, and empty selections retain deterministic
zero/boundary behavior.

The implementation evaluates the aggregate algebraically in `O(V + E)` after
adjacency construction. Degree totals, linked-neighbor sums, and products use
checked integer arithmetic. Scores convert to `Float64` only when exactly
representable at or below `2^53`; larger results return a structured execution
error instead of silently rounding. Batched checkpoints enforce shared iteration
and cancellation limits.

Stable topology order produces non-null `node_uuid: FixedSizeBinary(16)`,
non-null `score: Float64`, then materialized node properties with Arrow NULLs for
missing values. Internal execution surrogates never escape. `write_property` is
opt-in and atomically persists every returned score only after successful
execution. Rust is the sole production owner; Python and Node only translate
arguments and Arrow IPC and provide no backend, fallback, packaging dependency,
or recovery path.

### Implemented Adamic-Adar aggregate contract

`by="adamic_adar"` is the Rust-owned, deterministic missing-link aggregate for
every selected node. Its pairwise primitive is
`AA(u, v) = sum(1 / ln(degree(w)))` over the common neighbors `w` of `u` and
`v`. Rank returns `score(u) = sum(AA(u, v))` over all `v != u` for which the
selected topology has no link from `u` to `v`. Existing links and self-pairs are
excluded. The aggregation is why the public result has one row per node rather
than one row per candidate pair.

In directed mode, distinct outgoing neighbors define the two neighborhoods and
missing outgoing arcs define candidates. A shared successor's discount degree
is its distinct selected in-degree; every contributing successor therefore has
degree at least two. In undirected mode, the adapter supplies a symmetric simple
graph and ordinary distinct-neighbor degree. `via` filters before normalization;
same-direction parallel relationships collapse, reciprocal relationships remain
distinct only in directed mode, self-loops are ignored, and relationship weights
do not apply. Missing-link candidates cross component boundaries but contribute
zero without a shared neighbor. Complete, disconnected, edgeless, singleton,
and empty selections retain deterministic finite boundary behavior.

Stable topology and neighbor order drive pair intersections. Contributions and
node aggregates use compensated `Float64` summation, and a non-finite
contribution or result produces a structured execution error. Batched
checkpoints enforce the shared selected-node, adjacency-entry, output-row,
iteration, and cancellation limits.

The public schema remains non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Internal execution surrogates never escape. `write_property` is opt-in
and atomically persists every score only after successful execution. Rust is the
sole production owner; Python and Node only translate arguments and Arrow IPC
and provide no backend, fallback, packaging dependency, or recovery path.

### Implemented common-neighbors aggregate contract

`by="common_neighbors"` is the Rust-owned, deterministic missing-link
aggregate for every selected node. Its pairwise primitive is
`CN(u, v) = |N(u) intersect N(v)|`. Rank returns
`score(u) = sum(CN(u, v))` over all `v != u` for which the selected topology
has no link from `u` to `v`. Existing links and self-pairs are excluded. This
aggregation preserves the one-row-per-node rank contract rather than returning
one row per candidate pair.

In directed mode, common neighbors are shared distinct outgoing successors and
missing outgoing arcs define candidates. In undirected mode, the adapter
supplies a symmetric simple graph. `via` filters before normalization;
same-direction parallel relationships collapse, reciprocal relationships remain
distinct only in directed mode, self-loops are ignored, and relationship
weights do not apply. Candidate nodes may cross component boundaries but
contribute zero without a shared neighbor. Complete, disconnected, edgeless,
singleton, and empty selections retain deterministic zero/boundary behavior.

Stable topology and neighbor order drive exact set intersections. Pair counts
and node aggregates use checked integer arithmetic and convert to `Float64`
only when exactly representable at or below `2^53`; larger results return a
structured execution error. Shared selected-node, adjacency-entry, output-row,
iteration, and cancellation limits apply with batched checkpoints.

The public schema remains non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Internal execution surrogates never escape. `write_property` is opt-in
and atomically persists every score only after successful execution. Rust is the
sole production owner; Python and Node only translate arguments and Arrow IPC
and provide no backend, fallback, packaging dependency, or recovery path.

### Implemented resource-allocation aggregate contract

`by="resource_allocation"` is the Rust-owned, deterministic missing-link
aggregate for every selected node. Its pairwise primitive is
`RA(u, v) = sum(1 / degree(w))` over the common neighbors `w` of `u` and `v`.
Rank returns `score(u) = sum(RA(u, v))` over all `v != u` for which the selected
topology has no link from `u` to `v`. Existing links and self-pairs are
excluded, preserving the one-row-per-node rank contract rather than returning
one row per candidate pair.

In directed mode, distinct outgoing successors define the two neighborhoods
and missing outgoing arcs define candidates. A shared successor's discount is
its distinct selected in-degree. In undirected mode, the adapter supplies a
symmetric simple graph and ordinary distinct-neighbor degree. Every contributing
common neighbor therefore has degree at least two. `via` filters before
normalization; same-direction parallel relationships collapse, reciprocal
relationships remain distinct only in directed mode, self-loops are ignored,
and relationship weights do not apply. Candidates may cross component
boundaries but contribute zero without a common neighbor. Complete,
disconnected, edgeless, singleton, and empty selections retain deterministic
finite boundary behavior.

Stable topology and neighbor order drive pair intersections. Reciprocal-degree
contributions and node aggregates use compensated `Float64` summation; a
non-finite discount or score produces a structured execution error. Batched
checkpoints enforce shared selected-node, adjacency-entry, output-row,
iteration, and cancellation limits.

The public schema remains non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Internal execution surrogates never escape. `write_property` is opt-in
and atomically persists every score only after successful execution. Rust is the
sole production owner; Python and Node only translate arguments and Arrow IPC
and provide no backend, fallback, packaging dependency, or recovery path.

### Implemented total-neighbors aggregate contract

`by="total_neighbors"` is the Rust-owned, deterministic missing-link aggregate
for every selected node. Its pairwise primitive is
`TN(u, v) = |N(u) union N(v)|`, the denominator of Jaccard similarity. Rank
returns `score(u) = sum(TN(u, v))` over all `v != u` for which the selected
topology has no link from `u` to `v`. Existing links and self-pairs are
excluded, preserving the one-row-per-node rank contract rather than returning
one row per candidate pair.

In directed mode, distinct outgoing successors define both neighborhoods and
missing outgoing arcs define candidates. In undirected mode, the adapter
supplies a symmetric simple graph. `via` filters before normalization;
same-direction parallel relationships collapse, reciprocal relationships remain
distinct only in directed mode, self-loops are ignored, and relationship
weights do not apply. Candidates may cross component boundaries and can
contribute positively because union size does not require a shared neighbor.
For the same reason, an isolated node can receive a positive aggregate when
other selected nodes have neighbors. Complete and edgeless graphs, singleton
selections, and empty selections retain deterministic zero/boundary behavior.

Stable topology and neighbor order drive exact union counts. Pair counts and
node aggregates use checked integer arithmetic and convert to `Float64` only
when exactly representable at or below `2^53`; larger results return a
structured execution error. Shared selected-node, adjacency-entry, output-row,
iteration, and cancellation limits apply with batched checkpoints.

The public schema remains non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties with Arrow NULLs for missing
values. Internal execution surrogates never escape. `write_property` is opt-in
and atomically persists every score only after successful execution. Rust is
the sole production owner; Python and Node only translate arguments and Arrow
IPC and provide no backend, fallback, packaging dependency, or recovery path.

### Centrality (`by=`)

| Value | Algorithm | Source reference |
| -------------------- | -------------------------------------------------- | ------------------ |
| `pagerank` | PageRank | OCG / GDS / igraph |
| `betweenness` | Betweenness centrality (Brandes) | OCG / GDS / igraph |
| `closeness` | Closeness centrality | OCG / GDS / igraph |
| `harmonic_closeness` | Harmonic closeness (handles disconnected graphs) | GDS |
| `degree` | Degree centrality (normalized) | OCG / GDS / igraph |
| `eigenvector` | Eigenvector centrality | GDS |
| `article_rank` | ArticleRank (degree-penalized PageRank) | GDS |
| `hits_hub` | HITS hub score | GDS |
| `hits_authority` | HITS authority score | GDS |
| `celf` | Cost-Effective Lazy Forward influence maximization | GDS |

### Structural (`by=`)

| Value | Algorithm | Source reference |
| ------------------------ | ---------------------------- | ---------------- |
| `clustering_coefficient` | Local clustering coefficient | OCG / igraph |
| `triangles` | Triangle count per node | OCG / GDS |
| `k_core` | K-core decomposition number | igraph / GDS |

`local_clustering_coefficient` is accepted as a non-canonical input alias for
`clustering_coefficient`; it is not a separate catalog value.

### Link Prediction Scores (`by=`)

These score pairs of nodes for likely edge formation. `rank()` emits a score per node
based on its aggregate link-prediction likelihood; for pairwise results use `analyze()`.

| Value | Algorithm | Source reference |
| ------------------------- | ------------------------------------- | ---------------- |
| `preferential_attachment` | Preferential attachment score | GDS |
| `adamic_adar` | Adamic-Adar score | GDS |
| `common_neighbors` | Common neighbor count | GDS |
| `resource_allocation` | Resource allocation index | GDS |
| `total_neighbors` | Total neighbors (Jaccard denominator) | GDS |

---

## `forge.cluster()` — Community Membership

Returns an Arrow Table: all node properties + `community_id: Int64`.

`write_property` is opt-; default is read-only.

### Implemented Louvain contract

`by="louvain"` is the Rust-owned classic, unweighted multilevel Louvain
implementation at modularity resolution `1.0`. Every level begins with one
community per current node. Stable topology-ordered local moves are accepted
only when modularity gain exceeds `1e-12`; equal gains choose the community
with the smallest topology representative. The resulting communities are
condensed into weighted supernodes, and levels repeat until no positive move
or further condensation remains. These fixed defaults make repeated runs and
all bindings deterministic without adding binding-specific options.

`via` filters relationships before normalization. Louvain uses an undirected
simple projection: parallel and reciprocal relationships collapse, self-loops
are ignored, and relationship properties do not act as weights. The public
`directed` option is accepted but intentionally direction-insensitive, so both
modes return the same classic partition. Disconnected components never merge;
isolated and edgeless nodes remain singleton communities, and an empty
selection returns the typed zero-row result.

Final community IDs are consecutive `Int64` values assigned by each
community's first original topology-ordered node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties with Arrow NULLs for missing values. Execution
surrogates never escape. Shared graph/output/iteration limits and cancellation
are polled in bounded work chunks; overflow, non-finite modularity, limits,
cancellation, storage, shaping, and write-back failures remain structured Rust
errors. `write_property` is opt-in and atomically persists the full partition
only after successful execution. Rust is the sole production owner; Python and
Node only translate arguments and Arrow IPC and provide no backend, fallback,
packaging dependency, or recovery path.

### Implemented Leiden contract

`by="leiden"` is the Rust-owned classic Leiden implementation at modularity
resolution `1.0`. Each level performs topology-ordered local moves with the
same `1e-12` positive-gain threshold as Louvain, refines every coarse community
into connected subcommunities, then aggregates the refined partition while
seeding the next level from the coarse partition. Refinement considers only
positive-gain moves that preserve well-connected membership and samples them
with fixed temperature `0.01`. A fixed Rust pseudo-random stream makes that
sampling and every binding deterministic; the stream is not a public option.
Levels stop when no refinement contraction remains.

`via` filters relationships before normalization. Leiden uses an undirected,
unweighted simple projection: parallel and reciprocal relationships collapse,
self-loops are ignored, and relationship properties are not weights. The
public `directed` option is accepted but intentionally direction-insensitive.
Disconnected components never merge, every final community is connected,
isolated and edgeless nodes remain singleton communities, and an empty
selection returns the typed zero-row result.

Final community IDs are consecutive `Int64` values assigned by each
community's first original topology-ordered node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties with Arrow NULLs for missing values. Execution
surrogates never escape. Shared graph/output/iteration limits and cancellation
are polled in bounded work chunks; non-finite modularity, probability or
aggregation values, limits, cancellation, storage, shaping, and write-back
failures remain structured Rust errors. `write_property` is opt-in and
atomically persists the full partition only after successful execution. Rust
is the sole production owner; Python and Node only translate arguments and
Arrow IPC and provide no backend, fallback, packaging dependency, or recovery
path.

### Implemented label-propagation contract

`by="label_propagation"` is the Rust-owned classic asynchronous algorithm of
[Raghavan, Albert, and Kumara](https://doi.org/10.1103/PhysRevE.76.036106).
Every node starts with a unique label. Each sweep shuffles nodes using a fixed
Rust pseudo-random stream, then immediately adopts a uniformly selected label
among the most frequent labels held by its neighbors. The stream is not a
public option. Execution converges when every non-isolated node already holds
a locally dominant neighbor label, making repeated calls and all bindings
deterministic.

`via` filters relationships before an undirected, unweighted simple projection
is built. Parallel and reciprocal relationships collapse, self-loops are
ignored, relationship properties are not weights or seeds, and `directed=True`
is accepted but intentionally direction-insensitive. Disconnected components
never merge; isolated and edgeless nodes remain singleton communities; and an
empty selection returns the typed zero-row result.

Final community IDs are consecutive `Int64` values assigned by each
community's first original topology-ordered node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties with Arrow NULLs for missing values. Execution
surrogates never escape. Shared graph, output, and iteration limits plus
bounded cancellation checkpoints return structured Rust errors.
`write_property` is opt-in and atomically persists the full partition only
after successful execution. Rust is the sole production owner; Python and
Node only translate arguments and Arrow IPC and provide no backend, fallback,
packaging dependency, or recovery path.

### Implemented speaker-listener contract

`by="speaker_listener"` is the Rust-owned classic SLPA process of
[Xie, Szymanski, and Liu](https://arxiv.org/abs/1109.5720). Every node memory
starts with its unique topology label. For 100 fixed sweeps, a fixed Rust
pseudo-random stream shuffles listeners; each neighbor speaks a memory label
in proportion to its observed frequency, and the listener records a uniformly
selected most-popular received label. Updates are asynchronous. The stream and
sweep count are fixed rather than public options.

Classic SLPA produces an overlapping cover. GraphForge's stable cluster schema
and scalar write-back require one non-null `community_id` per node, so Rust
applies a documented primary-membership projection. Associations below the
paper's `0.05` threshold are pruned, then the strongest surviving association
is selected; equal strengths choose the smallest original topology label. If
thresholding removes every association, the same strongest-label rule is used
as a fallback. Bindings do not perform this reduction.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored,
properties are not weights or seeds, and `directed=True` is accepted but
intentionally direction-insensitive. Disconnected components never merge;
isolated and edgeless nodes remain singleton communities; and empty selections
return the typed zero-row result.

Final community IDs are consecutive `Int64` values ordered by each
community's first original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; memories and execution surrogates never escape.
Shared limits, bounded cancellation checkpoints, structured Rust errors, and
atomic opt-in `write_property` apply. Rust is the sole production owner;
Python and Node only translate arguments and Arrow IPC and provide no backend,
fallback, packaging dependency, or recovery path.

### Implemented Girvan-Newman contract

`by="girvan_newman"` is the Rust-owned divisive algorithm of
[Girvan and Newman](https://doi.org/10.1073/pnas.122653799). Rust computes
unweighted edge betweenness from every shortest path, removes exactly one
highest-scoring edge, and recomputes all scores after every removal. Equal
scores remove the edge with the lexicographically smallest original topology
endpoint pair. The initial connected-component partition and every resulting
split are recorded internally. Because the public cluster API returns one
partition rather than a dendrogram, GraphForge selects the recorded level with
maximum classic modularity at resolution `1.0`, measured against the original
normalized graph. A modularity tie within `1e-12` keeps the earlier, coarser
level.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored,
properties are not weights, and `directed=True` is intentionally
direction-insensitive. Initially disconnected components never merge;
isolated and edgeless nodes remain singleton communities; and empty selections
return the typed zero-row result.

Final IDs are consecutive `Int64` values ordered by each community's first
original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; removed edges, paths, and execution surrogates never
escape. Every removal consumes the shared iteration budget. Shared graph and
output limits, bounded cancellation checkpoints, structured Rust errors, and
atomic opt-in `write_property` apply. Rust is the sole production owner;
Python and Node only translate arguments and Arrow IPC and provide no backend,
fallback, packaging dependency, or recovery path.

### Implemented modularity-optimization contract

`by="modularity_optimization"` is the Rust-owned, single-level local-move
algorithm described by the
[Neo4j GDS canonical definition](https://neo4j.com/docs/graph-data-science/current/algorithms/modularity-optimization/),
using classic [Newman-Girvan modularity](https://doi.org/10.1103/PhysRevE.69.026113)
at resolution `1.0`. Every node starts in a singleton community. In stable
topology order, each node evaluates its neighboring communities and moves
immediately when the best modularity gain exceeds `1e-12`; equal gains choose
the community with the smallest topology representative. Sweeps stop when a
complete sweep makes no moves.

Unlike `by="louvain"`, this algorithm ends after the local-move phase. It never
condenses communities into supernodes, repeats on a hierarchy, or exposes a
hierarchy in the result. Resolution, tolerance, iteration count, weights, and
seeds are fixed Rust contract details rather than new public options.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored,
properties are not weights, and `directed=True` is accepted but intentionally
direction-insensitive. Disconnected components never merge; isolated and
edgeless nodes remain singleton communities; and empty selections return the
typed zero-row result.

Final IDs are consecutive `Int64` values ordered by each community's first
original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; execution surrogates never escape. Every sweep
consumes the shared iteration budget. Shared graph and output limits, bounded
cancellation checkpoints, structured Rust errors, and atomic opt-in
`write_property` apply. Rust is the sole production owner; Python and Node only
translate arguments and Arrow IPC and provide no backend, fallback, packaging
dependency, or recovery path.

### Implemented Fast Greedy contract

`by="fastgreedy"` is the Rust-owned Clauset-Newman-Moore agglomerative
algorithm described by the
[canonical igraph definition](https://r.igraph.org/reference/cluster_fast_greedy.html)
and the
[Clauset-Newman-Moore paper](https://doi.org/10.1103/PhysRevE.70.066111).
Every node starts in a singleton community. Rust repeatedly merges the
edge-adjacent community pair with the greatest classic modularity gain at
resolution `1.0`, including negative-gain merges, until each connected
component has a complete internal hierarchy. Gains equal within `1e-12`
choose the lexicographically smallest pair of original topology
representatives. Community adjacency, degrees, gains, and merge history are
maintained incrementally.

The public scalar cluster result is the recorded partition with maximum
modularity against the original normalized graph. A modularity tie within
`1e-12` keeps the earlier, finer partition. The internal hierarchy never
escapes: GraphForge adds no public dendrogram, merge matrix, weight,
resolution, tolerance, seed, target-community-count, or other Fast Greedy
option.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored,
properties are not weights, and `directed=True` is accepted but intentionally
direction-insensitive. Disconnected components never merge; isolated and
edgeless nodes remain singleton communities; and empty selections return the
typed zero-row result.

Final IDs are consecutive `Int64` values ordered by each community's first
original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; merge-state surrogates never escape. Every accepted
merge consumes the shared iteration budget. Shared graph and output limits,
bounded cancellation checkpoints, structured numeric errors, and atomic
opt-in `write_property` apply. Rust is the sole production owner; Python and
Node only translate arguments and Arrow IPC and provide no backend, fallback,
packaging dependency, or recovery path.

### Implemented Infomap contract

`by="infomap"` is GraphForge's deterministic Rust implementation of the
two-level map equation described by
[Rosvall and Bergstrom](https://doi.org/10.1073/pnas.0706851105), the
[MapEquation definition](https://www.mapequation.org/infomap/how-it-works/),
and [igraph's canonical API](https://r.igraph.org/reference/cluster_infomap.html).
Every node starts in a singleton module. In original topology order, Rust moves
nodes among incident modules (or back to an available singleton), then tests
adjacent module merges. Only a codelength improvement greater than `1e-12` is
accepted. Equal candidate scores choose the smallest original topology
representative; equal module pairs are ordered lexicographically. The fixed
single trial therefore returns the same partition on every call and binding.

`via` filters before a simple, unweighted projection is built. Parallel edges
collapse and self-loops are ignored. With `directed=False`, reciprocal
relationships also collapse and stationary visit probability is proportional
to degree. With `directed=True`, orientation is retained and Rust solves the
stationary random walk with uniform `0.15` teleportation and uniform dangling
redistribution, stopping at L1 delta `1e-12` or returning structured
non-convergence after 1,000 iterations. Weak components are optimized
independently; disconnected components never merge, isolates and edgeless
nodes remain singletons, and empty selections return the typed zero-row result.

The public API does not expose edge or vertex weights, teleportation controls,
trial counts, a seed, codelength, hierarchy, overlap, regularization, or any
Infomap-specific option. Final IDs are consecutive `Int64` values ordered by
each community's first original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; flow state and execution surrogates never escape.
Shared graph, output, and iteration limits, bounded cancellation checkpoints,
structured numeric errors, and atomic opt-in `write_property` apply. Rust is
the sole production owner; Python and Node only translate arguments and Arrow
IPC and provide no backend, fallback, packaging dependency, or recovery path.

### Implemented Leading Eigenvector contract

`by="leading_eigenvector"` is the Rust-owned recursive modularity bisection
defined by [igraph](https://r.igraph.org/reference/cluster_leading_eigen.html)
and [Newman's paper](https://doi.org/10.1103/PhysRevE.74.036104). For each
current community, Rust constructs the generalized modularity matrix
`B^(g) = B - diag(B row sums within g)`, assigns nodes by the signs of its
leading eigenvector, and recurses on both sides. A split is accepted only when
both its leading eigenvalue and classic modularity gain exceed `1e-12`.

The symmetric eigensolver is a built-in deterministic Jacobi rotation. It
chooses the largest off-diagonal magnitude, breaks pivot and eigenvalue-column
ties by matrix order, stops at tolerance `1e-12`, and orients the eigenvector
by making its first significant entry positive. The dense matrix path rejects
selections above 4,096 nodes with a structured resource-limit error.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored, and
`directed=True` is intentionally symmetrized just like `directed=False`.
Disconnected components are split independently; isolates and edgeless nodes
remain singleton communities; and empty selections return the typed zero-row
result. GraphForge exposes no public weights, steps, starting membership,
ARPACK options, callback, resolution, seed, tolerance, eigenvalue or
eigenvector output, merges, dendrogram, hierarchy, or algorithm-specific
option.

Final IDs are consecutive `Int64` values ordered by each community's first
original topology node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; spectral state and execution surrogates never escape.
Shared graph, output, and iteration limits, bounded cancellation checkpoints,
structured numeric errors, and atomic opt-in `write_property` apply. Rust is
the sole production owner; Python and Node only translate arguments and Arrow
IPC and provide no backend, fallback, packaging dependency, or recovery path.

### Implemented Walktrap contract

`by="walktrap"` is the Rust-owned Pons-Latapy algorithm described by
[igraph](https://r.igraph.org/reference/cluster_walktrap.html) and the
[original paper](https://jgaa.info/index.php/jgaa/article/view/paper124).
Rust computes each node's four-step transition distribution, then measures
communities by the paper's degree-normalized Euclidean distance between their
mean distributions. Isolates use a unit self-transition.

Starting from singletons, Rust repeatedly merges the edge-adjacent pair with
minimum Ward increase
`(1/n) * |C1||C2|/(|C1|+|C2|) * r(C1,C2)^2`. Costs equal within `1e-12`
choose the lexicographically smallest pair of original topology
representatives. Merges continue until each connected component has one
community. The public result is the recorded partition with maximum classic
modularity; a tie within `1e-12` keeps the earlier, finer partition.

`via` filters before an undirected, unweighted simple projection is built.
Parallel and reciprocal relationships collapse, self-loops are ignored, and
both `directed` values symmetrize. Disconnected components never merge;
isolates and edgeless nodes remain singletons; and empty selections return the
typed zero-row result. The dense transition path rejects selections above
4,096 nodes before allocation.

GraphForge exposes no public edge weights, walk `steps`, target cluster count,
merges, modularity series, dendrogram, hierarchy, seed, tolerance, or other
Walktrap option. Final IDs are consecutive `Int64` values ordered by each
community's first original node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; transition, distance, merge, and execution
surrogates never escape. Shared limits, bounded cancellation, structured
numeric errors, and atomic opt-in `write_property` apply. Rust is the sole
production owner; Python and Node only translate arguments and Arrow IPC and
provide no backend, fallback, packaging dependency, or recovery path.

### Implemented Spinglass contract

`by="spinglass"` is GraphForge's Rust-owned positive-edge
Reichardt-Bornholdt Potts model, following
[igraph's definition](https://r.igraph.org/reference/cluster_spinglass.html)
and the [Reichardt-Bornholdt paper](https://doi.org/10.1103/PhysRevE.74.016110).
For each undirected simple component, Rust uses the configuration null model
`B_ij = A_ij - k_i k_j/(2m)`, fixed resolution `gamma = 1`, and energy
`H = -sum(i<j) B_ij * [spin_i = spin_j]`. Up to 25 spin states are available,
capped by component size.

Annealing is asynchronous and fixed: temperature starts at `1.0`, stops below
`0.01`, cools by `0.99`, and performs one deterministic shuffled sweep per
temperature using GraphForge's built-in SplitMix64 stream and frozen seed.
Each proposal uniformly chooses another available spin and applies the
Metropolis rule. Energy ties and equal lowest-energy partitions use original
topology lexicographic order; Rust retains the lowest finite energy observed.

`via` filters before an undirected, unweighted simple projection is built.
Reciprocal and parallel relationships collapse, self-loops are ignored,
relationship properties are not weights, and both `directed` values produce
the same projection. Components are annealed independently, isolates and
edgeless nodes remain singletons, and empty selections return the typed
zero-row result. Dense Hamiltonian work rejects selections above 4,096 nodes
before allocation.

GraphForge exposes no public edge weights, target vertex, `spins`, parallel
update, temperature or cooling controls, update rule, `gamma`, negative
implementation, `gamma.minus`, seed, energy, cohesion/adhesion, hierarchy, or
other Spinglass option. Final IDs are consecutive `Int64` values ordered by
each community's first original node. Rows remain non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized properties; spins, Hamiltonian values, random state, and
execution surrogates never escape. Shared limits, bounded cancellation,
finite numeric validation, structured errors, and atomic opt-in
`write_property` apply. Rust is the sole production owner; Python and Node are
thin Arrow IPC bindings with no runtime backend, fallback, packaging
dependency, or recovery path.

### Implemented HDBSCAN contract

`by="hdbscan"` is GraphForge's Rust-owned HDBSCAN* implementation, based on
the hierarchy and excess-of-mass (EOM) selection described by
[Campello, Moulavi, Zimek, and Sander](https://doi.org/10.1145/2733381).
The [Neo4j GDS HDBSCAN documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/hdbscan/)
is the catalog-parity reference; GraphForge deliberately freezes its own
deterministic settings rather than mirroring GDS configuration options.

`vector_property` is required. Every selected node must have a non-empty,
same-dimensional list containing only finite numeric values. Rust loads those
graph-native vectors by UUID in topology order; the knowledge layer and vector
indexes are neither read nor required. HDBSCAN does not traverse relationships,
so `via` is rejected and both values of the inherited `directed` option return
the same result.

The metric is Euclidean. Both `min_cluster_size` and `min_samples` are fixed at
`5`; the core distance is the fifth-nearest distance including the point
itself. Mutual reachability is
`max(core(a), core(b), euclidean(a, b) / alpha)` with fixed `alpha = 1.0`.
Rust constructs a complete mutual-reachability minimum spanning tree, ordering
equal-weight edges by their canonical topology endpoint pair, then builds the
single-linkage and minimum-size-five condensed hierarchies. EOM selects a
parent when its stability equals the sum of its children. Single-cluster
selection is disabled, the root is never returned, and cluster-selection
epsilon is fixed at `0.0`. Final cluster IDs are consecutive `Int64` values
ordered by each cluster's first topology node; unselected points are noise with
ID `-1`.

Empty selections preserve the typed zero-row schema. Selections below five
points, an all-duplicate single-root hierarchy, and data with no selected
cluster return noise rather than inventing a cluster. The public schema is
non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized node properties with Arrow NULLs for
missing values. Internal distances, hierarchy nodes, stability values, and
execution surrogates never escape. `write_property` remains opt-in and
atomically persists every cluster or `-1` noise value only after successful
execution.

Vector clustering rejects more than 4,096 selected nodes or more than
16,777,216 feature cells before dense work. Checked arithmetic, finite-distance
validation, bounded cancellation checkpoints, shared output limits, storage,
shaping, and write-back failures use structured Rust errors. There are no
public min-cluster, sample, leaf-size, metric, alpha, cluster-selection,
probability, outlier, hierarchy, prediction, approximation, concurrency, or
other HDBSCAN options beyond `vector_property`. Rust is the sole production
owner; Python and Node only translate arguments and Arrow IPC and provide no
runtime backend, fallback, recovery path, or packaging dependency.

### Implemented K-means contract

`by="k_means"` is GraphForge's deterministic Rust-owned K-means implementation.
The [Neo4j GDS K-means documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/kmeans/)
is the catalog-parity reference, while GraphForge freezes a smaller public
contract. `vector_property` is required: every selected node must have a
non-empty, same-dimensional graph-native list of finite numbers. Vectors are
loaded by UUID in topology order. Knowledge-layer files and vector indexes are
not read or required, preserving graph-native execution independently of
Knowledge-layer presence. Relationships are not inputs, so `via` is rejected
and both `directed` values return the same result.

The cluster count is fixed at `k = 10`. The first topology point is the first
centroid; each later centroid is the point with the greatest squared distance
to its nearest selected centroid, with topology order breaking ties. Lloyd
assignment uses squared Euclidean distance and the lower centroid index wins an
equal-distance tie. Each non-empty centroid becomes the arithmetic mean of its
assigned vectors; an empty cluster retains its prior centroid. Execution stops
only when the full assignment is exactly unchanged and otherwise returns a
structured non-convergence error after 100 iterations. Final community IDs are
consecutive `Int64` values ordered by the first topology point assigned to each
observed community. Duplicate vectors are valid and may collapse multiple
centroids into fewer returned communities.

Empty selections preserve the typed zero-row result; non-empty selections with
fewer than ten nodes are rejected. The schema is non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties. Centroids, distances, assignments, and execution
surrogates never escape. Vector clustering rejects more than 4,096 selected
nodes or more than 16,777,216 feature cells before dense work. Invalid vectors,
checked numeric failures, resource exhaustion, non-convergence, cancellation,
shaping, storage, and write-back failures are structured Rust errors.
`write_property` is opt-in and atomically persists the complete successful
partition; any rejected write leaves existing values unchanged.

There is no public cluster-count, max-iteration, threshold, restart, seed,
sampler, seeded-centroid, distance, centroid, silhouette, concurrency, or other
K-means option beyond `vector_property`. Rust is the sole production owner.
Python and Node only translate arguments and Arrow IPC and have no algorithm
backend, fallback, recovery path, or packaging dependency.

### Implemented approximate maximum-cut contract

`by="approximate_max_k_cut"` is GraphForge's deterministic Rust-owned
approximate maximum-cut implementation. The public objective is fixed at an
unweighted two-way cut (`k = 2`). The
[Neo4j GDS Approximate Maximum k-cut documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/approx-max-k-cut/)
is the catalog-parity reference, and the
[NetworkX `one_exchange` definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.approximation.maxcut.one_exchange.html)
is an optional development reference for the local-search rule. Neither is a
runtime backend, fallback, recovery path, or dependency.

Rust initializes every node in community `0`, computes each node's change in
cut value, and repeatedly moves the node with the greatest strictly positive
gain. Equal gains choose the earliest topology node. Moving one node updates
the gains of its incident neighbors; execution stops at the deterministic
one-exchange local optimum where no single move improves the cut. If the first
topology node ends in community `1`, Rust flips the complete binary partition
so that node is canonical `0`. Isolates are always reset to `0`; the only
public community IDs are therefore stable `0` and `1`.

`via` filters relationships before execution. Orientation is intentionally
ignored, so both `directed` values return the same result. Every selected
relationship contributes one unit independently, including parallel and
reciprocal relationships; relationship properties are not weights. Self-loops
contribute zero. Disconnected components cannot affect one another's cut,
although their improving moves share one stable global ordering. An empty
selection returns the typed zero-row result, and an edgeless selection assigns
every isolate to `0`.

The schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized node properties with Arrow NULLs for
missing values. Internal topology IDs, gains, and move state never escape.
Selections above 4,096 nodes or 16,777,216 direction-expanded adjacency
entries fail before local search; more than 10,000 improving moves exhaust the
shared move budget. Checked finite non-negative internal weights, bounded
cancellation checkpoints, output limits, shaping, storage, and write failures
remain structured Rust errors. `write_property` is read-only by default and
atomically persists the complete successful partition when opted into; a
failed write leaves existing properties unchanged.

There is no public `k`, relationship-weight, seed, iteration, VNS,
concurrency, minimum-community-size, or other maximum-cut option, and
`vector_property` is rejected. Graph-native execution does not read or require
Knowledge-layer state, preserving the topology-only knowledge isolation invariant. Rust is the
sole production owner. Python and Node only translate arguments and Arrow IPC
and provide no algorithm backend, fallback, recovery path, or packaging
dependency.

### Implemented strongly-connected-components contract

`by="strongly_connected"` is GraphForge's deterministic Rust-owned
strongly-connected-components implementation. It finds each maximal set whose
members are mutually reachable, following the
[Neo4j GDS SCC contract](https://neo4j.com/docs/graph-data-science/current/algorithms/strongly-connected-components/).
The kernel is non-recursive Tarjan; the official
[NetworkX definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.strongly_connected_components.html)
is an optional development reference for that algorithm family only. Neither
project is a runtime backend, fallback, recovery path, or dependency.

With `directed=true`, relationship orientation determines mutual reachability.
With `directed=false`, the adjacency adapter symmetrizes selected relationships,
so the result is the ordinary connected-component partition. `via` filters
relationships before either projection. Relationship properties and weights
are ignored; parallel edges, reciprocal edges, and self-loops do not alter
membership. Empty selections return the typed zero-row result. Isolates are
singleton components, and disconnected subgraphs remain separate.

Raw Tarjan discovery labels are never public. Rust renumbers components to
consecutive `Int64` IDs in the order of each component's first node in stable
topology order, and emits rows in that same node order. The schema is non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties with Arrow NULLs for missing values. Internal
topology, discovery, low-link, stack, and frame values never escape.

The shared limits reject selections above 10,000,000 nodes, 100,000,000
direction-expanded adjacency entries, or 10,000,000 output rows. Iterative DFS
work has bounded cooperative checkpoints and the shared 10,000-checkpoint
budget; cancellation is checked before and during execution. Validation,
limits, cancellation, shaping, storage, and write failures remain structured
Rust errors. `write_property` is read-only by default and atomically persists
the complete successful partition when opted into; any failure preserves all
existing properties.

There is no public concurrency, consecutive-ID toggle, relationship-weight,
seed, iteration, minimum-component-size, stats, or other SCC option, and
`vector_property` is rejected. Graph-native execution does not read or require
Knowledge-layer state, preserving the topology-only knowledge isolation invariant. Rust is the
sole production owner. Python and Node only translate arguments and Arrow IPC
and provide no algorithm backend, fallback, recovery path, or packaging
dependency.

### Implemented biconnected-components contract

`by="biconnected"` is GraphForge's deterministic Rust-owned biconnected-block
implementation. A non-recursive Hopcroft-Tarjan DFS and edge stack compute the
true edge-partitioned block cover described by the official
[igraph biconnected-components contract](https://r.igraph.org/reference/biconnected_components.html).
The official
[NetworkX non-recursive DFS definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.biconnected_components.html)
is an optional development reference only. Neither project is a runtime
backend, fallback, recovery path, dependency, or packaging input.

The membership projection is undirected, unweighted, and simple. `via` filters
relationships first, both `directed` values have identical semantics,
reciprocal and parallel relationships collapse, self-loops are ignored, and
relationship properties are never weights. A dyad, including a bridge, is a
valid block. Empty selections preserve the typed zero-row result; edgeless
nodes and isolates receive singleton primary groups, and disconnected
subgraphs are processed independently.

Native blocks overlap at articulation vertices. Rust preserves that true cover
internally, then projects it onto the scalar `community_id` contract: members
within each block are sorted by topology order, blocks are ordered
lexicographically by those member lists, and every articulation vertex selects
its earliest containing block. Isolates extend the projection with deterministic
singleton primaries, and primary groups left empty by articulation selection
are omitted. Observed groups are renumbered consecutively by first topology
member; rows remain in stable topology order.

The schema is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized node properties with Arrow NULLs for
missing values. DFS, low-link, edge-stack, articulation, block, and topology
surrogates never escape. Selections above 10,000,000 nodes, 100,000,000
direction-expanded adjacency entries, or 10,000,000 output rows fail through
the shared guards. DFS and projection work use bounded cancellation checkpoints
and the shared 10,000-checkpoint budget. Validation, limits, cancellation,
execution, shaping, storage, and write failures remain structured Rust errors.

`write_property` is read-only by default and atomically stores the complete
successful scalar primary-membership projection when opted into; any failure
preserves all existing properties. There is no public concurrency, overlap
toggle, articulation output, relationship weight, seed, iteration, minimum
block size, stats, or other biconnected option, and `vector_property` is
rejected. Graph-native execution does not read or require knowledge-layer state,
preserving the topology-only knowledge isolation invariant. Rust is the
sole production owner. Python and Node only translate arguments and Arrow IPC
and provide no algorithm backend, fallback, recovery path, or packaging
dependency.

### Implemented k-core-decomposition contract

`by="k_core_decomposition"` is GraphForge's deterministic Rust-owned structural
decomposition. It shares the Batagelj-Zaversnik peeling kernel used by
`rank(by="k_core")`. The official
[igraph coreness contract](https://r.igraph.org/reference/coreness.html) and
[Neo4j GDS K-Core documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/k-core/)
are catalog and optional development-parity references only. Neither project,
nor NetworkX, is a runtime backend, fallback, recovery path, dependency, or
packaging input.

The topology projection is undirected, unweighted, and simple. `via` filters
relationships before projection; both `directed` values intentionally produce
the same result. Reciprocal and parallel relationships collapse, self-loops are
ignored, and relationship properties are never weights. Disconnected subgraphs
are peeled independently. Empty selections preserve the typed zero-row result;
edgeless nodes and isolates have core number `0`.

The public `community_id: Int64` is each node's exact non-negative core (shell)
number. It is not a partition label and is never renumbered. Rows remain in
stable topology order. The schema is non-null
`node_uuid: FixedSizeBinary(16)`, non-null `community_id: Int64`, then
materialized node properties with Arrow NULLs for missing values. Peeling
degrees, bins, positions, and topology surrogates never escape.

Shared limits reject selections above 10,000,000 nodes, 100,000,000
direction-expanded adjacency entries, or 10,000,000 output rows. Peeling uses
bounded cooperative checkpoints and the shared 10,000-checkpoint budget;
cancellation is checked before and during execution. Validation, limits,
cancellation, shaping, storage, and write failures remain structured Rust
errors. `write_property` is read-only by default and atomically persists the
complete successful core-number result when opted into; any failure preserves
all existing properties.

There is no public concurrency, degree-threshold, seed, iteration,
degeneracy-only, component, stats, relationship-weight, or other decomposition
option, and `vector_property` is rejected. Graph-native execution does not read
or require knowledge-layer state, preserving the topology-only knowledge isolation invariant. Rust is
the sole production owner. Python and Node only translate arguments and Arrow
IPC and contain no algorithm implementation.

### Community Detection (`by=`)

| Value | Algorithm | Source reference |
| ------------------------- | ----------------------------------------- | ------------------ |
| `louvain` | Louvain modularity maximization | OCG / GDS / igraph |
| `leiden` | Leiden (improved Louvain) | GDS / igraph |
| `label_propagation` | Label propagation | OCG / GDS / igraph |
| `speaker_listener` | Speaker-listener label propagation (SLPA) | GDS |
| `girvan_newman` | Girvan-Newman (edge betweenness) | OCG |
| `modularity_optimization` | Direct modularity optimization | GDS |
| `fastgreedy` | Clauset-Newman-Moore fastgreedy | igraph |
| `infomap` | Infomap flow-based clustering | igraph |
| `leading_eigenvector` | Newman leading eigenvector | igraph |
| `walktrap` | Walktrap random walk clustering | igraph |
| `spinglass` | Spinglass statistical mechanics | igraph |
| `hdbscan` | HDBSCAN density-based clustering | GDS |
| `k_means` | K-means on node embeddings | GDS |
| `approximate_max_k_cut` | Approximate max k-cut partitioning | GDS |

### Components (`by=`)

| Value | Algorithm | Source reference |
| -------------------- | -------------------------------------- | ------------------ |
| `components` | Weakly connected components | OCG / igraph |
| `strongly_connected` | Strongly connected components (Tarjan) | OCG / GDS / igraph |
| `biconnected` | Biconnected components | igraph |

### Structural Decomposition (`by=`)

| Value | Algorithm | Source reference |
| ---------------------- | ------------------------------------------- | ---------------- |
| `k_core_decomposition` | K-core decomposition (assigns shell number) | igraph / GDS |

---

## `forge.paths()` — Path Finding and Flow

Returns an Arrow Table whose schema depends on the algorithm:

- Shortest-path algorithms: `source_uuid`, `target_uuid`, `cost`, `path` (list of UUIDs)
- Flow algorithms: `source_uuid`, `sink_uuid`, `flow`, edge flow columns
- Random walk: `node_uuid`, `walk` (list of UUIDs)

`source` and `target` accept UUIDs, graph-owned `NodeHandle` values, or unique typed
label/property selectors. Internal adjacency and storage IDs never appear in results.

### Shortest Paths (`by=`)

| Value | Algorithm | Source reference |
| -------------------- | ----------------------------------------- | ---------------- |
| `bfs` | Breadth-first search (hop-count distance) | OCG / GDS |
| `dijkstra` | Dijkstra single-source or source-target | OCG / GDS |
| `dijkstra_all_pairs` | All-pairs Dijkstra | OCG / GDS |
| `astar` | A* with caller-supplied heuristic | OCG / GDS |
| `bellman_ford` | Bellman-Ford (handles negative weights) | OCG / GDS |
| `floyd_warshall` | Floyd-Warshall all-pairs | OCG / GDS |
| `delta_stepping` | Delta-stepping parallel SSSP | GDS |
| `yens` | Yen's k-shortest paths | GDS |

#### Canonical `bfs` contract

`bfs` is an unweighted hop-count traversal. With no `target`, it returns the source followed by
every reachable node; with a `target`, it returns one deterministic shortest path or zero rows
when the target is disconnected. The non-null schema is exactly
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. The path includes both endpoints and `cost` is its
hop count.

Rows are ordered by hop count and then stable topology/UUID order. Equal-length predecessor
ties use that same stable order, so repeated calls and all three bindings return identical
paths. `directed=True` is the default; `directed=False` traverses either edge direction, and
`via` restricts traversal to one relationship type. Parallel edges collapse for reachability,
self-loops do not revisit a node, and a singleton source produces one zero-cost row.

Canonical BFS requires `k=1` and no `weight`. Null or malformed selectors, missing, ambiguous,
or cross-graph nodes, blank `via`, unsupported options, and unavailable algorithms return
structured errors. Shared Rust graph/output limits and cooperative cancellation bound the
traversal. Python and Node only coerce these inputs and convert the Rust-produced Arrow batch.

#### Canonical `dijkstra` contract

`dijkstra` computes exact non-negative shortest paths. With no `target`, it returns the source
and every reachable node; with a `target`, it returns one deterministic shortest path or zero
rows when the target is disconnected. Its non-null schema is exactly
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Paths include both endpoints.

Omitting `weight` assigns every selected edge a cost of `1.0`. When `weight` names an edge
property, every selected edge must contain a finite, numeric, non-negative value; missing,
null, non-numeric, negative, or non-finite values fail the whole call with a structured error.
`directed=True` is the default, `directed=False` traverses either edge direction, and `via`
restricts the graph to one relationship type. Parallel edges remain distinct candidates;
self-loops cannot improve a simple shortest path.

All-reachable rows use stable topology/UUID order. Equal-cost paths are resolved by the stable
topology order of the complete path, then edge order, so Rust, Python, and Node return the same
result across repeated calls. Canonical Dijkstra requires `k=1`. Selector validation, shared
node/edge/output/iteration limits, and cooperative cancellation are enforced in typed Rust
code. Internal execution surrogates never escape, and results do not depend on a knowledge
layer being present.

The implementation and dispatch owner is `graphforge-exec`; Python and Node only coerce inputs and
convert its Arrow output. Official Neo4j Graph Data Science documentation is an optional
development parity reference only. GraphForge has no igraph, NetworkX, Neo4j, SciPy, or
external path-service runtime backend, fallback, packaging dependency, or recovery path.

#### Canonical `dijkstra_all_pairs` contract

`dijkstra_all_pairs` computes one exact shortest path for every reachable ordered pair of
distinct nodes in the selected graph. Self-pairs and unreachable pairs are omitted. The shared
`source` argument is still required and fully validated for existence and graph ownership, but
it does not restrict the computation; supplying any `target` is a structured validation error.
Rows are ordered by source topology/UUID and then target topology/UUID.

The non-null output schema is exactly `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Paths include both endpoints. Public results contain
UUID identity only; internal execution surrogates never escape.

Omitting `weight` assigns every selected edge a cost of `1.0`. A named weight is strict across
every selected edge and must be numeric, finite, and non-negative; zero is allowed. Missing,
null, non-numeric, negative, or non-finite values, and non-finite accumulated costs, fail the
whole invocation without partial output. Parallel edges remain eligible and the least-cost
route wins. Equal-cost paths resolve by the lexicographically smallest full node-topology
sequence and then edge topology order.

`directed=True` follows outgoing edges; `directed=False` uses the shared undirected projection.
`via=None` admits every relationship type, while `via` selects one valid type.
`dijkstra_all_pairs` requires `k=1`. Empty and disconnected graphs return a valid empty Arrow
batch. Shared node, edge, iteration, and output limits plus cooperative cancellation cover the
complete invocation.

Projection, validation, execution, deterministic tie resolution, limits, and shaping are owned
by Rust in `graphforge-exec`; Python and Node are thin argument/Arrow adapters. Results are independent
of knowledge-layer presence. Official
[Neo4j GDS all-pairs shortest path documentation][gds-apsp] is an optional catalog and
development reference only. GraphForge has no igraph, NetworkX, Neo4j, SciPy, external path
service, fallback, packaging dependency, or recovery runtime.

[gds-apsp]: https://neo4j.com/docs/graph-data-science/current/algorithms/all-pairs-shortest-path/

#### Canonical `astar` contract

`astar` computes one exact source-to-target shortest path and requires `target`; a disconnected
target returns zero rows. Its non-null UUID-only schema is `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. The path includes both endpoints, and a source equal
to its target returns the singleton path at zero cost.

Omitting `heuristic` uses zero for every node, making A* equivalent to source-target Dijkstra.
A named heuristic is a strict node-property projection across every selected node: values must
be numeric, finite, and non-negative, and the target value must be zero. Missing, null,
non-numeric, negative, non-finite, or nonzero-target values fail the whole invocation. The
caller is responsible for supplying an admissible heuristic; with that condition, A* returns
the exact shortest path. Unlike Dijkstra, A* may use an admissible estimate to avoid exploring
irrelevant nodes.

Edge selection and costs match Dijkstra: `directed=True` follows outgoing edges,
`directed=False` uses the shared undirected projection, `via` restricts relationship type, and
an omitted `weight` gives every edge cost `1.0`. A named weight must be numeric, finite, and
non-negative on every selected edge. `k` must be `1`. Equal-cost candidates use stable
full-path topology/UUID order and then edge order, so repeated Rust, Python, and Node calls
agree. Shared graph, iteration, output, and cancellation limits apply.

Projection, validation, execution, and Arrow shaping are owned by Rust in `graphforge-exec`; bindings
only adapt arguments and output. Results are independent of the knowledge layer.
Official graph-library material is an optional catalog/development reference only: GraphForge
has no igraph, NetworkX, Neo4j, SciPy, external path service, fallback, packaging dependency,
or recovery runtime.

#### Canonical `bellman_ford` contract

`bellman_ford` computes exact shortest paths while permitting negative edge weights. With no
`target`, it returns the source and every reachable node in stable topology/UUID order; with a
`target`, it returns one deterministic shortest path or zero rows when disconnected. A source
equal to its target returns the singleton path at zero cost. Its non-null UUID-only schema is
exactly `source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`,
`cost: Float64`, and `path: List<FixedSizeBinary(16) not null>`, with both endpoints in each
path.

Omitting `weight` gives every selected edge cost `1.0`. A named weight is strict across every
selected edge: each value must be present, non-null, numeric, and finite. Negative and zero
values are valid. Missing, null, non-numeric, non-finite, or non-finite accumulated costs fail
the whole call with a structured error. Unlike Dijkstra and A*, Bellman-Ford accepts negative
weights and does not accept `heuristic`; unlike A*, `target` is optional.

Any negative cycle reachable from the source fails the complete invocation without partial
output, including a target-specific call whose requested target is not on the cycle. A
negative self-loop is such a cycle. Under `directed=False`, each selected edge is traversable
both ways, so any reachable negative edge forms a two-edge negative cycle and fails the call.
`directed=True` follows outgoing edges, while `via` restricts the relationship type. Parallel
edges remain distinct candidates.

Equal-cost routes resolve by the lexicographically smallest complete node topology/UUID
sequence and then edge topology order. Canonical Bellman-Ford requires `k=1`. Shared node,
edge, iteration, and output limits plus cooperative cancellation bound relaxation and the
negative-cycle scan. Selector validation, projection, execution, tie resolution, and Arrow
shaping are owned by typed Rust in `graphforge-exec`; Python and Node only adapt arguments and output.

Results are independent of knowledge-layer presence, preserving the graph/knowledge boundary. External graph-library documentation is an optional development reference only.
GraphForge has no igraph, NetworkX, Neo4j, SciPy, external path service, runtime fallback,
packaging dependency, or recovery path for Bellman-Ford.

#### Canonical `floyd_warshall` contract

`floyd_warshall` computes exact shortest paths for every reachable ordered pair of distinct
nodes in the selected graph. The shared `source` argument remains required and is validated
for existence and graph ownership, but it does not restrict this global computation.
Supplying `target` is a structured validation error. Self-pairs and unreachable pairs are
omitted, so an empty graph or a graph without reachable non-self pairs returns a valid empty
table.

The non-null output schema is exactly `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Paths include both endpoints. Rows sort by source
topology/UUID order and then target topology/UUID order. Equal-cost candidates resolve by the
lexicographically smallest complete node topology/UUID sequence and then the complete edge
topology sequence. Parallel edges therefore remain distinct candidates while repeated calls
and every language surface return the same representative.

Omitting `weight` assigns every selected edge cost `1.0`. A named weight is strict over every
selected edge: each value must be present, non-null, numeric, and finite. Negative, zero, and
positive values are accepted. Missing, null, non-numeric, non-finite, or non-finite
accumulated costs fail the complete invocation without partial output. Unlike
`dijkstra_all_pairs`, Floyd-Warshall supports negative weights and uses one global dynamic
program rather than running a non-negative single-source search for each node.

Any negative cycle anywhere in the selected graph fails the entire invocation, including a
cycle in a component disconnected from other nodes. A negative self-loop is such a cycle.
With `directed=False`, each selected edge is traversable in both directions, so any negative
edge forms a negative two-way cycle and fails the call. `directed=True` follows edge
orientation, while `via` restricts the relationship type.

Canonical Floyd-Warshall requires `k=1` and does not accept `heuristic`. Its worst-case time is
cubic in selected-node count. This implementation retains complete deterministic paths per
reachable pair, so its worst-case retained-path space is also cubic rather than the quadratic
distance-only bound. Shared node, edge, work-iteration, and output limits plus cooperative
cancellation cover projection, matrix initialization and relaxation, negative-cycle checks,
path reconstruction, and Arrow shaping.

Typed Rust in `graphforge-exec` owns projection, validation, execution, deterministic ties, limits,
cancellation, and UUID-only Arrow shaping. Python and Node only adapt arguments and output.
Results are independent of knowledge-layer presence.
External graph-library documentation is an optional catalog and development reference only;
GraphForge has no igraph, NetworkX, Neo4j, SciPy, external path service, runtime fallback,
packaging dependency, or recovery path for Floyd-Warshall.

#### Canonical `delta_stepping` contract

`delta_stepping` computes exact non-negative single-source shortest paths with a fixed,
non-configurable bucket width of `delta = 1.0`. A tentative distance belongs to bucket
`floor(distance / 1.0)`. Within the lowest non-empty bucket, the Rust executor repeatedly
relaxes light edges (`weight <= 1.0`) until that bucket is closed, then relaxes heavy edges
(`weight > 1.0`) once from every node settled during the light phase. Improved distances are
reinserted into their corresponding buckets. Unlike canonical `dijkstra`, which selects the
next individual candidate from a priority queue, Delta-stepping advances distance buckets and
separates light from heavy relaxation; both return exact shortest paths under GraphForge's
non-negative-weight contract.

`source` is required. With no `target`, the result contains the source at cost `0.0` followed
by every reachable node; with a target, it contains one path or zero rows when disconnected.
A source equal to its target returns the singleton path at zero cost. The non-null UUID-only
schema is exactly `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`, with both endpoints in each path. All-reachable
rows use stable target topology/UUID order. Equal-cost candidates resolve by complete node
topology/UUID sequence and then complete edge topology order, including parallel-edge ties.

Omitting `weight` assigns every selected edge cost `1.0`. A named property is strict across
every selected edge: its value must be present, non-null, numeric, finite, and non-negative.
Missing, null, non-numeric, negative, or non-finite values, and non-finite accumulated costs,
fail the whole call with a structured error. `directed=True` follows outgoing edges;
`directed=False` makes each selected edge traversable in both directions. `via=None` includes
all relationship types, while a valid `via` selects one type. Blank relationship or weight
selectors are invalid. `k` must be `1`, and `heuristic` is not accepted.

Shared limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative iteration checkpoints.
Projection errors, invalid selectors or options, strict-weight failures, accumulated-cost
overflow, limit violations, cancellation, execution errors, and Arrow shaping failures abort
the invocation without partial output.

Projection, validation, bucket execution, deterministic tie resolution, limits, cancellation,
and Arrow shaping are Rust-owned in `graphforge-exec`. Python and Node are argument/Arrow adapters
only. Exploratory operation without a knowledge layer produces the same graph result,
satisfying the graph/knowledge boundary. External algorithm documentation and libraries may be used
as development parity references only; no igraph, NetworkX, Neo4j, SciPy, service, runtime
fallback, packaging dependency, or recovery path participates in Delta-stepping.

#### Canonical `yens` contract

`yens` returns up to `k` exact, distinct, loopless source-to-target paths. The Rust executor
finds the first constrained shortest path, then derives root-and-spur alternatives while
excluding nodes and edges used by previously accepted paths at the relevant spur. Candidates
are deduplicated by their public identity, the complete node topology/UUID sequence, and the
lowest-cost candidate for that sequence is retained. Equal candidates resolve by complete
node topology/UUID sequence and then complete edge topology order. The executor accepts the
lowest ordered candidate until `k` paths have been returned or no candidate remains.

Both `source` and `target` are required, and `k` must be at least `1`. Disconnected endpoints
return zero rows; equal endpoints return one rank-1, zero-cost singleton path. Every returned
path contains both endpoints and repeats no node, so cycles and self-loops cannot appear in a
result. Parallel-edge variants with the same node sequence collapse to one public path: the
least-cost variant wins, followed by stable edge topology order. Rows have the exact non-null
UUID-only schema `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `rank: UInt64`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Ranks are contiguous and one-based after ordering
by cost, complete node topology/UUID sequence, and complete edge topology sequence.

Omitting `weight` assigns every selected edge cost `1.0`. A named property is strict across
every selected edge: its value must be present, non-null, numeric, finite, and non-negative;
zero is valid. Missing, null, non-numeric, negative, or non-finite values, and non-finite
accumulated costs, fail the whole call with a structured error. `directed=True` follows
outgoing edges; `directed=False` makes each selected edge traversable in both directions.
`via=None` includes all relationship types, while a valid `via` selects one type. Blank
relationship or weight selectors are invalid, and `heuristic` is not accepted.

The constrained solver stores and explores complete simple-path states rather than one best
distance label per node. Its worst-case running time and candidate/path-state space are
therefore exponential in the number of selected nodes, especially in dense cyclic graphs;
each spur search may also clone and compare paths in proportion to path length. This
implementation does not have textbook Dijkstra complexity.

Shared limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative iteration checkpoints.
Projection errors, invalid selectors or options, strict-weight failures, accumulated-cost
overflow, limit violations, cancellation, execution errors, and Arrow shaping failures abort
the invocation without partial output.

Projection, validation, loopless path-state execution, deterministic candidate selection,
limits, cancellation, and Arrow shaping are Rust-owned in `graphforge-exec`. Python and Node are
argument/Arrow adapters only. The result depends only on selected graph topology and weights
and does not consume or require knowledge-layer state, preserving the graph/knowledge boundary.
External algorithm documentation and libraries may be development references only; no
igraph, NetworkX, Neo4j, SciPy, service, runtime fallback, packaging dependency, or recovery
path participates in Yen execution.

### Flow (`by=`)

| Value | Algorithm | Source reference |
| ------------------------- | ----------------------------------------- | ---------------- |
| `max_flow` | Maximum-flow scalar | OCG / GDS |
| `max_flow_edges` | Maximum-flow edge assignment | OCG / GDS |
| `min_cut` | Minimum cut capacity | OCG / GDS |
| `min_cut_edges` | Minimum-cut edge assignment | OCG / GDS |
| `min_cost_max_flow` | Minimum cost maximum flow | GDS |
| `min_cost_max_flow_edges` | Minimum cost maximum-flow edge assignment | GDS |
| `gomory_hu_tree` | Gomory-Hu tree (all-pairs min-cut) | igraph |

#### Canonical `max_flow` and `max_flow_edges` contract

The two catalog values are stable public views of one normalized, validated Rust flow
solution. `max_flow` returns exactly one non-null row with
`source_uuid: FixedSizeBinary(16)`, `sink_uuid: FixedSizeBinary(16)`, and `flow: Float64`.
`max_flow_edges` returns exactly one non-null row for every selected canonical edge, including
zero-flow edges, with `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and `flow: Float64`.
Both schemas have version `1` and `graphforge.verb=paths`; their algorithm metadata names the
selected view. UUIDs are stored public identity, never execution surrogates. Keeping scalar
and edge grain in separate catalog values prevents option-dependent nullable columns.

Both values require exactly one resolved source and target and reject equal endpoints.
`via=None` selects every relationship type; a valid `via` selects one type. Omitting `weight`
assigns unit capacity to every selected edge. A named capacity property is strict: every
selected edge must have a present, non-null, numeric, finite, nonnegative value. Zero is
valid. `k` must be `1`; `heuristic`, `walk_length`, and `seed` are not accepted.
`directed=True` uses stored edge orientation. With `directed=False`, each stored edge supplies
capacity in both directions while retaining one canonical edge UUID.

Parallel edges contribute independently. Self-loops cannot carry source-to-sink flow and
receive zero in the edge view. Unreachable targets produce scalar flow `0.0`; the edge view
still contains every selected edge with zero flow where unused. Stable node, adjacency, and
edge UUID order makes both the maximum value and the selected feasible assignment
deterministic. Edge rows are in canonical edge UUID order. Source outflow, sink inflow, and
the scalar result agree, and every nonterminal node satisfies flow conservation.

Shared limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Selector and option validation, strict-capacity loading, checked finite arithmetic, limits,
cancellation, execution, and Arrow shaping are atomic: failures return structured Rust
errors and no partial result.

Typed Rust in `graphforge-exec` owns projection, normalization, the shared flow kernel, canonical
assignment, controls, and Arrow shaping. Python and Node only adapt arguments and native
Arrow/Arrow IPC; no external graph runtime, fallback, service, packaging dependency, or
recovery path participates. Maximum flow consumes only resolved graph topology, properties,
selectors, and typed options. It never reads knowledge tables or implicitly uses confidence,
provenance, assertions, evidence, belief status, valid time, or hypothesis grouping, and none
of those values appear as options or conditional result columns. The knowledge layer may record a neutral
invocation descriptor and durable `algorithm_run_uuid`; the epistemic layer may resolve an immutable UUID
projection before dispatch and attach interpretations afterward without changing the algorithm
result.

#### Canonical `min_cut` and `min_cut_edges` contract

The two catalog values are stable public views of one normalized, validated Rust cut solution.
`min_cut` returns exactly one non-null row with
`source_uuid: FixedSizeBinary(16)`, `sink_uuid: FixedSizeBinary(16)`, and
`cut_value: Float64`. `min_cut_edges` returns one non-null row for each edge selected by that
same cut, with `edge_uuid: FixedSizeBinary(16)`, `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, and `capacity: Float64`. Both schemas have version `1`
and `graphforge.verb=paths`; their `graphforge.algorithm` metadata names the selected view.
UUIDs are stored public identity, never execution surrogates. Edge rows use canonical edge
UUID order, and separate catalog values keep scalar and per-edge grain stable.

Both values require exactly one resolved source and target and reject equal endpoints.
`via=None` selects every relationship type; a valid `via` selects one type. Omitting `weight`
assigns unit capacity to every selected edge. A named capacity property is strict: every
selected edge must have a present, non-null, numeric, finite, nonnegative value. Zero is
valid. `k` must be `1`; `heuristic`, `walk_length`, and `seed` are not accepted.
`directed=True` uses stored edge orientation. With `directed=False`, each stored edge
contributes equal capacity in both directions for cut computation while retaining its one
stored edge UUID and orientation in the edge result.

Parallel edges contribute independently. Self-loops never cross the cut and therefore do not
appear in the edge view. An unreachable target still produces the one scalar row with
`cut_value=0.0` and produces zero edge rows with the unchanged `min_cut_edges` schema.
Among equal-valued cuts, the implementation selects the lexicographically smallest sorted
source-side UUID set, then orders the resulting cut edges by canonical edge UUID. The scalar
value equals the checked sum of the selected edge capacities.

Shared limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Selector and option validation, strict-capacity loading, finite checked arithmetic,
canonical-cut construction, limits, cancellation, execution, and Arrow shaping are atomic:
failures return structured Rust errors and no partial result.

Typed Rust in `graphforge-exec` owns projection, normalization, the shared cut solution, deterministic
tie-breaking, controls, and Arrow shaping. Python and Node only adapt arguments and native
Arrow/Arrow IPC; no external graph runtime, fallback, service, packaging dependency, or
recovery path participates. Minimum cut consumes only resolved graph topology, properties,
selectors, and typed options. It never reads knowledge/epistemic knowledge tables or implicitly uses
confidence, provenance, assertions, evidence, belief status, valid time, or hypothesis
grouping, and none of those values appear as options or conditional result columns. Neutral
the shipped knowledge run API can persist the descriptor and a durable
`algorithm_run_uuid`; the epistemic layer may resolve an immutable UUID projection before dispatch and attach
interpretations afterward without changing the algorithm result.

#### Canonical `gomory_hu_tree` contract

> **Status:** shipped. The classic parent-update Rust kernel, graph-wide source-free invocation,
> `graphforge-api` dispatch, knowledge-isolation coverage, and fresh-wheel/fresh-addon Python and Node
> acceptance are merged. Evidence: kernel
> [PR 2169](https://github.com/CurateLabs/graphforge-legecy/pull/2169), dispatch and `graphforge-api`
> [PR 2364](https://github.com/CurateLabs/graphforge-legecy/pull/2364), isolation
> [PR 2365](https://github.com/CurateLabs/graphforge-legecy/pull/2365), and native bindings
> [PR 2366](https://github.com/CurateLabs/graphforge-legecy/pull/2366).

`gomory_hu_tree` is graph-wide over the complete selected projection. It has no source or
target node. The Rust invocation boundary permits an absent source for graph-wide path catalog
values while continuing to require one for source-based algorithms.
Gomory-Hu requires both source and target to be absent and rejects either when supplied
instead of ignoring it. Rust, Python, and Node all expose this source-free invocation.

The operation requires `directed=False`; `directed=True`, including the current public
default, is a structured validation error. `via=None` includes every relationship type and a
valid `via` selects one type. Omitting `weight` assigns unit capacity to every selected stored
edge. A named capacity property is strict: every selected edge must have a present, non-null,
numeric, finite, nonnegative value; zero is valid. `k` must be `1`; `heuristic`,
`walk_length`, and `seed` are not accepted.

Projection is an undirected capacity multigraph. Each stored relationship UUID is one
capacity record. Reciprocal records with distinct UUIDs are independent parallel records and
their capacities contribute separately; mirrored adjacency entries for one stored UUID
collapse to that one record. Self-loops contribute nothing to connectivity, cuts, or output.
Connected components are processed by ascending minimum raw node UUID. Empty and singleton
projections return the canonical zero-row CUT-schema batch. For component sizes `n_i`, the
forest contains exactly
`sum(n_i - 1) = |V| - component_count` rows; isolated components contribute zero. Rows are
synthetic cut-tree edges and therefore contain no stored `edge_uuid`.

The pinned construction is `classic-gomory-hu-parent-update-v1`. Within each component, sort
nodes by raw UUID as `v[0]..v[n-1]`, root at `v[0]`, initialize `parent[i]=0` for `i>0`, and
process `s=1..n-1` in that order. Let `t=parent[s]`. Compute the shared canonical minimum cut
between `v[s]` and `v[t]`, with canonical source side `S` containing `v[s]`, and let its value
be `c`. For every `i>s` with `parent[i]=t` and `v[i]` in `S`, set `parent[i]=s`. If `t != 0`
and `v[parent[t]]` is in `S`, apply the standard swap:
`parent[s]=parent[t]`, `parent[t]=s`, `cut[s]=cut[t]`, and `cut[t]=c`; otherwise set
`cut[s]=c`. Emit `(v[i], v[parent[i]], cut[i])` for every `i>0`, normalize each endpoint pair
so `source_uuid < target_uuid`, then sort all forest rows lexicographically by
`(source_uuid, target_uuid, cut_value)`.

The exact non-null schema is `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, and `cut_value: Float64`, with
`graphforge.algorithm=gomory_hu_tree`, `graphforge.verb=paths`, and
`graphforge.algorithm_schema_version=1`. Public identity is UUID-only; no execution
surrogate or stored edge identity appears.

One shared `AlgorithmControl` spans projection validation, component discovery, and every
repeated minimum-cut computation. Cancellation, cooperative work, memory, and output
accounting are aggregate across the complete invocation and do not reset for a component or
cut. Capacity accumulation is checked and finite. Invalid selectors or options, malformed
identity, capacity failure, overflow, resource exhaustion, cancellation, execution, or Arrow
shaping abort atomically without a partial forest.

Rust owns graph-native projection, property resolution, repeated cuts, parent updates,
determinism, controls, and Arrow shaping. The computation never reads knowledge/epistemic knowledge
tables or implicitly uses confidence, assertions, evidence, belief status, provenance, valid
time, or hypothesis membership. Those values are neither options nor conditional result
columns. The shipped knowledge run API can persist the
normalized graph-wide descriptor, projection and result fingerprints, and durable
`algorithm_run_uuid`; the epistemic layer may resolve an immutable UUID projection before dispatch and attach
assertions, evidence, or reasoning afterward. Neither layer changes this algorithm computation or
schema. External graph libraries are development parity oracles only, never runtime
backends, fallbacks, services, packaging dependencies, or recovery paths.

#### Canonical `min_cost_max_flow` and `min_cost_max_flow_edges` contract

> **Status:** shipped through Rust, `graphforge-api`, and the thin Python and Node bindings. Both native
> bindings accept explicit capacity and cost property names and return the Rust-produced Arrow
> result without an external runtime backend or fallback.

The two catalog values are stable views of one normalized, validated Rust solution.
`min_cost_max_flow` returns exactly one non-null row with
`source_uuid: FixedSizeBinary(16)`, `sink_uuid: FixedSizeBinary(16)`, `flow: Float64`, and
`cost: Float64`. `min_cost_max_flow_edges` returns exactly one non-null row for every selected
canonical edge, including zero-flow edges, with `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, `flow: Float64`,
`unit_cost: Float64`, and `flow_cost: Float64`. `flow_cost` records the edge's actual
directional contribution to the optimized objective, including residual cancellations; it is
not defined as signed net `flow * unit_cost` in undirected mode. Its canonical checked sum
equals the scalar cost. Both schemas have version `1` and `graphforge.verb=paths`;
`graphforge.algorithm` names the selected view. UUID fields contain stored public identity,
never execution surrogates. Edge rows use raw edge UUID order.

Both values require exactly one resolved source and target and reject equal endpoints.
`via=None` selects every relationship type; a valid `via` selects one type. The typed
`capacity_property` and `cost_property` selectors name graph-native edge properties. Omitting
`capacity_property` assigns unit capacity. A named capacity is strict: every selected edge
must have a present, non-null, numeric, finite, nonnegative value; zero is valid.
`cost_property` is required and equally strict about presence, numeric type, and finiteness,
but signed costs are valid. Cost has no implicit, confidence-derived, or other
knowledge-derived default. The legacy `weight` selector is not an alias for either property.
`k` must be `1`; `heuristic`, `walk_length`, and `seed` are not accepted.

`directed=True` is the default and uses stored orientation. With `directed=False`, each stored
edge contributes equal-capacity, equal-cost arcs in both directions under one edge UUID. The
edge view reports signed net flow in stored orientation: positive follows
`source_uuid → target_uuid`, negative flows oppositely. `flow_cost` separately preserves the
actual directional objective contribution accumulated for that edge, including residual
cancellations. Parallel edges remain independent. Self-loops retain zero `flow` and
`flow_cost`. An unreachable target produces the scalar row with `flow=0.0` and `cost=0.0`;
the edge view still returns every selected edge with zero flow.

Optimization is lexicographic: maximize source-to-sink flow first, minimize total cost second,
then choose the lexicographically smallest raw per-edge signed-flow vector in ascending edge
UUID order. Canonical UUID node, edge, adjacency, residual-arc, and reduction order makes both
views reproducible. A source-reachable negative-cost residual cycle makes the optimum
undefined and is a structured atomic error. Source outflow, sink inflow, and scalar `flow`
agree; nonterminal flow is conserved; scalar `cost` equals the canonical checked sum of
per-edge `flow_cost`.

Shared limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Selector and typed-option validation, strict property loading, residual-cycle detection,
checked finite flow/cost accumulation, limits, cancellation, execution, and Arrow shaping are
atomic: failures return structured Rust errors and no partial result.

Typed Rust in `graphforge-exec` owns projection, normalization, the shared solution, deterministic tie
refinement, controls, and Arrow shaping. `graphforge-api` owns typed property resolution and dispatch;
Python and Node only coerce arguments and convert the Rust-produced Arrow result. They provide
no external graph runtime or fallback. The operation consumes only resolved graph-native
topology, properties, selectors, and typed options. It never reads knowledge/epistemic knowledge tables or
implicitly uses confidence, provenance, assertions, evidence, belief status, valid time, or
hypothesis grouping, and those values never become options or conditional result columns.
The shipped knowledge run API can persist the normalized
property selectors, projection fingerprint, result fingerprint, and durable
`algorithm_run_uuid`; the epistemic layer may resolve an immutable UUID projection before dispatch and attach
assertions or interpretations afterward without changing the algorithm result.

### Steiner Trees (`by=`)

| Value | Algorithm | Source reference |
| ------------------------------- | ---------------------------------------------- | ------------------------ |
| `min_steiner_tree` | Exact minimum-weight undirected Steiner tree | Rust exact subset search |
| `prize_collecting_steiner_tree` | Exact prize-collecting undirected Steiner tree | Rust exact subset search |

#### Canonical Steiner v1 contracts

Both catalog values are source-free: callers pass `source=None`, `target=None`,
`directed=False`, and explicit mandatory terminal UUIDs in `terminal_uuids`. Normalization
sorts and deduplicates terminals by UUID and verifies membership in the selected graph before
dispatch. `min_steiner_tree` requires at least two distinct terminals;
`prize_collecting_steiner_tree` requires at least one. Positional endpoints, `directed=True`,
non-default `k`, and options owned by other path algorithms are rejected. A minimum-tree call
also rejects `prize_property`; a prize-collecting call requires an explicit, nonblank
`prize_property`.

For a selected undirected edge set \(F\), `min_steiner_tree` returns exactly one tree that
contains every mandatory terminal and minimizes

\[
C(F) = \sum_{e \in F} c(e).
\]

For `prize_collecting_steiner_tree`, let \(V(F)\) be the nodes incident to selected edges plus
the mandatory terminals, and let \(P\) be the selected graph's nonterminal nodes. It returns
exactly one connected tree containing every mandatory terminal and minimizes

\[
C(F) + \sum_{v \in P \setminus V(F)} p(v).
\]

This is the shipped exact Steiner v1 contract: the Rust kernels enumerate the bounded edge
subset space rather than substituting an approximation. An omitted `weight` gives every edge
unit cost `1.0`; a named `weight` property is a strict graph-native edge-cost selector. The
prize-collecting objective additionally loads the explicit `prize_property` for every selected
node. Selected costs and prizes must be present, numeric, finite, and nonnegative; missing,
NULL, nonnumeric, negative, NaN, or infinite values are errors. Confidence, evidence strength,
and belief status are never implicit costs or prizes.

Equal objective values prefer fewer selected edges and then the lexicographically smallest
sequence of stored edge UUIDs. Rows are normalized to ascending endpoint UUIDs and sorted by
ascending `edge_uuid`. Parallel stored edges remain distinct candidates and therefore
participate in the UUID tie-break; self-loops cannot participate. Disconnected mandatory
terminals are a structured undefined-result error. Prize-collecting output may contain zero
edges only when its normalized mandatory set contains one node; minimum-tree output always
connects at least two terminals.

Both algorithms use the exact non-null schema
`edge_uuid: FixedSizeBinary(16), source_uuid: FixedSizeBinary(16), target_uuid:
FixedSizeBinary(16), weight: Float64`. It contains one selected tree, no summary or `tree_id`
row, and exactly three metadata entries: `graphforge.algorithm` is the selected catalog value,
`graphforge.algorithm_schema_version=1`, and `graphforge.verb=paths`. Public identity is UUID
only; execution surrogates and knowledge/provenance fields never escape.

The shared controls cap selected nodes at 10,000,000, direction-expanded adjacency entries at
100,000,000, output rows at 10,000,000, cooperative checkpoints at 10,000, and exact-solver
states at 10,000,000. The minimum solver also rejects a search deeper than 4,096 non-loop edge
candidates. Projection, terminal normalization, strict property loading, checked finite
objective accumulation, exact search, cancellation, limits, and Arrow shaping are atomic:
invalid input, overflow, unreachable terminals, exhaustion, cancellation, or execution failure
returns a structured Rust error and no partial batch.

Rust owns normalization, projection, exact optimization, controls, deterministic selection, and
Arrow shaping. `graphforge-api` resolves graph-native properties and dispatches the typed invocation;
Python and Node only construct options and decode the freshly built native addon's Arrow IPC.
There is no external graph runtime or fallback.

The analyst-verb path consumes only graph-native topology and properties, normalized selectors and typed options,
and explicit resolved UUID mappings. It never reads knowledge/epistemic tables or implicitly consumes
confidence, assertions, evidence, belief status, provenance, valid time, or hypothesis groups;
none of those values becomes a cost, prize, option, or conditional schema column. The shipped
knowledge run API persists the normalized
descriptor, projection/result fingerprints, and a durable `algorithm_run_uuid` without changing
dispatch or output. The epistemic layer resolves a point-in-time or competing-hypothesis state into an
immutable UUID projection before dispatch and attach assertions or reasoning afterward. An
equivalent pre-resolved projection produces the same algorithm result; these orchestration APIs do
not alter the Steiner contract.

### Traversal / Sampling (`by=`)

| Value | Algorithm | Source reference |
| ------------- | ---------------------------- | ---------------- |
| `dfs` | Depth-first search traversal | GDS / igraph |
| `random_walk` | Random walk sequence | GDS |

#### Canonical `dfs` contract

`dfs` performs deterministic depth-first preorder over the selected component containing the
required source. It does not accept a target. The non-null schema is exactly
`node_uuid: FixedSizeBinary(16)`, `depth: UInt64`, and `order: UInt64`, with metadata identifying
algorithm `dfs`, schema version `1`, and verb `paths`. `order` starts at zero and records
discovery order; `depth` records traversal-tree depth from the source.

Neighbors use stable topology/UUID order. The traversal pushes them in reverse stable order so
the next discovery is the smallest stable neighbor, making Rust, Python, and Node results
identical across repeated calls. `directed=True` follows outgoing selected edges;
`directed=False` traverses either edge direction, and `via` restricts traversal to one
relationship type. Parallel edges do not duplicate discovery, self-loops do not revisit a node,
and nodes outside the source component are omitted. An isolate or a source with no selected edge
returns the one source row at depth/order zero. Because a valid source is mandatory, an empty
graph returns selector validation rather than a zero-row traversal.

Canonical DFS requires `k=1` and accepts neither `weight` nor `heuristic`. A target, unsupported
option, blank relationship selector, malformed/missing/ambiguous selector, or cross-graph handle
returns a structured validation error. Adjacency export enforces the shared 10,000,000-node and
100,000,000-entry limits; output shaping is capped at 10,000,000 rows, and traversal checkpoints
use the shared 10,000-work-unit budget. Cooperative cancellation and every failure abort without
partial output.

Rust owns selector validation, catalog projection, adjacency normalization, traversal, ordering,
limits, cancellation, and Arrow shaping. The Python and Node bindings only adapt public
arguments and convert the native Arrow result. DFS has no runtime external graph-library,
service, fallback, packaging dependency, or recovery path, and knowledge-layer presence cannot
change its topology-only result.

#### Canonical `random_walk` contract

`random_walk` samples graph-native adjacency from a required source selector. `k` is the number
of walks for each resolved start node and defaults to `1`; zero is invalid. `walk_length` is the
maximum number of edge transitions and defaults to `10`. A zero-length walk contains only its
start UUID. `seed` defaults to the fixed unsigned value `0`. A target selector is invalid.
`directed=True` samples outgoing selected edges, `directed=False` exposes both endpoint
directions, and `via` optionally selects one relationship type.

Without `weight`, each direction-expanded adjacency entry has equal probability. With a named
weight property, every eligible entry must have a present, numeric, finite, nonnegative value;
NULL, missing, nonnumeric, NaN, infinite, and negative values fail the invocation. Parallel edges
remain separate choices, self-loops remain valid choices, and a zero-total weighted choice set
terminates the walk. Dead ends also terminate immediately: results are never padded or given an
implicit repeated terminal node.

Reproducibility is pinned to RNG contract `splitmix64-v1`. Eligible choices sort by neighbor UUID
and then edge UUID before sampling. Each walk gets a deterministic stream derived from the
normalized seed, resolved-source ordinal, and walk ordinal. Changing SplitMix64, this derivation,
the integer-to-draw conversion, or canonical choice ordering requires a new named contract
version; it must not silently alter `splitmix64-v1`. For the same graph projection, normalized
options, contract version, and seed, the Arrow output is byte-identical across repeated calls.

Rows are ordered by resolved-source order and then zero-based walk ordinal. The exact non-null
schema is `start_uuid: FixedSizeBinary(16), walk: List<FixedSizeBinary(16) not null>`, with
`graphforge.algorithm=random_walk`, `graphforge.algorithm_schema_version=1`, and
`graphforge.verb=paths` metadata. Each list starts with `start_uuid` and contains at most
`walk_length + 1` UUIDs. No internal ID, confidence, provenance, assertion, evidence, belief
status, or valid-time column is conditional or appended.

Checked multiplication preflights `k × resolved starts` against the shared 10,000,000-row limit
and `k × resolved starts × walk_length` against the shared 10,000-checkpoint work limit.
Projection also retains the shared 10,000,000-node and 100,000,000-direction-expanded-entry
limits. Cooperative cancellation is checked before and during sampling. Validation, projection,
weight, overflow, limit, cancellation, execution, and Arrow-shaping failures are structured Rust
errors and return no partial batch.

Rust owns option normalization, projection, weight validation, RNG execution, limits,
cancellation, ordering, and Arrow shaping. Python and Node only pass typed options to the same
native handler and decode Arrow. The sampler reads only selected topology, explicit graph
properties, selectors, and normalized options. It never reads knowledge/epistemic knowledge tables or
implicitly consumes confidence, assertions, evidence, belief state, provenance, or valid time.
The knowledge layer can persist the neutral invocation descriptor and an `algorithm_run_uuid`; the epistemic layer may resolve an
immutable projection before dispatch and attach interpretations after dispatch. Neither layer
changes this algorithm schema or adds knowledge selectors to `PathsOptions`.

### Reachability (`by=`)

| Value | Algorithm | Source reference |
| -------------------- | --------------------------------- | ---------------- |
| `transitive_closure` | All (src, dst) reachability pairs | OCG |

#### Canonical `transitive_closure` contract

`transitive_closure` computes the global positive-length reachability relation
over the selected graph. The required `source` is fully validated for existence
and graph ownership but does not scope the computation; calls with different
valid sources return the same batch. `target` is invalid. A pair `(u, v)` is
present exactly when at least one path of one or more selected edges leads from
`u` to `v`. Consequently, a self-pair appears only when a positive-length cycle
returns to that node, including a selected self-loop. Isolated nodes and
unreachable ordered pairs produce no row, and an empty or edgeless graph
returns the typed zero-row batch.

`directed=True` follows stored edge orientation. `directed=False` uses the
shared symmetric projection, so every node incident to an edge can reach itself
by traversing that edge out and back. `via=None` includes every relationship
type, while a valid `via` selects one type. Parallel edges do not duplicate a
reachability pair. Relationship properties are not consulted: `weight` and
`heuristic` are invalid, `k` must remain `1`, and blank relationship selectors
are rejected.

The exact non-null schema is
`source_uuid: FixedSizeBinary(16), target_uuid: FixedSizeBinary(16)`, with
`graphforge.algorithm=transitive_closure` and `graphforge.verb=paths` metadata.
Rows are unique and lexicographically ordered by raw public source UUID and
then raw public target UUID. No internal adjacency or storage surrogate
escapes.

Rust performs one deterministic traversal per selected source: worst-case time
is `O(V(V + E))`, traversal state is `O(V)` per source, and retained output is
`O(R)` for `R` reachable ordered pairs (up to `V²`). The shared limits allow at
most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Cancellation is checked before and during traversal. Limit, cancellation,
projection, validation, execution, and Arrow-shaping failures return
structured Rust errors without partial output.

Catalog registration, graph projection, execution, limits, cancellation,
deduplication, ordering, and Arrow shaping are Rust-owned in `graphforge-exec`.
Python and Node only adapt selectors/options and the native Arrow result; they
contain no traversal, external backend, fallback, service, packaging
dependency, or recovery path. The result consumes only graph topology and is
unchanged by knowledge-layer presence, preserving the topology-only knowledge isolation boundary.

---

## `forge.analyze()` — Graph-Level Metrics

Returns an Arrow Table whose schema reflects the analysis type.
No `label` parameter is required for graph-global metrics; it is optional for node-level
output (coloring, matching, cycle listing).

### Spanning Trees (`by=`)

| Value | Algorithm | Source reference |
| ------------------------- | ----------------------------------------------------------- | ---------------- |
| `minimum_spanning_tree` | Kruskal MST → edge list with weights | OCG |
| `maximum_spanning_tree` | Kruskal maximum spanning tree | OCG |
| `minimum_k_spanning_tree` | Exact enumeration of the k cheapest distinct spanning trees | GraphForge |

#### Canonical `minimum_spanning_tree` contract

`minimum_spanning_tree` computes a deterministic minimum spanning **forest** with Kruskal's
algorithm. It requires `directed=False`; `directed=True`, including the public default, is a
structured validation error because a directed arborescence is a different algorithm. Each
nontrivial disconnected component contributes one tree. Empty, edgeless, and isolated-node
selections return a typed zero-row forest.

An optional `label` selects the node-induced graph. `via=None` includes every relationship
type, while a named `via` selects one valid type. Omitting `weight` assigns every selected
edge cost `1.0`. A named weight is strict across all selected edges: every value must be
present, non-null, numeric, and finite. Negative, zero, and positive finite values are valid.
Missing properties, nulls, non-numeric values, NaN, infinity, blank selectors, malformed
adjacency, storage failures, and shaping failures return structured errors without partial
output.

The undirected projection mirrors adjacency, then Rust collapses the two entries for one
stored edge by `edge_uuid`. Distinct parallel-edge UUIDs remain independent candidates.
Self-loops are ignored. Every output edge canonicalizes its endpoints so
`source_uuid < target_uuid`. Candidates and returned rows sort by `weight`, canonical
`source_uuid`, canonical `target_uuid`, then `edge_uuid`, all ascending. This defines stable
whole-edge and parallel-edge ties across repeated calls and every language binding.

The stable EDGE_LIST schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and
`weight: Float64`. UUID fields are non-null. The shared logical schema marks `weight`
nullable for edge-list algorithms, but minimum-spanning-forest rows always contain a non-null
unit or validated named weight. Internal execution surrogates never escape.

Kruskal execution is `O(V + E log E)` time and `O(V + E)` auxiliary space, where `E` is the
selected direction-expanded adjacency before mirrored UUID entries are collapsed. Shared hard
limits allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative iteration checkpoints. Projection,
candidate ordering, union-find selection, output shaping, limit failures, and cancellation
abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, validation, Kruskal/DSU execution, canonical ordering,
limits, cancellation, and Arrow shaping. Python and Node only adapt arguments and Arrow IPC.
Exploratory operation without a knowledge layer produces the same result, satisfying the
graph/knowledge boundary. External algorithm libraries and documentation are development parity
references only; no external runtime backend, fallback, packaging dependency, service, or
recovery path participates.

#### Canonical `maximum_spanning_tree` contract

`maximum_spanning_tree` computes a deterministic maximum spanning **forest**
with Kruskal's algorithm. It requires `directed=False`; `directed=True`,
including the public default, is a structured validation error because a
directed arborescence is a different algorithm. Each nontrivial disconnected
component contributes one tree. Empty, edgeless, isolated-node, and
label-empty selections return a typed zero-row forest.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a named `via` selects one
valid type. Omitting `weight` assigns every selected edge `1.0`. A named
weight is strict across all selected edges: every value must be present,
non-null, numeric, and finite. Negative, zero, and positive finite values are
valid. Missing properties, nulls, non-numeric values, NaN, infinity, blank
selectors, malformed adjacency, storage failures, and shaping failures return
structured errors without partial output.

The undirected projection mirrors adjacency, then Rust collapses only the two
entries sharing one stored `edge_uuid`. Distinct parallel and reciprocal
stored edges remain independent candidates, and self-loops are ignored.
Endpoints are canonicalized so `source_uuid < target_uuid`. Candidates and
returned rows sort by descending `weight`, then ascending canonical
`source_uuid`, canonical `target_uuid`, and `edge_uuid`. Kruskal considers
that complete whole-edge order, so it deterministically selects among
equal-weight parallel, reciprocal, and other cycle-forming candidates.

The exact EDGE_LIST schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and
`weight: Float64`. UUID fields are non-null and expose stored public identity,
never execution surrogates. The shared logical schema marks `weight` nullable,
but maximum-spanning-forest rows always contain a non-null unit or validated
named weight. Metadata includes
`graphforge.algorithm=maximum_spanning_tree`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`.

Kruskal execution is `O(V + E log E)` time and `O(V + E)` auxiliary space,
where `E` is the selected direction-expanded adjacency before mirrored UUID
entries are collapsed. Shared hard limits allow at most 10,000,000 selected
nodes, 100,000,000 direction-expanded adjacency entries, 10,000,000 output
rows, and 10,000 cooperative iteration checkpoints. Projection, validation,
candidate ordering, union-find selection, output shaping, limit failures, and
cancellation abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, validation, maximizing Kruskal/DSU
execution, canonical ordering, limits, cancellation, and Arrow shaping.
Python and Node only adapt arguments and native Arrow/Arrow IPC. Exploratory
operation without a knowledge layer produces the same topology-and-property
result. No external algorithm runtime,
fallback, packaging dependency, service, or recovery path participates.

#### Canonical `minimum_k_spanning_tree` contract

`minimum_k_spanning_tree` enumerates the requested number `k` of distinct
lowest-cost spanning trees of one connected selected graph. Here `k` counts
complete trees: it does not mean a tree spanning only `k` nodes or a degree,
size, or neighborhood constraint used by similarly named external APIs. The
default is `k=1`; `k=0` is a structured validation error. When fewer than `k`
distinct trees exist, GraphForge returns the complete finite set without an
error.

The operation requires `directed=False`; `directed=True`, including the public
default, is a structured validation error. An optional `label` selects the
node-induced graph. `via=None` includes every relationship type between
selected nodes, while a valid named `via` selects one type. Every nonempty
selection must be connected. A disconnected selection fails atomically rather
than returning a spanning forest.

Omitting `weight` assigns every selected edge unit cost `1.0`. A named weight
property is strict across every selected edge: each value must be present,
non-null, numeric, finite, and nonnegative. Negative values, NULL, non-numeric
values, NaN, infinity, blank selectors, malformed adjacency, storage failures,
and shaping failures return structured errors without partial output. Each
tree total is accumulated with checked finite arithmetic in canonical edge
order.

Mirrored adjacency entries sharing one stored `edge_uuid` collapse. Reusing
one edge UUID for inconsistent unordered endpoints or weights is an error.
Distinct parallel and reciprocal stored edges remain distinct candidates and
therefore may identify different trees. Self-loops are excluded because they
cannot belong to a spanning tree. Endpoints are canonicalized within each
returned edge. Trees sort by nondecreasing total cost, then by the
lexicographic sequence of their canonical edge UUIDs; edges within each tree
sort by edge UUID. This defines stable equal-cost, parallel-edge, and result
ordering across repeated calls and every binding.

The exact non-null schema is `tree_id: UInt64`,
`edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, and `weight: Float64`, in that order.
`tree_id` is the local zero-based ordinal in this ordered result, not durable
provenance identity. Metadata includes
`graphforge.algorithm=minimum_k_spanning_tree`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Internal execution surrogates never
escape.

An empty selection returns a typed zero-row edge list. A singleton has one
logical spanning tree containing zero edges and is represented by that same
typed zero-row edge list because this schema has one row per selected edge and
all fields are non-null. Callers distinguish empty and singleton inputs from
the selected input cardinality; GraphForge does not emit a sentinel row,
nullable UUID, or conditional column.

Exact enumeration is exponential in the worst case. Shared hard limits allow
at most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output edge rows, and 10,000 cooperative enumeration
checkpoints. Projection, normalization, enumeration, checked arithmetic,
output shaping, limit failures, and cancellation finish atomically without
partial trees or rows.

Typed Rust owns selection, validation, exact enumeration, deterministic
ordering, controls, and Arrow shaping. Python and Node only adapt typed
arguments and native Arrow/Arrow IPC. The computation consumes graph-native
topology, properties, selectors, and options only; it never reads confidence,
provenance, assertions, evidence, belief status, hypotheses, or valid time,
and none become options or result columns. The knowledge layer can persist a neutral invocation
descriptor and durable `algorithm_run_uuid`; the epistemic layer may resolve a graph projection
before dispatch and attach claims after dispatch. Neither changes this analyst-verb
computation, local `tree_id`, or schema. External libraries and similarly named
APIs are development references only, not runtime backends or definitions of
GraphForge's `k`.

### DAG Analysis (`by=`)

| Value | Algorithm | Source reference |
| --------------------------- | -------------------------------------------- | ---------------- |
| `topological_sort` | Kahn's topological ordering | OCG / GDS |
| `is_dag` | Boolean: is the graph a DAG? | OCG |
| `find_cycles` | Johnson's algorithm — list all simple cycles | OCG |
| `dag_longest_path` | Longest path by hop count | OCG |
| `dag_longest_path_weighted` | Longest path by edge weight | OCG / GDS |

#### Canonical `find_cycles` contract

`find_cycles` enumerates every distinct simple node cycle in the selected graph.
`label=None` selects every node; a valid label selects the node-induced graph.
`via=None` includes every relationship type between selected nodes, while a
valid named `via` selects one type. Both `directed=True` and `directed=False`
are supported, and any `weight` is a structured validation error because cycle
identity is unweighted.

Each returned cycle is a sequence of public node UUIDs and does not repeat its
starting node at the end. A directed cycle is the lexicographically smallest
rotation of its UUID sequence. An undirected cycle is the lexicographically
smallest rotation across both orientations. Rows sort lexicographically by the
complete canonical UUID sequence, producing the same result for every input
order, repeated call, and language binding.

A self-loop is one length-one cycle. Two reciprocal directed arcs form one
length-two cycle. A single undirected edge is not a cycle. For undirected
projection, Rust collapses mirrored adjacency entries only when they share one
stored `edge_uuid`; inconsistent reuse of an edge UUID is an atomic execution
error. Because the public result contains nodes rather than edges, parallel or
reciprocal edge representations that describe the same canonical node cycle
deduplicate to one row. Empty, acyclic, isolated-node, label-empty, and
disconnected selections are valid; only components containing cycles
contribute rows.

The exact non-null CYCLE schema is
`cycle: List<FixedSizeBinary(16) not null>`. The list itself and every UUID
item are non-null public stored identities; execution surrogates and edge
identities never escape. Metadata is `graphforge.algorithm=find_cycles`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.

Simple-cycle enumeration is inherently output-sensitive and has exponential
worst-case time and output size. Rust uses an explicit iterative DFS stack, so
graph depth cannot overflow the call stack. Its working graph and traversal
state are linear in `V + E`; retaining, canonicalizing, deduplicating, and
sorting results additionally requires space proportional to the total UUID
items in the distinct output cycles. Shared hard limits allow at most
10,000,000 selected nodes, 100,000,000 direction-expanded adjacency entries,
10,000,000 distinct output rows, and 10,000 cooperative iteration checkpoints.
Cancellation is checked throughout normalization and enumeration, output
limits count only newly inserted canonical cycles, and validation, storage,
limit, cancellation, and shaping failures abort atomically without partial
output.

Typed Rust in `graphforge-exec` owns projection, identity validation, iterative
enumeration, canonicalization, deduplication, ordering, limits, cancellation,
and Arrow shaping. Python and Node only adapt arguments and native Arrow/Arrow
IPC. Results depend only on selected topology and remain unchanged by
Knowledge-layer presence. No external
algorithm runtime, fallback, packaging dependency, service, or recovery path
participates.

#### Canonical `topological_sort` contract

`topological_sort` computes one complete Kahn ordering of the selected directed
acyclic graph. `label=None` selects every node; a valid label selects the
node-induced graph. `via=None` includes every relationship type between
selected nodes, while a valid `via` selects one type. The operation requires
`directed=True`; `directed=False` and any `weight` are structured validation
errors because topological order is directed and unweighted.

Each selected adjacency entry contributes to indegree, so parallel edges are
counted and decremented independently without duplicating node rows or changing
the precedence relation. A self-loop, reciprocal pair, or longer directed cycle
fails the complete invocation with `selected graph contains a cycle`; no
partial ordering is returned. Empty and label-empty selections return the
typed zero-row batch. Isolated nodes and disconnected DAG components are all
included.

Whenever multiple nodes have indegree zero, Rust selects the lexicographically
smallest raw public UUID. This ready-node rule applies globally across
disconnected components and after each edge removal, producing a deterministic
ordering across repeated calls and all bindings. The exact non-null schema is
`node_uuid: FixedSizeBinary(16), order: UInt64`, with
`graphforge.algorithm=topological_sort` and `graphforge.verb=analyze` metadata.
`order` is contiguous and zero-based from `0` through `V - 1`; rows appear in
that order. No internal node or adjacency surrogate escapes.

The ordered ready set makes execution `O((V + E) log V)` time with `O(V)`
additional state for indegrees, positions, and ready nodes, plus the `O(V)`
result. Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, validation, cycle detection,
overflow, limit, cancellation, execution, and Arrow-shaping failures return
structured Rust errors without partial output.

Catalog registration, projection, Kahn execution, deterministic ready
selection, limits, cancellation, and Arrow shaping are Rust-owned in
`graphforge-exec`. Python and Node only adapt arguments and the native Arrow result;
they contain no sorting implementation, external runtime backend, fallback,
service, packaging dependency, or recovery path. Results consume only graph
topology and are unchanged by knowledge-layer presence, preserving the
boundary tracked by
the topology-only knowledge isolation boundary.

#### Canonical `dag_longest_path` contract

`dag_longest_path` computes the global longest directed path by unweighted hop
count across the complete selected DAG. `label=None` selects every node; a
valid label selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid `via` selects one
type. It requires `directed=True`. `directed=False` and every `weight` are
structured validation errors; weighted DAG traversal belongs to the distinct
`dag_longest_path_weighted` catalog entry.

Empty and label-empty selections return exactly one row containing cost `0.0`
and an empty path. A singleton returns cost `0.0` and its UUID. For an
edgeless multi-node selection, the lexicographically smallest UUID is the
one-node path. Disconnected components compete in one global calculation.
The greatest hop count wins; equal-hop candidates choose the
lexicographically smallest complete raw UUID path. Node, edge, table, and
adjacency input order cannot affect the result.

Identical repeated adjacency entries with the same stored `edge_uuid` collapse.
Inconsistent reuse of one `edge_uuid` for different directed endpoints is an
error. Distinct parallel arcs collapse to one simple directed relation and
cannot increase hop count. A self-loop, reciprocal pair, or longer cycle
returns the structured non-DAG error with no partial path. Duplicate node
UUIDs, malformed identity, and endpoints outside the selected node set are
also structured errors.

The exact one-row non-null schema is
`cost: Float64, path: List<FixedSizeBinary(16) not null> not null`. Path items
are public node UUIDs, never internal surrogates. Metadata is
`graphforge.algorithm=dag_longest_path`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Hop increments and indegrees are
checked. The returned cost is a finite, exactly represented Float64 integer;
counts above `2^53`, conversion failure, or arithmetic overflow fail
structurally rather than rounding.

Let `V` be selected nodes, `S` stored adjacency entries, and `E` distinct
directed arcs after normalization. UUID ordering, stored-edge normalization,
and ordered Kahn traversal take
`O(V log V + S log S + (V + E) log V)` time. Exact complete-path propagation
currently gives `O(VE)` worst-case time and `O(V^2 + E)` worst-case auxiliary
space. Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, identity validation, normalization,
cycle detection, arithmetic, limit, cancellation, execution, and Arrow-shaping
failures abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, normalization, topology, longest-path
execution, deterministic tie-breaking, controls, and Arrow shaping. Python and
Node only adapt arguments and the native Arrow result. They contain no graph
algorithm, sorting implementation, external runtime backend, fallback,
service, packaging dependency, or recovery path. Knowledge-layer presence
cannot change this topology-only result, preserving the topology-only knowledge isolation boundary.

#### Canonical `is_dag` contract

`is_dag` returns exactly one non-null `is_dag: Boolean` row. With the default
`directed=True`, it reports whether the selected directed graph has no cycle. An optional `label`
selects the node-induced graph and `via` restricts relationship type. `directed=False` treats the
selection as undirected and returns `false`, because an undirected graph is not a directed acyclic
graph.

Empty graphs, isolated nodes, disconnected DAGs, and parallel edges in one direction return
`true`. Self-loops, reciprocal edges, and longer directed cycles return `false`. The Rust handler
uses deterministic, stable-order Kahn traversal with shared graph/output limits and cooperative
cancellation. Blank or malformed label/relationship selectors and unavailable algorithms produce
structured validation errors. Python and Node only coerce these inputs and convert the same
Rust-produced Arrow batch.

#### Canonical `dag_longest_path_weighted` contract

`dag_longest_path_weighted` computes the global maximum-weight directed path
across the complete selected DAG. `label=None` selects every node; a valid
label selects the node-induced graph. `via=None` includes every relationship
type between selected nodes, while a valid `via` selects one type. It requires
`directed=True` and a nonblank named `weight` property. `directed=False` or an
omitted weight is a structured validation error.

The named property is strict across every selected stored edge: values must be
present, non-null, numeric, and finite Float64. Negative, zero, and positive
finite weights are valid. Missing, null, non-numeric, NaN, infinite, blank,
malformed, or ambiguous weight selection fails before execution without a
partial result.

Empty and label-empty selections return exactly one row containing cost `0.0`
and an empty path. Every selected node is an eligible zero-cost singleton, so
a singleton returns its UUID and an all-negative graph returns a zero-cost
singleton. Disconnected components compete globally. The larger finite total
cost wins; exactly equal costs choose the lexicographically smallest complete
raw UUID path. Node, edge, table, adjacency, and component input order cannot
affect the result.

Repeated entries with the same stored `edge_uuid`, directed endpoints, and
weight collapse. Reusing one edge UUID with different endpoints or weight is
an error. Among distinct parallel arcs from one source to one target, only the
maximum weight participates; equal maximum weights deduplicate. A self-loop,
reciprocal pair, or longer directed cycle fails the complete invocation as a
non-DAG regardless of its weights. Duplicate node UUIDs, malformed identity,
and endpoints outside the selected node set are also structured errors.

The exact one-row non-null schema is
`cost: Float64, path: List<FixedSizeBinary(16) not null> not null`. Path items
are public node UUIDs, never execution surrogates. Metadata is
`graphforge.algorithm=dag_longest_path_weighted`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Every accumulated addition must remain finite; overflow to infinity, NaN, or
any other non-finite result is a structured error rather than a returned cost.

Let `V` be selected nodes, `S` stored edge entries, and `E` distinct directed
endpoint pairs after maximum-parallel normalization. UUID ordering,
stored-edge validation, and ordered topology require
`O(V log V + S log S + (V + E) log V)` time. Exact complete-path propagation
currently gives `O(VE)` worst-case time and `O(V^2 + E)` worst-case auxiliary
space. Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, weight and identity validation,
normalization, cycle detection, finite arithmetic, limit, cancellation,
execution, and Arrow-shaping failures abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, weight validation, normalization,
topology, maximum-path execution, deterministic tie-breaking, controls, and
Arrow shaping. Python and Node only adapt arguments and the native Arrow
result. They contain no graph algorithm, external runtime backend, fallback,
service, packaging dependency, or recovery path. Knowledge-layer presence
cannot change this topology-and-property result, preserving the boundary.

### Coloring (`by=`)

| Value | Algorithm | Source reference |
| ------------------ | ---------------------------------------- | ---------------- |
| `node_coloring` | Greedy vertex coloring | OCG |
| `edge_coloring` | Greedy edge coloring | OCG |
| `chromatic_number` | Exact chromatic number | OCG |
| `k1_coloring` | Deterministic distance-1 greedy coloring | GraphForge v1 |

#### Canonical `node_coloring` contract

`node_coloring` computes one deterministic greedy vertex coloring of the
selected undirected simple graph. It requires `directed=False`;
`directed=True`, including the public default, is a structured validation
error. The algorithm is unweighted and rejects every `weight`. `label=None`
selects every node, a valid label selects the node-induced graph, `via=None`
includes every relationship type, and a valid `via` selects one type.

Rust sorts selected nodes by ascending raw public UUID. In that order, each
node receives the smallest nonnegative color not already assigned to one of
its earlier neighbors. Colors therefore start at `0` and are deterministic,
but this greedy contract does not claim a globally minimum number of colors.
Rows use the same ascending UUID order.

Mirrored adjacency entries sharing one stored `edge_uuid` collapse.
Inconsistent reuse of one edge UUID is an error. Distinct parallel and
reciprocal stored edges deduplicate to one undirected neighbor pair, so they
cannot change the coloring. A self-loop makes the selected graph non-colorable
under this contract and returns a structured execution error rather than a
row. Duplicate node UUIDs, malformed identity, and endpoints outside the
selected node set are also structured errors.

Empty and label-empty selections return a typed zero-row result. Every selected
node in every disconnected component is returned; isolated nodes receive
color `0`. The exact non-null schema is
`node_uuid: FixedSizeBinary(16), color: UInt64`, with
`graphforge.algorithm=node_coloring`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. No execution surrogate
escapes. Color increments and conversion to UInt64 are checked, so range
failure cannot wrap.

With `V` selected nodes and `E` normalized undirected simple edges, UUID and
stored-edge ordering plus neighbor-set construction take
`O(V log V + E log(V + E))` time. Greedy assignment scans each adjacency and
maintains ordered used-color sets in `O(E log V + V log V)` time. Auxiliary
memory is `O(V + E)`, including colors and the normalized projection. Shared
limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, identity validation, normalization,
coloring, checked arithmetic, limit, cancellation, execution, and
Arrow-shaping failures abort atomically without partial rows.

Typed Rust in `graphforge-exec` owns projection, normalization, greedy coloring,
deterministic ordering, controls, and Arrow shaping. Python and Node only adapt
arguments and the native Arrow result. They contain no coloring algorithm,
external graph runtime, fallback, service, packaging dependency, or recovery
path. Knowledge-layer presence cannot change this topology-only result,
preserving the topology-only knowledge isolation boundary.

#### Canonical `k1_coloring` contract

`k1_coloring` contract version 1 computes one deterministic distance-1 greedy
vertex coloring of the selected undirected simple graph. The name does not
introduce a caller-supplied `k`, an iterative convergence result, or a claim of
minimum coloring. The result is always a proper coloring on success, but it may
use more colors than exact `chromatic_number`. It is also distinct from
`node_coloring`: K1 processes higher-degree nodes first, while
`node_coloring` processes nodes only by UUID.

The operation requires `directed=False`; `directed=True`, including the public
default, is a structured validation error. It is unweighted and rejects every
`weight`, `k`, and `partition_property`. `label=None` selects every node, a
valid label selects the node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

Rust first sorts selected nodes by ascending raw public UUID and normalizes
stored adjacency to an undirected simple graph. It then processes nodes by
descending normalized degree, breaking degree ties by ascending UUID. Each node
receives the smallest nonnegative color not already assigned to one of its
colored neighbors. Finally, Rust renumbers non-isolate colors by first
appearance in ascending node-UUID order. Every isolate is assigned color `0`
independently of that renumbering. Rows use ascending node-UUID order, making
repeated output byte-stable for the same resolved projection and limits.

Formally, for normalized neighbor sets `N(v)`, nodes are ordered by the key
`(-|N(v)|, uuid(v))`. At a node's turn, its raw color is
`min(NonNegativeIntegers \ {color(u) | u in N(v), u already colored})`.
Scanning nodes afterward by ascending UUID defines a first-appearance map from
raw colors to consecutive `UInt64` labels starting at `0`; isolates bypass that
map and return `0`. This ordering, assignment, and renumbering is the complete
version-1 algorithm.

Mirrored adjacency entries sharing one stored `edge_uuid` collapse.
Inconsistent reuse of one edge UUID is an error. Distinct parallel and
reciprocal stored edges also deduplicate to one undirected neighbor pair and
cannot change degree or color. A self-loop makes the selected graph
non-colorable under this contract and returns a structured execution error
without rows. Duplicate node UUIDs, malformed identity, endpoints outside the
selection, checked-counter overflow, and color conversion failure are likewise
structured atomic errors.

Empty and label-empty selections return a typed zero-row result. Every selected
node in every disconnected component is returned. The exact non-null schema is
`node_uuid: FixedSizeBinary(16), color: UInt64`, with
`graphforge.algorithm=k1_coloring`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. Public identity is UUID-only;
execution surrogates and knowledge-layer identity never appear.

Let `V` be selected nodes, `A` direction-expanded stored adjacency entries, and
`E` normalized undirected neighbor pairs. UUID and coloring-order sorts,
stored-edge normalization, ordered neighbor sets, used-color sets, and
canonical renumbering take
`O(V log V + A log(A + V) + E log V)` time and `O(V + E)` auxiliary memory.
Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, normalization, validation, checked
arithmetic, resource-limit, cancellation, execution, and Arrow-shaping
failures abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, normalization, coloring, deterministic
ordering, controls, and Arrow shaping. Python and Node only adapt arguments and
the native Arrow result; neither binding contains coloring logic. There is no
external graph runtime, fallback, service, packaging dependency, or recovery
path.

K1 coloring consumes only graph-native topology, selectors, typed options, and
resolved UUID identity. It never reads confidence, provenance, assertions,
evidence, belief status, hypotheses, or valid time, and none of those values
becomes an option or conditional result column. The shipped neutral invocation
descriptor records catalog value
`analyze.k1_coloring`, contract version `1`, the resolved projection
fingerprint, normalized label and relationship selectors, `directed=False`,
the absence of weight and unrelated options, applied limits, and result-schema
version `1`. Durable descriptor persistence and `algorithm_run_uuid` ownership
belong to knowledge-layer; point-in-time or competing-hypothesis projection resolution
belongs to epistemic before dispatch. Those shipped orchestration APIs do not alter
this algorithm computation or schema.

#### Canonical `edge_coloring` contract

`edge_coloring` greedily assigns a color to every selected stored edge so no
two edges incident to the same endpoint share a color. It requires
`directed=False`; `directed=True`, including the public default, is a
structured validation error. The algorithm is unweighted and rejects every
`weight`. `label=None` selects every node, a valid label selects the
node-induced graph, `via=None` includes every relationship type between
selected nodes, and a valid `via` selects one type.

Rust canonicalizes each edge's endpoint UUIDs and collapses identical or
mirrored adjacency entries only when they share one stored `edge_uuid`.
Distinct parallel and reciprocal stored edges retain distinct identities and
remain incident competitors, so each receives a different color at their
shared endpoints. Inconsistent reuse of one edge UUID is an error. A self-loop
cannot be properly edge-colored and fails the complete operation with a
structured non-colorable execution error.

Unique stored edges are processed by ascending raw `edge_uuid`. For each edge,
Rust assigns the smallest zero-based color not already used by a colored edge
incident to either endpoint. Colors use checked arithmetic and convert to
`UInt64`; this deterministic greedy assignment is not a promise to find the
minimum chromatic index. Rows remain in ascending raw `edge_uuid` order.

The exact schema is non-null `edge_uuid: FixedSizeBinary(16)`, non-null
`color: UInt64`, with `graphforge.algorithm=edge_coloring`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`
metadata. Empty, label-empty, edgeless, and isolate-only selections return the
typed zero-row result. Disconnected components are colored in the same global
edge-UUID order. Isolated nodes produce no edge rows. Duplicate node UUIDs,
endpoints outside the selection, malformed identity, and inconsistent edge
identity are structured errors; internal surrogates never escape.

Let `A` be direction-expanded adjacency entries, `E` unique selected stored
edges, and `Delta` the maximum incident-edge count. Deterministic projection
and identity normalization use `O(V log V + A log E)` time; greedy coloring
uses `O(E Delta log Delta)` time in the worst case. Working memory is
`O(V + E)`. Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, normalization, coloring, checked
arithmetic, limit, cancellation, execution, and Arrow-shaping failures abort
atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, normalization, coloring,
deterministic ordering, controls, and Arrow shaping. Python and Node only adapt
arguments and the native Arrow result; neither binding contains an edge-color
algorithm. Knowledge-layer presence cannot affect this topology-only result,
preserving the topology-only knowledge isolation boundary. No external
graph runtime, fallback, service, packaging dependency, or recovery path
participates.

#### Canonical `chromatic_number` contract

`chromatic_number` returns the minimum number of colors required to color every
selected node so adjacent nodes differ. It computes the exact chromatic number,
not a greedy approximation. The operation requires `directed=False`;
`directed=True`, including the public default, is a structured validation
error. It is unweighted and rejects every `weight`. `label=None` selects every
node, a valid label selects the node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

Rust canonicalizes edge endpoint UUIDs and builds an undirected simple graph.
Identical or mirrored adjacency entries with one stored `edge_uuid` collapse;
inconsistent reuse of that UUID is an error. Distinct parallel and reciprocal
stored edges deduplicate to one neighbor pair and cannot increase the result.
A self-loop makes proper coloring impossible and returns a structured
non-colorable execution error.

The exact search is deterministic DSATUR branch-and-bound. It chooses the
uncolored node with maximum distinct-neighbor-color saturation, then maximum
simple degree, then smallest raw public node UUID. Candidate existing colors
are tried in ascending order before the single symmetry-representative new
color. A deterministic greedy DSATUR pass supplies only the initial upper
bound; Rust does not return it until exhaustive search has proved no smaller
coloring exists. Shared iteration exhaustion therefore returns an error, never
an approximate answer.

The result is exactly one non-null `chromatic_number: UInt64` row with
`graphforge.algorithm=chromatic_number`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. An empty selection returns
`0`; every nonempty edgeless or isolate-only selection returns `1`.
Disconnected selections return the maximum exact chromatic number among their
components, and adding isolates to a nonempty graph does not raise it. Checked
color counts prevent wrapping. Duplicate node UUIDs, endpoints outside the
selection, malformed identity, and inconsistent edge identity are structured
errors; internal surrogates never escape.

Let `A` be direction-expanded adjacency entries, `E` normalized simple edges,
and `U <= V` the deterministic greedy upper bound. Projection and
normalization use `O(V log V + A log E)` time and `O(V + E)` memory. Exact
coloring is NP-hard and has exponential worst-case search, bounded by
`O(U^V (V + E) log V)` work with `O(V + E)` search state. Shared limits allow
at most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Projection, normalization, search, checked arithmetic, limit, cancellation,
execution, and Arrow-shaping failures abort atomically without a scalar row.

Typed Rust in `graphforge-exec` owns projection, normalization, exact search,
deterministic choices, controls, and Arrow shaping. Python and Node only adapt
arguments and the native Arrow result; neither contains a coloring algorithm.
Knowledge-layer presence cannot affect this topology-only scalar, preserving the topology-only knowledge isolation boundary. No external
graph runtime, fallback, service, packaging dependency, or recovery path
participates.

### Matching (`by=`)

| Value | Algorithm | Source reference |
| -------------------------- | ------------------------------------ | ---------------- |
| `max_weight_matching` | Maximum weight matching (edges) | OCG |
| `max_cardinality_matching` | Maximum cardinality matching (edges) | OCG |
| `max_bipartite_matching` | Maximum bipartite matching | igraph |

#### Canonical `max_weight_matching` contract

`max_weight_matching` returns a deterministic maximum-weight matching: a set of
stored edges in which no selected node occurs more than once and whose summed
edge weight is greatest. It requires `directed=False`; `directed=True`,
including the public default, is a structured validation error because directed
matching is a different problem. Empty, label-empty, edgeless, isolated-node,
and no-candidate selections return a typed zero-row matching.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a named `via` selects one valid
type. Omitting `weight` assigns every selected edge weight `1.0`, so the primary
objective becomes maximum cardinality. A named weight is strict across all
selected candidate edges: every value must be present, non-null, numeric, and
finite. Negative, zero, and positive finite values are valid. Missing
properties, nulls, non-numeric values, NaN, infinity, non-finite accumulated
weights, blank selectors, malformed adjacency, storage failures, and shaping
failures return structured errors without partial output.

The primary objective always maximizes the sum of the represented `Float64`
weights. Among equal-weight optima, GraphForge chooses the matching with more
edges, then the lexicographically smallest sorted sequence of canonical
`(source_uuid, target_uuid, edge_uuid)` tuples. Consequently an all-negative
weighted selection returns no rows, while zero-weight edges may be added when
they do not reduce the primary objective. There is no public
`maxcardinality` switch: cardinality is only the fixed secondary tie-break for
this weight-primary algorithm. Call `max_cardinality_matching` when cardinality
must be the primary objective.

The undirected projection mirrors adjacency, then Rust collapses only entries
sharing one stored `edge_uuid` and the same unordered endpoints. Reusing one
edge UUID with inconsistent endpoints is an error. Distinct parallel and
reciprocal stored edges remain independent candidates, and self-loops are
ignored because a matched edge must join two distinct nodes. Endpoints are
canonicalized so `source_uuid < target_uuid`. Returned rows sort by ascending
canonical `source_uuid`, canonical `target_uuid`, then `edge_uuid`, independently
of the order in which the matching search discovers them.

The exact EDGE_LIST schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and
`weight: Float64`. UUID fields are non-null and expose stored public identity,
never execution surrogates. The shared logical schema marks `weight` nullable,
but every returned maximum-weight-matching row contains a non-null unit or
validated named weight. Metadata includes
`graphforge.algorithm=max_weight_matching`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`.

A deterministic blossom/primal-dual implementation runs in `O(V^3)` time and
`O(V^2 + E)` auxiliary space after projection. Shared hard limits allow at most
10,000,000 selected nodes, 100,000,000 direction-expanded adjacency entries,
10,000,000 output rows, and 10,000 cooperative work checkpoints. Projection,
weight validation, blossom search, optimum tie resolution, output shaping,
limit failures, and cancellation abort atomically without partial output.

Typed, dependency-free Rust in `graphforge-exec` owns projection, validation,
blossom/primal-dual execution, deterministic choices, controls, and Arrow
shaping. Python and Node only adapt arguments and native Arrow/Arrow IPC.
Knowledge-layer presence cannot affect this topology-and-property operation,
preserving the topology-only knowledge isolation boundary. External
algorithm libraries and documentation are development parity references only;
no external runtime backend, fallback, service, packaging dependency, or
recovery path participates.

#### Canonical `max_cardinality_matching` contract

`max_cardinality_matching` returns a deterministic maximum-cardinality
matching of a general graph: a set of stored edges in which no selected node
occurs more than once and whose number of edges is greatest. It requires
`directed=False`; `directed=True`, including the public default, is a structured
validation error because directed matching is a different problem. The
operation is unweighted and rejects every `weight`.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid named `via` selects
one type. Empty, label-empty, edgeless, isolated-node-only, and no-candidate
selections return the same typed zero-row matching. Disconnected components
participate in one global objective, so their independently matchable edges all
contribute to the maximum cardinality.

The undirected projection mirrors adjacency, then Rust collapses only entries
sharing one stored `edge_uuid` and the same unordered endpoints. Reusing one
edge UUID with inconsistent endpoints is an error. Self-loops are ignored
because a matching edge must join two distinct nodes. Distinct parallel and
reciprocal stored edges remain independent candidates, but at most one can be
selected for the same endpoint pair because they share both nodes.

Among all maximum-cardinality matchings, GraphForge chooses the
lexicographically smallest sorted sequence of raw stored `edge_uuid` values.
This objective compares edge identity directly, not endpoint order: when two
equal-cardinality alternatives differ between a lower edge UUID with higher
endpoint UUIDs and a higher edge UUID with lower endpoint UUIDs, the lower edge
UUID wins. Returned rows sort by ascending `edge_uuid`; each row independently
canonicalizes its endpoints so `source_uuid < target_uuid`. This intentionally
differs from `max_weight_matching`, whose final tie-break and row order remain
canonical `(source_uuid, target_uuid, edge_uuid)` tuple order. The selected
stored edge identities are therefore independent of node, edge, table,
adjacency, relationship-type, component, and search-discovery order.

The exact `UNWEIGHTED_EDGE_LIST` schema is non-null
`edge_uuid: FixedSizeBinary(16)`, non-null
`source_uuid: FixedSizeBinary(16)`, and non-null
`target_uuid: FixedSizeBinary(16)`, in that order. It has no `weight` column.
UUID fields expose stored public identity, never execution surrogates. Metadata
includes `graphforge.algorithm=max_cardinality_matching`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.

Exact general-graph matching has an `O(V^3)` time and `O(V^2 + E)` auxiliary
space contract after projection. Shared hard limits allow at most 10,000,000
selected nodes, 100,000,000 direction-expanded adjacency entries, 10,000,000
output rows, and 10,000 cooperative work checkpoints. Selection, projection,
normalization, blossom search, optimum tie resolution, checked arithmetic,
output shaping, limit failures, and cancellation abort atomically without
partial output.

Typed, dependency-free Rust in `graphforge-exec` owns projection, exact matching,
deterministic choices, controls, and Arrow shaping. It may reuse the private
shared matching/blossom core used by other exact matching handlers, but that
core is not a public API and cannot change this cardinality-primary, unweighted
contract. Python and Node only adapt arguments and native Arrow/Arrow IPC.
Knowledge-layer presence cannot affect this topology-only operation,
preserving the topology-only knowledge isolation boundary. External
algorithm libraries and documentation are development parity references only;
no external runtime backend, fallback, service, packaging dependency, or
recovery path participates.

The neutral reproducibility inputs are catalog value
`analyze.max_cardinality_matching`, contract version `1`, the resolved
projection fingerprint, normalized `label` and `via` selectors,
`directed=False`, the absence of `weight`, applied limits, and result-schema
version `1`. The shipped knowledge run API can persist the descriptor and
`algorithm_run_uuid` without altering this algorithm computation or schema.

#### Canonical `max_bipartite_matching` contract

`max_bipartite_matching` returns a deterministic maximum-cardinality matching
of the selected bipartite graph. It requires `directed=False`; `directed=True`,
including the public default, is a structured validation error. The operation
is unweighted and rejects every `weight`. An optional `label` selects the
node-induced graph, `via=None` includes every relationship type between
selected nodes, and a valid named `via` selects one type.

Callers may supply `partition_property`. The graph facade resolves that
property for every selected node to a complete immutable
`node_uuid -> partition_id` mapping before kernel dispatch. Values must be
non-null scalar strings or integers; integers normalize to their decimal
strings. Missing, NULL, unsupported, duplicate, conflicting, incomplete, or
out-of-projection values are structured errors. Every edge-bearing selection
must resolve to exactly two partition identifiers, and every selected edge
must cross between them. The lexicographically smaller normalized partition
identifier is the left side.

When `partition_property` is omitted, Rust deterministically two-colors each
connected component. It starts each component at its lowest canonical public
node UUID, assigns that node to the left side, and visits neighbors in
canonical UUID order. A self-loop or odd cycle fails the complete invocation
with a structured non-bipartite error. Isolated nodes are valid and unmatched;
under inference each isolate is its own left-rooted component. Empty,
label-empty, edgeless, isolate-only, and no-match selections return the same
typed zero-row result.

Mirrored adjacency entries sharing one stored `edge_uuid` collapse. Reusing
one edge UUID for different unordered endpoints is an error. Distinct parallel
and reciprocal stored edges remain present through partition validation.
Matching uses canonical left-node, right-node, and adjacency order so equal
maximum-cardinality choices are stable. If parallel edges join a selected
matched endpoint pair, the lowest canonical stored edge UUID represents that
pair. Every result row is oriented from the resolved or inferred left side to
the right side and rows sort by ascending
`(source_uuid, target_uuid, edge_uuid)`.

The exact `UNWEIGHTED_EDGE_LIST` schema is non-null
`edge_uuid: FixedSizeBinary(16)`, non-null
`source_uuid: FixedSizeBinary(16)`, and non-null
`target_uuid: FixedSizeBinary(16)`, in that order, with no `weight` or
Knowledge-layer column. UUID fields expose stored public identity, never
execution surrogates. Metadata includes
`graphforge.algorithm=max_bipartite_matching`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`.

The deterministic Hopcroft-Karp kernel runs in `O(E sqrt(V))` time after
projection and uses `O(V + E)` auxiliary space. Shared hard limits allow at
most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Projection, partition validation or inference, matching, output shaping, limit
failures, and cancellation abort atomically without partial output.

Typed Rust owns graph-property resolution, the validated UUID mapping boundary,
partition inference, matching, deterministic choices, controls, and Arrow
shaping. The matching kernel consumes only graph-native topology and the
resolved UUID mapping; it does not read graph storage properties or knowledge
tables. Python and Node only adapt arguments and native Arrow/Arrow IPC.
Confidence, provenance, assertions, evidence, belief status, hypotheses, and
valid time are neither inputs nor result fields. The knowledge layer can persist the neutral
invocation descriptor, mapping fingerprint, and `algorithm_run_uuid`; the epistemic layer may
resolve an equivalent immutable mapping before dispatch and attach knowledge
after dispatch. Neither layer changes this algorithm computation or schema. No
external runtime backend, fallback, service, packaging dependency, or recovery
path participates.

### Eulerian Graph (`by=`)

| Value | Algorithm | Source reference |
| ------------------- | ------------------------------------------------- | ---------------- |
| `euler_circuit` | Eulerian circuit aligned node/edge UUID sequences | OCG |
| `euler_path` | Eulerian path aligned node/edge UUID sequences | OCG |
| `has_euler_circuit` | Boolean: has Eulerian circuit? | OCG |
| `has_euler_path` | Boolean: has Eulerian path? | OCG |

#### Canonical Euler construction contracts

`euler_circuit` and `euler_path` construct a trail over the selected stored
multigraph; they are not aliases for the Boolean `has_euler_circuit` and
`has_euler_path` classifiers below. Each construction returns zero or one row
with this exact non-null schema:

```text
node_path: List<FixedSizeBinary(16)> not null
edge_path: List<FixedSizeBinary(16)> not null
```

The list items are also non-null. Schema metadata is
`graphforge.algorithm=euler_circuit` or `euler_path`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Both lists contain public UUID identity only. For a returned trail,
`edge_path.len() + 1 == node_path.len()`, and edge `i` joins node `i` to node
`i + 1` in the requested direction mode. Every selected logical edge UUID
appears exactly once. A circuit additionally repeats its first node UUID as
its last; a path may be open or closed.

Both values are unweighted and reject every `weight`. `label=None` selects
every node, a valid label selects the node-induced graph, `via=None` includes
every relationship type, and a valid `via` selects one type. With the default
`directed=True`, stored orientation is preserved. With `directed=False`,
endpoints are canonicalized for identity validation and either orientation is
valid in the returned trail. Identical directed rows, or identical/mirrored
undirected rows, sharing one edge UUID collapse; reusing that UUID for
different canonical endpoints is an error. Distinct UUIDs remain distinct
parallel edges. Self-loops remain one edge, appear once in `edge_path`, and
produce adjacent equal node UUIDs.

The construction first applies the corresponding predicate's degree and
connectivity classification. `euler_circuit` requires circuit classification.
`euler_path` accepts either an open path or a circuit. An open undirected path
starts at the lowest UUID among the two odd-degree endpoints; an open directed
path starts at the required node whose out-degree exceeds its in-degree by
one. For a circuit, both values start at the lowest UUID incident to an edge.
After the mathematically required start class is chosen, deterministic
Hierholzer traversal consumes eligible edges in ascending canonical edge-UUID
order. Thus UUID ordering breaks only genuine ties; it never overrides the
required endpoint class.

An empty or label-empty selection returns a typed zero-row table. Any non-empty
edgeless selection, including a singleton, returns one row containing the
lowest selected node UUID and an empty `edge_path`. A non-Eulerian request is
not an empty result: it returns the structured `UndefinedEulerCircuit` or
`UndefinedEulerPath` error. Duplicate node UUIDs, endpoints outside the
selection, malformed or inconsistent identity, invalid selectors/options,
checked-arithmetic overflow, allocation/execution failures, and Arrow-shaping
failures are structured errors.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, deterministic identity normalization uses
`O(V log V + A log E)` time. Classification and iterative Hierholzer traversal
use `O(V + E)` time and `O(V + E)` working memory; the returned lists use
`O(E)` space. Shared limits allow at most 10,000,000 selected nodes,
100,000,000 direction-expanded adjacency entries, 10,000,000 output rows, and
10,000 cooperative work checkpoints. Projection, classification, traversal,
list allocation, and shaping check cancellation and limits. Complete
validation precedes publication, so every failure is atomic and exposes no
partial row.

This is deterministic Hierholzer contract version `1` and result-schema
version `1`. Rust owns projection, classification, traversal, controls, and
Arrow shaping. Python and Node only coerce arguments and convert the freshly
built native Arrow result; neither contains an implementation or runtime
external-library fallback.

The neutral algorithm invocation descriptor is separate from the Arrow result and
contains the catalog value and contract version, normalized selectors,
direction, typed options and limits, canonical projection fingerprint, and
result-schema version. The shipped knowledge run API can persist it and a result
fingerprint under an
`algorithm_run_uuid`; that UUID never enters algorithm options or this schema. epistemic
may resolve point-in-time or competing-hypothesis state to an explicit UUID
projection before dispatch and attach assertions, evidence, and reasoning
after dispatch. analyst-verb never reads those knowledge tables or implicitly uses
confidence, assertions, evidence, belief status, provenance, valid time, or
hypothesis membership, and none of those fields condition this schema.

#### Canonical `has_euler_circuit` contract

`has_euler_circuit` returns whether every selected stored edge can appear
exactly once in one closed trail. Both direction modes are supported. With the
default `directed=True`, Rust applies the directed multigraph predicate;
`directed=False` applies the undirected multigraph predicate. The operation is
unweighted and rejects every `weight`. `label=None` selects every node, a valid
label selects the node-induced graph, `via=None` includes every relationship
type, and a valid `via` selects one type.

An empty, label-empty, or edgeless selection returns `true`. Isolated nodes do
not participate in connectivity requirements. In undirected mode, every
non-isolated node must have even degree and all non-isolated nodes must belong
to one connected component. A self-loop contributes two to its node's degree.
In directed mode, every node must have equal checked in-degree and out-degree,
and all nodes incident to an edge must belong to one strongly connected
component. A self-loop contributes one incoming and one outgoing incidence.

Stored `edge_uuid` is logical edge identity. In directed mode, duplicate rows
with the same UUID and ordered endpoints collapse; reusing that UUID for any
other ordered endpoint pair is an error. In undirected mode, endpoints are
canonicalized and identical or mirrored rows with the same UUID collapse.
Distinct UUIDs always remain distinct edges: parallel and reciprocal stored
edges contribute independently. In undirected mode, reciprocal UUIDs become
parallel edges and each contributes to both endpoint degrees.

The result is exactly one non-null `has_euler_circuit: Boolean` row with
`graphforge.algorithm=has_euler_circuit`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. Degree increments are
checked and cannot wrap. Duplicate node UUIDs, endpoints outside the
selection, malformed identity, and inconsistent edge identity are structured
errors; no internal surrogate appears in the scalar result.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, deterministic identity normalization uses
`O(V log V + A log E)` time. Degree classification plus iterative
connectivity or forward-and-transpose reachability uses `O(V + E)` time;
working memory is `O(V + E)`. Shared limits allow at most 10,000,000 selected
nodes, 100,000,000 direction-expanded adjacency entries, 10,000,000 output
rows, and 10,000 cooperative work checkpoints. Projection, normalization,
degree overflow, traversal, limit, cancellation, execution, and Arrow-shaping
failures abort atomically without a Boolean row.

#### Canonical `has_euler_path` contract

`has_euler_path` returns whether every selected stored edge can appear exactly
once in one trail. The trail may be open or closed. Both direction modes are
supported. With the default `directed=True`, Rust applies the directed
multigraph predicate; `directed=False` applies the undirected multigraph
predicate. The operation is unweighted and rejects every `weight`. `label=None`
selects every node, a valid label selects the node-induced graph, `via=None`
includes every relationship type, and a valid `via` selects one type.

An empty, label-empty, or edgeless selection returns `true`. Isolated nodes do
not participate in connectivity requirements. In undirected mode, every
non-isolated node must belong to one connected component and exactly zero or
two nodes may have odd degree. A self-loop contributes two to its node's
degree. In directed mode, all nodes incident to an edge must belong to one
weakly connected component of the underlying undirected graph. Either every
node has equal checked in-degree and out-degree, or exactly one start node has
`out_degree - in_degree = 1`, exactly one end node has
`in_degree - out_degree = 1`, and every other node is balanced. A self-loop
contributes one incoming and one outgoing incidence.

Stored `edge_uuid` is logical edge identity. In directed mode, duplicate rows
with the same UUID and ordered endpoints collapse; reusing that UUID for any
other ordered endpoint pair is an error. In undirected mode, endpoints are
canonicalized and identical or mirrored rows with the same UUID collapse.
Distinct UUIDs always remain distinct edges: parallel and reciprocal stored
edges contribute independently. In undirected mode, reciprocal UUIDs become
parallel edges and each contributes to both endpoint degrees.

The result is exactly one non-null `has_euler_path: Boolean` row with
`graphforge.algorithm=has_euler_path`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. Degree increments and
directed degree differences are checked and cannot wrap. Duplicate node UUIDs,
endpoints outside the selection, malformed identity, and inconsistent edge
identity are structured errors; no internal surrogate appears in the scalar
result.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, deterministic identity normalization uses
`O(V log V + A log E)` time. Degree classification plus iterative active
connectivity uses `O(V + E)` time; working memory is `O(V + E)`. Shared limits
allow at most 10,000,000 selected nodes, 100,000,000 direction-expanded
adjacency entries, 10,000,000 output rows, and 10,000 cooperative work
checkpoints. Projection, normalization, degree overflow, traversal, limit,
cancellation, execution, and Arrow-shaping failures abort atomically without
a Boolean row.

Typed Rust in `graphforge-exec` owns projection, normalization, degree accounting,
connectivity, controls, and Arrow shaping. Python and Node only adapt arguments
and the native Arrow result; neither contains an Eulerian implementation.
Knowledge-layer presence cannot affect this topology-only predicate,
preserving the topology-only knowledge isolation boundary. No external
graph runtime, fallback, service, packaging dependency, or recovery path
participates.

### Planarity and Structure (`by=`)

| Value | Algorithm | Source reference |
| --------------------- | ----------------------------------------------------- | ------------------------------------------------- |
| `is_planar` | Boyer-Myrvold / LR planarity test | OCG |
| `articulation_points` | Articulation point detection | GDS |
| `bridges` | Bridge (cut-edge) detection | GDS |
| `triangle_count` | Global triangle count | GDS |
| `conductance` | Conductance metric for a partition | GDS |
| `modularity` | Modularity of a given partition | igraph |
| `transitivity` | Global transitivity | igraph |
| `triad_census` | MAN triad census | igraph |
| `dyad_census` | Dyad census (mutual/asymmetric/null) | igraph |
| `count_automorphisms` | Exact individualization/refinement/backtracking count | `automorphism-ir-v1` / `automorphism-count-ir-v1` |

#### Canonical `count_automorphisms` contract

`count_automorphisms` counts every topology automorphism of the selected
multigraph exactly once. `label` selects a node-induced graph, `via` optionally
restricts the relationship type, and both `directed=True` and
`directed=False` are valid. The operation is unweighted and seedless:
`weight`, `k`, and `partition_property` are structured validation errors.
Node and edge properties never color vertices or edges.

The Rust-owned `automorphism-ir-v1` normalization sorts nodes and stored edges
by UUID for deterministic scheduling, collapses mirrored adjacency records for
the same stored edge UUID, and preserves loops, distinct parallel edges, and
reciprocal-edge multiplicity. Directed mode preserves ordered endpoints.
Undirected mode canonicalizes endpoint pairs while preserving each distinct
stored edge. UUID values determine branch order only; renaming every UUID
without changing topology leaves the count unchanged.

`automorphism-count-ir-v1` builds an equitable structural partition from loop,
outgoing, incoming, and adjacency-multiplicity signatures, then performs exact
individualization/refinement/backtracking. Candidate and branch order is
canonical, and only adjacency multiplicity decides whether a completed
permutation is an automorphism. Empty and singleton selections each have count
one.

The result is exactly one non-null row with `count: UInt64`. Metadata is
`graphforge.algorithm=count_automorphisms`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. No UUID, property, confidence,
provenance, assertion, evidence, belief, hypothesis, valid-time, or
`algorithm_run_uuid` column is conditional on graph contents.

Normalization uses a checked dense `V`-by-`V` multiplicity matrix, so its
working memory is `O(V^2)` and its deterministic matrix cap is 16,000,000
entries. Exact search is exponential in the worst case. Search depth is capped
at 1,024 and aggregate search state at 16,000,000 entries, in addition to the
shared projection, adjacency, one-row output, iteration, and cancellation
controls. Invalid identities or projections, inconsistent stored-edge UUIDs,
allocation or supported-range failures, cancellation, resource or state-limit
exhaustion, and `UInt64` count overflow are distinct structured failures. Every
failure is atomic and returns no partial scalar.

Rust owns normalization, exact search, controls, and Arrow shaping. Python and
Node only coerce arguments and expose the same native result; igraph may be
used as an optional development parity oracle but is never a runtime backend,
fallback, packaging dependency, or recovery path. Knowledge tables and
sidecars cannot affect the graph-native topology consumed by analyst-verb. The epistemic layer may
resolve an immutable projection before dispatch, but equivalent native and
resolved projections produce the same result.

The neutral deterministic invocation descriptor consists of the
`count_automorphisms` catalog value, the two algorithm versions above,
normalized `label` and `via`, `directed`, the absence of graph-native property
inputs and RNG state, applied limits, the canonical projection fingerprint,
and result-schema version `1`. Durable descriptor persistence and
`algorithm_run_uuid` are shipped knowledge-layer orchestration, not algorithm computation, and
the run UUID is never a result column.

Shipped evidence is finite and layer-specific. The two
`algorithm_analyze_automorphism` modules cover normalization, refinement,
brute-force small-graph equivalence, UUID-renaming invariance, overflow,
cancellation, depth, matrix, search-state, iteration, allocation, and malformed
projection boundaries. `algorithm_analyze.rs` proves registered dispatch,
canonical families, schema, replay, and atomic controls. The `graphforge-api`
acceptance tests prove persisted reopen, exact metadata and nullability,
closed options, overflow, and graph-property independence.
`knowledge_isolation.rs` proves reserved-sidecar and equivalent resolved
projection isolation. The fresh-wheel Python smoke test and the independent
Node native acceptance file prove the same directed, undirected, empty,
singleton, schema, metadata, replay, isolation, and structured-error contract
through both thin bindings.

#### Canonical `conductance` contract

`conductance` measures an explicit partition of the selected undirected graph.
Callers must pass `directed=False` and a non-empty `partition_property`;
`directed=True`, including the public default, and an omitted or blank partition
property are structured validation errors. The facade resolves that property
for every selected node. Values must be non-null scalar strings or integers and
are normalized to deterministic string partition identifiers. At least two
non-empty partitions are required.

An omitted `weight` gives every selected edge unit weight. A named weight
property must resolve on every selected edge to a finite nonnegative numeric
value; missing, NULL, non-numeric, NaN, infinite, and negative values are
errors. Mirrored adjacency entries for one stored edge collapse to that edge,
while distinct parallel edges contribute independently. Each non-self-loop
edge contributes its weight once to each endpoint partition's volume and, when
its endpoints have different partitions, once to both partitions' cut. A
self-loop contributes twice its weight to its node partition's volume and
never contributes to a cut.

For each partition \(S\), Rust computes

\[
\operatorname{conductance}(S) =
\frac{\operatorname{cut}(S, V-S)}
{\min(\operatorname{volume}(S), \operatorname{volume}(V-S))}.
\]

If either denominator volume is zero, the whole invocation fails with a
structured undefined-conductance error naming the partition. It never returns
NaN, infinity, NULL, or partial rows.

The canonical Arrow result contains one non-null row per partition with schema
`partition_id: Utf8, conductance: Float64`, ordered lexicographically by the
normalized partition identifier. Metadata is
`graphforge.algorithm=conductance`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Equal resolved graph inputs and
options produce the same rows and order.

The graph facade resolves `partition_property` to a validated immutable
`node_uuid -> partition_id` mapping before dispatch. Typed Rust owns
projection, validation, checked accumulation, cancellation, shared node,
direction-expanded edge, output-row, and cooperative-work limits, computation,
and Arrow shaping. Every projection, limit, cancellation, storage, execution,
or shaping failure aborts atomically. Python and Node only adapt arguments and
the native Arrow result; no external graph library is a production backend.

Conductance consumes graph topology, graph-native properties, selectors,
typed options, and an explicit resolved UUID mapping only. It never reads
knowledge tables or implicitly consumes confidence, evidence, assertions,
belief status, provenance, or valid time. `as_of`, epistemic status,
hypothesis grouping, confidence policy, and provenance selectors are not analyst-verb
options, and no knowledge or provenance fields are conditionally appended to
the canonical result. The knowledge layer can persist a neutral invocation descriptor and
durable `algorithm_run_uuid`. The epistemic layer may resolve a point-in-time or
competing-hypothesis belief state to an equivalent immutable UUID projection
before dispatch, then attach assertions, evidence, and reasoning after
dispatch. Neither layer changes algorithm options, computation, or schema.

#### Canonical `modularity` contract

> **Status:** The Rust kernel, canonical schema, graph-native partition-property
> resolution, and `graphforge-api` dispatch are shipped. Fresh-native Python and Node
> acceptance coverage is also shipped; both bindings remain thin adapters over
> the Rust-owned result.

`modularity` measures an explicit partition of the selected undirected graph
using weighted Newman-Girvan modularity at the fixed analyst-verb resolution
\(\gamma=1.0\). Callers must pass `directed=False` and a non-empty
`partition_property`; `directed=True`, including the public default, and an
omitted or blank partition property are structured validation errors. The
facade resolves that property for every selected node to a complete immutable
`node_uuid -> partition_id` mapping. Values must be non-null scalar strings or
integers and are normalized to deterministic string partition identifiers.
Missing, NULL, unsupported, duplicate, conflicting, incomplete, or
out-of-projection mapping entries are structured errors.

An omitted `weight` gives every selected edge unit weight. A named weight
property must resolve on every selected edge to a finite nonnegative numeric
value; missing, NULL, non-numeric, NaN, infinite, and negative values are
errors. Mirrored adjacency entries for one stored edge collapse to that edge,
while distinct parallel and reciprocal stored edges contribute independently.
Let \(A_{ij}\) be the total selected edge weight between nodes \(i\) and \(j\),
\(k_i\) be node \(i\)'s weighted degree, \(2m = \sum_i k_i\), and \(c_i\) be
its normalized partition identifier. Rust computes

\[
Q = \frac{1}{2m}\sum_{i,j}
\left(A_{ij} - \frac{k_i k_j}{2m}\right)
\delta(c_i,c_j).
\]

A self-loop of weight \(w\) contributes \(2w\) to its node's degree and to
\(2m\), and the adjacency sum accounts for the same doubled contribution.
Parallel-edge weights remain distinct contributions before their canonical
sum. An empty selection, an edgeless selection, or any selection whose total
edge weight is zero fails atomically with a structured undefined-modularity
error. Checked accumulation in canonical node-UUID and edge-UUID order rejects
overflow or every non-finite intermediate or final value rather than returning
NaN or infinity.

The canonical Arrow result is exactly one non-null row with schema
`modularity: Float64`. Metadata is `graphforge.algorithm=modularity`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Equal resolved graph inputs and normalized options produce the same value and
schema. No partition rows, community assignments, or resolution parameter are
returned.

Typed Rust owns graph projection, property resolution, mapping validation,
checked accumulation, cancellation, shared node, direction-expanded edge,
output-row, and cooperative-work limits, computation, and Arrow shaping.
Validation completes before result publication, and every validation, limit,
cancellation, storage, execution, or shaping failure aborts without partial
output. Public identity at the mapping boundary is UUID-only; execution
surrogates never escape. Python and Node are thin argument and native Arrow
result adapters, with fresh-native acceptance coverage for equivalent logical
results. External graph
libraries may serve as development parity oracles but are never production
backends, fallbacks, services, packaging dependencies, or recovery paths.

Modularity consumes graph-native topology, properties, selectors, typed
options, and the explicit resolved UUID mapping only. It never reads knowledge/epistemic
knowledge tables or implicitly consumes confidence, evidence, assertions,
belief status, provenance, valid time, or hypothesis membership. `as_of`,
epistemic status, hypothesis grouping, confidence policy, and provenance
selectors are not algorithm options, and no knowledge or provenance fields are
conditionally appended to the scalar schema. The shipped knowledge run API can
persist the descriptor, mapping and
projection fingerprints, and durable `algorithm_run_uuid`. The epistemic layer may resolve a
point-in-time or competing-hypothesis projection and equivalent immutable UUID
mapping before dispatch, then attach assertions, evidence, and reasoning after
dispatch. Neither layer changes this algorithm computation, options, or schema.

#### Canonical `is_planar` contract

`is_planar` is an exact predicate over the selected undirected simple graph.
Callers must pass `directed=False`; `directed=True`, including the public
default, is rejected instead of silently projecting direction. The predicate
is unweighted and rejects every `weight`.

An optional `label` selects a node-induced subgraph. `via=None` includes all
selected relationship types, while a valid named `via` filters to one type.
After selection, Rust canonicalizes each endpoint pair, collapses mirrored
adjacency rows and all distinct parallel or reciprocal edges to one undirected
edge, and ignores self-loops. Identity inconsistencies, endpoints outside the
selection, blank selectors, malformed adjacency, storage failures, and shaping
failures are structured errors with no partial output.

Every valid selection returns exactly one non-null `is_planar: Boolean` row.
Empty, label-empty, singleton, edgeless, forest, and disconnected graphs are
valid; a disconnected graph is planar exactly when each component is planar.
The Boolean is deterministic. No embedding or Kuratowski-subgraph certificate
is part of the public result, so internal certificate choice and ordering are
not contractual.

Metadata is `graphforge.algorithm=is_planar`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. A linear-time planarity test over the
normalized graph uses `O(V + E)` working memory. Shared limits allow at most
10,000,000 selected nodes, 100,000,000 direction-expanded adjacency entries,
10,000,000 output rows, and 10,000 cooperative work checkpoints. Cancellation
and every projection, limit, execution, storage, or Arrow-shaping failure abort
atomically before a Boolean row is returned.

Typed Rust in `graphforge-exec` owns selection, normalization, validation, planarity,
controls, and Arrow shaping. Python and Node only adapt arguments and the
native Arrow result. Knowledge-layer presence cannot affect this topology-only
predicate, preserving the topology-only knowledge isolation boundary. No external
graph library, service, runtime backend, fallback, packaging dependency, or
recovery path participates.

#### Canonical `transitivity` contract

`transitivity` computes global undirected simple-graph transitivity as
`3 * triangles / connected_triples`, equivalently the number of closed wedges
divided by all wedges. It requires `directed=False`; `directed=True`, including
the public default, is a structured validation error. The metric is unweighted
and rejects any `weight`.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid named `via` selects one
type. Rust canonicalizes endpoints and collapses mirrored adjacency entries
that share one stored `edge_uuid`. Distinct parallel and reciprocal stored
edges deduplicate to one undirected neighbor pair, and self-loops are ignored.
Duplicate node UUIDs, endpoints outside the selection, inconsistent reuse of
an edge UUID, blank selectors, malformed adjacency, storage failures, and
shaping failures return structured errors without partial output.

The result is exactly one deterministic row for every valid selection. Empty,
edgeless, isolate-only, disconnected, and label-empty graphs are valid. Counts
are global across every disconnected component. When the selected simple graph
contains no wedges, including a graph made only of isolated nodes or disjoint
single edges, the result is the finite value `0.0`.

The exact non-null scalar schema is `transitivity: Float64`. Metadata is
`graphforge.algorithm=transitivity`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. No node, edge, or execution-surrogate
identity is exposed by this graph-level scalar.

Degree wedges, triangles, and `3 * triangles` use checked `UInt64` arithmetic;
overflow is a structured execution error rather than wraparound. Conversion
occurs only for the final deterministic finite `Float64` ratio. Deterministic
UUID indexing and simple-neighbor sets make the value independent of input
order. Ordered normalization is `O((V + E) log(V + E))`; the current exact
triangle scan is `O(V^3 log V)` in the dense worst case because neighbor
membership uses ordered sets. Working memory is `O(V + E)` for the simple
projection.

Shared hard limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative iteration checkpoints. The scalar consumes exactly one output row.
Cancellation is checked during projection and counting, and validation,
arithmetic, storage, limit, cancellation, execution, and Arrow-shaping
failures abort atomically without partial output.

Typed Rust in `graphforge-exec` owns projection, validation, normalization, checked
counting, controls, and Arrow shaping. Python and Node only adapt arguments and
native Arrow/Arrow IPC. The result depends only on selected topology and is
unchanged by knowledge-layer presence. No
external algorithm runtime, fallback, packaging dependency, service, or
recovery path participates.

#### Canonical `triad_census` contract

`triad_census` classifies every unordered triple of distinct selected nodes
exactly once by its induced directed simple graph. It implements the standard
Davis-Leinhardt/MAN census and requires `directed=True`; `directed=False` is a
structured validation error. The census is unweighted and rejects every
`weight`.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid named `via` selects
one type. Rust reduces the selected multigraph to ordered-pair presence:
mirrored adjacency entries and any number of parallel edges from `u` to `v`
establish one directed tie. A selected edge from `v` to `u` establishes the
reciprocal tie independently. Self-loops are ignored because a triad contains
three distinct nodes. Duplicate node UUIDs, endpoints outside the selection,
inconsistent reuse of an edge UUID, blank selectors, malformed adjacency,
storage failures, and shaping failures return structured errors without
partial output.

The result always contains exactly these 16 rows in this order:

`003`, `012`, `102`, `021D`, `021U`, `021C`, `111D`, `111U`, `030T`,
`030C`, `201`, `120D`, `120U`, `120C`, `210`, `300`.

The first three digits are the counts of mutual, asymmetric, and null dyads
within the triple. Where that signature has multiple orientations, `D`, `U`,
`C`, and `T` distinguish down, up, cyclic, and transitive forms. This naming
and order follow the standard 16 directed triads documented by
[NetworkX](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.triads.triadic_census.html)
and the igraph reference already named by the catalog. Those libraries are
development references only.

Empty, missing-label, singleton, and two-node selections return all 16 rows
with zero counts. For an edgeless selection, `003` is `V choose 3` and every
other count is zero. Disconnected selections are one global census, not
component-local results. Checked `UInt64` arithmetic enforces that the sum of
all 16 counts is exactly `V choose 3`; overflow is a structured execution
error.

The exact schema is non-null `triad_type: Utf8, count: UInt64`. Metadata is
`graphforge.algorithm=triad_census`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. No node, edge, or execution-surrogate
identity is exposed. Fixed category order plus raw-UUID node and ordered-pair
normalization makes the result independent of node, edge, table, adjacency,
relationship-type, component, and storage input order. The implementation
must compute the exact census; it may not return an approximation or omit
zero-count categories.

Deterministic projection and pair normalization use
`O(V log V + A log E)` preprocessing for `V` selected nodes and `A`
direction-expanded adjacency entries. The exact census follows the
Batagelj-Mrvar sparse triad-census strategy rather than materializing every
three-node subgraph; working memory is `O(V + E)`. Shared hard limits allow at
most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints. The
census consumes exactly 16 output rows. Cancellation is checked during
projection, normalization, and counting. Validation, arithmetic, storage,
limit, cancellation, execution, and Arrow-shaping failures abort atomically
without partial rows.

Typed Rust in `graphforge-exec` owns projection, validation, normalization, exact
classification, checked counting, controls, and Arrow shaping. Python and Node
only adapt arguments and native Arrow/Arrow IPC. The topology-only result is
unchanged by knowledge-layer presence, preserving the graph/knowledge boundary. No
igraph, NetworkX, Python runtime, external graph service, runtime fallback,
packaging dependency, or recovery path participates.

#### Canonical `articulation_points` contract

`articulation_points` returns every selected node whose removal increases the
number of connected components in the selected undirected multigraph. It
requires `directed=False`; `directed=True`, including the public default, is a
structured validation error. The algorithm is unweighted and rejects any
`weight`.

An optional `label` selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid `via` selects one
type. Rust projects the selected adjacency as undirected and collapses only the
two mirrored entries that share one stored `edge_uuid`. Distinct stored edges
retain distinct identities, so parallel and reciprocal edges remain
independent. Self-loops are ignored. Low-link traversal tracks the parent
_edge_ rather than only the parent node, preserving correct multigraph
semantics.

Empty and label-empty selections, isolated nodes, and components without a cut
vertex return the typed zero-row result. Disconnected selections are processed
component by component. Traversal roots, adjacency, and final rows use ascending
raw public UUID order, making results deterministic across repeated calls and
all bindings. The exact non-null schema is
`node_uuid: FixedSizeBinary(16)`, with
`graphforge.algorithm=articulation_points` and `graphforge.verb=analyze`
metadata. Internal execution surrogates never escape.

The explicit-stack low-link traversal is stack-safe and runs in `O(V + E)`
time with `O(V + E)` working memory after deterministic projection ordering;
sorting the projection adds `O(V log V + E log E)` preprocessing overhead.
Shared limits allow at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. Projection, traversal, overflow, limit,
cancellation, execution, and Arrow-shaping failures abort atomically without
partial output.

Catalog dispatch, projection, low-link execution, ordering, limits,
cancellation, and Arrow shaping are Rust-owned in `graphforge-exec`. Python and Node
only adapt arguments and the native Arrow result. The topology-only result is
unchanged by knowledge-layer presence, preserving the topology-only knowledge isolation boundary. No external
graph runtime, fallback, service, packaging dependency, or recovery path
participates.

#### Canonical `bridges` contract

`bridges` returns every stored edge whose removal increases the number of
connected components in the selected undirected multigraph. It requires
`directed=False`; the public `directed=True` default and every `weight` are
structured validation errors. An optional `label` selects the node-induced
graph. `via=None` includes every relationship type between selected nodes,
while a valid `via` selects one type.

Rust collapses only the two mirrored adjacency entries sharing one stored
`edge_uuid`. Distinct parallel and reciprocal stored edges remain independent,
so no member of a parallel pair is a bridge. Self-loops are ignored, and
low-link traversal tracks parent edge identity. Output endpoints are
canonicalized so `source_uuid < target_uuid`; rows sort by `source_uuid`,
`target_uuid`, then `edge_uuid`, all ascending.

The exact non-null schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, with
`graphforge.algorithm=bridges` and `graphforge.verb=analyze` metadata. Empty,
label-empty, isolate-only, edgeless, and bridge-free selections return this
typed zero-row result. Disconnected selections return bridges from every
component. Internal surrogates never escape.

The explicit-stack low-link traversal is stack-safe and `O(V + E)` time with
`O(V + E)` working memory after projection; deterministic projection and row
ordering add `O(V log V + E log E)` sorting overhead. Shared limits allow at
most 10,000,000 selected nodes, 100,000,000 direction-expanded adjacency
entries, 10,000,000 output rows, and 10,000 cooperative work checkpoints.
Projection, traversal, shaping, limit, and cancellation failures abort
atomically without partial output.

Rust in `graphforge-exec` owns projection, low-link execution, ordering, limits,
cancellation, and Arrow shaping. Python and Node only adapt arguments and the
native result. Knowledge-layer presence cannot change this topology-only
result, preserving the topology-only knowledge isolation boundary.
No external graph runtime, fallback, service, packaging dependency, or
recovery path participates.

#### Canonical `triangle_count` contract

`triangle_count` returns the exact number of unordered three-node cliques in
the complete selected undirected simple graph. It requires
`directed=False`; `directed=True`, including the public default, is a
structured validation error. The algorithm is unweighted and rejects every
`weight`. `label=None` selects every node, a valid label selects the
node-induced graph, `via=None` includes every relationship type, and a valid
`via` selects one type.

Rust canonicalizes each edge's endpoint UUIDs. Mirrored adjacency entries with
the same stored `edge_uuid` collapse, and inconsistent reuse of one edge UUID
is an error. Distinct parallel and reciprocal stored edges collapse to one
simple undirected neighbor relation. Self-loops are ignored. Consequently,
mirrors, parallel edges, reciprocal edges, and loops cannot multiply a
triangle; each unordered node triple contributes exactly one.

The operation always returns exactly one non-null row. Empty, label-empty,
edgeless, isolate-only, acyclic, and otherwise triangle-free selections return
`0`. Disconnected selections contribute the checked sum of triangles from
every component. Sorting nodes by raw public UUID and using ordered neighbor
sets makes the scalar independent of table, node, edge, adjacency, component,
and relationship-type input order.

The exact schema is `triangle_count: UInt64` with
`graphforge.algorithm=triangle_count`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. Counting uses checked
`UInt64` increments; exceeding the supported range is a structured execution
error rather than a wrapped count. Duplicate node UUIDs, endpoints outside the
selected node set, malformed identity, projection failure, and shaping failure
also abort without output.

Let `V` be selected nodes and `E` distinct undirected simple edges after
normalization. Projection and ordered-set construction use
`O(V log V + E log V)` time and `O(V + E)` memory. Ordered wedge enumeration
costs
`O(sum((u,v) in E, u < v) d+(v) log d(u))`, where `d+` counts higher-UUID
neighbors; the dense worst case is `O(V^3 log V)`. Shared limits allow at most
10,000,000 selected nodes, 100,000,000 direction-expanded adjacency entries,
10,000,000 output rows, and 10,000 cooperative work checkpoints. Projection,
normalization, counting, checked arithmetic, limit, cancellation, execution,
and Arrow-shaping failures are atomic and return no partial scalar.

Typed Rust in `graphforge-exec` owns projection, normalization, exact counting,
deterministic ordering, controls, and Arrow shaping. Python and Node only adapt
arguments and the native Arrow result. They contain no triangle algorithm,
external graph runtime, fallback, service, packaging dependency, or recovery
path. Knowledge-layer presence cannot change this topology-only result,
preserving the topology-only knowledge isolation boundary.

#### Canonical `dyad_census` contract

`dyad_census` classifies every unordered pair of distinct selected nodes by
its directed-edge presence. It requires `directed=True`; `directed=False` is a
structured validation error. The operation is unweighted and rejects every
`weight`. `label=None` selects every node, a valid label selects the
node-induced graph, `via=None` includes every relationship type, and a valid
`via` selects one type. This is a two-node dyad census, not the separate
three-node induced-subgraph classification performed by `triad_census`.

The result always contains exactly three rows in this order:

1. `mutual` — at least one selected edge exists in each direction.
2. `asymmetric` — selected edges exist in exactly one direction.
3. `null` — no selected edge exists in either direction.

Each unordered pair contributes to exactly one row. Self-loops are ignored
because a dyad contains two distinct nodes. Identical adjacency entries and
parallel stored edges with the same ordered endpoints collapse to presence;
edge multiplicity never increases a count. At least one edge in each
direction makes the pair mutual, regardless of the number or identity of
those reciprocal edges.

Empty, label-empty, and singleton selections return all three rows with count
`0`. For every edgeless selection, `mutual` and `asymmetric` are `0` and
`null` is the checked number of unordered node pairs. The checked sum of the
three counts is always `V * (V - 1) / 2`. Raw UUID ordering and fixed category
ordering make the result independent of node, edge, table, adjacency,
relationship-type, and component input order.

The exact non-null schema is `dyad_type: Utf8, count: UInt64`, with
`graphforge.algorithm=dyad_census`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. Duplicate node UUIDs,
endpoints outside the selection, malformed identity, and inconsistent edge
identity are structured errors; internal execution surrogates never escape.
Pair totals and category increments use checked arithmetic.

Let `A` be direction-expanded adjacency entries and `E` distinct ordered
non-loop endpoint pairs. Deterministic projection and normalization use
`O(V log V + A log E)` time and `O(V + E)` memory; category counting is
`O(E log E)` time. Shared limits allow at most 10,000,000 selected nodes,
100,000,000 direction-expanded adjacency entries, 10,000,000 output rows, and
10,000 cooperative work checkpoints. Projection, normalization, counting,
checked arithmetic, limit, cancellation, execution, and Arrow-shaping
failures abort atomically without partial rows.

Typed Rust in `graphforge-exec` owns projection, normalization, exact counting,
deterministic ordering, controls, and Arrow shaping. Python and Node only adapt
arguments and the native Arrow result; neither contains a census algorithm.
Knowledge-layer presence cannot affect this topology-only result, preserving the topology-only knowledge isolation boundary. The
[igraph dyad-census reference](https://igraph.org/c/html/develop/igraph-Motifs.html#igraph-dyad-census)
defines the three presence categories and is a development reference only. No
external graph runtime, fallback, service, packaging dependency, or recovery
path participates.

### Node Embeddings (`by=`)

These shipped Rust handlers follow the
[authoritative v1 contracts](embedding-v1.md) for fixed-width Arrow results,
deterministic execution, and the graph/knowledge-layer boundary. Python and
Node are thin adapters over the same native implementations.

| Value | Algorithm | Source reference |
| ------------------------ | -------------------------------------------------------------------------- | ----------------------------------------- |
| `node2vec` | [Node2Vec random-walk embedding](embedding-v1.md#node2vec) | [paper](https://arxiv.org/abs/1607.00653) |
| `graphsage` | [GraphSAGE inductive embedding](embedding-v1.md#graphsage) | [paper](https://arxiv.org/abs/1706.02216) |
| `fast_random_projection` | [Fast random projection embedding](embedding-v1.md#fast-random-projection) | [paper](https://arxiv.org/abs/1908.11512) |
| `hashgnn` | [HashGNN feature-based embedding](embedding-v1.md#hashgnn) | [paper](https://arxiv.org/abs/2105.14280) |

All four handlers are read-only. They return canonical UUID/vector Arrow data
and a neutral invocation descriptor; they do not write a vector property,
publish a search artifact, start a provider request, or refresh an embedding space.
Publishing a complete result is an explicit operation governed by the
[embedding-space publication contract](embedding-v1.md#embedding-space-publication).
That operation preserves the algorithm/version and graph projection in the
space compatibility identity. Multiple spaces may contain the same UUID, and
neither search freshness nor knowledge/epistemic run state changes the algorithm result.

---

## `forge.similar()` — Pairwise Similarity

Node and subgraph similarity algorithms return a pairwise Arrow Table
(`node1_uuid: FixedSizeBinary(16)`, `node2_uuid: FixedSizeBinary(16)`,
`similarity: Float64`). Exposed via `forge.similar()`; `similar` is read-only and
does not accept `directed` or `write_property`.

```python
forge.similar(label, by="node_similarity", via="KNOWS", k=10)
forge.similar(label, by="filtered_node_similarity", via="KNOWS", k=10)
forge.similar(label, by="knn", vector_property="embedding", k=10)
forge.similar(label, by="cosine", vector_property="embedding", k=10)
forge.similar(
    label,
    by="filtered_knn",
    via="KNOWS",
    vector_property="embedding",
    k=10,
)
```

The Rust `node_similarity` handler computes unweighted Jaccard similarity over
distinct outgoing-neighbor sets selected by `label` and optional `via`.
Parallel edges collapse; self-loops remain ordinary neighbor membership;
properties, NULL values, and knowledge-layer presence do not alter topology.
For each non-empty source, it emits positive reciprocal candidates independently,
orders them by score descending then target topology order, and keeps the first
positive `k` (default 10). Source groups remain in topology order. Empty inputs,
empty neighborhoods, and zero-overlap pairs emit no rows with the same schema.
`k=0`, `vector_property`, malformed selectors, and unavailable handlers produce
structured errors. Shared Rust graph/output limits and cooperative cancellation
bound execution.

The Rust `filtered_node_similarity` handler uses the same exact unweighted
Jaccard neighborhoods, then admits a target only when the source has a direct
outgoing selected relationship to it. `via` selects the relationship type for
both neighborhoods and candidate eligibility; `via=None` admits all types.
Parallel relationships collapse, self-candidates are excluded while self-loops
remain ordinary neighborhood membership, weights are ignored, and reciprocal
rows are independently eligible only when both outgoing relationships exist.

Only positive scores are emitted. Eligible targets are ordered by score
descending and target topology order before positive `k` (default 10) is
applied; source groups follow topology order. Empty and singleton selections,
isolates, empty neighborhoods, zero-overlap pairs, and missing relationship
types return no rows with the stable UUID schema. `vector_property` is rejected.
Shared node, edge, iteration, 10,000,000-row, and cancellation guards return
structured errors without partial results; issue 772 knowledge-layer
independence applies.

Rust solely owns topology projection, exact scoring, validation, ordering,
limits, and Arrow shaping; Python and Node remain thin bindings. The
[Neo4j GDS Filtered Node Similarity documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/filtered-node-similarity/)
is a catalog and optional development reference only. GDS filters source and
target node sets; GraphForge exposes neither selector and does not claim that
semantic. igraph, NetworkX, Neo4j, and vector systems are not runtime backends,
fallbacks, recovery paths, or dependencies.

The Rust `knn` handler performs an exhaustive cosine comparison of every pair
of nodes selected by `label`; topology and `via` do not participate. The
required `vector_property` must exist on every selected node as a non-empty,
finite numeric list. All vectors must have the same dimension and a non-zero,
finite norm. Each node excludes itself, ranks candidates by cosine similarity
descending and target topology order, retains up to positive `k` candidates,
and emits reciprocal directions independently. Negative similarities are
omitted; zero is included. Source groups remain in topology order, so repeated
runs are deterministic. Empty and singleton selections return the stable empty
schema.

Vector loading is limited to 4,096 selected nodes and 16,777,216 total vector
cells; the shared 10,000,000-row output guard and cooperative cancellation also
apply without returning partial results. Missing, non-list, empty, ragged,
non-finite, inexact integer, zero-norm, and overflowing-norm vectors produce
structured validation or execution errors. Knowledge-layer presence does not
affect graph-native vectors or results (the issue 772 independence contract).

GraphForge exposes no public metric, cutoff, sampler, iteration, random seed,
concurrency, index, convergence, or other approximate-KNN control. The
[Neo4j GDS KNN documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/knn/)
is a catalog and optional development reference only. The exact kernel,
validation, ordering, limits, and Arrow shaping are owned solely by Rust;
Python and Node are thin bindings. igraph, NetworkX, Neo4j, and vector stores
are not runtime backends, fallbacks, recovery paths, or dependencies.

The Rust `cosine` handler performs the same exact exhaustive pair comparison
and strict vector validation as `knn`, but every finite cosine score is
eligible: positive, zero, and negative. `vector_property` is required and
`via` is rejected because relationships do not participate. Each node excludes
itself, candidates are ordered by score descending then target topology order,
and positive `k` (default 10) truncates that stable order. Reciprocal directions
are evaluated independently and source groups follow topology order.

Empty and singleton selections return the stable empty non-null UUID/UUID/
Float64 Arrow schema. Missing, non-list, empty, ragged, non-finite, inexact
integer, zero-norm, and overflowing-norm vectors return structured errors. The
4,096-node, 16,777,216-vector-cell, and 10,000,000-output-row limits and
cooperative cancellation stop without partial output. Results are independent
of topology and knowledge-layer presence under issue 772.

Rust solely owns loading, validation, scoring, ordering, limits, and Arrow
shaping; Python and Node are thin bindings. The
[Neo4j GDS similarity functions documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/similarity-functions/)
is a catalog and optional development parity reference only. GraphForge has no
igraph, NetworkX, Neo4j, SciPy, vector-store, approximate, fallback, packaging,
or recovery runtime path. Unlike `cosine`, `knn` deliberately omits negative
scores while retaining zero.

The Rust `filtered_knn` handler applies the same exact cosine comparison and
vector validation after topology eligibility is established. For each source
selected by `label`, candidates are the distinct selected nodes reachable by
an outgoing relationship of type `via`; `via=None` admits every relationship
type. Parallel relationships collapse, self-candidates are excluded, weights
are ignored, and reciprocal rows are admitted independently only when each
outgoing relationship exists. Isolates and missing relationship types produce
no rows with the stable schema.

Eligible candidates are ordered by cosine similarity descending and target
topology order, then truncated to positive `k` (default 10). Negative scores
are omitted and zero is included; source groups follow topology order. The
4,096-node, 16,777,216-vector-cell, and 10,000,000-output-row limits,
cooperative cancellation, structured errors, UUID-only Arrow schema, issue 772
independence, and sole Rust ownership are identical to `knn`. GraphForge has no
source/target-node filters, seeding, public cutoff, approximate search, or
external runtime path. The
[Neo4j GDS Filtered KNN documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/filtered-knn/)
is a catalog and optional development reference; its source/target node filters
and optional seeding are not GraphForge semantics.

| Value | Algorithm | Source reference |
| -------------------------- | -------------------------------------------- | ------------------------ |
| `node_similarity` | Exact Jaccard similarity | GDS |
| `knn` | Exact exhaustive cosine K-nearest neighbors | GDS |
| `filtered_knn` | Relationship-filtered exact cosine KNN | GDS |
| `filtered_node_similarity` | Relationship-filtered exact Jaccard | GDS |
| `cosine` | Exact exhaustive all-score cosine similarity | GDS similarity functions |

---

## Verb Summary

| Verb | Primary output column | Use when |
| ----------------------------------------------- | --------------------- | --------------------------------------------------------- |
| `forge.rank(label, by=...)` | `score: Float64` | You want every node scored |
| `forge.cluster(label, by=...)` | `community_id: Int64` | You want nodes partitioned |
| `forge.paths(source=None, target=None, by=...)` | varies by algorithm | You want routes, flows, cuts, traversals, or reachability |
| `forge.analyze(label=None, by=...)` | varies by algorithm | Graph-level or structural metrics |
| `forge.similar(label, by=...)` | `similarity: Float64` | You want pairwise node similarity |

All verbs accept `via`. Only `rank`, `cluster`, `paths`, and `analyze` accept
`directed`; only `rank` and `cluster` accept opt-in `write_property`. `similar`
instead exposes `k` and `vector_property`. All return Arrow Tables.

---

## Implementation Notes

- Typed catalog ownership and stable schemas live in `graphforge-core::algorithms`.
- Rust handler registration and dispatch live in `graphforge-exec::algorithm_dispatch` and the
  verb-specific `graphforge-exec::algorithm_*` modules.
- The shared adjacency export maps Parquet topology to internal execution IDs; the result
  shaper maps those back to UUID-only Arrow columns.
- Python and Node only coerce public arguments and convert the same Rust-produced Arrow result.
- Production dispatch has no igraph or NetworkX runtime path.

---

## References

- [API Reference](../../reference/api.md) — public verb signatures
- [Architecture Overview](overview.md) — how verbs fit into the pipeline
- [Execution Model](execution-model.md) — DataFusion integration
- OCG crate: https://docs.rs/ocg/latest/ocg/ — algorithm interface reference (Apache-2.0)
- Neo4j GDS: https://neo4j.com/docs/graph-data-science/current/algorithms/
- igraph Python API: https://igraph.org/python/api/latest/
