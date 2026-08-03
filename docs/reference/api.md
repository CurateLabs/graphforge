# API Reference

> **Checkpoint contract (v0.5.0):** The accepted Rust-owned contract for named
> checkpoints, read-only historical views, complete-workspace revert, deletion,
> retention, deterministic diff, and binding/CLI parity is
> [ADR 0014](../adr/0014-workspace-checkpoints.md), with exact request and Arrow
> schema inventory in
> [`graphforge-checkpoint-api/1`](../contracts/checkpoint-api-v1.json).

!!! note "v0.5.0 unified API"
This page documents the **v0.5.0 API**. All methods return Arrow Tables — there are no
`CypherValue` wrappers or `SearchHit` objects. The conventional instance name is `forge`,
not `db`. The Rust crate API is documented separately below.

UUID normalization and canonical Arrow/result fingerprints are identical
across Rust, Python, and Node because all three surfaces use the versioned
[canonical fingerprint contract](../book/architecture/canonical-fingerprints-v1.md)
and its shared golden vectors.

The shipped immutable provenance, assertion, confidence, evidence, capability,
and algorithm-run surfaces implement the frozen
[Immutable knowledge public API contract](../book/architecture/m20-public-api-v1.md). Their
generation, recovery, isolation, and schema contracts are summarized in the
[Immutable knowledge ledger](../book/architecture/knowledge-ledger.md).
The shipped additive epistemic status, reasoning, supersession, hypothesis,
valid-time, resolved-projection, and attachment behavior is normative in
[ADR 0006](../adr/0006-epistemic-model.md), with exact Arrow schemas in the
[Epistemic inventory](m21-schema-inventory.json).

---

## Main API

### `GraphForge` — constructor

```python
forge = GraphForge()            # in-memory, ephemeral
forge = GraphForge("path/")     # Parquet-backed, persistent
forge = GraphForge(
    "path/",
    write_mode="optimistic_multi_writer",
    write_queue_capacity=64,
    max_rebase_attempts=3,
)
```

The persistent form opens a project root that must already exist. GraphForge initializes
project files inside an existing directory; it never creates the directory itself. A missing
path fails with `StorageError: path does not exist`, and a foreign non-project directory fails
with `GF_UNSUPPORTED_PROJECT_FORMAT`, both before any mutation.

Python and Node expose the same three embedded modes: `single_writer` (the
default), `queued_writer`, and `optimistic_multi_writer`. Node passes them as a
second options object using `writeMode`, `writeQueueCapacity`, and
`maxRebaseAttempts`. Queue capacity is bounded to 1–65,536 and optimistic
rebase attempts to 0–32. Only composite transactions use optimistic replay;
other mutation APIs retain their single-writer behavior. See
[ADR 0015](../adr/0015-embedded-write-modes.md).

---

### `execute(query, params=None)` → `pyarrow.Table`

The openCypher entry point. Runs through the full compiler pipeline
(recursive-descent parser → binder → Graph IR → DataFusion) and returns an Arrow Table.

```python
table = forge.execute("MATCH (p:Person)-[:KNOWS]->(b) RETURN p.name, b.name")
```

`params` is an optional `dict` of query parameters (`$name` placeholders in the query string).
Rust callers bind graph identity with `IrLiteral::Uuid([u8; 16])`; Python callers
use `uuid.UUID`; Node callers use the exact tagged value
`{ "$uuid": "550e8400-e29b-41d4-a716-446655440000" }`. UUID-looking strings remain
ordinary strings, and malformed or non-canonical UUID tags fail before execution.

### Composite transaction request

`CompositeTransactionRequest` contract version 1 represents every graph
create/delete and property add/change/remove plus 14 explicit knowledge/epistemic participant
vectors. Created graph identities and the operation identity are caller-supplied
UUIDv7 values. Existing independent limits remain 1,000,000 rows per knowledge
or provenance participant, 65,536 reasoning-content bytes, and 1,024 hypothesis
question-key bytes. Every graph mutation and explicit participant row counts
once toward the separate 100,000-entry aggregate cap.

`authorize_composite_transaction(request, snapshot, prior)` composes
request-shape, ontology, identity, graph reference, participant-reference, and
idempotency decisions against a [`CompositeValidationSnapshot`] into one
canonical Arrow receipt from `composite_receipt_schema()` without staging
participants or publishing a generation. The singleton receipt row carries:

| Field | Type | Notes |
| --- | --- | --- |
| `request_identity` | `FixedSizeBinary(16)` | Caller operation UUID |
| `transaction_uuid` | `FixedSizeBinary(16)` | Same as `request_identity` for later publication |
| `generation_uuid` | `FixedSizeBinary(16)` | Deterministic UUID from request identity + content fingerprint |
| `content_fingerprint` | `FixedSizeBinary(32)` | Canonical composite request fingerprint |
| `contract_version` | `UInt32` | Always `1` for this vocabulary |
| `graph_mutation_count` | `UInt64` | Ordered mutation count |
| `{kind}_count` | `UInt64` | One column per entry in `COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS`; empty optional sets are `0`, never null |

Schema metadata freezes `graphforge.composite_contract_version=1`,
`graphforge.composite_kind=receipt`, `graphforge.row_order=singleton`, and
`graphforge.authorization=pre_staging`. Exact retry with the same request
identity returns the prior receipt byte-for-byte; conflicting reuse returns
`GF_IDEMPOTENCY_CONFLICT` with zero storage mutation.

`GraphForge::publish_composite_transaction(request)` runs that authorization,
then stages and publishes graph mutations with the explicit knowledge/epistemic participants
as exactly one project generation through the existing journal/`CURRENT`
recovery protocol. Validation failures before staging leave the previous
generation authoritative. Exact retry after a successful publication returns
the same receipt without restaging; conflicting identity reuse returns
`GF_IDEMPOTENCY_CONFLICT` with zero mutation.

Python projects the same contract through
`GraphForge.publish_composite_transaction(*, operation_uuid, graph_mutations,
knowledge=None, actor_uuid=None, contract_version=1)`. The binding converts
caller dicts into `CompositeTransactionRequest`, requires caller UUIDs for
every new node and edge, performs no participant inference, and returns the
canonical Arrow receipt. `graphforge.composite_provenance_uuid` exposes the
derived provenance identity used by explicit lineage and knowledge rows.

---

### Construction

Python exposes `add_nodes` / `add_edges` as language-specific container
normalizers over the Rust-owned publication methods. Node accepts canonical
Arrow IPC through `publishBulkNodes` / `publishBulkEdges`; it does not expose
`addNodes` / `addEdges` convenience methods. Both bindings therefore exercise
the same atomic publication, receipt, idempotency, and reopen contract without
duplicating normalization behavior in JavaScript.
The former Node convenience stubs were removed as an intentional compile-time
break; callers migrate directly to the two `publishBulk*` methods.

#### `add_node(label, **props)` → `NodeHandle`

Add a single node. Returns a `NodeHandle` exposing stable `.uuid` identity and
primary-label metadata without a numeric storage surrogate.

```python
alice = forge.add_node("Person", name="Alice", age=30)
```

#### `publish_bulk_nodes(operation_uuid, data)` → `pyarrow.Table`

Publish one atomic Arrow bulk node batch through the Rust-owned contract. `data`
must be a canonical Arrow Table (or Arrow-compatible container) with
`node_uuid` / `label` topology columns and bulk metadata. Returns the canonical
bulk receipt table (`entity_uuid`, `row_ordinal`, generation identity, …).

```python
receipt = forge.publish_bulk_nodes(operation_uuid, node_table)
```

#### Python: `add_nodes(label, data, *, operation_uuid)` → `pyarrow.Table`

Convenience projection onto `publish_bulk_nodes`. `data` may be an Arrow Table,
a pandas or Polars DataFrame, or a `list[dict]`. Missing `label` / `node_uuid`
columns are injected before Rust publication. Returns the same receipt table.

```python
receipt = forge.add_nodes(
    "Person",
    [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
    operation_uuid=operation_uuid,
)
```

#### `add_edge(src, rel_type, dst, **props)` → `EdgeHandle`

Add a single directed relationship. Returns an `EdgeHandle` exposing stable
`.uuid` identity and relationship-type metadata without a numeric storage surrogate.

```python
forge.add_edge(alice, "KNOWS", bob, since=2020)
```

#### `publish_bulk_edges(operation_uuid, data)` → `pyarrow.Table`

Publish one atomic Arrow bulk edge batch. Endpoint UUIDs must already exist.
Returns the canonical bulk receipt table.

```python
receipt = forge.publish_bulk_edges(operation_uuid, edge_table)
```

#### Python: `add_edges(rel_type, data, *, operation_uuid, src="src_id", dst="dst_id")` → `pyarrow.Table`

Convenience projection onto `publish_bulk_edges`. `src` / `dst` name endpoint
columns (default `src_id` / `dst_id`) and are renamed to `source_uuid` /
`target_uuid` before Rust publication.

```python
receipt = forge.add_edges(
    "KNOWS",
    edges_df,
    operation_uuid=operation_uuid,
    src="src_id",
    dst="dst_id",
)
```

---

## Analyst Verbs

All analyst verbs bypass the Cypher compiler. Implemented algorithms export the relevant
subgraph from Parquet, dispatch through the Rust algorithm registry, and return Arrow Tables.
All are read-only by default; `write_property` opts into graph mutation where supported.

Common parameters (availability varies by verb):

| Parameter | Available on | Description |
| ---------------- | ------------------------------------------------ | ------------------------------------------------------------------- |
| `via` | `rank`, `cluster`, `paths`, `analyze`, `similar` | Relationship type to traverse — `None` means all types |
| `directed` | `rank`, `cluster`, `paths`, `analyze` | Treat edges as directed (the verb signature defines the default) |
| `write_property` | `rank`, `cluster` | If set, persist the result column back to each node under this name |

Full algorithm catalog: [Algorithm Verbs](../book/architecture/algorithms.md)

---

### `rank(label, *, by, via=None, directed=True, write_property=None)` → `pyarrow.Table`

Score every node. Returns non-null `node_uuid: FixedSizeBinary(16)`, non-null
`score: Float64`, then materialized node properties.

`by="degree"`, `by="pagerank"`, `by="betweenness"`, `by="closeness"`, and
`by="harmonic_closeness"`, and `by="eigenvector"` are implemented in Rust and return rows in
deterministic topology order with non-null `node_uuid: FixedSizeBinary(16)`,
non-null `score: Float64`, then materialized node properties. Missing properties
remain Arrow NULLs; internal numeric node surrogates are never returned.

The degree score is the selected adjacency-entry count divided by
`max(selected_nodes - 1, 1)`. Directed mode (the default) counts outgoing
entries; undirected mode counts both endpoints. Parallel edges count
individually, and an undirected self-loop contributes two entries. `via=None`
selects every relationship type. When `write_property` is set, all scores are
persisted atomically only after successful execution; omitting it is read-only.

PageRank is unweighted, uses damping `0.85`, uniform initialization,
teleportation, and dangling-mass redistribution, and converges when L1 delta is
at most `selected_node_count * 1e-10`. Directed mode follows outgoing adjacency;
undirected mode exports both endpoint directions. Parallel edges contribute
independently and self-loops are retained. Empty selections return the stable
typed schema with zero rows; disconnected selections retain every node and
normalized score mass. The shared Rust node, edge, output, iteration, and
cancellation limits produce structured errors. Python and Node call this same
Rust handler without an algorithm backend or fallback of their own.

Betweenness is exact, unweighted Brandes node betweenness. For `n > 2`, scores
are normalized by `1 / ((n - 1) * (n - 2))`; selections of at most two nodes
score zero. Directed mode follows outgoing adjacency, while undirected mode
exports both endpoint directions. Parallel edges are distinct shortest-path
choices, self-loops do not shorten routes, unreachable pairs contribute zero,
and disconnected selections retain every node. Output order and floating-point
accumulation are deterministic. Empty selections return the stable typed
zero-row schema, and the shared Rust limits, cancellation, structured errors,
and atomic opt-in write-back contract apply unchanged. Python and Node contain
no separate betweenness implementation or fallback.

Closeness is exact, unweighted, and outward in directed mode. For `N` selected
nodes, `r` reachable other nodes, and finite distance sum `S`, it uses the
Wasserman-Faust value `r² / ((N - 1) * S)` when all denominators are positive,
and `0.0` otherwise. Undirected mode exports both endpoint directions. Parallel
edges and self-loops do not reduce distances; disconnected and unreachable
nodes remain finite output rows. Stable traversal order, the UUID-only schema,
empty typed results, shared Rust limits/cancellation/errors, and atomic opt-in
write-back apply unchanged. Python and Node contain no separate closeness
implementation or fallback.

HITS hub scoring is unweighted and deterministic. Authority and hub vectors
start at `1.0`; each of 20 fixed iterations computes incoming hub sums and
globally L2-normalizes authorities, then computes outgoing authority sums and
globally L2-normalizes hubs. A zero norm stays zero. Directed mode uses selected
outgoing adjacency; undirected mode exports both endpoint directions. `via`,
parallel edges, self-loops, isolated/disconnected/edgeless/empty behavior,
stable UUID-only schema/order, shared Rust limits/cancellation/errors, and
atomic opt-in write-back follow the architecture contract. Python and Node
contain no separate HITS implementation or fallback.

HITS authority scoring uses that same fixed 20-iteration Rust recurrence and
returns the final globally L2-normalized authority vector instead of the hub
vector. In directed mode each target accumulates predecessor hub scores;
undirected mode exports both endpoint directions. A zero norm stays zero.
`via`, parallel edges, self-loops, isolated/disconnected/edgeless/empty
behavior, stable UUID-only schema/order, shared limits/cancellation/errors, and
atomic opt-in write-back follow the same HITS contract. Python and Node contain
no separate authority implementation or fallback.

ArticleRank is unweighted and uses deterministic delta propagation. Every
selected node starts at `0.15`; each positive-outdegree source sends its current
delta over every selected adjacency entry divided by its outdegree plus the
selected graph's average outdegree. Targets multiply incoming messages by
`0.85`, add the result to their accumulated scores, and stop when all new deltas
are at most `1e-7` or after 20 rounds. Directed mode sends over outgoing selected
edges; undirected mode exports both endpoint directions. `via`, parallel edges,
self-loops, dangling/disconnected/edgeless/empty behavior, stable UUID-only
schema/order, shared Rust limits/cancellation/errors, and atomic opt-in
write-back follow the documented architecture contract. Python and Node contain
no separate ArticleRank implementation or fallback.

Eigenvector centrality is unweighted and uses deterministic shifted power
iteration over `Aᵀ + I`. Every selected node starts at `1 / N`; each iteration
accumulates incoming-neighbor scores, then L2-normalizes globally. Execution
stops after every component changes by at most `1e-7` or after 20 iterations,
returning the last normalized iterate. Directed mode uses incoming selected
edges; undirected mode exports both endpoint directions. Parallel edges
contribute independently, and a stored self-loop contributes in addition to the
identity shift. Disconnected components share the global norm, an edgeless
selection scores each node `1 / sqrt(N)`, and an empty selection returns the
typed zero-row schema. Stable order, UUID-only identity, shared Rust
limits/cancellation/errors, and atomic opt-in write-back apply unchanged.
Python and Node contain no separate eigenvector implementation or fallback.

Harmonic closeness is exact, normalized, unweighted, and outward in directed
mode. For `N` selected nodes, it sums `1 / distance(u, v)` over reachable
selected nodes `v != u` and divides by `N - 1`; selections of at most one node
score `0.0`. Unreachable nodes contribute zero. Undirected mode exports both
endpoint directions, while parallel edges and self-loops do not reduce
distances. Stable traversal order, the UUID-only schema, empty typed results,
shared Rust limits/cancellation/errors, and atomic opt-in write-back apply
unchanged. Python and Node contain no separate harmonic-closeness implementation
or fallback.

CELF performs deterministic Independent Cascade influence maximization in Rust.
The `rank()` surface selects the full node set, so each node's score is its
marginal expected spread at selection and all scores sum to the selected node
count. Spread uses 100 fixed simulations, activation probability `0.1`, and
live-edge decisions keyed by simulation identifier, public source UUID, and
public edge UUID. CELF lazily recomputes stale candidate gains and resolves equal
gains by public UUID; result rows remain in stable topology order.

Directed mode follows outgoing adjacency, undirected mode exports both endpoint
directions, and `via` filters before simulation. Parallel edges are independent
activation opportunities, self-loops activate no new node, disconnected spread
is additive, and an edgeless node scores `1.0`. Empty selections return the typed
zero-row schema. The shared Rust limits, cancellation, structured errors,
UUID-only schema, and atomic opt-in write-back contracts apply unchanged. Python
and Node contain no separate CELF implementation or fallback.

Clustering coefficient is deterministic and unweighted. The canonical
`clustering_coefficient` value and `local_clustering_coefficient` input alias use
one Rust handler and always emit canonical `clustering_coefficient` metadata.
Directed mode applies Fagiolo's coefficient over Boolean adjacency: self-loops are
ignored, same-direction parallel arcs collapse, reciprocal arcs remain distinct,
and total/reciprocal degree normalize directed triangles. Undirected mode exports
both endpoint directions and reduces to classic `2T / (k * (k - 1))` local
transitivity. A zero denominator scores `0.0`.

`via` filters before simplification. Disconnected, isolated, degree-one, and empty
selections retain stable finite behavior and topology order. The UUID-only schema,
materialized NULL properties, shared Rust limits/cancellation/structured errors,
and atomic opt-in write-back contracts apply unchanged. Python and Node contain no
separate clustering coefficient implementation or fallback.

Triangle count is deterministic and unweighted. `by="triangles"` counts each
distinct unordered three-node clique once for every participating node. The
rank-wide `directed` option is accepted, but edge orientation is intentionally
ignored: both modes symmetrize the selected topology and return identical scores.
This is an undirected triangle count, not a directed-motif count.

`via` filters before symmetrization. Parallel and reciprocal relationships
collapse to one adjacency, self-loops are ignored, and isolated or degree-one
nodes score `0.0`. Disconnected and empty selections preserve the stable UUID-only
Arrow schema and canonical topology order. The shared Rust limits, cancellation,
structured errors, materialized NULL properties, and atomic opt-in write-back
contracts apply unchanged. Python and Node contain no separate triangle-count
implementation or fallback.

K-core is deterministic, unweighted, and Rust-owned. `by="k_core"` returns each
selected node's maximum core number: the largest `k` for which the node belongs
to a maximal induced subgraph with minimum degree at least `k`. Although scores
are exact integers, the stable rank schema represents them as `Float64`.

The rank-wide `directed` option is accepted but intentionally ignored: both
modes use the corresponding undirected simple graph and return identical scores.
`via` filters before symmetrization, parallel and reciprocal relationships
collapse to one adjacency, self-loops are ignored, and weights do not apply.
Disconnected components peel independently, isolated or edgeless nodes score
`0.0`, and empty selections preserve the typed zero-row schema.

Results remain in canonical topology order with UUID-only public identity:
`node_uuid: FixedSizeBinary(16)`, `score: Float64`, then materialized node
properties. Shared Rust limits, cancellation, and structured errors apply;
`write_property` atomically persists all core numbers only after successful
execution. Python and Node contain no separate k-core implementation or fallback.

Preferential attachment is deterministic, unweighted, and Rust-owned.
`by="preferential_attachment"` starts with the pair score
`PA(u, v) = degree(u) * degree(v)`, then returns one score per selected node by
summing that value over every other node to which the node does not already
link. Self-pairs and existing links are excluded. This per-node missing-link
aggregate preserves the rank result contract; it is not a table of candidate
node pairs.

With `directed=True`, degree and missing links use distinct outgoing neighbors
and absent outgoing arcs. With `directed=False`, they use the corresponding
undirected simple graph. `via` filters relationships before normalization.
Same-direction parallel relationships collapse, reciprocal relationships remain
distinct only in directed mode, self-loops are ignored, and weights do not
apply. Candidate nodes in different disconnected components remain eligible.
Isolated, complete, edgeless, singleton, and empty selections therefore have
well-defined zero scores where no positive candidate contribution exists.

The Rust implementation uses checked integer sums and products and returns an
exact `Float64` score only while it is at most `2^53`; larger values produce a
structured execution error rather than a rounded result. Results remain in
canonical topology order with UUID-only public identity:
`node_uuid: FixedSizeBinary(16)`, `score: Float64`, then materialized node
properties. Shared Rust limits and cancellation apply, and `write_property`
atomically persists the complete result only after successful execution. Python
and Node only translate arguments and Arrow IPC; neither provides an algorithm
backend, fallback, packaging dependency, or recovery path.

Adamic-Adar is deterministic, unweighted, and Rust-owned. Its pair score is
`AA(u, v) = sum(1 / ln(degree(w)))` over the shared neighbors `w` of `u` and
`v`. `rank()` returns one score per selected node by summing `AA(u, v)` over
every missing link to another selected node; self-pairs and existing links are
excluded. This aggregation preserves the stable rank schema rather than
returning candidate pairs.

With `directed=True`, shared neighbors are common outgoing successors, missing
links are absent outgoing arcs, and each successor is discounted by its distinct
selected in-degree. With `directed=False`, the corresponding symmetric simple
graph supplies both neighborhoods and discount degrees. `via` filters before
normalization. Same-direction parallel relationships collapse, reciprocal arcs
remain distinct only in directed mode, self-loops are ignored, and weights do
not apply. Candidates may cross components, although they score zero without a
shared neighbor. Complete, disconnected, edgeless, singleton, and empty
selections retain deterministic finite boundary behavior.

Neighbor contributions and node aggregates use stable topology order and
compensated `Float64` summation; any non-finite result is a structured execution
error. Results expose only `node_uuid: FixedSizeBinary(16)`, `score: Float64`,
then materialized node properties. Shared Rust limits and cancellation apply,
and `write_property` atomically persists every score only after successful
execution. Python and Node only translate arguments and Arrow IPC; neither
contains an algorithm backend, fallback, packaging dependency, or recovery path.

Common neighbors is deterministic, unweighted, and Rust-owned. Its pair score
is `CN(u, v) = |N(u) intersect N(v)|`. `rank()` returns one score per selected
node by summing `CN(u, v)` over every missing link to another selected node;
self-pairs and existing links are excluded. This aggregation is why the result
uses the stable rank schema instead of returning candidate pairs.

With `directed=True`, neighborhoods are distinct outgoing successors and
candidates are absent outgoing arcs. With `directed=False`, both use the
corresponding symmetric simple graph. `via` filters before normalization.
Same-direction parallel relationships collapse, reciprocal arcs remain distinct
only in directed mode, self-loops are ignored, and weights do not apply.
Candidates may cross components, although they score zero without a shared
neighbor. Complete, disconnected, edgeless, singleton, and empty selections
retain deterministic zero/boundary behavior.

Pair counts and aggregates use checked integer arithmetic and become `Float64`
only when exactly representable at or below `2^53`; larger results produce a
structured execution error. Results expose only
`node_uuid: FixedSizeBinary(16)`, `score: Float64`, then materialized node
properties. Shared Rust limits and cancellation apply, and `write_property`
atomically persists every score only after successful execution. Python and
Node only translate arguments and Arrow IPC; neither contains an algorithm
backend, fallback, packaging dependency, or recovery path.

Resource allocation is deterministic, unweighted, and Rust-owned. Its pair
score is `RA(u, v) = sum(1 / degree(w))` over the shared neighbors `w` of `u`
and `v`. `rank()` returns one score per selected node by summing `RA(u, v)` over
every missing link to another selected node; self-pairs and existing links are
excluded. This aggregation preserves the stable rank schema rather than
returning candidate pairs.

With `directed=True`, shared neighbors are common outgoing successors, missing
links are absent outgoing arcs, and each successor is discounted by its distinct
selected in-degree. With `directed=False`, the corresponding symmetric simple
graph supplies both neighborhoods and ordinary degree. `via` filters before
normalization. Same-direction parallel relationships collapse, reciprocal arcs
remain distinct only in directed mode, self-loops are ignored, and weights do
not apply. Candidates may cross components, although they score zero without a
shared neighbor. Complete, disconnected, edgeless, singleton, and empty
selections retain deterministic finite boundary behavior.

Neighbor contributions and node aggregates use stable topology order and
compensated `Float64` summation; any non-finite discount or score is a
structured execution error. Results expose only
`node_uuid: FixedSizeBinary(16)`, `score: Float64`, then materialized node
properties. Shared Rust limits and cancellation apply, and `write_property`
atomically persists every score only after successful execution. Python and
Node only translate arguments and Arrow IPC; neither contains an algorithm
backend, fallback, packaging dependency, or recovery path.

Total neighbors is deterministic, unweighted, and Rust-owned. Its pair score
is `TN(u, v) = |N(u) union N(v)|`, the Jaccard-similarity denominator.
`rank()` returns one score per selected node by summing `TN(u, v)` over every
missing link to another selected node; self-pairs and existing links are
excluded. This aggregation preserves the stable rank schema rather than
returning candidate pairs.

With `directed=True`, neighborhoods are distinct outgoing successors and
candidates are absent outgoing arcs. With `directed=False`, both use the
corresponding symmetric simple graph. `via` filters before normalization.
Same-direction parallel relationships collapse, reciprocal arcs remain distinct
only in directed mode, self-loops are ignored, and weights do not apply.
Candidates across disconnected components can contribute positively because a
union does not require shared neighbors. An isolated node can likewise receive
a positive aggregate when another selected node has neighbors. Complete and
edgeless graphs, singleton selections, and empty selections retain deterministic
zero/boundary behavior.

Union counts and aggregates use checked integer arithmetic and become `Float64`
only when exactly representable at or below `2^53`; larger results produce a
structured execution error. Results expose only
`node_uuid: FixedSizeBinary(16)`, `score: Float64`, then materialized node
properties. Shared Rust limits and cancellation apply, and `write_property`
atomically persists every score only after successful execution. Python and
Node only translate arguments and Arrow IPC; neither contains an algorithm
backend, fallback, packaging dependency, or recovery path.

**Centrality** (`by=`):
`pagerank`, `betweenness`, `closeness`, `harmonic_closeness`, `degree`,
`eigenvector`, `article_rank`, `hits_hub`, `hits_authority`, `celf`

**Structural** (`by=`):
`clustering_coefficient`, `triangles`, `k_core`

`local_clustering_coefficient` is accepted as a non-canonical input alias for
`clustering_coefficient`; it is not a separate catalog value.

**Link prediction** (`by=`):
`preferential_attachment`, `adamic_adar`, `common_neighbors`,
`resource_allocation`, `total_neighbors`

```python
table = forge.rank("Person", by="degree")
page_rank = forge.rank("Person", by="pagerank", via="KNOWS")
between = forge.rank("Person", by="betweenness", via="KNOWS")
close = forge.rank("Person", by="closeness", via="KNOWS")
harmonic = forge.rank("Person", by="harmonic_closeness", via="KNOWS")
eigenvector = forge.rank("Person", by="eigenvector", via="KNOWS")
article = forge.rank("Person", by="article_rank", via="KNOWS")
hub = forge.rank("Person", by="hits_hub", via="KNOWS")
authority = forge.rank("Person", by="hits_authority", via="KNOWS")
influence = forge.rank("Person", by="celf", via="KNOWS")
clustering = forge.rank("Person", by="clustering_coefficient", via="KNOWS")
triangles = forge.rank("Person", by="triangles", via="KNOWS")
cores = forge.rank("Person", by="k_core", via="KNOWS")
attachment = forge.rank("Person", by="preferential_attachment", via="KNOWS")
adamic = forge.rank("Person", by="adamic_adar", via="KNOWS")
common = forge.rank("Person", by="common_neighbors", via="KNOWS")
resource = forge.rank("Person", by="resource_allocation", via="KNOWS")
total = forge.rank("Person", by="total_neighbors", via="KNOWS")
forge.rank("Person", by="degree", write_property="degree_score")
forge.rank("Person", by="pagerank", write_property="page_rank")
forge.rank("Person", by="betweenness", write_property="betweenness_score")
forge.rank("Person", by="closeness", write_property="closeness_score")
forge.rank("Person", by="harmonic_closeness", write_property="harmonic_score")
forge.rank("Person", by="eigenvector", write_property="eigenvector_score")
forge.rank("Person", by="article_rank", write_property="article_score")
forge.rank("Person", by="hits_hub", write_property="hub_score")
forge.rank("Person", by="hits_authority", write_property="authority_score")
forge.rank("Person", by="celf", write_property="influence_gain")
forge.rank("Person", by="clustering_coefficient", write_property="clustering")
forge.rank("Person", by="triangles", write_property="triangle_count")
forge.rank("Person", by="k_core", write_property="core_number")
forge.rank("Person", by="preferential_attachment", via="KNOWS", write_property="attachment")
forge.rank("Person", by="adamic_adar", via="KNOWS", write_property="adamic_score")
forge.rank("Person", by="common_neighbors", via="KNOWS", write_property="common_score")
forge.rank("Person", by="resource_allocation", via="KNOWS", write_property="resource_score")
forge.rank("Person", by="total_neighbors", via="KNOWS", write_property="total_score")
forge.rank("Person", by="degree", via="KNOWS", directed=False)
```

---

### `cluster(label, *, by, vector_property=None, via=None, directed=False, write_property=None)` → `pyarrow.Table`

Assign community membership. Returns non-null `node_uuid: FixedSizeBinary(16)`,
non-null `community_id: Int64`, then materialized node properties.

Community detection defaults to undirected (`directed=False`).

`by="components"` is implemented in Rust and returns weakly connected components in
deterministic topology order with `node_uuid: FixedSizeBinary(16)`,
`community_id: Int64`, then materialized node properties. Missing properties remain
Arrow NULLs; internal numeric node surrogates are never returned. Component IDs are
zero-based and assigned by the first topology-ordered node seen in each component.

`via=None` selects every relationship type; otherwise only the named type contributes
edges. Both directed and undirected exports produce weak membership for their selected
edge set, so edge orientation does not change the result. Parallel edges and self-loops
do not change membership, and every isolated node forms its own component. When
`write_property` is set, all component IDs are persisted atomically only after successful
execution; omitting it is read-only.

`by="louvain"` performs deterministic, unweighted classic multilevel Louvain
in Rust at resolution `1.0`. Each level applies topology-ordered local moves
whose modularity gain exceeds `1e-12`, resolves equal gains by the smallest
topology representative, condenses the communities, and repeats until stable.
Final `community_id` values are consecutive and ordered by the first original
node in each community.

`via` filters before an undirected simple projection is built. Parallel and
reciprocal relationships collapse, self-loops are ignored, and relationship
properties are not weights. `directed=True` is accepted but intentionally has
the same classic partition semantics as `directed=False`. Disconnected
components remain separate; isolated and edgeless nodes are singleton
communities, while empty selections preserve the typed zero-row schema.

The stable result is `node_uuid: FixedSizeBinary(16)`, `community_id: Int64`,
then materialized properties; numeric surrogates never escape. Shared Rust
limits, bounded cancellation checkpoints, and structured errors apply.
`write_property` atomically persists the whole partition only after success.
Python and Node only translate arguments and Arrow IPC and contain no algorithm
backend, fallback, packaging dependency, or recovery path.

`by="leiden"` performs deterministic, unweighted classic Leiden in Rust at
resolution `1.0`. Each level applies topology-ordered positive-gain local
moves, refines coarse communities into connected subcommunities, and aggregates
the refined partition with the coarse partition seeding the next level.
Positive refinement moves use a fixed `0.01` temperature and a fixed Rust
pseudo-random stream, so repeated calls and all bindings return the same
partition without exposing a seed option.

`via` filters before an undirected simple projection is built. Parallel and
reciprocal relationships collapse, self-loops are ignored, relationship
properties are not weights, and `directed=True` intentionally has the same
semantics as `directed=False`. Disconnected components never merge; final
communities are connected; isolated and edgeless nodes are singleton
communities; and empty selections preserve the typed zero-row schema.

The stable result is `node_uuid: FixedSizeBinary(16)`, `community_id: Int64`,
then materialized properties, with consecutive IDs ordered by the first
original node in each community. Shared Rust limits, bounded cancellation
checkpoints, structured numeric errors, UUID-only identity, and atomic opt-in
`write_property` apply. Python and Node remain thin Arrow IPC bindings with no
algorithm backend, fallback, packaging dependency, or recovery path.

`by="girvan_newman"` performs the deterministic, unweighted algorithm of
[Girvan and Newman](https://doi.org/10.1073/pnas.122653799) in Rust. It computes
all-shortest-path edge betweenness, removes one highest-scoring edge, and
recomputes scores after every removal; equal scores choose the
lexicographically smallest original topology endpoint pair. The scalar cluster
API does not expose a dendrogram or target community count. Rust records the
initial connected-component partition and every split, then returns the level
with maximum classic modularity at resolution `1.0` against the original graph,
keeping the earlier, coarser level when values are equal within `1e-12`.

`via` filters before an undirected, unweighted simple projection. Parallel and
reciprocal relationships collapse, self-loops are ignored, properties are not
weights, and `directed=True` has the same semantics as `directed=False`.
Initially disconnected components never merge; isolates and edgeless nodes are
singletons; and empty selections preserve the typed zero-row schema. UUID-only
identity, consecutive stable community IDs, materialized properties, shared
limits, bounded cancellation, structured Rust errors, and atomic opt-in
`write_property` apply. Python and Node remain thin Arrow IPC bindings with no
algorithm backend, fallback, packaging dependency, or recovery path.

`by="modularity_optimization"` performs the deterministic, single-level local
modularity optimization defined by
[Neo4j GDS](https://neo4j.com/docs/graph-data-science/current/algorithms/modularity-optimization/)
in Rust, using classic
[Newman-Girvan modularity](https://doi.org/10.1103/PhysRevE.69.026113) at
resolution `1.0`. Nodes start as singleton communities. Topology-ordered
asynchronous sweeps accept the best neighboring-community gain above `1e-12`,
with equal gains choosing the smallest topology representative, and stop after
a full sweep makes no moves.

This is the direct local-move result: unlike `louvain`, it does not condense
communities, create supergraphs, or repeat over a hierarchy. It adds no public
resolution, tolerance, iteration, weight, or seed option. `via` filters an
undirected, unweighted simple projection; reciprocal and parallel edges
collapse, loops are ignored, and `directed=True` is direction-insensitive.
Disconnected components never merge, isolates and edgeless nodes are
singletons, and empty selections preserve the typed zero-row schema.

The result contains UUID-only identity, consecutive stable community IDs, and
materialized properties. Shared limits, bounded cancellation, structured Rust
errors, and atomic opt-in `write_property` apply. Rust is the sole production
owner; Python and Node remain thin Arrow IPC bindings with no algorithm
backend, fallback, packaging dependency, or recovery path.

`by="fastgreedy"` performs the deterministic, unweighted
Clauset-Newman-Moore algorithm defined by
[igraph](https://r.igraph.org/reference/cluster_fast_greedy.html) and the
[original paper](https://doi.org/10.1103/PhysRevE.70.066111) in Rust. Nodes
start as singleton communities. Rust incrementally maintains community
adjacency, degrees, and modularity gains while repeatedly merging the
edge-adjacent pair with greatest gain at resolution `1.0`, even when the best
remaining gain is negative. Gains equal within `1e-12` choose the
lexicographically smallest original representative pair.

The full within-component hierarchy remains internal. GraphForge returns the
recorded maximum-modularity partition, with a tie within `1e-12` retaining the
earlier, finer level. It exposes no dendrogram, merge matrix, weight,
resolution, tolerance, seed, target-community-count, or other Fast Greedy
option. `via` filters an undirected, unweighted simple projection; reciprocal
and parallel edges collapse, loops are ignored, and `directed=True` is
direction-insensitive. Disconnected components never merge, isolates and
edgeless nodes are singletons, and empty selections preserve the typed
zero-row schema.

The result is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized properties, with consecutive IDs in
first-node order. Shared limits, bounded cancellation, structured Rust errors,
and atomic opt-in `write_property` apply. Rust is the sole production owner;
Python and Node remain thin Arrow IPC bindings with no algorithm backend,
fallback, packaging dependency, or recovery path.

`by="infomap"` performs the deterministic two-level map equation of
[Rosvall and Bergstrom](https://doi.org/10.1073/pnas.0706851105), following the
[MapEquation definition](https://www.mapequation.org/infomap/how-it-works/) and
[igraph reference](https://r.igraph.org/reference/cluster_infomap.html),
entirely in Rust. Nodes begin in singleton modules. Topology-ordered node moves
and adjacent-module merges accept only codelength improvements above `1e-12`;
ties choose the smallest original representative or representative pair. One
fixed deterministic trial is run.

`via` filters a simple, unweighted projection; parallel edges collapse and
loops are ignored. `directed=False` uses the undirected degree-stationary walk.
`directed=True` preserves orientation and uses uniform `0.15` teleportation,
uniform dangling redistribution, and a `1e-12` stationary tolerance. Weak
components are independent, isolates and edgeless nodes are singletons, and
empty selections preserve the typed zero-row schema.

No public edge/vertex weight, teleportation, trials, seed, codelength,
hierarchy, overlap, regularization, or algorithm-specific option is added. The
result is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized properties, with stable consecutive
IDs. Shared limits, bounded cancellation, structured Rust errors, and atomic
opt-in `write_property` apply. Rust is the sole production owner; Python and
Node remain thin Arrow IPC bindings with no backend, fallback, packaging
dependency, or recovery path.

`by="leading_eigenvector"` implements Newman's recursive modularity bisection
in Rust, following the
[igraph contract](https://r.igraph.org/reference/cluster_leading_eigen.html)
and [original paper](https://doi.org/10.1103/PhysRevE.74.036104). Each current
community is split by the signs of the leading eigenvector of its generalized
modularity matrix. Rust accepts a split only when its leading eigenvalue and
classic modularity gain both exceed `1e-12`.

The deterministic built-in Jacobi solver uses matrix-order ties, tolerance
`1e-12`, and stable eigenvector orientation. Its dense matrix path is limited
to 4,096 selected nodes. `via` filters an undirected, unweighted simple
projection; parallel and reciprocal edges collapse, loops are ignored, and
both `directed` values symmetrize. Components remain independent, isolates and
edgeless nodes are singletons, and empty selections preserve the typed schema.

GraphForge adds no public weights, steps, starting membership, ARPACK options,
callback, resolution, seed, tolerance, eigenvalue/vector output, merges,
dendrogram, hierarchy, or algorithm-specific option. Results use the stable
UUID-only cluster schema and consecutive first-node-ordered IDs. Shared limits,
cancellation, structured Rust errors, and atomic opt-in `write_property`
apply. Rust is the sole production owner; Python and Node remain thin Arrow
IPC bindings with no backend, fallback, packaging dependency, or recovery
path.

`by="walktrap"` implements the Pons-Latapy random-walk algorithm entirely in
Rust, following the
[igraph contract](https://r.igraph.org/reference/cluster_walktrap.html) and
[original paper](https://jgaa.info/index.php/jgaa/article/view/paper124).
Rust uses fixed four-step transition distributions and the paper's
degree-normalized community distance. Adjacent communities merge by minimum
Ward increase; `1e-12` cost ties choose the smallest original representative
pair. The returned partition is the recorded maximum-classic-modularity level,
with a modularity tie retaining the earlier, finer partition.

`via` filters an undirected, unweighted simple projection. Reciprocal and
parallel edges collapse, loops are ignored, and both `directed` values
symmetrize. Components remain independent, isolates and edgeless nodes are
singletons, and empty selections preserve the typed schema. The dense
transition path is limited to 4,096 selected nodes.

GraphForge adds no public edge weights, walk `steps`, target cluster count,
merges, modularity series, dendrogram, hierarchy, seed, tolerance, or
algorithm-specific option. Results use the stable UUID-only cluster schema
and consecutive first-node-ordered IDs. Shared limits, cancellation,
structured Rust errors, and atomic opt-in `write_property` apply. Rust is the
sole production owner; Python and Node remain thin Arrow IPC bindings with no
backend, fallback, packaging dependency, or recovery path.

`by="spinglass"` implements the positive-edge Reichardt-Bornholdt Potts model
entirely in Rust, following
[igraph's contract](https://r.igraph.org/reference/cluster_spinglass.html) and
the [original paper](https://doi.org/10.1103/PhysRevE.74.016110). On each
undirected simple component it uses the configuration null model
`B_ij = A_ij - k_i k_j/(2m)`, fixed `gamma = 1`, and
`H = -sum(i<j) B_ij * [spin_i = spin_j]`, with at most 25 spin states.

The asynchronous schedule is fixed at start temperature `1.0`, stop below
`0.01`, cooling factor `0.99`, and one deterministic shuffled sweep per
temperature. A built-in SplitMix64 stream with a frozen seed drives uniform
alternate-spin proposals and the Metropolis rule. Original-topology
lexicographic order resolves energy and retained-partition ties.

`via` filters an undirected, unweighted simple projection. Reciprocal and
parallel relationships collapse, loops are ignored, and both `directed`
values produce the same result. Components remain independent, isolates and
edgeless nodes are singletons, empty selections preserve the typed schema,
and dense Hamiltonian work is limited to 4,096 selected nodes.

There are no public weights, target vertex, `spins`, parallel-update flag,
temperature/cooling controls, update rule, `gamma`, negative implementation,
`gamma.minus`, seed, energy, cohesion/adhesion, hierarchy, or other Spinglass
options. Results use the stable non-null UUID-only cluster schema and
consecutive first-node-ordered IDs. Shared limits, cancellation, finite
numeric validation, structured Rust errors, and atomic opt-in
`write_property` apply. Rust is the sole production owner; Python and Node
remain thin Arrow IPC bindings with no runtime backend, fallback, packaging
dependency, or recovery path.

`by="label_propagation"` performs deterministic, unweighted classic
asynchronous label propagation in Rust. Nodes begin with unique labels. A
fixed Rust pseudo-random stream shuffles each sweep and uniformly resolves
ties among the most frequent neighbor labels while updates take effect
immediately. The stream is not a public seed option. Execution stops when
every non-isolated node holds a locally dominant neighbor label.

`via` filters before an undirected simple projection is built. Parallel and
reciprocal relationships collapse, self-loops are ignored, relationship
properties are neither weights nor seeds, and `directed=True` intentionally
has the same semantics as `directed=False`. Disconnected components never
merge; isolated and edgeless nodes are singleton communities; and empty
selections preserve the typed zero-row schema.

The stable result is `node_uuid: FixedSizeBinary(16)`, `community_id: Int64`,
then materialized properties, with consecutive IDs ordered by the first
original node in each community. Shared Rust limits, bounded cancellation
checkpoints, structured errors, UUID-only identity, and atomic opt-in
`write_property` apply. Python and Node remain thin Arrow IPC bindings with no
algorithm backend, fallback, packaging dependency, or recovery path.

`by="speaker_listener"` performs deterministic classic Speaker-Listener Label
Propagation in Rust. Unique labels initialize node memories. A fixed Rust
pseudo-random stream shuffles 100 asynchronous listener sweeps, samples each
speaker's label in proportion to its memory frequency, and resolves the
listener's most-popular received-label ties uniformly. Neither a seed nor the
sweep count is a public option.

SLPA natively finds overlapping memberships, while GraphForge's stable cluster
schema and write-back store one `community_id` per node. Rust prunes
associations below `0.05`, selects the strongest survivor, breaks equal
strengths by the smallest original topology label, and falls back to that
strongest-label rule if pruning removes all associations. Final IDs are
consecutive and ordered by the first original node in each primary community.

`via` filters before an undirected, unweighted simple projection. Parallel and
reciprocal relationships collapse, self-loops are ignored, properties are not
weights or seeds, and `directed=True` has the same semantics as
`directed=False`. Disconnected components never merge; isolates and edgeless
nodes are singleton communities; and empty selections preserve the typed
zero-row schema. UUID-only identity, materialized properties, shared limits,
bounded cancellation, structured Rust errors, and atomic opt-in
`write_property` apply. Python and Node remain thin Arrow IPC bindings with no
algorithm backend, fallback, packaging dependency, or recovery path.

`by="hdbscan"` clusters graph-native vectors entirely in Rust. Pass the name
of a required node-list property through `vector_property`; every selected
node must supply a non-empty, same-dimensional list of finite numbers. `via`
is rejected because relationships are not inputs, and `directed=True` and
`directed=False` are equivalent. Knowledge-layer files and vector indexes are
not required.

The frozen HDBSCAN* contract uses Euclidean distance,
`min_cluster_size = min_samples = 5` (the sample includes the point itself),
`alpha = 1.0`, an exact mutual-reachability minimum spanning tree, a condensed
hierarchy, EOM selection, disabled single-cluster selection, and cluster
selection epsilon `0.0`. Equal MST weights use canonical topology endpoints;
equal EOM stability selects the parent. IDs are consecutive by each cluster's
first topology node and noise is `-1`. This follows the method of
[Campello, Moulavi, Zimek, and Sander](https://doi.org/10.1145/2733381),
with [Neo4j GDS](https://neo4j.com/docs/graph-data-science/current/algorithms/hdbscan/)
as the catalog-parity reference.

The stable Arrow schema is `node_uuid: FixedSizeBinary(16)`, then
`community_id: Int64`, then materialized node properties. Empty selections
return that typed zero-row schema; fewer than five points, an all-duplicate
single-root hierarchy, and no selected cluster yield `-1` noise. Rust rejects
more than 4,096 selected nodes, more than 16,777,216 feature cells, invalid
vectors, non-finite distances, cancellation, or write failures with structured
errors. `write_property` is opt-in and atomic, including noise values.

No public min-cluster, sample, leaf-size, metric, alpha, cluster-selection,
probability, outlier, hierarchy, prediction, approximation, concurrency, or
other HDBSCAN option exists beyond `vector_property`. Rust is the sole
production owner. Python and Node are thin argument/Arrow IPC bindings with no
runtime backend, fallback, recovery path, or packaging dependency.

`by="k_means"` clusters graph-native vectors entirely in Rust. Supply the
required `vector_property`; every selected node must provide a non-empty,
same-dimensional list of finite numbers. Knowledge-layer data and vector
indexes are not required. Relationships are not inputs, so `via` is rejected
and both `directed` values are equivalent. The
[Neo4j GDS K-means documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/kmeans/)
is the catalog-parity reference.

GraphForge fixes `k = 10`. Initialization starts with the first topology point,
then repeatedly chooses the point farthest from its nearest centroid by squared
Euclidean distance; topology order breaks ties. Lloyd assignment uses squared
Euclidean distance and the lower centroid index for ties. Non-empty centroids
become arithmetic means, empty centroids retain their previous value, and exact
assignment stability is required within 100 iterations. Community IDs are
consecutive by each observed community's first topology point. Duplicate
vectors are valid and can produce fewer than ten observed communities.

The Arrow result is non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized properties. Empty selections preserve
that zero-row schema; non-empty selections below ten nodes are rejected. Rust
rejects invalid vectors, more than 4,096 selected nodes, more than 16,777,216
feature cells, non-finite numeric state, non-convergence, or cancellation with
structured errors. `write_property` is opt-in and atomic for the complete
partition. No public `k`, max-iteration, threshold, restart, seed, sampler,
seeded-centroid, distance, centroid, silhouette, concurrency, or other K-means
option exists. Rust is the only production owner; Python and Node provide no
runtime backend, fallback, recovery path, or packaging dependency.

`by="strongly_connected"` returns maximal mutually reachable node sets using a
deterministic, non-recursive Tarjan implementation in Rust. The
[Neo4j GDS SCC documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/strongly-connected-components/)
defines the catalog behavior. The official
[NetworkX non-recursive Tarjan definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.strongly_connected_components.html)
is an optional development parity reference only, not a runtime backend,
fallback, recovery path, or dependency.

`directed=True` follows relationship orientation; `directed=False` symmetrizes
the selected graph and therefore returns ordinary connected components. `via`
filters relationships. Weights and relationship properties are ignored, while
parallel edges, reciprocal edges, and self-loops leave membership unchanged.
Empty selections preserve the typed zero-row schema, isolates are singleton
components, and disconnected subgraphs remain separate. Component IDs are
consecutive in first-topology-member order, with rows in stable topology order.

Results are non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized properties. No internal topology or
Tarjan state escapes. Shared node, direction-expanded edge, output, and
cooperative-checkpoint limits apply; cancellation and all validation, shaping,
storage, and write failures are structured Rust errors. `write_property` is
opt-in and atomically commits the complete partition only after success.

There is no public concurrency, consecutive-ID toggle, relationship-weight,
seed, iteration, minimum-component-size, stats, or other SCC option;
`vector_property` is rejected. Execution remains independent of knowledge-layer
presence.
Rust is the sole production owner, and Python and Node remain thin argument and
Arrow IPC bindings with no algorithm logic or packaging dependency.

`by="biconnected"` computes the true overlapping biconnected block cover with
a deterministic, non-recursive Hopcroft-Tarjan DFS entirely in Rust. The
official
[igraph biconnected-components documentation](https://r.igraph.org/reference/biconnected_components.html)
defines the catalog behavior. The official
[NetworkX non-recursive DFS definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.biconnected_components.html)
is an optional development reference only; neither project is a runtime
backend, fallback, recovery path, dependency, or packaging input.

Membership uses an undirected, unweighted simple projection: `via` filters
relationships first, both `directed` values are equivalent, parallel and
reciprocal relationships collapse, self-loops are ignored, and relationship
properties are not weights. A dyad or bridge is a valid block. Empty selections
preserve the typed zero-row schema; edgeless nodes and isolates become singleton
primaries, and disconnected subgraphs remain independent.

The native blocks overlap at articulation vertices, while the public cluster
schema requires one scalar membership. Rust sorts each block by topology order,
orders blocks lexicographically by those member lists, and assigns each
articulation vertex to its earliest containing block. Isolates extend the cover
with deterministic singleton primaries; empty projected primaries are omitted.
Community IDs are consecutive by each observed primary group's first topology
member, and rows remain in stable topology order.

Results are non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized node properties. No topology, DFS,
edge-stack, articulation, or native-block surrogate escapes. Shared node,
direction-expanded edge, output-row, and cooperative-checkpoint limits apply;
cancellation and validation, execution, shaping, storage, and write failures
are structured Rust errors. `write_property` is opt-in and atomically commits
the complete successful primary projection, preserving existing values on any
failure.

There is no public concurrency, overlap toggle, articulation output,
relationship weight, seed, iteration, minimum block size, stats, or other
biconnected option; `vector_property` is rejected. Execution remains independent
of knowledge-layer presence per the topology-only knowledge isolation contract. Rust is the
only production owner, and Python and Node remain thin argument and Arrow IPC
bindings with no algorithm logic or packaging dependency.

`by="k_core_decomposition"` assigns every selected node its exact non-negative
core (shell) number using the same deterministic Rust Batagelj-Zaversnik peeling
kernel as `rank(by="k_core")`. The
[official igraph coreness documentation](https://r.igraph.org/reference/coreness.html)
and
[Neo4j GDS K-Core documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/k-core/)
are catalog and optional development-parity references only. igraph, NetworkX,
and Neo4j are not runtime backends, fallbacks, recovery paths, dependencies, or
packaging inputs.

The algorithm uses an undirected, unweighted simple projection. `via` filters
relationships first; `directed=True` and `directed=False` are equivalent.
Parallel and reciprocal relationships collapse, self-loops are ignored, and
relationship properties are not weights. Disconnected subgraphs are processed
independently. Empty selections preserve the typed zero-row schema, while
edgeless nodes and isolates receive core number `0`.

Results are non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized node properties, in stable topology
order. `community_id` is the exact shell number and is not renumbered into
consecutive partition IDs. No internal topology, degree, bin, position, or
peeling surrogate escapes. Shared node, direction-expanded adjacency, output,
and cooperative-checkpoint limits apply; cancellation and validation, shaping,
storage, and write failures are structured Rust errors.

`write_property` is read-only by default and atomically commits the complete
successful core-number result when opted into, preserving existing values on
any failure. There is no public concurrency, degree-threshold, seed, iteration,
degeneracy-only, component, stats, relationship-weight, or other decomposition
option; `vector_property` is rejected. Execution remains independent of
knowledge-layer presence per the topology-only knowledge isolation contract. Rust is the
sole production owner, and Python and Node remain thin argument and Arrow IPC
bindings with no algorithm implementation.

`by="approximate_max_k_cut"` computes a deterministic unweighted two-way cut
entirely in Rust. The public contract fixes `k = 2` and starts all nodes in
community `0`. Rust repeatedly moves the earliest-topology node among those
with the greatest strictly positive cut-value gain, then stops when no single
move improves the cut. This is the one-exchange rule described by the
[official NetworkX definition](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.approximation.maxcut.one_exchange.html),
used only as an optional development parity reference. The
[Neo4j GDS documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/approx-max-k-cut/)
is the catalog reference; neither project is a GraphForge runtime dependency
or fallback.

The first topology node is canonical community `0`: Rust flips the complete
binary partition if necessary. Isolates are always `0`. `via` selects
relationships, while both `directed` values intentionally ignore orientation.
Each selected parallel or reciprocal relationship contributes one unit;
self-loops contribute zero and relationship properties are not weights.
Disconnected components do not exchange gain, empty selections preserve the
typed zero-row schema, and edgeless selections return only community `0`.

Results are non-null `node_uuid: FixedSizeBinary(16)`, non-null
`community_id: Int64`, then materialized properties, in topology row order.
No internal surrogate escapes. Rust rejects more than 4,096 selected nodes,
more than 16,777,216 direction-expanded adjacency entries, or more than 10,000
improving moves with structured errors. Finite non-negative internal-weight
validation, cancellation, shared output limits, shaping, storage, and atomic
write failures are also structured. `write_property` is opt-in and commits the
complete partition atomically only after success.

There is no public `k`, relationship-weight, seed, iteration, VNS,
concurrency, minimum-community-size, or other maximum-cut option;
`vector_property` is rejected. Graph-native execution remains independent of
knowledge-layer presence per the topology-only knowledge isolation contract. Rust is the
only production owner. Python and Node remain thin argument/Arrow IPC bindings
with no backend, fallback, recovery path, or packaging dependency.

**Community** (`by=`):
`louvain`, `leiden`, `label_propagation`, `speaker_listener`, `girvan_newman`,
`modularity_optimization`, `fastgreedy`, `infomap`, `leading_eigenvector`,
`walktrap`, `spinglass`, `hdbscan`, `k_means`, `approximate_max_k_cut`

**Components** (`by=`):
`components`, `strongly_connected`, `biconnected`

**Decomposition** (`by=`):
`k_core_decomposition`

```python
table = forge.cluster("Person", by="components")
forge.cluster("Person", by="components", write_property="community")
forge.cluster("Person", by="components", via="KNOWS", directed=True)
scc = forge.cluster(
    "Person",
    by="strongly_connected",
    via="KNOWS",
    directed=True,
)
forge.cluster(
    "Person",
    by="strongly_connected",
    via="KNOWS",
    directed=True,
    write_property="scc",
)
blocks = forge.cluster("Person", by="biconnected", via="KNOWS")
forge.cluster(
    "Person",
    by="biconnected",
    via="KNOWS",
    write_property="primary_block",
)
cores = forge.cluster("Person", by="k_core_decomposition", via="KNOWS")
forge.cluster(
    "Person",
    by="k_core_decomposition",
    via="KNOWS",
    write_property="core_number",
)
louvain = forge.cluster("Person", by="louvain", via="KNOWS")
forge.cluster("Person", by="louvain", via="KNOWS", write_property="community")
leiden = forge.cluster("Person", by="leiden", via="KNOWS")
forge.cluster("Person", by="leiden", via="KNOWS", write_property="community")
labels = forge.cluster("Person", by="label_propagation", via="KNOWS")
forge.cluster(
    "Person",
    by="label_propagation",
    via="KNOWS",
    write_property="community",
)
primary = forge.cluster("Person", by="speaker_listener", via="KNOWS")
forge.cluster(
    "Person",
    by="speaker_listener",
    via="KNOWS",
    write_property="community",
)
girvan_newman = forge.cluster("Person", by="girvan_newman", via="KNOWS")
forge.cluster(
    "Person",
    by="girvan_newman",
    via="KNOWS",
    write_property="community",
)
direct = forge.cluster("Person", by="modularity_optimization", via="KNOWS")
forge.cluster(
    "Person",
    by="modularity_optimization",
    via="KNOWS",
    write_property="community",
)
fast = forge.cluster("Person", by="fastgreedy", via="KNOWS")
forge.cluster(
    "Person",
    by="fastgreedy",
    via="KNOWS",
    write_property="community",
)
flow = forge.cluster("Person", by="infomap", via="KNOWS", directed=True)
forge.cluster(
    "Person",
    by="infomap",
    via="KNOWS",
    directed=True,
    write_property="community",
)
spectral = forge.cluster("Person", by="leading_eigenvector", via="KNOWS")
forge.cluster(
    "Person",
    by="leading_eigenvector",
    via="KNOWS",
    write_property="community",
)
walk = forge.cluster("Person", by="walktrap", via="KNOWS")
forge.cluster(
    "Person",
    by="walktrap",
    via="KNOWS",
    write_property="community",
)
spins = forge.cluster("Person", by="spinglass", via="KNOWS")
forge.cluster(
    "Person",
    by="spinglass",
    via="KNOWS",
    write_property="community",
)
hdbscan = forge.cluster(
    "Point",
    by="hdbscan",
    vector_property="features",
)
forge.cluster(
    "Point",
    by="hdbscan",
    vector_property="features",
    write_property="density_cluster",
)
kmeans = forge.cluster(
    "Point",
    by="k_means",
    vector_property="features",
)
forge.cluster(
    "Point",
    by="k_means",
    vector_property="features",
    write_property="community",
)
cut = forge.cluster(
    "Person",
    by="approximate_max_k_cut",
    via="KNOWS",
)
forge.cluster(
    "Person",
    by="approximate_max_k_cut",
    via="KNOWS",
    write_property="cut_side",
)
```

---

### `paths(source=None, target=None, *, by, via=None, directed=True, k=1, weight=None, capacity_property=None, cost_property=None, heuristic=None, walk_length=None, seed=None, terminal_uuids=None, prize_property=None)` → `pyarrow.Table`

Find paths or compute flows. Public identity is UUID-only; schemas vary by algorithm:

- **Shortest path**: `source_uuid: FixedSizeBinary(16)`,
  `target_uuid: FixedSizeBinary(16)`, `cost: Float64`,
  `path: List<FixedSizeBinary(16) not null>`
- **All-pairs**: one row per reachable (source, target) pair
- **Maximum-flow scalar**: `source_uuid`, `sink_uuid`, `flow: Float64`
- **Maximum-flow edges**: `edge_uuid`, `source_uuid`, `target_uuid`, `flow: Float64`
- **Minimum-cut scalar**: `source_uuid`, `sink_uuid`, `cut_value: Float64`
- **Minimum-cut edges**: `edge_uuid`, `source_uuid`, `target_uuid`, `capacity: Float64`
- **Minimum-cost maximum-flow scalar**: `source_uuid`, `sink_uuid`, `flow: Float64`,
  `cost: Float64`
- **Minimum-cost maximum-flow edges**: `edge_uuid`, `source_uuid`, `target_uuid`,
  `flow: Float64`, `unit_cost: Float64`, `flow_cost: Float64`
- **Traversal**: UUID-keyed traversal columns

Graph-wide, multi-terminal, random-walk, and closure algorithms have their own
stable non-null schemas in the
[canonical per-value catalog](../book/architecture/algorithms.md); the summary above
is illustrative rather than exhaustive.

`source` and `target` accept UUID strings, graph-owned `NodeHandle` values, or explicit unique
property selectors such as
`{"label": "Person", "property": "name", "value": "Alice"}`. Pass `target=None` for
single-source algorithms. Null, malformed, missing, ambiguous, and cross-graph selectors raise
structured errors before dispatch.

For `by="max_flow"` and `by="max_flow_edges"`, both source and target must resolve to exactly
one distinct node. The values expose two stable views of the same normalized Rust solution:

- `max_flow` returns one non-null
  `source_uuid: FixedSizeBinary(16), sink_uuid: FixedSizeBinary(16), flow: Float64` row.
- `max_flow_edges` returns one non-null
  `edge_uuid: FixedSizeBinary(16), source_uuid: FixedSizeBinary(16),
target_uuid: FixedSizeBinary(16), flow: Float64` row per selected canonical edge, including
  zero-flow edges.

Both schemas have `graphforge.algorithm_schema_version=1` and
`graphforge.verb=paths`; `graphforge.algorithm` is the selected catalog value. The edge view
uses canonical edge UUID order. Separate catalog values keep scalar and per-edge grain stable
instead of conditionally mixing them in one nullable table.

`via=None` includes every relationship type; a valid `via` selects one type. Omitting
`weight` gives every selected edge unit capacity. A named capacity property must be present,
non-null, numeric, finite, and nonnegative on every selected edge; zero is valid.
`directed=True` uses stored orientation. With `directed=False`, each stored edge supplies
capacity in both directions while retaining its one public edge UUID. Parallel edges
contribute independently, self-loops receive zero flow, and unreachable targets have scalar
flow zero. `k` must be `1`; `heuristic`, `walk_length`, and `seed` are invalid.

Stable topology, adjacency, and edge UUID ordering makes repeated scalar and edge calls
deterministic. The edge assignment obeys capacities and nonterminal conservation, while
source outflow and sink inflow equal the scalar result. Shared selected-node,
direction-expanded-adjacency, output-row, and work-checkpoint limits apply. Missing or
ambiguous selectors, equal endpoints, invalid options or capacities, non-finite accumulated
flow, resource limits, cancellation, execution, and shaping failures remain structured Rust
errors and return no partial result.

Rust owns projection, validation, the shared solution, deterministic assignment, controls,
and Arrow shaping. Python and Node are thin argument and native Arrow/Arrow IPC adapters;
there is no external runtime backend or fallback. The operation consumes only graph
topology, properties, selectors, and typed options. It never queries knowledge/epistemic knowledge
tables or implicitly uses confidence, provenance, assertions, evidence, belief status,
valid time, or hypothesis grouping, and none of those values become options or conditional
result columns. The knowledge layer may record a neutral invocation descriptor and `algorithm_run_uuid`;
The epistemic layer may resolve an immutable UUID projection before dispatch and attach interpretations
afterward without changing the algorithm result.

For `by="min_cut"` and `by="min_cut_edges"`, both source and target must resolve to exactly
one distinct node. The values expose two stable views of the same normalized Rust cut solution:

- `min_cut` returns one non-null
  `source_uuid: FixedSizeBinary(16), sink_uuid: FixedSizeBinary(16),
cut_value: Float64` row.
- `min_cut_edges` returns one non-null
  `edge_uuid: FixedSizeBinary(16), source_uuid: FixedSizeBinary(16),
target_uuid: FixedSizeBinary(16), capacity: Float64` row per selected cut edge.

Both schemas have `graphforge.algorithm_schema_version=1` and
`graphforge.verb=paths`; `graphforge.algorithm` is the selected catalog value. UUID fields
contain stored public identity, never execution surrogates. The edge view uses canonical edge
UUID order. Separate catalog values keep scalar and per-edge grain stable instead of
conditionally mixing them in one table.

`via=None` includes every relationship type; a valid `via` selects one type. Omitting
`weight` gives every selected edge unit capacity. A named capacity property must be present,
non-null, numeric, finite, and nonnegative on every selected edge; zero is valid.
`directed=True` uses stored orientation. With `directed=False`, each stored edge contributes
equal capacity in both directions for the cut while retaining its one stored UUID and
orientation in the edge result. Parallel edges contribute independently, and self-loops never
cross the cut. `k` must be `1`; `heuristic`, `walk_length`, and `seed` are invalid.

Stable topology and UUID ordering makes repeated scalar and edge calls deterministic. Among
equal-valued cuts, GraphForge selects the lexicographically smallest sorted source-side UUID
set and then canonical cut-edge UUID order. The scalar value equals the checked sum of the
selected edge capacities. An unreachable target returns one scalar row with `cut_value=0.0`
and zero edge rows with the same stable edge schema.

Shared selected-node, direction-expanded-adjacency, output-row, and work-checkpoint limits
apply. Missing or ambiguous selectors, equal endpoints, invalid options or capacities,
non-finite accumulated capacity, resource limits, cancellation, execution, and shaping
failures remain structured Rust errors and return no partial result.

Rust owns projection, validation, the shared cut solution, deterministic tie-breaking,
controls, and Arrow shaping. Python and Node are thin argument and native Arrow/Arrow IPC
adapters; there is no external runtime backend or fallback. The operation consumes only
graph topology, properties, selectors, and typed options. It never queries knowledge/epistemic knowledge
tables or implicitly uses confidence, provenance, assertions, evidence, belief status, valid
time, or hypothesis grouping, and none of those values become options or conditional result
columns. The shipped knowledge run API can persist the
descriptor and a durable `algorithm_run_uuid`; the epistemic layer may resolve an immutable UUID projection
before dispatch and attach interpretations afterward without changing the algorithm result.

For `by="gomory_hu_tree"`, the classic parent-update Rust kernel, graph-wide source-free
invocation, `graphforge-api` dispatch, knowledge-isolation proof, and fresh-wheel/fresh-addon Python
and Node acceptance are shipped. Merged evidence is recorded in
[PR 2169](https://github.com/CurateLabs/graphforge-legecy/pull/2169),
[PR 2364](https://github.com/CurateLabs/graphforge-legecy/pull/2364),
[PR 2365](https://github.com/CurateLabs/graphforge-legecy/pull/2365), and
[PR 2366](https://github.com/CurateLabs/graphforge-legecy/pull/2366), respectively.

Gomory-Hu is graph-wide over the complete selected projection and requires both source and
target to be absent. The Rust invocation boundary permits an absent source for
graph-wide path catalog values while preserving the source requirement for every
source-based algorithm. Supplying either endpoint to `gomory_hu_tree` is a structured
validation error. The operation also requires `directed=False`; `directed=True`, including
the current default, is invalid.

`via=None` includes every relationship type and a valid `via` selects one type. Omitting
`weight` assigns unit capacity. A named capacity property must be present, non-null, numeric,
finite, and nonnegative on every selected edge; zero is valid. `k` must be `1`, and
`heuristic`, `walk_length`, and `seed` are invalid.

Each stored relationship UUID is one undirected capacity record. Distinct reciprocal records
remain independent parallel records and contribute capacity separately. Mirrored adjacency
for one UUID collapses, and self-loops contribute nothing. Empty and singleton projections
return a typed zero-row batch. If connected component sizes are `n_i`, the forest has exactly
`sum(n_i - 1) = |V| - component_count` rows. Isolated components contribute no row. These are
synthetic cut-tree edges, not stored relationships, so no `edge_uuid` is returned.

Construction is pinned as `classic-gomory-hu-parent-update-v1`. Components sort by minimum
raw UUID. Within each component, nodes sort by raw UUID, the minimum UUID is root, and every
other node initially has that root as parent. Nodes are processed in ascending UUID order.
Each step uses the shared canonical minimum-cut source side to rewrite later nodes with the
same parent and applies the classic parent/cut-value swap when the current parent's parent
lies on that source side. The exact state transition is specified in the
[architecture contract](../book/architecture/algorithms.md#canonical-gomory_hu_tree-contract).

The result schema is exactly non-null `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, and `cut_value: Float64`. Each synthetic edge is
normalized so `source_uuid < target_uuid`; forest rows sort lexicographically by
`(source_uuid, target_uuid, cut_value)`. Metadata is
`graphforge.algorithm=gomory_hu_tree`, `graphforge.verb=paths`, and
`graphforge.algorithm_schema_version=1`. UUIDs are public graph identity; execution
surrogates never escape.

One `AlgorithmControl` covers projection, component discovery, and every repeated cut.
Cancellation, cooperative work, memory, and output accounting are aggregate for the whole
invocation and never reset per component or cut. Invalid invocation state, strict-capacity
failure, non-finite or overflowing accumulation, limits, cancellation, execution, and
shaping fail atomically without partial rows.

Rust owns graph-native projection, capacity resolution, repeated cuts, canonical parent
updates, controls, and Arrow shaping. Python and Node are shipped thin adapters for this
graph-wide invocation and contain no fallback. The analyst-verb path never queries knowledge tables or
implicitly consumes confidence, assertions, evidence, belief status, provenance, valid time,
or hypotheses; none become options or result columns. The shipped knowledge run API can
store the descriptor, fingerprints, and durable `algorithm_run_uuid`, and
The epistemic layer may resolve an immutable UUID projection before dispatch and attach knowledge afterward.
Neither layer changes the algorithm result. External graph engines are development parity oracles only.

For `by="min_cost_max_flow"` and `by="min_cost_max_flow_edges"`, Rust, Python, and Node expose
two views of one normalized, validated Rust solution. Both source and target must resolve to
exactly one distinct node:

- `min_cost_max_flow` returns one non-null
  `source_uuid: FixedSizeBinary(16), sink_uuid: FixedSizeBinary(16), flow: Float64,
cost: Float64` row.
- `min_cost_max_flow_edges` returns one non-null
  `edge_uuid: FixedSizeBinary(16), source_uuid: FixedSizeBinary(16),
target_uuid: FixedSizeBinary(16), flow: Float64, unit_cost: Float64,
flow_cost: Float64` row for every selected canonical edge, including zero-flow edges.

Both schemas have `graphforge.algorithm_schema_version=1` and `graphforge.verb=paths`;
`graphforge.algorithm` is the selected catalog value. UUIDs are stored public identity.
Per-edge rows use ascending raw edge UUID order, and scalar `cost` is the checked canonical
sum of `flow_cost`. Each `flow_cost` is the edge's actual directional objective contribution,
including residual cancellations; in undirected mode it is not defined as signed net
`flow * unit_cost`.

The typed `capacity_property` and `cost_property` options name graph-native edge properties.
Omitting `capacity_property` gives every edge unit capacity; a named capacity must be present,
non-null, numeric, finite, and nonnegative. `cost_property` is required and must resolve on
every selected edge to a finite numeric value; negative costs are valid. There is no implicit
or knowledge-derived cost default, and `weight` is not an alias. `via=None` includes every
relationship type; a valid `via` selects one type. `k` must be `1`; `heuristic`,
`walk_length`, and `seed` are invalid.

`directed=True` uses stored orientation. In undirected mode, each stored edge supplies equal
capacity and cost in both directions while retaining one UUID. Edge `flow` is signed net flow
in stored orientation. `flow_cost` separately records the actual directional objective
contribution accumulated for that edge, including residual cancellations. Parallel edges
remain independent; self-loops have zero flow. An unreachable target returns scalar
`flow=0.0, cost=0.0` and every selected edge row with zero flow.

GraphForge maximizes flow, then minimizes total cost, then selects the lexicographically
smallest raw per-edge signed-flow vector in edge UUID order. A source-reachable negative-cost
residual cycle is a structured undefined-result error. Invalid selectors or properties,
equal endpoints, non-finite checked accumulation, limits, cancellation, execution, and shaping
fail atomically without partial rows. The shared selected-node, direction-expanded-adjacency,
output-row, and cooperative-work limits apply.

Rust owns projection, validation, the shared solution, canonical tie refinement, controls, and
Arrow shaping. Python and Node only coerce their public capacity/cost arguments and convert the
native Arrow result; neither binding adds an external runtime fallback. The analyst-verb path never queries
knowledge tables or derives capacity/cost from confidence, provenance, assertions, evidence,
belief status, valid time, or hypotheses. The shipped knowledge run API can persist
the invocation descriptor and durable `algorithm_run_uuid`; the epistemic layer may resolve an
immutable UUID projection before dispatch and attach interpretations afterward without changing
these schemas or values.

**Shortest paths** (`by=`):
`bfs`, `dijkstra`, `dijkstra_all_pairs`, `astar`, `bellman_ford`,
`floyd_warshall`, `delta_stepping`, `yens`

**Flow** (`by=`):
`max_flow`, `max_flow_edges`, `min_cut`, `min_cut_edges`, `min_cost_max_flow`,
`min_cost_max_flow_edges`, `gomory_hu_tree`

**Steiner trees** (`by=`):
`min_steiner_tree`, `prize_collecting_steiner_tree`

The two Steiner catalog values are source-free, exact, undirected Rust computations. Pass no
positional source or target, set `directed=False`, and provide mandatory node UUIDs through
`terminal_uuids`. Terminals are sorted, deduplicated, and checked against the selected graph.
`min_steiner_tree` requires at least two distinct terminals;
`prize_collecting_steiner_tree` requires at least one. Positional endpoints, `directed=True`,
non-default `k`, and unrelated path options are structured validation errors. Minimum Steiner
rejects `prize_property`; prize-collecting Steiner requires an explicit, nonblank
`prize_property`.

For selected tree edges \(F\), minimum Steiner minimizes \(\sum_{e\in F}c(e)\) while connecting
all mandatory terminals. Prize-collecting Steiner minimizes
\(\sum_{e\in F}c(e)+\sum_{v\notin V(F)}p(v)\), where the second sum covers omitted,
nonterminal nodes in the selected graph. This is the exact Steiner v1 contract, implemented by
bounded Rust edge-subset search rather than an approximation. Omitted `weight` means unit edge
cost `1.0`; a named `weight` is loaded strictly. Prize-collecting also loads every selected
node's explicit prize property strictly. Costs and prizes must be present, numeric, finite, and
nonnegative. Missing, NULL, nonnumeric, negative, NaN, and infinite selected values fail
atomically.

Objective ties prefer fewer edges and then the lexicographically smallest sorted stored-edge
UUID sequence. Parallel edges remain distinct; loops are excluded. Endpoints in each row are
normalized to ascending UUID and rows sort by `edge_uuid`. Disconnected mandatory terminals are
an undefined-result error. A zero-edge prize-collecting result is valid only for one normalized
mandatory terminal.

The exact non-null result is
`edge_uuid: FixedSizeBinary(16), source_uuid: FixedSizeBinary(16), target_uuid:
FixedSizeBinary(16), weight: Float64`. It contains one tree and no summary or `tree_id` row.
Metadata is exactly `graphforge.algorithm=<catalog value>`,
`graphforge.algorithm_schema_version=1`, and `graphforge.verb=paths`. Shared selected-node,
direction-expanded-adjacency, output-row, checkpoint, and exact-state limits apply; minimum
Steiner additionally caps non-loop search depth at 4,096 candidates. Cancellation, overflow,
limit, validation, connectivity, property, execution, and Arrow-shaping failures return a
structured Rust error with no partial result.

Rust owns projection, normalization, optimization, controls, ordering, and Arrow shaping.
Python and Node only construct the typed arguments and convert the freshly built native result;
there is no external runtime fallback. The analyst-verb path never reads knowledge/epistemic tables or derives
costs or prizes from confidence, evidence, assertions, belief state, provenance, valid time, or
hypotheses. Those fields are neither options nor result columns. Neutral descriptor and
`algorithm_run_uuid` persistence in the knowledge layer and pre-dispatch knowledge projection in the epistemic layer are
shipped. The epistemic layer resolves explicit UUID graph input before this same algorithm dispatch and cannot
change the canonical schema.

```rust
use graphforge_api::{NodeSelector, PathAlgorithm, PathsOptions};

let minimum = graph.paths(
    None::<&NodeSelector>,
    None,
    PathsOptions {
        by: PathAlgorithm::MinSteinerTree,
        directed: false,
        weight: Some("cost".into()),
        terminal_uuids: vec![*alice.uuid.as_bytes(), *bob.uuid.as_bytes()],
        ..PathsOptions::default()
    },
)?;
```

```python
minimum = graph.paths(
    None,
    by="min_steiner_tree",
    directed=False,
    weight="cost",
    terminal_uuids=[alice.uuid, bob.uuid],
)
prize = graph.paths(
    None,
    by="prize_collecting_steiner_tree",
    directed=False,
    weight="cost",
    terminal_uuids=[alice.uuid],
    prize_property="prize",
)
```

```javascript
const minimumIpc = graph.paths(
  undefined,
  undefined,
  "min_steiner_tree",
  undefined,
  false,
  1,
  "cost",
  undefined,
  undefined,
  undefined,
  terminals,
);
```

**Traversal / sampling** (`by=`):
`dfs`, `random_walk`

**Reachability** (`by=`):
`transitive_closure`

For `by="bfs"`, `target=None` returns the source plus all reachable nodes; a target returns one
deterministic shortest path or zero rows when disconnected. Rows sort by hop count and stable
topology/UUID order, including predecessor ties. Paths include both endpoints. `directed=True`
is the default, `directed=False` traverses both directions, and `via` filters relationship type.
Parallel edges do not duplicate reachability and self-loops do not revisit a node. BFS is
unweighted and requires `k=1`, so `weight`, another `k`, blank `via`, or an unavailable `by`
value raises a structured validation error. Shared Rust resource limits and cancellation apply.

For `by="dfs"`, `target` must be omitted. The result is deterministic depth-first preorder over
the source's selected connected component with the non-null schema
`node_uuid: FixedSizeBinary(16)`, `depth: UInt64`, `order: UInt64`. `order` is zero-based
discovery order and `depth` is the selected-edge distance along the traversal tree. Stable
topology/UUID neighbor order makes repeated Rust, Python, and Node calls identical.
`directed=True` follows outgoing edges, `directed=False` traverses either direction, and `via`
filters relationship type. Parallel edges collapse for discovery, self-loops never rediscover
the source, and disconnected nodes are omitted. An isolated source or a source with no selected
relationship produces its single `(depth=0, order=0)` row; an empty graph cannot supply the
required source and fails selector validation.

DFS is unweighted and requires `k=1`; a target, `weight`, `heuristic`, another `k`, blank `via`,
or a malformed/missing/ambiguous/cross-graph source returns a structured error. Rust owns
selector validation, adjacency export, traversal, ordering, limits, cancellation, and Arrow
shaping. Shared limits cap selected nodes, direction-expanded adjacency entries, output rows,
and cooperative checkpoints; cancellation or a limit failure returns no partial batch. Python
and Node only coerce arguments and decode the native Arrow result. The schema metadata is
`graphforge.algorithm=dfs`, `graphforge.algorithm_schema_version=1`, and
`graphforge.verb=paths`.

```rust
use graphforge_api::{GraphForge, NodeSelector, PathAlgorithm, PathsOptions};

let rows = graph.paths(
    &NodeSelector::Handle(alice),
    None,
    PathsOptions {
        by: PathAlgorithm::Dfs,
        via: Some("KNOWS".into()),
        directed: true,
        k: 1,
        weight: None,
        heuristic: None,
        walk_length: None,
        seed: None,
    },
)?;
```

```python
rows = graph.paths(alice, by="dfs", via="KNOWS", directed=True)
```

```javascript
const ipc = graph.paths(alice, undefined, "dfs", "KNOWS", true);
```

For `by="random_walk"`, the required source selects the start node or nodes and `target` must be
omitted. `k` requests the number of walks per resolved start and defaults to `1`; zero is a
structured validation error. `walk_length` is the maximum number of transitions and defaults to
`10`; zero returns the start UUID alone. `seed` is an unsigned 64-bit value and defaults to the
fixed constant `0`.

`directed=True` samples outgoing selected edges, `directed=False` samples both endpoint
directions, and `via` selects an optional relationship type. Omitting `weight` samples uniformly
over direction-expanded adjacency entries. A named weight samples proportionally and requires
every eligible value to be present, numeric, finite, and nonnegative. Missing, NULL, nonnumeric,
NaN, infinite, and negative weights fail the call. Parallel edges are distinct choices,
self-loops are valid, and dead ends or an all-zero eligible weight set terminate without
padding.

The reproducibility contract is `splitmix64-v1`. Choices are ordered by neighbor UUID and edge
UUID, then each walk uses a stream deterministically derived from the normalized seed, resolved
start ordinal, and walk ordinal. Changing the RNG, seed derivation, draw conversion, or choice
ordering requires a new named contract version. Given the same resolved graph projection,
normalized options, contract version, and seed, repeated calls return byte-identical Arrow
output.

Rows sort by resolved-start order and then walk ordinal. The exact non-null result is
`start_uuid: FixedSizeBinary(16), walk: List<FixedSizeBinary(16) not null>`. Each list includes
its start and has at most `walk_length + 1` UUIDs. Metadata identifies algorithm `random_walk`,
schema version `1`, and verb `paths`. Checked `k × starts × walk_length` work, shared graph,
adjacency, and output limits, and cooperative cancellation fail atomically with structured Rust
errors before any partial result is returned.

Rust owns projection, strict weights, RNG, ordering, validation, limits, cancellation, and Arrow
shaping. Python and Node are thin native adapters. Random walk consumes only graph topology,
explicit properties, selectors, and typed options; it never reads knowledge/epistemic tables or
implicitly uses confidence, assertions, evidence, belief status, provenance, or valid time.
Those values are neither options nor conditional result columns. The knowledge layer may record the neutral
invocation descriptor and `algorithm_run_uuid`, while the epistemic layer may resolve an immutable projection
before dispatch and attach interpretations afterward.

```rust
use graphforge_api::{NodeSelector, PathAlgorithm, PathsOptions};

let walks = graph.paths(
    &NodeSelector::Handle(alice),
    None,
    PathsOptions {
        by: PathAlgorithm::RandomWalk,
        via: Some("KNOWS".into()),
        directed: true,
        k: 2,
        weight: None,
        heuristic: None,
        walk_length: Some(3),
        seed: Some(42),
    },
)?;
```

```python
walks = graph.paths(
    alice,
    by="random_walk",
    via="KNOWS",
    k=2,
    walk_length=3,
    seed=42,
)
```

```javascript
const ipc = graph.paths(
  alice,
  undefined,
  "random_walk",
  "KNOWS",
  true,
  2,
  undefined,
  undefined,
  3,
  42n,
);
```

For `by="dijkstra"`, `target=None` returns the source plus every reachable node; a target
returns one exact shortest path or zero rows when disconnected. Omitting `weight` assigns
every selected edge cost `1.0`. A named weight property is strict: every selected edge must
have a finite, numeric, non-negative value. Missing, null, non-numeric, negative, and
non-finite weights fail the call. Dijkstra requires `k=1`; `directed` and `via` select edge
direction and relationship type. Equal-cost paths use stable full-path topology/UUID order
and then edge order, while all-reachable rows use stable topology/UUID order.

Dijkstra is implemented, validated, limited, and cancelled in Rust. Python and Node only
coerce arguments and convert the UUID-only Arrow result. It is independent of knowledge-layer
presence. Official Neo4j Graph Data Science documentation is used only as an optional
development parity reference; igraph, NetworkX, Neo4j, SciPy, and external path services are
not runtime backends, fallbacks, packaging dependencies, or recovery paths.

For `by="dijkstra_all_pairs"`, the required `source` is validated but does not restrict the
computation; supplying `target` is an error. The result contains every reachable ordered
non-self pair and omits self and unreachable pairs. Rows sort by source topology/UUID and then
target topology/UUID. The non-null schema is `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`.

Unit-cost, strict named-weight, direction, relationship filtering, parallel-edge, and stable
equal-cost tie semantics match `dijkstra`. `k` must be `1`. Empty or disconnected selections
produce a valid empty Arrow table, and shared limits and cancellation apply to the whole
invocation without partial output. Rust owns projection through Arrow shaping; Python and Node
remain thin adapters, and knowledge-layer presence cannot change the result. The official
[Neo4j GDS all-pairs shortest path documentation][gds-apsp] is a development reference only;
there is no igraph, NetworkX, Neo4j, SciPy, external path service, fallback, packaging
dependency, or recovery runtime.

For `by="astar"`, `target` is required and the result is one exact source-to-target path or
zero rows when disconnected. The UUID-only schema matches Dijkstra, paths include both
endpoints, and a self path has zero cost. Omitting `heuristic` assigns every node estimate
zero, producing Dijkstra-equivalent behavior. When `heuristic` names a node property, every
selected node must have a finite, numeric, non-negative value and the target value must be
zero; missing, null, non-numeric, negative, non-finite, and nonzero-target values fail the
call. The caller must ensure the heuristic is admissible for the exactness guarantee.

`weight`, `via`, and `directed` use Dijkstra's strict cost and edge-selection rules, and `k`
must be `1`. Equal-cost candidates resolve by stable full-path topology/UUID order and then
edge order. Rust owns projection, validation, limits, cancellation, execution, and Arrow
shaping; Python and Node are thin adapters. The result is independent of knowledge-layer
presence, and no external graph library or service is a runtime backend, fallback, packaging
dependency, or recovery path.

For `by="bellman_ford"`, `target=None` returns the source plus every reachable node in stable
topology/UUID order; a target returns one exact deterministic shortest path or zero rows when
disconnected. A self path has zero cost. The non-null UUID-only schema is
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`, and paths include both endpoints.

Omitting `weight` assigns each selected edge cost `1.0`. A named weight is strict and every
selected edge must have a present, non-null, numeric, finite value; negative and zero values
are allowed. Missing, null, non-numeric, non-finite, and non-finite accumulated costs fail the
whole call. Any negative cycle reachable from the source fails the entire invocation without
partial output, even when a requested target is not on that cycle. A reachable negative
self-loop therefore fails. With `directed=False`, every selected edge is traversable in both
directions, so a reachable negative edge itself forms a two-edge negative cycle.

`directed=True` follows outgoing edges and `via` filters relationship type. Parallel edges
remain distinct. Equal-cost routes resolve by stable complete-path topology/UUID order and
then edge order. `k` must be `1`, and `heuristic` is not accepted. Shared graph, iteration,
output, and cancellation limits cover both relaxation and negative-cycle detection.

Rust owns strict projection, validation, execution, deterministic ties, limits, cancellation,
and Arrow shaping. Python and Node are thin adapters, knowledge-layer presence cannot change
the result, and no external graph library or service is a runtime backend, fallback,
packaging dependency, or recovery path.

For `by="floyd_warshall"`, the required `source` is validated for existence and graph
ownership but does not restrict the global all-pairs computation; supplying `target` is an
error. The result contains one exact shortest path for every reachable ordered pair of
distinct nodes. Self-pairs and unreachable pairs are omitted. Empty and disconnected
selections therefore return a valid empty or partial reachable-pair table.

The non-null UUID-only schema is `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Paths include both endpoints. Rows sort by source
topology/UUID order and then target topology/UUID order. Equal-cost routes resolve by stable
complete-path topology/UUID order and then the complete edge topology sequence, including
parallel-edge ties.

Omitting `weight` assigns every selected edge cost `1.0`. A named weight is strict across
every selected edge and must be present, non-null, numeric, and finite; negative and zero
values are allowed. Missing, null, non-numeric, non-finite, and non-finite accumulated costs
fail the whole call without partial output. Unlike non-negative `dijkstra_all_pairs`,
Floyd-Warshall accepts negative weights.

Any negative cycle anywhere in the selected graph fails the invocation, even in a disconnected
component. A negative self-loop is a cycle. With `directed=False`, each selected edge is
traversable both ways, so any negative edge forms a negative two-way cycle. `directed=True`
preserves orientation and `via` filters relationship type. `k` must be `1`, and `heuristic`
is invalid.

Shared graph, work-iteration, output, and cancellation limits cover projection, matrix
initialization and relaxation, negative-cycle detection, reconstruction, and shaping. Rust
owns the complete operation and UUID-only Arrow result; Python and Node are thin adapters.
Knowledge-layer presence cannot affect results, and no external graph library, service,
fallback, packaging dependency, or recovery path participates at runtime.

For `by="delta_stepping"`, `source` is required. With `target=None`, the result contains the
source at cost `0.0` and every reachable node in stable topology/UUID order. Supplying a
target returns one exact path, or zero rows when disconnected; a self target returns the
zero-cost singleton path. Paths include both endpoints. The non-null UUID-only schema is
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`.

Delta-stepping uses the fixed, non-configurable bucket width `delta = 1.0`: distance `d`
belongs to bucket `floor(d / 1.0)`. For the lowest bucket, Rust repeatedly relaxes light edges
with `weight <= 1.0` until closure, then relaxes heavy edges with `weight > 1.0` once from the
settled set. This bucket-phased work differs from Dijkstra's next-candidate priority queue but
still computes exact shortest paths for finite non-negative weights.

Omitting `weight` gives every selected edge cost `1.0`. A named weight is strict across every
selected edge and must be present, non-null, numeric, finite, and non-negative. Missing, null,
non-numeric, negative, non-finite, or non-finite accumulated costs fail the complete call.
`directed=True` follows outgoing edges; `directed=False` traverses both directions.
`via=None` includes every relationship type, while `via` filters to one valid type. Blank
relationship or weight selectors are invalid. `k` must be `1`, and `heuristic` is not
accepted.

Equal-cost paths resolve by stable complete node topology/UUID order and then complete edge
topology order, including parallel-edge ties. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at 10,000,000, and
cooperative iteration checkpoints at 10,000. Selector, option, projection, weight, overflow,
limit, cancellation, execution, or shaping failures return structured errors without partial
output.

Rust owns projection, validation, execution, ordering, limits, cancellation, and Arrow
shaping. Python and Node are thin adapters. Results are independent of the graph/knowledge boundary
knowledge-layer presence. External libraries and documentation are development parity
references only; no external runtime backend, fallback, packaging dependency, service, or
recovery path exists.

For `by="yens"`, both `source` and `target` are required and `k` must be at least `1`. The
result contains up to `k` exact, distinct, loopless paths, zero rows when the endpoints are
disconnected, or one rank-1, zero-cost singleton path when they are equal. Paths contain both
endpoints and repeat no node. Parallel-edge variants with the same complete node UUID sequence
collapse to the least-cost variant, with complete edge topology order selecting its stable
representative.

The exact non-null UUID-only schema is `source_uuid: FixedSizeBinary(16)`,
`target_uuid: FixedSizeBinary(16)`, `rank: UInt64`, `cost: Float64`, and
`path: List<FixedSizeBinary(16) not null>`. Ranks are contiguous and one-based after stable
ordering by cost, complete node topology/UUID sequence, and complete edge topology sequence.

Omitting `weight` gives every selected edge cost `1.0`. A named weight must be present,
non-null, numeric, finite, and non-negative on every selected edge; zero is valid. Missing,
null, non-numeric, negative, non-finite, or non-finite accumulated costs fail the complete
call. `directed=True` follows outgoing edges; `directed=False` traverses both directions.
`via=None` includes every relationship type, while `via` filters to one valid type. Blank
relationship or weight selectors are invalid, and `heuristic` is not accepted.

Shared limits cap selected nodes at 10,000,000, direction-expanded adjacency entries at
100,000,000, output rows at 10,000,000, and cooperative iteration checkpoints at 10,000.
Selector, option, projection, weight, overflow, limit, cancellation, execution, or shaping
failures return structured errors without partial output. Rust owns projection, validation,
execution, ordering, limits, cancellation, and Arrow shaping; Python and Node are thin
adapters. Results depend only on selected graph topology and weights and do not consume or
require knowledge-layer state. No external runtime backend, fallback, packaging dependency,
service, or recovery path exists.

For `by="transitive_closure"`, the required `source` is validated for existence and graph
ownership but does not restrict the global computation. Supplying `target` is an error. The
result contains every unique ordered pair connected by a selected path of positive length.
A node therefore pairs with itself only when a selected cycle returns to it, including a
self-loop. Isolated nodes and unreachable pairs are omitted; empty and edgeless graphs return
the typed zero-row table.

`directed=True` follows stored orientation. `directed=False` makes each selected edge
traversable in both directions, so each node incident to an edge has a positive-length route
back to itself. `via=None` includes every relationship type, while `via` filters to one valid
type. Parallel edges collapse to one public pair. `k` must be `1`; `weight` and `heuristic`
are invalid, and blank `via` values raise structured validation errors.

The exact non-null UUID-only schema is `source_uuid: FixedSizeBinary(16)` and
`target_uuid: FixedSizeBinary(16)`, with `graphforge.algorithm=transitive_closure` and
`graphforge.verb=paths` metadata. Rows sort lexicographically by raw source UUID and then raw
target UUID. No execution surrogate is exposed.

The Rust implementation runs one traversal per selected source: worst-case time is
`O(V(V + E))`, per-source traversal state is `O(V)`, and result storage is `O(R)` for
`R` reachable pairs, up to `V²`. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at 10,000,000, and
cooperative work checkpoints at 10,000. Cancellation and all failures abort without partial
output.

Rust owns catalog dispatch, projection, validation, traversal, deduplication, ordering,
limits, cancellation, and Arrow shaping. Python and Node only adapt arguments and the native
Arrow result. Knowledge-layer presence cannot affect the topology-only result, preserving the topology-only knowledge isolation boundary. No external graph
library, service, runtime backend, fallback, packaging dependency, or recovery path
participates.

```python
# Hop-count distance, all reachable nodes from alice
forge.paths(alice, by="bfs")

# Deterministic depth-first preorder from alice
forge.paths(alice, by="dfs", via="KNOWS", directed=True)

# Unit-cost shortest paths to every reachable node
forge.paths(alice, by="dijkstra")

# Weighted shortest path between two nodes
forge.paths(alice, bob, by="dijkstra", weight="distance")

# Weighted paths for every reachable ordered non-self pair.
# `alice` is validated but does not restrict the all-pairs computation.
forge.paths(alice, by="dijkstra_all_pairs", weight="distance")

# Exact A* route using a caller-supplied admissible node estimate
forge.paths(alice, bob, by="astar", weight="distance", heuristic="estimate")

# Negative-weight shortest paths from alice to every reachable node
forge.paths(alice, by="bellman_ford", weight="balance")

# Negative-weight paths for every reachable ordered non-self pair.
# `alice` is validated but does not restrict the global computation.
forge.paths(alice, by="floyd_warshall", weight="balance")

# Exact fixed-bucket single-source paths (delta is always 1.0)
forge.paths(alice, by="delta_stepping", weight="distance")

# Top-3 loopless road paths, ranked by distance
forge.paths(alice, bob, by="yens", k=3, via="ROAD", weight="distance")

# Global positive-length KNOWS reachability.
# `alice` is validated but does not restrict the computation.
forge.paths(alice, by="transitive_closure", via="KNOWS")

# Max flow between two nodes
forge.paths(alice, bob, by="max_flow", weight="capacity")

# Canonical edge assignment for the same normalized flow solution
forge.paths(alice, bob, by="max_flow_edges", weight="capacity")

# Minimum-cost maximum flow and its canonical per-edge view share one Rust solution
forge.paths(
    alice,
    bob,
    by="min_cost_max_flow",
    capacity_property="capacity",
    cost_property="cost",
)
forge.paths(
    alice,
    bob,
    by="min_cost_max_flow_edges",
    capacity_property="capacity",
    cost_property="cost",
)
```

Node exposes the same two property names as the final positional arguments of `paths()`; omitted
intermediate optional arguments remain `undefined`:

```javascript
const scalarIpc = forge.paths(
  alice,
  bob,
  "min_cost_max_flow",
  undefined,
  true,
  1,
  undefined,
  undefined,
  undefined,
  undefined,
  undefined,
  undefined,
  "capacity",
  "cost",
);
```

[gds-apsp]: https://neo4j.com/docs/graph-data-science/current/algorithms/all-pairs-shortest-path/

---

### `analyze(label=None, *, by, via=None, directed=None, weight=None, partition_property=None, k=None, embedding_options=None)` → `pyarrow.Table`

Graph-level and structural metrics. Schema varies by algorithm.

**Spanning trees** (`by=`):
`minimum_spanning_tree`, `maximum_spanning_tree`, `minimum_k_spanning_tree`

For `by="minimum_k_spanning_tree"`, pass `directed=False` and optionally
`k=<count>` to request that many distinct lowest-cost complete spanning trees.
`k` defaults to `1` and must be positive. It counts returned trees, not nodes
or a structural constraint from a similarly named external API. The selected
graph must be connected when it contains nodes; disconnected selections fail
instead of returning a forest. If the graph has fewer than `k` distinct
spanning trees, every available tree is returned without an error.

Omitting `weight` gives every selected edge unit cost. A named weight property
must be present, numeric, finite, and nonnegative on every selected edge.
Parallel and reciprocal stored edges retain distinct UUID identity, while
mirrored rows for one stored edge collapse and self-loops are excluded. Trees
are ordered by nondecreasing total cost and canonical edge-UUID tie order;
their edges are ordered by edge UUID.

The exact result schema is non-null `tree_id: UInt64`, non-null
`edge_uuid: FixedSizeBinary(16)`, non-null
`source_uuid: FixedSizeBinary(16)`, non-null
`target_uuid: FixedSizeBinary(16)`, and non-null `weight: Float64`.
`tree_id` is zero-based within the result and is not durable provenance
identity. An empty selection is a typed zero-row edge list. A singleton is one
logical zero-edge tree, also encoded as that typed zero-row edge list; callers
use selected input cardinality to distinguish the cases rather than relying on
sentinel rows or nullable identity.

Rust owns exact enumeration, canonical ordering, checked arithmetic,
cancellation, resource limits, and Arrow shaping. Failures are atomic and do
not return partial trees. Python and Node are thin adapters. Knowledge-layer
confidence, provenance, assertions, evidence, belief state, hypotheses, and
valid time are neither algorithm options nor result columns; knowledge/epistemic may record
or annotate a completed run without changing this algorithm schema.

**DAG** (`by=`):
`topological_sort`, `is_dag`, `find_cycles`,
`dag_longest_path`, `dag_longest_path_weighted`

**Coloring** (`by=`):
`node_coloring`, `edge_coloring`, `chromatic_number`, `k1_coloring`

For `by="k1_coloring"`, pass `directed=False` to compute the version-1
deterministic distance-1 greedy coloring of the selected undirected simple
graph. The name does not accept a `k` option and does not promise the minimum
number of colors. Use `chromatic_number` for the exact minimum scalar.
`k1_coloring` is also distinct from `node_coloring`: K1 processes selected
nodes by descending normalized degree and then ascending public UUID.

For each node, Rust assigns the smallest nonnegative color absent from its
already colored neighbors. It then renumbers non-isolate colors by first
appearance in ascending UUID order; every isolate is color `0`. Rows use that
ascending UUID order. Mirrored entries for one stored edge collapse, and
distinct parallel or reciprocal edges deduplicate to one neighbor pair, so
they cannot change degree or color. Self-loops fail atomically as
non-colorable.

`label=None` selects all nodes; a valid label selects the node-induced graph.
`via=None` includes every relationship type, while a valid `via` selects one
type. The algorithm rejects `directed=True`, every `weight`, `k`, and
`partition_property`. Empty or label-empty selections return a typed zero-row
table.

The exact result schema is non-null
`node_uuid: FixedSizeBinary(16), color: UInt64`, with metadata
`graphforge.algorithm=k1_coloring`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. UUIDs are the only public node
identity. Duplicate or malformed identity, inconsistent stored-edge identity,
out-of-selection endpoints, self-loops, checked overflow, cancellation,
resource limits, execution, and Arrow shaping are structured errors and return
no partial rows.

Shared limits permit at most 10,000,000 selected nodes, 100,000,000
direction-expanded adjacency entries, 10,000,000 output rows, and 10,000
cooperative work checkpoints. With `V` selected nodes, `A` stored adjacency
entries, and `E` normalized neighbor pairs, the implementation uses
`O(V log V + A log(A + V) + E log V)` time and `O(V + E)` auxiliary memory.
Rust owns projection, normalization, coloring, ordering, controls, and Arrow
shaping. Python and Node are thin native adapters; no external graph runtime
or fallback participates.

K1 coloring never reads knowledge/epistemic tables or implicitly uses
confidence, provenance, assertions, evidence, belief status, hypotheses, or
valid time. Those values are neither options nor result columns. The shipped
neutral invocation descriptor records catalog value
`analyze.k1_coloring`, contract version `1`, resolved projection fingerprint,
normalized selectors and direction, absence of weight/unrelated options,
applied limits, and result-schema version `1`. Durable descriptor persistence
and `algorithm_run_uuid` belong to the knowledge layer; the epistemic layer resolves an immutable point-in-time
or competing-hypothesis projection before dispatch. Those shipped APIs cannot
change this algorithm result.

**Matching** (`by=`):
`max_weight_matching`, `max_cardinality_matching`, `max_bipartite_matching`

For `by="max_weight_matching"`, pass `directed=False` to compute an undirected
maximum-weight matching. Omitting `weight` assigns every candidate edge `1.0`
and therefore maximizes cardinality. A named weight property must be present,
numeric, and finite on every candidate edge; negative values are accepted, and
an all-negative selection returns an empty matching. Weight is always the
primary objective. Equal-weight optima prefer more edges and then the
lexicographically smallest canonical edge-UUID sequence; there is no
cardinality-primary option on this algorithm.

Self-loops are ignored. Distinct parallel and reciprocal stored edges remain
independent candidates, with stored UUID identity preserved in the result.
Rows sort by canonical `(source_uuid, target_uuid, edge_uuid)` and return
exactly `edge_uuid`, `source_uuid`, `target_uuid`, and non-null `weight`
columns. Empty and no-match selections return the same typed zero-row table.
Limit failures, cancellation, invalid selectors or weights, storage failures,
and Arrow-shaping failures are atomic structured errors.

For `by="max_cardinality_matching"`, pass `directed=False` to compute an exact
maximum-cardinality matching of a general graph. The result contains as many
stored edges as possible without using any selected node more than once.
`weight` is not part of this objective and every supplied `weight` is rejected.
An optional label selects the node-induced graph; `via=None` includes every
relationship type and a valid named `via` selects one type.

Self-loops are ignored. Mirrored entries for one stored edge are collapsed,
while distinct parallel and reciprocal stored edges remain candidates. Among
equal-cardinality optima, GraphForge chooses the lexicographically smallest
sorted sequence of raw stored `edge_uuid` values. Edge identity takes
precedence over endpoint order: a lower edge UUID with higher endpoint UUIDs
wins over a higher edge UUID with lower endpoint UUIDs. Rows sort by ascending
`edge_uuid`, with each row's endpoints normalized so
`source_uuid < target_uuid`. This is deliberately different from
`max_weight_matching`, whose final tie-break and row order remain canonical
`(source_uuid, target_uuid, edge_uuid)` tuple order. Empty, label-empty,
edgeless, isolated-only, and no-match selections return a typed zero-row
table.

The exact result schema is non-null `edge_uuid: FixedSizeBinary(16)`, non-null
`source_uuid: FixedSizeBinary(16)`, and non-null
`target_uuid: FixedSizeBinary(16)`, in that order, with no `weight` column.
Metadata contains `graphforge.algorithm=max_cardinality_matching`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Malformed selectors or adjacency, inconsistent edge identity, checked
arithmetic, shared resource limits, cancellation, storage failures, and
Arrow-shaping failures are atomic structured errors.

The neutral reproducibility inputs are catalog value
`analyze.max_cardinality_matching`, contract version `1`, the resolved
projection fingerprint, normalized `label` and `via` selectors,
`directed=False`, the absence of `weight`, applied limits, and result-schema
version `1`. The shipped knowledge run API can persist the descriptor and
`algorithm_run_uuid` without changing this result.

For `by="max_bipartite_matching"`, pass `directed=False`; the operation is
unweighted and rejects `weight`. Supply `partition_property="side"` to resolve
an explicit partition from every selected node's non-null scalar string or
integer property. The facade validates a complete UUID-keyed mapping before
dispatch: the edge-bearing selection must identify exactly two partitions and
every edge must cross them. The lexicographically smaller normalized partition
identifier is the left side.

Omit `partition_property` to infer partitions deterministically. Each connected
component is two-colored from its lowest canonical node UUID, which becomes
that component's left side. Self-loops and odd cycles are structured
non-bipartite errors. Isolated nodes are valid and unmatched. Missing, NULL,
unsupported, conflicting, or incomplete explicit partition values and
same-partition edges fail atomically.

Matching maximizes edge count and uses canonical UUID traversal for stable
choices. Mirrored rows for one stored edge collapse; distinct parallel and
reciprocal stored edges remain valid, and the lowest stored edge UUID represents
a matched endpoint pair. Rows are oriented left-to-right and sorted by
`(source_uuid, target_uuid, edge_uuid)`. The exact result schema is non-null
`edge_uuid: FixedSizeBinary(16)`, non-null
`source_uuid: FixedSizeBinary(16)`, and non-null
`target_uuid: FixedSizeBinary(16)`, in that order. Empty, disconnected,
isolate-only, and no-match selections preserve that typed schema.

Rust owns partition resolution or inference, matching, cancellation, resource
limits, and Arrow shaping. Python and Node are thin adapters. The kernel
consumes only graph-native topology and a validated UUID mapping: knowledge
tables, confidence, provenance, assertions, evidence, belief state,
hypotheses, and valid time cannot affect its options, result, or schema.

**Eulerian** (`by=`):
`euler_circuit`, `euler_path`, `has_euler_circuit`, `has_euler_path`

**Structure** (`by=`):
`is_planar`, `articulation_points`, `bridges`, `triangle_count`,
`conductance`, `modularity`, `transitivity`, `triad_census`, `dyad_census`,
`count_automorphisms`

**Node embeddings** (`by=`):
`node2vec`, `graphsage`, `fast_random_projection`, `hashgnn`
(returns `node_uuid: FixedSizeBinary(16)` + `embedding: List<Float32>`)

For `by="count_automorphisms"`, GraphForge returns the exact number of
topology-preserving permutations of the selected multigraph. `label` selects a
node-induced graph, `via` optionally restricts the relationship type, and
`directed` accepts both its `True` default and `False`. The operation is
unweighted and seedless; `weight`, `k`, and `partition_property` are structured
validation errors. Node and edge properties do not color or constrain
automorphisms.

Directed mode preserves loop, parallel-edge, reciprocal-edge, and endpoint
direction multiplicity. Undirected mode canonicalizes endpoint orientation
while retaining loops and every distinct stored parallel or reciprocal edge.
Mirrored adjacency records for one stored edge UUID collapse to that edge;
inconsistent reuse of an edge UUID fails. UUID order makes normalization and
branching deterministic, but UUID values are not structural colors: a
topology-preserving UUID rename returns the same count.

Rust implements `automorphism-ir-v1` normalization followed by the exact
`automorphism-count-ir-v1` individualization/refinement/backtracking search.
Empty and singleton selections each return one. The exact Arrow schema is one
non-null `count: UInt64` row with
`graphforge.algorithm=count_automorphisms`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata.

The Python and Node calls below reach that same Rust handler and return the same
Arrow table:

```python
forge.analyze("Person", by="count_automorphisms", via="ROAD", directed=False)
```

```javascript
forge.analyze("count_automorphisms", "Person", "ROAD", false);
```

The dense multiplicity matrix uses `O(V^2)` memory and is capped at 16,000,000
entries. Exact search is exponential in the worst case, with depth capped at
1,024 and aggregate search state capped at 16,000,000 entries, plus shared
projection, adjacency, one-row output, iteration, and cancellation controls.
Invalid selectors, identities, or projections; inconsistent edge identities;
allocation or supported-range failures; cancellation; resource or state-limit
exhaustion; and `UInt64` overflow are structured atomic errors with no partial
result.

Rust owns projection normalization, exact search, controls, and Arrow shaping.
Python and Node only adapt arguments and the native Arrow result. There is no
Python, JavaScript, NetworkX, igraph, service, or other runtime fallback.
Knowledge/provenance tables and sidecars cannot affect the computation or add
result columns. An equivalent epistemic-resolved immutable projection produces the
same output as direct graph-native input.

The neutral deterministic invocation descriptor contains the catalog value,
`automorphism-ir-v1` and `automorphism-count-ir-v1`, normalized `label` and
`via`, `directed`, no property or RNG fields, applied limits, the canonical
projection fingerprint, and result-schema version `1`. The shipped knowledge run API
can persist the descriptor and `algorithm_run_uuid`; the run UUID is
never part of the algorithm Arrow schema.

For `by="conductance"`, GraphForge measures an explicit partition of the
selected undirected graph. Pass `directed=False` and a non-empty
`partition_property`; the public `directed=True` default and an omitted or
blank partition property are structured validation errors. Every selected node
must have a non-null scalar string or integer value for that property.
GraphForge normalizes those values to deterministic string partition
identifiers and requires at least two non-empty partitions.

When `weight` is omitted, every selected edge has unit weight. A named weight
property must be present and finite, nonnegative, and numeric on every selected
edge. Distinct parallel edges contribute independently. A non-self-loop edge
contributes its weight once to each endpoint's partition volume and, when its
endpoints belong to different partitions, once to the cut. A self-loop
contributes twice its weight to its node partition's volume and never to the
cut.

For each partition `S`, the result is
`cut(S, V-S) / min(volume(S), volume(V-S))`. If either denominator volume is
zero, the invocation fails atomically with a structured error naming the
partition; it does not return NaN, infinity, NULL, or partial rows.

The result has one non-null row per partition with schema
`partition_id: Utf8, conductance: Float64`, ordered lexicographically by
normalized `partition_id`. Metadata is
`graphforge.algorithm=conductance`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Repeating the same resolved graph
selection and options returns the same rows and order.

Rust resolves the graph-native property to a validated UUID-keyed mapping and
owns projection, validation, checked accumulation, cancellation, resource
limits, computation, and Arrow shaping. Python and Node only forward arguments
and adapt the native Arrow result. Conductance never reads knowledge tables or
implicitly consumes confidence, assertions, evidence, belief status,
provenance, or valid time. `as_of`, epistemic status, hypothesis grouping, and
provenance selectors are not conductance options or result fields. The knowledge
layer may persist a neutral invocation descriptor and durable
`algorithm_run_uuid`. The epistemic layer may resolve a point-in-time or
competing-hypothesis immutable UUID projection before dispatch and attach
assertions, evidence, and reasoning afterward. Neither layer changes algorithm
options, computation, or schema.

The Rust modularity kernel, canonical schema, graph-native partition-property
resolution, and `graphforge-api` dispatch are shipped. Fresh-native Python and Node
acceptance coverage is also shipped, and the contract below describes the
currently available facade behavior.

For `by="modularity"`, GraphForge measures an explicit partition of the
selected undirected graph using weighted Newman-Girvan modularity at fixed
resolution `1.0`. Pass `directed=False` and a non-empty
`partition_property`; the public `directed=True` default and an omitted or
blank partition property are structured validation errors. Every selected
node must have a non-null scalar string or integer value for that property.
GraphForge normalizes those values to deterministic strings and validates a
complete immutable UUID-to-partition mapping before kernel dispatch.

When `weight` is omitted, every selected edge has unit weight. A named weight
property must be present and finite, nonnegative, and numeric on every selected
edge. Distinct parallel and reciprocal stored edges contribute independently;
mirrored adjacency for one stored edge collapses. With `A_ij` the total edge
weight between nodes `i` and `j`, `k_i` the weighted degree,
`2m = sum_i(k_i)`, and `delta(c_i,c_j)` equal to one only within a partition,
Rust computes
`Q = (1 / 2m) * sum_ij [A_ij - (k_i * k_j / 2m)] delta(c_i,c_j)`.
A self-loop of weight `w` contributes `2w` to its node's degree and to `2m`;
the adjacency sum accounts for the same doubled contribution. Empty,
edgeless, and zero-total-weight selections fail atomically with a structured
undefined-result error.

The exact result is one non-null row with schema `modularity: Float64`.
Metadata is `graphforge.algorithm=modularity`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Canonical node-UUID and edge-UUID
accumulation makes repeated results stable. Overflow and non-finite
intermediate or final values are structured errors, never NaN, infinity, NULL,
or partial output.

Rust owns projection, partition-property resolution, validated UUID mapping,
checked accumulation, deterministic ordering, cancellation, resource limits,
and Arrow shaping. Python and Node are thin argument and native Arrow result
adapters, with fresh-native acceptance coverage for equivalent logical results.
Modularity never reads knowledge/epistemic knowledge
tables or implicitly uses confidence, assertions, evidence, belief status,
provenance, valid time, or hypothesis membership. `as_of`, epistemic status,
hypothesis grouping, confidence policy, and provenance selectors are not
modularity options or
result fields. The shipped knowledge run API can persist its descriptor, mapping and
projection fingerprints, and durable
`algorithm_run_uuid`; the epistemic layer may resolve an immutable point-in-time or
competing-hypothesis projection and UUID mapping before dispatch and attach
assertions, evidence, and reasoning afterward. Neither layer changes analyst-verb
options, computation, or schema. External graph libraries are development
oracles only, not runtime dependencies or fallbacks.

For `by="is_planar"`, GraphForge tests whether the selected topology has a
crossing-free embedding. The operation requires `directed=False`; the public
default `directed=True` is a structured validation error rather than an
implicit direction drop. Every `weight` is rejected because planarity is
topology-only.

`label=None` selects every node, while a valid label selects its node-induced
subgraph. `via=None` includes every relationship type whose endpoints are both
selected, and a valid `via` includes only that type. Blank or malformed
selectors and malformed selected adjacency return structured errors.

Rust projects the selection to an undirected simple graph. Mirrored adjacency
rows for one stored edge collapse, distinct parallel and reciprocal stored
edges collapse to one unordered endpoint pair, and self-loops are ignored.
Duplicate node UUIDs, endpoints outside the selection, malformed identity, and
inconsistent reuse of an edge UUID are errors. Projection never exposes an
internal execution surrogate.

The result is exactly one deterministic row with the non-null schema
`is_planar: Boolean`. Empty, label-empty, singleton, edgeless, forest, and
disconnected selections are planar and return `true`; disconnected selections
are planar exactly when every connected component is planar. The public result
does not include a planar embedding or a Kuratowski-subgraph certificate, and
the implementation is not required to choose a stable internal certificate.

Metadata is `graphforge.algorithm=is_planar`,
`graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Shared limits cap selected nodes at
10,000,000, direction-expanded adjacency entries at 100,000,000, output rows
at 10,000,000, and cooperative work checkpoints at 10,000. Cancellation,
limit, storage, projection, execution, and Arrow-shaping failures abort
atomically without a Boolean row.

Rust owns selection, simple-graph projection, validation, planarity testing,
controls, and Arrow shaping. Python and Node only adapt arguments and the
native Arrow result. No external graph library, runtime backend, fallback,
service, packaging dependency, or recovery path participates.

For `by="triad_census"`, GraphForge classifies every unordered triple of
distinct selected nodes exactly once by its induced directed simple graph. The
operation requires `directed=True`; `directed=False` is a structured
validation error. Every `weight` is rejected. `label=None` selects every node,
a valid label selects its node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

The result always contains exactly 16 rows in this standard order:

`003`, `012`, `102`, `021D`, `021U`, `021C`, `111D`, `111U`, `030T`,
`030C`, `201`, `120D`, `120U`, `120C`, `210`, `300`.

The first three digits count mutual, asymmetric, and null dyads within a
triple; suffixes distinguish the standard down, up, cyclic, and transitive
orientations. Every row is present even when its count is zero. The exact
schema is non-null `triad_type: Utf8, count: UInt64`, with
`graphforge.algorithm=triad_census`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata.

Selected parallel edges with the same ordered endpoints and mirrored
adjacency entries collapse to one directed tie. A reciprocal selected
direction remains a second tie. Self-loops are ignored because census triples
contain distinct nodes. Empty, missing-label, singleton, and two-node
selections return all 16 zero-count rows. For an edgeless selection, `003` is
`V choose 3` and every other count is zero. Counts use checked arithmetic, and
their checked sum is always `V choose 3`.

Fixed category order and raw-UUID pair normalization make the exact result
independent of input/storage order. Shared projection, adjacency, output-row,
iteration, and cancellation limits apply; failures return no partial census.
Rust owns selection, normalization, classification, controls, and Arrow
shaping. Python and Node are argument/result adapters only. igraph, NetworkX,
external services, and Python runtimes are not production backends, fallbacks,
packaging dependencies, or recovery paths. Knowledge-layer presence cannot
change this topology-only result.

For `by="edge_coloring"`, GraphForge greedily colors every selected stored edge
so incident edges never share a color. The operation requires
`directed=False`; `directed=True`, including the public default, is a
structured validation error. Every `weight` is rejected. `label=None` selects
every node, a valid label selects the node-induced graph, `via=None` includes
every relationship type, and a valid `via` selects one type.

Undirected projection canonicalizes endpoint UUIDs. Identical or mirrored
adjacency entries collapse only when they share one stored `edge_uuid`.
Distinct parallel and reciprocal stored edges retain distinct identities and
therefore compete for colors at each shared endpoint. Inconsistent reuse of
one edge UUID fails. A self-loop is not properly edge-colorable and returns a
structured execution error without rows.

Rust processes unique stored edges in ascending raw `edge_uuid` order. Each
edge receives the smallest zero-based color not already used by a colored edge
incident to either endpoint. Colors use checked arithmetic and checked
conversion to `UInt64`. This rule is deterministic across input order and all
bindings, but greedy coloring does not guarantee a globally minimum number of
colors. Result rows remain ordered by ascending raw `edge_uuid`.

The exact schema is non-null `edge_uuid: FixedSizeBinary(16)`, non-null
`color: UInt64`. Metadata is `graphforge.algorithm=edge_coloring`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Empty, missing-label, edgeless, and isolate-only selections return this typed
zero-row table. Disconnected selections are colored in global edge-UUID order,
and isolated nodes produce no rows. Duplicate node UUIDs, selected edges with
outside endpoints, malformed identities, inconsistent edge identities, and
shaping failures abort atomically; internal execution surrogates are never
exposed.

For `V` selected nodes, `A` direction-expanded adjacency entries, `E` unique
stored edges, and maximum incident-edge count `Delta`, deterministic projection
and normalization use `O(V log V + A log E)` time. Greedy coloring is
`O(E Delta log Delta)` in the worst case, with `O(V + E)` working memory.
Shared limits cap selected nodes at 10,000,000, direction-expanded adjacency
entries at 100,000,000, output rows at 10,000,000, and cooperative work
checkpoints at 10,000. Projection, normalization, checked colors, limits,
cancellation, execution, and Arrow shaping return no partial output.

Rust owns dispatch, projection, normalization, coloring, deterministic
ordering, controls, and Arrow shaping. Python and Node only adapt arguments and
the native Arrow result; neither provides an algorithm backend or fallback.
Knowledge-layer presence cannot affect this topology-only result, preserving the topology-only knowledge isolation boundary. No external
graph runtime, service, packaging dependency, or recovery path participates.

For `by="chromatic_number"`, GraphForge returns the exact minimum number of
colors needed for a proper coloring of the selected graph. It does not return
a greedy approximation. The operation requires `directed=False`;
`directed=True`, including the public default, is a structured validation
error. Every `weight` is rejected. `label=None` selects every node, a valid
label selects the node-induced graph, `via=None` includes every relationship
type, and a valid `via` selects one type.

Undirected projection canonicalizes endpoint UUIDs. Identical or mirrored
adjacency entries collapse when they share one stored `edge_uuid`, and
inconsistent reuse of that UUID fails. Distinct parallel and reciprocal stored
edges deduplicate to one simple neighbor pair. A self-loop is not properly
colorable and returns a structured execution error without a result row.

Rust first computes a deterministic greedy upper bound, then proves the exact
answer with DSATUR branch-and-bound. Each search state chooses maximum distinct
neighbor-color saturation, then maximum simple degree, then smallest raw node
UUID. It tries feasible existing colors in ascending order before one new
color. The greedy result is only an upper bound; cancellation or iteration
exhaustion returns a structured error rather than an approximation.

The exact schema is one non-null `chromatic_number: UInt64` row. Metadata is
`graphforge.algorithm=chromatic_number`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Empty and missing-label selections
return `0`; a nonempty edgeless or isolate-only selection returns `1`.
Disconnected selections return the maximum component chromatic number, and
isolates do not raise a nonempty graph's result. Color counts use checked
arithmetic and checked UInt64 conversion.

Duplicate node UUIDs, endpoints outside the selection, malformed identities,
inconsistent edge identities, projection, storage, and shaping failures abort
atomically without a scalar row. Internal execution surrogates are never
exposed.

For `V` selected nodes, `A` direction-expanded adjacency entries, `E`
normalized simple edges, and deterministic greedy bound `U <= V`, projection
and normalization use `O(V log V + A log E)` time. Exact coloring is NP-hard
and has exponential worst-case search bounded by
`O(U^V (V + E) log V)`, with `O(V + E)` state. Shared limits cap selected
nodes at 10,000,000, direction-expanded adjacency entries at 100,000,000,
output rows at 10,000,000, and cooperative work checkpoints at 10,000.
Limits, cancellation, search, checked arithmetic, and Arrow-shaping failures
return no partial or approximate result.

Rust owns dispatch, projection, normalization, exact search, deterministic
choices, controls, and Arrow shaping. Python and Node only adapt arguments and
the native Arrow result; neither provides an algorithm backend or fallback.
Knowledge-layer presence cannot affect this topology-only scalar, preserving the topology-only knowledge isolation boundary. No external
graph runtime, service, packaging dependency, or recovery path participates.

For `by="euler_circuit"` or `by="euler_path"`, GraphForge constructs a
deterministic Euler trail rather than returning the Boolean classification
from the corresponding `has_euler_*` value. Both construction values return
zero or one row with the exact schema
`node_path: List<FixedSizeBinary(16)> not null, edge_path:
List<FixedSizeBinary(16)> not null`; list items are non-null. Metadata is
`graphforge.algorithm=euler_circuit` or `euler_path`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.

The lists expose UUIDs only. Every selected logical edge UUID occurs exactly
once, `edge_path.len() + 1 == node_path.len()`, and edge `i` joins nodes `i`
and `i + 1` under the requested direction mode. A circuit repeats its first
node at the end. A path may be open or closed: a graph that has a circuit also
has a valid Euler path.

Both values reject `weight`. `label` and `via` have their ordinary node-induced
and relationship-type selection semantics. `directed=True` preserves stored
orientation; `directed=False` permits either orientation. Duplicate adjacency
rows sharing the same UUID collapse only when their canonical endpoints agree.
Distinct parallel or reciprocal UUIDs remain distinct edges, and each
self-loop appears once with equal adjacent node UUIDs.

`euler_circuit` first requires circuit classification. For an open
`euler_path`, the start is the mathematically required endpoint: the lowest
UUID among the two odd-degree nodes when undirected, or the directed node with
`out_degree - in_degree = 1`. A circuit starts at the lowest UUID incident to
an edge. Deterministic Hierholzer traversal then chooses eligible edges in
ascending edge-UUID order. Endpoint class therefore takes precedence over UUID
tie-breaking.

An empty or missing-label selection returns a typed zero-row table. A non-empty
edgeless selection returns one row containing the lowest selected node UUID and
no edges. A non-Eulerian request returns structured
`UndefinedEulerCircuit`/`UndefinedEulerPath`, not an empty path. Invalid
selectors/options, duplicate node identity, outside endpoints, inconsistent
edge identity, overflow, allocation/execution, cancellation, limit, and Arrow
shaping failures are structured and atomic; no partial row is published.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, canonical projection uses
`O(V log V + A log E)` time. Classification and iterative Hierholzer traversal
use `O(V + E)` time and memory, with `O(E)` result space. Shared limits cap
selected nodes at 10,000,000, direction-expanded adjacency entries at
100,000,000, output rows at 10,000,000, and cooperative work checkpoints at
10,000. This API implements deterministic Hierholzer contract version `1` and
result-schema version `1`.

Rust owns construction, validation, controls, and Arrow shaping. Python and
Node are thin native adapters with no Python, JavaScript, NetworkX, igraph, or
other runtime fallback. The analyst-verb path consumes only graph-native topology, selectors,
typed options, limits, and explicit resolved UUID mappings. Its neutral
invocation descriptor is separate from the Arrow table. The shipped knowledge-layer run
API can persist it and an
`algorithm_run_uuid`, but that UUID never enters algorithm options or schema. The epistemic layer may
resolve knowledge state into a UUID projection before dispatch and attach
knowledge after dispatch. Confidence, assertions, evidence, belief status,
provenance, valid time, and hypotheses are never implicit algorithm inputs or result
fields.

For `by="has_euler_circuit"`, GraphForge returns whether the selected
multigraph has one closed trail containing every stored edge exactly once.
Both direction modes are valid. The default `directed=True` applies directed
Eulerian rules; `directed=False` applies undirected rules. The predicate is
unweighted and rejects every `weight`. `label=None` selects every node, a
valid label selects the node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

Empty, missing-label, and edgeless selections return `true`; isolates are
excluded from connectivity requirements. For `directed=False`, every
non-isolated node must have even degree and the active subgraph must be
connected. A self-loop contributes degree two. For `directed=True`, every node
must have equal in-degree and out-degree and the active subgraph must be
strongly connected. A self-loop contributes one incoming and one outgoing
incidence.

Logical edge identity is the stored `edge_uuid`. Directed duplicate rows
collapse only when UUID and ordered endpoints agree; a reversed or otherwise
different endpoint pair using that UUID is inconsistent. Undirected projection
canonicalizes endpoints, so identical or mirrored rows sharing one UUID
collapse. Distinct parallel and reciprocal edge UUIDs remain independent
edges. In undirected mode, reciprocal stored edges become parallel edges and
each contributes one degree incidence at both endpoints.

The exact schema is one non-null `has_euler_circuit: Boolean` row. Metadata is
`graphforge.algorithm=has_euler_circuit`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Checked degree increments prevent
wrapping. Duplicate node UUIDs, selected edges with outside endpoints,
malformed identities, inconsistent edge identities, projection, storage, and
shaping failures abort atomically without a row. No execution surrogate is
exposed.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, deterministic projection and identity normalization use
`O(V log V + A log E)` time. Degree checks and iterative connectivity, or
directed forward-and-transpose reachability, use `O(V + E)` time with
`O(V + E)` working memory. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative work checkpoints at 10,000. Limits, cancellation,
checked arithmetic, traversal, and Arrow-shaping failures return no partial
predicate.

For `by="has_euler_path"`, GraphForge returns whether the selected multigraph
has one open or closed trail containing every stored edge exactly once. Both
direction modes are valid. The default `directed=True` applies directed
Eulerian rules; `directed=False` applies undirected rules. The predicate is
unweighted and rejects every `weight`. `label=None` selects every node, a
valid label selects the node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

Empty, missing-label, and edgeless selections return `true`; isolates are
excluded from connectivity requirements. For `directed=False`, the active
subgraph must be connected and exactly zero or two nodes may have odd degree.
A self-loop contributes degree two. For `directed=True`, the active underlying
undirected graph must be weakly connected. Either every node is balanced, or
exactly one start node has `out_degree - in_degree = 1`, exactly one end node
has `in_degree - out_degree = 1`, and every other node has equal in-degree and
out-degree. A self-loop contributes one incoming and one outgoing incidence.

Logical edge identity is the stored `edge_uuid`. Directed duplicate rows
collapse only when UUID and ordered endpoints agree; a reversed or otherwise
different endpoint pair using that UUID is inconsistent. Undirected projection
canonicalizes endpoints, so identical or mirrored rows sharing one UUID
collapse. Distinct parallel and reciprocal edge UUIDs remain independent
edges. In undirected mode, reciprocal stored edges become parallel edges and
each contributes one degree incidence at both endpoints.

The exact schema is one non-null `has_euler_path: Boolean` row. Metadata is
`graphforge.algorithm=has_euler_path`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Checked degree increments and
directed degree differences prevent wrapping. Duplicate node UUIDs, selected
edges with outside endpoints, malformed identities, inconsistent edge
identities, projection, storage, and shaping failures abort atomically without
a row. No execution surrogate is exposed.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
unique logical edges, deterministic projection and identity normalization use
`O(V log V + A log E)` time. Degree checks and iterative active connectivity
use `O(V + E)` time with `O(V + E)` working memory. Shared limits cap selected
nodes at 10,000,000, direction-expanded adjacency entries at 100,000,000,
output rows at 10,000,000, and cooperative work checkpoints at 10,000. Limits,
cancellation, checked arithmetic, traversal, and Arrow-shaping failures return
no partial predicate.

Rust owns dispatch, projection, normalization, degree accounting, connectivity,
controls, and Arrow shaping. Python and Node only adapt arguments and the
native Arrow result; neither provides an Eulerian backend or fallback.
Knowledge-layer presence cannot affect this topology-only predicate,
preserving the topology-only knowledge isolation boundary. No external
graph runtime, service, packaging dependency, or recovery path participates.

For `by="minimum_spanning_tree"`, GraphForge returns a deterministic Kruskal minimum spanning
forest and requires `directed=False`. The default `directed=True` is a structured validation
error. Each disconnected component contributes a tree; empty, edgeless, isolated-node, or
label-empty selections return a typed zero-row table.

`label` selects the node-induced graph. `via=None` includes every relationship type, while a
valid named `via` filters to that type. Omitting `weight` assigns every selected edge `1.0`.
A named weight is strict across every selected edge and must be present, non-null, numeric,
and finite; negative and zero values are accepted. Missing, null, non-numeric, NaN, infinite,
blank, or malformed values and selectors fail the complete call without partial output.

Mirrored undirected adjacency entries are deduplicated by `edge_uuid`; parallel edges with
distinct UUIDs remain candidates and self-loops are ignored. Output endpoints are canonical,
with `source_uuid < target_uuid`. Candidate and row order is ascending by `weight`,
`source_uuid`, `target_uuid`, then `edge_uuid`, which also resolves all equal-weight and
parallel-edge ties deterministically.

The exact EDGE_LIST schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and
`weight: Float64`. The UUID fields are non-null. The shared edge-list schema marks `weight`
nullable, but this algorithm emits only non-null unit or validated named weights.

Execution is `O(V + E log E)` time and `O(V + E)` auxiliary space. Shared limits cap selected
nodes at 10,000,000, direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative iteration checkpoints at 10,000. Projection, validation,
candidate ordering, union-find, shaping, limits, and cancellation are Rust-owned and return
no partial output. Python and Node are thin adapters. Results are independent of the graph/knowledge boundary
knowledge-layer presence, and no external runtime backend, fallback, packaging dependency,
service, or recovery path exists.

For `by="maximum_spanning_tree"`, GraphForge returns a deterministic Kruskal
maximum spanning forest and requires `directed=False`. The default
`directed=True` is a structured validation error. Each disconnected component
contributes a tree; empty, edgeless, isolated-node, or label-empty selections
return a typed zero-row table.

`label` selects the node-induced graph. `via=None` includes every relationship
type between selected nodes, while a valid named `via` filters to that type.
Omitting `weight` assigns every selected edge `1.0`. A named weight is strict
across every selected edge and must be present, non-null, numeric, and finite;
negative, zero, and positive values are accepted. Missing, null, non-numeric,
NaN, infinite, blank, malformed, storage, or shaping failures abort the
complete call with a structured error and no partial output.

Mirrored undirected adjacency entries collapse only when they share one stored
`edge_uuid`; parallel and reciprocal stored edges with distinct UUIDs remain
independent candidates, and self-loops are ignored. Output endpoints satisfy
`source_uuid < target_uuid`. Candidate selection and returned rows use
descending `weight`, then ascending `source_uuid`, `target_uuid`, and
`edge_uuid`, deterministically resolving every equal-weight or parallel-edge
tie.

The exact EDGE_LIST schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, and
`weight: Float64`. UUID fields are non-null public stored identities. The
shared edge-list schema marks `weight` nullable, but this algorithm emits only
non-null unit or validated named weights. Metadata is
`graphforge.algorithm=maximum_spanning_tree`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`.

Execution is `O(V + E log E)` time and `O(V + E)` auxiliary space. Shared
limits cap selected nodes at 10,000,000, direction-expanded adjacency entries
at 100,000,000, output rows at 10,000,000, and cooperative iteration
checkpoints at 10,000. Projection, validation, maximizing Kruskal/union-find,
ordering, shaping, limits, and cancellation are Rust-owned and atomic. Python
and Node remain thin native Arrow adapters. Results are independent of
the graph/knowledge boundary knowledge-layer presence, and no external runtime backend, fallback,
packaging dependency, service, or recovery path exists.

For `by="find_cycles"`, GraphForge enumerates every distinct simple node cycle
in the selected graph. `label=None` selects every node; a valid label selects
the node-induced graph. `via=None` includes every relationship type between
selected nodes, while a valid named `via` selects one type. Both
`directed=True` and `directed=False` are supported. Any `weight` is a
structured validation error.

Returned UUID sequences do not repeat the starting node. Directed cycles use
the lexicographically smallest rotation. Undirected cycles use the smallest
rotation across both orientations. Complete canonical UUID sequences sort
lexicographically, so input order, repeated calls, and native bindings produce
the same rows.

A self-loop is one length-one cycle. Reciprocal directed arcs form one
length-two cycle, while one undirected edge is not a cycle. Undirected mirrored
adjacency entries collapse only when they share one stored `edge_uuid`;
inconsistent UUID reuse fails atomically. Parallel or reciprocal edge
representations that describe the same node cycle deduplicate because the
result contains nodes, not edges. Empty, acyclic, isolated-node, label-empty,
and disconnected selections are valid; only cyclic components emit rows.

The exact non-null schema is
`cycle: List<FixedSizeBinary(16) not null>`. The list and every item are
non-null public stored UUIDs. Metadata is
`graphforge.algorithm=find_cycles`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Surrogate and edge identities are not
exposed.

Enumeration is output-sensitive with exponential worst-case time and output
size. An explicit iterative DFS stack prevents recursion-depth failures.
Working graph and traversal state use linear `V + E` space; retained canonical
results additionally use space proportional to all UUID items in the distinct
cycles. Shared limits cap selected nodes at 10,000,000, direction-expanded
adjacency entries at 100,000,000, distinct output rows at 10,000,000, and
cooperative iteration checkpoints at 10,000. Cancellation is checked during
normalization and enumeration, only new canonical rows consume the output
limit, and every validation, storage, limit, cancellation, or shaping failure
returns no partial output.

Projection, validation, iterative enumeration, canonicalization,
deduplication, ordering, controls, and Arrow shaping are Rust-owned. Python and
Node are thin native Arrow adapters. Results are independent of the graph/knowledge boundary
knowledge-layer presence, and no external runtime backend, fallback, packaging
dependency, service, or recovery path exists.

For `by="topological_sort"`, GraphForge returns one complete deterministic Kahn ordering of
the selected directed acyclic graph. `label=None` selects every node; a valid label selects
the node-induced graph. `via=None` includes every relationship type between selected nodes,
while `via` filters to one valid type. The operation requires `directed=True` and rejects
every `weight`.

Every parallel edge contributes independently to indegree and is decremented independently,
but each selected node appears exactly once. A self-loop, reciprocal pair, or longer directed
cycle fails the complete invocation with a structured execution error and no partial rows.
Empty and label-empty selections return the typed zero-row table. Isolated nodes and every
disconnected DAG component remain in the result.

When several nodes are ready, Rust selects the lexicographically smallest raw public UUID.
The same rule applies across disconnected components and after every edge removal. The exact
non-null schema is `node_uuid: FixedSizeBinary(16), order: UInt64`, with
`graphforge.algorithm=topological_sort` and `graphforge.verb=analyze` metadata. Rows follow
the chosen topology, and `order` is contiguous and zero-based from `0` through `V - 1`. No
execution surrogate is exposed.

The ordered ready set gives `O((V + E) log V)` time and `O(V)` additional state plus the
`O(V)` result. Shared limits cap selected nodes at 10,000,000, direction-expanded adjacency
entries at 100,000,000, output rows at 10,000,000, and cooperative work checkpoints at
10,000. Projection, validation, cycle detection, overflow, limit, cancellation, execution,
and shaping failures abort without partial output.

Rust owns catalog dispatch, projection, Kahn traversal, deterministic ordering, limits,
cancellation, and Arrow shaping. Python and Node only adapt arguments and the native Arrow
result. Knowledge-layer presence cannot change the topology-only result, preserving the
topology-only knowledge isolation boundary. No external graph
library, service, runtime backend, fallback, packaging dependency, or recovery path
participates.

For `by="dag_longest_path"`, GraphForge returns the global longest directed
path by hop count across the complete selected DAG. `label=None` selects every
node; a valid label selects the node-induced graph. `via=None` includes every
relationship type between selected nodes, while a valid `via` selects one
type. The operation requires `directed=True`; `directed=False` and every
`weight` are structured validation errors because this catalog entry is
directed and unweighted. Weighted DAG paths use the separate
`dag_longest_path_weighted` catalog entry.

Empty and label-empty selections return exactly one row with `cost=0.0` and an
empty path. A singleton returns cost `0.0` and its UUID; among multiple
isolated nodes, the lexicographically smallest UUID is selected. Disconnected
components compete globally. The greatest hop count wins, and equal-hop paths
are resolved by the lexicographically smallest complete raw UUID sequence, not
only by their endpoints. Input ordering cannot affect the result.

Repeated identical adjacency entries with one stored `edge_uuid` collapse to
one stored edge. Reusing an `edge_uuid` for inconsistent endpoints is a
structured error. Distinct parallel arcs also collapse to one simple directed
adjacency relation, so they cannot inflate the hop count. A self-loop,
reciprocal pair, or longer directed cycle fails the complete invocation as a
non-DAG; malformed identity, duplicate node UUID, or an endpoint outside the
selection also fails without a partial result.

The result is exactly one non-null row with
`cost: Float64, path: List<FixedSizeBinary(16) not null> not null`. Path items
are public node UUIDs; no execution surrogate escapes. Metadata is
`graphforge.algorithm=dag_longest_path`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Hop counts use checked arithmetic and
must be exactly representable as finite Float64 integers (at most `2^53`);
overflow or an inexact larger count is a structured error.

UUID sorting, edge normalization, deterministic Kahn traversal, and exact
full-path propagation are Rust-owned. With `V` selected nodes, `S` stored edge
entries, and `E` distinct directed arcs, normalization and topology require
`O(V log V + S log S + (V + E) log V)` time. Full-path copying and comparison
make the current exact implementation `O(VE)` time and `O(V^2 + E)` auxiliary
space in the worst case. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative iteration checkpoints at 10,000. Projection,
validation, cycle detection, checked arithmetic, limits, cancellation,
execution, and Arrow shaping abort atomically without partial output.

Python and Node only adapt arguments and the native Arrow result; no graph
algorithm is reimplemented in either binding. Knowledge-layer presence cannot
change this topology-only result, preserving the topology-only knowledge isolation boundary. No external
algorithm runtime, fallback, service, packaging dependency, or recovery path
participates.

For `by="articulation_points"`, GraphForge returns every node whose removal
increases the number of connected components in the selected undirected
multigraph. The operation requires `directed=False`; the public
`directed=True` default and every `weight` are structured validation errors.
`label` selects a node-induced graph, and `via` optionally restricts the
relationship type.

The undirected projection collapses only mirrored adjacency entries with the
same stored `edge_uuid`. Distinct parallel and reciprocal stored edges remain
independent, self-loops are ignored, and traversal tracks parent edge identity.
Empty, label-empty, isolate-only, and cut-vertex-free selections return the
typed zero-row table. Every disconnected component is processed.

The exact non-null schema is `node_uuid: FixedSizeBinary(16)`, with
`graphforge.algorithm=articulation_points` and `graphforge.verb=analyze`
metadata. Rows are ordered by ascending raw public UUID. The explicit-stack
low-link traversal is stack-safe and `O(V + E)` after deterministic projection
ordering; projection sorting adds `O(V log V + E log E)` preprocessing
overhead.

Shared limits cap selected nodes at 10,000,000, direction-expanded adjacency
entries at 100,000,000, output rows at 10,000,000, and cooperative work
checkpoints at 10,000. Projection, traversal, overflow, limit, cancellation,
execution, and shaping failures abort without partial output. Rust owns
dispatch, multigraph projection, low-link execution, deterministic ordering,
limits, cancellation, and Arrow shaping. Python and Node only adapt arguments
and the native Arrow result. Knowledge-layer presence cannot change the result,
and no external graph runtime, fallback, service, packaging dependency, or
recovery path participates.

For `by="bridges"`, GraphForge returns every stored edge whose removal
increases the connected-component count in the selected undirected multigraph.
It requires `directed=False`, rejects every `weight`, applies `label` as a
node-induced selection, and uses `via` to optionally restrict relationship
type.

Mirrored adjacency entries collapse only when they share one stored
`edge_uuid`; distinct parallel and reciprocal stored edges remain independent,
so parallel edges are not bridges. Self-loops are ignored and parent edge
identity is preserved. Endpoints satisfy `source_uuid < target_uuid`; rows sort
ascending by `source_uuid`, `target_uuid`, then `edge_uuid`.

The exact non-null schema is `edge_uuid: FixedSizeBinary(16)`,
`source_uuid: FixedSizeBinary(16)`, `target_uuid: FixedSizeBinary(16)`, with
standard Bridges/analyze metadata. Empty, label-empty, isolate-only, edgeless,
and bridge-free selections return a typed zero-row table; disconnected
selections include every component.

The stack-safe low-link traversal is `O(V + E)` time and `O(V + E)` working
memory after projection; deterministic sorting adds
`O(V log V + E log E)` overhead. Shared node, direction-expanded adjacency,
output-row, and cooperative checkpoint limits apply; cancellation and every
failure return no partial output. Rust owns execution and Arrow shaping.
Python and Node remain thin adapters, knowledge-layer presence cannot affect
the topology-only result, and no external runtime or fallback participates.

For `by="triangle_count"`, GraphForge returns the exact global count of
unordered three-node cliques in the selected undirected simple graph. The
operation requires `directed=False`; `directed=True`, including the public
default, is a structured validation error. Every `weight` is rejected because
the algorithm is unweighted. `label=None` selects every node, a valid label
selects the node-induced graph, `via=None` includes every relationship type,
and a valid `via` selects one type.

Rust canonicalizes edge endpoint UUIDs and collapses mirrored adjacency entries
sharing one stored `edge_uuid`. Inconsistent reuse of one edge UUID fails.
Distinct parallel and reciprocal stored edges collapse to one undirected
neighbor relation, and self-loops are ignored. These multigraph forms cannot
multiply a triangle: each unordered node triple contributes at most one.

The result is always exactly one non-null row with
`triangle_count: UInt64`. Empty, missing-label, edgeless, isolate-only,
acyclic, and otherwise triangle-free selections return `0`; disconnected
selections sum every component. Metadata is
`graphforge.algorithm=triangle_count`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Raw UUID ordering and ordered neighbor
sets make the scalar independent of every input order. Checked `UInt64`
increments turn overflow into a structured error rather than wrapping.

With `V` selected nodes and `E` normalized undirected simple edges, projection
and ordered-set construction use `O(V log V + E log V)` time and `O(V + E)`
memory. Ordered wedge enumeration costs
`O(sum((u,v) in E, u < v) d+(v) log d(u))`, with a dense worst case of
`O(V^3 log V)`. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative iteration checkpoints at 10,000. Identity,
projection, normalization, counting, checked arithmetic, limit, cancellation,
execution, and shaping failures abort atomically without partial output.

Rust owns dispatch, projection, normalization, exact counting, controls, and
Arrow shaping. Python and Node only adapt arguments and the native Arrow
result; neither binding contains a triangle algorithm. Knowledge-layer
presence cannot affect this topology-only result, preserving the topology-only knowledge isolation boundary. No external
graph runtime, fallback, service, packaging dependency, or recovery path
participates.

For `by="dyad_census"`, GraphForge classifies every unordered pair of distinct
selected nodes by directed-edge presence. It requires `directed=True`;
`directed=False` is a structured validation error, and every `weight` is
rejected. `label=None` selects every node, a valid label selects the
node-induced graph, `via=None` includes every relationship type, and a valid
`via` selects one type.

The exact result contains three non-null rows in fixed order:
`("mutual", count)`, `("asymmetric", count)`, then `("null", count)`.
A mutual dyad has at least one selected edge in each direction; an asymmetric
dyad has selected edges in exactly one direction; a null dyad has no selected
edge in either direction. Every unordered pair contributes exactly once. This
two-node census is distinct from `triad_census`, which classifies induced
three-node subgraphs.

Self-loops are ignored. Identical adjacency entries and parallel edges with
the same ordered endpoints collapse to presence, so multiplicity cannot
increase a count. Any selected edges in both directions establish one mutual
dyad, including reciprocal edges with distinct stored UUIDs.

Empty, missing-label, and singleton selections return the same three rows with
zero counts. An edgeless selection returns every checked unordered pair in
`null`. The checked sum of all counts is always `V * (V - 1) / 2`.
Deterministic UUID normalization and fixed category order make the result
independent of every input order.

The exact schema is non-null `dyad_type: Utf8, count: UInt64`. Metadata is
`graphforge.algorithm=dyad_census`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Duplicate node UUIDs, endpoints
outside the selection, malformed identity, inconsistent edge identity,
projection, storage, and shaping failures abort atomically. Internal
surrogates never escape.

For `V` selected nodes, `A` direction-expanded adjacency entries, and `E`
distinct ordered non-loop endpoint pairs, deterministic projection and
normalization use `O(V log V + A log E)` time and `O(V + E)` memory; category
counting is `O(E log E)`. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative work checkpoints at 10,000. Limits, cancellation,
checked arithmetic, execution, and Arrow shaping return no partial rows.

Rust owns dispatch, projection, normalization, exact counting, deterministic
ordering, controls, and Arrow shaping. Python and Node only adapt arguments
and the native Arrow result; neither provides an algorithm backend or
fallback. Knowledge-layer presence cannot affect this topology-only result,
preserving the topology-only knowledge isolation boundary. No external
graph runtime, service, packaging dependency, or recovery path participates.

For `by="transitivity"`, GraphForge returns global undirected simple-graph
transitivity: `3 * triangles / connected_triples`, equivalently closed wedges
divided by all wedges. It requires `directed=False`; the default
`directed=True` is a structured validation error. The metric rejects every
`weight`.

`label` optionally selects a node-induced graph. `via=None` includes every
relationship type between selected nodes; a valid named `via` selects one
type. Mirrored adjacency entries sharing one stored `edge_uuid` collapse.
Parallel and reciprocal stored edges deduplicate to one undirected neighbor
pair, and self-loops are ignored. Duplicate node UUIDs, endpoints outside the
selection, inconsistent edge-UUID reuse, blank selectors, malformed adjacency,
storage failures, and shaping failures abort the complete call.

Every valid selection returns exactly one deterministic row. Counts combine all
disconnected components. Empty, edgeless, isolate-only, and label-empty
selections are valid. If there are no wedges, including disjoint single-edge
components, the value is exactly finite `0.0`.

The exact non-null schema is `transitivity: Float64`, with
`graphforge.algorithm=transitivity`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1` metadata. This graph-level scalar
contains no public or surrogate identity column.

Wedge, triangle, and closed-wedge counts use checked `UInt64` arithmetic;
overflow is a structured error. Only the final ratio converts to deterministic
finite `Float64`. Ordered UUID indexing makes input order irrelevant.
Ordered normalization is `O((V + E) log(V + E))`; the exact triangle scan is
`O(V^3 log V)` in the dense worst case because neighbor membership uses
ordered sets. Working memory is `O(V + E)` for the simple projection.

Shared limits cap selected nodes at 10,000,000, direction-expanded adjacency
entries at 100,000,000, output rows at 10,000,000, and cooperative iteration
checkpoints at 10,000. The scalar consumes one output row. Cancellation is
checked throughout projection and counting; validation, arithmetic, storage,
limit, cancellation, execution, and shaping failures return no partial output.

Projection, validation, normalization, checked counting, controls, and Arrow
shaping are Rust-owned. Python and Node remain thin native Arrow adapters.
Results are independent of the graph/knowledge boundary knowledge-layer presence, and no external
runtime backend, fallback, packaging dependency, service, or recovery path
exists.

For `by="is_dag"`, the result is exactly one non-null `is_dag: Boolean` row. `label` optionally
selects a node-induced graph and `via` filters relationship type. `directed=True` is the default;
`directed=False` returns `false` because the selected graph is treated as undirected. Empty graphs,
isolated nodes, disconnected DAGs, and one-direction parallel edges return `true`; self-loops,
reciprocal edges, and longer directed cycles return `false`. Rust owns deterministic Kahn
traversal, validation, resource limits, cancellation, and Arrow shaping; Python and Node only
coerce inputs and convert the Rust result. Blank selectors and unavailable `by` values raise
structured validation errors.

For `by="dag_longest_path_weighted"`, GraphForge returns the global
maximum-weight directed path across the complete selected DAG. The operation
requires `directed=True` and a nonblank named `weight` property;
`directed=False` or an omitted weight is a structured validation error.
`label=None` selects every node, a valid label selects the node-induced graph,
`via=None` includes every relationship type, and a valid `via` selects one
type.

Every selected stored edge must contain a non-null numeric finite Float64
value for the named property. Negative, zero, and positive finite values are
valid. Missing, null, non-numeric, NaN, infinite, blank, malformed, or
ambiguous weight selection fails atomically.

Empty and missing-label selections return one row with `cost=0.0` and an empty
path. Every node is an eligible zero-cost singleton, so a singleton returns
its UUID and an all-negative graph returns a zero-cost singleton. Disconnected
components compete globally. Larger finite total cost wins; exactly equal
costs choose the lexicographically smallest complete raw UUID path. Input
ordering cannot change the result.

Identical repeated entries sharing stored `edge_uuid`, directed endpoints, and
weight collapse. Inconsistent edge UUID reuse fails. Distinct parallel arcs
retain only their maximum weight, with equal maxima deduplicated. A self-loop,
reciprocal pair, or longer directed cycle is a structured non-DAG error
regardless of weight. Duplicate node UUIDs, malformed identity, and endpoints
outside the selected node set also fail without a partial result.

The exact result is one non-null row with
`cost: Float64, path: List<FixedSizeBinary(16) not null> not null`. Path items
are public node UUIDs. Metadata is
`graphforge.algorithm=dag_longest_path_weighted`,
`graphforge.verb=analyze`, and `graphforge.algorithm_schema_version=1`.
Every accumulated addition must remain finite; overflow, NaN, or another
non-finite total is a structured error rather than a returned value.

With `V` selected nodes, `S` stored edge entries, and `E` normalized directed
endpoint pairs, projection and ordered topology require
`O(V log V + S log S + (V + E) log V)` time. Exact complete-path propagation
currently gives `O(VE)` worst-case time and `O(V^2 + E)` worst-case auxiliary
space. Shared limits cap selected nodes at 10,000,000, direction-expanded
adjacency entries at 100,000,000, output rows at 10,000,000, and cooperative
iteration checkpoints at 10,000. Validation, projection, normalization, cycle
detection, finite arithmetic, limits, cancellation, execution, and shaping
failures abort atomically without partial output.

Rust owns dispatch, weight validation, topology, maximum-path execution,
deterministic ordering, controls, and Arrow shaping. Python and Node only
adapt arguments and the native Arrow result; neither binding contains a graph
algorithm. Knowledge-layer presence cannot change this topology-and-property
result, preserving the topology-only knowledge isolation boundary. No external
runtime, fallback, service, packaging dependency, or recovery path
participates.

For `by="node_coloring"`, GraphForge returns one deterministic greedy coloring
of the selected undirected simple graph. It requires `directed=False`;
`directed=True`, including the public default, is a structured validation
error, and every `weight` is rejected. `label=None` selects every node, a
valid label selects the node-induced graph, `via=None` includes every
relationship type, and a valid `via` selects one type.

Nodes are processed in ascending raw public UUID order. Each receives the
smallest nonnegative UInt64 color not used by an earlier neighbor, starting at
`0`. This makes repeated calls and every language surface deterministic; it
does not promise the graph's minimum chromatic number. Output rows follow the
same ascending UUID order.

Mirrored adjacency entries sharing one stored `edge_uuid` collapse.
Inconsistent edge UUID reuse fails. Distinct parallel and reciprocal stored
edges deduplicate to one undirected neighbor pair. A self-loop is a structured
non-colorable execution error. Duplicate node UUIDs, malformed identity, and
endpoints outside the selection also abort without partial output.

Empty and missing-label selections return a typed zero-row table.
Disconnected components and isolated nodes are all returned, with isolates
colored `0`. The exact non-null schema is
`node_uuid: FixedSizeBinary(16), color: UInt64`. Metadata is
`graphforge.algorithm=node_coloring`, `graphforge.verb=analyze`, and
`graphforge.algorithm_schema_version=1`. Checked color increments and UInt64
conversion reject range failure instead of wrapping.

With `V` selected nodes and `E` normalized undirected simple edges, projection
and deterministic ordering take `O(V log V + E log(V + E))` time. Greedy
assignment takes `O(E log V + V log V)` time with ordered used-color sets;
auxiliary memory is `O(V + E)`. Shared limits cap selected nodes at 10,000,000,
direction-expanded adjacency entries at 100,000,000, output rows at
10,000,000, and cooperative iteration checkpoints at 10,000. Projection,
validation, normalization, coloring, checked arithmetic, limits,
cancellation, execution, and shaping failures abort atomically without
partial rows.

Rust owns dispatch, projection, greedy coloring, deterministic ordering,
controls, and Arrow shaping. Python and Node only adapt arguments and the
native Arrow result; neither binding contains a coloring implementation.
Knowledge-layer presence cannot affect this topology-only result, preserving the topology-only knowledge isolation boundary. No external
runtime, fallback, service, packaging dependency, or recovery path
participates.

```python
# Check for cycles
result = forge.analyze(by="is_dag")

# Topological ordering
order = forge.analyze(by="topological_sort")

# Minimum spanning forest (undirected is required)
edges = forge.analyze(
    by="minimum_spanning_tree",
    via="ROAD",
    directed=False,
    weight="distance",
)

# Three cheapest distinct complete spanning trees
trees = forge.analyze(
    by="minimum_k_spanning_tree",
    via="ROAD",
    directed=False,
    weight="distance",
    k=3,
)

# Maximum-total-score matching (undirected is required)
matching = forge.analyze(
    "Person",
    by="max_weight_matching",
    via="KNOWS",
    directed=False,
    weight="score",
)

# Maximum bipartite matching with an explicit graph property
bipartite_matching = forge.analyze(
    "Person",
    by="max_bipartite_matching",
    via="WORKS_WITH",
    directed=False,
    partition_property="side",
)

# Cut vertices in an undirected relationship selection
cuts = forge.analyze(
    "Person",
    by="articulation_points",
    via="KNOWS",
    directed=False,
)

# Cut edges in an undirected relationship selection
cut_edges = forge.analyze(
    "Person",
    by="bridges",
    via="KNOWS",
    directed=False,
)

# Node embeddings for downstream ML
emb = forge.analyze("Person", by="node2vec", directed=False)
```

The four embedding values—`node2vec`, `graphsage`,
`fast_random_projection`, and `hashgnn`—return exactly:

```text
node_uuid: FixedSizeBinary(16) not null
embedding: FixedSizeList<Float32, dimensions> not null
```

Embedding items are non-null. Metadata includes
`graphforge.algorithm`, `graphforge.verb=analyze`,
`graphforge.algorithm_version`, `graphforge.algorithm_schema_version=1`,
`graphforge.dimensions`, `graphforge.seed`, `graphforge.rng_version`, and
`graphforge.rng_derivation`. Embedding analysis is read-only: it has no
`write_property` behavior and does not publish or refresh an embedding space.

---

### `similar(label, *, by, k=10, vector_property=None, via=None)` → `pyarrow.Table`

Pairwise node similarity. Returns exactly `node1_uuid: FixedSizeBinary(16)`,
`node2_uuid: FixedSizeBinary(16)`, and `similarity: Float64`; internal numeric
surrogates are never returned. `similar` has no `directed` or `write_property`
argument and is always read-only.

`by="node_similarity"` is implemented in Rust as unweighted Jaccard similarity
over each selected node's distinct outgoing-neighbor set. `via=None` selects all
relationship types; otherwise `via` selects one type. Parallel edges collapse
to one neighbor, while a self-loop contributes the node itself to its neighbor
set. Missing properties and knowledge-layer data do not affect this
graph-native calculation.

For every source with a non-empty neighbor set, positive scores are ordered by
descending similarity with target topology order as the tie-breaker, then
truncated to `k` (default 10). Sources themselves are excluded, reciprocal
pairs are emitted independently, zero scores are omitted, and source groups
follow stable topology order. Empty graphs and nodes without a positive match
produce no rows while preserving the schema.

`k` must be positive. `node_similarity` rejects `vector_property`; invalid
selectors and unavailable catalog values return structured errors. Execution
uses the shared Rust node, edge, output-row, iteration, and cancellation limits.

`by="filtered_node_similarity"` computes the same exact unweighted Jaccard
score, but restricts each source's candidates to selected nodes reached by a
direct outgoing selected relationship. `via` selects the relationship type for
both neighborhood construction and candidate eligibility; `via=None` admits
all types. Parallel relationships collapse, self is excluded as a candidate,
self-loops remain neighborhood membership, weights are ignored, and reciprocal
rows are independently eligible only when both outgoing relationships exist.

Positive scores are ordered descending with target topology order as the stable
tie-breaker, then truncated to positive `k` (default 10). Source groups follow
topology order and zero scores are omitted. Empty or singleton selections,
isolates, empty neighborhoods, and missing relationship types return the stable
empty non-null UUID/UUID/Float64 Arrow schema. `vector_property`, `k=0`, and
malformed selectors return structured errors.

Shared node, edge, iteration, 10,000,000-row, and cooperative cancellation
limits stop without partial output. Results satisfy the graph/knowledge boundary knowledge-layer
independence. Rust is the sole implementation and Python/Node are thin
bindings. The
[Neo4j GDS Filtered Node Similarity documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/filtered-node-similarity/)
is only a catalog and optional development reference: GDS source/target node
filters are not exposed or claimed by GraphForge. igraph, NetworkX, Neo4j, and
vector systems are not runtime backends, fallbacks, dependencies, or recovery
paths.

**Topology-based** (`by=`):
`node_similarity`, `filtered_node_similarity`

**Vector-based** (`by=`):
`knn`, `filtered_knn`, `cosine`

```python
forge.similar("Paper", by="node_similarity", via="CITES", k=5)
forge.similar("Paper", by="filtered_node_similarity", via="CITES", k=5)
forge.similar("Paper", by="knn", vector_property="embedding", k=10)
forge.similar("Paper", by="cosine", vector_property="embedding", k=10)
forge.similar(
    "Paper",
    by="filtered_knn",
    via="CITES",
    vector_property="embedding",
    k=10,
)
```

`by="knn"` is an exact, exhaustive Rust cosine KNN implementation. It requires
`vector_property`, rejects `via`, and compares every selected node with every
other selected node without reading graph topology. Every selected node must
have a non-empty, finite numeric-list vector of one common dimension and a
non-zero finite norm. Missing, invalid, ragged, non-finite, zero-norm, and
overflowing-norm vectors return structured errors.

For each source, self is excluded and candidates are ordered by similarity
descending, then target topology order. Negative scores are omitted, zero is
included, reciprocal directions are selected independently, and only the first
positive `k` candidates are returned. Source groups follow topology order.
Empty and singleton selections return the same empty UUID Arrow schema:
non-null `node1_uuid: FixedSizeBinary(16)`, non-null
`node2_uuid: FixedSizeBinary(16)`, and non-null `similarity: Float64`.

The vector boundary is 4,096 selected nodes and 16,777,216 total vector cells;
the shared 10,000,000-row result limit and cooperative cancellation stop work
without partial output. Results are independent of relationships and
knowledge-layer presence. Python and Node delegate to
this Rust implementation and return the same Arrow schema.

GraphForge deliberately exposes no metric, cutoff, sampler, iteration, random,
concurrency, index, convergence, or other approximate-KNN option. The
[Neo4j GDS KNN documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/knn/)
is a catalog and optional development reference, not a runtime path. igraph,
NetworkX, Neo4j, and vector stores are not runtime backends, fallbacks,
packaging dependencies, or recovery paths.

`by="cosine"` is exact exhaustive all-score cosine similarity in Rust. It
requires `vector_property`, rejects `via`, and does not read relationships.
Every selected node needs a non-empty finite numeric list of one common
dimension with a non-zero finite norm. Self is excluded; positive, zero, and
negative scores are all eligible. Candidates are ordered by score descending
with target topology order as the stable tie-breaker, then truncated to
positive `k` (default 10). Reciprocal directions are independent and source
groups follow topology order. This differs from `knn`, which omits negative
scores while retaining zero.

Empty and singleton selections preserve the non-null `node1_uuid:
FixedSizeBinary(16)`, `node2_uuid: FixedSizeBinary(16)`, `similarity: Float64`
schema. Missing, invalid, empty, ragged, non-finite, inexact-integer, zero-norm,
and overflowing-norm vectors return structured errors. The 4,096-node,
16,777,216-vector-cell, and shared 10,000,000-row limits plus cooperative
cancellation stop without partial results. Topology and knowledge-layer
presence cannot affect results, satisfying the graph/knowledge boundary.

Rust owns vector loading, validation, scoring, ordering, limits, and Arrow
shaping; Python and Node only coerce arguments and expose the same result. The
[Neo4j GDS similarity functions documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/similarity-functions/)
is an optional catalog/development parity reference, never a runtime path.
igraph, NetworkX, Neo4j, SciPy, vector stores, and approximate search are not
backends, fallbacks, packaging dependencies, or recovery paths.

`by="filtered_knn"` uses the same exact Rust cosine kernel, required
`vector_property`, vector validation, and stable UUID Arrow schema as `knn`.
It first restricts each source's candidates to distinct selected nodes reached
by an outgoing relationship of type `via`; `via=None` admits all relationship
types. Parallel relationships collapse, self is excluded, weights are ignored,
and direction matters: reciprocal output is independently eligible only when
both outgoing relationships exist.

For each source, eligible candidates are ordered by score descending and target
topology order before positive `k` (default 10) is applied. Negative scores are
omitted, zero is included, and source groups follow topology order. Empty or
singleton selections, isolates, and missing relationship types return no rows
with the same non-null `FixedSizeBinary(16)`, `FixedSizeBinary(16)`, `Float64`
schema. Missing or malformed vectors, invalid selectors, and `k=0` return
structured errors.

The 4,096-node and 16,777,216-vector-cell boundaries, shared 10,000,000-row
limit, cooperative cancellation without partial output, and the graph/knowledge boundary
knowledge-layer independence apply. Rust is the sole implementation; Python
and Node remain thin bindings. GraphForge exposes no source/target-node filter,
seeding, public cutoff, approximate-search option, or external graph/vector
runtime path. The
[Neo4j GDS Filtered KNN documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/filtered-knn/)
is only a catalog and optional development reference: Neo4j's source/target
node filters and optional seeding are deliberately not this API's semantics.

---

### `find(query=None, *, label, vector=None, similar_to=None, semantic_query=None, limit=10, space=None, force_stale=False, rerank=None, suppress_rerank_advisory=False)` → `pyarrow.Table`

Search one required label by text, vector, or deterministic hybrid retrieval.
The canonical Arrow field order is:

1. non-null `node_uuid: FixedSizeBinary(16)`;
2. label property fields in canonical name order (reserved collisions are
   qualified rather than replacing search fields);
3. non-null `score: Float64`;
4. non-null `matched_on: Utf8`, exactly `"text"`, `"vector"`, or
   `"text+vector"`.

Empty results retain the same schema. Single-channel results order by score
descending then raw UUID bytes. Hybrid retrieval requests `limit` candidates
from each channel and uses `rrf@1`: one-based ranks, constant 60, UUID union,
`sum(1 / (60 + rank))`, then score descending and UUID ascending. No raw BM25
or cosine column is conditionally appended.

`query` is the BM25 text channel. The vector channel has three mutually
exclusive forms:

- `vector`: a finite, non-zero raw vector compatible with `space`;
- `similar_to`: an existing node selector whose vector is read from `space`;
- `semantic_query`: text embedded with the provider/model/tokenizer contract
  already pinned by `space`.

`space` is required for every vector form. Structural spaces that cannot embed
arbitrary text reject `semantic_query`. Neither channel, conflicting vector
forms, a space without a vector form, a vector form without a space, an empty
text query, an unknown label/space, incompatible dimensions/identity, or
`limit` outside `1..=10_000` is a structured validation error.

Text indexing remains derived: a missing or stale matching text index triggers
one coordinated lazy build from observed string properties. Vector data is
primary and is never fabricated. A `stale` space may serve its last complete
generation while refresh is queued. A `substantially_stale` space refreshes
successfully first or fails; `force_stale=True` may explicitly serve only its
last complete generation with a warning and immediately filters deleted or
label-ineligible UUIDs. Force never bypasses corruption, incompatibility,
dimensions, tokenizer/model identity, or a missing complete generation.

`rerank` is an explicit `RerankOptions` value applied only after bounded
canonical retrieval. Omitting it leaves rows, RRF scores, schema, and order
unchanged; a configured compatible reranker may produce a suppressible
advisory. A reranker error does not silently fall back unless its options name
the `canonical_unreranked` policy. Provider/model/tokenizer identity is
auditable without conditional result columns.

```python
# Exact BM25.
forge.find("graph neural networks", label="Paper", limit=20)

# Exact vector search with three distinct query forms.
forge.find(label="Paper", vector=query_vector, space="sbert")
forge.find(label="Paper", similar_to=paper_handle, space="node2vec-v1")
forge.find(label="Paper", semantic_query="graph neural networks", space="sbert")
forge.find(label="Paper", vector=query_vector, space="sbert", force_stale=True)

# Deterministic text + semantic-vector RRF, followed by explicit reranking.
forge.find(
    "graph neural networks",
    label="Paper",
    semantic_query="graph neural networks",
    space="sbert",
    limit=20,
    rerank=RerankOptions.remote(model="provider/reranker-model"),
)
```

Search is graph-native and UUID-only. It reads selected labels/properties and
the selected vector space, never confidence, provenance, evidence, assertions,
reasoning, hypothesis state, status, or valid time. The epistemic layer may supply a resolved
UUID scope before search and attach knowledge afterward without changing these
options or canonical rows.

### `index(label, *, properties=None, rebuild=False, node=None, vector=None, space=None)` → `dict | None`

Explicitly build one derived text index or narrowly upsert one caller-supplied
node vector. These are distinct validated modes:

```python
# Derived text index: properties=None discovers all observed strings.
receipt = forge.index("Paper", properties=["title", "abstract"], rebuild=True)
assert receipt["state"] == "current"
assert receipt["artifact_source_fingerprint"] == receipt["source_fingerprint"]

# Manual single-node vector upsert. This does not claim complete space coverage.
forge.index("Paper", node=paper_handle, vector=embedding, space="scratch")
```

Text properties must be distinct non-empty observed string fields. The vector
mode requires exactly `node`, `vector`, and `space`, validates current label
membership, finite non-zero fixed dimensions, limits, cancellation, and atomic
publication, and exposes no numeric execution surrogate. Use complete
generation publication below for algorithm, local, callback, provider, or bulk
caller workflows.

Text mode returns the same canonical inspection exposed by
`inspect_text_index(label, properties=...)`: project generation UUID, ordered
properties, current source generation/fingerprint, immutable artifact
generation, artifact-recorded source identity, and bounded `state`/`reason`.
States are `missing`, `current`, `stale`, or `incompatible`; reasons are closed
tokens such as `not_built`, `source_generation_changed`, and
`backend_version`. Python uses snake_case keys and Node uses camelCase keys,
with identical values. Inspection never builds, repairs, or labels a stale,
partial, corrupt, or incompatible artifact current. A successful build receipt
is returned only after the containing project generation publishes atomically.
Vector indexing continues to return `None`.

`index_adjacency()` returns the canonical adjacency build receipt;
`inspect_adjacency()` returns the identical shape without mutation. The receipt
contains the project generation UUID, source topology generation and SHA-256,
the base artifact source generation, effective generation, effective artifact
SHA-256, and bounded state/reason. States are `missing`, `current`, `stale`, or
`incompatible`; reasons are `not_built`, `mixed_artifact_generation`,
`incomplete_delta_chain`, `missing_csr`, `unreadable_artifact`,
`content_mismatch`, or `future_artifact_generation`. A base older than topology
is current only when the complete bounded delta chain covers the gap and every
manifest-referenced CSR validates. Effective generation/fingerprint remain
reported after a complete chain even when another manifest CSR makes the
overall state incompatible. Rebuilds are privately staged and validated,
serialized at the workspace visibility boundary, and rollback-safe under
failure or cancellation; Rust, Python, and Node checkpoint views expose the
same read-only inspection.
Rust and Python accept the shared cooperative cancellation token on the
explicit rebuild operation. Node keeps its existing synchronous indexing
capability and returns the same canonical receipt and structured failures; it
does not introduce a binding-only cancellation-token protocol.

### `publish_embeddings(label, *, space, request, replace=False, retention=2)` → `None`

Atomically publish one complete embedding-space generation. `request` is one
of three statically distinct Rust-owned variants with matching Python/Node
types:

- `M18EmbeddingPublication(result=table)`: validates a canonical analyze embedding
  Arrow result and its schema metadata against the current committed graph
  projection. `analyze()` itself remains read-only.
- `EmbeddingBatchPublication(batch=table, identity=...)`: publishes a complete
  caller-supplied `node_uuid + FixedSizeList<Float32, N>` batch with an explicit
  compatibility descriptor.
- `PropertyEmbeddingPublication(properties=..., producer=..., model=...,
tokenizer=..., chunking=...)`: selects graph text explicitly and generates a
  complete batch through a local, callback, or remote producer.

One UUID may participate in multiple independently versioned spaces. A display
name is an alias, not compatibility identity. Publishing incompatible output
under an occupied name fails unless the caller explicitly chooses another name
or reassigns an alias after publication. `replace=True` replaces only a
compatible lineage and keeps the previous complete generation visible until
the new one commits. The default retention keeps the active and immediately
previous complete generations. Identical complete content is idempotent.

```python
# Publish a read-only algorithm result explicitly.
embeddings = forge.analyze("Paper", by="node2vec", directed=False)
forge.publish_embeddings(
    "Paper",
    space="node2vec-v1",
    request=M18EmbeddingPublication(result=embeddings),
)

# Complete caller-supplied batch; independent from the structural space.
forge.publish_embeddings(
    "Paper",
    space="local-sbert",
    request=EmbeddingBatchPublication(batch, identity=local_sbert_identity),
)
```

All UUID membership, uniqueness, coverage, dimensions, finite/non-zero values,
source fingerprint, limits, and cancellation validate in a private generation.
Data and manifest commit before one atomic active-generation swap. Cancellation,
provider failure, resource exhaustion, crash recovery, or concurrent graph
mutation leaves the prior complete generation as the sole visible one.

### `plan_embeddings(label, *, space, request)` → `pyarrow.Table`

Inspect a property/provider publication before outbound work. The dry-run table
reports selected label/properties, item count, exact/provider-reported/
approximate token-count class, provider/model/revision, tokenizer and input
limit, normalization, chunk size/overlap/aggregation, truncation policy,
batching, rate/time/retry/spend bounds, and whether data leaves the machine. It
never contains raw source text, vectors, provider payloads, or credentials.

```python
remote_request = PropertyEmbeddingPublication(
    properties=["title", "abstract"],
    producer=RemoteEmbeddingProducer(),  # resolves visibly to OpenRouter
    model="provider/embedding-model",
    chunking=Chunking.max_tokens(size=512, overlap=32, aggregation="mean"),
)
plan = forge.plan_embeddings("Paper", space="remote-sbert", request=remote_request)
# Publish only after inspecting outbound fields, tokenizer, budgets, and batches.
forge.publish_embeddings("Paper", space="remote-sbert", request=remote_request)
```

Selecting remote inference without a provider normalizes visibly to
`provider="openrouter"`. OpenRouter is optional and never a fallback or
dependency for analyst-verb, local, callback, or caller-supplied operation. Unsupported
capability, missing credentials, or provider failure is structured. Silent
truncation is forbidden: oversized input requires an explicit versioned
chunking/aggregation policy or fails before a request. Credentials are accepted
only from approved runtime configuration and never enter project files,
manifests, Arrow results, logs, errors, snapshots, or diagnostics.

### Embedding-space lifecycle

```python
forge.embedding_spaces()                   # all aliases/identities/generations
forge.embedding_spaces(space="sbert")      # one space plus freshness details
forge.refresh_embeddings("sbert")          # explicit atomic refresh
forge.set_embedding_alias("default", "sbert")
forge.delete_embedding_alias("default")    # generations remain
forge.delete_embedding_space("sbert")      # explicit; refuses active readers
```

`embedding_spaces()` returns Arrow fields for display name, compatibility
identity, active generation, dimensions, producer/model, source fingerprint,
freshness (`fresh`, `stale`, `substantially_stale`, `incompatible`, or
`corrupt`), refresh policy/worker state, queued mutation summary, and last
refresh outcome. It excludes vectors, secrets, source text, and knowledge data.

Lazy refresh runs on applicable `index`/`find` demand. Proactive refresh is
enabled by default while a process is open and is configurable per project or
space. Mutations debounce and coalesce to the newest durable fingerprint; at
most one refresh is active per space and project-wide producer work is bounded.
Closing every process stops private work. Reopen reconstructs durable freshness
and resumes according to policy; it never treats restart as freshness. Refresh
requires a reproducible producer recipe. A caller-supplied batch without one
returns a structured refresh-unavailable error, and a callback producer must be
registered again after reopen before its space can refresh.

---

## Graph Inspection

### `schema()` → `pyarrow.Table`

Columns: `label`, `node_count`, `rel_type`, `rel_count`.

The table reports only labels and relationship types present in the committed
graph generation. Label rows come first with null relationship columns,
followed by relationship rows with null label columns; each section is sorted
lexically. Counts are `UInt64`. An empty graph returns a zero-row table with the
same four nullable fields.

### `labels()` → `list[str]`

All node labels present in the graph, sorted lexically. Ontology declarations
and runtime-catalog observations without a matching node are excluded.

### `relationship_types()` → `list[str]`

All relationship types present in the graph, sorted lexically. Declarations and
observations without a matching relationship are excluded.

### `node_count(label=None)` → `int`

Total node count, or the exact count for a specific label. Multi-label nodes
count once in the total and once for every matching label; an unknown label
returns `0`.

GraphForge does not expose generic `begin()`, `commit()`, or `rollback()`
methods. Each `execute()` write publishes atomically; use
`publish_composite_transaction()` when graph and knowledge mutations must share
one committed generation.

### `explain(query, stage=None)` → `str`

Compiler plan for a Cypher query. `stage`: `"ast"`, `"ir"`, `"logical"`, `"physical"`,
or omit for a full multi-stage summary.

### `load_ontology(path)` → `None`

Load an ontology YAML. Governs label validation, property types, relationship constraints,
and inference rules used during query planning. This method is session-scoped
and does not change the committed project.

### `adopt_ontology(request)` → `None`

Validates a YAML/JSON ontology and atomically publishes it with the effective
advisory or strict mode and every other authoritative project participant in
one new generation. `request` contains a `WriteContext`, import `path`, and
non-exploratory `mode`. The import file is never independent project authority.

### `clear_ontology(request)` → `None`

Publishes explicit ontology absence and exploratory mode in a complete new
generation. The request carries a `WriteContext` for deterministic
idempotency.

### `workspace_ontology()` / `workspace_configuration()`

Return the validated generation-managed Rust records selected by the current
project generation. Thin Python, Node, and CLI projections land with the
checkpoint binding contract described below.

### Named checkpoints

`graphforge-api` exposes `GraphForge.checkpoint`, `list_checkpoints`,
`show_checkpoint`, `open_checkpoint`, `diff_checkpoints`,
`revert_to_checkpoint`, and `delete_checkpoint` as the Rust-owned
[`graphforge-checkpoint-api/1`](../contracts/checkpoint-api-v1.json) contract.
Existing thin Python and Node checkpoint projections return `pyarrow.Table`
and Arrow IPC buffers, respectively. `show_checkpoint(name)` resolves and
verifies one authoritative active checkpoint record and returns the exact
one-row metadata schema used by `list_checkpoints`.
`open_checkpoint(name)` remains the query surface: it returns a pinned, read-only
`CheckpointView` with `checkpoint_uuid`, `generation_uuid`, `execute`, and
`project_capabilities`. It intentionally exposes no mutation methods.

Create, delete, and revert require a canonical UUID `idempotency_key`. Revert
also requires a non-empty `reason`. List and diff accept the native bounded
`limit`, continuation token, and cancellation controls. Diff endpoints are a
named checkpoint or the current generation; scope and detail use the closed
enums in the contract file. Bindings do not copy, scan, validate, or restore
workspace data themselves.

The CLI mirrors these operations and writes Arrow IPC to stdout:

```text
gf --project PATH checkpoint create NAME --idempotency-key UUID [--description TEXT]
gf --project PATH checkpoint list [--limit N] [--after TOKEN]
gf --project PATH checkpoint show NAME
gf --project PATH checkpoint open NAME -- MATCH (n) RETURN n
gf --project PATH checkpoint diff --from NAME --to-current --scope graph --detail records
gf --project PATH --json revert NAME --preview
gf --project PATH revert NAME --reason TEXT --idempotency-key UUID --yes
gf --project PATH checkpoint delete NAME --idempotency-key UUID
```

The CLI accepts checkpoint names, never generation paths. `show` inspects
metadata; `open` runs one read-only query and does not create a mutable shell.
Revert preview is non-mutating. An actual revert fails closed unless `--yes`
explicitly authorizes it, and its success/replay receipt includes the prior
current generation.

The default native result surface is Arrow IPC. Global `--json` emits
`graphforge-cli-result/1`, with schema-first `columns` (`name`, Arrow
`data_type`, and `nullable`), followed by metadata and positional rows.
Structured JSON failures contain stable `code`, `message`, and bounded safe
`details`; they never expose credentials, raw project data, descriptions,
reasons, or unrestricted filesystem paths. Exit codes remain stable.

---

## Recipes

::: graphforge.recipes
options:
show_root_heading: true
show_source: false

---

## Rust Crate API

Generate the full Rust API reference with:

```bash
cargo doc --workspace --no-deps --open
```

Key public types:

| Type | Crate | Description |
| ----------------- | ------------- | ------------------------------------ |
| `GraphForge` | `graphforge-core` | Main engine facade — builder pattern |
| `ExecutionResult` | `graphforge-core` | `{ schema, batches, stats }` |
| `GraphPlan` | `graphforge-ir` | Versioned graph IR envelope |
| `OntologyHandle` | `graphforge-ontology` | Loaded ontology reference |
| `GfError` | `graphforge-core` | Typed public error enum |

---

_Python documentation is automatically generated from source code docstrings._
