# Embedding v1 contract

This document freezes the first GraphForge contracts for `analyze(...,
by="node2vec")`, `graphsage`, `fast_random_projection`, and `hashgnn`. It is
normative for independent implementations and golden fixtures. The cited
papers define each algorithm family; every rule labelled **GraphForge v1** is
a repository choice made to remove ambiguity, bound resources, or preserve the
algorithm graph/knowledge-layer boundary.

It also freezes the boundary between a read-only embedding computation and an
embedding-space space. Producing an Arrow table does not publish, replace, or
refresh a searchable space. The [publication contract](#embedding-space-publication)
below applies equally to algorithm output, local models, callbacks, remote providers,
and caller-supplied vectors.

The source papers are [node2vec](https://arxiv.org/abs/1607.00653),
[GraphSAGE](https://arxiv.org/abs/1706.02216), [FastRP](https://arxiv.org/abs/1908.11512),
and [HashGNN](https://arxiv.org/abs/2105.14280). Neo4j's [FastRP](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/) and [HashGNN](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/hashgnn/) docs and the
authors' [Node2Vec](https://github.com/aditya-grover/node2vec) and
[GraphSAGE](https://github.com/williamleif/GraphSAGE) implementations are development
references only, never runtime, fallback, service, or packaging dependencies.

## Shared result and execution contract

**Paper-sourced.** All four families produce one fixed-width real vector per
node. Their papers do not define GraphForge's Arrow schema, UUID identity,
metadata, reproducibility, limits, cancellation, or knowledge-layer behavior.

**GraphForge v1.** The canonical non-null result is:

```text
node_uuid: FixedSizeBinary(16), embedding: FixedSizeList<Float32, dimensions>
```

Rows use ascending raw UUID-byte order. `dimensions` is a positive option and
the list width in the runtime schema; no generic `List` representation is
permitted. Values must be finite. Empty selections return zero rows with the
same runtime schema and metadata. Duplicate UUIDs, dangling endpoints,
malformed identities, non-finite inputs, invalid options, arithmetic overflow,
resource exhaustion, and cancellation are structured errors. Execution is
atomic and returns no partial table.

The schema metadata is stable and contains exactly these algorithm keys (in
addition to Arrow-required metadata):

```text
graphforge.algorithm=<node2vec|graphsage|fast_random_projection|hashgnn>
graphforge.verb=analyze
graphforge.algorithm_version=<node2vec-v1|graphsage-unsupervised-v1|fastrp-v1|hashgnn-v1>
graphforge.algorithm_schema_version=1
graphforge.dimensions=<base-10 dimensions>
graphforge.seed=<base-10 unsigned seed>
graphforge.rng_version=splitmix64-v1
graphforge.rng_derivation=graphforge-embedding-substream-v1
```

The version value corresponds one-to-one: `node2vec` uses `node2vec-v1`,
`graphsage` uses `graphsage-unsupervised-v1`, `fast_random_projection` uses
`fastrp-v1`, and `hashgnn` uses `hashgnn-v1`.

Omitting `seed` is exactly `seed=0`. `graphforge-embedding-substream-v1` is a
domain derivation layered on the `splitmix64-v1` generator already shipped for
random walks; it does not alter that generator, its derivation, or any existing
random-walk golden. Its complete unsigned-64-bit wrapping pseudocode is:

```text
GAMMA = 0x9E3779B97F4A7C15; state = 0x6A09E667F3BCC909
mix(x):
  x = (x xor (x >> 30)) * 0xBF58476D1CE4E5B9
  x = (x xor (x >> 27)) * 0x94D049BB133111EB
  return x xor (x >> 31)
field(tag, payload) = tag:u8 || len(payload):u64-be || payload
UTF8(s) = field(0x01, strict_utf8(s))
U64(x)  = field(0x02, x:u64-be)
UUID(x) = field(0x03, x:16 raw bytes)
BYTES(x)= field(0x04, x)
encoded = UTF8("graphforge-embedding-substream-v1")
          || UTF8(algorithm) || UTF8(phase) || U64(seed)
          || each phase field in its documented order
for each consecutive 8-byte chunk of encoded, padding the final chunk on the
right with zero bytes and decoding it u64-be:
  state = mix(state xor chunk) + GAMMA
stream = SplitMix64(state)
SplitMix64.next(): stream.state += GAMMA; return mix(stream.state)
unit_f64() = f64::from_bits(0x3ff0000000000000 | (next() >> 12)) - 1.0
bounded(n): require n > 0; threshold = (-n modulo 2^64) modulo n;
            repeat draw = next() until draw >= threshold; return draw modulo n
```

The exact 52-bit `unit_f64` construction and unbiased rejection in `bounded`
are mandatory. In each golden row below, `next` and `unit_f64` are measured
separately from a freshly initialized stream with the printed derived state:

```text
node2vec/walk, seed 0, UUID 00000000-0000-0000-0000-000000000001, U64(0), U64(0)
0x0b8a2ce41ee372c1, 0xde12f26345fef4de, 0.8674766056410552
fastrp/node-projection, seed 42, UUID ffffffff-ffff-ffff-ffff-ffffffffffff, U64(7)
0x73d50f07771478be, 0xbcf4d12cf3fcae14, 0.7381106123684611
```

Uniform integer choices use `bounded`; weighted choices use `unit_f64`.
Priority ordering uses `next()` and breaks ties with the stated stable key.
Derivation algorithm tokens are exactly `node2vec`, `graphsage`, `fastrp`, and
`hashgnn`; phase tokens are the literal hyphenated names printed in this
document. Phase fields shown in parentheses are appended as typed fields in
that order.

For `exp`, `ln1p`, and `sqrt`, exact bytes are guaranteed only on the recorded
pinned Rust toolchain and target. On another IEEE-754 target, every final
Float32 coordinate must differ from the pinned expected coordinate by at most
`8 * f32::EPSILON * max(1,abs(expected))`; exceeding that tolerance requires a
new algorithm version.

Every implementation preflights checked time, row, dimension, and memory
bounds before allocating or mutating output. The internal `memory_bytes` limit
defaults to 1 GiB and includes output, working matrices, topology projections,
optimizer state, and bounded scratch described below. Work is cooperatively
cancellable at deterministic outer-loop boundaries. Rust in `graphforge-exec` owns
projection, computation, limits, cancellation, and Arrow shaping; `graphforge-api`
dispatches it, and Python and Node are thin argument/result adapters.

The neutral algorithm invocation descriptor may persist the algorithm/version,
normalized graph selectors, typed options, limits, seed/RNG contract, and a
graph-projection fingerprint. It contains no confidence, assertion, evidence,
belief, provenance, valid-time, `as_of`, epistemic-status, or hypothesis data.
The knowledge layer may associate that descriptor with a durable `algorithm_run_uuid`. The epistemic layer may
resolve an explicit point-in-time or competing-hypothesis UUID projection
before dispatch and attach reasoning records after dispatch. analyst-verb never reads
knowledge tables or changes its schema or result because those layers exist.

## Node2Vec

**Paper-sourced.** Node2Vec uses second-order biased random walks followed by a
skip-gram objective with negative sampling.

**GraphForge v1.** Defaults are `dimensions=128`, `walk_length=80` transitions,
`walks_per_node=10`, `p=1.0`, `q=1.0`, `window_size=10`, `negative_samples=5`,
`epochs=1`, `learning_rate=0.025`, and `seed=0`. Counts must be positive,
`p`, `q`, and the learning rate must be finite and positive, and dimensions
must not exceed 4,096. A selected edge weight is optional (default `1.0`) and
must be finite and non-negative.

For the previous node `t`, current node `v`, and candidate traversal `(v,x,e)`,
the unnormalized mass is

```text
weight(e) / p   if x = t
weight(e)       if x is adjacent to t in the selected projection
weight(e) / q   otherwise.
```

This is the paper's `alpha_pq(t,x) * weight(v,x)` rule. GraphForge defines
the first transition to use `weight(e)` without a second-order bias and
adjacency using the requested projection: in directed mode distance one means
that a selected `x -> t` edge exists; in undirected mode either orientation
qualifies. Each parallel edge is a separate candidate and a self-loop appears
once. If a weight property is supplied, every eligible edge must have one
numeric, finite, non-negative value; missing, NULL, non-numeric, NaN, infinite,
and negative values fail the call. Candidates are ordered by
`(neighbor UUID bytes, edge UUID bytes)`. Selection scans cumulative binary64
mass in that order using `walk`
(`UUID(start), U64(walk ordinal), U64(transition ordinal)`). Zero total mass
terminates the walk; an isolate therefore contributes its start token only.

The fixed corpus is generated once, independently of `epochs`, in start UUID,
walk ordinal, then token-position order and replayed for every epoch. For every
center, contexts are positions in ascending order from
`max(0,i-window_size)` to `min(length,i+window_size)`, excluding `i`; the window
is not randomized. Token counts over that fixed corpus define

```text
P_negative(v) = count(v)^0.75 / sum_u count(u)^0.75.
```

For each positive context, remove that node's mass, renormalize the remaining
UUID-ordered distribution, and sample each negative with one `unit_f64` draw
from `negative` (`U64(epoch), UUID(start), U64(walk ordinal),
U64(center position), U64(context position), U64(negative ordinal)`). If no
mass remains, the negative set for the example is empty. This conditioned
distribution makes the classic context exclusion total without a
probability-dependent redraw loop.

The input matrix is initialized coordinate-wise from
`Uniform[-0.5/dimensions, 0.5/dimensions)` using `node2vec-init-input`
(`UUID(node), U64(coordinate)`); the output matrix starts at zero. Training
order is the corpus
and context order above, positive example first, then its negative examples.
For one center/context example, snapshot center input row `a`, initialize
`delta_a=0`, then process the positive output row followed by negative output
rows. For label `y` in `{0,1}`, current output row `b`, and constant learning
rate `eta`, each sample is

```text
s = sigmoid(dot(a,b_old))
g = eta * (y - s)
delta_a[j] <- delta_a[j] + g * b_old[j]
b[j] <- b_old[j] + g * a[j]
```

After all samples, update `a[j] <- a[j] + delta_a[j]` in coordinate order. This
is the classic accumulated center-delta SGNS update; duplicate negative rows
are updated in draw order. Dot products, accumulation, and updates use Float32
in increasing coordinate order. `sigmoid(x)=1/(1+exp(-clamp(x,-15,15)))`.
The returned embedding is the final input matrix, without normalization.

The logical corpus and its random choices are fixed once; an implementation
may replay that identical token stream for counting and training instead of
storing it. Memory is bounded by two `nodes * dimensions * sizeof(Float32)`
matrices, token counts, the normalized topology, one walk, and Arrow output.
Checked work includes
`nodes * walks_per_node * walk_length * (1 + epochs)`, every fixed-window
context, and exactly one negative draw per requested negative sample when
eligible mass exists. Cancellation is checked per walk, transition,
center/context pair, SGNS sample, and output row. These resource and cadence
rules are GraphForge v1 choices.

## GraphSAGE

**Paper-sourced.** GraphSAGE samples neighborhoods, aggregates their features,
concatenates the aggregate with the node representation, and trains an
unsupervised random-walk loss with negative samples.

**GraphForge v1.** Defaults are `dimensions=256`, `hidden_dimensions=256`,
`layers=2`, `sample_sizes=[25,10]`, `aggregator="mean"`, `epochs=1`,
`negative_samples=20`, `learning_rate=0.000002`, and `seed=0`.
`sample_sizes.len()` must equal `layers`; all sizes and counts are positive,
dimensions are at most 4,096, and only `mean` is accepted in v1.
Version 1 always generates exactly 50 walks per selected node with at most five
transitions per walk. Those are algorithm-version constants, not public
options, normalized descriptor fields, or values callers may override.

An explicit, ordered, non-empty list of numeric feature properties is required.
A property may be scalar or a non-empty fixed-length numeric list; its shape
must be consistent across all selected nodes and the concatenated total feature
width must be positive. Values are concatenated in property then list-index
order. Missing, NULL, non-numeric, empty-list, ragged-list, and non-finite
values fail before training. The resolved matrix is the
UUID-to-feature boundary. V1 is an undirected, unweighted projection. Parallel
edges remain distinct sampling candidates, self-loops are ignored, and
candidates are ordered `(neighbor UUID, edge UUID)`. Directed or weighted
requests are rejected. These input and projection rules are GraphForge v1
choices.

Let `h_v^0` be the ordered binary64 feature vector. For fanout `f`, degree
`degree(v) >= f` samples exactly `f` incident candidates without replacement by
sorting priorities; `0 < degree(v) < f` samples exactly `f` with replacement
using `bounded(degree(v))`; degree zero samples none. Parallel candidates can
therefore repeat a neighbor in the mean. For an empty sample the mean is zero.

`neighbor-priority` is
`(U64(epoch), U64(example), BYTES(role path), U64(layer), UUID(node),
UUID(neighbor), UUID(edge))`; `neighbor-index` is
`(U64(epoch), U64(example), BYTES(role path), U64(layer), UUID(node),
U64(slot))`. A role path uses the shared typed-field encoding, beginning
`UTF8(center)`, `UTF8(positive)`, or
`UTF8(negative) || U64(negative ordinal)`, then appending
`UUID(parent) || U64(sampled slot)` at every recursive expansion. It
distinguishes identical UUIDs reached through different computation-graph
positions. These fanout and key rules are GraphForge v1 choices. Then

```text
m_v^l = mean({h_u^(l-1) : sampled (v,u,e)})
z_v^l = W_l * concat(h_v^(l-1), m_v^l)
h_v^l = unit_l2(ReLU(z_v^l)).
```

There is no bias. Layer widths are `hidden_dimensions` except the last, whose
width is `dimensions`. ReLU is `max(0,x)`; its derivative is `1` only when
`x>0` and `0` otherwise. For `h=unit_l2(z)=z/r`, `r=norm_l2(z)>0`, an incoming
gradient `g` becomes `(g-h*(h dot g))/r`. At `r=0`, both the normalized vector
and its gradient are zero. Weight entries use Xavier uniform initialization
`[-sqrt(6/(fan_in+fan_out)), sqrt(6/(fan_in+fan_out)))`, keyed by layer, output
coordinate, and input coordinate.

Positive walks use `positive-walk`
(`UUID(start), U64(walk ordinal), U64(transition ordinal)`). Every visited
transition yields one positive `(start, visited)` pair, so a partial walk
contributes all completed steps with
multiplicity; a dead-end start contributes none. Pair order is start UUID, walk
ordinal, then transition ordinal. Walks are generated once and replayed across
epochs. Each transition uses `bounded` over canonical candidates. The negative
distribution is degree-to-the-`0.75` power over nonzero-degree nodes,
normalized and scanned in UUID order. `graphsage-negative`
(`U64(epoch), U64(positive-pair ordinal), U64(negative ordinal)`) selects with
`unit_f64`. Negatives are retained even when
equal to either positive endpoint. These corpus details specialize the paper's
random-walk loss for GraphForge v1.

If the whole selected projection produces no positive pair, training fails
atomically with a structured undefined-training error. It never returns
initialized or otherwise untrained embeddings as a fallback.

For encoded endpoints `z_u,z_v`, score `s(a,b)=z_a dot z_b`, and negatives
`v_n`, the paper loss specialized by GraphForge is evaluated with stable
`softplus(x)=max(x,0)+ln1p(exp(-abs(x)))`:

```text
softplus(-s(u,v)) + sum_(n=1..negative_samples) softplus(s(u,v_n)).
```

For each positive pair, encode its center, positive, then negatives in ordinal
order from one pre-update parameter snapshot. Sum all positive and negative
loss gradients into one binary64 gradient tensor. Standard reverse-mode
derivatives pass through dot product, L2 normalization, ReLU, concatenation,
and mean, accumulating repeated-node paths in role-path order. Parameters use
binary64 Adam with `beta1=0.9`, `beta2=0.999`, `epsilon=1e-8`, constant `eta`,
zero initial moments, and one-based step `t` per positive pair:

```text
m_t = beta1*m_(t-1) + (1-beta1)*g_t
v_t = beta2*v_(t-1) + (1-beta2)*(g_t*g_t)
m_hat = m_t / (1-beta1^t)
v_hat = v_t / (1-beta2^t)
theta_t = theta_(t-1) - eta*m_hat/(sqrt(v_hat)+epsilon)
```

Compute `beta1^t` and `beta2^t` by repeated binary64 multiplication in
increasing `t`, carrying the preceding power to the next step rather than
calling a platform power function.

All right sides use the pre-update parameter/moment snapshot. Traverse layers
descending for backpropagation and parameters by layer, output coordinate,
then input coordinate. Reductions use the documented example, role-path,
candidate, and coordinate orders. Xavier initialization uses
`graphsage-xavier`
(`U64(layer), U64(output coordinate), U64(input coordinate)`).

After training, output is one deterministic full-neighborhood forward pass,
an explicit GraphForge v1 choice rather than a paper requirement:
each mean uses all canonical incident candidates, not a sample, and rows are
cast from binary64 to Float32 using IEEE round-to-nearest, ties-to-even. Memory
includes weights and two Adam moments, resolved
features, the largest bounded sampled computation graph, the final layer
buffers, normalized topology, and output; positive walks are regenerated, not
retained. Dispatch also counts the simultaneously live source adjacency,
resolved feature matrix, and conservative edge-staging buffer before cloning
the normalized GraphSAGE projection.

The following conservative formulas are normative and use checked integer
arithmetic. Let `N` be selected nodes, `A` canonical undirected adjacency
entries after loops are removed, `f_i=sample_sizes[i]`, `w_0=feature_width`,
and `w_l` the layer output width. Define:

```text
max_pairs = N * 50 * 5
roots = epochs * max_pairs * (2 + negative_samples)
sampled_nodes_per_root = 1 + sum_(l=1..layers) product_(i=1..l) f_i
parameter_coordinates = sum_(l=1..layers) w_l * (2 * w_(l-1))
inference_work = A * sum_(l=1..layers) w_(l-1)
                 + N * sum_(l=1..layers) w_l * (2*w_(l-1) + 1)
graphsage_work_checkpoints = N*50*5
                             + roots*sampled_nodes_per_root
                             + epochs*max_pairs*parameter_coordinates
                             + inference_work
```

`graphsage_work_checkpoints` is one aggregate, non-resetting bound for the
entire invocation. It charges each attempted positive-walk transition, sampled
root/node expansion, parameter coordinate for every optimizer step, and
full-neighborhood inference adjacency/coordinate operation. Checked allocation
terms are resolved features `N*w_0*8`, output `N*dimensions*4`, every weight
matrix plus its gradient and two Adam moments, the largest sampled computation
graph `sampled_nodes_per_root` references, normalized adjacency, and
bounded scratch; their checked sum must fit `memory_bytes`. The implementation
checks cancellation at each charged unit and before allocation, each epoch,
each optimizer step, and Arrow publication. Overflow, limit, cancellation, or
undefined training fails atomically.

## Fast random projection

**Paper-sourced.** FastRP combines a sparse random projection, degree
normalization, and powers of a row-normalized adjacency matrix.

**GraphForge v1.** Defaults are `dimensions=128`,
`iteration_weights=[0.0,1.0,1.0]`, `normalization_strength=0.0`,
`feature_weight=0.0`, no feature properties, and `seed=0`. Dimensions are at
most 4,096; the finite iteration-weight list is non-empty, at most 65 entries,
and not all zero. Feature properties are ordered, nonblank, and unique;
`feature_weight` is finite and non-negative and may be positive only with
features. If a weight property is supplied, every eligible edge must resolve a
numeric, finite, non-negative value; missing, NULL, non-numeric, NaN, infinite,
and negative values fail. Directed mode uses
outgoing adjacency; undirected mode expands symmetrically. Parallel edges
contribute separately and a loop once.

Let `s_v` be outgoing strength and `S=sum_v s_v`. The transition matrix is

```text
P[v,u] = sum_(edges v->u) weight(e) / s_v, or 0 when s_v=0.
```

For `n` nodes, `q=max(1,sqrt(n))`. The node projection `R` is independently

```text
+sqrt(q) with probability 1/(2q)
0        with probability 1-1/q
-sqrt(q) with probability 1/(2q),
```

using `node-projection` (`UUID(node), U64(coordinate)`). With
`beta=normalization_strength`, `L_v=(s_v/S)^beta`; `beta=0` is exactly `1`,
including isolates. For `beta>0` an isolate gets `0`; `beta<0` with any isolate
is an error. `S=0` is valid only for `beta=0`. Define `X_v=L_v R_v`.

If `p` feature properties are present, all nodes must have finite scalar values.
Define a second sparse projection `Q` with `q_f=max(1,sqrt(p))` and the same
three-point distribution, using `feature-projection`
(`UTF8(property), U64(property ordinal), U64(output coordinate)`), and
`F_v=sum_l feature(v,l) Q_l`. Then

```text
H_0 = unit_l2(X) + feature_weight * unit_l2(F)
H_t = P H_(t-1)
embedding(v) = sum_t iteration_weights[t] * unit_l2(H_t[v]).
```

`unit_l2(0)=0`; there is no final normalization. Computation uses binary64 in
canonical adjacency, iteration, and coordinate order, followed by a checked
Float32 cast. An isolate has `H_t=0` for `t>=1`.

Memory includes output, two `nodes * dimensions` binary64 iteration buffers, a
binary64 accumulator, UUID projection state, normalized adjacency, and the
`properties * dimensions` feature projection. Checked arithmetic covers each
matrix and their aggregate bytes; work is checked as at least
`adjacency_entries * (iteration_weights.len()-1) * dimensions` plus projection
and accumulation coordinates. Cancellation is checked per projection row,
iteration, adjacency row, and output row. These ordering, numeric, resource,
and cancellation rules are GraphForge v1 choices.

## HashGNN

**Paper-sourced.** HashGNN replaces learned message passing with repeated
minhash-style neighborhood feature propagation; its heterogeneous extension
uses node and relationship types in hashing.

**GraphForge v1.** Defaults are `dimensions=256`, `iterations=2`,
`embedding_density=0.25`, `heterogeneous=false`, and `seed=0`. Dimensions are
at most 8,192, iterations at most 64, and density is finite in `(0,1]`. Let
`K=max(1,ceil(embedding_density * dimensions))`, which must not exceed
dimensions. The projection is unweighted. Directed mode uses outgoing edges;
undirected mode expands them symmetrically. Parallel edges and loops retain
their edge UUID identity.

For each node, rank every coordinate `j` by `initial-code`
(`UUID(node), [UTF8(node type)], U64(j)`), where the bracketed field is present
only in heterogeneous mode; set exactly the `K` smallest `(priority,j)` bits.
Call this binary vector `B_v^0`. This initialization is a GraphForge v1 choice.

For iteration `t` and sample `r` in `[0,K)`, candidates are:

```text
(SELF, v UUID, zero edge UUID, j) for each active bit j of B_v^(t-1)
(EDGE, u UUID, edge UUID, j)      for each canonical (v,u,edge) and
                                  active bit j of B_u^(t-1).
```

Rank candidates with `minhash-select` (`U64(iteration), U64(sample),
UTF8(role), UUID(source), UUID(edge-or-zero), [UTF8(node type)],
[UTF8(relationship type)], U64(coordinate)`), omitting both bracketed type
fields in homogeneous mode and the relationship type for `SELF`. Choose the
lexicographically smallest `(priority, role, source UUID, edge UUID,
coordinate)` and set that
coordinate in `B_v^t`. Sampling is with replacement; collisions can therefore
leave fewer than `K` active output bits. An isolate preserves `B_v^0` exactly
at every iteration. The result is `0.0`/`1.0` Float32 with no training or
normalization. Candidate construction, replacement, collision, tie, isolate,
and output rules are GraphForge v1 choices.

When `heterogeneous=true`, explicit `node_type_property` and
`relationship_type_property` names are required. Before dispatch, every
selected node and edge must resolve to a non-null scalar string or integer;
canonical UTF-8 tokens are `string:<utf8-byte-length>:<value>` and
`integer:<base-10-value>` and are included in every corresponding initial-code
and minhash key. One property cannot mix string and integer values. Missing,
mixed, or unsupported values are errors. Labels and
`via` remain selection filters, not implicit type sources. When false, type
properties are rejected and no type token enters hashing.

The concrete minhash specialization is `splitmix64-minhash-v1` over the shared
length-delimited derivation. Memory includes the normalized adjacency, output,
and at most three packed bit matrices of
`nodes * ceil(dimensions/64) * sizeof(u64)`, plus bounded candidate scratch;
implementations need not materialize candidate sets. Checked arithmetic covers
those terms and at least
`iterations * nodes * K * (self_active_bits + adjacency_active_bits)` work.
Cancellation is checked per iteration, node, sample, adjacency row, and output
row. These controls and the aggregate 1 GiB default are GraphForge v1 choices.

## Embedding-space publication

### Ownership and identity

`analyze()` is read-only. It returns the canonical Arrow table described
above and a neutral invocation descriptor; it never writes graph properties or
search artifacts. Publication is a separate, explicit find/index operation. The same
atomic publication path accepts a complete algorithm result, local or callback output,
remote-provider output, or a complete caller-supplied UUID/vector batch.
Individual vector upserts remain a narrow manual operation and never claim that
a space has complete generation coverage.

Four identities are deliberately distinct:

| Identity | Contract |
|---|---|
| display name | User-facing, case-sensitive alias. Normalize to NFC; reject leading/trailing whitespace, control characters, path separators, `.`/`..`, and UTF-8 lengths outside `1..=255`. Filesystem paths use digests, never this text. |
| compatibility identity | SHA-256 of canonical JSON containing schema version, producer kind and contract version, algorithm/provider/model plus immutable revision when known, dimensions, `Float32`, normalization, distance, tokenizer/chunking contract, normalized hyperparameters/input recipe, and source projection recipe. Object keys sort by UTF-8 bytes; numbers use their normalized decimal form; arrays preserve order. Credentials, timestamps, aliases, and run IDs are excluded. |
| generation identity | SHA-256 of `compatibility identity || source fingerprint || canonical UUID/vector content digest`. UUID rows sort by raw bytes and vector coordinates use little-endian IEEE-754 Float32 bytes. Identical complete publication is therefore content-idempotent. |
| run identity | Optional knowledge-layer provenance identity for one computation or provider invocation. It may refer to a generation but never participates in compatibility, search ordering, or the analyst-verb/find canonical schema. |

One UUID may appear in any number of compatibility identities. A display name
points to one active compatibility lineage. Publishing incompatible output
under an occupied name fails; it never silently replaces or combines the old
lineage. The caller must choose a new name or explicitly reassign an alias after
the new complete generation commits. Replacement within a lineage atomically
switches the active generation. By default GraphForge retains the active and
immediately previous complete generation; callers may request a larger bounded
retention count. Deleting an alias does not delete generations. Deleting a
lineage is explicit, refuses active readers, and removes only that identity.

The persisted compatibility descriptor records these producer kinds exactly:
`m18`, `local`, `callback`, `remote`, and `caller_supplied`. `m18` includes the
algorithm version and graph projection fingerprint. `remote` includes provider,
model, immutable revision when exposed, and response-contract version. An
unknown immutable model revision is recorded as `unavailable`, not guessed.

### Source fingerprint and freshness

Every complete generation records the committed graph generation, selected
label membership digest, dependency recipe, source-property or topology digest,
eligible UUID count, and generated/committed timestamps. A mutation is relevant
only when it changes label membership or a topology/property input named by the
dependency recipe. Node deletion or label loss makes that UUID immediately
ineligible even while an older complete generation is retained.

Freshness state is reconstructed from durable graph and space metadata on every
open; process restart never resets it:

| State | Deterministic meaning | Ordinary search |
|---|---|---|
| `fresh` | Recorded source fingerprint equals the current dependency projection. | Serve. |
| `stale` | A known relevant mutation occurred, but neither substantial threshold below is met. | The last complete generation may be served while one refresh is queued. Ineligible UUIDs are filtered immediately. |
| `substantially_stale` | A structural space has any relevant topology mutation; or at least 5% of the recorded eligible UUIDs changed membership/input properties; or 128 relevant committed mutation batches accumulated; or mutation scope cannot be proven. | Refresh successfully first or return a structured error. |
| `incompatible` | Persisted schema, dimensions, normalization, distance, producer/model revision, tokenizer/chunking, or source-recipe identity does not match the requested compatibility identity. | Fail closed. |
| `corrupt` | Manifest/vector bytes, digest, count, UUID uniqueness, dimensions, or active-generation linkage fails validation. | Fail closed. |

For an empty recorded generation, any relevant mutation is substantial. The 5%
fraction is `changed_distinct_uuids / max(1, recorded_eligible_uuids)` using
checked integer comparison (`changed * 100 >= eligible * 5`), not floating
point. A mutation counted in multiple scopes contributes one distinct UUID and
one committed batch. Wall-clock age alone never changes freshness; timestamps
are diagnostic because an embedded project may remain closed indefinitely.

An explicit `force_stale` request may serve only the last complete
`substantially_stale` generation, after immediately filtering deleted or
label-ineligible UUIDs. It emits a stable diagnostic containing the space and
generation identities, source/current fingerprints, and staleness reason. It
cannot bypass `incompatible`, `corrupt`, missing-complete-generation, dimension,
tokenizer/model, or identity failures. `stale` does not require force.

### Atomic refresh lifecycle

A builder validates membership, UUID uniqueness, dimensions, finite non-zero
vectors, coverage policy, resource limits, and cancellation in a private
generation. It writes data first and the checksummed manifest last, fsyncs the
private tree and parent, then atomically swaps the active-generation pointer.
Failure, cancellation, crash, provider error, resource exhaustion, or graph
mutation leaves the previous complete generation as the sole visible one. A
source snapshot is rechecked immediately before publication; one full retry may
target the newest stable snapshot, after which concurrent mutation is a
structured error.

Lazy refresh runs before an ordinary substantially stale `index` or `find` can
serve. Proactive refresh is enabled by default while a `GraphForge` process is
open and can be disabled per project or space. Relevant mutations debounce for
500 ms. There is at most one active and one coalesced pending refresh per space,
the pending request always targets the newest durable fingerprint, and at most
two producer jobs run per project. Lazy demand takes priority over queued
proactive work without interrupting an active atomic build. Shutdown cancels
private work; reopen reconstructs durable state and resumes lazily or
proactively. No background-work promise exists while every process is closed.

Time, memory, temporary disk, UUID/vector cells, provider calls, tokens, retries,
and queue depth are bounded by typed Rust controls. Cancellation checkpoints
cover source reads, producer batches, validation, writes, publication, retrieval,
and reranking. No path returns partial rows or exposes a private generation.

### Providers, tokenization, and reranking

Provider interfaces are statically distinct for document/node embeddings,
query embeddings, and reranking. Offline analyst-verb, local, callback, and
caller-supplied workflows require no remote dependency. When and only when the
caller selects remote inference without naming a provider, the normalized
provider is `openrouter`. This is a visible default, never a runtime fallback;
missing credentials, unsupported capability, or provider failure is a
structured error.

Before outbound work, the caller can inspect and must explicitly select the
properties/text that may leave the machine. The resolved contract exposes
provider/model/revision, tokenizer identifier/version (or `unavailable`), count
class (`exact_local`, `provider_reported`, or `approximate`), model input limit,
normalization, chunk size, overlap, aggregation, truncation policy, item/token
budgets, batching, rate, timeout, retry, and spend bounds. Silent truncation is
forbidden: oversized input uses an explicit versioned chunking/aggregation
policy or fails before a request. These fields participate in compatibility;
approximate counts never claim provider-token equivalence.

Credentials are runtime-only. Manifests, Arrow results, logs, errors, snapshots,
and diagnostics never contain credentials, raw source text, vectors, provider
payloads, or knowledge/epistemic confidence, provenance, evidence, assertions, reasoning,
hypothesis state, or valid time. Remote endpoints are allowlisted/configured,
redirect and response sizes are bounded, and provider/model fallback is never
silent.

Canonical text, vector, and `rrf@1` retrieval remains the deterministic default.
Reranking is an explicit post-retrieval operation over a bounded candidate set.
Its provider/model/tokenizer/input-shaping identity is auditable without adding
conditional Arrow columns. If a compatible reranker is configured but omitted,
bindings may emit a suppressible advisory; rows, scores, schema, and order remain
byte-equivalent. Reranker failure returns a structured error unless the caller
explicitly selected a policy named `canonical_unreranked`.

### Required completion evidence

Implementation must cover multiple spaces on one UUID, identical publication,
incompatible-name collision, atomic replacement failure, relevant mutation,
each freshness transition, forced stale filtering, reopen, coalesced refresh,
concurrent mutation, cancellation, corrupt manifests, provider capability and
redaction, token limits/chunking, and explicit/omitted reranking. Rust owns the
state machine and fake-provider tests; fresh-native Python and Node tests prove
thin, identical behavior. epistemic-equivalent pre-resolved UUID scopes must yield the
same graph-native search result and may attach knowledge only after search.
