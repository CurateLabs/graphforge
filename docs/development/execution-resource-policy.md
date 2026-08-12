# Embedded Execution Resource Policy (#337)

GraphForge exposes one Rust-owned, per-instance
[`ExecutionResourcePolicy`](../../crates/graphforge-api/src/resource_policy.rs)
on [`GraphForgeOptions::resource`](../../crates/graphforge-api/src/write_modes.rs).
The facade normalizes and validates the policy **before** creating the long-lived
Tokio runtime or any DataFusion execution session.

## What it covers

| Knob | Default (Explicit) | Applied to |
|---|---|---|
| `tokio_worker_threads` | `2` | Facade multi-thread Tokio runtime |
| `target_partitions` | `2` | DataFusion `SessionConfig` |
| `batch_size` | `8192` | DataFusion `SessionConfig`; analyst Arrow shaping / property enrichment (#341) |
| `memory_budget_bytes` | `512 MiB` | DataFusion `RuntimeEnv` memory pool |
| `spill` | disabled | Optional absolute spill directory + byte cap |
| `io_concurrency` | `2` | Reserved I/O concurrency budget |
| `max_concurrent_heavy_queries` | `64` | Instance-owned admission semaphore |
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #501 betweenness; #503 closeness BFS; #504 clustering coefficient; #506 Degree; #515 triangles; #518 Components; #535 Jaccard similarity) |


Defaults preserve pre-#337 fixed two-worker / two-partition behavior.

## Modes

- **Explicit** — caller-supplied knobs (omitted fields use the defaults above).
- **Automatic** — derive a bounded configuration from
  `std::thread::available_parallelism()`:
  - ≤2 logical CPUs → serial/minimal (`1` worker, `1` partition)
  - otherwise → `ceil(cpus/2)` clamped to `[1, 8]`

Automatic records the selected values on
[`NormalizedResourcePolicy`](../../crates/graphforge-api/src/resource_policy.rs)
(`observed_logical_cpus`, workers, partitions). Timing remains hardware-specific
evidence, never a CI threshold.

## Validation (fail closed)

Normalization returns structured [`GfError::Validation`](../../crates/graphforge-core)
(or `GF_RESOURCE_LIMIT` for admission) when:

- thread counts are outside `1..=256`
- combined Tokio + partition concurrency exceeds the machine-relative budget
  (`min(max(4, 2×cpus), 512)`)
- reserved `io_concurrency` / `compute_threads` exceed
  `max(tokio_workers, observed_cpus)`
- spill is enabled without an absolute, non-symlink directory
- spill directory/max_bytes are set while spill is disabled
- memory / batch / heavy-query bounds are out of range

Invalid settings never partially construct a runtime or authoritative spill
state.

## Application

1. `GraphForgeOptions::validate` → `(options, NormalizedResourcePolicy)`
2. Facade stores the normalized policy + `HeavyQueryAdmission`
3. `build_runtime(&policy)` sets Tokio worker threads
4. Every DataFusion `ExecutionSession` receives a
   [`SessionResourceConfig`](../../crates/graphforge-exec/src/lib.rs) with
   partitions, batch size, memory, optional spill, and `io_concurrency`
5. Heavy ops (`run_query`, streams construction, `rank`, `similar`,
   `analyze_embedding`) take an admission permit
6. Query-facing Parquet scans (`GraphForgeParquetExec`, #339) defer file I/O to
   `ExecutionPlan::execute`, emit batches sized from `batch_size`, declare
   natural file fragments, and acquire the session `io_concurrency` semaphore
   before opening each fragment

Resources are **instance-owned**, not process-global. `compute_threads` sizes a
private Rayon pool on each `GraphForge` instance. Exact cosine KNN / similarity
(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), exact
Jaccard node similarity (#535), local clustering coefficient (#504), triangle
ranking (#515), Degree (#506), betweenness Brandes source searches (#501), and
`cluster(by="components")` (#518), and transitive closure (#554) may partition
independent work across that pool above documented crossovers; work never uses
Rayon's process-global pool. Cosine dot products retain serial coordinate order,
PageRank keeps canonical contribution order with serial dangling/delta
reductions, Jaccard retains serial candidate order per source, clustering
coefficient merges node-range scores in canonical dense-ordinal order, triangles
merge node-owned counts by ascending dense ordinal, Degree merges node chunks in
dense ordinal order, betweenness reduces per-source dependency arrays in
canonical source order, Components merges worker-local forests in canonical
source-range order, Node2Vec skip-gram training stays serial, and transitive
closure merges source ranges canonically, so fingerprints match the one-thread

`cluster(by="components")` (#518) may partition independent work across that
pool above documented crossovers; work never uses Rayon's process-global pool. Maximum-cardinality matching (#557) is explicitly dispositioned serial because blossom/primal-dual search mutates ordered matching state.
Cosine dot products retain serial coordinate order, PageRank keeps canonical
contribution order with serial dangling/delta reductions, Jaccard retains
serial candidate order per source, clustering coefficient merges node-range
scores in canonical dense-ordinal order, triangles merge node-owned counts by
ascending dense ordinal, Degree merges node chunks in dense ordinal order,
betweenness reduces per-source dependency arrays in canonical source order,
Components merges worker-local forests in canonical source-range order, and
Node2Vec skip-gram training stays serial, so fingerprints match the one-thread

(#342), PageRank (#343), and Node2Vec walk-corpus generation (#344) may partition
independent work across that pool above documented crossovers; work never uses
Rayon's process-global pool. GraphSAGE training (#560) is explicitly
dispositioned serial because positive-pair replay, sampled computation graphs,
gradient accumulation, Adam moment updates, and final full-neighborhood
inference are one accepted state stream. Cosine dot products retain serial
coordinate order, PageRank keeps canonical contribution order with serial
dangling/delta reductions, Node2Vec skip-gram training stays serial, and
GraphSAGE keeps one-thread training order, so fingerprints match the one-thread
path.
(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
eigenvector destination updates (#507) may partition independent work across
that pool above documented crossovers; work never uses Rayon's process-global
pool. Cosine dot products retain serial coordinate order, PageRank keeps
canonical contribution order with serial dangling/delta reductions, eigenvector
keeps per-destination incoming contribution order with serial norm/convergence
reductions, and Node2Vec skip-gram training stays serial, so fingerprints match
the one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
common-neighbors source aggregates (#505) may partition independent work across
that pool above documented crossovers; work never uses Rayon's process-global
pool. Cosine dot products retain serial coordinate order, PageRank keeps
canonical contribution order with serial dangling/delta reductions, Node2Vec
skip-gram training stays serial, and common-neighbors keeps serial
candidate/intersection order per source, so fingerprints match the one-thread
path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and triad census
source-range enumeration (#587) may partition independent work across that pool
above documented crossovers; work never uses Rayon's process-global pool. Cosine
dot products retain serial coordinate order, PageRank keeps canonical
contribution order with serial dangling/delta reductions, Node2Vec skip-gram
training stays serial, and triad census merges integer class counts in canonical
source-range order, so fingerprints match the one-thread path.

(#342), PageRank (#343), and Node2Vec walk-corpus generation (#344) may partition
independent work across that pool above documented crossovers; work never uses
Rayon's process-global pool. Leiden (#527) is explicitly dispositioned serial
because local moves, refinement, fixed random sampling, and aggregation consume
the accepted state from the previous step. Cosine dot products retain serial
coordinate order, PageRank keeps canonical contribution order with serial
dangling/delta reductions, Node2Vec skip-gram training stays serial, and Leiden
keeps one-thread refinement order, so fingerprints match the one-thread path.

(#342), PageRank (#343), and Node2Vec walk-corpus generation (#344) may partition
independent work across that pool above documented crossovers; work never uses
Rayon's process-global pool. Maximum-weight matching (#558) is explicitly
dispositioned serial because exact weighted blossom labels, dual arithmetic,
contractions, expansions, and augmenting-path commits mutate one shared
alternating forest. Cosine dot products retain serial coordinate order,
PageRank keeps canonical contribution order with serial dangling/delta
reductions, Node2Vec skip-gram training stays serial, and max-weight matching
keeps one-thread weighted blossom state order, so fingerprints match the
one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and transitivity
triangle counting (#586) may partition independent work across that pool above
documented crossovers; work never uses Rayon's process-global pool. Cosine dot
products retain serial coordinate order, PageRank keeps canonical contribution
order with serial dangling/delta reductions, Node2Vec skip-gram training stays
serial, and transitivity merges integer triangle counts in canonical source-range
order, so fingerprints match the one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and conductance
row evaluation (#566) may partition independent work across that pool above
documented crossovers; work never uses Rayon's process-global pool. Cosine dot
products retain serial coordinate order, PageRank keeps canonical contribution
order with serial dangling/delta reductions, Node2Vec skip-gram training stays
serial, and conductance keeps weighted cut/volume accumulation serial, so
fingerprints match the one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
Adamic-Adar source aggregates (#499) may partition independent work across that
pool above documented crossovers; work never uses Rayon's process-global pool.
Cosine dot products retain serial coordinate order, PageRank keeps canonical
contribution order with serial dangling/delta reductions, Node2Vec skip-gram
training stays serial, and Adamic-Adar keeps serial candidate/intersection order
per source, so fingerprints match the one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and ArticleRank
(#500) may partition independent work across that pool above documented
crossovers; work never uses Rayon's process-global pool. Cosine dot products
retain serial coordinate order, PageRank keeps canonical contribution order with
serial dangling/delta reductions, ArticleRank keeps canonical per-destination
message order with serial score/delta updates, and Node2Vec skip-gram training
stays serial, so fingerprints match the one-thread path.

## Parallel cosine KNN (#342)

Exact, all-score, and filtered cosine partition **independent source rows**
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated multiply-adds (`sources × candidates × dimensions`, or the filtered
  candidate total × dimensions) are at least
  `COSINE_PARALLEL_CROSSOVER_OPS` (`32_768`) in `graphforge-exec`. That
  threshold is the smallest power-of-two multiply-add count at/above the measured
  win boundary on the M4 agent host (4 vCPU, adversarial fixture, 4 private
  workers, release build): ~16k ops still tax serial, ~36k ops first clear win,
  ≥65k ops ≥2×. Smaller workloads stay serial. Worker-local checkpoint counters
  avoid shared atomic contention on the multiply-add path.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Inner floating-point reductions
stay serial per source→target pair (no parallel dot products, no approximate
similarity). Worker outputs merge in canonical source order so schemas, row
order, scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel Jaccard node similarity (#535)

Exact and filtered `similar(by="node_similarity")` Jaccard partition
**independent source rows** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated source-degree candidate probes (source neighborhood size × candidate
  count, summed over non-empty sources) are at least
  `JACCARD_PARALLEL_CROSSOVER_OPS` (`16_384`) in `graphforge-exec`. That
  threshold is the smallest power-of-two probe count below the first stable win
  boundary on the M4 agent host (4 vCPU, adversarial set fixture, 4 private
  workers, release build): ≤4.3k probes are noise-dominated, ~17k probes first
  stable win (~0.65× serial), ≥48k probes ≥2×. Smaller workloads stay serial.
  Worker-local checkpoint counters avoid shared atomic contention on the
  candidate path.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Each worker preserves the existing
serial behavior for candidate validation, duplicate suppression, set
intersection, top-k ordering, and tie-breaking. Worker outputs merge in
canonical source order so schemas, row order, scores, ties, and fingerprints
match the one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel PageRank (#343)

PageRank destination updates run on the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `PAGERANK_PARALLEL_CROSSOVER_EDGES` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial source-scatter path runs with no pool scheduling tax. Parallel work is
destination-owned: each worker owns a contiguous dense-ordinal destination
range and applies inbound contributions in the same canonical source/edge order
as serial scatter. Dangling-mass and L1 convergence reductions stay serial so
IEEE accumulation order matches the accepted oracle. Schemas, row order,
scores, iteration counts, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel clustering coefficient (#504)

Local clustering coefficient partitions **independent dense-ordinal node
ranges** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated local neighbor-pair probes are at least
  `CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK` (`32_768`) in
  `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial node-order path runs with no pool scheduling tax. Each worker computes
complete node-local triangle/wedge scores using the same canonical directed
simple adjacency and the same per-node integer arithmetic as the one-thread
path. Worker outputs merge by ascending node range, so schemas, row order,
scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations. The crossover is a conservative
structural cutoff for embedded CPU work; timing remains hardware-specific
evidence, not a universal scale claim.

## Parallel triangles (#515)

`rank(by="triangles")` normalizes the selected graph to the same simple
undirected neighbor lists as the serial path, then partitions **node-owned
triangle counts** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected node count is at least `TRIANGLES_PARALLEL_CROSSOVER_NODES` (`256`)
  in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
original serial nested-neighbor path runs with no pool scheduling tax. Parallel
workers own contiguous dense-ordinal node ranges, keep each node's neighbor-pair
scan serial, and return worker-local score vectors. Results merge in ascending
range order, so schemas, row order, counts, and fingerprints match the
one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel Degree (#506)

Normalized degree scores partition **independent dense node ordinals** across
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected node count is at least
  `DEGREE_PARALLEL_CROSSOVER_NODES` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Workers emit `(uuid, score)` rows
for contiguous ordinal ranges; the sink merges them in ascending node order so
schemas, row order, scores, and fingerprints match the one-thread result at

## Parallel ArticleRank (#500)

ArticleRank destination message sums run on the instance-owned private compute
pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES` (`131_072`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the serial
destination-pull path runs with no pool scheduling tax. Parallel work is
destination-owned: each worker owns a contiguous dense-ordinal destination range
and applies inbound messages in the same canonical source/edge order as the
one-thread recurrence. Score accumulation, delta damping, and convergence
checks remain serial in dense node order, so schemas, row order, scores,
iteration counts, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.
## Parallel eigenvector (#507)

`rank(by="eigenvector")` shifted power-iteration destination updates may run on
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `EIGENVECTOR_PARALLEL_CROSSOVER_EDGES` (`8_192`) in `graphforge-exec`.

Release-mode local evidence on the 4-worker M4 agent host showed regular graphs
that converge during warm-up avoid the pool, while irregular non-converged
fixtures crossed over by ~8K selected adjacency entries:
`8_689`, `24_440`, `65_505`, and `130_544` edge irregular fixtures repeatedly
ran at roughly `0.4×–0.6×` of the one-thread time on four private workers.
Those timings are hardware-specific evidence, not a CI gate.

Below that crossover, or when the policy provides one compute thread, the
serial source-scatter path runs with no pool scheduling tax. Above the
crossover, the first two required power iterations also stay serial; if the
workload converges there, no inbound CSR or worker scheduling tax is paid.
Remaining non-converged work is destination-owned: each worker owns a
contiguous dense-ordinal destination range and applies inbound contributions
after the implicit identity term in the same canonical source/edge order as
serial scatter. L2 normalization and component-wise convergence checks stay
serial, so scores, iteration counts, and fingerprints match the one-thread
result at `1`/`2`/`4`/`8`/automatic configurations.
## Parallel HITS hub (#510)
## Parallel HITS hub / authority (#510 / #509)

The shared `hits_scores` kernel used by `rank(by="hits_hub")` prepares selected
and `rank(by="hits_authority")` prepares selected adjacency once as
dense-ordinal outgoing and incoming CSR, then partitions independent node-score
updates across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected adjacency entries are at least
  `HITS_PARALLEL_CROSSOVER_EDGES` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Parallel work is node-owned: the
authority phase owns contiguous target ranges over incoming CSR, and the hub
phase owns contiguous source ranges over outgoing CSR. Each node's
floating-point sum still walks its CSR slice in canonical source/edge order,
and global L2 norms remain serial dense-ordinal reductions, so schemas, row
order, scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations. The crossover follows the same
4 vCPU release-build structural benchmark regime used for adjacent M4 kernels:
the parallel path avoids small-fixture tax and clears the worker-pool cost once
20 fixed HITS iterations amortize two sparse matrix-vector phases per
iteration. It is CPU-only; no GPU or universal scaling claim is made.

The threshold is the first measured win on the M4 agent host using the ignored
release harness (`measure_article_rank_parallel_crossover`) with a shared
four-worker private pool: sizes through 32k selected entries stayed near parity
within timing noise, while 131k and 262k selected entries were clear wins over
the one-thread path.

## Parallel Node2Vec walk generation (#344)

Walk-corpus construction partitions **independent `(start ordinal, walk
ordinal)` tasks** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated transitions (`starts × walks_per_node × walk_length`) are at least
  `NODE2VEC_WALK_PARALLEL_CROSSOVER` (`256`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Worker-local token counts merge
by canonical node ordinal with checked addition. Training order and arithmetic
are unchanged, so schemas, row order, metadata, and fingerprints match the
one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel betweenness Brandes BFS (#501)

`rank(by="betweenness")` partitions independent Brandes **source** searches
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated source-search work
  (`selected_nodes × (selected_nodes + selected_adjacency_entries)`) is at least
  `BETWEENNESS_PARALLEL_CROSSOVER_WORK` (`65_536`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
serial source loop runs with no pool scheduling tax. The crossover is the
smallest power-of-two work estimate at/above the measured win boundary on the
M4 agent host (4 vCPU, directed chord fixture, 4 private workers, release
build): sub-32k source-search work remains noise dominated, while fixtures above
64k first amortize pool scheduling.

Each worker runs a complete serial Brandes BFS/dependency pass for every source
it owns; no individual BFS frontier, predecessor list, or dependency array is
parallelized. Worker outputs carry one dependency contribution array per source.
The caller replays cooperative checkpoints and accumulates those arrays in
ascending source ordinal order, matching the one-thread floating-point addition
order. Worker loops use cancellation checks rather than shared checkpoint
mutation; cancellation and limit failures remain structured. Schemas, row order,
scores, ties, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Parallel Components (#518)

`cluster(by="components")` partitions **independent source-node adjacency scans**
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- selected direction-expanded adjacency entries are at least
  `COMPONENTS_PARALLEL_CROSSOVER_EDGES` (`16_384`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the serial
union-find path runs with no pool scheduling or worker-local map setup cost. The
crossover is the measured M4 boundary where chunk-local union-find begins to
amortize Rayon scheduling and deterministic merge overhead on multi-component
fixtures.

Parallel work is source-range-owned: each worker builds a local min-root
union-find forest for the edges whose source ordinal falls in its chunk, then the
main thread merges those local links by ascending source range. Final component
IDs are still assigned by canonical node order, so schemas, row order, labels,
and fingerprints match the one-thread result at `1`/`2`/`4`/`8`/automatic
configurations. Cancellation remains cooperative both before worker launch and
inside chunk scans.
## Serial paths(by="gomory_hu_tree") (#544)

`paths(by="gomory_hu_tree")` has no parallel crossover. Its disposition is
**serial Gomory-Hu parent updates** for every `compute_threads` setting,
including `1`/`2`/`4`/`8`/automatic policies.

The Rust kernel consumes CSR-native undirected capacity adjacency (#340), finds
canonical connected components, and runs the classic parent-update sequence.
Each min-cut result rewrites parent links that choose subsequent cut pairs and
final forest rows. Launching those cuts independently would compute against the
wrong parent state and could change cut values, row order, and fingerprints.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and no process-global Rayon pool. The M4 harness records
`paths-gomory-hu-tree` evidence and verifies one-thread parity across supported
resource-policy cells; timing remains evidence only.

## Parallel transitive closure (#554)

`paths(by="transitive_closure")` partitions independent source-node traversals
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated traversal work (`sources × direction-expanded adjacency entries`) is
  at least `TRANSITIVE_CLOSURE_PARALLEL_CROSSOVER_WORK` (`65_536`) in
  `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the serial
per-source breadth-first traversal runs with no pool scheduling tax. Parallel
workers preserve the serial traversal inside each source, sort reachable targets
by public UUID, and merge worker outputs by ascending canonical source range.
Schemas, row order, reachable-pair sets, and fingerprints match the one-thread
result at `1`/`2`/`4`/`8`/automatic configurations.

## Parallel triad census source ranges (#587)

`analyze(by="triad_census")` keeps UUID indexing and directed-neighbor
normalization serial so duplicate-edge validation, self-loop elision, and
canonical node ordering remain unchanged. The Batagelj-Mrvar enumeration then
partitions independent source-ordinal ranges across the instance-owned private
compute pool when:

- `compute_threads > 1`, and
- normalized weak dyads are at least
  `TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS` (`4_096`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, triad
census stays serial. Each worker builds the same per-dyad union sets as the
serial path for its source range and returns worker-local `u64` counts for the 16
MAN classes. Chunk results merge in ascending source-range order before deriving
the closed-form `003` count and validating the `V choose 3` invariant. Schemas,
row order, class counts, structured errors, and fingerprints match the one-thread
result at `1`/`2`/`4`/`8`/automatic configurations.

## Serial k-core decomposition (#523)

`cluster(by="k_core_decomposition")` is intentionally SERIAL. The exact
core-number peel mutates node degrees through a canonical min-heap; each removal
changes later heap priorities and stale-entry rejection. Splitting that frontier
across workers would either change tie order/core assignment evidence or add a
global synchronization point around every peel, leaving no safe independent
kernel for the private compute pool.

The handler keeps the Rust-owned projection path and bounded Arrow sink, avoids
the Rayon global pool, and observes shared graph/output/iteration limits. A
fingerprint test attaches private compute pools for `1`/`2`/`4`/`8` configured
compute threads and requires identical schemas, row ordering, core labels, and
rows.

## Serial Leiden modularity optimization (#527)

`cluster(by="leiden")` has no parallel crossover. Its performance disposition is
**serial local-move/refinement/aggregation** for every `compute_threads`
setting, including `1`/`2`/`4`/`8`/automatic resource policies.

Each Leiden level first runs topology-ordered positive-gain local moves, then
refines coarse communities through connected subcommunities using the fixed
Rust random stream and accepted membership state, then aggregates the refined
partition while seeding the next level from the coarse partition. Reordering
candidate moves, random draws, or aggregation updates would require a new
numeric and tie contract and could change connected-community guarantees,
community IDs, row ordering, or fingerprints.

The #527 disposition therefore preserves the serial contract, CSR-native
selected adjacency access, shared cancellation/checkpoint controls, structured
resource errors, and bounded Arrow shaping. Schemas, row order, community IDs,
projection fingerprints, cancellation, and limit behavior match the one-thread
oracle at supported thread configurations. No GPU, distributed, approximate, or
foreign-engine fallback is implied.

## Serial bridges (#564)

`analyze(by="bridges")` is intentionally SERIAL. Bridge classification depends
on the shared low-link DFS traversal: discovery order, parent-edge identity,
multigraph parallel-edge handling, and low-link propagation determine whether an
edge is a bridge. Parallelizing that state would change the canonical edge
evidence or require synchronization around each DFS transition, so there is no
safe independent kernel for the private compute pool.

The handler keeps Rust-owned adjacency projection and bounded Arrow output,
does not use Rayon's global pool, and preserves cancellation and shared resource
limits. A fingerprint test attaches private compute pools for `1`/`2`/`4`/`8`
configured compute threads and requires identical schemas, bridge ordering, and
rows.

## Serial paths(by="dijkstra") (#541)

`paths(by="dijkstra")` for one source, with or without one target, has no
parallel crossover. Its disposition is **serial non-negative shortest-path
relaxation** for every `compute_threads` setting. The all-pairs variant is
tracked independently by #542.

The Rust kernel validates non-negative finite weights, then repeatedly pops one
canonical heap entry, compares it with the current best-path map, and relaxes
stable outgoing edges. The public target early exit and equal-cost path-vector
and edge-UUID ties depend on that exact pop order. Parallel relaxations would
need to arbitrate competing predecessors and could alter paths, row order, or
fingerprints.

The path still uses CSR-native selected adjacency, bounded Arrow shaping,
structured cancellation and limit checks, and no process-global Rayon pool.
Costs, selected node sequences, unreachable-target behavior, structured errors,
and cancellation behavior match the one-thread oracle across supported
resource-policy cells.

## Serial maximum-weight matching (#558)

`analyze(by="max_weight_matching")` has no parallel crossover. Its performance
disposition is **serial exact weighted blossom search** for every
`compute_threads` setting.

The weighted handler normalizes the selected undirected multigraph, validates
finite weights, and uses the shared exact blossom/primal-dual core with summed
Float64 weights as the primary objective, cardinality as a secondary objective,
and canonical edge tuples for ties. Labels, root queues, exact-weight dual
steps, blossom contractions/expansions, and augmenting-path commits all update
one alternating forest. Parallel speculation would need conflict resolution
across shared vertices/blossoms and could change the selected maximum-weight
edge set or tie objective.

The #558 disposition preserves CSR-native selected adjacency access before
normalization, shared cancellation/checkpoint controls, structured resource
errors, and bounded Arrow shaping. Schemas, row order, selected edge UUIDs and
weights, projection fingerprints, cancellation, and limit behavior match the
one-thread oracle at supported thread configurations. No GPU, distributed,
approximate, or foreign-engine fallback is implied.

## Parallel transitivity triangle counting (#586)

`analyze(by="transitivity")` keeps UUID indexing, undirected simple-neighbor
normalization, and the wedge denominator serial. Those steps validate duplicate
edge UUIDs, self-loops, endpoint membership, and the zero-wedge early return
before any pool scheduling. The triangle-closure scan then partitions independent
source-ordinal ranges across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- closed-wedge candidates are at least
  `TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES` (`32_768`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread,
transitivity stays serial. Parallel workers return local `u64` triangle counts;
chunks merge in ascending source-range order before the single final `3T / wedge`
floating-point division. Schemas, scalar row shape, ratio bits, structured
errors, and fingerprints match the one-thread result at `1`/`2`/`4`/`8`/automatic
configurations.

## Parallel conductance row evaluation (#566)

`analyze(by="conductance")` keeps weighted edge normalization, cut accumulation,
and volume accumulation serial. Those stages contain floating-point additions
whose order is part of the accepted bit-level contract. After the serial
accumulators are complete, each partition row can be evaluated independently on
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- normalized partitions are at least
  `CONDUCTANCE_PARALLEL_CROSSOVER_PARTITIONS` (`128`) in `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread,
conductance stays fully serial. Parallel workers evaluate canonical partition
ranges; each row preserves the serial `BTreeMap` complement summation order, and
chunks merge in ascending range order so row ordering and the first undefined
partition error remain deterministic. Schemas, row order, conductance bits,
structured errors, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.
## Serial depth-first search (#540)

`paths(by="dfs")` is intentionally SERIAL. The public result is preorder with a
stable discovery index and depth. That order is defined by a single stack over
sorted neighbor lists; parallelizing frontier expansion would change discovery
order, depth ties, or require serial reassembly of every stack mutation.

The handler uses the Rust-owned adjacency projection and bounded Arrow sink,
does not use Rayon's global pool, and preserves shared cancellation and limits.
A fingerprint test attaches private compute pools for `1`/`2`/`4`/`8`
configured compute threads and requires identical schemas, preorder rows,
depths, and discovery ordinals.

## Parallel common-neighbors aggregate (#505)

`rank(by="common_neighbors")` partitions **independent source ordinals** across
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- the estimated pair/intersection work is at least
  `COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK` (`1_048_576`) in
  `graphforge-exec`.

The estimate is `sources² + 2 × sources × distinct_adjacency_entries`, a
conservative O(V + E) proxy for the serial pair loop and two-pointer
intersection scans. The threshold is the smallest power-of-two work estimate
below the measured win boundary on the M4 agent host (4 vCPU, directed
ring-lattice fixture, 4 private workers, debug test profile after a clean
target-dir build): ~230k units was still slower (~1.80x serial), ~540k units
was still slower (~1.20x serial), ~1.2M units first won (~0.70x serial), and
>=2.1M units was >=1.8x faster.
## Parallel Adamic-Adar aggregate (#499)

`rank(by="adamic_adar")` partitions **independent source ordinals** across the
instance-owned private compute pool when:

- `compute_threads > 1`, and
- the estimated pair/intersection work is at least
  `ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK` (`524_288`) in `graphforge-exec`.

The estimate is `sources² + 2 × sources × distinct_adjacency_entries`, a
conservative O(V + E) proxy for the serial pair loop and two-pointer
intersection scans. The threshold is the smallest power-of-two work estimate
below the measured win boundary on the M4 agent host (4 vCPU, directed
ring-lattice fixture, 4 private workers, release build): ~230k units was still
neutral/slower, ~540k units first won (~0.61x serial), and >=2.1M units was
>=2.8x faster.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Parallel workers only own source
ranges. Candidate order, missing-link checks, two-pointer neighborhood
intersection, and checked integer accumulation remain serial per source.
Worker score chunks merge in canonical source order, so schemas, row order,
scores, and fingerprints match the one-thread result at `1`/`2`/`4`/`8`/
automatic configurations.
intersection, logarithmic discounts, and compensated floating-point summation
remain serial per source. Worker score chunks merge in canonical source order,
so schemas, row order, scores, and fingerprints match the one-thread result at
`1`/`2`/`4`/`8`/automatic configurations.

## Serial articulation points (#563)

`analyze(by="articulation_points")` is intentionally SERIAL. It shares the
low-link DFS kernel used for cut-vertex and bridge classification; discovery
indices, parent edges, root child counts, and low-link propagation are ordered
state. Parallelizing those updates would change canonical cut-vertex evidence or
require synchronization at each DFS step, which does not provide safe
independent work for the private compute pool.

The handler keeps Rust-owned adjacency projection and bounded Arrow output,
does not use Rayon's global pool, and preserves shared cancellation and
resource-limit behavior. A fingerprint test attaches private compute pools for
`1`/`2`/`4`/`8` configured compute threads and requires identical schemas,
cut-vertex ordering, and rows.

## Serial GraphSAGE training (#560)

`analyze_embedding(by="graphsage")` keeps the GraphSAGE-v1 training and final
inference path serial under every `compute_threads` setting. The operation
replays positive pairs in start UUID / walk / transition order, builds sampled
role-path computation graphs for the center, positive, and negative examples,
accumulates one binary64 gradient tensor, and updates Adam moments in layer /
output-coordinate / input-coordinate order. Each accepted pair changes the
parameter and moment state consumed by the next pair, so private-pool
speculation would require a new reduction contract and could change binary64
Adam state, final Float32 embeddings, and fingerprints.

The #560 disposition therefore preserves the serial numeric contract and records
thread-policy parity rather than claiming a crossover. The Rust path consumes
the normalized projection, shared resource preflight, cooperative cancellation,
structured work/output errors, and bounded Arrow shaping already used by the
embedding surface. Schemas, row order, fingerprints, cancellation, and limit
behavior match the one-thread oracle at `1`/`2`/`4`/`8`/automatic
configurations. No GPU, distributed, approximate, or foreign-engine fallback is
implied.

## Serial maximum-cardinality matching (#557)

`analyze(by="max_cardinality_matching")` has no parallel crossover. Its
performance disposition is **serial exact blossom search** for every
`compute_threads` setting.

The unweighted wrapper normalizes the selected undirected multigraph, then uses
the shared exact blossom/primal-dual core with cardinality as the primary
objective and raw edge UUIDs as the deterministic tie order. Labels, root
queues, blossom contractions/expansions, dual steps, and augmenting-path commits
all update one alternating forest. Parallel speculation would need conflict
resolution across shared vertices/blossoms and could change the chosen maximum
matching or raw-edge UUID tie objective.

The #557 disposition preserves CSR-native selected adjacency access before
normalization, shared cancellation/checkpoint controls, structured resource
errors, and bounded Arrow shaping. Schemas, row order, selected edge UUIDs,
projection fingerprints, cancellation, and limit behavior match the one-thread
oracle at supported thread configurations. No GPU, distributed, approximate, or
foreign-engine fallback is implied.

## Serial paths(by="max_flow_edges") (#546)

`paths(by="max_flow_edges")` has no parallel crossover. Its disposition is
**serial Edmonds-Karp edge assignment** for every `compute_threads` setting,
including `1`/`2`/`4`/`8`/automatic resource policies.

The scalar and per-edge maximum-flow views share one Rust kernel. The kernel
consumes the CSR-native projection (#340), sorts capacities by public edge UUID,
then performs one canonical residual BFS and residual update at a time. The
edge view is not independent post-processing: signed stored-orientation flow is
updated during each augmentation and emitted only after the final residual state
is known. Parallel augmentations would require shared residual coordination and
would risk changing edge-flow ties, row ordering, and fingerprints.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and never uses Rayon's process-global pool. Timing remains
evidence only, not a CI threshold.

## Serial paths(by="astar") (#536)

`paths(by="astar")` has no parallel crossover. Its disposition is **serial
priority-queue A*** for every `compute_threads` setting, including
`1`/`2`/`4`/`8`/automatic policies.

The Rust kernel consumes CSR-native weighted adjacency and graph-native
heuristic values (#340), then drives one priority queue ordered by estimate,
cost, path, and edge ID. Each pop validates and mutates the best-path map that
subsequent relaxations observe. Parallel relaxations would need shared heap and
best-map coordination and could change accepted ties, cancellation points, or
fingerprints.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and no process-global Rayon pool. The M4 harness records
`paths-astar` evidence and verifies one-thread parity; timing is report-only.

## Serial paths(by="min_cut") (#549)

`paths(by="min_cut")` has no parallel crossover. Its disposition is
**serial constrained min-cut oracle** for every `compute_threads` setting,
including `1`/`2`/`4`/`8`/automatic resource policies.

The Rust kernel consumes CSR-native capacity adjacency (#340), computes the
minimum value through the shared serial max-flow oracle, then constructs the
canonical source-side partition one UUID at a time. Every include/exclude
decision runs a constrained cut-value oracle against the partition forced by
previous decisions. That dependency chain fixes tie behavior and row ordering;
parallelizing it would require speculative shared residual state and could
change the accepted cut fingerprint.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and never uses Rayon's process-global pool. The M4 entry
harness records `paths-min-cut` structural evidence and verifies one-thread
fingerprint parity for supported resource-policy cells. Timing is evidence
only, never a CI threshold.

## Serial analyze(by="minimum_spanning_tree") (#582)

`analyze(by="minimum_spanning_tree")` has no parallel crossover. Its
disposition is **serial Kruskal ascending union-find** for every
`compute_threads` setting, including `1`/`2`/`4`/`8`/automatic policies.

The Rust kernel consumes CSR-native weighted undirected adjacency (#340),
collapses mirrored entries by edge UUID, performs a stable canonical edge sort,
and accepts candidates through one union-find state. Each accepted edge changes
component state for all later edges, while equal-weight edge UUID ties are part
of the public fingerprint. Parallel acceptance would need shared union-find
coordination and could change rows or cancellation boundaries.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and no process-global Rayon pool. The M4 harness records
`analyze-minimum-spanning-tree` evidence and verifies one-thread parity; timing
is report-only.

## Serial paths(by="min_steiner_tree") (#551)

`paths(by="min_steiner_tree")` has no parallel crossover. Its disposition is
`serial_exact_subset_steiner_search`: exact subset search keeps one global best
candidate, and cost / fewer-edge / edge-UUID ties make branch order and pruning
part of the deterministic public contract.

Acceptance for #551 is documented here and covered by the short CI matrix
`paths-min-steiner-tree` evidence with one-thread parity across supported worker
counts.

## Serial paths(by="bellman_ford") (#537)

`paths(by="bellman_ford")` has no parallel crossover. Its disposition is
**serial ordered Bellman-Ford relaxation** for every `compute_threads` setting,
including `1`/`2`/`4`/`8`/automatic policies.

The Rust kernel consumes CSR-native weighted adjacency (#340) and applies
relaxation rounds in canonical node and edge order. Each successful relaxation
mutates the best-path map used by later relaxations, tie comparisons, and the
reachable negative-cycle scan. Parallel relaxations would require shared map
coordination and could alter accepted paths, row order, or fingerprints.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and no process-global Rayon pool. The M4 harness records
`paths-bellman-ford` evidence and verifies one-thread parity; timing is
report-only.

## Serial paths(by="min_cost_max_flow") (#547)

`paths(by="min_cost_max_flow")` has no parallel crossover. Its disposition is
**serial Bellman-Ford residual augmentation** for every `compute_threads`
setting, including `1`/`2`/`4`/`8`/automatic resource policies.

The Rust kernel consumes CSR-native capacity and cost projections (#340), sorts
public identities canonically, rejects undefined negative residual cycles, and
then augments along one Bellman-Ford shortest residual path at a time. Each
augmentation mutates residual capacities and accumulated cost before the next
path is chosen, so the scalar optimum is a sequential state machine rather than
independent work for the private compute pool.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and never uses Rayon's process-global pool. The M4 harness
records `paths-min-cost-max-flow` structural evidence and verifies parity
against the one-thread oracle for supported resource-policy cells. Timing is
hardware-specific evidence only.

## Observability

`GraphForge::resource_policy()` and `GraphForge::resource_diagnostics()` expose
safe aggregates: mode, workers, partitions, batch size, memory, spill flag,
I/O / compute budgets, heavy-query limit and available permits, and observed
CPUs. Diagnostics never include query text, parameters, UUIDs, properties,
graph contents, or local project paths.

## Bindings

Python and Node keep existing construction defaults via
`..Default::default()` / `resource: defaults.resource`. No new public binding
knobs are required for #337.

## Thread-parity matrix

The M4 harness executes contract cells `threads-1` / `2` / `4` / `8` /
`threads-automatic` through the public Rust facade under this policy:

```bash
export CARGO_TARGET_DIR=/tmp/gf-m4-337-target
cargo test -p graphforge-api --test m4_entry_baseline \
  thread_parity_matrix_executes_under_resource_policy -- --nocapture
make m4-entry-matrix-check
```

Configurations that exceed the host concurrency budget are recorded
`unavailable` with a reason; results are never fabricated. Executed cells must
share schemas, ordering, fingerprints, structured error codes, and LIMIT row
counts.

## Related docs

- [M4 Entry Baseline](m4-entry-baseline.md)
- [Scale Limits](../reference/scale-limits.md)
- Contract: [`tests/contracts/m4-entry-matrix.json`](../../tests/contracts/m4-entry-matrix.json)

| Knob | Default (Explicit) | Applied to |
|---|---|---|
| `tokio_worker_threads` | `2` | Facade multi-thread Tokio runtime |
| `target_partitions` | `2` | DataFusion `SessionConfig` |
| `batch_size` | `8192` | DataFusion `SessionConfig`; analyst Arrow shaping / property enrichment (#341) |
| `memory_budget_bytes` | `512 MiB` | DataFusion `RuntimeEnv` memory pool |
| `spill` | disabled | Optional absolute spill directory + byte cap |
| `io_concurrency` | `2` | Reserved I/O concurrency budget |
| `max_concurrent_heavy_queries` | `64` | Instance-owned admission semaphore |
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #535 Jaccard similarity; #504 clustering coefficient; #515 triangles; #506 Degree; #501 betweenness; #518 Components; #503 closeness BFS) |
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #507 eigenvector) |

## Parallel closeness BFS (#503)
