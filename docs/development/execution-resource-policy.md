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
| `compute_threads` | `2` | Instance-owned private CPU pool (#342 cosine KNN; #343 PageRank; #344 Node2Vec walks; #501 betweenness; #503 closeness BFS; #504 clustering coefficient; #506 Degree; #510 HITS hub; #513 resource allocation; #515 triangles; #518 Components; #534 filtered Jaccard; #535 Jaccard similarity; #542 Dijkstra APSP sources) |

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
`cluster(by="components")` (#518) may partition independent work across that
pool above documented crossovers; work never uses Rayon's process-global pool.
Cosine dot products retain serial coordinate order, PageRank keeps canonical
contribution order with serial dangling/delta reductions, Jaccard retains
serial candidate order per source, clustering coefficient merges node-range
scores in canonical dense-ordinal order, triangles merge node-owned counts by
ascending dense ordinal, Degree merges node chunks in dense ordinal order,
betweenness reduces per-source dependency arrays in canonical source order,
Components merges worker-local forests in canonical source-range order, and
Node2Vec skip-gram training stays serial, so fingerprints match the one-thread
path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
`paths(by="dijkstra_all_pairs")` source searches (#542) may partition
independent work across that pool above documented crossovers; work never uses
Rayon's process-global pool. Cosine dot products retain serial coordinate order,
PageRank keeps canonical contribution order with serial dangling/delta
reductions, Node2Vec skip-gram training stays serial, and each Dijkstra source
search remains serial, so fingerprints match the one-thread path.

(#342), PageRank (#343), Node2Vec walk-corpus generation (#344), and
resource-allocation source aggregates (#513) may partition independent work
across that pool above documented crossovers; work never uses Rayon's
process-global pool. Cosine dot products retain serial coordinate order,
PageRank keeps canonical contribution order with serial dangling/delta
reductions, Node2Vec skip-gram training stays serial, and resource allocation
keeps serial candidate/intersection order per source, so fingerprints match the
one-thread path.

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

`filtered_node_similarity` shares this private-pool path and `JACCARD_PARALLEL_CROSSOVER_OPS` crossover; filtered candidate sets are per-source neighborhoods, and workers merge in source order for one-thread oracle parity (#534).

Exact `similar(by="node_similarity")` (#535) and `similar(by="filtered_node_similarity")` (#534) Jaccard partition
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

## Parallel Dijkstra all-pairs sources (#542)

`paths(by="dijkstra_all_pairs")` partitions **independent source nodes** across
the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated source-edge inspections (`selected_nodes × CSR adjacency entries`)
  are at least `DIJKSTRA_APSP_PARALLEL_CROSSOVER_WORK` (`8_192`) in
  `graphforge-exec`.

Below that crossover, or when the policy provides one compute thread, the
existing serial source loop runs with no pool scheduling tax. Above the
crossover, each worker runs the same single-source Dijkstra serially: heap
ordering, equal-cost path ties, edge order, cost accumulation, cancellation
checkpoints, and source-local allocations are unchanged. Worker outputs merge by
canonical source range, and output limits are checked during that merge, so
schemas, row order, costs, paths, structured errors, and fingerprints match the
one-thread result at supported `1`/`2`/`4`/`8`/automatic configurations. The
implementation consumes CSR-native adjacency and the existing bounded Arrow
shaping path; it does not introduce a parallel-only graph copy or a global
edge-index map.

## Parallel resource-allocation aggregate (#513)

`rank(by="resource_allocation")` partitions **independent source ordinals**
across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- the estimated pair/intersection work is at least
  `RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK` (`524_288`) in
  `graphforge-exec`.

The estimate is `sources² + 2 × sources × distinct_adjacency_entries`, a
conservative O(V + E) proxy for the serial pair loop and two-pointer
intersection scans. The threshold is the smallest power-of-two estimate below
the measured source-partition win boundary on this M4 agent host (directed
ring-lattice fixture, 4 private workers, release build): ~69k and ~230k units
were neutral, ~540k units first won (~0.74x serial), and >=1.2M units ran about
0.52-0.56x serial. Below the boundary, pool scheduling can dominate; at and
above it, source-owned chunks amortize pool work while preserving exact score
bits.

Below that crossover, or when the policy provides one compute thread, the
serial path runs with no pool scheduling tax. Parallel workers only own source
ranges. Candidate order, missing-link checks, two-pointer neighborhood
intersection, reciprocal-degree discounts, and compensated floating-point
summation remain serial per source. Worker score chunks merge in canonical
source order, so schemas, row order, scores, and fingerprints match the
one-thread result at `1`/`2`/`4`/`8`/automatic configurations.

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

## Serial analyze(by="maximum_spanning_tree") (#580)

`analyze(by="maximum_spanning_tree")` has no parallel crossover. Its
disposition is **serial Kruskal descending union-find** for every
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

## Serial strongly connected components (#532)

`cluster(by = "strongly_connected")` intentionally stays serial. The public
algorithm is exact Tarjan SCC: discovery indices, low-link propagation,
component-stack membership, component emission, and the final canonical label
order all depend on one DFS traversal order. Forcing private-pool work into
that state would require either shared mutable low-link/stack coordination or a
different component algorithm, both of which would risk changing public row
order, labels, fingerprints, cancellation points, and structured failure
boundaries.

The serial path still consumes #340 CSR-native adjacency: Tarjan walks
`AdjacencyGraph::neighbors` slices directly in canonical edge order and keeps
only the O(V) ordinal map plus Tarjan state (`discovery`, `lowlink`, stack
flags, labels, and explicit DFS frames). It does not build a parallel-only graph
copy and does not use Rayon's process-global pool. Checkpoints remain on the
DFS edge/frame path so cancellation and limits return structured outcomes
without partial public results.

Evidence is carried by:

- `graphforge-exec::algorithm_cluster_scc` unit coverage for exact labels,
  duplicate/self-loop handling, exhaustive three-node digraph parity with
  mutual reachability, deep iterative DFS, malformed adjacency errors, and
  cancellation.
- `graphforge-exec::algorithm_cluster` dispatch coverage for the public schema,
  stable UUID-ordered output, one Rust owner, node/edge/output/iteration limits,
  and cancellation.


There is no crossover for SCC in M4: the documented disposition is `serial_tarjan`. Timing remains hardware-specific evidence, never a CI pass/fail gate.
## Serial analyze(by="minimum_k_spanning_tree") (#581)

`analyze(by="minimum_k_spanning_tree")` has no parallel crossover. Its
disposition is **serial exact k-spanning-tree enumeration** for every
`compute_threads` setting, including `1`/`2`/`4`/`8`/automatic policies.

The Rust kernel consumes CSR-native weighted undirected adjacency (#340),
normalizes candidates, then explores edge combinations while maintaining one
canonical top-k tree set ordered by total weight and edge UUID sequence. Every
accepted candidate can replace the current worst tree, so parallel enumeration
would require shared top-k coordination and could alter tie order, resource
errors, or fingerprints.

The path still uses bounded Arrow output (#341), structured cancellation and
resource checks, and no process-global Rayon pool. The M4 harness records
`analyze-minimum-k-spanning-tree` evidence and verifies one-thread parity;
timing is report-only.

## Serial CELF influence maximization (#502)

`rank(by="celf")` keeps the Cost-Effective Lazy Forward search serial under every
`compute_threads` setting. The next useful marginal-spread recomputation is the
current globally best stale candidate; once recomputed, that candidate either
becomes the next seed or re-enters the heap with a new gain. Speculative
multi-candidate refresh would need extra conflict resolution against changing
seed state and could alter equal-gain UUID tie order or marginal-score bits.

The #502 polish therefore removes avoidable serial allocation inside the accepted
path rather than claiming a universal crossover. It reuses candidate-seed,
activation, and queue buffers during spread evaluation, keeps selected adjacency
access CSR-native, and continues to shape output through the bounded Arrow sink.
Schemas, row order, marginal scores, projection fingerprints, structured
limit/cancellation errors, and write-back atomicity match the one-thread oracle
at `1`/`2`/`4`/`8`/automatic configurations. No GPU, distributed, approximate, or
foreign-engine fallback is implied.

## Serial biconnected components (#517)

`cluster(by="biconnected")` is intentionally SERIAL. The implementation is an
iterative Tarjan-style low-link traversal whose discovery indices, edge stack,
and block-pop boundaries are sequential state; parallelizing those steps would
either change the canonical primary-block labels or add synchronization that
does not expose independent work to the private compute pool.

The handler still uses Rust-owned adjacency projection and the bounded Arrow
sink, avoids any Rayon global pool, and observes cancellation and shared
iteration/output limits. The serial disposition is covered by a fingerprint test
that attaches private compute pools for `1`/`2`/`4`/`8` configured compute
threads and requires identical schemas, row ordering, labels, and rows.

## Serial k-core peeling (#511)

`rank(by="k_core")` has no parallel crossover. Its performance disposition is
**serial priority-queue peeling** for every `compute_threads` setting, including
`1`/`2`/`4`/`8`/automatic resource policies.

The Rust kernel consumes the CSR-native `AdjacencyGraph` projection (#340),
normalizes simple undirected neighbor ordinals for the exact core-decomposition
contract, then peels through a min-priority queue keyed by
`(current degree, dense node ordinal)`. Each heap pop decides which live
neighbors are decremented and which stale entries will later be ignored, so the
frontier is order-dependent rather than a set of independent work units. Using
the private compute pool here would require concurrent decrease-key/tie
coordination and would risk changing accepted row ordering, ties, and
fingerprints.

The path still uses shared cancellation/checkpoint controls, structured
resource errors, and the bounded Arrow sink (#341); it never creates a
parallel-only graph copy or uses Rayon's process-global pool. The M4 entry
harness records `rank-k-core` structural evidence (work units, serial path,
peak RSS, result fingerprint, and hardware-specific timing) and verifies that
executed thread configurations match the one-thread oracle. Timing remains
evidence only, not a CI threshold.

## Parallel FastRP row kernels (#559)

`analyze_embedding(by="fast_random_projection")` partitions **independent
source rows** across the instance-owned private compute pool when:

- `compute_threads > 1`, and
- estimated row/coordinate operations are at least
  `FASTRP_PARALLEL_CROSSOVER_OPS` (`65_536`) in `graphforge-exec`.

The estimate covers sparse propagation
(`adjacency_entries × propagated_iterations × dimensions`), initial projection,
feature projection/mixing, and per-iteration accumulation. Below that crossover,
or when the policy provides one compute thread, FastRP stays serial with no pool
scheduling tax.

Parallel FastRP never parallelizes a row's floating-point reduction. Initial
projection, feature mixing, accumulation, and sparse matvec work are row-owned;
each source row visits neighbors and coordinates in the same order as the
one-thread oracle, and worker chunks merge by canonical node ordinal. Schemas,
row order, f32 embedding bits, cancellation, and work-limit errors are covered at
`1`/`2`/`4`/`8` thread configurations.

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
- [Dijkstra all-pairs source parallelism evidence](dijkstra-all-pairs-parallel-evidence.md)
- [Scale Limits](../reference/scale-limits.md)
- Contract: [`tests/contracts/m4-entry-matrix.json`](../../tests/contracts/m4-entry-matrix.json)
