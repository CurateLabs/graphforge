# M4 algorithm dispositions (#498/#499–#588)

Per-algorithm performance dispositions for the later M4 algorithm batch, recorded
on the frozen #345 exit tree. Each entry states whether the shipped kernel is
serial or uses the instance-owned private compute pool, and why. These are
structural dispositions, not wall-clock claims; hardware timing and RSS
observations stay non-gating. The pool policy itself is documented in
[execution-resource-policy.md](execution-resource-policy.md); the exit ledger is
[m4-exit-evidence.md](m4-exit-evidence.md).

Formerly one file per algorithm (`m4-disposition-<issue>-<slug>.md`); folded
into this page by #1625.

## cluster(by="approximate_max_k_cut") (#516)

Disposition: serial deterministic local-search heuristic.

`approximate_max_k_cut` is an approximate public algorithm, but its shipped
implementation still uses deterministic move order, seeded tie handling, and
community renumbering. Parallel proposal/evaluation rounds are not introduced
because simultaneous moves can change later gains and public community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized undirected topology, deterministic move evaluation, cut-gain updates, canonical community output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, or foreign-engine fallback is implied, and hardware
timing/RSS observations remain non-gating evidence only.

## cluster(by="fastgreedy") (#519)

Disposition: serial greedy modularity merging.

`fastgreedy` repeatedly chooses the next canonical community merge under the
current modularity state. Each accepted merge rewrites the candidate frontier,
so parallel merge proposals are not trivially safe without conflict resolution
that could change merge order and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized community graph, best-merge search, modularity scoring, canonical partition output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic partitions, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="girvan_newman") (#520)

Disposition: serial edge-betweenness removal.

`girvan_newman` repeatedly computes edge betweenness for the current graph and
removes the single canonical best edge before recomputing partitions. Parallel
removal is not safe because each deletion changes the next betweenness scores,
modularity comparison, and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized community graph, edge-betweenness search, canonical edge removal, modularity tracking, output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic splits, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="hdbscan") (#521)

Disposition: serial reachability-tree clustering.

`hdbscan` builds the accepted reachability tree and extracts stable labels with
canonical ordering. The current path is not split into parallel distance/tree
fragments because tree construction, edge ordering, and cluster extraction share
global tie state that defines public labels.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Feature/adjacency projection, reachability-tree construction, stable label extraction, bounded output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic labels, noise/isolate handling, limits, cancellation, invalid inputs, and Rust registration. |
| Resource shape | Uses selected Rust data and bounded node/label output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="infomap") (#522)

Disposition: serial flow/module optimization.

`infomap` normalizes directed flow, computes stationary visits, and chooses
module assignments under deterministic component and tie order. Parallel module
updates are not introduced because candidate changes affect the shared coding
objective and final canonical community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Flow normalization, stationary iteration, module-move scoring, component ordering, canonical output. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected semantics, stationary convergence, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="label_propagation") (#525)

Disposition: serial asynchronous label-propagation sweeps.

`label_propagation` shuffles node order with a deterministic random stream, then
updates labels in place. Later listeners in the same sweep observe earlier label
changes, so parallel synchronous buckets would be a different algorithm and
could change convergence, ties, random consumption, and community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized topology, seeded node-order shuffle, in-place label updates, stability check, canonical output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, normalized boundaries, cancellation, limits, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="leading_eigenvector") (#526)

Disposition: serial spectral split recursion.

`leading_eigenvector` computes modularity-matrix power iterations and recursive
splits with deterministic floating-point accumulation and tie handling. Parallel
reductions are not introduced because they could change low-bit scores, split
acceptance, and final canonical community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Component extraction, serial power iteration, split scoring, recursive partitioning, canonical output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic splits, disconnected/isolate handling, limits, cancellation, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="modularity_optimization") (#529)

Disposition: serial modularity move/condense loop.

`modularity_optimization` evaluates community moves against the current global
partition and then condenses accepted state. Parallel moves are not introduced
because simultaneous updates can invalidate gains and alter canonical community
renumbering.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized weighted topology, ordered move evaluation, modularity gain checks, condensation, canonical output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, empty/isolate handling, limits, cancellation, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="speaker_listener") (#530)

Disposition: serial speaker-listener sweeps.

`speaker_listener` advances a deterministic random stream while each listener
samples neighbor memories and mutates its own memory. Parallel listener updates
would consume randomness and expose memory snapshots differently, changing label
selection and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized topology, seeded listener order, memory sampling/update, canonical label extraction, output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, memory thresholds, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="spinglass") (#531)

Disposition: serial annealing / MCMC-style community search.

`spinglass` advances one deterministic annealing state with seeded transition
order and component-local canonicalization. Parallel proposals would race against
the same temperature/state updates and could change accepted moves, random-stream
consumption, or final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Component extraction, seeded annealing sweeps, energy/gain updates, canonical community output. |
| Determinism | Existing `graphforge-exec` tests cover component isolation, deterministic labels, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## cluster(by="walktrap") (#533)

Disposition: serial random-walk agglomeration.

`walktrap` computes deterministic random-walk distances and then advances one
canonical agglomeration state. Parallel merge candidates are not introduced
because each accepted merge changes later distances, dendrogram state, and
public community ID canonicalization.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Random-walk distance setup, ordered merge scoring, agglomeration updates, canonical community output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic communities, isolates, limits, cancellation, empty graphs, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="chromatic_number") (#565)

Disposition: serial exact search.

`chromatic_number` keeps its Rust-owned branch-and-bound coloring search on one
thread for every `compute_threads` setting. The next bound, incumbent color
count, and UUID tie order are shared search state; speculative parallel branches
would need extra merge policy and could change the exact witness ordering or
limit/cancellation point. This branch therefore records an explicit serial
disposition instead of claiming a crossover.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Canonical node ordering, normalized edge projection, recursive color choices, and incumbent pruning. |
| Determinism | Existing `graphforge-exec` tests cover schemas, exact color count, loop rejection, repeatability, and structured limits. |
| Resource shape | No parallel-only graph copy; output is the existing one-row bounded Arrow shape. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="count_automorphisms") (#567)

Disposition: serial exact individualization/refinement search.

`count_automorphisms` counts adjacency-multiplicity-preserving permutations with
one shared search budget, equitable partitions, and leaf verification. Parallel
subtree counting is not trivially safe because budget consumption, cancellation
points, overflow handling, and canonical candidate ordering are observable
structured outcomes.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Automorphism IR normalization, partition refinement, recursive individualization, leaf verification, one-row count output. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected counting, UUID rename invariance, limits, cancellation, state-budget errors, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="dag_longest_path") (#568)

Disposition: serial topological dynamic programming.

`dag_longest_path` first consumes the shared deterministic Kahn topology, then
relaxes edges in that order with canonical tie-breaking for the single public
best path. Parallel relaxation would require synchronization across predecessor
state and could change equal-hop tie outcomes or structured cycle errors.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID projection, stable topological order, edge relaxation, best-path tie resolution, one-row path output. |
| Determinism | Existing `graphforge-exec` tests cover disconnected DAGs, tie-breaking, cycles, empty graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="dag_longest_path_weighted") (#569)

Disposition: serial weighted topological dynamic programming.

`dag_longest_path_weighted` combines stable Kahn ordering with weighted edge
relaxation and exact deterministic tie-breaking. Floating-point accumulation,
invalid-weight validation, and predecessor-state updates remain serial so the
public best path and error ordering match the one-thread oracle.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID/weight projection, stable topological order, weighted relaxation, best-path tie resolution, one-row path output. |
| Determinism | Existing `graphforge-exec` tests cover disconnected DAGs, equal-weight ties, cycles, invalid weights, empty graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="edge_coloring") (#571)

Disposition: serial greedy edge coloring.

`edge_coloring` colors stored edges in a deterministic UUID/topology order. Each
edge observes colors already assigned to adjacent edges, so the next legal color
depends on all earlier decisions and canonical tie-breaking. Parallel coloring is
not trivially safe without adding conflict-repair passes that could change row
order, color IDs, or cancellation points.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Stored-edge normalization, adjacency-color lookup, first-available color assignment, bounded row shaping. |
| Determinism | Existing `graphforge-exec` tests cover parallel edges, self-loop rejection, duplicate-edge validation, limits, and repeatability. |
| Resource shape | Uses the selected Rust projection and bounded Arrow sink; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="euler_circuit") (#572)

Disposition: serial deterministic Euler construction.

`euler_circuit` uses one authoritative trail state: each consumed edge changes
the next available edge frontier and the final canonical node/edge sequence.
Parallel consumption is not trivially safe because it would need to merge partial
circuits while preserving stored-edge UUID order, loop/parallel-edge handling,
and structured undefined-circuit errors.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Euler projection validation, degree/connectivity checks, trail stack updates, one-row path shaping. |
| Determinism | Existing `graphforge-exec` tests cover empty/singleton cases, loops, parallel edges, UUID rename equivariance, repeatability, and registration. |
| Resource shape | Uses selected Rust projection and bounded path output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="euler_path") (#573)

Disposition: serial deterministic Euler trail construction.

`euler_path` has a single mutable trail frontier. Each stored edge consumed by
the current stack determines the next edge choice and the final public node/edge
sequence. A parallel path would need non-trivial partial-trail splicing and could
alter UUID tie order, open-trail endpoint selection, or structured undefined-path
errors.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Euler projection validation, endpoint/degree checks, trail stack updates, one-row path shaping. |
| Determinism | Existing `graphforge-exec` tests cover empty/singleton cases, open trails, loops, parallel edges, UUID rename equivariance, repeatability, and registration. |
| Resource shape | Uses selected Rust projection and bounded path output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="find_cycles") (#574)

Disposition: serial simple-cycle enumeration.

`find_cycles` performs deterministic DFS-style simple-cycle enumeration with a
canonical cycle set and output-limit checks as cycles are discovered. Partitioned
starts are not trivially safe because deduplication, row order, and the first
structured output-limit error depend on global discovery order.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, edge normalization, stack-driven DFS, canonical cycle rotation, bounded row shaping. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected cycles, loops, duplicate edges, cancellation, output limits, repeatability, and registration. |
| Resource shape | Uses selected Rust projection and bounded Arrow cycle rows; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="has_euler_circuit") (#575)

Disposition: serial Euler feasibility predicate.

`has_euler_circuit` validates one normalized projection, degree balance, and
connectivity reachability before returning a single boolean. The scan and search
share canonical validation and checkpoint order; introducing parallel fragments
would add synchronization without a measured safe crossover for this predicate.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Projection validation, degree/in-out balance, non-isolated connectivity, one-row boolean shaping. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected cases, loops, parallel edges, disconnected graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="has_euler_path") (#576)

Disposition: serial Euler feasibility predicate.

`has_euler_path` validates one normalized projection, open-trail endpoint counts,
degree balance, and reachability before returning a single boolean. The accepted
kernel is already bounded and deterministic; no independent parallel frontier is
introduced for this predicate without a measured crossover.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Projection validation, endpoint/degree checks, non-isolated connectivity, one-row boolean shaping. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected cases, open trails, loops, parallel edges, disconnected graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="is_dag") (#577)

Disposition: serial Kahn-topology predicate.

`is_dag` uses the shared deterministic Kahn topology. Indegree updates release
the next ready node into a globally UUID-ordered set, so the observable acyclic
decision and structured cycle error depend on serial ready-set progression.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Directed-adjacency validation, indegree accumulation, UUID-ordered ready set, one-row boolean output. |
| Determinism | Existing `graphforge-exec` tests cover empty/disconnected graphs, self-loops, parallel edges, undirected rejection, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust adjacency and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="is_planar") (#578)

Disposition: serial LR planarity predicate.

`is_planar` uses deterministic simple-graph normalization, an Euler edge-count
early reject, and an LR-style embedding state with DFS/lowpoint dependencies.
Those state transitions are not independent work units, so this branch keeps the
accepted serial predicate rather than adding speculative parallel embedding
passes.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, loop/parallel simplification, edge-count gate, LR embedding state, one-row boolean output. |
| Determinism | Existing `graphforge-exec` tests cover planar/non-planar fixtures, invalid projection cases, cancellation, limits, repeatability, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="k1_coloring") (#579)

Disposition: serial degree-ordered greedy coloring.

`k1_coloring` first fixes a descending-degree, ascending-UUID node order and then
assigns the first color not used by already-colored neighbors. Later colors
depend on earlier assignments and on canonical post-normalization of color IDs,
so parallel buckets are not trivially safe without extra repair logic.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, simple-neighbor normalization, ordered greedy assignment, canonical color compaction. |
| Determinism | Existing `graphforge-exec` tests cover graph-size/output limits, invalid endpoints, duplicate UUIDs, isolates, and repeatability. |
| Resource shape | Uses selected adjacency only and the existing bounded Arrow node/color output. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="node_coloring") (#584)

Disposition: serial UUID-ordered greedy coloring.

`node_coloring` walks nodes in ascending public UUID order and assigns the first
available color after inspecting already-colored neighbors. The color for each
node is therefore a function of the exact previous prefix, so parallel coloring
would require conflict resolution and could change color IDs or observable
checkpoint ordering.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, simple-edge normalization, prefix-dependent greedy color assignment, bounded rows. |
| Determinism | Existing `graphforge-exec` tests cover ordered colors, invalid/self-loop handling, limits, cancellation, and registration. |
| Resource shape | Uses the Rust selected projection and bounded Arrow output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.

## analyze(by="topological_sort") (#585)

Disposition: serial deterministic Kahn ordering.

`topological_sort` emits the exact order chosen by the shared Kahn topology.
Every indegree decrement can release a node into a globally UUID-ordered ready
set, so partitioned updates would need merge policy that could change public row
order or the first structured cycle/cancellation outcome.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Directed-adjacency validation, indegree accumulation, UUID-ordered ready set, ordered node rows. |
| Determinism | Existing `graphforge-exec` tests cover ready-node tie order, disconnected DAGs, parallel edges, cycles, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust adjacency and bounded Arrow node rows; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
