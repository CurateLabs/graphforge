# Executor kernel dead-code audit (#1026)

The audit covers all 43 module-wide `allow(dead_code)` declarations in
`crates/graphforge-exec/src/lib.rs` at the start of #1026. Every module has
production consumers. None is an abandoned or wholly test-only module, so all
43 remain in production with their blanket allowances removed.

The table records direct production consumers, excluding each module's own
`#[cfg(test)] mod tests`. File names are relative to
`crates/graphforge-exec/src/`. Analyze, cluster, and paths dispatchers invoke the
kernels; shared foundation modules are also consumed by the named active kernels.
The tracking column preserves issue numbers from the removed allowance comments;
it records their historical rationale, not a claim that those issues remain open.
The current cleanup is tracked by #1026.

| Retained module | Direct production consumers | Tracking recorded in allowance |
| --- | --- | --- |
| `algorithm_analyze_automorphism` | `algorithm_analyze.rs`, `algorithm_analyze_automorphism_count.rs` | #2106 |
| `algorithm_analyze_automorphism_count` | `algorithm_analyze.rs` | #2111 |
| `algorithm_analyze_bipartite` | `algorithm_analyze.rs`, `algorithm_analyze_bipartite_matching.rs` | #1223 |
| `algorithm_analyze_bipartite_matching` | `algorithm_analyze.rs` | #1223 |
| `algorithm_analyze_chromatic_number` | `algorithm_analyze.rs` | #1214 |
| `algorithm_analyze_dag_longest_path` | `algorithm_analyze.rs`, `algorithm_analyze_dag_longest_path_weighted.rs` | #1206 |
| `algorithm_analyze_dag_longest_path_weighted` | `algorithm_analyze.rs` | #1208 |
| `algorithm_analyze_dag_topology` | `algorithm_analyze.rs` | #1741 |
| `algorithm_analyze_dyad_census` | `algorithm_analyze.rs` | #1884 |
| `algorithm_analyze_edge_coloring` | `algorithm_analyze.rs` | #1212 |
| `algorithm_analyze_euler` | `algorithm_analyze.rs`, `algorithm_analyze_has_euler_circuit.rs`, `algorithm_analyze_has_euler_path.rs` | #2104 |
| `algorithm_analyze_find_cycles` | `algorithm_analyze.rs` | #1204 |
| `algorithm_analyze_has_euler_circuit` | `algorithm_analyze.rs` | #1227 |
| `algorithm_analyze_has_euler_path` | `algorithm_analyze.rs` | #1228 |
| `algorithm_analyze_is_planar` | `algorithm_analyze.rs` | #1229 |
| `algorithm_analyze_k1_coloring` | `algorithm_analyze.rs` | #1217 |
| `algorithm_analyze_lowlink` | `algorithm_analyze.rs` | #1230, #1231 |
| `algorithm_analyze_max_cardinality_matching` | `algorithm_analyze.rs` | #1221 |
| `algorithm_analyze_minimum_k_spanning_tree` | `algorithm_analyze.rs` | #1198 |
| `algorithm_analyze_modularity` | `algorithm_analyze.rs` | #1234 |
| `algorithm_analyze_triad_census` | `algorithm_analyze.rs` | #1885 |
| `algorithm_analyze_triangle_count` | `algorithm_analyze.rs` | #1232 |
| `algorithm_cluster_scc` | `algorithm_cluster.rs` | None recorded |
| `algorithm_cluster_spinglass` | `algorithm_cluster.rs` | None recorded |
| `algorithm_embedding_control` | `algorithm_analyze.rs`, `algorithm_embedding_fastrp.rs`, `algorithm_embedding_graphsage.rs`, `algorithm_embedding_hashgnn.rs`, `algorithm_embedding_node2vec.rs`, `algorithm_embedding_output.rs` | None recorded |
| `algorithm_embedding_options` | `algorithm_analyze.rs`, `algorithm_embedding_output.rs` | None recorded |
| `algorithm_embedding_node2vec` | `algorithm_analyze.rs` | None recorded |
| `algorithm_embedding_output` | `algorithm_analyze.rs`, `algorithm_embedding_fastrp.rs`, `algorithm_embedding_graphsage.rs`, `algorithm_embedding_hashgnn.rs`, `algorithm_embedding_node2vec.rs` | None recorded |
| `algorithm_embedding_rng` | `algorithm_embedding_fastrp.rs`, `algorithm_embedding_graphsage.rs`, `algorithm_embedding_hashgnn.rs`, `algorithm_embedding_node2vec.rs` | None recorded |
| `algorithm_partition` | `algorithm_analyze.rs`, `algorithm_analyze_bipartite.rs`, `algorithm_analyze_conductance.rs`, `algorithm_analyze_modularity.rs`, `algorithm_graph.rs` | #1223, #1233 |
| `algorithm_paths_astar` | `algorithm_paths.rs` | #1683 |
| `algorithm_paths_bellman_ford` | `algorithm_paths.rs` | #1691 |
| `algorithm_paths_delta_stepping` | `algorithm_paths.rs` | #1706 |
| `algorithm_paths_dfs` | `algorithm_paths.rs` | #1220 |
| `algorithm_paths_dijkstra` | `algorithm_paths.rs`, `algorithm_paths_astar.rs`, `algorithm_paths_bellman_ford.rs`, `algorithm_paths_delta_stepping.rs`, `algorithm_paths_floyd_warshall.rs` | #1665 |
| `algorithm_paths_floyd_warshall` | `algorithm_paths.rs` | #1701 |
| `algorithm_paths_gomory_hu` | `algorithm_paths.rs` | #2121 |
| `algorithm_paths_max_flow` | `algorithm_paths.rs`, `algorithm_paths_gomory_hu.rs`, `algorithm_paths_min_cut.rs` | #1209 |
| `algorithm_paths_min_cost_flow` | `algorithm_paths.rs` | #1213 |
| `algorithm_paths_min_cut` | `algorithm_paths.rs`, `algorithm_paths_gomory_hu.rs` | #1211 |
| `algorithm_paths_prize_steiner` | `algorithm_paths.rs` | None recorded |
| `algorithm_paths_random_walk` | `algorithm_paths.rs` | #1222 |
| `algorithm_paths_steiner` | `algorithm_paths.rs` | #2159 |

## Item-level disposition

Removing the blanket allowances exposed seven unused-item diagnostics in a fresh
executor compilation. The cleanup follows their actual use:

- `SpinglassGraph::move_delta` remains behind `#[cfg(test)]` because the energy
  delta regression uses it as an independent oracle for the production optimizer.
- `EmbeddingRng::derived_state` remains behind `#[cfg(test)]` for deterministic
  seed/state vectors. Production continues to use the same generator methods.
- The unused `walk_task_ordinal` helper and its self-assertions are deleted.
  Production task assignment and deterministic chunk/parallel walk tests remain.
- The unused `RANDOM_WALK_RNG_CONTRACT` string and its self-assertion are deleted.
  The splitmix64-v1 contract is documented at module level; exact seeded walk
  vectors, weighted choices, and serial/parallel equivalence tests remain.
- Prize-Steiner `ResolvedNumber` retains its production `Float64` representation.
  Unconstructed integer/null/non-numeric variants are deleted. Production property
  normalization already validates nulls, numeric types and exact integer conversion
  before dispatch. A real API regression covers valid integers and invalid negative,
  oversized, null, boolean and string prizes; kernel finite/nonnegative checks remain.
- `NormalizedSteinerInvocation::kind`, its unused stored kind, and the unused
  `steiner_schema` wrapper are deleted. The kind still validates options and terminal
  requirements during normalization; schema tests exercise the canonical algorithm
  schema directly.

## Validation

The acceptance gate is `cargo clippy --workspace -- -D warnings` plus executor
unit tests. The changed property boundary also requires the focused API
`prize_steiner_validates_property_types_before_kernel_dispatch` regression.
Formatting and the repository fast gates apply to the final tree. Validation
results belong to the focused PR; this inventory does not claim unrun checks.
