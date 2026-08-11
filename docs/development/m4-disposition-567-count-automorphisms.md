# M4 disposition: analyze(by="count_automorphisms") (#567)

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
