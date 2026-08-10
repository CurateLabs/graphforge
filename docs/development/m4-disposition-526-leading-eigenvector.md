# M4 disposition: cluster(by="leading_eigenvector") (#526)

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
