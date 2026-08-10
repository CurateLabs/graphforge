# M4 disposition: cluster(by="infomap") (#522)

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
