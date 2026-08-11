# M4 disposition: cluster(by="approximate_max_k_cut") (#516)

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
