# M4 disposition: cluster(by="modularity_optimization") (#529)

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
