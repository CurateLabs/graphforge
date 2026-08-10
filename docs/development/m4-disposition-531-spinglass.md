# M4 disposition: cluster(by="spinglass") (#531)

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
