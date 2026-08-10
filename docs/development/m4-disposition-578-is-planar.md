# M4 disposition: analyze(by="is_planar") (#578)

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
