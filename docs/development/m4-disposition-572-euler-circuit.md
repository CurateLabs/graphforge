# M4 disposition: analyze(by="euler_circuit") (#572)

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
