# M4 disposition: analyze(by="euler_path") (#573)

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
