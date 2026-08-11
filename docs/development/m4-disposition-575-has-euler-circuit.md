# M4 disposition: analyze(by="has_euler_circuit") (#575)

Disposition: serial Euler feasibility predicate.

`has_euler_circuit` validates one normalized projection, degree balance, and
connectivity reachability before returning a single boolean. The scan and search
share canonical validation and checkpoint order; introducing parallel fragments
would add synchronization without a measured safe crossover for this predicate.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Projection validation, degree/in-out balance, non-isolated connectivity, one-row boolean shaping. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected cases, loops, parallel edges, disconnected graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
