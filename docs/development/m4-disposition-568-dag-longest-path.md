# M4 disposition: analyze(by="dag_longest_path") (#568)

Disposition: serial topological dynamic programming.

`dag_longest_path` first consumes the shared deterministic Kahn topology, then
relaxes edges in that order with canonical tie-breaking for the single public
best path. Parallel relaxation would require synchronization across predecessor
state and could change equal-hop tie outcomes or structured cycle errors.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID projection, stable topological order, edge relaxation, best-path tie resolution, one-row path output. |
| Determinism | Existing `graphforge-exec` tests cover disconnected DAGs, tie-breaking, cycles, empty graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
