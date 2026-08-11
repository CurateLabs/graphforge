# M4 disposition: analyze(by="dag_longest_path_weighted") (#569)

Disposition: serial weighted topological dynamic programming.

`dag_longest_path_weighted` combines stable Kahn ordering with weighted edge
relaxation and exact deterministic tie-breaking. Floating-point accumulation,
invalid-weight validation, and predecessor-state updates remain serial so the
public best path and error ordering match the one-thread oracle.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID/weight projection, stable topological order, weighted relaxation, best-path tie resolution, one-row path output. |
| Determinism | Existing `graphforge-exec` tests cover disconnected DAGs, equal-weight ties, cycles, invalid weights, empty graphs, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust projection and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
