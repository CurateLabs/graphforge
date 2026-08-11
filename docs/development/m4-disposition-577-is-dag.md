# M4 disposition: analyze(by="is_dag") (#577)

Disposition: serial Kahn-topology predicate.

`is_dag` uses the shared deterministic Kahn topology. Indegree updates release
the next ready node into a globally UUID-ordered set, so the observable acyclic
decision and structured cycle error depend on serial ready-set progression.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Directed-adjacency validation, indegree accumulation, UUID-ordered ready set, one-row boolean output. |
| Determinism | Existing `graphforge-exec` tests cover empty/disconnected graphs, self-loops, parallel edges, undirected rejection, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust adjacency and bounded one-row output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
