# M4 disposition: analyze(by="topological_sort") (#585)

Disposition: serial deterministic Kahn ordering.

`topological_sort` emits the exact order chosen by the shared Kahn topology.
Every indegree decrement can release a node into a globally UUID-ordered ready
set, so partitioned updates would need merge policy that could change public row
order or the first structured cycle/cancellation outcome.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Directed-adjacency validation, indegree accumulation, UUID-ordered ready set, ordered node rows. |
| Determinism | Existing `graphforge-exec` tests cover ready-node tie order, disconnected DAGs, parallel edges, cycles, cancellation, limits, and registration. |
| Resource shape | Uses selected Rust adjacency and bounded Arrow node rows; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
