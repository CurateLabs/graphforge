# M4 disposition: cluster(by="fastgreedy") (#519)

Disposition: serial greedy modularity merging.

`fastgreedy` repeatedly chooses the next canonical community merge under the
current modularity state. Each accepted merge rewrites the candidate frontier,
so parallel merge proposals are not trivially safe without conflict resolution
that could change merge order and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized community graph, best-merge search, modularity scoring, canonical partition output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic partitions, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
