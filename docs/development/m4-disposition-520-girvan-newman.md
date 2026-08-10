# M4 disposition: cluster(by="girvan_newman") (#520)

Disposition: serial edge-betweenness removal.

`girvan_newman` repeatedly computes edge betweenness for the current graph and
removes the single canonical best edge before recomputing partitions. Parallel
removal is not safe because each deletion changes the next betweenness scores,
modularity comparison, and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized community graph, edge-betweenness search, canonical edge removal, modularity tracking, output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic splits, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
