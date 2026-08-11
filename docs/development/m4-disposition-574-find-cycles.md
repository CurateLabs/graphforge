# M4 disposition: analyze(by="find_cycles") (#574)

Disposition: serial simple-cycle enumeration.

`find_cycles` performs deterministic DFS-style simple-cycle enumeration with a
canonical cycle set and output-limit checks as cycles are discovered. Partitioned
starts are not trivially safe because deduplication, row order, and the first
structured output-limit error depend on global discovery order.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, edge normalization, stack-driven DFS, canonical cycle rotation, bounded row shaping. |
| Determinism | Existing `graphforge-exec` tests cover directed/undirected cycles, loops, duplicate edges, cancellation, output limits, repeatability, and registration. |
| Resource shape | Uses selected Rust projection and bounded Arrow cycle rows; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
