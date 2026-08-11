# M4 disposition: cluster(by="walktrap") (#533)

Disposition: serial random-walk agglomeration.

`walktrap` computes deterministic random-walk distances and then advances one
canonical agglomeration state. Parallel merge candidates are not introduced
because each accepted merge changes later distances, dendrogram state, and
public community ID canonicalization.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Random-walk distance setup, ordered merge scoring, agglomeration updates, canonical community output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic communities, isolates, limits, cancellation, empty graphs, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
