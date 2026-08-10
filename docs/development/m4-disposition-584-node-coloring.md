# M4 disposition: analyze(by="node_coloring") (#584)

Disposition: serial UUID-ordered greedy coloring.

`node_coloring` walks nodes in ascending public UUID order and assigns the first
available color after inspecting already-colored neighbors. The color for each
node is therefore a function of the exact previous prefix, so parallel coloring
would require conflict resolution and could change color IDs or observable
checkpoint ordering.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, simple-edge normalization, prefix-dependent greedy color assignment, bounded rows. |
| Determinism | Existing `graphforge-exec` tests cover ordered colors, invalid/self-loop handling, limits, cancellation, and registration. |
| Resource shape | Uses the Rust selected projection and bounded Arrow output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
