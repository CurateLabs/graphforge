# M4 disposition: analyze(by="edge_coloring") (#571)

Disposition: serial greedy edge coloring.

`edge_coloring` colors stored edges in a deterministic UUID/topology order. Each
edge observes colors already assigned to adjacent edges, so the next legal color
depends on all earlier decisions and canonical tie-breaking. Parallel coloring is
not trivially safe without adding conflict-repair passes that could change row
order, color IDs, or cancellation points.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Stored-edge normalization, adjacency-color lookup, first-available color assignment, bounded row shaping. |
| Determinism | Existing `graphforge-exec` tests cover parallel edges, self-loop rejection, duplicate-edge validation, limits, and repeatability. |
| Resource shape | Uses the selected Rust projection and bounded Arrow sink; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
