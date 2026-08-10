# M4 disposition: cluster(by="hdbscan") (#521)

Disposition: serial reachability-tree clustering.

`hdbscan` builds the accepted reachability tree and extracts stable labels with
canonical ordering. The current path is not split into parallel distance/tree
fragments because tree construction, edge ordering, and cluster extraction share
global tie state that defines public labels.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Feature/adjacency projection, reachability-tree construction, stable label extraction, bounded output. |
| Determinism | Existing `graphforge-exec` tests cover deterministic labels, noise/isolate handling, limits, cancellation, invalid inputs, and Rust registration. |
| Resource shape | Uses selected Rust data and bounded node/label output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
