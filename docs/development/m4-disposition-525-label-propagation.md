# M4 disposition: cluster(by="label_propagation") (#525)

Disposition: serial asynchronous label-propagation sweeps.

`label_propagation` shuffles node order with a deterministic random stream, then
updates labels in place. Later listeners in the same sweep observe earlier label
changes, so parallel synchronous buckets would be a different algorithm and
could change convergence, ties, random consumption, and community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized topology, seeded node-order shuffle, in-place label updates, stability check, canonical output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, normalized boundaries, cancellation, limits, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
