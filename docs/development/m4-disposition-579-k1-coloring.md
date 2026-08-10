# M4 disposition: analyze(by="k1_coloring") (#579)

Disposition: serial degree-ordered greedy coloring.

`k1_coloring` first fixes a descending-degree, ascending-UUID node order and then
assigns the first color not used by already-colored neighbors. Later colors
depend on earlier assignments and on canonical post-normalization of color IDs,
so parallel buckets are not trivially safe without extra repair logic.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | UUID indexing, simple-neighbor normalization, ordered greedy assignment, canonical color compaction. |
| Determinism | Existing `graphforge-exec` tests cover graph-size/output limits, invalid endpoints, duplicate UUIDs, isolates, and repeatability. |
| Resource shape | Uses selected adjacency only and the existing bounded Arrow node/color output. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
