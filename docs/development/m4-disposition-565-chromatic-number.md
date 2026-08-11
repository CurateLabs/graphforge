# M4 disposition: analyze(by="chromatic_number") (#565)

Disposition: serial exact search.

`chromatic_number` keeps its Rust-owned branch-and-bound coloring search on one
thread for every `compute_threads` setting. The next bound, incumbent color
count, and UUID tie order are shared search state; speculative parallel branches
would need extra merge policy and could change the exact witness ordering or
limit/cancellation point. This branch therefore records an explicit serial
disposition instead of claiming a crossover.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Canonical node ordering, normalized edge projection, recursive color choices, and incumbent pruning. |
| Determinism | Existing `graphforge-exec` tests cover schemas, exact color count, loop rejection, repeatability, and structured limits. |
| Resource shape | No parallel-only graph copy; output is the existing one-row bounded Arrow shape. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
