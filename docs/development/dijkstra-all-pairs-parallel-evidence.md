# Dijkstra all-pairs source parallelism evidence (#542)

`paths(by="dijkstra_all_pairs")` now parallelizes only independent source
searches. Each source still runs the existing serial Dijkstra heap/tie logic, and
worker chunks merge in canonical source-range order.

## Structural disposition

| Item | Before #542 | After #542 |
|---|---|---|
| Work unit | Whole all-pairs invocation looped sources serially | Contiguous canonical source ranges above crossover |
| Crossover | None; always serial | `selected_nodes * CSR adjacency entries >= 8_192` and `compute_threads > 1` |
| Threads/path | One thread for every source | Up to `compute_threads` private-pool workers; each source remains serial |
| Graph representation | Existing CSR-native `AdjacencyGraph` | Same CSR-native graph; no parallel-only graph copy or O(E) edge map |
| Output shaping | Existing bounded Arrow shaping | Same bounded Arrow shaping after deterministic merge |
| Failure behavior | Structured cancellation/limits/execution errors | Same; worker errors/panics return structured errors without partial public output |

## Local evidence

Host policy rejected `threads-8` on this 4-vCPU agent because the explicit
Tokio+DataFusion request exceeded the resource-policy budget. Executed cells
shared schema, row count, ordering, and content fingerprint. Fingerprint values
are UUID-sensitive and are compared within a run.

```text
CARGO_TARGET_DIR=/tmp/cargo-m4-542 cargo test -p graphforge-exec algorithm_paths_dijkstra --lib
result: ok. 9 passed; 0 failed; 0 ignored; 717 filtered out.

CARGO_TARGET_DIR=/tmp/cargo-m4-542 cargo test -p graphforge-api dijkstra_all_pairs --lib -- --nocapture
threads-1: rows=2256 fingerprint=6ea5e53d27fcb1bf3813d4bbc6dab32a4c2c554a06ea26d554941c21ab2d8806
threads-2: rows=2256 fingerprint=6ea5e53d27fcb1bf3813d4bbc6dab32a4c2c554a06ea26d554941c21ab2d8806
threads-4: rows=2256 fingerprint=6ea5e53d27fcb1bf3813d4bbc6dab32a4c2c554a06ea26d554941c21ab2d8806
threads-8: unavailable resource policy: validation error: combined tokio/partition concurrency 16 exceeds instance budget 8
threads-automatic: rows=2256 fingerprint=6ea5e53d27fcb1bf3813d4bbc6dab32a4c2c554a06ea26d554941c21ab2d8806
result: ok. 3 passed; 0 failed; 0 ignored; 543 filtered out.

CARGO_TARGET_DIR=/tmp/cargo-m4-542 cargo clippy -p graphforge-exec -p graphforge-api -- -D warnings
result: ok.

CARGO_TARGET_DIR=/tmp/cargo-m4-542 cargo fmt --all -- --check
result: ok.
```

Timing is hardware-specific and not a pass/fail gate. Shell keyword timing for
the public parity cell on this agent reported `real 11.639 user 8.881 sys 2.914`
including cargo/test harness overhead. `/usr/bin/time -v` is not installed on
this VM, so local peak RSS was unavailable from the shell timing tool.
