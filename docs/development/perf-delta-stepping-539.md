# Delta-stepping #539 performance disposition

**Date:** 2026-08-10  
**Branch:** `cursor/539-parallel-delta-stepping-9e9f`  
**Machine:** Cursor Cloud Linux 6.12.94+ x86_64  
**Command:** `CARGO_TARGET_DIR=/tmp/cargo-m4-539 cargo test --release -p graphforge-exec --lib algorithm_paths_delta_stepping::tests::measure_delta_stepping_parallel_crossover -- --ignored --nocapture`

## Disposition

`paths(by="delta_stepping")` now uses the instance-owned private `ComputePool`
for deterministic proposal collection when a bucket wave has at least **262,144
direction-expanded edge scans** and more than one current source. Smaller waves,
single-source waves, one-thread policies, and missing-pool controls stay serial.

Workers read CSR neighbor slices and emit local candidate proposals only. Bucket
mutation, best-distance updates, stale-bucket filtering, final proposal sorting,
and public row ordering remain canonical on the caller thread, so multi-thread
results must match the one-thread oracle.

## Release-mode crossover evidence

Synthetic fixtures create one source, a light-edge middle bucket, and repeated
middle-to-target relaxations so the second bucket wave is eligible for
parallel proposal generation. All thread counts below matched the one-thread
oracle exactly.

| Middle nodes | Fanout | Edge entries | Rows | 1 thread | 2 threads | 4 threads | 8 threads |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 192 | 96 | 18,816 | 289 | 12.005 ms | 15.840 ms | 11.871 ms | 18.963 ms |
| 512 | 256 | 132,096 | 769 | 63.214 ms | 105.937 ms | 87.360 ms | 59.756 ms |
| 1,024 | 384 | 395,264 | 1,409 | 271.509 ms | 154.973 ms | 130.093 ms | 108.390 ms |

The first two fixtures do not consistently justify the pool tax across supported
thread counts. The 395k-edge-entry fixture is the first measured wave where
2/4/8 workers all beat one thread, so the implementation uses the conservative
power-of-two threshold below that fixture and above the inconsistent 132k
fixture: `262_144`.

## Structural gates

- No process-global Rayon pool; parallel work is installed only on the
  `AlgorithmControl` compute pool.
- No parallel-only graph copy and no O(E) hash map expansion; workers borrow
  CSR neighbor slices from `AdjacencyGraph`.
- Canonical merge sorts all proposals by target, complete node path, then edge
  path before applying improvements.
- Tests cover serial crossover selection, one-thread vs 2/4/8-thread equality,
  and structured iteration-limit failure without partial results.
