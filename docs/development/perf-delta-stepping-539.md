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
oracle exactly. Rows below the 262,144 crossover stay on the serial path even
when the control carries a multi-thread pool, so those columns reflect serial
noise under different controls rather than parallel speedups.

| Middle nodes | Fanout | Edge entries | Rows | 1 thread | 2 threads | 4 threads | 8 threads |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 192 | 96 | 18,816 | 289 | 4.851 ms | 3.916 ms | 3.971 ms | 3.561 ms |
| 512 | 256 | 132,096 | 769 | 40.090 ms | 46.920 ms | 34.302 ms | 41.604 ms |
| 1,024 | 384 | 395,264 | 1,409 | 139.790 ms | 137.109 ms | 120.984 ms | 102.622 ms |

Peak resident set reported by `/proc/self/status` `VmHWM` for the evidence test
process: **276,480 KiB**.

An exploratory release-mode run with the provisional 8,192 threshold showed that
the 132k-edge-entry fixture did not consistently justify the pool tax (2 and 4
threads were slower while 8 threads was only slightly faster). The 395k fixture
is the first measured wave where 2/4/8 workers beat one thread in the repeat
production-threshold run above, so the implementation uses the conservative
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
