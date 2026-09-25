# Complete-ingest core scaling (#1448)

Evidence for [#1448](https://github.com/CurateLabs/graphforge/issues/1448): the
baseline core-count curve and region profile, recorded before any candidate
change. The issue requires a scale-up criterion agreed with the maintainers
after this baseline and before any candidate result. This document records the
baseline only.

## Baseline freeze (recorded before any timed run)

| Item | Value |
| --- | --- |
| Baseline tree | `origin/main` `cca7a1fc`: #1452, the #1526 fix, #1562, #1581 and #1586 are all in |
| Binary | Production `gf` (`cargo build --release -p graphforge-cli`, no test-support), SHA-256 `b07bce4d92bf0f7b69fe82d231e66db0b525b3535e031aa1107f2cca1309e8da` |
| Generator | SHA-256 `eb186490eaf686df30be7b72a958c02427429e6ddb189d529546c19aa68d1709` |
| Inputs | Graph500, edge factor 16, seed `13907095936298285200`; S18, S20, S22 |
| Host | OVHC-AGENCY: 16 logical CPUs, ext4 on md RAID 1 |

## Method

Driver: [`core-scaling-1448/baseline.sh`](core-scaling-1448/baseline.sh), with
[`ingest-one.sh`](core-scaling-1448/ingest-one.sh) running one complete ingest.

- **Timed boundary.** The complete ingest: the ladder profile's five
  `import-session` commands (begin, two `register-parquet`, validate, commit)
  into a fresh project. BenchExec's `runexec` measures the whole process tree,
  which is its role under `docs/development/benchmarking.md`.
- **CPU allocation.** `runexec --cores 0-(n-1)`. The resource policy sizes
  itself from `available_parallelism`, which honours the core set. The worker
  limit therefore follows the allocation: in automatic mode, compute threads
  are `min(8, ceil(n / 2))`, and 1 when `n ≤ 2`.
- **Core-count curve.** S18 at 1, 2, 4, 8 and 16 cores, under a 4,000 MB
  `runexec` memory limit, which matches the #1448 A/B envelope. One run per
  point; points whose noise could change a decision are repeated.
- **Region profile.** S18, S20 and S22 on all 16 cores, with no memory limit.
  Every `import-session --json` receipt carries `region_diagnostics`.
- **Per run.** Page cache dropped, then 60 s of sustained quiet with no peer
  measurement process running (`gf`, generator, storage and API test binaries,
  `runexec`). The host guard is checked again after the run.
- **Reported per point.** Usable cores, wall, CPU seconds, effective cores
  (CPU / wall), throughput relative to one core, peak memory, BenchExec CPU
  and IO pressure, and the limiting region.
- **Verification (untimed).** The project is reopened and its edges counted.

## Baseline results

All eight runs ran between 05:41 and 06:03 UTC on 2026-09-25, each after 60 s
of sustained quiet, and the host was quiet after each. Every run published and
reopened with the full edge count. Raw records are under
[`core-scaling-1448/runs/`](core-scaling-1448/runs/): `runexec` output and the
validate and commit receipts with their region diagnostics.

### Core-count curve, S18 (4,194,304 edges), 4,000 MB limit

| Cores | Compute threads | Wall (s) | CPU (s) | Effective cores | Relative throughput | CPU pressure (s) | IO pressure (s) | Peak memory (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 33.5 | 26.1 | 0.78 | 1.00 | 1.53 | 5.09 | 739 |
| 2 | 1 | 35.1 | 27.2 | 0.77 | 0.95 | 0.10 | 3.75 | 737 |
| 4 | 2 | 36.0 | 28.1 | 0.78 | 0.93 | 0.06 | 2.65 | 762 |
| 8 | 4 | 31.7 | 28.8 | 0.91 | 1.06 | 0.05 | 2.26 | 809 |
| 16 | 8 | 30.9 | 28.2 | 0.91 | 1.09 | 0.05 | 2.06 | 807 |

**Complete ingest does not scale with cores.** Going from 1 core to 16 gives
1.09× throughput, and effective cores never pass 0.91. Each point is a single
run. Differences of 5–7% between neighbouring points are within what single
runs vary, and no plausible noise would turn this curve into scaling, so no
point was repeated.

**The low CPU/wall ratio is serial code, not starvation.**
- **CPU.** At 2 or more cores, CPU pressure is at most 0.10 s of a 31–36 s
  run, and validate's threads spend no measurable time runnable-but-waiting
  at 16 cores.
- **Memory.** There is no memory pressure; the peak is 0.8 GB against a
  4 GB limit.
- **I/O.** I/O pressure is 2–5 s.
- **The validate thread.** At 16 cores it runs 20.9 s of 27.9 s wall, sleeps
  7.0 s and waits on disk 4.1 s.
- **Workload size.** The profile is the same at S20 and S22 below, so this is
  not a small-input artifact.

### Region profile on 16 cores

Inclusive wall, with CPU/wall in parentheses. The regions are nested:
shaping, canonical encoding and append are inside validate.

| Region | S18 | S20 | S22 |
| --- | --- | --- | --- |
| Complete ingest | 31.9 s (0.90) | 126.7 s (0.92) | 516.5 s (0.93) |
| validate | 28.3 s (0.96) | 114.4 s (0.97) | 467.0 s (0.98) |
| validate/seal/shaping | 14.9 s (0.88) | 59.2 s (0.89) | 244.4 s (0.90) |
| validate/seal/canonical_encoding | 7.8 s (0.95) | 32.8 s (0.94) | 130.5 s (0.95) |
| …/canonical_encoding/adjacency_encoding | 4.1 s (0.95) | 17.1 s (0.93) | 65.7 s (0.94) |
| validate/append | 4.2 s (0.88) | 16.4 s (0.88) | 67.1 s (0.88) |
| validate/normalization | 0.9 s (2.91) | 4.2 s (2.88) | 17.8 s (2.92) |
| commit/publish | 2.4 s (0.54) | 9.8 s (0.53) | 39.6 s (0.54) |
| Throughput | 131,514 edges/s | 132,428 edges/s | 129,932 edges/s |

- **Shaping** is 47% of complete ingest at every scale and runs on under one
  core. **Canonical encoding** is 25%, **append** 13% and **publication** 8%.
  **Normalization**, the one region already parallel (#1472), is 3%.
- **Inside shaping**, the region tree attributes only fsync (5.1 s over 27,149
  calls at S20) and artifact authentication (1.0 s). The remaining 53 s of the
  59 s is unattributed.

### CPU profile inside validate (S18, flat)

`perf record -F 499` over one S18 validate on the frozen binary; the top 80
symbols are in [`core-scaling-1448/perf-validate-s18-flat.txt`](core-scaling-1448/perf-validate-s18-flat.txt).
The profile is flat:

| Share of validate CPU | Item |
| ---: | --- |
| 7.3% | SHA-256 compression, the largest single item: the digest written inline with each shaped and encoded output |
| about 6% | Zstd (Parquet encoding) |
| about 6% | Sorts |
| 2.2% | `PartitionPlan::partition_of` |
| 1.9% | Runtime catalog build |
| 2.3% | SipHash `HashMap` hashing |

No operation dominates.

### What this bounds (model, not measurement)

Using the S18 16-core profile, and assuming perfect parallel efficiency on 8
cores:

| Parallelized | Ingest | Speedup over the 1-core baseline |
| --- | ---: | ---: |
| Shaping alone | about 18.9 s | about 1.8× |
| Shaping and canonical encoding | about 12.0 s | about 2.8× |

Real efficiency will be lower. A scale-up criterion of 2× or more at 8 cores
therefore cannot be met by shaping alone, which is consistent with #1448's
scope: shaping first, then the next limiting region as a bounded blocker.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Baseline freeze and method, recorded before any timed run. |
| 2026-09-25 | Baseline curve, S18/S20/S22 region profiles and a flat CPU profile recorded. No candidate measured. |
