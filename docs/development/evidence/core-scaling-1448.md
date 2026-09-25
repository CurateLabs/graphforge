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

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Baseline freeze and method, recorded before any timed run. |
