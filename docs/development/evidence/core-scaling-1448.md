# Complete-ingest core scaling (#1448)

Evidence for [#1448](https://github.com/CurateLabs/graphforge/issues/1448): the
baseline core-count curve and region profile, recorded before any candidate
change. The issue requires a scale-up criterion agreed with the maintainers
after this baseline and before any candidate result. The criterion, the shaping
lanes candidate and its measurements follow the baseline below.

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
| about 8% | Sorts |
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

## Material scale-up criterion (agreed 2026-09-25, before any candidate result)

Recorded on #1448 after the baseline above and before any candidate was measured. At S18, with the baseline method, the build under test must reach both:

- throughput at 8 usable cores of at least 2.0 times its own 1-core throughput;
- at least 2.0 effective cores (CPU / wall) at 8 usable cores.

The 1- and 8-core points are the median of three runs. The shaping adoption gate (at least 10% whole-ingest median wall at S18 and S20, identical answers) is separate and unchanged.

## Where shaping spent its time (2026-09-25)

The baseline's region tree attributed only about 6 of shaping's 59 s at S20. This branch adds shaping sub-regions:

- `shape_planning`, `shape_routing` and `shape_family_finish`;
- `partition_load_wait`, entered only when the coordinator blocks on a load;
- `surrogate_assignment`, `endpoint_resolution`, `shape_row_finish`, `runtime_catalog` and `shape_completion`.

The same names are added to the certify allowlist and schema.

A frame-pointer `perf` profile of the coordinator over one S18 validate, together with `strace -c`, showed that shaping's coordinator time was mostly per-file control work rather than data work:

| Share of shaping coordinator CPU | Function |
| ---: | --- |
| 27% | family finish, **58% of it retiring sealed segments** one at a time: authenticate, unlink the segment and its capability, sync the directory twice |
| 20% | fixed-width routing: SHA-256 of the source, spill writes, `partition_of` |
| 14% | endpoint resolution |
| 12% | sealing boundary spills: fsync, rename, capability install, one after another |
| 6% | runtime catalog |
| 5.5% | splitter sampling, 70% of it `statx` |

The same `validate` made 1,493,465 `statx`, 201,416 `openat` and 546,424 `read` calls. The redundant part is independent of lanes and is filed as #1596.

## The candidate: shaping lanes

All lanes are leased from the instance construction CPU admission (#1586, ADR 0047). Each lane does only filesystem work; the coordinator charges the evidence afterwards in a fixed order.

- **Segment retirement.** A finish stage's segments are unlinked on up to 8 lanes. Every segment is attempted even after one fails, so the set removed and the evidence charged are the same for any lane count.
- **Boundary seals.** Every fixed family's open spills at a boundary are sealed by one lane pool. Each lane seals into its own directory batch, which the boundary's batch absorbs, so a boundary still makes its names durable with one directory flush.
- **Finish-time loads.** Up to 8 workers, with a second, byte bound on the window: a partition is dispatched only while the routed bytes of every unconsumed partition stay within two partition budgets. The worst-case materialization is unchanged; small partitions load concurrently.

Without an admission, or without a free lane, the calling thread does the same work in the same order. Tests compare one lane with eight, and a deliberately permuted schedule where lanes take their jobs in reverse, for equal shaped outputs and equal evidence. File identities are normalized, because inode numbers differ between runs and are reused after unlinks. Mutations that lose a lane batch's flush, charge in the wrong family, or stop retirement at the first failure are all caught.

## Candidate results

Driver: [`core-scaling-1448/candidate.sh`](core-scaling-1448/candidate.sh). Supplementary pair: [`supplement.sh`](core-scaling-1448/supplement.sh). Summaries: `summarize_candidate.py`.

- **Setup.** Three alternating pairs per scale (AB, BA, AB), BenchExec on CPUs 0–15 with a 4,000 MB limit. Caches are dropped and the host is held quiet for 60 s before each run.
- **Excluded runs.** A run whose host check after it ended reads `BUSY` is excluded from the medians, as protocol §5.3 requires.
- **Baseline.** `8d553e0e`, `gf` SHA-256 `92184d6b…a2c32c4`.

### First A/B: candidate `e0746156` (raw records in `core-scaling-1448/candidate/`)

| Scale | Baseline median | Candidate median | Change | Per-pair candidate − baseline |
| --- | ---: | ---: | ---: | --- |
| S18 | 31.53 s | 28.45 s | −9.8% | −2.97, −3.08, −2.92 s |
| S20 | 128.00 s | 113.51 s | −11.3% | −16.12, −15.49, −14.08 s |
| S22 (one run each) | 527.97 s | 453.77 s | −14.1% | — |

S18 missed the 10% gate by 0.2 points. The maintainer decision recorded on #1448 was one further lanes step within the same concern, then one rerun of the full A/B, reporting both runs. That step pooled every family's boundary seals into one lane pool; before it, routing waited on four pools per boundary.

### Rerun: candidate `a8123879`, after seal pooling (raw records in `core-scaling-1448/candidate-rerun/`)

| Scale | Baseline median | Candidate median | Change | Per-pair candidate − baseline (accepted pairs) |
| --- | ---: | ---: | ---: | --- |
| S18 | 31.44 s (3 runs) | 28.08 s (4 runs) | **−10.7%** | −3.27, −3.99, −3.45 s |
| S20 | 132.03 s | 114.49 s | **−13.3%** | −17.39, −16.89, −18.34 s |
| S22 (one run each) | 528.92 s | 479.42 s | −9.4% | — |

Over the three accepted S18 pairs alone, the candidate median is 27.98 s, −11.0%. Query answers (node count and scan, edge count and scan) are identical between the arms at S18 and S20, and equal the digests #1585 recorded.

Shaping ran at 1.13–1.20 CPU/wall in the candidate, against 0.86–0.89 in the baseline. Peak memory rose 1–3% (S18 about 830 MiB against 810 MiB; S20 about 2.77 GB against 2.70 GB).

**Method deviations, all disclosed in the raw logs:**

- **`ab-s18-r1-base` ended `BUSY`.** Another project's agent started work on the shared host right after the run. Its own CPU pressure (0.047 s) matches every other run, but it is excluded as the protocol requires, and a supplementary pair (`r4`) was measured instead.
- **Two runs overlapped.** The first attempt at that supplement was started by a chained command whose wait had timed out, so it ran alongside the curve. The two overlapping runs (`ab-s18-r4-cand`, kept as `excluded-overlap-ab-s18-r4-cand`, and `curve-s18-c1-r3`) are excluded. The supplement was rerun after the main driver finished, with a replacement 1-core run, `curve-s18-c1-x1`.
- **`regions-s22-cand` ended `BUSY`.** Its CPU pressure over the run (0.78 s) is below its baseline's (0.89 s). S22 is a single-run region profile, not part of the gate, and the first run's fully quiet S22 pair is reported above.

**Shaping adoption gate: met on the rerun** (S18 −10.7%, S20 −13.3%, answers identical). The first A/B, which did not meet it at S18, is reported above.

### Candidate core-count curve, S18 (rerun, 4,000 MB)

| Cores | Runs | Wall (s) | CPU (s) | Effective cores | Relative throughput |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3 | 35.46 | 26.70 | 0.75 | 1.00 |
| 2 | 1 | 34.97 | 27.55 | 0.79 | 1.01 |
| 4 | 1 | 36.38 | 28.83 | 0.79 | 0.97 |
| 8 | 3 | 29.41 | 27.96 | 0.95 | 1.21 |
| 16 | 1 | 27.48 | 28.37 | 1.03 | 1.29 |

**Core-use criterion: not met.** At 8 cores the candidate gives 1.21× its 1-core throughput and 0.95 effective cores; the agreed target is at least 2.0 on both. The first run's curve agrees: 1.19× and 0.97.

Up to 4 cores the candidate is no faster than at 1 core. The lanes are leased from the construction admission, which ADR 0047 sizes as `compute_threads − reserve`. In automatic mode that gives a limit of 1 at 1–4 cores, which means no parallel lanes, 3 at 8 cores and 7 at 16 cores (`construction_cpu_split`, `resource_policy.rs`).

**Where the 8-core candidate still runs on one core** (median of three runs):

| Region | Wall | CPU / wall |
| --- | ---: | ---: |
| validate/seal/canonical_encoding | 8.30 s | 0.90 |
| validate/seal/shaping/shape_routing | 4.52 s | 0.83 |
| validate/append | 4.19 s | 0.86 |
| commit/publish | 2.50 s | 0.53 |

**Next mechanism.** Reaching 2× at 8 cores needs about 17 s against 29 s now, and no single region closes that. Perfect four-way encoding would save about 6 s, which gives about 1.3× (model, not measurement). Encoding is the largest single step. It is filed as #1600, a native blocker of #1448. Routing and append parallelism remain candidates after it.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-25 | Baseline freeze and method, recorded before any timed run. |
| 2026-09-25 | Baseline curve, S18/S20/S22 region profiles and a flat CPU profile recorded. No candidate measured. |
| 2026-09-25 | Scale-up criterion agreed and recorded on #1448 before any candidate result. |
| 2026-09-25 | Shaping profile, candidate lanes, first A/B (S18 −9.8%, below the gate) and the candidate curve. |
| 2026-09-26 | Maintainer decision: one further lanes step (seal pooling), then one rerun. Rerun: S18 −10.7%, S20 −13.3%; core-use criterion not met; next mechanism filed as #1600. |
