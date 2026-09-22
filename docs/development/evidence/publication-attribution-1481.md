# Publication attribution on the integrated tree (#1481)

**Measured 2026-09-20 on OVHC-AGENCY.** Stock release CLI at `main` `097e7631` (stock) and the same tree plus the named publication regions of PR #1512 (instrumented). Same Graph500 inputs (profile seed), fresh project per run, BenchExec CPUs 0–15 / 4 GiB, quiet-host guard before, during and after every observation; contended attempts are retained under `contended/` and excluded. Scheduler statistics were enabled for the runs and restored afterwards. Complete-ingest wall is the BenchExec wall of the five-command boundary (begin, two registrations, validate, commit), so it includes process startup and facade opening.

This reconciles publication with complete ingest; it is not a throughput result, an S26 qualification, or a claim that the #1387 floor is reachable or unreachable.

## Headline

| Rung | Variant | n | Edges | Ingest wall s (median) | Ingest CPU s | Allowance at 1M/s | Ingest ÷ allowance | Publish wall s | Publish CPU s | Publish µs/edge | Publish share of allowance |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S18 | stock | 3 | 4,194,304 | 28.477 | 26.708 | 4.194 | 6.79× | 2.326 | 1.330 | 0.554 | 55.4% |
| S18 | inst | 3 | 4,194,304 | 27.901 | 26.607 | 4.194 | 6.65× | 2.246 | 1.340 | 0.535 | 53.5% |

## Publication decomposition (instrumented runs)

Sequential children of `import_command/commit/publish`; wall is inclusive, sleep/runnable are calling-thread scheduler observations, and the parent residual is the visibility swap plus uninstrumented time.

### s18-inst-b1-a1 — publish 2.355 s wall, 1.340 s CPU, residual 0.001 s

| Child | Wall s | Process CPU s | CPU/wall | Thread sleeping s | Thread runnable s |
|---|---:|---:|---:|---:|---:|
| `generation_commit` | 0.743 | 0.440 | 0.59 | 0.308 | 0.0012 |
| `cas_install` | 0.737 | 0.360 | 0.49 | 0.376 | 0.0002 |
| `hydration` | 0.438 | 0.260 | 0.59 | 0.171 | 0.0001 |
| `publication_receipt` | 0.360 | 0.200 | 0.56 | 0.155 | 0.0002 |
| `read_authority` | 0.045 | 0.050 | 1.11 | 0.002 | 0.0000 |
| `prepare_encoding` | 0.024 | 0.030 | 1.23 | 0.001 | 0.0000 |
| `publication_authentication` | 0.006 | 0.000 | 0.00 | 0.002 | 0.0000 |
| `publication_intent` | 0.002 | 0.000 | 0.00 | 0.000 | 0.0000 |

### s18-inst-b2-a2 — publish 2.246 s wall, 1.340 s CPU, residual 0.001 s

| Child | Wall s | Process CPU s | CPU/wall | Thread sleeping s | Thread runnable s |
|---|---:|---:|---:|---:|---:|
| `cas_install` | 0.731 | 0.350 | 0.48 | 0.376 | 0.0001 |
| `generation_commit` | 0.698 | 0.440 | 0.63 | 0.258 | 0.0003 |
| `hydration` | 0.409 | 0.260 | 0.64 | 0.144 | 0.0002 |
| `publication_receipt` | 0.333 | 0.210 | 0.63 | 0.130 | 0.0001 |
| `read_authority` | 0.045 | 0.050 | 1.11 | 0.002 | 0.0000 |
| `prepare_encoding` | 0.022 | 0.020 | 0.91 | 0.000 | 0.0000 |
| `publication_authentication` | 0.004 | 0.010 | 2.65 | 0.001 | 0.0000 |
| `publication_intent` | 0.002 | 0.000 | 0.00 | 0.000 | 0.0000 |

### s18-inst-b3-a2 — publish 2.136 s wall, 1.330 s CPU, residual 0.001 s

| Child | Wall s | Process CPU s | CPU/wall | Thread sleeping s | Thread runnable s |
|---|---:|---:|---:|---:|---:|
| `cas_install` | 0.670 | 0.360 | 0.54 | 0.311 | 0.0001 |
| `generation_commit` | 0.669 | 0.430 | 0.64 | 0.240 | 0.0001 |
| `hydration` | 0.400 | 0.260 | 0.65 | 0.137 | 0.0001 |
| `publication_receipt` | 0.322 | 0.210 | 0.65 | 0.119 | 0.0002 |
| `read_authority` | 0.044 | 0.040 | 0.91 | 0.001 | 0.0000 |
| `prepare_encoding` | 0.024 | 0.030 | 1.25 | 0.000 | 0.0000 |
| `publication_authentication` | 0.004 | 0.000 | 0.00 | 0.001 | 0.0000 |
| `publication_intent` | 0.002 | 0.000 | 0.00 | 0.000 | 0.0000 |

## Where the CSR cost is

Adjacency (CSR) encoding runs inside `validate/seal/canonical_encoding`, before commit. Publication does not rebuild it.

| Observation | validate s | seal s | shaping s (CPU) | canonical_encoding s (CPU) | adjacency_encoding s (CPU) | adjacency µs/edge |
|---|---:|---:|---:|---:|---:|---:|
| s18-inst-b1-a1 | 25.206 | 19.641 | 12.211 (11.160) | 7.371 (7.010) | 3.652 (3.490) | 0.871 |
| s18-inst-b2-a2 | 25.064 | 19.545 | 11.862 (11.180) | 7.628 (7.280) | 3.693 (3.530) | 0.881 |
| s18-inst-b3-a2 | 24.868 | 19.304 | 11.872 (11.340) | 7.377 (7.070) | 3.566 (3.450) | 0.850 |
| s18-stock-b1-a1 | 25.530 | 19.974 | 12.392 (11.240) | 7.528 (7.160) | — (—) | — |
| s18-stock-b2-a2 | 25.510 | 19.928 | 12.039 (11.100) | 7.834 (7.430) | — (—) | — |
| s18-stock-b3-a1 | 25.327 | 19.710 | 11.980 (11.130) | 7.672 (7.320) | — (—) | — |

## Per-observation ingest accounting

| Observation | Ingest wall s | Ingest CPU s | validate cmd s | commit cmd s | Block I/O read GB | Block I/O write GB | Peak cgroup MB |
|---|---:|---:|---:|---:|---:|---:|---:|
| s18-inst-b1-a1 | 28.173 | 26.324 | 25.207 | 2.387 | 10.79 | 9.78 | 849 |
| s18-inst-b2-a2 | 27.901 | 26.661 | 25.065 | 2.273 | 10.75 | 9.78 | 835 |
| s18-inst-b3-a2 | 27.559 | 26.607 | 24.869 | 2.166 | 10.75 | 9.78 | 835 |
| s18-stock-b1-a1 | 28.554 | 26.567 | 25.530 | 2.380 | 10.75 | 9.78 | 850 |
| s18-stock-b2-a2 | 28.477 | 26.708 | 25.510 | 2.354 | 10.75 | 9.78 | 835 |
| s18-stock-b3-a1 | 28.159 | 26.738 | 25.328 | 2.248 | 10.75 | 9.78 | 853 |

## S22 receipts from contended attempts (structure only; timing excluded)

These runs completed correctly but the quiet guard saw other agents' cargo/rustc processes during the run, so their timings are **not accepted**. Shares are indicative of structure, not of duration.

### s22-inst-b1-a1 — 67,108,864 edges; publish 40.3 s (0.600 µs/edge, 60% of allowance), contended

| Region | Wall s | Process CPU s | Share |
|---|---:|---:|---:|
| `validate` | 443.8 | 432.3 | 100% of validate |
| `validate/seal` | 348.2 | 315.2 | 78% of validate |
| `validate/seal/shaping` | 196.5 | 183.8 | 44% of validate |
| `validate/seal/canonical_encoding` | 150.6 | 130.7 | 34% of validate |
| `validate/seal/canonical_encoding/adjacency_encoding` | 83.1 | 68.1 | 19% of validate |
| `commit/publish/cas_install` | 15.44 | 6.25 | 38% of publish |
| `commit/publish/generation_commit` | 11.16 | 6.98 | 28% of publish |
| `commit/publish/hydration` | 6.58 | 4.18 | 16% of publish |
| `commit/publish/publication_receipt` | 6.17 | 3.43 | 15% of publish |
| `commit/publish/read_authority` | 0.63 | 0.63 | 2% of publish |
| `commit/publish/prepare_encoding` | 0.31 | 0.30 | 1% of publish |
| `commit/publish/publication_authentication` | 0.01 | 0.02 | 0% of publish |
| `commit/publish/publication_intent` | 0.00 | 0.00 | 0% of publish |

## Current integrated-tree baseline (2026-09-22, `main` `aaf20d83`)

`main` moved after the S18 section above (the Source/Artifact lifecycle #1502 and the shape-end fix #1531 landed), so the current tree was re-baselined before the repair A/B. Same inputs (SHA-256 verified identical), same limits, quiet guard before/during/after; every observation below was accepted by the guard. Medians of complete-ingest wall (five-command boundary).

| Rung | Variant | n | Ingest wall s (median) | Ingest µs/edge | ÷ allowance | Publish wall s (median) | Publish µs/edge | Publish share of allowance |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| S18 | base | 3 | 31.818 | 7.586 | 7.59× | 2.394 | 0.571 | 57.1% |
| S22 | base | 3 | 547.803 | 8.163 | 8.16× | 42.014 | 0.626 | 62.6% |

Clean S22 publication decomposition (base-s22-1-a1, publish 42.014 s wall / 22.66 s CPU): `cas_install` 16.296 s (6.85 CPU), `generation_commit` 11.637 s (7.10 CPU), `hydration` 7.318 s (4.23 CPU), `publication_receipt` 5.723 s (3.45 CPU), `read_authority` 0.715 s (0.71 CPU), `prepare_encoding` 0.313 s, `publication_authentication` 0.008 s, `publication_intent` 0.002 s. The four large regions carry the same structure the contended run showed (39/28/17/14%), now on quiet timings; each runs at 0.4–0.65 effective cores. Adjacency CSR encoding remains inside `validate/seal/canonical_encoding` on the current tree. Per-observation walls: 547.80 / 582.47 / 536.86 s; block I/O ≈ 185–192 GB read / 180 GB write per run; every run reached the enforced 4 GB RSS ceiling.

Publication alone (0.571–0.626 µs/edge) still exceeds half of the 1.00 µs/edge total allowance while complete ingest is 7.6–8.2 µs/edge, so no publication-side allocation can fit the floor without the parallel-ingest work owned by #1387's other children.

## Reader-preparation repair and A/B (2026-09-22)

**Repair (one commit on top of `aaf20d83`).** Construction reader preparation (`hydration` + `read_authority`) moves from after `CURRENT` to against the durable, lease-verified candidate immediately before `CURRENT`, using the publisher's pre-`CURRENT` preparation seam with cancellation still pollable to the commit point. Effect: a candidate that cannot be hydrated or authorized fails closed — `CURRENT` stays on the parent — instead of committing a generation whose reader workspace must be recovered afterward. Receipts nest `hydration`/`read_authority` under `generation_commit` for fresh publications; replay keeps the sibling shape. `BeforeInstall` keeps the committed-authority recovery contract; an interrupted attempt still recovers through the standard transaction-recovery entrypoint. Correctness evidence: new storage tests (preparation observes `CURRENT` unchanged; preparation failure leaves `CURRENT` unchanged and the session recovers), the split API refresh-failure tests, and the unmodified recovery/corruption/cancellation suites (graphforge-storage lib 1235 passed; graphforge-api construction suites green; CLI receipt test asserts the new nesting).

**A/B.** Binaries differ only by the repair commit (`aaf20d83` vs `aaf20d83`+repair; SHA-256 in the machine-readable companion). Alternating quiet pairs, same inputs, same limits. S18 n=3/arm; S22 n=2/arm candidate against the n=3 baseline above.

| Rung | Variant | n | Ingest wall s (median) | Ingest µs/edge | Publish wall s (median) | Publish µs/edge |
|---|---|---:|---:|---:|---:|---:|
| S18 | base | 3 | 31.818 | 7.586 | 2.394 | 0.571 |
| S18 | cand | 3 | 31.045 | 7.402 | 2.398 | 0.572 |
| S22 | base | 3 | 547.803 | 8.163 | 42.014 | 0.626 |
| S22 | cand | 2 | 534.653 | 7.967 | 41.737 | 0.622 |

**Criterion outcome.** Publish wall is unchanged within noise (S18 2.394 vs 2.398 s; S22 42.0 vs 41.7 s median): the repair moved work inside the same publish region and did not remove it, as predicted. Whole-ingest medians improved by 0.77 s at S18 and 13.2 s at S22, both **below** the predeclared benefit thresholds (9.2 s and 91.3 s from `max(3% of baseline median, 2× baseline spread)`), so **no whole-ingest throughput gain is demonstrated or claimed**. The repair is justified and kept for its fail-closed publication semantics (issue completion scenario 2): corruption or failure during reader preparation now leaves the acknowledged generation unchanged. Publication's remaining budget deficit is not addressed by this repair and remains owned by the parallel-ingest workstreams under #1387; attribution and this repair must not be read as progress toward the 1M edges/s floor.

## Provenance

```json
{
  "contract": "graphforge-publication-attribution-1481/1",
  "host": "OVHC-AGENCY (16 CPUs, md RAID1 ext4 root)",
  "main_sha": "a902f23f83af2fa0cf018bccbf1c1e9997e03603",
  "stock_tree": "097e7631d12dea22ca24bff2601ad31801684cb3",
  "binaries": {
    "stock-gf": "6b6e62566c37044e006fdbeddd588d1e54966c4ac15b60d341486b866e6e90c0",
    "instrumented-gf": "8627f201182520610a3fab171ac44681d05f1408109f34b331f58d6b9662c098"
  },
  "inputs": {
    "generator_source_sha256": "a7cd8397ce191c48094d8812eb43dfc894084645dd439e76fb8704b35bb61fdc",
    "seed": 13907095936298285200,
    "edge_factor": 16,
    "s18": {
      "edges.parquet": "f112ccbec94875f36f113e9bdf3e6e7d88e3105bafbaeeb3a7cb42ac4883e9c4",
      "nodes.parquet": "44c9dfd9325013d0f6ea2f03bd86b00d2bea01265254cb28f70f0881a6478075"
    },
    "s22": {
      "edges.parquet": "1c0ff75485f75e904cbd59b6f5d42da1d8b1af6ddac59ee6c495a4948a428d13",
      "nodes.parquet": "bcbcbea526e61ceb63f6006ee5f56de6bb4f74cffdd68dc6eff6d230d3897f06"
    }
  },
  "resource_limits": "BenchExec runexec --cores 0-15 --memlimit 4 GiB, systemd user scope, fresh project per run",
  "boundary": "complete five-command ingest: begin, register-parquet x2, validate, commit (walltime from BenchExec)",
  "quiet_guard": "gf-quiet-host.sh + per-process guard before/during(1 s)/after; contended observations retained under contended/ and excluded",
  "scheduler_stats": "kernel.sched_schedstats=1 for the runs, restored to 0 afterwards",
  "floor_edges_per_second": 1000000
}
```

Raw receipts, BenchExec outputs, quiet samples and the harness (`run-measured.sh`, `ingest.py`, `quiet.py`, `run-pairs.sh`, `analyze.py`, `render-report.py`) are retained in `/home/ubuntu/gf-1481-evidence/` on OVHC-AGENCY. The machine-readable companion is `publication-attribution-1481.json`.

## Predeclared A/B protocol for the publication repair (declared before measurement)

The S18 attribution justified one bounded repair: reader preparation
(`hydration` + `read_authority`) moves from after `CURRENT` to against the
durable candidate immediately before `CURRENT`, using the publisher's existing
pre-`CURRENT` preparation seam. Its primary predicted effect is fail-closed
publication (a candidate that cannot be hydrated never becomes visible); the
wall-time effect is bounded by work moved inside the same publish region and is
not assumed to be a gain. The A/B decision rule, declared before any
candidate-build measurement:

- **Builds.** Baseline = release CLI at the pre-repair integrated tree; candidate
  = the same tree plus the repair commit only. Binaries hashed; inputs are the
  recorded Graph500 parquet files (SHA-256 verified); fresh project per run.
- **Resources.** Every observation runs under BenchExec `runexec --no-container
  --cores 0-15 --memlimit 4GB`; quiet-host guard before, during (1 s samples)
  and after; contended attempts are retained under their run directory and
  excluded. Scheduler statistics enabled for the runs and restored afterwards.
- **Rungs.** S18 first, three accepted alternating observations per arm; S22
  two accepted observations per arm if the repair shows an S18 effect or a
  correctness-shaped benefit, else S22 records the baseline reconciliation
  only.
- **Comparison.** Medians of complete-ingest wall (the five-command boundary).
  A whole-ingest benefit is demonstrated only when candidate median improves on
  baseline median by more than `max(3% of baseline median, 2× baseline spread)`;
  publication-region changes are reported separately as µs/edge and never
  counted as whole-ingest gains on their own.
- **Correctness gate.** The repair lands only with the existing
  recovery/corruption/cancellation suites unweakened plus new tests proving
  preparation runs before `CURRENT` and that preparation failure keeps
  `CURRENT` unchanged.
