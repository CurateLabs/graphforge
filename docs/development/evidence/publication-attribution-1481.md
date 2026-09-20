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
