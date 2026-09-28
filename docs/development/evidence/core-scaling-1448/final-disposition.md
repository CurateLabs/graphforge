# Complete-ingest core scaling: final disposition (#1448)

## Decision

The bounded work under #1448 is complete, but its core-use target is **not
met**. Shaping and canonical encoding were adopted after their predeclared
whole-ingest gates passed. The agreed S18/8 gate still fails on both measures:
the integrated candidate reaches 1.39× its own one-core throughput and 1.19
effective cores, against 2.0× and 2.0. Routing and append lane experiments
also received evidence-backed no-go dispositions, so no more implementation
is authorized by this issue. The latest measured bottleneck is ordered durable
append; further parallelism there would require changing its durability
protocol, outside this bounded stack.

This is a Linux OVHC-AGENCY result. It makes no Apple-specific performance
claim. #1387 retains its unchanged 1,000,000 edges/s target and remains open;
this evidence supplies its current measured rate and the core-scaling outcome
to #1572. #1572 still owns the explicit v0.6.0 release-scope decision.

## Integrated results

The adopted shaping result and its baseline region profile are in
[`core-scaling-1448.md`](../core-scaling-1448.md). The post-encoding A/B, curve,
per-run receipts, and determinism evidence are in
[`encoding-lanes-1600`](../encoding-lanes-1600/README.md). The following
representative rows use the post-encoding candidate: the S18 and S20 runs
whose complete-ingest wall equals the A/B median, and the one fresh S22 run on
current `main`. Region values are inclusive, so nested rows must not be added.
The `validate residual` is the direct work left at the validate root after its
child regions; the `other ingest` remainder is whole-process `runexec` minus
the validate-root receipt. These two remainders reconcile the nested profile
to the complete-ingest boundary.

| Scale | Complete ingest wall / CPU / effective cores | Validate wall / CPU | Validate residual wall / CPU | Shaping wall / CPU | Canonical encoding wall / CPU | Append wall / CPU | Other ingest wall / CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S18 | 23.66 s / 29.96 s / 1.27 | 20.58 s / 28.39 s | 0.41 s / 0.11 s | 10.39 s / 13.06 s | 4.61 s / 8.85 s | 4.18 s / 3.70 s | 3.08 s / 1.57 s |
| S20 | 94.68 s / 118.74 s / 1.25 | 82.29 s / 112.77 s | 1.59 s / 0.56 s | 41.51 s / 51.35 s | 18.46 s / 34.33 s | 16.33 s / 14.26 s | 12.39 s / 5.97 s |
| S22 | 363.24 s / 502.34 s / 1.38 | 323.27 s / 478.23 s | 6.03 s / 2.52 s | 165.99 s / 224.24 s | 64.07 s / 137.84 s | 67.33 s / 59.09 s | 39.97 s / 24.11 s |

The S18/S20 profiles are from 16 usable CPUs under the 4,000 MB A/B cap.
Their representative receipts are the median-wall candidate runs; the CPU
values are from those same runs. The S22 run used 16 usable CPUs with no memory
limit. It returned 4,194,304 nodes and 67,108,864 edges, and reopened full-scan
digests are retained beside its receipts. S22 throughput was 184,749 edges/s,
well below #1387's unchanged 1,000,000 edges/s floor.

The S18/S20 candidate and core-count curve were built from `c94b6cf1`, the
production behavior merged as #1605. Current `main` adds #1595's query-spill
policy and tests; that change does not alter the storage import path. The new
S22 profile below is measured directly on current `main`.

The post-encoding S18 core-count curve used three runs each at 1 and 8 CPUs and
one run at 2, 4, and 16 CPUs. Median whole-ingest wall was 34.17 s at 1 CPU and
24.67 s at 8 CPUs: 1.39× throughput, 1.19 effective cores at 8 CPUs. At 16
CPUs the single run reached 1.45× and 1.28 effective cores. CPU pressure at
8 CPUs was 0.05 s, IO pressure 2.44 s, and peak memory 996 MiB under the 4,000
MB limit; CPU starvation and memory pressure do not explain the low core use.
The lane admission and remaining serial/I/O-bound phases constrain scaling.
Per-region running, sleeping, runnable, and I/O-wait counters are retained in
the receipts; the fresh S22 runexec record reports 0.655 s CPU pressure and
20.544 s IO pressure.

The current S22 run used source `e90db03637eba072b4d733fb25eceb017a8815f6`
and a release binary with SHA-256 recorded in
[`final-integrated-s22/host-state.txt`](final-integrated-s22/host-state.txt).
It ran `runexec --no-container --cores 0-15` after dropping caches and a
60-second quiet-host window. BenchExec reported 363.244 s wall, 502.345 s CPU,
11,106,258,944 bytes peak memory, 0.655 s CPU pressure, and 20.544 s IO
pressure. The project was on ext4. The complete command record, host/input
identity, five import receipts, node/edge counts, and full-scan answer digests
are in [`final-integrated-s22`](final-integrated-s22/).

## Child outcomes and correctness

- #1599 shaping passed its gate on the accepted rerun: S18 improved 10.7%, S20
  13.3%, and reopened answers matched. #1600 encoding passed its separate
  admission-lane gate at 2.1086 median encoding CPU/wall; its integrated A/B
  improved complete-ingest wall 12.1% at S18 and 14.6% at S20, with matching
  answers. Both changes are retained in production.
- #1606 routing was a no-go: route CPU/wall was 1.2277, and complete-ingest
  improvements were 6.9% at S18 and 6.6% at S20, below the 10% adoption gate.
- #1607 append preparation was a no-go: candidate append CPU/wall was 0.926;
  complete-ingest improvements were 2.8% at S18 and 0.8% at S20. The ordered
  durable append path remains the measured bottleneck. No routing or append
  candidate code was adopted.
- The encoding determinism suite covers one- and eight-lane output identity,
  reversed dispatch, cancellation after artifact installation, cleanup, and
  retry. On current `main`,
  after `mkdir -p /home/ubuntu/gf-1448/.agent-tmp/tck`,
  `TMPDIR=/home/ubuntu/gf-1448/.agent-tmp/tck CARGO_TARGET_DIR=/home/ubuntu/gf1448-check-target cargo test --locked -p graphforge-api --test bdd`
  passed 118 public API scenarios and the full 3,897-scenario openCypher TCK
  with no regressions. The runner emitted its advisory TCK time warning (135.9
  s against a 66.5 s baseline); this is not an ingest measurement or a
  correctness failure. From `benchmarks/`,
  `PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -p 'test_gdc_*.py'`
  passed 82 GDC contract and runner tests. These are contract/runner checks,
  not an audited live GDC certification. The #1605 merge gate also passed CI Gate, Rust
  Quality, macOS graphforge-storage Durability, and Native Durability
  Aggregate. The fresh S22 import and reopened count/full-scan checks passed;
  digests and all evidence-file hashes are retained in this directory.

The lane experiments preserved the single coordinator for progress and
publication boundaries. No durable format, public API, or durability-protocol
change is proposed here, so no ADR is needed.

## Disposition

Close #1448 as a measured no-go on the parent core-use objective, with the
adopted scoped improvements retained and all bounded child experiments
complete. Record the measured S22 rate and unmet scale-up criterion on #1387;
keep its 1M target unchanged. Supply this result to #1572, where a maintainer
must choose the release path. This issue does not make that release decision
or waive #1387.
