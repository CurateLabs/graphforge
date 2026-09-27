# Canonical encoding admission lanes (#1600)

The candidate compresses canonical Parquet batches on bounded admission lanes,
then replays the original write chunks on the coordinator. Adjacency construction
pipelines projected decoding, sorts both directions concurrently, and builds
independent CSR direction/relation files on admission lanes. Artifact receipts
and the adjacency manifest retain their original order.

## Byte identity and cancellation

`determinism.json` records the exact `DETERMINISM_SAME_INPUT` digest set from the
#1599 merge tree (`e4a4f3cb1a056eaedbd85768449c8dd12740629b`) and the candidate.
The encoded, shaped, and partition digests are identical. The candidate test also
compares the same fixture with eight admission lanes and reversed job dispatch.

Commands use an isolated `CARGO_TARGET_DIR` and an ext4 `TMPDIR`:

```sh
# On the baseline tree:
cargo test -p graphforge-storage --lib same_input_twice_produces_identical_digests -- --nocapture
# On the candidate tree:
cargo test -p graphforge-storage --lib determinism -- --nocapture
```

The baseline test passed. The candidate determinism suite passed 16 tests, with
one existing measurement test ignored. Additional lane tests cover heterogeneous
property artifacts, spilled CSR byte/metric identity, cancellation after a real
Parquet artifact is installed, cancellation during decoding and CSR work, worker
joins, spill cleanup, and successful retry after cancellation. Both ordinary and
reversed schedules are exercised.

## Measurement protocol

`measure.sh BASE_GF CAND_GF INPUTS OUT` collects 21 runs: three alternating
baseline/candidate pairs at each of S18 and S20 on 16 cores, plus the candidate's
S18 curve at 1, 2, 4, 8, and 16 cores. The 1- and 8-core points have three runs.
Each run uses BenchExec with a 4,000 MB limit, dropped caches, and 60 seconds of
sustained host quiet before execution. The host must also be quiet afterward.
Each completed ingest is reopened for node/edge counts and full identity scans;
the receipts contain query-answer digests.

The driver first measures the three 8-core runs and stops if median canonical
encoding process CPU / wall is below 2.0. `summarize.py OUT` rejects missing,
unexpected, failed, or busy runs and mismatched S18/S20 answer digests. It reports
whole-ingest wall and the parent #1448 core-use curve separately from #1600's
canonical-encoding criterion.

## Results

The frozen candidate binary is built from `c94b6cf13e6375dcdbad072bdb3db6ee9358cc71`.
Subsequent commits add tests, comments, and this evidence; production behavior is
unchanged. `candidate/source.json` and `candidate/host-state.txt` record source,
binary, and input identities. `candidate/driver.log` records all 21 quiet runs;
`candidate/runs/` contains the raw command receipts and BenchExec reports.
Reproduce the summary with:

```sh
python3 docs/development/evidence/encoding-lanes-1600/summarize.py \
  docs/development/evidence/encoding-lanes-1600/candidate
```

Canonical encoding at S18 / 8 usable cores reached CPU/wall ratios **1.9259,
2.1276, and 2.1086**: median **2.1086**, meeting #1600's >= 2.0 criterion. No run
was excluded or replaced. Median region wall was 4.178 seconds.

| Whole ingest, 16 cores | Baseline median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| S18 | 26.93 s | 23.66 s | -12.1% |
| S20 | 110.90 s | 94.68 s | -14.6% |

All baseline/candidate count and full-scan answer digests match at both scales.
Peak memory across each three-run arm rose from 829 to 1,067 MiB at S18 and from
2,783 to 2,901 MiB at S20, below the 4,000 MB cap. The bounded compression window
retains Arrow batches and compressed chunks while the coordinator installs them.

| S18 usable cores | Runs | Whole-ingest median wall | CPU/wall | Relative throughput |
| --- | ---: | ---: | ---: | ---: |
| 1 | 3 | 34.17 s | 0.77 | 1.00 |
| 2 | 1 | 34.30 s | 0.79 | 1.00 |
| 4 | 1 | 35.46 s | 0.79 | 0.96 |
| 8 | 3 | 24.67 s | 1.19 | 1.39 |
| 16 | 1 | 23.49 s | 1.28 | 1.45 |

**#1448 remains open:** whole-ingest 8-core throughput and CPU/wall are below its
2.0 criteria. Admission gives only one background construction lane at 2 and 4
usable cores, where the multi-worker Parquet/CSR path stays serial. Routing and
append remain outside this issue's scope. The complete per-run results and
pressure counters are in `candidate/summary.txt`.
