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
