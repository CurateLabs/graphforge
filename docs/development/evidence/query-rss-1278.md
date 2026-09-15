# Query RSS diagnosis for #1278

This is diagnosis evidence, not an S24 admission result or a completed repair.
The accepted S18/S19/S20/S22 prefix remains unchanged. The investigated executable
source is `989579078cc7e71ca4deaa1464afbbed7d222222`; its Rust product sources,
benchmark runner sources and Cargo lockfile are identical to the accepted
`710c6c64f4718c0664d08bdd3aafe21de4a29eba` measurement source.

## Preserved refusal

The existing `completed_prefix` validator accepts all four preserved rungs.
Calling the unchanged `progressive_host_run._admit_projection` for S24 refuses
only `rss_bounded_or_plateaued`, both with the issue's historical free-capacity
value and with newly measured free space. The reserve remains 141,258,578,535 B.
The historical process peak rises 199,135,232 → 262,049,792 B: exactly 60 MiB,
or 31.5939%, above the 10% gate.

The largest phase peaks are query and clean-import query proof. Query operator
receipts retain 1,000 emitted candidates, zero full node/edge scans and zero
DataFusion reservations after release. These bounded operator observations do
not bound the full CLI process. In particular, rebuilding derived adjacency is
part of the real query path before the ordered query returns its rows.

## Measurement method

The isolated build command was:

```bash
env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u LLVM_PROFILE_FILE \
  CARGO_TARGET_DIR="$DIAG_TARGET" CARGO_BUILD_JOBS=4 \
  CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --release -p graphforge-cli
```

Rust is 1.96.0 (`ac68faa20`, LLVM 22.1.2), with release optimization and debug
symbols. The frozen executable SHA-256 is
`ff1df1d01ee229db4f0ed816d62ef018f6107e41fedaa801e2849b79dc16e127`.
The diagnostic generator is the existing frozen accepted generator, preserving
its seed, edge factor, label scrambling, duplicate relationships and self-loops.

`benchmarks/diagnostics/rss_1278.py` defines S16/S17/S18 (1×/2×/4×) fixtures,
ordinary CLI construction/reopen/count/query/export/full verification/clean
import, and three fresh-process observations for every canonical query on both
source and imported projects. The canonical output remains Parquet, including
when its filename ends in `.arrow`. An independent oracle reads generated edge
multiplicities and excludes reuse of the same edge in a two-hop path; results
must match that oracle and agree across projects/repetitions.

Only post-exec `/proc/PID/status` samples from the target executable contribute
to process VmHWM. Samples separately retain current, anonymous and file-backed
RSS. Linux `wait4` maxrss is recorded separately because it can include the
Python parent's pre-exec high-water mark. A dedicated 128-MiB-parent/`true` probe
returned 142,960 KiB in `wait4`, demonstrating why it is unsuitable here.

Heaptrack 1.5 traces are allocation diagnostics, not admissible RSS/time receipts.
`heaptrack_print -m 0` avoids its documented incorrect merged-peak presentation.
The accompanying event summarizer independently checks allocation totals, global
live-heap peak and remaining live bytes. Individual category peaks are not
simultaneous and must not be summed.

## Attempt ledger

- The initial S16 workload and query correctness checks passed, but its comparison
  was stopped when the pre-exec `wait4` measurement flaw was verified. Preserve
  this attempt; its `wait4` values are not comparable GraphForge RSS.
- The corrected second attempt refused before its first command because another
  host build was active. It contains no measured workload.
- The preliminary S16 two-hop Heaptrack trace ran while a sibling build was
  active. Its allocation events are diagnostic; its RSS and elapsed time do not
  establish an uncontended comparison. It recorded 47,168 allocation events,
  103,545,094 B peak live heap and 146,532 B live at process exit, matching
  Heaptrack's own totals to its printed precision.

## Attribution under investigation

The S16 trace follows the real Rust facade into
`PersistentAdjacencyProvider::rebuild_and_serve`, then the streaming adjacency
builder. At the heap peak, accumulator/flush allocations account for
101,713,264 B. Independently, merge cursors peak at 1,048,576 B. Trace stacks bind
that allocation to `RunCursor::open`'s `BufReader::with_capacity(1 << 20, ...)`.

The production defaults use 1,048,576 entries per run and merge fan-in 64.
`merge_keyed_runs` opens all inputs of a merge concurrently. Thus the default
S20/S22 workloads imply 16/64 simultaneous 1-MiB reader buffers, a 48-MiB increase
before reaching the fan-in cap. Accumulator capacities remain alive during
merging. This is a concrete bounded-growth hypothesis, not an unbounded leak,
and explains a potential 80% of the historical 60-MiB process-RSS difference.
It does not yet assign the remaining difference or prove a repaired gate pass.

## Repair direction and regression design

Evaluate a smaller, explicitly bounded aggregate merge-reader working set in
`crates/graphforge-storage/src/adjacency/builder.rs`. Preserve fan-in, exact
24-byte records, ordering, spill limits, cancellation and failure cleanup.
Do not increase the memory budget or modify admission calculations.

The repair regression should observe actual reader allocation through the
production adjacency build used by a real facade query, crossing 16/64 readers
and the 65-run compaction boundary. Assert its aggregate live-reader byte bound,
then check canonical counts and ordered one-/two-hop results against independent
edge-multiset oracles before and after reopen/export/verification/import.
Measure process VmHWM separately: logical counters alone are not native RSS proof.

After a verified repair and integration with any separately justified sibling
work, regenerate a single comparable S18/S19/S20/S22 prefix with frozen identities.
Replay full S24 admission using fresh capacity and the unchanged reserve. Stop
at the first failed gate; do not execute S24/S25/S26. #1278 remains open until its
repair and full admission acceptance outcomes are met.

## Completed comparison and allocation profiles

[The sanitized ledger](query-rss-1278.json) preserves all 135 successful lifecycle
and repeated-query command observations, their correctness results, identities,
profile summaries, and hashes of local raw evidence. All 72 repeated query
observations matched the independent input oracle and source/imported results.

| Fixture | Edges | Maximum repeated-query VmHWM B | Peak live heap B (profiled two-hop) | Peak live cursor buffers B |
| --- | --- | --- | --- | --- |
| S16 | 1,048,576 | 176,492,544 | 103,545,207 | 1,048,576 |
| S17 | 2,097,152 | 176,668,672 | 104,306,760 | 2,097,152 |
| S18 | 4,194,304 | 179,408,896 | 106,843,178 | 4,194,304 |

The source two-hop query at S18 had the largest measured query peak and was
selected for profiling across all three sizes. Profiled outputs match the
independently verified results. Heaptrack event counts are 47,167 / 67,233 /
108,153; final live bytes are 157,159 / 149,030 / 157,323. They do not show a
retained leak proportional to graph size.

At the S18 *simultaneous* heap peak, the allocation categories are:

| Category | Live bytes at that instant |
| --- | --- |
| Retained accumulator/flush state | 50,336,904 |
| CSR writer state | 17,826,667 |
| Merge-reader buffers | 4,194,304 |
| CSR serialization/authentication path | 34,115,073 |
| Other | 370,230 |
| **Total** | **106,843,178** |

This establishes actual run-count-dependent reader allocation in the public
query rebuild path: four direction/relation merge calls allocate 4 / 8 / 16
one-MiB reader buffers in total, with 1 / 2 / 4 simultaneously live. The
simultaneous peak includes retained accumulator capacity, rather than replacing
that capacity with the merge working set. The S20/S22 16/64-MiB values remain
code-grounded extrapolations; these are not newly measured S20/S22 traces.

The observed mechanism supplies a concrete repair target. Use a fixed aggregate
reader budget per merge, distributed across its live cursors, so opening 64
streams cannot add 48 MiB over 16 streams. An initial 1-MiB aggregate budget
would give 64/16-KiB buffers at 16/64 readers. Keep record decoding independent
of buffer boundaries and test 63/64/65-run boundaries, including partial reads,
cancellation and spill cleanup. Measure I/O/runtime impact before selecting the
final production budget; this diagnosis does not claim an optimal buffer size.

The smaller comparison exposes the mechanism without requiring a new S22 run.
It does **not** reproduce a 31.59% RSS increase: that refusal is established by
the preserved, fully validated S20/S22 prefix. The remaining historical 12 MiB
is not assigned to a specific allocation site. Allocator retention, residency
and shard serialization differences remain plausible contributors, not proven
causes. Do not subtract the predicted 48 MiB from a receipt and call it a pass.

## Reproduction and checks

From the repository root, with an empty `DIAG_OUTPUT`, frozen executables and
`DIAG_TARGET` on the admitted native filesystem:

```bash
.venv/bin/python benchmarks/diagnostics/rss_1278.py \
  --gf "$FROZEN_GF" --generator "$FROZEN_GENERATOR" --output "$DIAG_OUTPUT"
heaptrack -o "$TRACE" "$FROZEN_GF" --json --project "$SOURCE_PROJECT" \
  query --cypher 'MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000' \
  --output "$PROFILE_RESULT"
python3 benchmarks/diagnostics/heaptrack_summary_1278.py "$TRACE.zst"
heaptrack_print -f "$TRACE.zst" -m 0 -a 0 -T 0 -p 0
```

The diagnostic pins the first 16 allowed CPUs, retains the original reserve,
and stops on command/correctness/resource failure. It waits for observed host
builds/campaigns before launching each command. This is a diagnostic runner,
not a substitute for native BenchExec admission or an independently certified
whole-rung resource envelope. Sampling can miss a short-lived peak or command;
zero samples for the fast `--info` control do not mean zero process memory.
The initial and final instrumented allocation traces are kept separate from
unprofiled process observations; later sibling work overlapped instrumentation.
No raw trace, graph contents or private command paths are checked in.

Validation completed for this diagnostic-only change:

- `cargo build --locked --release -p graphforge-cli` with the isolated debug-symbol
  environment above: passed (16m43s including pauses for sibling host work).
- From `benchmarks`, `PYTHONPATH=harness .venv/bin/python -m unittest
  tests.test_progressive_host_run tests.test_progressive_qualification -q`:
  23 tests passed after linking the worktree's existing benchmark environment.
  Earlier attempts with the root interpreter or missing worktree environment
  failed dependency/path setup and are not represented as test passes.
- `make pre-push-fast`: passed.
- `make gate-registry-check`: passed, including 14 registry tests.
- `ruff check benchmarks/diagnostics` and `git diff --check`: passed.
- Independent oracle control with parallel edges and one self-loop: passed.
- Real S16/S17/S18 lifecycle runner: 135 successful commands, including all 72
  repeated query/oracle comparisons. Three profiled results also match.

No Rust behavior, public API, durable format or admission policy changed. Full
workspace coverage and exact-head PR CI are not claimed by this local diagnosis.
