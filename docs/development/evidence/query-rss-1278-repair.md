# Adjacency merge-reader repair for #1278

The allocation correction is verified. Native S20/S22 process-RSS comparison and full
S24 admission remain pending. No S24 workload has been executed and #1278 remains
open. The [pre-repair diagnosis](query-rss-1278.md) and its receipts are preserved.

## Change and identities

Measured candidate source is `6cbfde81494f864a3a61e4d887138aaaa256c92e`, on top of
`5224ebb7` (the independently merged #1279 shaping repair). The only additional
Rust behavior change is in `graphforge-storage/src/adjacency/builder.rs`:
`open_run_cursors` distributes a 1,048,576-byte aggregate buffer budget across
all readers in a merge. Both final merges and compaction use this path. Previously
each reader allocated 1 MiB, implying 16/64 MiB at S20/S22.

The budget belongs to the merge rather than growing with each cursor. It gives
64-KiB buffers at 16 readers and 16-KiB buffers at 64 readers. Integer division
never rounds the aggregate allocation above the budget; an unusually large
caller-selected fan-in can use zero-capacity readers, which read directly.
Record decoding, fan-in, chunk sizes, ordering, spill limits, cancellation,
publication, the durable format and public APIs are unchanged.

Frozen executable SHA-256 identities:

| Executable | SHA-256 | Profile |
| --- | --- | --- |
| `gf` | `7be606cef7e3fb982edef8b11280914d51f1fa0b6c73cf5a602b2621d6ec439d` | release, debug=1 |
| `graphforge-benchmark-certify` | `6ca9df51704c5842c971de232e35ddba3afb29b2fb4e22a8b89b0265cd097847` | release, debug=0 |
| `graphforge-benchmark-graph500-generator` | `f0bb6614b2a4119bf3deee381783abc506be91b76abe10da58f5aafd7a73026f` | release, debug=0 |

Rust remains 1.96.0 (`ac68faa20`, LLVM 22.1.2). Generator source SHA-256 remains
`a7cd8397ce191c48094d8812eb43dfc894084645dd439e76fb8704b35bb61fdc`.
The root/benchmark Cargo lockfile hashes are
`d80355162144c38a02b5df9787cc1d81e7a0299dc385bde1f342d0bdb06e4ba1` /
`1c1e58085c53aef2d50affdabcdcbc7fac7b6b77bc4d90b07af0065acbf39bb6`.

## Allocation and correctness evidence

The repaired executable ran the previously selected ordered two-hop query on
the same preserved S16/S17/S18 source projects used by the diagnosis. All three
results equal the independently verified pre-repair outputs. The generator and
fixture contents were not changed for this comparison.

| Fixture | Pre-repair live reader buffers B | Repaired live reader buffers B | Repaired peak live heap B | Live bytes at exit |
| --- | --- | --- | --- | --- |
| S16 | 1,048,576 | 1,048,576 | 103,545,710 | 152,791 |
| S17 | 2,097,152 | 1,048,576 | 103,635,340 | 147,091 |
| S18 | 4,194,304 | 1,048,576 | 103,740,748 | 155,639 |

At the repaired global heap peak, `EntryGroup` accumulator/flush allocations
are exactly 101,713,416 B at every size. The remaining 1,832,294 / 1,921,924 /
2,027,332 B are other allocations. The peak has moved away from the simultaneous
merge-reader/CSR-serialization overlap seen before repair at S17/S18. These
instantaneous totals are distinct from independently occurring category peaks.

Heaptrack 1.5 traces and the checked-in event summarizer attribute the reader
allocations to the real facade's query-time adjacency rebuild. The traces were
collected while the separate validation build was active. They establish live
allocation ownership and query correctness, not uncontended runtime or process
RSS. They are not admission evidence.

The deterministic Rust regression opens actual production cursors at 1/16/63/64/
65 runs, asserts their combined allocated capacity, and checks every emitted
record against an independently sorted input multiset. It exercises refill
boundaries and 65-run compaction. A second regression checks zero-/one-/23-/24-/
25-byte readers and truncated-record rejection. All 13 adjacency-builder tests
passed, including existing cancellation and spill-limit checks.

## Uninstrumented small comparison

The unchanged diagnostic runner completed all 135 preselected commands and all
72 repeated query/oracle comparisons. All construction, reopen, counts, ordered
one-/two-hop results, export, full verification and clean import checks passed.
No build or other native campaign overlapped this series. All observations and
raw-file hashes are retained in the [sanitized ledger](query-rss-1278-repair.json).

| Fixture | Before: max repeated-query VmHWM B | Before: max across all commands B | After: max repeated-query and all-command VmHWM B |
| --- | --- | --- | --- |
| S16 | 176,492,544 | 176,492,544 | 172,969,984 |
| S17 | 176,668,672 | 176,668,672 | 173,449,216 |
| S18 | 179,408,896 | 180,178,944 | 173,228,032 |

The pre-repair S18 all-command maximum occurred in the imported-project lifecycle
proof, outside the three repeated-query rounds. It is retained rather than
excluded in favor of the lower repeated-query maximum. These are diagnostic
process observations; the native host ladder is the admission authority.

This series reused the accepted generator executable with SHA-256
`4829806c4666d1c21e2afd680fec3c8749588be713e493233e81b77eb2bcf551`
to keep that binary identical to the pre-repair small comparison. The host ladder
uses the rebuilt generator listed above, with the same verified generator source
and unchanged semantics.

## Preserved refusal and validation attempts

After the repair, replay of the complete historical S18/S19/S20/S22 prefix still
refuses full S24 admission only on `rss_bounded_or_plateaued`. Fresh free space
was 699,611,693,056 B and the reserve remained 141,258,578,535 B. No gate changed.

- Targeted optimized storage tests: 13 passed (1.42 seconds after compilation).
- Complete optimized storage unit-test executable, with supported `/tmp` and
  parent-only `--test-threads=1`: 1,111 passed, zero failed, two existing ignored
  tests, in 66.58 seconds.
- Host/admission unit tests: 23 passed.
- Workspace Clippy, fast checks and gate-registry checks: passed.
- Initial full `make pre-push`: failed in storage with 1,109 passed, two failed
  and two ignored. One existing test hardcodes `/tmp`, which is tmpfs on this
  host and correctly fails native filesystem admission. The other expected
  zero process-global topology reads but observed one during parallel tests;
  it passed when executed in isolation. These files are unchanged by this PR.
- The hardcoded-`/tmp` test passed with ext4 bound at `/tmp` inside a private
  mount namespace. The host's own `/tmp` remains tmpfs. Full revalidation uses
  this supported filesystem and a Cargo runner that adds `--test-threads=1`
  only to the storage unit-test executable, consistent with the I/O-counter
  documentation's requirement for nonconcurrent measurement. Child processes
  keep their normal harness mode. Every assertion remains enabled.
- The first serialization attempt used an inherited `RUST_TEST_THREADS=1`. It
  was stopped after a restart test waited indefinitely for a child line exactly
  equal to `ready`; the serial harness prefixes that line with the test name.
  A separate output probe reproduced that prefix. Parent-only serialization
  avoids changing the child protocol. The failed/aborted attempt logs remain
  separate from subsequent validation results.

## Native host ladder (in progress)

The unchanged controller has accepted S18 and S19 with native process peaks of
181,678,080 and 183,021,568 B. Its full S20 projection admitted with every check
passing. S20/S22 results and full S24 admission remain pending. The cap is S22;
there is no S24 workload authorization or execution.

## Commands

The first host invocation refused before any rung because the new work-root
directory did not yet exist. That preflight result is retained. After creating
the dedicated directory, the unchanged controller started S18.

Paths below use operator-selected variables to exclude private host paths.
Native targets and frozen executables are isolated from other worktrees.

```bash
# Storage regression and symbolized CLI, from the repository root.
env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u LLVM_PROFILE_FILE \
  CARGO_TARGET_DIR="$DIAG_TARGET" CARGO_BUILD_JOBS=4 \
  CARGO_PROFILE_RELEASE_DEBUG=1 TMPDIR="$NATIVE_TMP" \
  cargo test --locked --release -p graphforge-storage adjacency::builder::tests \
  -- --test-threads=4

env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u LLVM_PROFILE_FILE \
  CARGO_TARGET_DIR="$DIAG_TARGET" CARGO_BUILD_JOBS=4 \
  CARGO_PROFILE_RELEASE_DEBUG=1 TMPDIR="$NATIVE_TMP" \
  cargo build --locked --release -p graphforge-cli

# Benchmark target was copied from a warm cache into an isolated directory.
CARGO_TARGET_DIR="$BENCH_TARGET" cargo clean --manifest-path benchmarks/Cargo.toml \
  -p graphforge-storage -p graphforge-filesystem
env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u LLVM_PROFILE_FILE \
  CARGO_TARGET_DIR="$BENCH_TARGET" CARGO_BUILD_JOBS=4 TMPDIR="$NATIVE_TMP" \
  cargo build --locked --release --manifest-path benchmarks/Cargo.toml \
  --bin graphforge-benchmark-certify --bin graphforge-benchmark-graph500-generator

# Repeated for each preserved S16/S17/S18 source project.
TMPDIR="$NATIVE_TMP" taskset -c 0-15 heaptrack -o "$TRACE" "$BIN/gf" \
  --json --project "$PROJECT" query --cypher \
  'MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000' \
  --output "$QUERY_OUTPUT"
python3 benchmarks/diagnostics/heaptrack_summary_1278.py "$TRACE.zst"
heaptrack_print -m 0 -f "$TRACE.zst"
```

```bash
# Complete small comparison, using the accepted frozen generator.
.venv/bin/python benchmarks/diagnostics/rss_1278.py --gf "$BIN/gf" \
  --generator "$ACCEPTED_BIN/graphforge-benchmark-graph500-generator" \
  --output "$RAW/repair-small"

# Native ladder, from benchmarks; WORK_ROOT must already exist.
TMPDIR="$NATIVE_TMP" PYTHONPATH=harness .venv/bin/python \
  -m graphforge_bench.progressive_host_run --maximum-scale 22 \
  --output-dir "$EVIDENCE" --work-root "$WORK_ROOT" --gf "$BIN/gf" \
  --certify "$BIN/graphforge-benchmark-certify" \
  --generator "$BIN/graphforge-benchmark-graph500-generator" \
  --benchexec-python /usr/bin/python3 --reserved-headroom-bytes 141258578535
```
