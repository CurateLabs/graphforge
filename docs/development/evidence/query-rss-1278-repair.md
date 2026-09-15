# Adjacency merge-reader repair for #1278

The allocation correction and complete native S18/S19/S20/S22 prefix are verified.
Full S24 admission passes all nine unchanged checks: S20–S22 process RSS grows
5.8766%, below the 10% gate. No S24 workload has been executed and #1278 remains
open. The [pre-repair diagnosis](query-rss-1278.md) and its sole RSS refusal are
preserved. PR #1281 remains blocked by an unrelated retention-test CI failure.

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
- The resumed full core Rust coverage run also passed, including all 1,111 storage
  tests (76.43 seconds), API/CLI integration tests and 118 API BDD scenarios.
  Its core coverage report was produced. Instrumented native acceptance passed
  241 Python tests, 277 Node tests, seven additional Node test cases, 126 Node BDD
  scenarios and the native smoke check. The final coverage ledger refused with
  `coverage source tree has uncommitted changes`: the new evidence documentation
  was being authored during validation. No Rust source changed during the run.
  This attempt is retained as passed execution tests and a failed ledger assembly,
  not a green complete `make pre-push` or a verified coverage-floor claim.
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

## Native host ladder and full admission

The unchanged controller completed S18 → S19 → S20 → S22 with exit zero, then
`completed_prefix` independently validated all four rungs and their shared frozen
identities. Every rung passed construction, reopen, canonical counts, queries,
export, full verification, clean import and imported-project proof. S22 source
and imported counts are both 4,194,304 nodes and 67,108,864 edges. No build,
coverage test or allocation profiler overlapped the native campaign.

| Scale | Historical process peak B | Repaired process peak B | Historical wall seconds | Repaired wall seconds |
| --- | --- | --- | --- | --- |
| S18 | 183,107,584 | 181,678,080 | 87.884 | 84.462 |
| S19 | 189,415,424 | 183,021,568 | 175.969 | 167.728 |
| S20 | 199,135,232 | 182,755,328 | 359.113 | 339.062 |
| S22 | 262,049,792 | 193,495,040 | 1,682.601 | 1,510.619 |

The S20–S22 process-RSS increase is 10,739,712 B (5.8766%), compared with the
historical 62,914,560 B (31.5939%). Repaired query-phase peaks are 182,472,704 →
193,495,040 B; imported-project proof peaks are 182,755,328 → 187,973,632 B.
The measurements establish a gate pass, not a constant-RSS claim. The remaining
growth is not fully allocation-attributed, and RSS includes allocator retention
and mapped residency beyond live Rust heap. BenchExec's S20/S22 cgroup peaks are
3,108,929,536 / 12,419,530,752 B, including charged durable-write page cache.
The unchanged host controller uses a 96-GB BenchExec kill ceiling while retaining
the 4-GiB product process-VmHWM envelope; these are different measurements and
limits. Both remain separately recorded in the receipts.

The integrated candidate also contains the separately merged #1279 repair.
Therefore elapsed-time and total-RSS differences from the historical binary
are integrated observations; the paired allocation profiles establish the
specific merge-reader ownership change. The native series contains one prescribed
observation per rung, with no discarded or repeated rung measurements.

The S20 query phase took 38.371 → 38.520 seconds, and imported proof took
53.328 → 53.380 seconds. At S22 they took 161.463 → 162.901 and
219.491 → 218.075 seconds, respectively. Whole-rung physical read/write bytes
at S22 were 885,782,102,016 / 371,891,003,392 before and
886,423,552,000 / 372,299,218,944 after. These observations retain the buffer
tradeoff's measured I/O/runtime evidence; they do not establish an optimal budget
or a statistically isolated performance change.

Fresh free space was 699,090,202,624 B, with the unchanged 141,258,578,535-B reserve.
Full S24 projection admits correctness, RSS headroom and plateau, time headroom,
retained/transient/combined storage headroom, measured I/O capacity and I/O
headroom. It projects 204,234,752 B process RSS and 6,195 policy wall seconds.
This is admission to a future workload, not an executed S24 qualification.

The [complete projection](query-rss-1278-repair/s24-projection-replay.json) has
SHA-256 `61410952e445cf51b0829e0d622c51e7e2cd17640265f9b1085efb7269c9243a`.
All 23 sanitized native receipt/projection files are retained byte-for-byte in
`query-rss-1278-repair/`; the JSON ledger records their SHA-256 hashes, full phase
metrics and identities. Replay of the historical prefix still refuses only RSS.

## CI blocker

The independent retention-lock concern is tracked in
[#1283](https://github.com/CurateLabs/graphforge/issues/1283).

[CI run 34914305740](https://github.com/CurateLabs/graphforge/actions/runs/34914305740)
at `aefdbb2644c6df5a850cccece5a1b1cc43cbad44` failed the Bazel storage aggregate:
1,111 tests passed, one failed and two were ignored. The failing unchanged test,
`project_retention::tests::work_and_byte_limits_are_fail_closed`, received
`GF_WRITER_BUSY` instead of `GF_RESOURCE_LIMIT` at its second preview assertion.
Other completed Rust quality, Python/Node binding and Windows/macOS durability
lanes passed. The independent read-only review found no actionable RSS-repair
findings. Neither successful local tests nor host admission override failed CI.

Inspection shows `run_cleanup` acquires a bare writer-lock `File`; a classification
error returns before its success-path explicit unlock. A duplicated or inherited
descriptor can retain that kernel lock after the parent closes its handle.
Concurrent fork inheritance is a plausible trigger for this CI occurrence, but
the CI log alone does not prove it. A separate deterministic regression should
retain a duplicate descriptor across each bounded-error return and assert that
an independent writer can immediately acquire the lock. Preserve genuine writer
contention behavior. This lifetime concern is outside the adjacency-buffer repair;
no retry or weakened assertion was used to turn this failure green.

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

```python
# Admission replay only, from benchmarks with PYTHONPATH=harness.
from pathlib import Path
from graphforge_bench import progressive_host_run as host

root = Path.cwd()
completed = host.completed_prefix(root, evidence_dir)
assert [rung["scale"] for rung in completed] == [18, 19, 20, 22]
capacity = host.measure_host_capacity(work_root, 141258578535)
projection, digest = host._admit_projection(root, evidence_dir, 24, capacity)
assert projection["decision"] == "admitted"
assert all(projection["checks"].values())
```

The local supported-filesystem test command uses a private mount namespace:

```bash
sudo -n unshare --mount --propagation private "$NAMESPACE_WRAPPER" \
  "$DIAG_TARGET/release/deps/graphforge_storage-31ec0b322dbee9b5" \
  --test-threads=1
```

The wrapper binds the dedicated ext4 temporary directory at `/tmp` only within
that namespace, drops privileges back to the operator, and removes inherited
`RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, `LLVM_PROFILE_FILE` and
`RUST_TEST_THREADS`. Its Cargo runner appends `--test-threads=1` only when the
invoked executable basename starts with `graphforge_storage-`; other binaries
and child self-spawns retain their normal harness mode. The complete local
validation command is the same namespace wrapper followed by `make pre-push`.
