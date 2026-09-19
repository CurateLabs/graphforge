# Compact detail partition loads (#1441)

Detail partitions now retain their compact wire bytes and one `usize` offset per
record. Sorting moves offsets and compares the complete wire record; other
fixed-width families keep their array representation. The existing codec still
validates each record while reading. The worker/coordinator boundary, cancellation,
read counters, cache release and publication protocol are unchanged.

For four-byte names on a 64-bit host, node details require 21 + 8 resident bytes
per record instead of 272; edge details require 53 + 8 instead of 304. Those are
allocation-model factors of 9.38 and 4.98, before process and allocator overhead.
Maximum-length names instead add eight bytes per record. This change does not
claim a universal five-to-sixfold process-memory reduction.

## Wire compatibility

The same existing `same_input_twice_produces_identical_digests` fixture ran in
separate baseline and candidate test binaries built from `19b5c7cf` without and
with this representation change. All five shaped and 28 encoded digests match.
The four fixed-width outputs are:

| Output | SHA-256 on both sides |
| --- | --- |
| identities | `6ea5b8462fc1b5d19b1a084b17b84d59fb76c3a988dd385d4a675d6cad1cb660` |
| node details | `d7eac2fdc3d4e0e35907ac487e49a344d66c147bbb65dee571a7861d02418899` |
| edge details | `f663cd8489a1610f623e1d9c52d4e34ccf00497b35cafc7e43e5147e80062dcc` |
| endpoints | `14a3c60b71fdeed5257dcb2a930ec72bbb5869cc4fc477759d636d304e74a6bc` |

The edge hashes quoted in the original issue predate the per-family splitter
change. `construction_determinism_tests.rs` already documents that historical
change. The comparison above uses the current baseline and candidate at identical
recorded parameters; neither edge output changes in this PR.

## Details-only memory comparison

The fixtures `node_detail_partition_load_preserves_sorted_wire_bytes` and
`edge_detail_partition_load_preserves_sorted_wire_bytes` stream reversed records
to one sealed partition, call the actual production loader, and compare every
sorted wire record with its expected bytes. They retain no full-size input copy.
`GF_DETAIL_PARTITION_ROWS=1048576` selects the fixed measurement scale; ordinary
CI uses 16,384 rows.

BenchExec supervises one fresh test process per observation. The boundary includes
fixture writing, loading/sorting, byte assertions and cleanup. The local
`runexec-rss.py` subclass exposes `ru_maxrss` from the process resource usage
BenchExec already collects through `wait4`, converting Linux KiB to bytes.
That field is a single-process RSS high-water mark. Standard BenchExec wall/CPU
and cgroup memory output is also retained; cgroup memory includes cache and is
not labeled RSS. The fixtures do not spawn child processes.

Three alternating baseline/candidate rounds per family use the same 16 logical
CPUs, 4 GiB cgroup limit and four-byte name. Observations require a quiet-host
guard and preserve failed attempts. All twelve observations passed. Median single-process peak RSS:

| Family | Padded peak RSS (MiB) | Compact peak RSS (MiB) | Reduction factor |
| --- | ---: | ---: | ---: |
| Node details | 296.56 | 54.82 | 5.41× |
| Edge details | 328.41 | 86.45 | 3.80× |

Node process RSS falls by 5.41×; edge process RSS falls by 3.80×. Thus the
historical 5–6× prediction is reached for nodes, but overstates the observed edge
process reduction. The extra offset and process/runtime overhead explain why
process ratios are lower than the pure record-allocation factors. Every paired
fixture output digest and record count matched. No timing improvement is claimed.

One preliminary attempt failed in the wrapper before launching a fixture (an
incorrect subclass installation recursed during BenchExec initialization). The
wrapper now instantiates the subclass through BenchExec's execution API; that
failed attempt is retained and excluded. No measured observation was discarded.

## Validation

- `cargo test --release -p graphforge-storage --lib`: 1,205 passed, six existing
  ignored tests, including `partition_count_changes_the_layout_but_not_the_logical_result`.
- Regression tests compare padded and compact sorting/output with duplicate UUIDs,
  different edge endpoints, variable-length UTF-8 names and maximum-length names.
- `make pre-push-fast`, `make gate-registry-check`, and
  `cargo clippy --release -p graphforge-storage --lib -- -D warnings` passed.

Raw binaries, comparison digests, measurement plan and framework output are kept
at `/home/ubuntu/gf-1441-evidence/` on OVHC-AGENCY. No throughput improvement is
claimed by this memory experiment.
