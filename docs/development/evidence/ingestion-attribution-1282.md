# Remaining ingestion attribution (#1282)

This study measures the merged ingestion path after #1279. It introduces only
opt-in diagnostic measurements and deterministic fixtures. Recommendations are
for later bounded experiments; no sorting, encoding, authentication, publication,
durable format, public API or query behavior is changed.

## Accepted prefix and scope

The separate [prefix analysis](ingestion-attribution-1282-prefix.json) reads the
completed #1278 S18/S19/S20/S22 receipts through `read_native_rung`. It requires
matching source, executable, generator, host and tool identities across the
prefix. Missing, unfinished, mismatched or altered evidence fails validation.
The historical #1279 analysis and receipts are unchanged.

| Accepted integrated scale | Ingestion (s) | Whole lifecycle (s) | Ingestion share | Construction calls (s) | Outside those calls (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| S18 | 49.027 | 84.462 | 58.05% | 43.616 | 5.411 |
| S19 | 96.659 | 167.728 | 57.63% | 85.472 | 11.187 |
| S20 | 197.749 | 339.062 | 58.32% | 174.695 | 23.054 |
| S22 | 931.762 | 1510.619 | 61.68% | 829.221 | 102.541 |

The prefix executable is frozen source
`6cbfde81494f864a3a61e4d887138aaaa256c92e`, including both shaping and RSS
repairs. Its report was subsequently integrated by PR #1281; the later guard
integration is not a new native measurement. Historical S20/S22 ingestion
shares reproduce as **60.50% / 65.62%**. Neither comparison isolates a #1279
speedup. A single accepted observation at each scale cannot estimate variance.

The new study uses a branch from `a07bf2d6`, after #1281 completed and the
designated host was idle. New observations are **diagnostic study evidence**, not
new accepted ladder receipts, qualification or higher-scale admission.

## Fixed protocol and identities

The scaling cases use unchanged Graph500 S16/S17/S18 as 1×/2×/4×, edge factor
16, seed `13907095936298285200`, unchanged schemas and 65,536-row default batches.
All merges retain production fan-in 32. Each ordinary case has three repetitions,
preselected in repetition-major order before execution. Each gets fresh source
and clean-imported projects and runs the canonical CLI lifecycle through the
real Rust facade. Independent input-derived oracles check both exact counts and
ordered one-/two-hop results, in both projects, after reopen, export, full
verification and clean import.

The separate facade boundary fixture appends one row per durable chunk. It
selects node/edge chunk pairs `(31,31)`, `(32,32)`, `(33,33)`, `(1023,1023)`,
`(1024,1024)`, `(1025,1025)`, `(16,256)` and `(64,1024)`, three repetitions each.
The last two reproduce total-chunk shapes 272 and 1,088. They are small
structural fixtures, not Graph500-scale performance substitutes. The ring
topology has an independent exhaustive ordered-query oracle. The fixture uses
the existing API test-support build with diagnostics disabled for ordinary
measurements; its timed ingestion excludes its subsequent query assertions.
CPU and block-I/O measurements enclose the whole fixture command, so include
those assertions and must not be presented as ingestion-only CPU.

No system-wide cache changes occur. Workloads run sequentially on OVHC-AGENCY,
on its admitted ext4 filesystem, pinned to the same 16 CPUs. The existing
96,000,000,000-byte controller ceiling, 4-GiB process-RSS limit,
141,258,578,535-byte free-disk reserve and 14,400-second lifecycle timeout remain.
First failure stops a suite. No S24+ workload runs. Host activity, execution
order, commands, failures, raw receipts and profiles are retained privately.
Published observations contain their hashes, not graph rows, paths or UUID
inventories.

Frozen ordinary Rust source is `5fd6e59fe912719ccf147f90625e994a14a914d6`,
tree `948e1d2113061b4d55111f8eb1a184e44998d04a`, with Rust 1.96.0, locked
dependencies, release optimization and debug level 1. Later collector/report
changes have separate source and script identities. The diagnostic CLI must be
built with both storage and CLI selected: selecting the CLI alone activates the
named storage feature only on its dev-dependency and produces an ordinary
binary. That initial build was detected by equal hashes and absent diagnostic
symbols before profiling; it supplies no instrumented observations.
[Build identities and retained build-log hashes](ingestion-attribution-1282-builds.json)
include the corrected diagnostic executable. Its native source inputs equal
the ordinary build; only the test-support feature differs.

## What each measure means

| Measure | Scope and interpretation |
| --- | --- |
| Accepted lifecycle wall | Existing native BenchExec receipt, including its controller scope. |
| New command wall | Monotonic elapsed time around each subprocess; includes startup and at most polling-resolution collection delay. Sum excludes independent collector-side oracle work. |
| Public construction calls | Existing begin/append/seal/resume/publish durations are disjoint within ingestion. Residual includes source decoding, normalization, checkpoints, startup and other work outside these calls. |
| Diagnostic scopes | Per-process monotonic intervals for shaping, canonical encoding and authentication. Inclusive parents contain children. Residuals subtract the union of contained intervals once. |
| Merge family work | Deltas of existing logical row, byte and synchronization counters around actual merge calls; no estimate from total chunks. Fixed bytes are actual compact wire bytes. Parquet bytes come from the existing counting reader/writer. |
| CPU | `wait4` user+system CPU, or separately sampled CPU stacks. Neither is elapsed wall time. Inclusive stack ownership percentages can overlap. |
| Logical I/O | Final committed construction application-I/O counters, read once. Cumulative intermediate receipts are not summed. |
| Block-accounted I/O | `wait4` block counts × 512, including kernel accounting for the command and descendants. These are not logical bytes or a whole-device bandwidth measurement. |
| I/O wait | Unavailable: task delay accounting is disabled on this host. It is not inferred from wall minus CPU. |
| Sync latency | Separate timestamped `fsync`/`fdatasync` tracing. Summed syscall latency is thread time, with interval union available for elapsed coverage. It overlaps enclosing phases. |
| Resources | Process `VmHWM` sampled after executable replacement, plus separately reported construction staging **disk** allocation peaks. A short command with no sample is unobserved, not proof of zero memory use. |

### Ownership map

All paths below are repository-relative. The CLI entry is
`crates/graphforge-cli/src/portable_cli.rs::run_import_session`.

| Work | Command and ownership path |
| --- | --- |
| Source preparation | `import-session register-parquet`; `graphforge-api/src/import_session.rs` copies and authenticates source files. `import-session validate` calls `for_each_source_batch`, normalizes Arrow chunks and persists per-batch checkpoints. |
| Append | `import-session validate` → `GraphImportSession::validate_with_cancellation` → facade append → `graphforge-storage/src/graph_construction.rs::append_with_cancellation`. |
| Shaping | Same command → `validate_and_seal` → `shape_canonical_inner` → `graph_construction/shaping_merge.rs::{merge_fixed_group,merge_row_group}`. |
| Canonical encoding | Same seal → `graph_construction_encoding.rs::encode`, including node/edge/property writers and authenticated source spools. |
| Authentication | `authenticate_artifact`, `authenticate_inventory` and `authenticate_inventory_payloads`; these run in seal, reopen/recovery and publication contexts. Do not assign every hash sample to one phase. |
| Recovery | Each fresh CLI facade open and `import-session commit` resume use construction/publication recovery and reauthentication. Public `resume` excludes earlier facade-open work. |
| Publication | `import-session commit` → `GraphImportSession::commit` → `seal_and_publish` → `publish_canonical_with_cancellation`, including preauthentication, CAS installation and durable visibility. |
| Synchronization | `StableDirectory`/storage-I/O capabilities and `sync_all_and_release` inside append, merges, encoding, control checkpoints and publication. It is nested work, not an additional disjoint phase. |

## Ordinary scaling baseline

All nine selected lifecycles pass. The [machine-readable baseline](ingestion-attribution-1282-scaling.json)
retains every command observation and raw-artifact hash.

| Scale | Ingestion min / median / max (s) | Median ingestion command CPU (s) | Maximum sampled process bytes | Maximum construction staging allocation (bytes) |
| --- | ---: | ---: | ---: | ---: |
| S16, 1× | 9.134 / 9.172 / 9.256 | 8.154 | 178,970,624 | 419,770,368 |
| S17, 2× | 21.149 / 21.429 / 21.555 | 19.161 | 178,237,440 | 940,236,800 |
| S18, 4× | 45.670 / 45.820 / 46.292 | 41.272 | 179,363,840 | 1,880,457,216 |

| Scale | Median append (s) | Median seal (s) | Median resume (s) | Median publish (s) | Median outside public construction calls (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| S16 | 1.045 | 6.194 | 0.047 | 0.517 | 1.321 |
| S17 | 2.022 | 15.776 | 0.097 | 0.978 | 2.607 |
| S18 | 3.962 | 34.463 | 0.188 | 1.935 | 5.335 |

Begin is approximately 0.001 seconds at each scale. These are independently
computed medians, so their sum need not equal the median total. Seal is the
largest public call; its nested shaping, encoding and authentication require
the separate diagnostic observations below.

Before any later optimization experiment, freeze the following baseline-derived
screen: ingestion wall improvement must **exceed both the observed baseline
range and 0.010-second collection resolution**. This gives floors of
0.122032 / 0.406389 / 0.621842 seconds for S16/S17/S18 (rounded upward here;
the JSON retains exact values). Also retain all hard limits and compare against
the resource maxima above and the boundary envelopes. This is a minimum useful
benefit, not a confidence interval, guarantee or timing assertion in CI. A later
experiment needs its own preselected comparable repetitions and accounting for
any extra memory, disk or durability work. No candidate optimization was run.

## Diagnostic scaling attribution

All three [diagnostic scaling lifecycles](ingestion-attribution-1282-diagnostic.json)
pass. Every final construction logical-I/O counter equals all three ordinary
repetitions of the same case. Signed ingestion-wall differences from the ordinary
medians are −0.89%, −1.67% and +0.75%. Each absolute difference is smaller than
that case's observed baseline range. One later diagnostic run per scale cannot
isolate instrumentation overhead from run variation, cache state or code layout.
It does not demonstrate a speedup or zero overhead.

| Scope, inclusive seconds | S16 | S17 | S18 |
| --- | ---: | ---: | ---: |
| Shaping | 4.538 | 12.153 | 27.863 |
| Shaping outside observed merge/authentication children | 2.008 | 3.886 | 7.891 |
| Canonical encoding | 1.454 | 2.746 | 5.551 |
| Artifact authentication within shaping | 0.051 | 0.107 | 0.226 |
| Inventory payload authentication, two calls | 0.079 | 0.169 | 0.342 |

These rows overlap. In particular, one inventory payload authentication is inside
canonical encoding's inventory authentication. The report computes each parent's
residual by subtracting the union of contained intervals. Hashing while reading
or writing streams also remains inside merge/encoding time; the named
authentication scopes are not an estimate of all hashing CPU.

S18 shaping includes **10.789 s fixed-record merges**, **8.956 s Parquet row
merges**, **0.226 s artifact authentication**, and **7.891 s outside these observed
children**. Other shaping work includes initial conversions, ordinal/endpoint
resolution, inventories and control operations. Canonical encoding adds 5.551 s,
of which 5.379 s is outside observed authentication children. The public seal
is 34.520 s; about 0.93 s remains outside these top-level measured scopes.

### Actual S18 merge work

| Family | Input runs | Merge groups | Maximum scheduler level | Rows read = written | Fixed bytes read = written | Parquet bytes read / written | Inclusive seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Identity | 68 | 4 | 2 | 8,912,896 | 231,735,296 | 0 / 0 | 1.236 |
| Node detail | 4 | 1 | 1 | 262,144 | 5,505,024 | 0 / 0 | 0.128 |
| Edge detail | 64 | 3 | 2 | 8,388,608 | 444,596,224 | 0 / 0 | 4.601 |
| Endpoint | 64 | 3 | 2 | 16,777,216 | 553,648,128 | 0 / 0 | 2.411 |
| Resolved endpoint | 128 | 5 | 2 | 16,777,216 | 419,430,400 | 0 / 0 | 2.413 |
| Node schema rows | 4 | 1 | 1 | 262,144 | 0 | 4,197,480 / 4,196,544 | 0.137 |
| Edge schema rows | 64 | 3 | 2 | 8,388,608 | 0 | 402,764,714 / 402,771,494 | 8.819 |

The fixed-byte oracle uses actual compact record widths: identity 26, node detail
21, edge detail 53, endpoint 33 and resolved endpoint 25 bytes in these fixtures.
Parquet counters measure returned reader bytes, including footer/range rereads,
and the hashing writer's actual output bytes. File verification independently
checks the emitted artifacts. Group deltas exclude initial conversions and
other shaping readers, so they are a subset of construction I/O. Maximum
scheduler level is not a claim that every row takes that many passes.

The final S18 construction ledger records 9,511,424,160 logical read bytes and
4,330,719,595 logical write bytes. Shaping owns 4,024,521,536 reads and
3,007,963,183 writes; its measured merge groups account for 2,061,877,266 reads
and 2,061,883,110 writes. The rest is other shaping work. Encoding owns
1,442,843,347 reads and 368,025,637 writes; recovery reauthentication owns
3,708,296,467 reads. These ownership counters do not assign all recovery work
to the public `resume` call. Ordinary S18 ingestion separately observes about
13.56 GB block-accounted reads and 4.55 GB writes. Logical and block I/O must
remain separate.

At S17 the edge-row family has exactly 32 inputs. It merges them once, then
decodes and rewrites the single completed root again: **2,097,152 extra rows
read and written**, **100,674,389 read / 100,692,937 written Parquet bytes**, and
**2.098 s** in the second call. This is measured work available for a bounded
root-adoption investigation. It is an upper bound on removable work in that
call, not a measured optimization benefit. S18 has 64 edge-row runs and needs
to combine two roots; it has no final single-input row merge.

## Facade boundary results

All **24 ordinary** and **8 diagnostic** boundary lifecycles pass. The
[ordinary observations](ingestion-attribution-1282-boundary.json) retain all
three repetitions, resource maxima and benefit floors for each case; the
[diagnostic observations](ingestion-attribution-1282-boundary-diagnostic.json)
retain every family, group-level distribution, input-count distribution and
row/byte total. All diagnostic aggregate merge/Parquet work counters equal
each ordinary repetition of the same case.

For equal node/edge chunk counts, node detail and edge detail have the same
record-work count; each Parquet schema family has the row-work count below.
Identity and resolved-endpoint families have **twice** the listed input-run
count, while endpoint runs contain two records per edge. The full family
census is in the JSON.

| Node runs = edge runs | Detail records read = written per family | Parquet rows read = written per family | Detail / Parquet maximum scheduler level | Ordinary ingestion min / median / max (s) |
| --- | ---: | ---: | ---: | ---: |
| 31 | 31 | 31 | 1 / 1 | 1.292 / 1.317 / 1.318 |
| 32 | 32 | 64 | 1 / 2 | 1.314 / 1.327 / 1.345 |
| 33 | 65 | 65 | 2 / 2 | 1.390 / 1.398 / 1.402 |
| 1,023 | 2,046 | 2,046 | 2 / 2 | 59.866 / 60.002 / 61.752 |
| 1,024 | 2,048 | 3,072 | 2 / 3 | 59.767 / 60.632 / 60.702 |
| 1,025 | 3,073 | 3,073 | 3 / 3 | 59.380 / 60.545 / 61.449 |

The fixed-record work increases after a fan-in power. The Parquet final-root
rewrite instead adds work **at** the exact power. The 1,023–1,025 timing ranges
overlap, so there is no timing-cliff claim. Median append times are about
15.7 s and seal/publication about 44.4–44.9 s: these tiny-row fixtures also pay
the control and durability costs of thousands of chunks.

### Historical total-chunk shapes, measured per family

These are the small facade cases `(16,256)` and `(64,1024)`, not new S20/S22
Graph500 measurements. Each run contains one row; the two totals are 272 and
1,088 chunks.

| Family | Input runs at 272 / 1,088 total chunks | Records or rows read = written at 272 / 1,088 |
| --- | ---: | ---: |
| Identity | 272 / 1,088 | 544 / 3,264 |
| Node detail | 16 / 64 | 16 / 128 |
| Edge detail | 256 / 1,024 | 512 / 2,048 |
| Endpoint | 256 / 1,024 | 1,024 / 4,096 |
| Resolved endpoint | 512 / 2,048 | 1,024 / 6,144 |
| Node schema rows | 16 / 64 | 16 / 128 |
| Edge schema rows | 256 / 1,024 | 512 / 3,072 |

Thus the earlier 544/3,264 fixed-record model matches the identity family;
applying it to every family would misattribute work. Ordinary ingestion medians
for these small cases are 6.633 / 30.190 s. The observed diagnostic differences
from all eight boundary medians range from −0.386 to +1.237 s; their signed
values and ordinary ranges remain in the reports. These are overhead
observations, not controlled causal estimates.

## Synchronization and CPU observations

The separate [syscall run](ingestion-attribution-1282-sync.json) records **8,225
successful fsync calls**, no fdatasync calls and no failed synchronization calls
across the five ingestion commands. Summed thread latency is **2.803 s**;
the within-command interval unions give the same sum because these observed
calls do not overlap. The slowest call is **19.713 ms**. These are traced
latencies, not an uninstrumented fsync contribution: traced ingestion is
**58.344 s**, 27.33% above the ordinary S18 median. The trace encloses CLI
checkpoint/reopen/control work as well as construction, so its syscall count
is not interchangeable with the narrower logical construction counter.

The first [CPU sample inventory](ingestion-attribution-1282-cpu.json) uses
199-Hz cpu-clock sampling with 8,192-byte DWARF stacks. The dominant ingestion
command has 7,866 leaf samples: SHA-256 compression accounts for **11.48%**,
BTreeMap insertion **6.47%**, `run_record_bytes` **4.75%**, and compact detail
reading **4.72%**. Unknown libc leaves account for **21.85%**. These are disjoint
sample counts; percentages are not seconds and do not assign hashing to a
particular caller. Its parent stacks are incomplete (4,895 samples have no
unwound frames; none has a symbolized application caller). This limits inclusive
ownership claims. The capture and its decoding artifacts remain retained.

## Ranked recommendations for later experiments

1. **Investigate adopting an already-complete Parquet root.** The strongest
   concrete opportunity is the S17 edge-row unary merge: 2.098 s and 201.37 MB
   of extra read/write work, including 2,097,152 rows in each direction. Ownership
   is `graph_construction/shaping_merge.rs` (`RowMergeAccumulator::finish` and
   `merge_row_group`), with shape receipt and successor ownership in
   `graph_construction.rs`. Expected benefit is removal of this decode/encode
   pass, bounded above by its measured call time; there is no measured repair
   speedup. Root adoption should require no additional row buffers and should
   reduce temporary writes, but authenticated identity, durable naming and
   predecessor reclamation must remain valid. The deterministic oracle is one
   fewer unary group and its exact row/byte work at 32/1,024 inputs, unchanged
   canonical results, and no extra omission at 31/33 or 1,023/1,025. Run the
   consumed-shape-root cancellation, returned-failure, successor-corruption and
   crash/replay regressions, plus every facade lifecycle oracle.
2. **Investigate repeated compact-record validation and copying within merges.**
   S18 fixed merges consume 10.789 s; the leaf samples identify both
   `graph_construction::run_record_bytes` and `construction_detail_codec::DetailCodec::read`.
   The latter reconstructs a padded record and validates compact bytes; writing
   validates the padded record again. This supports a bounded experiment to
   avoid redundant in-memory conversion while keeping every trust-boundary
   check. It does not establish that all those samples are redundant or that
   their sum is removable wall time. Preserve compact wire bytes and the
   existing fan-in/memory bound; any cached metadata must have an explicit
   per-run bound and show measured RSS/disk maxima. The oracle is byte-identical
   compact output, exact family row/byte counters and rejection of malformed
   lengths, UTF-8, padding and truncation. Run compact-detail corruption,
   cancellation, copy/retry, multi-level resume and durable crash-replay tests,
   followed by the full facade lifecycle.
3. **Retain authentication, synchronization and concurrency policy.** Hashing is
   visible CPU work and reauthentication reads are substantial, but this study
   does not prove any authentication boundary redundant. Traced fsync latency
   also does not justify removing or combining durability barriers. There is
   no controlled evidence that more threads improve this workload. Recommend
   no change to these mechanisms. Any future proposal must first identify
   precisely eliminated work and preserve same-inode corruption, replaced or
   linked survivor, corrupt successor, cancellation and every publication
   crash-boundary refusal/recovery oracle. No new buffers, threads or resource
   budget are authorized by this conclusion.

Before a later experiment, freeze its baseline and primary case. On this
baseline a useful median ingestion reduction must exceed both measurement
resolution (10 ms) and the observed three-run range: **0.122 s / 0.406 s /
0.622 s** for S16/S17/S18, respectively. Boundary-specific floors and observed
RSS, disk and I/O maxima are in the ordinary JSON reports. These empirical
floors are not confidence intervals or timing-test assertions. Retain all
existing hard resource limits and report any regression against baseline
maxima; a favorable time result cannot waive correctness or durability.

## Validation contract

`benchmarks/diagnostics/ingestion_1282.py` freezes selection and source manifests;
`report_ingestion_1282.py` checks their hashes, exact command order, completion,
raw artifact hashes and mandatory diagnostic families. It refuses missing,
duplicate, reordered, unsuccessful or tampered observations. The small fixed
family oracle independently predicts exact rows and compact record widths,
including the final Parquet root rewrite at exact powers of 32. Every measured
case retains eight independent lifecycle result checks.

The new Rust diagnostic module is compiled only with `test-support` (or unit
tests) and requires `GRAPHFORGE_INGEST_DIAGNOSTICS=1282`. Default Cargo CLI builds do
not contain it. Merge wrappers invoke the existing call once and return the
same result. Diagnostic stderr failures cannot change durable operation
results; missing diagnostics instead invalidate the collector's report.

Reproduction, after prerequisite host authorization and idle-host checks:

```bash
PYTHONPATH=benchmarks/harness .venv/bin/python -m graphforge_bench.ingestion_attribution --evidence docs/development/evidence/query-rss-1278-repair
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_ingestion_1282.py "$PRIVATE_SUITE"
# From benchmarks/:
PYTHONPATH=harness ../.venv/bin/python -m unittest tests.test_ingestion_attribution tests.test_lifecycle_runtime tests.test_progressive_qualification -q
# Frozen builds, sequentially, with isolated CARGO_TARGET_DIR and debug=1:
cargo build --locked --release -p graphforge-cli
cargo build --locked --release -p graphforge-cli -p graphforge-storage --features graphforge-storage/test-support
cargo test --locked --release -p graphforge-api --test ingestion_attribution -- --nocapture
```

The private suite commands specify `--suite scaling`, `boundary`, `diagnostic`,
`perf` or `sync`; diagnostic boundary collection additionally uses `--instrument`.
Each invocation requires a fresh output directory and the existing 96-GB cgroup.
Executables and generator arguments resolve to the recorded hashes. Raw
commands retain their full invocation; published paths use placeholders.
