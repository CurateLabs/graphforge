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
16, the profile's fixed seed, unchanged schemas and 65,536-row default batches.
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
| Resources | Process `VmHWM` sampled after executable replacement, plus separately reported construction staging allocation peaks. A short command with no sample is unobserved, not proof of zero memory use. |

### Ownership map

All paths below are repository-relative. The CLI entry is
`crates/graphforge-cli/src/portable_cli.rs::run_import_session`.

| Work | Command and ownership path |
| --- | --- |
| Source preparation | `import add-file`; `graphforge-api/src/import_session.rs` copies and authenticates source files. `import validate` calls `for_each_source_batch`, normalizes Arrow chunks and persists per-batch checkpoints. |
| Append | `import validate` → `GraphImportSession::validate_with_cancellation` → facade append → `graphforge-storage/src/graph_construction.rs::append_with_cancellation`. |
| Shaping | Same command → `validate_and_seal` → `shape_canonical_inner` → `graph_construction/shaping_merge.rs::{merge_fixed_group,merge_row_group}`. |
| Canonical encoding | Same seal → `graph_construction_encoding.rs::encode`, including node/edge/property writers and authenticated source spools. |
| Authentication | `authenticate_artifact`, `authenticate_inventory` and `authenticate_inventory_payloads`; these run in seal, reopen/recovery and publication contexts. Do not assign every hash sample to one phase. |
| Recovery | Each fresh CLI facade open and `import commit` resume use construction/publication recovery and reauthentication. Public `resume` excludes earlier facade-open work. |
| Publication | `import commit` → `GraphImportSession::commit` → `seal_and_publish` → `publish_canonical_with_cancellation`, including preauthentication, CAS installation and durable visibility. |
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

## Validation contract

`benchmarks/diagnostics/ingestion_1282.py` freezes selection and source manifests;
`report_ingestion_1282.py` checks their hashes, exact command order, completion,
raw artifact hashes and mandatory diagnostic families. It refuses missing,
duplicate, reordered, unsuccessful or tampered observations. The small fixed
family oracle independently predicts exact rows and compact record widths,
including the final Parquet root rewrite at exact powers of 32. Every measured
case retains eight independent lifecycle result checks.

The new Rust diagnostic module is compiled only with `test-support` (or unit
tests) and requires `GRAPHFORGE_INGEST_DIAGNOSTICS=1282`. Default CLI builds do
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
