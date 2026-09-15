# Completed Parquet root retention (#1286)

The repair retains a schema group's completed intermediate Parquet root when it
is the only remaining merge input. Its name, writer receipt, identity, digest
and allocation ownership become the completed shape inventory entry. A raw
single-source chunk still materializes, and multiple inputs still merge.

The #1282 S17 observation identified a 2.098-second unary rewrite of 2,097,152
rows. The fresh baseline/candidate comparison below measures the whole-ingestion
effect separately from that historical diagnostic call.

## Contract and ownership

Given one scheduler-produced intermediate root, finishing performs no new
payload read/write, installation, rename or unlink. Existing shape inventory and
checkpoint synchronization remain; encoding authenticates and consumes the named
root, and existing successor-based reclamation retires it. The implementation
changes no public API, durable format, authentication rule or query contract.

Production fan-in tests prove Parquet row read/write work of 31/32/65 for
31/32/33 input runs, and 2,046/2,048/3,073 for 1,023/1,024/1,025 runs. At exact
powers, payload bytes, receipt bytes, physical identity, digest, allocation map
and I/O counters remain unchanged across finishing. Original singletons,
empty optional families and two independent schema groups are covered.

Cancellation, completed/incomplete root corruption, replacement inodes, extra
links, crashes after installation/inventory/checkpoint, repeated reopen and
retirement use real construction sessions. The real facade fixture independently
checks counts and ordered one-/two-hop results after reopen. Measured lifecycles
also export, fully verify and clean-import, with eight independent source/imported
oracles per case.

## Measurement method

Baseline source is merged `b0df2b85`; candidate native source is `888192cb`.
Both ordinary executables use Rust 1.96.0, locked dependencies and release with
debug information level 1. Graph500 S16/S17/S18 use the unchanged generator,
seed 13907095936298285200, edge factor 16, default batches of 65,536 and fan-in 32.
The same Python interpreter runs both collectors.

The preselection freezes three ordinary repetitions per scale for each build,
in repetition-major S16/S17/S18 order: all baseline observations, threshold
freeze, then all candidate observations. Separate candidate diagnostic scaling
and eight family-boundary cases follow. Historical diagnostic comparisons retain
their own source identities. Fresh projects use natural cache state without
system-wide cache changes; builds and other campaigns do not overlap measurements.

The primary decision is S17 median ingestion saving strictly exceeding 10 ms,
the historical baseline range (0.406388363 seconds), and the fresh baseline range.
S16/S18 are controls. Ranges are empirical variation, not confidence intervals.
Every preselected command, failure, completion/oracle record and raw-artifact
hash is retained. The supported ext4 host keeps the existing 96-GB cgroup ceiling,
4-GiB process limit, 141,258,578,535-byte reserve, 14,400-second case timeout and
S18 cap. I/O wait remains unavailable when kernel task-delay accounting is off.
Wall time, CPU, logical I/O, block-accounted I/O, RSS and staging allocation remain
separate; inclusive child times are not added to parent times.

## Ordinary comparison

All 18 preselected ordinary lifecycles passed their commands and eight independent
source/imported checks each (144 checks). The baseline-derived thresholds were
frozen after the baseline completed and before the first candidate command.

| Scale | Baseline ingestion median (s) | Candidate median (s) | Saving (s) | Frozen minimum (s) | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| S16 | 9.401911 | 9.166562 | 0.235350 | 0.315276 | Within baseline variation |
| S17 | 21.679509 | 19.461003 | 2.218506 | 0.406388 | Exceeds threshold |
| S18 | 46.604302 | 46.436246 | 0.168056 | 0.621841 | Within baseline variation |

S17 improves by 10.23% on this host and fixture. S16/S18 do not establish an
ingestion improvement. The three-run ranges are not confidence intervals and
do not establish performance on other hosts or scales.

| Scale | Baseline / candidate maximum process RSS (bytes) | Baseline / candidate peak staging allocation (bytes) |
| --- | ---: | ---: |
| S16 | 178,544,640 / 178,892,800 | 419,774,464 / 419,774,464 |
| S17 | 178,900,992 / 179,441,664 | 940,236,800 / 940,236,800 |
| S18 | 179,232,768 / 179,650,560 | 1,880,461,312 / 1,880,457,216 |

RSS maxima increased by 348,160 / 540,672 / 417,792 bytes (0.20% / 0.30% /
0.23%). These small observed increases are disclosed; no additional buffer or
retained allocation is introduced. Staging maxima are unchanged at S16/S17 and
4,096 bytes lower at S18. Removing the late rewrite does not reduce the earlier
whole-construction disk peak. Every resource maximum remains below the unchanged
hard limits. CPU, block-accounted I/O, logical construction I/O and complete
command timings remain separately available in the machine-readable reports.

## Other measured costs

Medians remain separately labeled; whole-lifecycle command wall is the sum of
selected command durations, not the sum of inclusive internal phase timings.
Ingestion CPU is user plus system CPU. Block-accounted I/O is `wait4` block
accounting, not the logical byte counters above or a direct device trace.

| Scale | Baseline / candidate lifecycle command wall (s) | Ingestion CPU (s) | Ingestion block-accounted input (bytes) | Ingestion block-accounted output (bytes) |
| --- | ---: | ---: | ---: | ---: |
| S16 | 17.632187 / 17.447993 | 8.377694 / 8.152543 | 2,916,036,608 / 2,916,032,512 | 871,485,440 / 871,485,440 |
| S17 | 38.000402 / 36.016327 | 19.239253 / 17.160757 | 6,499,741,696 / 6,073,643,008 | 2,019,512,320 / 1,918,812,160 |
| S18 | 78.563611 / 78.215439 | 41.857728 / 41.450852 | 13,555,613,696 / 13,555,613,696 | 4,546,826,240 / 4,546,826,240 |

## Diagnostic work and overhead

The separate candidate diagnostic S17 edge family has exactly 32 original runs,
one 32-input merge, 2,097,152 rows read and written, 100,707,968 Parquet bytes read,
100,692,937 bytes written and 53 logical synchronization calls. The historical
#1282 diagnostic has two groups, including a final unary group: 4,194,304 rows
in each direction, 201,382,357 bytes read, 201,385,874 bytes written and 60 sync
calls. The removed group accounts for exactly 2,097,152 rows in each direction,
100,674,389 bytes read, 100,692,937 bytes written and seven sync calls. The retained
32-input merge still performs its necessary work. These counters describe
logical work; they are not device I/O or independently additive phase durations.
The historical diagnostic and fresh candidate have separately recorded sources;
this comparison establishes omitted work, not a paired timing experiment.

| Scale | Ordinary candidate ingestion median (s) | Separate diagnostic observation (s) | Observed difference (s) |
| --- | ---: | ---: | ---: |
| S16 | 9.166562 | 9.368995 | +0.202433 |
| S17 | 19.461003 | 19.664221 | +0.203218 |
| S18 | 46.436246 | 47.051361 | +0.615115 |

All eight preselected boundary lifecycles passed (64 independent source/imported
checks). At 32 and 1,024 actual inputs per row family, final unary work disappears;
31/33 and 1,023/1,025 preserve their required final merges. The historical
272/1,088 total-chunk cases contain node/edge family inputs 16/256 and 64/1,024,
respectively. Only the latter edge family retains a single completed root; total
chunks are not substituted for family inputs.

Each diagnostic scale runs once. These differences disclose instrumentation and
run variation together; they do not isolate an overhead estimate. Diagnostics
are excluded from the ordinary benefit decision. No new CPU-stack or syscall
profiling campaign was selected; the existing #1282 profiles remain historical
context with their original identities.

## Validation and remaining gate

The full storage suite passes 1,120 tests (zero failed, two existing ignored).
Retained-root recovery tests also pass with diagnostics enabled. The historical
runtime analysis, attribution and new comparison suites pass 3, 10 and 2 tests.
Formatting, storage feature Clippy, fast pre-push and gate-registry validation
pass. Independent review reproduces the ordinary comparison from its raw hashes
and verifies source/build identities, ordering, thresholds and resource limits.

The full `make pre-push` was run on the supported host. Its executed workspace
Rust, facade, BDD and native binding tests passed, but the command **failed** at
the core coverage floor: 91.30% against 95%. Its coverage ledger also records
79.34% CLI coverage against 80%. The changed production lines are 7/7 covered
(100%); uncovered governed production lines are outside this patch. These are
this run's measurements, not a separately measured historical-main baseline.
Native Python/Node adapter coverage passes its 80% floors (80.97% / 80.92%).

The remaining wrapper checks were run separately after that coverage stop.
Python passes 110 tests and 96.97% coverage; Node passes 277 tests but fails its
85% wrapper floor at 80.32% in unchanged wrapper code. All five BDD mutation
checks reject the intended faults. No floor, assertion or test was weakened.
The initial direct-merge fixture failure was corrected by initializing its
allocation authority as real sessions do; the subsequent complete tests pass.
The exact-head PR CI Gate remains required before merge.

## Decision and evidence

Retain the bounded repair: the preselected S17 benefit exceeds its frozen floor,
the removed work is directly attributable, deterministic correctness and recovery
checks pass, and observed resource changes remain small and within all limits.
This decision makes no useful speedup claim for S16/S18 and no staging-peak saving
claim for S17. Later optimizations need their own baseline-derived threshold and
must retain the current correctness, recovery and resource limits.

Machine-readable evidence:

- [Ordinary baseline](parquet-root-1286-baseline.json) and
  [ordinary candidate](parquet-root-1286-candidate.json).
- [Frozen thresholds](parquet-root-1286-thresholds.json) and
  [comparison](parquet-root-1286-comparison.json).
- [Separate diagnostics](parquet-root-1286-diagnostic.json) and
  [boundary diagnostics](parquet-root-1286-boundary.json).
- [Experiment selection and build identities](parquet-root-1286-builds.json) and
  [validation census and raw log hashes](parquet-root-1286-validation.json).

Reports retain content-free timing, work, resource, source, executable, generator,
lockfile and raw-artifact hashes. Raw commands and artifacts stay on the designated
host. No graph contents, UUID inventories or private host paths are published.
The unchanged collector and historical-policy baseline report retain their #1282
metadata; the enclosing experiment and candidate policy identify #1286. Historical
#1282 reports are unchanged.

Reproduce validation/reporting with the frozen artifacts and an admitted host:

```bash
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_parquet_root_1286.py report "$BASELINE"
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_parquet_root_1286.py freeze "$BASELINE" > "$THRESHOLDS"
# Execute the preselected ordinary candidate only after this freeze.
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_parquet_root_1286.py compare "$BASELINE" "$CANDIDATE" "$THRESHOLDS"
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_parquet_root_1286.py report "$DIAGNOSTIC" --candidate
PYTHONPATH=benchmarks/harness .venv/bin/python benchmarks/diagnostics/report_parquet_root_1286.py report "$BOUNDARY" --candidate
```
