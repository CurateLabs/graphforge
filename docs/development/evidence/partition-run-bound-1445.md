# Partition routing buffer bound (#1445)

`PartitionRun` previously accumulated an entire same-partition run within one
staged chunk. The repair bounds retained wire bytes, flushes before a record
would exceed the bound, and sends an oversized record directly. Records remain
whole; partition order, row counts and the publication protocol are unchanged.
Only one routing accumulator is live per routing call; this allocation is not
multiplied by the number of partitions.

## Baseline histogram

Measured on OVHC-AGENCY using release CLI code from `51a0d6b9`, plus temporary
flush-size logging. Graph500 S18, edge factor 16, seed `13907095936298285200`.
The logging patch is retained with the raw evidence and is absent from the PR.

| Family | Flushes | Mean wire bytes | Maximum wire bytes |
| --- | ---: | ---: | ---: |
| Identities (26-byte records) | 323 | 358,723 | 456,144 |
| Node details (compact, padded width 272) | 259 | 21,255 | 21,672 |
| Edge details (compact, padded width 304) | 304 | 731,244 | 922,624 |
| Endpoints (33-byte records) | 16,365 | 16,916 | 511,863 |

This does not support the historical hypothesis of multi-megabyte **mean**
flushes at S18. Routing restarts for each staged chunk. The remaining defect is
lack of an explicit byte bound, as the maintainer's updated #1445 acceptance
comment states; the earlier throughput regression was already repaired.

## Bound-selection protocol

BenchExec measures the complete command tree for `begin`, registration of both
Parquet sources, `validate`, and `commit`, including Python orchestration and CLI
launch costs. Each observation uses a fresh project, the same generated input,
16 logical CPUs and a 4 GiB cgroup limit. Reported memory is peak **cgroup memory**,
including cache; it is not process RSS. No cache dropping is performed.

Compare 64 KiB, 256 KiB and 1 MiB with the baseline in three rotating-order S18
rounds. The predeclared selection rule is the smallest bound with median wall
at most 5% above the matched baseline. This is a selection tolerance, not a
statistical confidence interval or an improvement claim. The baseline binary
retains the temporary logging branch but runs with logging disabled. Candidate
binaries contain no logging branch and differ only in the bound constant.

Require the quiet-host guard before and after each observation, with one-second
checks during execution. The local guard also checks executable paths for renamed
GraphForge benchmark binaries, which the shared name-based guard misses.
Retain failed attempts and invalidate any contended
observation. The selected candidate must also protect #1445's recorded S18/S19/S20
throughput floors: 101,175 / 105,415 / 105,802 edges/s.

The initial curve was stopped after a concurrent #1452 benchmark with a renamed
executable was observed during round three. Its timing observations are retained
but excluded from bound selection. A fresh curve uses the stronger guard and
requires an initial 30-second quiet interval. Three clean rounds completed; the table reports medians:

| Bound | Complete ingest wall (s) | CPU (s) | Peak cgroup memory (MiB) |
| --- | ---: | ---: | ---: |
| Baseline, unbounded | 29.320 | 25.724 | 755.77 |
| 64 KiB | 28.798 | 25.612 | 755.27 |
| 256 KiB | 29.136 | 25.581 | 766.22 |
| 1 MiB | 29.294 | 25.272 | 754.90 |

**Selected: 64 KiB**, the smallest tested bound within the baseline +5% threshold
(30.786 s). This result supports bounding the accumulator without a material
whole-ingest regression under the stated selection rule; it does not establish a
throughput improvement. Two additional attempted 64 KiB observations were rejected
by the stronger guard when builds overlapped; neither enters these medians.

All three historical floors pass under the complete-ingest measurement boundary:

| Scale | Complete ingest wall (s) | Edges/s | Recorded floor |
| --- | ---: | ---: | ---: |
| S18 | 28.798 | 145,644 | 101,175 |
| S19 | 57.463 | 145,982 | 105,415 |
| S20 | 113.882 | 147,321 | 105,802 |

S18 uses the three-run median; S19 and S20 each use one qualified observation.
Both larger ingests committed successfully, processed the expected input row
counts, and passed the before/during/after quiet checks. Peak cgroup memory was
1,337.23 MiB at S19 and 2,549.37 MiB at S20.

## Correctness evidence

`cargo test --release -p graphforge-storage --lib` passed 1,202 tests, with six
existing ignored tests. This includes determinism across sessions and partition
counts, interrupted shaping/recovery, corruption refusal and the new accumulator
regressions. The latter compare exact emitted bytes and row counts while checking
capacity at all three candidate bounds, partition transitions, compact records,
oversized records and final/empty flushes.

`cargo test --release -p graphforge-api --test bdd` passed all 118 required API
scenarios and all 3,897 openCypher scenarios (zero regressions). Timing warnings
from that concurrent validation run are diagnostics, not benchmark evidence.

Raw inputs, binaries, hashes, logging patch, command wrappers, histograms and
BenchExec output are retained at `/home/ubuntu/gf-1445-evidence/` on OVHC-AGENCY.
This report does not qualify S22–S26 or establish #1387's 1M edges/s floor.
