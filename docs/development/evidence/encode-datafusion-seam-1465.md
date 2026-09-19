# Encode Arrow/DataFusion seam (#1465)

## Scope and decision criteria

This experiment keeps GraphForge's authenticated descriptors, recorded UUID
splitters, surrogate assignment, receipts, recovery, and CAS publication. It
replaces only the execution of an already prepared Arrow batch through the
existing Parquet writer. It is compiled for tests or `test-support`, selected
only by `GF_ENCODE_SEAM_SPIKE=datafusion`; normal execution is unchanged.

The spike is limited to one adapter design. At 17:58 UTC, before measurement,
a 20:00 UTC decision deadline was recorded, excluding external review/CI and
waits for a naturally quiet host. Correctness and resource compatibility determine seam viability.
Performance is a separate observation; no throughput improvement is required
or claimed from worker count. A pass permits only a bounded shaping experiment
with its own predeclared end-to-end benefit/noise criterion.

## Execution and ownership

The physical plan is emitted from DataFusion's plan formatter, not inferred
from an API name. `DataSinkExec` consumes one `StreamingTableExec` partition.
There is no repartition, global merge, `SortExec`, or additional input-data batch
materialization. GraphForge already holds the input batch. The source and sink
share its Arrow arrays through `Arc`. DataSinkExec also creates a one-row
count result. The sink records the estimated memory of columns whose
`ArrayRef` identity changed; unchanged identity confirms input-array reuse
at this boundary. A changed wrapper could still share underlying buffers.

Encode's existing row-index sort is bounded by an output batch; the artifact
inventory sort orders metadata paths. Neither introduces a skew-sensitive
DataFusion sort, so #1439 is not a prerequisite for this experiment. It remains
a prerequisite before adopting a skew-sensitive `SortExec`.

One scoped worker runs one current-thread Tokio runtime. No blocking thread
pool or asynchronous filesystem wrapper is introduced. The caller polls its
existing cancellation callback, latches cancellation for the sink, and joins
the worker before returning, including failure paths. The sink checks between
batches and before final compression/footer generation; an individual Parquet
write remains a bounded, non-preemptible unit, as in the baseline.

The sink owns the already-open writer, including intermediate cache-window
fsyncs. The completed descriptor returns to GraphForge for final file fsync,
namespace installation, receipts and recovery. Errors leave the existing
private temporary-file guard responsible for cleanup.

## Resource accounting and its limits

A DataFusion `GreedyMemoryPool` reserves the input batch's Arrow memory before
execution. The default experimental reservation ceiling is 64 MiB and can be
set by `GF_ENCODE_SEAM_POOL_BYTES`. This pool does **not** account for all writer,
compression or runtime allocations. The experiment separately admits the whole
process tree under a 4 GiB cgroup limit and 16 logical CPUs (0–15).

`changed_column_estimated_bytes` is an identity-change diagnostic, not an
allocation profiler or exact copy counter. Source inspection establishes that
the adapter adds no input-payload copy: it clones `Arc` references. This says
nothing about unchanged Parquet/internal allocation traffic. `writer_retained_bytes` is the
writer's retained memory after a batch, not a peak-memory measurement. Timed
runs disable extra worker logging. A separate instrumented run records pool,
writer and operator counters.

The existing region instrument reports canonical encode wall and process CPU.
Its coordinator-thread scheduler counters now describe waiting on the worker.
Instrumented worker captures are separate, explicitly thread-scoped trees;
their overlapping process CPU values must never be added as disjoint work.
Whole-process cgroup peak memory includes file cache and is not RSS or an
encode-exclusive peak.

## Correctness evidence

The focused seam tests compare actual baseline/candidate Parquet bytes, reject
an insufficient pool budget, and verify failed/cancelled outputs leave no
private temporary files or published output counters. A deterministic stream
handshake raises cancellation after the first batch and before the second;
the sink must refuse the second batch and never return a completed descriptor.
The outer cancellation test separately exercises cleanup and joined return.

With `GF_ENCODE_SEAM_SPIKE=datafusion`, the complete release storage library
suite passed **1,210 tests**, with six existing ignored tests. This includes
ordinary readers, heterogeneous properties, cancellation/retry, authenticated
source mutation, encoded corruption, and crash recovery at durable boundaries.
Separate baseline and candidate processes also ran
`same_input_twice_produces_identical_digests`; their emitted shaped and encoded
payload digest maps are identical. The established fixture pins session clock
and operation identity and excludes the explicitly nonce-bearing ordinal
receipt control file. This does not claim to fix #1416's wall-clock defect.

Strict Clippy with `test-support`, `make pre-push-fast`, and
`make gate-registry-check` passed. Exact commands and measurement provenance are
recorded with the accompanying JSON evidence.

## Measurement boundaries

The same release binary runs both modes over Graph500 S18: 262,144 nodes,
4,194,304 edges, edge factor 16, seed 13907095936298285200. Each boundary uses
three alternating matched pairs and fresh projects. The frozen measured code
is `f5b89e68`, based on `fafa0aba`; #1452 landed separately during measurement
and is outside this A/B tree. These figures describe this adapter comparison,
not a new qualification of the moving `main` branch. The shared quiet-host
check must print `QUIET` before and after; a stronger executable-path check
samples every second during execution. Raw contended attempts would be retained
and excluded, not treated as accepted timing observations.

Complete ingest measures the process tree for begin, two Parquet registrations,
validate, and commit. Every accepted complete ingest must publish successfully
with 4,456,448 accepted input rows and zero rejected rows.

For resumed encoding, preparation stages, seals and shapes the same inputs,
then deliberately refuses the first sink reservation with a zero-byte pool.
This happens outside the measurement. A fresh process resumes validate with the
normal pool budget. Its measured memory envelope includes startup, recovery,
authority checks and completed-shape revalidation as well as encoding; it is
not an exclusive sink peak. The named `canonical_encoding` region provides
the narrower encode wall/process-CPU observation in both boundaries.

Three complete-ingest observations per mode gave these medians:

| Measurement | Baseline | DataFusion seam |
| --- | ---: | ---: |
| Complete ingest wall | 29.334 s | 29.556 s |
| Complete ingest CPU | 25.737 s | 25.660 s |
| Canonical encode wall | 7.798 s | 7.885 s |
| Canonical encode process CPU | 6.960 s | 7.070 s |
| Complete ingest cgroup peak memory | 753.34 MiB | 750.55 MiB |

Complete-ingest wall ranges were 28.788–29.372 s for baseline and
29.196–30.334 s for the seam. These small samples describe observed variation;
they do not establish statistical significance. The seam's median complete
ingest wall is 0.76% higher, with encode wall 1.11% higher and encode CPU 1.58%
higher. This is **not a throughput improvement**.

The three resumed-encoding pairs gave:

| Measurement | Baseline | DataFusion seam |
| --- | ---: | ---: |
| Resume-to-encoded invocation wall | 8.039 s | 8.614 s |
| Resume-to-encoded invocation CPU | 7.502 s | 8.057 s |
| Canonical encode wall | 7.594 s | 8.169 s |
| Canonical encode process CPU | 7.270 s | 7.820 s |
| Invocation cgroup peak memory | 632.35 MiB | 621.13 MiB |

The resumed-encoding seam was slower in all three matched pairs. Its median
canonical encode wall increased 7.57% and CPU increased 7.57%. The invocation
wall ranges were 7.958–8.069 s versus 8.473–8.662 s. Fresh-process recovery and
cache state differ from encoding immediately after shaping, so these results
are a separate boundary, not additional replicates of complete ingest.
Preparation was outside the measured process limit; all persisted construction
budgets were checked identical across both modes and both boundaries. Every
measured invocation used the same CPU and total-memory limits.

The separate instrumented S18 ingest observed **69 sink executions**, each with
one input batch, and 4,456,449 rows across those calls (input graph rows plus
the surrogate-tail metadata row). All input-column identities were preserved.
The maximum reserved Arrow estimate was 5,768,072 bytes; the largest observed
post-batch writer retention was 4,785,261 bytes. The actual plans contained only
the sink and one streaming source, with topology-node, edge, or surrogate-tail
projections. Each sink also produced its one-row count result. No sort,
repartition or global merge appeared. This instrumented invocation is excluded
from performance medians.

All 12 timed observations passed before/during/after quiet checks; none needed
exclusion. Full samples, ranges, input/binary hashes, persisted budgets, plans,
I/O counters and receipt hashes are in
[`encode-datafusion-seam-1465.json`](encode-datafusion-seam-1465.json).
Raw binaries, input references, scripts, preparation refusals and BenchExec
logs remain at `/home/ubuntu/gf-1465-evidence/` on the measurement host.

## Decision

**Seam: viable for this bounded encode surface.** Arrow identity, descriptor
ownership, cancellation/join behavior, pool refusal, recovery and corruption
refusal survived the tested integration. The pool remains partial accounting;
the separate total-memory budget is mandatory. This does not qualify every
DataFusion operator, spilling, or arbitrary concurrent execution.

**Performance: retain the baseline.** This adapter adds orchestration without
removing existing encode work, and the resumed-encoding measurements show a
consistent regression. It remains an opt-in test-support experiment, not the
default encoder.

A bounded shaping experiment is justified to test whether a reusable operator
can remove measured work while retaining these ownership boundaries. It must
first define its own scope and end-to-end benefit/noise criterion under equal
limits; #1439 must be satisfied before a skew-sensitive sort is adopted. This
result authorizes neither shaping migration nor a claim about #1387's floor.
