# N-way S18 ingest contention (#1480)

Synchronization blocking is the largest observed contributor to the concurrent
slowdown: main-thread sync blocking rises from 2.500 s alone to 52.105–54.697 s
per import at N=16. Runnable delay and CPU consumption also rise. The evidence
supports a bounded durability-preserving barrier-amortization experiment in
shaping; it does not predict its speedup.

This experiment characterizes `import-session validate` on OVHC-AGENCY.
It does not measure complete ingest, publication, or a redesigned parallel
single ingest. No production synchronization or filesystem admission changes
are included.

## Scope and provenance

The executable is the preserved stock release binary from
`fa4dec51369451679e3f7fe6d29ba8a87d8c542d` used in the historical N-way
probe. It was copied, not rebuilt. Measurements therefore characterize that
build; they are not current-main performance claims. The report branch starts
at `7645f96e`, after the publication CSR change. Publication is not executed.

Input is Graph500 S18, edge factor 16, seed `13907095936298285200`.
Every process receives a fresh project with `begin` and both Parquet sources
registered before measurement. The original input files are shared, matching
the historical probe; registered source copies and output projects are distinct.
Cache preparation is `sync` followed by `drop_caches=3`, once per configuration.
Configurations execute sequentially; processes within each configuration run
concurrently. `RLIMIT_NOFILE=65536` is explicit in every successful run.

The host is an AMD Ryzen 7 3800X, eight physical cores / sixteen logical CPUs,
with native ext4 on md RAID1 over two Samsung PM983 NVMe devices. Both devices
belong to the same array. There is no independent-storage comparison and no
synchronization ablation. All successful ingest runs retain normal durability.

## Instrument and boundaries

The temporary bpftrace diagnostic observes scheduler exec/fork/switch/wakeup/
exit events and the current syscall at switch-out. It records every fsync and
fdatasync entry/return, the descriptor's inode type, and function entry/return
for the binary's shaping and encoding symbols. The stock binary is unchanged.
Raw events use one monotonic clock and are sorted by timestamp before analysis.

Thread states are mutually exclusive: running, runnable, blocked, and unknown.
A sleeping thread becomes runnable at wakeup, not at its next switch-in.
Preemption (`prev_state=256`) and a runnable switch-out (`prev_state=0`) remain
runnable. Newly forked threads have an explicit unknown interval before the
first observed wakeup. Blocked time is classified by the syscall active at
switch-out: synchronization, futex, read/write, timer, other syscall, or unknown.
A futex wait identifies a kernel wait, not which application lock or pool join
caused it. On-CPU stalls remain running time.

Main-thread states reconcile to its exec-to-exit wall boundary. Worker thread
seconds are reported separately and may overlap each other and the main
thread. Idle pool threads are not evidence of critical-path delay. Scheduler
running time includes time scheduled on a CPU, including interrupt effects;
`wait4` user plus system CPU is separately retained, including launch before
exec. Neither scope is silently substituted for the other.

Shaping/encoding intervals are process-wide **phase windows** on the main
thread. Worker intervals are split at those boundaries for temporal attribution;
this does not prove which phase logically owns every worker's computation.
All time outside those two windows remains explicit. Receipt `seal` includes
more than those windows; receipt `append` is a sum of 68 calls. The persisted
receipt's `begin` belongs to preparation, outside the measured process, and is
excluded from validate-only reconciliation.

Sync inclusive latency is reconciled to running, runnable and blocked segments
inside the actual syscall. Counts and latency distributions distinguish file
and directory descriptors. Summed syscall durations are thread seconds;
interval unions describe elapsed coverage within one process. Neither is an
estimate of wall time saved by deleting barriers, and sync time is already
inside the enclosing scheduler/phase totals.

## Controls and limitations

Controls include one second of CPU-bound work, a one-second nanosleep, and two
CPU-bound processes pinned to one CPU. The latter separates runnable delay
from sleeping. A file/directory synchronization control checks exact syscall
counts and descriptor classification. Untraced/traced pairs measure tracing-associated differences;
overhead and run variability are not separately identified; tracer startup and teardown are outside batch wall.

The first ingest attempt failed with `EMFILE` under the inherited descriptor
limit. Its error and partial execution remain in the evidence. After setting
the explicit limit, all selected configurations use identical limits.

The initial N=1 and N=8 traces used bpftrace `-c`, which restricted uprobes to
the launcher rather than its children. Their scheduler/syscall observations
remain available, but their missing phase events are not interpreted as zero
phase work. Later runs attach the tracer independently and verify phase
coverage for every measured process.

All configurations use CPUs 0–15 with no cgroup CPU quota or memory cap at the
session or ancestor cgroups. This is the historical concurrency-probe resource
scope, not the ladder's per-rung memory qualification. Full cgroup limits,
hardware, existing swap occupancy, input/binary SHA-256s and commands are in
the machine-readable evidence.

The existing quiet-host guard checks a finite list of executable names. It
reported quiet before each configuration. No heavy builds or parallel benchmark
campaigns were launched during this study, but this is a shared host, not
exclusive hardware. The guard cannot rule out arbitrary background work.
The repeat pairs bound observed variability; they are not a statistical
confidence interval. Tracing consumes CPU and writes raw events to the shared
filesystem, and its kernel work contributes to tracee CPU. Traced timings are
diagnostics, not production throughput qualification.

## Results

All values in the following tables are **measured**, rounded for display.
N>1 per-process values are arithmetic means. Every successful process accepted
4,456,448 input rows (262,144 node and 4,194,304 edge attempts), with zero rejected
rows and `outcome=validated`. These are not published live-edge counts.

### Complete configuration census

| Run           |   N | Traced | Batch wall s | CPU s/process (wait4) | Seal CPU s/process | Seal elapsed s/process |
| ------------- | --: | ------ | -----------: | --------------------: | -----------------: | ---------------------: |
| `n1-plain-b`  |   1 | no     |       21.912 |                20.204 |             14.450 |                 15.253 |
| `n1-trace-b`  |   1 | yes    |       22.902 |                21.157 |             15.300 |                 16.095 |
| `n8-plain-b`  |   8 | no     |       51.523 |                24.457 |             16.999 |                 39.502 |
| `n8-trace-b`  |   8 | yes    |       52.789 |                26.346 |             18.794 |                 40.359 |
| `n16-plain-b` |  16 | no     |       96.069 |                36.157 |             25.837 |                 73.414 |
| `n16-trace-b` |  16 | yes    |      101.107 |                35.071 |             24.808 |                 77.729 |
| `n16-trace-c` |  16 | yes    |      102.722 |                36.316 |             26.309 |                 78.426 |
| `n16-plain-c` |  16 | no     |      101.390 |                36.167 |             26.279 |                 77.968 |
| `n1-trace-c`  |   1 | yes    |       23.213 |                21.292 |             15.460 |                 16.411 |
| `n1-plain-c`  |   1 | no     |       21.967 |                19.852 |             14.130 |                 15.244 |

The first two pairs are phase-incomplete pilots. The N=16 pairs reverse order
(plain→trace, then trace→plain); the final N=1 pair is trace→plain. The failed
`n1-plain-a` attempt is retained separately: 10.099 s batch wall, exit 3,
`GF_IO: Too many open files`, inherited soft limit 1024.

**Derived comparisons, not additional measurements:** tracing-associated wall
differences are +4.52% / +5.67% at N=1, +2.46% at N=8, and +5.24% / +1.31%
at N=16. The two untraced N=16 runs themselves differ by 5.54%, so a precise
causal tracing-overhead estimate is not identified. N-process throughput
speedups from matched untraced pairs are 3.40× at N=8 and 3.65× / 3.47× at
N=16 (`N*T(1)/T(N)`). These are neither effective-core measurements nor
limits for one parallel ingest.

### Main-thread wall reconciliation

Each row partitions mean exec-to-exit wall exactly before rounding. Synchronization
and other blocking partition the blocked category; no inclusive sync latency
is added again. Unknown state is the entire interval whose blocked/runnable
split could not be established.

| Trace         | Main wall s | Running s | Runnable s | Sync blocked s | Other blocked s | Unknown state s |
| ------------- | ----------: | --------: | ---------: | -------------: | --------------: | --------------: |
| `n1-trace-c`  |      23.200 |    18.058 |      0.084 |          2.500 |           2.558 |           0.000 |
| `n16-trace-b` |      99.954 |    29.674 |      2.410 |         52.105 |          15.687 |           0.078 |
| `n16-trace-c` |     102.207 |    30.853 |      2.438 |         54.697 |          14.149 |           0.070 |

Other blocking includes main-thread futex waits (1.796 s alone; 3.169 / 3.243 s
at N=16), read/write (0.633; 6.611 / 5.669 s), other syscalls (0.041;
5.842 / 5.176 s), and blocked time of unknown cause (0.089; 0.065 / 0.061 s).
Unknown **cause** within an observed blocked interval differs from unknown
scheduler **state**.

Worker threads add 3.151 s scheduled running alone and 5.318 / 5.376 s per
process at N=16. Their accumulated waiting overlaps the main thread and
includes idle pools. In particular, N=16b has 6.258 s/process of unknown worker
state (N=16c: 0.061 s); do not infer critical-path lock contention from worker
wait sums. Across all threads, scheduled running differs from wait4 CPU by
about 0.08 s/process; exact per-process values are retained.

### Phase placement and barrier distributions

Every selected process has exactly one shaping and one encoding interval,
both entered on the main thread. Counts below are **per process** and equal
in all selected traces. Percentiles are nearest-rank percentiles over all calls
in the named phase/type for that configuration, not averages of percentiles.
The table contrasts N=1c and N=16b; the full N=16c distributions are in JSON.

| Phase                | Descriptor | Calls/process | N=1 inclusive s/process | N=16b inclusive s/process | N=1 mean / p99 ms | N=16b mean / p99 ms |
| -------------------- | ---------- | ------------: | ----------------------: | ------------------------: | ----------------: | ------------------: |
| shape                | file       |         2,739 |                   1.560 |                    26.383 |     0.570 / 2.577 |     9.632 / 100.119 |
| shape                | directory  |         6,554 |                   0.555 |                    15.397 |     0.085 / 0.139 |      2.349 / 19.937 |
| encode               | file       |            88 |                   0.189 |                     1.549 |   2.146 / 115.843 |    17.603 / 391.232 |
| encode               | directory  |            98 |                   0.008 |                     0.450 |     0.086 / 0.166 |     4.594 / 119.047 |
| outside_shape_encode | file       |         1,295 |                   0.547 |                     7.229 |     0.423 / 2.010 |      5.582 / 47.170 |
| outside_shape_encode | directory  |         2,380 |                   0.135 |                     2.779 |     0.057 / 0.137 |      1.168 / 17.105 |

There are 13,154 actual sync syscalls per process (9,032 directory, 4,122 file).
Shaping contains 9,293 of them. All return successfully. The raw trace retains
syscall identity, descriptor number/type, thread, timestamps and phase
placement; it does not distinguish every source-level callsite inside a phase.
The receipt application-I/O sync count has a narrower instrumentation scope
and is not used as the syscall oracle.

Inclusive sync latency alone is 2.995 s at N=1c; 53.788 / 56.278 s/process at
N=16. Its observed blocked component is 2.500; 52.105 / 54.697 s, respectively.
Running, runnable and unknown segments inside those calls explain the
remainder. Per-process interval unions and a separately identified pooled
batch union are retained; pooled distribution rows use `pid=0` in JSON.

| Trace         | Shape elapsed s/process | Encode elapsed s/process | Seal minus those windows s/process | Main wall minus receipt append and seal s/process | wait4 CPU minus receipt append and seal CPU s/process |
| ------------- | ----------------------: | -----------------------: | ---------------------------------: | ------------------------------------------------: | ----------------------------------------------------: |
| `n1-trace-c`  |                  12.523 |                    3.814 |                              0.074 |                                             2.846 |                                                 2.492 |
| `n16-trace-b` |                  68.124 |                    9.435 |                              0.170 |                                             5.840 |                                                 4.228 |
| `n16-trace-c` |                  67.531 |                   10.595 |                              0.300 |                                             5.655 |                                                 4.038 |

The outside-window phase includes append, decoding, checkpoints, startup and
other validation work; it is not an extra phase to add to receipt elapsed time.
Seal CPU is process-wide. Exact per-phase scheduled thread seconds, including
worker overlap, are retained separately instead of fabricating phase CPU from
receipt CPU fractions. No publication work is included in any of these rows.

## Instrument validation and unresolved observations

- The one-second sleep has 1.000039 s observed timer blocking. The CPU control
  has 1.000341 s scheduled running. Two one-CPU competitors each record about
  one second running and one second runnable, without a blocking interval.
- The synchronization control records exactly two file calls (fsync and
  fdatasync) and one directory fsync, all successful.
- Selected traces have matching batch/traced PIDs, exec/fork/exit counts,
  sync entry/exit counts, and exactly two paired phase windows per process.
  Neither output stream reports lost/dropped events. Absence of such a message
  alone is not proof of complete kernel-event delivery.
- The first strict analysis **failed**: 46 N=8, 1,362 N=16b and 1,155 N=16c
  switch-ins lacked an intervening recorded wakeup. The trace cannot determine
  those gaps' blocked/runnable split, and their kernel/tracer cause is not
  established. The corrected analysis retains each entire gap as unknown,
  rather than assigning it to blocking. Initial analysis and failure output
  remain archived. A synthetic regression checks both absent and present
  wakeup events and proves the ambiguous gap is never counted as blocked.
- Final acceptance checks pass for controlled states, exact sync counts/types,
  phase coverage, process CPU reconciliation and unknown accounting. Negative
  controls reject empty traces, missing required phases, and explicit event
  loss. All observations, including pilots and failures, remain retained.

## Decision supported

Synchronization blocking is the largest observed contribution to this
**N-process contention loss**. Compared with N=1c, the N=16 main timeline grows
by 76.754 / 79.008 s per process: sync blocking accounts for 49.605 / 52.197 s
of that growth, running for 11.616 / 12.795 s, and observed runnable delay for
2.326 / 2.354 s. Other blocking and explicit unknown intervals complete the
accounting. These differences are **derived** from the measured timelines;
they are not recoverable savings estimates.

CPU consumption also rises: untraced wait4 CPU is 19.852–20.204 s/import alone
versus 36.157–36.167 s at N=16. This does not establish increased instruction
count or identify a cache, memory-bandwidth, frequency, or kernel mechanism.
It rules out a diagnosis based only on waiting while leaving that CPU increase
as a separate observed effect. Runnable delay is real but is substantially
smaller than synchronization blocking in these main-thread traces.

The bounded next intervention is **#1452's durability-preserving barrier
amortization in shaping/spill publication**, with the 6,554 shaping directory
and 2,739 shaping file calls as the measured target. Revalidate crash/refusal
contracts and compare the same durable S18 path before claiming a benefit.
These results do not authorize removing fsync or prove that the shared array,
ext4 journal, directory locking, and device service time have been separately
isolated. #1477 should carry explicit scheduler uncertainty into reusable
instrumentation. #1481 still owns publication attribution.

No CPU/edge ceiling, 1M-edge/s qualification, complete-ladder gain, or
single-parallel-ingest performance claim follows from this study.

## Evidence and reproduction

[Machine-readable observations and sources](ingest-contention-1480.json)
contain every selected run, per-process receipts/resources, phase and scheduler
breakdowns, latency distributions, final temporary diagnostic sources, failed
controls, provenance and SHA-256 inventory. Raw traces and prepared projects
remain on OVHC-AGENCY at `/home/ubuntu/graphforge-1480/`. The sources are
experiment-specific reproducer material, not permanent instrumentation APIs.

```bash
python3 /home/ubuntu/graphforge-1480/jobs/test-analysis.py
python3 /home/ubuntu/graphforge-1480/jobs/validate.py
python3 /home/ubuntu/graphforge-1480/jobs/tables.py
```

The first two commands pass. To reproduce a fresh configuration, use a new
name with `jobs/run.py run NAME N plain|trace`; the runner refuses to overwrite
an existing project. Run configurations sequentially on a quiet native host,
with the recorded binary, input hashes and limits. Tracing requires privileged
bpftrace access. Rebuilding the binary requires re-resolving the uprobe symbols.
