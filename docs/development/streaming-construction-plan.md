# Streaming construction plan — the #1387 floor (rev 11)

> **Status: current, rev 11, 2026-09-18.** This is the plan of record for
> [#1387](https://github.com/CurateLabs/graphforge/issues/1387) (the ingest
> throughput floor) and [#1456](https://github.com/CurateLabs/graphforge/issues/1456)
> (the construction-engine foundation), both on milestone
> [M5](https://github.com/CurateLabs/graphforge/milestone/5). It supersedes rev 10 of
> the "Streaming Construction Plan" Claude artifact, which is now a pointer to this file.
> Revisions are made by pull request; the evidence it cites lives on the bench host
> `OVHC-AGENCY` at the paths in [section 12](#12-evidence).
>
> **Read the labels.** Every number is marked **measured** (a value a run reported) or
> **estimated** (a value derived by applying a ratio or a model to measured values).
> The two are never mixed in one table. A rung-wide CPU/wall ratio applied to one
> phase is an estimate, and so is anything produced by inverting Amdahl's law.

## 0 · What rev 11 changed, and why

Rev 10 was red-teamed on 2026-09-18 (`/redteam critique`, scored 10/24). Five P0 and
five P1 findings were raised. This revision is the fix. The one-line map:

| finding | rev 10 said | rev 11 says | how it was settled |
|---|---|---|---|
| P0.1 bytes bind first | bytes are phase 3, "gates S25 alone" | bytes bind the floor harder than cores at S24+; [§2.2](#22-bytes-per-edge-measured) | arithmetic on rung evidence, no new run |
| P0.2 "CPU banked" assumed 16 threads = 16 cores | budget met at 8.13 µs | met **single-threaded**; this host delivers 7.3–8.0 single-thread-equivalent cores on sort/decode-shaped work, 15.1 on hashing; [§2.3](#23-what-sixteen-threads-deliver-on-this-host-measured) | **measured**, F2 probe, quiet host |
| P0.3 "71% serial" | Amdahl serial fraction | seal's share of CPU; the serial fraction has never been measured and CPU/wall cannot measure it; [§2.4](#24-what-effective-cores-does-and-does-not-say) | re-derivation; instrument spec in [§5 step 1](#step-1--make-the-instrument-report-three-numbers-not-one) |
| P0.4 critical path through a refuted region | phase 1c "prefix sum" is "the one" | removed; the sequencing rationale it carried is recorded as open; [§5](#5--the-work-in-dependency-order) | #1387 comments 5734073771, 5734143331 |
| P0.5 merge queue | baseline excludes twelve open PRs | eight landed; five remain, state recorded; [§4](#4--the-merge-queue-is-part-of-the-plan) | `origin/main` at `d16c3791` |
| P1.1 rising throughput may be page-cache regime | "shape defect absent across 16×" | true in the cached regime; S24 is the first uncached rung and it bends; [§8](#8--open-unknowns) | #1473 owns the experiment (F3) |
| P1.2 unattributed ingest wall | not mentioned | 14–18% of ingest, mildly super-linear; [§3.3](#33-what-the-receipt-does-not-attribute-measured) | receipts, all nine rungs |
| P1.3 `publish` | not in any phase | 28.2 s at S22 at 0.58 effective cores; on the list; [§6](#6--nothing-sacred) | receipt `operation_timings` |
| P1.4 seam spike criterion | "does the seam hold" | proposed throughput criterion, decision on #1465; [§9 D3](#9--decisions-this-plan-needs-from-the-maintainer) | — |
| P1.5 the code's own floor | not mentioned | `INGEST_FLOOR_EDGES_PER_SECOND = 15_000` with a ratchet rule; the epic's acceptance box is untrue in code; [§9 D5](#9--decisions-this-plan-needs-from-the-maintainer) | `m6_storage_io.rs:309` |

Nothing in this revision changes #1387's floor. Where the analysis argues for a
different *statement* of the floor, it is written as a proposal in
[§9](#9--decisions-this-plan-needs-from-the-maintainer) and the decision is the
maintainer's.

## 1 · The floor

**#1387: 1,000,000 edges per second, sustained at every supported scale.** Binding,
confirmed by the maintainer on 2026-09-18 (#1387 comment 5724050403): not retired, not
deferred past v0.6.0, and no invariant is exempt from re-examination against it. S26
admission is a release claim about a billion-edge round trip; it does not close the epic.

The floor is one number. The machine it must be met on has four resources that can each
refuse it — CPU per edge, bytes per edge, achievable parallelism, and the fixed cost at
the small end — and the ladder adds a fifth, the scaling exponent. Section 2 states what
each one measures today and what the floor requires of it. That is the change from rev
10, which collapsed the requirement to one resource (parallelism) and was wrong to.

## 2 · What the floor requires, per resource

### 2.1 The reference rows

Four clean rungs on `93c041df` (measurement commits on top of `76462543`; the `gf`
binary carried no diagnostics), quiet host, all ten phases passed. **Measured**, from
`s18-s22-93c041df-evidence/`:

| rung | edges | ingest wall | edges/s | ingest eff. cores | `seal` wall | `seal` eff. cores | `seal` share of ingest wall |
|---|---:|---:|---:|---:|---:|---:|---:|
| S18 | 4,194,304 | 38.3 s | 109,581 | 0.88 | 26.6 s | 0.90 | 69.4% |
| S19 | 8,388,608 | 75.4 s | 111,258 | 0.90 | 51.4 s | 0.92 | 68.1% |
| S20 | 16,777,216 | 149.2 s | 112,470 | 0.91 | 99.8 s | 0.95 | 66.9% |
| S22 | 67,108,864 | 593.1 s | 113,152 | 0.92 | 386.8 s | 0.97 | 65.2% |

`seal` effective cores here are **measured** — the branch carried per-operation
`cpu_ns` (now on `main` via #1470/#1474): 376.19 CPU-s over 386.79 s at S22.

The S24 row exists only on `f80f69fe` (`clean-f80f69fe-evidence/`, quiet host, older
build). **Measured** wall and edges; the CPU column applies the rung-wide CPU/wall ratio
to the ingest phase and is therefore **estimated**:

| rung | edges | ingest wall | edges/s | rung eff. cores | µs CPU/edge *(est.)* |
|---|---:|---:|---:|---:|---:|
| S22 | 67,108,864 | 642.9 s | 104,390 | 0.88 | 8.46 |
| S24 | 268,435,456 | 2,754.0 s | 97,469 | 0.89 | 9.12 |

Two ladders, 5–8% apart on the same rungs, different builds and days. Neither is an
improvement claim over the other.

### 2.2 Bytes per edge (measured)

Construction `application_io` totals, from the rung JSON. **Measured**:

| rung | logical read | logical write | read B/edge | write B/edge |
|---|---:|---:|---:|---:|
| S22 (`93c041df` and `f80f69fe`, identical) | 81.07 GB | 47.67 GB | 1,208 | 710 |
| S24 (`f80f69fe`) | 325.45 GB | 191.11 GB | 1,212 | 712 |

Logical bytes per edge are a **constant**: 1.2 KB read and 0.7 KB written per edge,
before any physical amplification. Physical bytes as benchexec reports them (the harness
double-counts on md RAID1 — reads about 2×, writes about 3× — so these are "harness
units", and the ratio below is stated in the same units):

| rung | physical read | physical write | phys/logical read | phys/logical write | rung wall | achieved read + write rate |
|---|---:|---:|---:|---:|---:|---:|
| S22 (`93c041df`) | 352.8 GB | 248.6 GB | 4.35× | 5.21× | 863 s | 697 MB/s |
| S24 (`f80f69fe`) | 1,724.6 GB | 1,533.7 GB | 5.30× | 8.03× | 4,724 s | 690 MB/s |

**What the floor requires of bytes.** At 1,000,000 edges/s the logical traffic alone is
**1.21 GB/s read + 0.71 GB/s written**, in the best possible case — perfect page cache,
zero write amplification. The harness measures this md RAID1 at roughly 360 MB/s read and
270–325 MB/s write (the rates it uses for `io_reader_publication_headroom`; they are
achieved rates, see the caveat). So the ideal case is **3.4× over on reads and 2.4× over
on writes**. At the S24 physical ratios it is 6.4 GB/s + 5.7 GB/s — roughly **18×
over** in harness units, roughly 9× after correcting for double counting. At S26 the
floor allows 18 minutes of ingest; today's bytes per edge put ~13 TB of harness-unit
physical I/O into those 18 minutes.

**Bytes per edge must fall by an order of magnitude before the floor is a CPU question
at S24 and above.** Rev 10 sequenced bytes last, as "phase 3, orthogonal, gates S25
alone". They gate the floor harder than any item in phases 0–2. This agrees with the
standing project note that S25 admission is gated on bytes, not time, and with #1387's
own acceptance criterion "bytes read per edge reduced by at least an order of magnitude".

*Caveat (falsifier F1, open):* nobody has measured the device's ceiling; the harness
treats the achieved rate as capacity. An `fio` measurement of the md RAID1 under a
fsync-heavy mixed pattern could relax the multiplier. It cannot relax the conclusion:
the ideal-cache case is already 2.4–3.4× over.

### 2.3 What sixteen threads deliver on this host (measured)

Rev 10 said the CPU budget was met: 8.13 µs per edge against #1387's "under 9.0". That
figure is **single-thread-equivalent** CPU. #1387's Amdahl table (1,557,097 edges/s at
0% serial with a 25% cut) silently assumes 13.9 effective cores from 8 physical cores
with SMT. Whether the host delivers that was never measured. It now is.

**F2 probe, 2026-09-18, quiet host (`gf-quiet-host.sh` printed QUIET; load 0.10).**
N identical single-threaded processes on independent data, run concurrently;
scaling(N) = N × T(1) / T(N). A known positive is built in: 8 processes on 8 physical
cores must scale ≈ 8× or the instrument, not the host, is at fault. **Measured**:

| workload | 2 | 4 | 8 | 12 | 16 | SMT yield (16 over 8) |
|---|---:|---:|---:|---:|---:|---:|
| SHA-256, `openssl speed -multi`, 64 KiB blocks (SHA-NI) | 2.02× | 3.93× | **8.04×** | 11.6× | **15.07×** | 1.87× |
| zstd decode, `zstd -d -T1`, 133 MB text, 40 passes/process | 1.94× | 3.73× | **7.43×** | — | **7.96×** | 1.07× |
| integer sort, GNU `sort -n --parallel=1`, 8M lines | 1.86× | 3.32× | **6.14×** | — | **7.27×** | 1.18× |

Method notes: SHA-256 is `openssl speed -seconds 3 -bytes 65536 -multi N sha256`,
aggregate throughput reported by openssl. The other two are wall-clock over concurrent
processes (`/home/ubuntu/gf-redteam-scratch/f2-*.log`). GNU sort and zstd are proxies
for GraphForge's external sort and Parquet decode, not the code itself; they were chosen
because the shape of the work (memory-bound comparison sort; branchy LZ decode) is what
governs SMT yield, not the implementation.

**What it means.**

- The known positive holds for hashing and decode (8.04×, 7.43× on 8 cores). Sort reaches
  only 6.14× on 8 physical cores: it is memory-system bound before it is core bound.
- The host is **bimodal**. Hash-shaped work SMT-scales almost perfectly (15.1 of 16).
  Sort- and decode-shaped work tops out at **7.3–8.0 single-thread-equivalent cores** on
  16 threads.
- The floor needs **8.13 single-thread-equivalent cores** at today's CPU per edge
  (**estimated** from the rung-wide ratio). Shaping (55% of `validate`, sort-shaped) and
  encoding (18%, decode/encode-shaped) are the majority of the path. On that majority the
  host's ceiling is *below* the requirement.
- So rev 10's P0.2 was half right. The "16 threads = 16 cores" assumption is falsified
  in both directions: it undercounts hashing and overcounts sorting. The conclusion that
  survives is narrower and firmer: **the CPU-per-edge cut is not optional.** On
  sort/decode-shaped work every microsecond removed is worth 1/7.3 of the floor; no
  parallelism work has that exchange rate on this host.

What the CPU budget has to be for the floor to have margin on this host is derived in
[§9 D1](#9--decisions-this-plan-needs-from-the-maintainer).

### 2.4 What "effective cores" does and does not say

Rev 10's KPI tile read "71% serial today — floor needs < 6.5%". Both halves are
Amdahl-inverted numbers, and the first is not one. Amdahl with s = 0.71 on 16 threads
gives 1.37 effective cores; the measurement is 0.92. **71% is `seal`'s share of ingest
CPU**, a real and useful number, and it is what the tile now says.

In the Amdahl sense the path is ~100% serial today: no region exceeds 1.16 effective
cores (#1387 comment 5734143331 — nine stages, none above 1.16, none below 0.78,
aggregate 0.92). And effective cores *below* 1.0 is not serial dependency at all; it is
**wall with no CPU on it**. **Measured** at S22 (`93c041df`):

| region | wall | CPU | eff. cores | wall with no CPU |
|---|---:|---:|---:|---:|
| ingest phase (rung-wide ratio, so this row is *estimated*) | 593.1 s | ~545.7 s | 0.92 | ~47 s (8%) |
| `seal` | 386.8 s | 376.2 s | 0.97 | 10.6 s (3%) |
| `append` | 69.2 s | 61.2 s | 0.88 | 8.0 s (12%) |
| `publish` | 28.2 s | 16.3 s | 0.58 | 11.9 s (42%) |
| whole rung, PSI `pressure_io_seconds` ("some" stall) | 863 s | — | — | 54.8 s (6.3%) |

At S24 the rung-wide PSI I/O stall is 296.8 s of 4,724 s — the same 6.3%. That idle is
I/O wait or lock wait, not sequential dependency, and #1448's spike already showed the
difference matters: its 1.46× "was overlapping fsync waits, not parallel CPU", with
CPU-seconds flat. An instrument that reports CPU/wall alone cannot tell the two apart,
and the landed module says so in its own doc comment (`concurrency_attribution.rs`:
"It conflates two different things, and that matters"). #1462 as filed asked for a
serial fraction; what landed (#1470, #1474) is effective cores plus an Amdahl inversion
labelled as an estimate. That is honest and it is not the quantity #1387 budgets.
[§5 step 1](#step-1--make-the-instrument-report-three-numbers-not-one) says what the
instrument has to report before any A/B is judged on it.

### 2.5 Scaling and the small end

- **Scaling exponent, measured** on `f80f69fe` S18→S24: ingest wall 41.5 → 2,754.0 s for
  64× edges, exponent 1.009. CPU per edge (*est.*) rises 8.46 → 9.12 µs from S22 to S24
  (+7.8%). A floor at every scale forbids that drift; see [§8](#8--open-unknowns) for
  what bends.
- **Fixed cost, measured** at S18: `begin` 0.001 s + `resume` 0.25 s + `publish` 1.7 s
  ≈ 2.0 s of a 38.3 s ingest before or after any edge is touched. At the floor S18's
  4.19M edges are allowed 4.2 s in total. A pure rate has no term for this; the
  proposal in §9 D1 gives it one.

## 3 · Where the time goes (measured)

### 3.1 Inside `validate` at S18 rung scale

`gf import-session validate` driven directly on real S18 Graph500 data, release, quiet
host, diagnostics scopes compiled in, 97.7% attributed (#1387 comments 5734073771,
5734143331). **Measured**; wall 36.14 s, 0.92 effective cores:

| stage | wall | % of validate | eff. cores |
|---|---:|---:|---:|
| `shaping` | 19.88 s | 55.0% | 0.93 |
| `canonical_encoding` | 6.50 s | 18.0% | 0.88 |
| normalization (`normalize_import_*_chunk`) | 4.69 s | 13.0% | 1.01 |
| `append` (from the receipt) | 4.23 s | 11.7% | 0.87 |
| parquet decode + residual | ~0.84 s | 2.3% | — |

Inside shaping:

| region | wall | % of validate | eff. cores |
|---|---:|---:|---:|
| `shape.after_chain` | 5.91 s | 16.5% | 0.81 |
| `shape.chunk_loop` | 5.27 s | 14.7% | 0.78 |
| `shape.loop_to_chain` (`finish_optional`) | 3.91 s | 10.9% | 1.16 |
| `chain.resolve_endpoint_surrogates` | 2.65 s | 7.4% | 1.11 |
| `chain.validate_staged_details` | 1.19 s | 3.3% | 1.00 |
| `chain.assign_surrogates` | 0.29 s | 0.8% | 0.82 |

**Read the right-hand column.** Nine stages, each at roughly one core. Parallelising any
one perfectly leaves eight untouched. That is why #1448 (1.46×), #1429 and an 8× load-
worker sweep (3.3%) all disappointed. It is also why a prefix sum on a 0.8% region
(#1464) cannot be a critical path; that issue is refuted at every scale measured.

Two standing rules from the same measurements: **do not take ratios from a small
fixture** (a 131k-identity fixture put `finish_optional` at 51% / 0.49 cores; the rung
says 10.9% / 1.16), and **the shipped `gf` cannot emit these scopes** — they are behind
`#[cfg(any(test, feature = "test-support"))]`, so every rung-scale attribution needs a
rebuild. Whether to expose them in release is decision D4 in §9.

### 3.2 Across the ingest receipt

`operation_timings` per rung, `93c041df`. **Measured**:

| rung | `append` | `seal` | `publish` | `resume` | attributed | ingest wall |
|---|---:|---:|---:|---:|---:|---:|
| S18 | 4.2 s | 26.6 s | 1.7 s | 0.25 s | 32.7 s | 38.3 s |
| S19 | 8.5 s | 51.4 s | 3.4 s | 0.49 s | 63.7 s | 75.4 s |
| S20 | 17.2 s | 99.8 s | 6.8 s | 0.99 s | 124.8 s | 149.2 s |
| S22 | 69.2 s | 386.8 s | 28.2 s | 3.98 s | 488.2 s | 593.1 s |

### 3.3 What the receipt does not attribute (measured)

Ingest wall minus every operation the receipt times. **Measured** on both ladders:

| rung | `93c041df` unattributed | share | `f80f69fe` unattributed | share | µs/edge (`f80f69fe`) |
|---|---:|---:|---:|---:|---:|
| S18 | 5.6 s | 14.5% | 5.7 s | 13.7% | 1.35 |
| S19 | 11.7 s | 15.5% | 11.5 s | 14.5% | 1.37 |
| S20 | 24.4 s | 16.3% | 25.0 s | 15.8% | 1.49 |
| S22 | 104.9 s | 17.7% | 107.4 s | 16.7% | 1.60 |
| S24 | — | — | 466.2 s | 16.9% | 1.74 |

Rev 10 said `validate` was "97.7% attributed". That is true of `validate` driven
directly; it is not true of the ladder's ingest phase, where **14–18% of the wall is
outside every timed operation**, and that term grows faster than the edges (S18→S24:
1.35 → 1.74 µs/edge, exponent ≈ 1.06). The candidate is the `register-parquet` source
copy plus session bookkeeping, neither of which the receipt times. This is falsifier F6,
now answered from evidence: it is a bookkeeping gap *and* a mild super-linear term, and
it is not the S24 bend on its own (§8).

## 4 · The merge queue is part of the plan

Rev 10's baseline excluded twelve open pull requests. As of `origin/main` at `d16c3791`
(2026-09-18 evening) **eight of them have landed**: #1459 (worker-local partition loads),
#1468 (#1460 counter isolation), #1444 (no re-read after encode), #1458 (−33% validate
user CPU), #1469 (#1439 cut-sweep measurement), #1470 (effective cores per region),
#1461 (#1455 property-free pipeline), #1450 (inventory hashed once). Five remain, all
MERGEABLE:

| PR | what | state |
|---|---|---|
| #1453 | publish the adjacency CSR with the generation | CI red at 20:39Z (`graphforge_storage_test` exit 101); **#1466 depends on it** — #1466 alone hangs S18 5/5 |
| #1466 | derive concurrency from the machine; drop the RSS growth gate (#1463) | held out of the queue until #1453 lands, by its author |
| #1474 | carry process CPU on every construction operation (#1462) | open |
| #1451 | reprice cache-release budget 64 MiB → 1 GiB (#1442) | open |
| #1415 | attribute the transient peak (#1393 phase one) | open |

**Consequence for every number in this document:** all of §2–§3 was measured on a
`main` that lacked #1458, #1444, #1450 and #1461. They reduce CPU and bytes. The next
clean S18–S22 ladder is the real baseline, and until it runs, every CPU and byte figure
here is an upper bound on the current tree. Do not run that ladder while the five PRs
above are still landing — it would measure a baseline that is about to change.

## 5 · The work, in dependency order

The order below is the plan's proposal. Where it departs from the sequencing recorded
on #1456 (comment 5724135325, "foundation before optimisation") it says so, and the
departure is decision D2 in §9. What does *not* depart: the instrument comes first, the
in-flight work-reduction PRs land, #1439 precedes any `SortExec` adoption, and the
engine swap is judged on a measurement.

### Step 0 · Land the queue, then baseline once

In dependency order: #1453 (fix the red test) → #1466 → #1474 → #1451 → #1415. WIP
limit: no new performance PRs until the open count is ≤ 4. Then one clean S18–S22
ladder on a quiet host. Nothing in steps 2–5 should be scoped from the numbers in §2–§3
once that ladder exists.

### Step 1 · Make the instrument report three numbers, not one

#1470 and #1474 give effective cores per region and per operation. That is the number
#1387 quotes; it is not the number #1387 budgets, and the module's own doc says why. The
A/B for any engine change needs three separate figures, all engine-agnostic (measured
at phase and operation boundaries, reaching into no partitioner internal):

1. **CPU-busy fraction** — process CPU / (wall × threads available). What #1470 reports.
2. **Off-CPU wait on the critical thread** — wall the phase spent with no runnable
   thread of its own: I/O wait, fsync, lock. `pressure_io_seconds` at rung scope is the
   coarse version already in the evidence (6.3% at S22 and S24); the phase-scoped version
   is what is missing.
3. **Achieved parallelism per stage** — max concurrent workers actually on-CPU inside a
   region, from the worker pool, not derived from CPU/wall.

Known-positive validation is not optional: force one worker and confirm (1) reports ~1
core and (3) reports 1; block on an `fsync` and confirm (2) sees it. #1449 is the
standing reason: an instrument that silently under-reports is worse than none.
**Until (2) and (3) exist, no "serial fraction" number should appear on a KPI tile.**

### Step 2 · Bytes first, targeted at an order of magnitude

Engine-independent, two-way doors, and each also moves S25's only refusal. The
attribution to rank them exists (**measured**, S24, `application_io`):

| phase | read | write | share of construction I/O |
|---|---:|---:|---:|
| `shape_consume_reauthentication` | 196.3 GB | 115.7 GB | 60.4% |
| `encode_write_postwrite_authentication` | 71.0 GB | 13.4 GB | 16.3% |
| `append_merge` | 0 | 50.0 GB | 9.7% |
| `recovery_reauthentication` | 45.4 GB | 0 | 8.8% |
| `cas_install_read_write` | 11.4 GB | 11.4 GB | 4.4% |

`shape_consume_reauthentication` is the merge tree plus the Parquet writes — the core
shaping work, not a redundant check despite the name (its `write_bytes` is
`merge_written_bytes + parquet_write_bytes`). The question is how many passes shaping
needs. `recovery_reauthentication` reads 45 GB at S24 for zero writes;
`encode_write_postwrite_authentication` reads 71 GB back; the `register-parquet` source
copy is a full extra write and read of the input (§6). Owner: #1194 and its children
(#1384, #1393, #1418, #1442).

### Step 3 · Work removal, re-profiled after step 0

Target: the CPU budget in D1. #1458 (landed, −33% validate user CPU) is the shape of
the work: per-record hot loops, the `BulkNodeRow`-per-endpoint allocation (8.4M at
S18), normalization allocation churn (#1472). On this host every microsecond removed on
sort-shaped work is worth 1/7.3 of the floor (§2.3); no item in steps 4–5 has that rate.

### Step 4 · Overlap before decomposition

Nine roughly equal stages at ~1 core each pipeline to the longest stage
(`after_chain`, 16.5%): a theoretical ~6× on wall, realistically 2–3×, with no engine
swap and nothing on #1456's "stays ours" list touched. This is #1456 work item 3
("overlap input preparation with append… needs no evidence change") and #1472 axis 2.
Start with two stages — normalize ∥ append at S19 quiet — and gate on ≥ 1.1× (F5); if it
fails, pipelining is not the cheap win claimed here and engine-first regains its case.
Queue governed by bytes, not batch count; one process-wide CPU budget; the memory
admission budget stays separate.

### Step 5 · The engine swap, gated on throughput

#1439 (skew) stays a precondition. #1465 (encode seam spike) stays time-boxed — but its
go/no-go must be a floor criterion, not a seam criterion (D3): **proceed only if achieved
parallelism (step 1, number 3) in shaping at S20 rises by ≥ 2×** in the prototype,
alongside bytes copied and recovery correctness. A passing seam test alone would
green-light a near-one-way engine swap on a criterion unrelated to the floor. What
DataFusion can parallelise is sort/spill/partition execution — `chunk_loop`,
`after_chain`, `loop_to_chain`, about 42% of `validate`, and `loop_to_chain` is already
at 1.16. Normalization, `validate_staged_details`, surrogates, canonical encoding and
append are on the "stays ours" list and the swap does not touch them.

### Step 6 · Hardware last, as a reference-host demonstration only

A 32-core box changes neither bytes per edge nor the S24 drift; it makes the number
appear on a demo. Reject as a primary lever. Accept one run on a defined reference host
once steps 2–4 make the per-resource budgets pass on `OVHC-AGENCY`.

### What stays from rev 10 unchanged

Phase 0b (#1463, PR #1466) is still the precondition for any instrument measuring the
engine rather than a two-worker facade. #1441 may dissolve into Arrow's columnar
representation — verify that it did rather than build the fix twice. #1460 is landed
(#1468).

## 6 · Nothing sacred

Reopened by the maintainer's ruling; each to be argued on measurement, not reversed by
default. Rev 11 adds three rows rev 10 lacked.

| invariant | what it costs (measured) | the question |
|---|---|---|
| The `register-parquet` source copy (closed 2026-09-17 as durability policy) | a full copy of the input in the transient peak and on the serial path; probably the bulk of §3.3's unattributed 14–18% | does a digest pinned at registration and re-checked at read buy "a user editing the source cannot affect an in-flight import" as a *check* instead of a copy? |
| `shape_consume_reauthentication` | 60% of construction I/O at S24 | how many passes does shaping need? |
| Read/write amplification | 5.3× / 8.0× physical over logical at S24 (harness units) | a log-structured store on object storage measures 3–5×; what accounts for the rest? |
| Durability barrier count | 155,314 fsyncs at S22; 623,895 at S24 | the design, not only the count: 8 per spill on one directory inode (#1452); repricing is #1451 |
| **`publish`** *(new)* | 28.2 s at S22 at 0.58 effective cores; 114.3 s at S24 | at the floor S22's whole ingest is 67 s; publish alone would be 42% of it. Nothing in any phase touches it |
| **The identity B-tree** *(new)* | #1387 workstream 5: "the only component with a size-dependent constant" | candidate for the S24 drift; unmeasured since the ruling |
| **The fsync-per-spill barrier model** *(new)* | see above | count is listed; the barrier *design* is not on any issue |
| The 2% serial budget | — | written against the old baseline; needs the step 1 instrument before any number replaces it |
| The RSS growth gate (removed 2026-09-18) | refused S25 at 757 MiB against 4 GiB | settled: deleted from admission, fraction still reported; revisit once lanes exist. #1473 argues the rung peak it gated on tracks bytes moved, not memory held |

**Not on this list:** the open-time content sweep. Mutation testing with it removed shows
a corrupted `topology/edges` Parquet accepted with queries returning results. "Nothing
sacred" is licence to re-argue cost, not to trade correctness for throughput. This belongs
in the acceptance criteria, not in a note.

## 7 · Cautions that must survive into any design

1. Worker count, execution partitions and durable partition layout are three different
   numbers. Hash or round-robin repartitioning does not replace recorded UUID splitters.
2. Streaming does not eliminate blocking stages. Sorts must accumulate or spill.
3. Keep a separate total-memory admission budget. A framework memory pool accounts for
   registered consumers, not every flowing batch or writer buffer.
4. CPU work in async needs explicit admission and cooperative cancellation.
5. Retain GraphForge's filesystem authority. `tokio::fs` removes no fsync cost.
6. Coordinate pool concurrency so no two schedulers each claim all eight cores — and, per
   the agent brief, cap build jobs the same way.
7. *(new)* **Hash-shaped and sort-shaped work do not share a core budget** on this host
   (§2.3). Size worker pools for the sort-shaped ceiling (7–8), not the thread count.
8. *(new)* **Do not move work across the ingest boundary to move the metric.** "Edges/s
   during ingest" can be raised by relabelling (integrity checks behind `verify`, CSR
   build into publish). Pair the floor with the lifecycle-time budget in D1.

## 8 · Open unknowns

| unknown | why it matters | cheapest experiment | status |
|---|---|---|---|
| Device ceiling of the md RAID1 under the ingest fsync pattern | sets the byte multiplier in §2.2 (F1) | `fio` mixed rw with fsync, quiet host, ~2 min | open |
| Is the rung peak RSS page cache? | if yes, "throughput rises with scale" is a cached-regime result and S24 is the first honest rung (F3) | cgroup `memory.stat` during one S22 rung | open, #1473 |
| **What bends at S24** | a floor forbids drift | already in the evidence: per edge S22→S24 (`f80f69fe`, *measured*): `seal` 6.42 → 6.84 µs (+6.6%), `append` 1.07 → 1.20 (+12%), unattributed 1.60 → 1.74 (+9%), `publish` 0.43 → 0.43 (flat) | **answered in part**: the bend is spread across three terms, not located in one |
| What the 14–18% unattributed ingest wall is | it is the fastest-growing per-edge term (§3.3) | time `register-parquet` and session bookkeeping in the receipt | open |
| Off-CPU wait per phase | distinguishes I/O idle from sequential dependency (§2.4) | step 1, number 2 | open |
| Effective parallelism at 8 balanced workers | fallback if lanes disappoint | rebase #1429, force `threads = 8`, seal wall at S19 quiet | open |
| #1442 wall gain on a quiet host | fsync count measured (−65%); wall never measured quiet | `gf-s26plan/ab_s18.sh` when QUIET, 4 min | open |
| Ladder-scale confirmation after #1459 and the eight landed PRs | every figure here is pre-#1458 | step 0 | blocked on the queue |

## 9 · Decisions this plan needs from the maintainer

These are recorded as proposals because each reverses or restates something the
maintainer decided. The plan stops on them rather than deciding.

**D1 · Restate the floor as five per-resource budgets that jointly imply it.** The floor
stays 1,000,000 edges/s at every scale; that is #1387's requirement and this plan does not
change it. As a *statement* it is unfalsifiable on the only designated bench host in both
directions: at the small end a pure rate fails on fixed cost, at the large end it is
byte-bound 2.4–18× on this disk. The proposal is to keep the ambition and give it
budgets the code can be held to individually, each derived from §2:

| budget | proposed value | derivation |
|---|---|---|
| CPU per edge, single-thread-equivalent | **≤ 5 µs** | sort-shaped ceiling 7.3 cores (§2.3) ÷ 1.3 design margin ≈ 5.6; today 8.13 *(est.)* |
| Physical I/O per edge, harness units | **≤ 1 KB** | today 12.1 KB at S24; at 1 KB the floor needs ~1 GB/s combined, so this budget alone still leaves a device gap of ~1.5× against the harness's ~660 MB/s — bytes and device are both on the table |
| Achieved parallelism (step 1, number 3), S20 | **≥ 6 of 8 physical cores** on the critical path, off-CPU wait ≤ 10% of ingest wall | today ~1 core and ~8% idle |
| Scaling exponent, S18→S26 | **≥ 0.98 on wall and on CPU/edge** | the "every scale" clause made measurable; today 1.009 on wall S18→S24 and +7.8% CPU/edge S22→S24 |
| Fixed cost at S18 | **≤ 5 s** for begin + resume + publish | today ~2.0 s; gives the small end a term |

Plus a **lifecycle-time budget** beside the ingest rate, so the rate cannot be met by
relabelling (caution 8).

**D2 · Sequencing.** #1456 comment 5724135325 ruled foundation-first and "do not open new
work-reduction PRs in that class ahead of phase 1". The rationale it recorded — the
surrogate chain as the 48–50% serial remainder, and hand-parallelising seal being work
the refactor deletes — did not survive the rung-scale attribution (#1464 refuted at every
scale; the serial work is in the "stays ours" list). §5 proposes bytes and work removal
ahead of and alongside the seam spike, with overlap (step 4) before decomposition. This
reverses part of that ruling and needs the maintainer's yes or no.

**D3 · #1465's go/no-go.** Proposed: achieved parallelism in shaping at S20 ≥ 2× in the
prototype, in addition to the seam criteria. Without it, the spike's pass green-lights
a one-way engine change on a criterion unrelated to the floor.

**D4 · Diagnostics scopes in release builds.** The shipped `gf` cannot be asked where its
time goes; every rung-scale attribution needed a rebuild. Expose them (behind a flag) or
accept that every attribution claim requires a custom binary.

**D5 · The gate the code implements.** `crates/graphforge-storage/benches/m6_storage_io.rs`
sets `INGEST_FLOOR_EDGES_PER_SECOND = 15_000.0` with the comment that a gate at
1,000,000 "is switched off within a week" and a rule to ratchet it up as each workstream
lands. #1387's acceptance box "fails on the floor at any measured size" is therefore
untrue in code today. Proposed: keep the ratchet, make it explicit in the epic
(15k → 250k → 500k → 1M, raised in the PR that wins each gain), and retire the acceptance
wording that the code cannot honour.

## 10 · Standards that do not move

TCK 3,897 scenarios green (`cargo test -p graphforge-api --test bdd`) · G500 S26 admits
*and completes* · GDC suites green · every feature retained · fail-closed publication ·
no silently accepted corruption.

**Explicitly spendable:** recoverability-without-restart · eager verification whose refusal
is duplicated at a consuming boundary · in-memory representations, buffer sizes, process
boundaries, phase counts · any constant priced for different hardware · wire formats,
with a version bump, a read path and the determinism suite green · byte-identical
intermediates, retired by ADR 0038 in favour of semantic equivalence at the publication
boundary.

## 11 · Reversibility

| commitment | door | note |
|---|---|---|
| byte-cut PRs (#1444, #1450 landed; #1451, #1418 open) | two-way | land freely |
| unpin resource policy (#1466) | two-way | sequenced behind #1453 |
| removing the RSS growth gate | two-way | watch the reported fraction |
| encode seam spike (#1465) | two-way | only with D3's criterion |
| replacing sort/spill/memory accounting with DataFusion in shaping | **one-way in practice** | rewrites the evidence, spill lifecycle and admission budget; deserves the heaviest gate on this page, and rev 10 gave it the lightest |
| restating the floor (D1) | looks two-way, is reputational | propose, do not decide |

## 12 · Evidence

- **Rungs:** `/home/ubuntu/graphforge-ladder/s18-s22-93c041df-evidence/` (S18–S22, quiet
  host, per-operation CPU; the source of §2.1, §2.4, §3.2, §3.3 left columns);
  `clean-f80f69fe-evidence/` (S18–S24; the only S24; §2.1 second table, §2.2, §3.3 right
  columns, §8); `s18-403fc02a-instrumented-evidence/` and #1387 comments 5734006616 /
  5734073771 / 5734143331 (§3.1).
- **F2 probe:** `/home/ubuntu/gf-redteam-scratch/f2-sha256.log`, `f2-sort-zstd.log`,
  `f2-zstd-long.log`, scripts beside them.
- **Continuation log:** `/home/ubuntu/gf-ladder-continue-f80f69fe.log` (S25 refusal).
- **Quiet-host guard:** `~/.claude/gf-quiet-host.sh`; never `pgrep -f` to detect a build.
- **Host:** OVHC-AGENCY, Ryzen 7 3800X (8 cores / 16 threads), 125 GB RAM, ext4 on md
  RAID1. benchexec double-counts I/O on md RAID1 (reads ~2×, writes ~3×). Where the host
  was contended, CPU-seconds and phase fractions are reliable and absolute wall is not.
- **Tracker:** #1387 (floor) ← #1462, #1456, #1448, #1455, #1433 · #1456 (foundation) ←
  #1463, #1439, #1465, #1464, #1441, #1445, #1452, #1416, #1460 · #1194 (amplification)
  ← #1384, #1393, #1418, #1442 · #1388 (query) ← #1446, #1449 · #735 (scale &
  interchange) ← #745 → #900, #1194, #1436.
- **Related documents:** [`perf-g500-ladder.md`](perf-g500-ladder.md) (the ladder),
  [`g500-certification.md`](g500-certification.md) (the S26 claim),
  [`integrated-storage-1194.md`](integrated-storage-1194.md) (#1194's evidence),
  [ADR 0038](../adr/0038-determinism-at-the-publication-boundary.md) (determinism at the publication boundary).
