# Ingest-floor plan #1478, revision 14 — archived body (2026-09-21)

This is the verbatim body of GitHub issue
[#1478](https://github.com/CurateLabs/graphforge/issues/1478) as it stood at
revision 14 on 2026-09-21, immediately before the issue was rewritten as a
focused work order. It is preserved because the plan's measured tables,
revision corrections, cautions, research routing and evidence index are the
only remaining record for ladder runs whose on-disk evidence was removed from
OVHC-AGENCY on 2026-09-21 (see #1530).

Nothing below is maintained. The live plan is the issue body; decisions D1–D5
and the cautions in §7 were carried into the rewritten issue. Historical
observations remain attributed to their original builds. Filesystem paths
under `/home/ubuntu/` named in §12 no longer exist unless #1530 restored them.

---

# Streaming construction plan — the #1387 floor, per resource

Plan of record and close gate for the ingest-throughput workstream. #1387 owns the floor and #1456 owns the construction foundation beneath it; this issue closes only after their acceptance outcomes are met. **#735 remains M5's canonical closing tracker.** This plan does not close the milestone.

## 0 · How this plan stays current

**This body owns the strategy, ordering rationale, decisions and material findings. GitHub owns execution state.** Read the linked issues' current specifications, native parent/sub-issue and **Blocked by / Blocking** relationships, linked PRs and acceptance evidence before choosing work. The live entry points are in §13. Section 5 explains why prerequisites exist; it is not a list of remaining work.

- **Routine progress needs no plan edit:** issue closure/reopening, PR creation/merge, CI results, a prerequisite being satisfied, or a narrower child issue within the agreed scope. Record execution evidence on the owning issue/PR and maintain native relationships there. Do not mirror status, remaining counts, current `main`, queue contents or completed-step checkboxes here. Preserve satisfied relationships for traceability.
- **Edit this plan when the order of operations changes**, including a new/removed prerequisite or a conditional prerequisite becoming necessary, or when a substantial finding changes a decision, assumption, budget, scope, correctness contract, measurement interpretation or closure criterion. Update the affected rationale and issue specifications/native relationships together. Ordinary movement through the existing order does not change that order.
- **Keep each substantive edit local:** cite its evidence and explain what changed and why in the issue history. No revision-number/title bump or progress-only rewrite is required. Routine new measurements live with their owner; incorporate them here when they materially change the argument.
- **Select work from the live graph:** follow native children recursively, check blockers and their acceptance evidence, and respect repository WIP/merge policy. Step numbers are references, not a serial queue. A closed prerequisite is not work to repeat; a reopened prerequisite must be assessed again. If the graph and rationale conflict, resolve the discrepancy before dependent implementation.

**Evidence convention.** The measurements and historical corrections below are attributed to their recorded build, workload and date; they do not describe the latest tree. A number is **measured** (reported by a run) or **estimated** (derived from measured values); keep those in separate tables. Preserve historical evidence when new results arrive. Current baseline/qualification evidence belongs to the owning issue, and neither an old measurement nor issue closure alone proves the full-ingest floor.

This issue is the editable plan home established by #1479, superseding the repository plan from #1475. Earlier revision names remain only as provenance for corrected arguments.

<details>
<summary>Historical decision corrections (rev 12–14; not an execution-status feed)</summary>

### Rev 14 — 2026-09-21: baseline state, evidence loss, continuation rule

Maintainer-authorized, 2026-09-21. No decision in §9 changes. One prerequisite is added and one measurement interpretation is corrected:

- **Step 0 gains a prerequisite: the #1526 fix.** Two clean single-run ladders on 2026-09-21: `ab1a713e` (contains #1452 and the #1466 cohort) completed S18–S22 at **155,093 edges/s at S22 (432.7 s, 0.99 effective cores)**, a 6.45× remaining gap; `4fbfe84c` (adds #1519) **fails S22 deterministically** (`control record exceeds bound`, #1526). One ladder per tree cannot rank the two trees (S18/S19 differ by 13–14% one way, S20 by 19% the other, and `ab1a713e`'s S20 ran at 0.87 effective cores). Step 0 runs on a tree containing both #1452 and the #1526 fix. Evidence: #1387 comment 5765933707.
- **Evidence loss.** `/home/ubuntu/graphforge-ladder` was deleted and recreated at 18:48:27 UTC on 2026-09-21. Every rung directory §12 cited under that path is gone, including the only S24 rung; the tables already copied into this plan and #1387 are now the record for those runs. Retention rule: #1530; the test suite that shares that path: #1529. Step 0 evidence is archived under #1530 before it is reported.
- **Continuation rule.** After each landed repair, recompute the remaining gap from a complete-ingest measurement on the integrated tree before starting or continuing structural work. Structural A/Bs run against a frozen baseline SHA under the owning issue's predeclared gate (#1448: ≥10% whole-ingest median at S18 and S20, identical digests). Shaping alone is bounded below 1.92× (48% of validate), so #1448 is not scoped as a floor-closing change.
- **The explicit-exchange, owned-artifact pipeline** proposed and red-teamed on 2026-09-21 is routed to #1509 as a hypothesis row (§8), not adopted as a second roadmap. Its admission-manager, manifest and publication-protocol components need their own ADRs before any experiment includes them. #1448 records the single structural hypothesis it will test and its revisit triggers.

### Rev 13 — historical corrections

Maintainer-authorized corrections, 2026-09-19:

- **Withdraw CPU ≤ 3.54 µs/edge as a derived necessity.** The N-process throughput speedup is neither CPU/wall nor a ceiling for one redesigned ingest. #1476 must not enforce that derivation.
- **Replace the circular validation with matched-boundary accounting.** CPU and wall from the same execution satisfy an identity; reconstructing wall from a CPU estimate derived from that wall is not independent validation (§2.6).
- **D3 stays on encode.** #1465 decides seam viability; a pass permits a bounded shaping experiment, not the migration. No ≥2× parallelism acceptance condition applies to either surface.
- **Publication has an owner: #1481**, a native sub-issue of and blocker for #1387. Its historical ~0.42 µs/edge is a scale-dependent operation cost, not fixed startup overhead. Re-measure after CSR publication landed and budget complete ingest.
- **#1480 separates synchronization, scheduling and CPU effects.** Loop devices or logical volumes backed by the same array do not isolate that array. Diagnostic durability ablations are not production throughput evidence.
- **#1477 distinguishes CPU concurrency, scheduler activity and throughput speedup** with explicit scopes and separate validation. Epic summaries and gate specifications are updated with these decisions.
- **Dependency follow-up:** §5 records the native execution and closure links. #1480 is a child of #1387; blocking relationships now match the plan, with conditional/non-dependencies explicit.

The 1,000,000 edges/s floor, correctness contracts and #735's role are unchanged. Historical observations below remain attributed to their original builds.

### Rev 12 — historical corrections; rev 13 supersedes its CPU-budget conclusions

Three things happened after rev 11 merged. The maintainer ruled on all five decisions (2026-09-19; §9). Two commissioned measurements — F1 (the array under the ingest request pattern) and `perf stat` plus an N-way scaling probe on the real `validate` — **refuted the two premises rev 11's ordering stood on** (§2.2, §2.3); each measurement records the decision rule it was given and why that rule was withdrawn before the runs finished. And an independent cross-model review found errors that survive both; its five checkable arithmetic claims were re-derived and all hold (`verification-of-astra-review.md`).

| rev 11 said | rev 12 says | settled by |
|---|---|---|
| §2.2: "bytes bind the floor harder than cores at S24+", "bytes must fall by an order of magnitude before the floor is a CPU question", "the ideal-cache case is already 2.4–3.4× over", "~18× over at S24 ratios", "~13 TB of physical I/O into S26's 18 minutes" | **withdrawn.** Every one of those ratios divided by ~360 MB/s, which is the harness's *achieved whole-rung rate in harness units*, not capacity. The array delivers ≥ 2.1 GB/s sequential O_DIRECT read, 0.9–1.15 GB/s write, 1.62 GB/s under the derived ingest pattern at 16 threads, ~1.8 GB/s buffered. Whether bytes bind is **open**: ingest-phase demand has never been measured in device units (§2.2) | F1, **measured** |
| §2.3: "sort/decode-shaped work tops out at 7.3–8.0 cores on this host; shaping is sort-shaped; that ceiling is below the 8.13 the floor needs" | **withdrawn as argued; replaced by a measurement of the code.** `validate` is neither sort- nor hash-shaped (IPC 1.92 against sort 0.76 and SHA 1.27; DRAM fills 0.36/k-instr against sort's 1.67; retire-token stalls 0.99% against 26.3%; store-queue stalls 6.52%, its own signature). It runs on **0.93 cores**. Sixteen copies of the real binary deliver **3.54×**, eight deliver 3.27×; the proxies said 7.27–15.07× — over by 2–4×. Rev 12 transferred the CPU-cut conclusion to that number; rev 13 withdraws that transfer (§2.3) | perf stat + probe, **measured** |
| "the majority of the work scales below 8.13×, therefore the CPU cut is mandatory" | an invalid inference: 73% at 7.3× plus 27% at 15.07× aggregates to `1/(0.73/7.3 + 0.27/15.07)` = **8.48×**, above 8.13×. Dropped; rev 12's attempted re-derivation from 3.54× is also withdrawn in rev 13 | external review, verified |
| "71% is `seal`'s share of ingest CPU" | **68.9%**: 376.19 CPU-s ÷ 545.7 *estimated* ingest CPU-s. The 71% (70.9%) was `seal` **wall** 386.79 over ingest CPU — the wall/CPU conflation P0.3 was raised to condemn, reproduced inside the correction to it (§2.4) | external review, verified |
| §5 step 4: "pipeline to the longest stage (`after_chain`, 16.5%)" | on the plan's own table the longest stage is `canonical_encoding` at 18.0% (§5 step 5) | external review |
| D1: "as a statement the floor is unfalsifiable on this host in both directions" | wrong. An eligible ingest completing below 1,000,000 edges/s falsifies it. Difficult at both ends is not unfalsifiable (§1) | external review |
| D1 budgets ≤ 5 µs, ≤ 1 KB, ≥ 6 of 8 cores, exponent ≥ 0.98, fixed cost ≤ 5 s | rev 12 decisions, **superseded where corrected in rev 13 §9 D1**: its 5.0-versus-3.54 comparison conflated CPU concurrency and throughput speedup; the 1 KB "gap" was against an achieved rate; ≥ 0.98 passes an exponent of 2.0; 5 s exceeds S18's entire 4.19 s allowance | maintainer, 2026-09-19 |
| §5 step 1: "CPU-busy fraction — process CPU / (wall × threads available). What #1470 reports", validated by "force one worker and confirm it reports ~1 core" | **the spec was wrong; the module is right.** That definition is a fraction (one worker on sixteen threads gives ~1/16, not ~1), contradicts its own known positive, and misdescribes #1470, which correctly computes `cpu_nanos / wall_nanos`. Implemented literally it would have turned correct code into a fraction and then failed its own validation. Fixed in §5 step 2; tracked on #1477 | code read; #1477 |
| measured/estimated mixing and promoted hypotheses: "S24 is the first uncached rung" (§0); the source copy "is" the unattributed 14–18% (§3.3); old figures are "upper bounds" on the current tree (§4); "wall with no CPU" and PSI `some` read as whole-path idle (§2.4); a digest "buys the same contract" as the source copy (§6); "size pools for the 7–8 ceiling" (caution 7) | each relabelled as hypothesis, expectation, CPU-time deficit, semantic trade or measurement-sized pool, in the section named | external review |
| *(not named in rev 10 or rev 11)* | **fsync serialisation on the shared array** is a candidate constraint that is neither "bytes moved" nor "CPU per edge". The N-way probe cannot separate it from core contention; separating the two is this plan's next measurement (§2.3, §5 step 1) | F1 + probe, **measured** |

Nothing here changes #1387's floor.

</details>

## 1 · The floor

**#1387: 1,000,000 edges per second, sustained at every supported scale.** Binding, confirmed by the maintainer on 2026-09-18 (#1387 comment 5724050403): not retired, not deferred past v0.6.0, and no invariant is exempt from re-examination against it. S26 admission is a release claim about a billion-edge round trip; it does not close the epic.

The floor is one number and it is falsifiable: any eligible ingest that completes below it falsifies it. Rev 11 called it "unfalsifiable on this host in both directions" and meant *hard at both ends*: at the small end a pure rate is dominated by setup, at the large end it turns into a resource question. D1 uses resource measurements and operation budgets to plan against the directly tested deadline `T_ingest(E) <= E / 1,000,000`. The matched-boundary identity in §2.6 is accounting, not a predictive model or substitute acceptance gate. Section 2 separates historical observations, policy allocations and unresolved measurements.

## 2 · What the floor requires, per resource

Evidence limits in this section describe the cited runs. Consult #1387 and the owning issues for later measurements.

### 2.1 The reference rows

Four clean rungs on `93c041df` (measurement commits on `76462543`; the `gf` binary carried no diagnostics), quiet host, all ten phases passed. **Measured**, from `s18-s22-93c041df-evidence/`; the `seal` CPU column is the receipt's per-operation `cpu_ns`, which that branch carried and which is now on `main` (#1474):

| rung | edges | ingest wall | edges/s | `seal` wall | `seal` CPU | `seal` eff. cores | `seal` share of ingest wall |
|---|---:|---:|---:|---:|---:|---:|---:|
| S18 | 4,194,304 | 38.3 s | 109,581 | 26.6 s | ~23.9 s | 0.90 | 69.4% |
| S19 | 8,388,608 | 75.4 s | 111,258 | 51.4 s | ~47.3 s | 0.92 | 68.1% |
| S20 | 16,777,216 | 149.2 s | 112,470 | 99.8 s | ~94.8 s | 0.95 | 66.9% |
| S22 | 67,108,864 | 593.1 s | 113,152 | 386.8 s | 376.19 s | 0.97 | 65.2% |

(S18–S20 `seal` CPU is the receipt value rounded through the published effective-cores figure; S22 is the exact receipt value.)

The ingest-phase CPU figures rev 10 and rev 11 quoted are **estimated** — the rung-wide CPU/wall ratio applied to the ingest wall — and are kept in their own table:

| rung | build | ingest eff. cores *(est.)* | ingest CPU *(est.)* | µs CPU/edge *(est.)* |
|---|---|---:|---:|---:|
| S22 | `93c041df` | 0.92 | 545.7 s | 8.13 |
| S22 | `f80f69fe` | 0.88 | — | 8.46 |
| S24 | `f80f69fe` | 0.89 | — | 9.12 |

The S24 row exists only on `f80f69fe` (`clean-f80f69fe-evidence/`, quiet host, older build): **measured** 268,435,456 edges in 2,754.0 s = 97,469 edges/s, against 104,390 at S22 on the same build. Two ladders 5–8% apart on the same rungs, different builds and days; neither is an improvement claim over the other.

**Historical post-#1458 data point on `fa4dec51`.** The `perf stat` packet ran `validate` (append + seal, receipts on) on a stock release build of `fa4dec51` at S18, quiet host: **17.6 CPU-s** for 4,194,304 edges = **4.20 µs/edge, validate only** (N = 1 row of §2.3's probe table). The same operations at S18 on `93c041df` cost ≈ 27.6 CPU-s (≈ 6.6 µs/edge, from the table above). Same rung, same operations, both from receipt `cpu_ns`; different builds with #1458 (−33% validate user CPU), #1444, #1450 and #1461 in between. That is a −36% cut in validate CPU at S18, consistent with what those PRs claimed. It is **not** an ingest figure (no `publish`, `resume`, `register-parquet`, session bookkeeping) and not S22; the post-integration baseline (§4) is what turns it into one.

### 2.2 Bytes per edge: historical demand/capacity comparison

**Demand, logical.** Construction `application_io` totals from the rung JSON. **Measured**:

| rung | logical read | logical write | read B/edge | write B/edge |
|---|---:|---:|---:|---:|
| S22 (`93c041df` and `f80f69fe`, byte-identical) | 81.07 GB | 47.67 GB | 1,208 | 710 |
| S24 (`f80f69fe`) | 325.45 GB | 191.11 GB | 1,212 | 712 |

Logical bytes per edge are constant across S22–S24: 1.2 KB read, 0.7 KB written, before cache and before amplification. **Logical reads are not mandatory device reads** — a re-read served from page cache never reaches the array — so 1.21 GB/s of application reads at the floor is not a lower bound on device bandwidth. Rev 11 treated it as one.

**What the "360 MB/s" was.** The S20 projection's `native_capacity.observed_rates` (`rate_source: "completed_adjacent_rungs"`) divides S18's whole-rung physical bytes by S18's whole-rung wall: 20.99 GB / 54.9 s = 382 MB/s read, 231 MB/s write, in benchexec's units (≈ 2× on reads, ≈ 3× on writes on this md RAID1). Over S18–S24 the same quotient is 365–409 MB/s read and 231–325 MB/s write. It is an achieved average over ten lifecycle phases, in harness units. It was never a capacity, and rev 11's own caveat said so.

**Capacity, measured (F1, 2026-09-18, `/dev/md3`, quiet host, cache dropped before every run; `f1-device-ceiling/results.md`).** Parameters were derived from the S20 rung's `application_io` (51 KiB mean read, 43 KiB mean write, 59% reads by call, one fsync per 7 writes; offsets and concurrency are not recorded and were assumed, `PROVENANCE.md`):

| job | pattern | result |
|---|---|---|
| control, sequential O_DIRECT, 1 MiB | libaio qd32 / psync qd1 | read **2,097 / 2,126 MB/s**; write **897 / 1,148 MB/s**, sustained over 64 GiB |
| derived pattern, O_DIRECT, psync | 1 / 4 / 16 threads | 273 / 812 / **1,623 MB/s** aggregate; one thread is 90% inside `read()` at ≈ 0.28 ms per call |
| derived pattern, buffered, psync | 1 / 4 / 16 threads | **1,786 / 1,814 / 1,825 MB/s** — flat; fsync mean 0.18 → 1.45 → 5.88 ms, p99 0.63 → 12.8 ms; thread time inside `fsync()` 39% → 77% → 79% |
| append into fresh extents, buffered, fsync every 7 writes | 1 / 16 threads | 550 / 826 MB/s; 37% / 92% of thread time in `fsync()`; single fsyncs of 0.7 s / 1.4 s observed |

**What this licenses.** The old achieved whole-rung rate is not a demonstrated capacity ceiling. F1 does not establish that no GraphForge phase saturates storage or synchronization. The sequential ceiling is 5–6× the read rate and 3–4× the write rate rev 11 treated as capacity. Under the derived pattern the array delivers 1.6 GB/s O_DIRECT at sixteen threads and 1.8 GB/s buffered at any thread count. GraphForge's achieved rate — 697 MB/s combined at S22 in harness units, roughly 300 MB/s after the double-counting correction, averaged over the whole rung — sits in the neighbourhood of what the same pattern moves on **one** O_DIRECT thread (273 MB/s). Underfeeding is one hypothesis; these differently scoped observations do not identify the limiting phase or prove it.

**What F1 does not establish.** F1 draws no conclusion about whether bytes bind before cores, for three reasons it records: the plan's physical byte totals cover the whole rung lifecycle while its logical table is construction-only, so their quotient is not an ingest amplification factor; correcting only the numerator of a ratio whose two sides are both in harness units changes nothing; and logical reads are not device reads. The demand side — **device bytes the ingest phase must move per edge** — was not measured in these runs in a form comparable to the capacity numbers, and F1 could not measure GraphForge's I/O concurrency, offsets, cache setting or fsync placement either, because the rung JSON records calls and bytes and nothing about time inside them. The gap between 273 MB/s (O_DIRECT, one thread) and 1,786 MB/s (buffered, one thread) shows the access setting moves the answer by 6.5×, more than the block sizes do.

**The policy allocation (D1): ≤ 1.6 KB physical per edge, device units, ingest-scoped; serviceability remains unvalidated.** At the floor that is 1.6 GB/s, which the array delivered under the derived pattern at sixteen O_DIRECT threads (1,623 MB/s) and is below the reported buffered aggregate rate. Two caveats belong next to it. The budget has **no margin** against the O_DIRECT sixteen-thread figure and assumes I/O concurrency not established by these runs; buffered readahead gets there on one thread but is fsync-bound past one. And the number it is checked against — ingest-phase device bytes per edge — is absent from this evidence; producing it (phase-scoped device reads and writes, reconciled across the md device and its members, in the same run as the receipt) is part of step 0 in §5. The physical/logical ratios rev 11 quoted (4.35× / 5.21× at S22, 5.30× / 8.03× at S24, harness units, whole rung) are kept in §6 as the amplification question, not as a floor argument.

### 2.3 What this host delivers on GraphForge's own code (measured)

Rev 11 measured three synthetic proxies and transferred their SMT scaling to GraphForge by analogy ("shaping is sort-shaped"). The `perf stat` packet (`perf-stat-validate/results.md`, 2026-09-18 22:45–22:53 UTC, quiet host, `gf` built from `origin/main` at `fa4dec51`, release profile) ran the code instead, between the same two anchors.

**Instruction mix, pass A and the memory/dispatch passes.** **Measured**; S18 and S19 agree to within 3% on every ratio:

| quantity | SHA-256 ×16 (anchor A) | sort ×16 (anchor B) | `validate` S18 |
|---|---:|---:|---:|
| IPC | 1.27 | 0.76 | **1.92** |
| demand fills from DRAM per k-instr | 0.000 | 1.67 | **0.36** |
| retire-token stalls, % cycles | 0.22 | **26.3** | 0.99 |
| store-queue token stalls, % cycles | 0.014 | 0.87 | **6.52** |
| frontend-stalled cycles | 0.45% | 4.50% | **9.21%** |
| cache-miss rate (misses / references) | 0.47% | 26.5% | 10.5% |
| cores actually used, (user + sys) / elapsed | 15.8 | 15.7 | **0.93** |
| CPU-µs per accepted row | — | — | 4.58 (perf) / 3.95 (receipt, probe N = 1) |

`validate` is not a scaled copy of either anchor. It has the highest IPC of the three, a fifth of sort's DRAM traffic, a fortieth of sort's retire stalls, and a store-queue stall share 7.5× sort's and ≈ 450× SHA's: a high-IPC, store-heavy, moderately L3-missing, frontend-stalling mix of ≈ 35.5 k instructions per row, **executed on one core**. The 0.93 agrees with the S22 receipt's 0.92 independently. Zen 2 exposes no `stalled-cycles-backend` and no top-down metrics, so no "memory-bound %" exists on this host; the decision rule that wanted one was withdrawn for that reason and because such a percentage does not determine scaling anyway.

**Scaling of the real binary (`jobs/run-scaling.sh`).** N fresh S18 projects, cache dropped, N `validate` processes started concurrently; scaling(N) = N × T(1) / T(N), the definition rev 11 used for its proxies. Every process validated 4,456,448 rows. CPU is the receipt's `cpu_ns` from the stock build. **Measured**:

| N | wall | scaling | CPU-s per process | CPU-µs per row | `append` CPU / elapsed | `seal` CPU / elapsed |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21.60 s | 1.00× | 17.6 | 3.95 | 3.2 / 3.8 | 14.4 / 15.0 |
| 2 | 24.91 s | 1.73× | 18.3 | 4.11 | 3.4 / 4.1 | 14.9 / 17.5 |
| 4 | 31.59 s | 2.74× | 19.6 | 4.40 | 3.6 / 4.4 | 16.0 / 23.5 |
| 8 | 52.77 s | **3.27×** | 21.4 | 4.81 | 4.2 / 7.7 | 17.3 / 39.7 |
| 16 | 97.49 s | **3.54×** | 30.0 | 6.73 | 6.0 / 14.4 | 24.0 / 75.6 |

Rev 11's proxies at the same N: SHA-256 15.07×, zstd decode 7.96×, GNU sort 7.27× (at N = 8: 8.04×, 7.43×, 6.14×).

**What it says.**

- Sixteen independent copies of the real `validate` deliver **3.54 single-process equivalents** on this host; eight deliver 3.27, and the curve is flat between them. The proxies report 2–4× larger throughput scaling ratios under different workloads. This comparison describes concurrent independent imports, not CPU capacity or the speedup available within one import.
- CPU consumption rises in the historical receipt: 17.6 → 30.0 s/process from N=1 to N=16. Cache/memory contention, SMT sharing and spinning are hypotheses; these data do not establish the mechanism or an increased instruction count. The matched follow-up is in §2.3.1.
- The historical `seal` CPU/elapsed ratio falls from 14.4/15.0 to 24.0/75.6. Receipt CPU and batch wall have different scopes; subtracting them does not attribute waiting. §2.3.1 supplies actual scheduler and barrier observations.
- **The original probe alone could not separate scheduling from synchronization.** Its imports shared one md RAID1, while F1 measured a different derived storage workload. #1480 has now measured GraphForge’s own sync and scheduler intervals (§2.3.1); the physical-storage, journal and directory-lock mechanisms remain unseparated.
- It does **not** license a throughput for one parallelised ingest of one project: N processes on N inputs is not that, which these runs did not test. `gf` used 0.93 cores in every single-process run, so the "sixteen threads" question is still about parallelism the measured path does not have.

**What follows for the CPU budget.** The 3.54× observation is N-process throughput speedup, `N × T(1) / T(N)`. It is not effective cores `C/W`, and does not bound a redesigned single ingest. CPU duration per process also changes with concurrency; its underlying mechanism is not established. Therefore the ≤3.54 µs/edge budget and the claimed mandatory 2.30× cut are withdrawn. #1480 characterizes synchronization, scheduling and CPU costs; it does not convert a throughput ratio into a CPU budget. Set any new CPU design allocation from the integrated baseline and a representative candidate's complete ingest deadline, with an explicit margin and validation. #1476 retains valid interim regression gates without adopting the withdrawn derivation.

### 2.3.1 Completed contention attribution (#1480 → #1482)

**Measured**, 2026-09-19 on OVHC-AGENCY, using the preserved stock durable `fa4dec51` binary at S18 with matched input/cache/resource conditions. N=1, 8 and 16 configurations ran sequentially, with concurrent imports within each configuration. The merged report and evidence in §12 retain every run, control, failure, scope and residual.

| Mean seconds/import, observed scope | N=1 traced | N=16 traced b | N=16 traced c |
|---|---:|---:|---:|
| Main exec-to-exit wall | 23.200 | 99.954 | 102.207 |
| Main scheduled running | 18.058 | 29.674 | 30.853 |
| Main runnable delay | 0.084 | 2.410 | 2.438 |
| Main synchronization blocking | 2.500 | 52.105 | 54.697 |
| Main scheduler-state unknown | 0 | 0.078 | 0.070 |
| Shaping sync blocking (subset of main sync blocking) | 1.775 | 40.687 | 41.164 |

Other blocking completes the timeline; nested shaping time is not added again. Inclusive sync latency is separately 2.995 versus 53.788 / 56.278 s/import. Worker waits overlap the main thread, include idle pools, and retain substantial uncertainty (6.258 s/import at N=16b).

**Measured CPU, separate scope:** untraced whole-process `wait4` CPU rises from 19.852–20.204 to 36.157–36.167 s/import. The mechanism and instruction-count change are not established. **Derived accounting:** additional sync blocking contributes 49.605 / 52.197 s of the 76.754 / 79.008 s main-wall increase, versus 2.326 / 2.354 s additional runnable delay. These are not recoverable-savings estimates.

CPU, known-delay, pinned-contention and exact file/directory-sync controls pass. EMFILE, phase-incomplete pilots and failed initial analysis are retained. Missing-wakeup gaps remain unknown rather than assumed blocked; their cause is unresolved. Traced/untraced differences include run variability.

**Supported intervention: #1452.** Shaping executes 6,554 directory and 2,739 file sync calls/import. Test bounded durability-preserving barrier amortization with crash/refusal coverage and matched durable before/after evidence. No independent storage was available and no isolation or ablation was used; device service, ext4 journaling and directory locking remain unseparated. This is historical-build validate characterization, not current-main complete ingest, publication, a single redesigned parallel ingest, or a CPU/edge ceiling. Step 0, #1477 and #1481 remain open.

### 2.4 What "effective cores" does and does not say

`seal` is **68.9% of ingest CPU** at S22: 376.19 CPU-s measured, over 545.7 CPU-s *estimated* (rung-wide ratio). Rev 10's KPI tile said "71% serial"; rev 11 corrected the noun to "share of CPU" and kept the number, which was `seal` wall (386.79 s) over ingest CPU — 70.9% — a wall/CPU conflation inside the correction of one. Neither share is an Amdahl fraction: s = 0.71 on sixteen threads gives 1.37 effective cores, the measurement is 0.92.

The observed regions average approximately one effective core (#1387 comment 5734143331 — none above 1.16, none below 0.78, aggregate 0.92). That does not establish an inherent Amdahl serial fraction or rule out bursts of parallel execution. Effective cores below 1.0 is a **net CPU-time deficit**, not a measured idle duration: execution can alternate between parallel bursts and waiting, and CPU/wall cannot tell I/O wait, lock wait and sequential dependency apart. **Measured** at S22 (`93c041df`), per-operation receipt CPU:

| operation | wall | CPU | eff. cores | wall − CPU |
|---|---:|---:|---:|---:|
| `seal` | 386.8 s | 376.2 s | 0.97 | 10.6 s |
| `append` | 69.2 s | 61.2 s | 0.88 | 8.0 s |
| `publish` | 28.2 s | 16.3 s | 0.58 | 11.9 s |

Rung-scope PSI `pressure_io_seconds` ("some" stall) is 54.8 s of 863 s at S22 and 296.8 s of 4,724 s at S24, 6.3% both times. PSI `some` means at least one task was stalled on I/O; it is not whole-path idle. #1448's spike is the standing warning: its 1.46× came with CPU-seconds flat, which the epic's own comments say is *consistent with* overlapping fsync waits but does not prove it. The landed module (#1470, `concurrency_attribution.rs`) says the same in its doc comment: effective cores "conflates two different things, and that matters". #1462 asked for a serial fraction; what landed is effective cores plus an Amdahl inversion that returns `None` outside 0..=1 rather than fabricating a value. That is correct, and it is not the quantity #1387 budgets. §5 step 2 says what the instrument must report before an A/B is judged on it, and this time the spec agrees with the code.

### 2.5 Scaling, publication and startup

**Historical scaling:** on `f80f69fe`, S18 → S24 ingest wall 41.5 → 2,754.0 s for 64× edges gives exponent 1.009. Estimated CPU/edge rises 8.46 → 9.12 µs from S22 to S24. D1's wall exponent ≤1.02 is a diagnostic policy tolerance, not proof of the floor or of no measurable degradation; the direct per-rung floor and the epic's separate shape criterion remain.

**Publication is not fixed overhead.** Reported publication wall was 1.7 s at S18 and 28.2 s at S22 on `93c041df`, and 114.3 s at S24 on `f80f69fe`. Dividing by each rung's ingest allowance at 1M edges/s gives approximately 40.5%, 42.0%, and 42.6% (derived from displayed rounded timings). These historical observations support ~0.42 µs/edge over the observed scales; they do not prove the current-tree cost or a single-build scaling law.

If publication stays sequential at 0.42 µs/edge, the rest of ingest has ~0.58 µs/edge, or must process ~1.72M edges/s. That is conditional budget arithmetic. #1481 owns attribution and a justified repair or documented budget disposition, including the CSR cost introduced by #1453/#1446. It reuses #1477/#1480.

At S18, begin + resume + publish took ~2.0 s. Holding that sum constant at larger scales is invalid: publish and resume both grow with input. The ≤0.84 s S18 sum in D1 is a policy allocation for these named operations at S18, not a measured fixed-cost intercept or an all-scale bound. True startup overhead must be measured separately. The S22 unattributed residual (104.9 s) is larger than publication (28.2 s) and remains owned by the full-path attribution work.

### 2.6 Matched-boundary accounting and the actual deadline

For one execution with E edges, total CPU time C and elapsed time W measured across the same boundary:

```
W / E = (C / E) / (C / W)
```

The denominator is effective CPU concurrency (CPU-seconds per wall-second). It is neither maximum workers nor N-process throughput speedup. This is an algebraic identity, not independent validation or a forecast. Rev 12 reconstructed S22 wall from a CPU estimate already derived from that wall; its +1.1% discrepancy came from substituting a different scope's 0.93 for 0.92.

The predictive question is whether a concrete candidate meets:

```
T_ingest(E) <= E / 1,000,000
```

Budget registration, validation/append/seal, resume, publication and remaining orchestration using disjoint operation boundaries and measured residuals. Nested scopes must not be summed. Count overlap only when demonstrated by the dependency/timing evidence. If C and W cover complete ingest, publication and startup are already included; adding them again double-counts them. If the measurements cover only validate, account explicitly for all work outside validate.

CPU, device I/O, synchronization and memory measurements explain the budget and its risks; no identity establishes that they overlap enough to meet the deadline. Report the unchanged lifecycle boundary alongside ingest. #1476 protects comparable measured gains and preserves a separate direct 1M floor gate.

## 3 · Where the time goes (measured)

### 3.1 Inside `validate` at S18 rung scale

`gf import-session validate` driven directly on real S18 Graph500 data, release, quiet host, diagnostics scopes compiled in, 97.7% attributed (#1387 comments 5734073771, 5734143331), **pre-#1458**. **Measured**; wall 36.14 s, 0.92 effective cores:

| stage | wall | % of validate | eff. cores |
|---|---:|---:|---:|
| `shaping` | 19.88 s | 55.0% | 0.93 |
| `canonical_encoding` | 6.50 s | 18.0% | 0.88 |
| normalization (`normalize_import_*_chunk`) | 4.69 s | 13.0% | 1.01 |
| parquet decode + residual | ~0.84 s | 2.3% | — |

`append` (4.23 s, 11.7%, 0.87) is from the ingest receipt of the same rung, not from the scoped run, and is listed here rather than in the table so the table has one source. Inside shaping:

| region | wall | % of validate | eff. cores |
|---|---:|---:|---:|
| `shape.after_chain` | 5.91 s | 16.5% | 0.81 |
| `shape.chunk_loop` | 5.27 s | 14.7% | 0.78 |
| `shape.loop_to_chain` (`finish_optional`) | 3.91 s | 10.9% | 1.16 |
| `chain.resolve_endpoint_surrogates` | 2.65 s | 7.4% | 1.11 |
| `chain.validate_staged_details` | 1.19 s | 3.3% | 1.00 |
| `chain.assign_surrogates` | 0.29 s | 0.8% | 0.82 |

Nine stages, each at roughly one core; parallelising any one perfectly leaves eight untouched. That is why #1448 (1.46×), #1429 and an 8× load-worker sweep (3.3%) all disappointed, and why a prefix sum on a 0.8% region (#1464) cannot be a critical path. Two standing rules: **do not take ratios from a small fixture** (a 131k-identity fixture put `finish_optional` at 51% / 0.49 cores; the rung says 10.9% / 1.16), and these historical scopes were behind `#[cfg(any(test, feature = "test-support"))]`, so those rung-scale attributions needed a rebuild — the limitation D4 (§9) addresses. Every figure above predates #1458's −33%; the shares will have moved.

### 3.2 Across the ingest receipt

`operation_timings` per rung, `93c041df`. **Measured**:

| rung | `append` | `seal` | `publish` | `resume` | attributed | ingest wall |
|---|---:|---:|---:|---:|---:|---:|
| S18 | 4.2 s | 26.6 s | 1.7 s | 0.25 s | 32.7 s | 38.3 s |
| S19 | 8.5 s | 51.4 s | 3.4 s | 0.49 s | 63.7 s | 75.4 s |
| S20 | 17.2 s | 99.8 s | 6.8 s | 0.99 s | 124.8 s | 149.2 s |
| S22 | 69.2 s | 386.8 s | 28.2 s | 3.98 s | 488.2 s | 593.1 s |

### 3.3 What the receipt does not attribute

Ingest wall minus every operation the receipt times. **Measured** on both ladders:

| rung | `93c041df` unattributed | share | `f80f69fe` unattributed | share | µs/edge (`f80f69fe`) |
|---|---:|---:|---:|---:|---:|
| S18 | 5.6 s | 14.5% | 5.7 s | 13.7% | 1.35 |
| S19 | 11.7 s | 15.5% | 11.5 s | 14.5% | 1.37 |
| S20 | 24.4 s | 16.3% | 25.0 s | 15.8% | 1.49 |
| S22 | 104.9 s | 17.7% | 107.4 s | 16.7% | 1.60 |
| S24 | — | — | 466.2 s | 16.9% | 1.74 |

"97.7% attributed" is true of `validate` driven directly and not of the ladder's ingest phase, where **14–18% of the wall is outside every timed operation** and that term grows faster than the edges (1.35 → 1.74 µs/edge S18 → S24 on one build, exponent ≈ 1.06). *Hypothesis, not measured:* the `register-parquet` source copy plus session bookkeeping, neither of which the receipt times. Timing them is part of the instrument work (#1477).

## 4 · Integrated baseline contract and historical build provenance

Rev 10's baseline excluded twelve open pull requests; rev 11 recorded eight landed and five open. **All have landed.** The last five, in landing order on `main` after `d16c3791`: #1451 (`af85d5d0`, cache-release budget 64 MiB → 1 GiB), #1474 (`fa4dec51`, process CPU on every construction operation), #1415 (`65edeb9d`, transient-peak attribution), #1453 (`71c9e84e`, adjacency CSR published with the generation), #1466 (`6d473dbd`, concurrency derived from the machine; RSS growth gate dropped). #1453 landed before #1466, honouring the dependency measured on 2026-09-18 (#1466 alone hung S18 5/5). That implementation cohort ended at **`6d473dbd`**. The #1480 report subsequently merged as **`52f29ad0`** (#1482); it adds characterization, not product changes or a new integrated performance baseline.

**Consequence for the older ladder/phase tables.** Their CPU, byte and share figures were measured before the full integration of #1458, #1444, #1450, #1461, #1451, #1453 and #1466. Those PRs were built to reduce CPU and bytes and one of them changes the worker count, so the figures are *expected* to be above the current tree — an expectation, not a bound; integration can move work and concurrency either way. §2.1's historical `fa4dec51` validate-only point and §2.3.1's new characterization of that same binary are post-#1458 measurements; neither supplies the integrated ladder baseline. The historical point is 36% below the pre-#1458 figure at S18.

**The post-integration baseline is step 0 of §5; its evidence belongs on #1387.** Use a recorded integrated `main` SHA containing the implementation cohort ending at `6d473dbd`, S18–S22 on a quiet host (`gf-quiet-host.sh` prints `QUIET`), stock release build — per-operation `cpu_ns` is now in the shipped receipt, so no measurement branch is needed — and it should record what the previous ladders could not: ingest-phase device bytes (md device and members, phase-scoped) and time inside `fsync()`, if the instruments in step 1 and step 2 exist by then; if they do not, run it anyway and rerun the affected columns later. Scope steps 3–7 from the applicable integrated baseline, not the historical §2–§3 figures. Record the SHA, workload, resource limits and evidence on #1387; reuse qualifying evidence and refresh affected measurements when intervening changes invalidate comparability, without rewriting this baseline contract.

## 5 · The work, in dependency order

**D2 is approved (§9):** bytes and work removal run ahead of and alongside the seam spike, and overlap is tried before decomposition. What did not change from #1456 comment 5724135325: the instrument comes first among the code changes, #1439 precedes any `SortExec` adoption, and the engine swap is judged on a measurement.

### Native issue relationships — execution and closure

Native relationships carry the live prerequisites and their state. This table retains **ordering rationale regardless of completion**; it is not an open-blocker snapshot. See the issue's dependency panel before acting.

| Dependent issue | Prerequisite evidence / decision | Reason |
|---|---|---|
| #1476 — metric gates | #1477 | Scope-aware metric gates use the corrected instrument; existing valid regression checks remain in place. |
| #1465 — encode seam | #1477 | The engine A/B uses the permanent region instrument. |
| #1472 — normalization / input overlap | #1477 | Measure affected regions and full ingest with the corrected instrument. |
| #1452 — spill-barrier repair | #1480 | Attribute synchronization and scheduling before selecting an intervention. The #1482 findings support bounded shaping/spill barrier amortization with durability preserved; a supported no-change disposition is valid. |
| #1481 — publication workstream | #1477, #1480 | Reuse shared instrumentation and contention evidence for publication attribution, budget and any repair. |
| #1448 — partition/shaping decomposition | #1439, #1445, #1465, #1472, #1452 (all satisfied); **#1506, #1508** | Bound skew and buffers, establish the seam decision, and evaluate overlap/barrier work before decomposition. The encode seam was viable but a performance no-go (#1496), so it authorized no bounded shaping experiment. The library sorting/partitioning and scheduling evaluation now lives in #1504; #1448 takes its mechanism from the #1506/#1508 dispositions instead of building the same machinery by hand first. |

**Conditional and non-dependencies:** #1480 can use validated temporary diagnostics without waiting for #1477. #1465 is not unconditionally blocked by #1439: establish whether encode reaches a skew-sensitive sort and add the native blocker if it does. Baseline and read-only attribution can proceed without pretending incomplete instruments exist; qualification of dependent implementations still requires the relevant evidence.

**Closure:** #1387 gates #1478, and #1456 gates #1387. Follow each parent's live native children and blockers rather than copying their membership here. #1464 requires a documented close/re-scope disposition, not implementation of the refuted prefix-sum proposal. #1194 remains a separate storage workstream under #735; related storage work does not automatically block the encode spike.

Closing children does not automatically close a parent or prove its acceptance criteria. Each parent requires its own acceptance evidence. #735 remains M5's canonical tracker.

### Step 0 · Baseline once, on the integrated tree

§4. Require a qualifying clean S18–S22 ladder on the recorded integrated tree, **containing #1452 and the #1526 fix, archived under #1530 before it is reported** (rev 14). Consult #1387 for baseline evidence and any measurement gaps; reuse evidence that still satisfies the contract.

### Step 1 · Synchronization, scheduling and CPU attribution (#1480)

**Evidence anchor: #1482, merged as `52f29ad0`.** §2.3.1 contains the measured result, scope, controls and uncertainty. Synchronization blocking dominates the observed N-process slowdown; CPU duration and runnable delay also increase. The physical storage/journal/locking mechanism and the cause of increased CPU duration remain unisolated.

The supported barrier intervention belongs to #1452: durability-preserving amortization in shaping/spill publication, guided by the measured 6,554 directory and 2,739 file synchronization calls/import. Preserve crash/refusal contracts and compare the same durable path before claiming a benefit. The temporary instrument and its explicit unknown-state accounting are reusable evidence for #1477/#1481, not completion of either issue. No fsync removal, independent-storage isolation, CPU ceiling, complete-ingest qualification or single-parallel-ingest performance claim follows.

### Step 2 · The instrument, with the spec corrected (#1477)

D4 requires reusable phase/region attribution in stock release output. #1477 owns the implementation, validation and completion evidence. Historically, #1474 supplied per-operation `cpu_ns` / `cpu_unmeasured_calls`, while #1470's region API lacked production callers; #1480 used external uprobes on the stock binary. Those facts explain the work, not its current status. The instrument contract is:

1. **Effective cores per region** — CPU / wall over the same region. Preserve `effective_cores()` units. A controlled CPU-bound single worker should report approximately one; a blocked worker should report less.
2. **Scheduler and wait attribution** — distinguish running, runnable-but-not-running, and blocked time, with barrier/lock/I/O attribution where observable. Critical-thread blocking, process-wide inactivity and PSI `some` are different quantities. Retain explicit unknown time and validate a controlled blocked interval separately from scheduler delay.
3. **Stage throughput and execution profile** — report useful work/time and controlled worker-count speedup separately from time-weighted on-CPU concurrency and its distribution. Worker-pool occupancy does not prove scheduler execution; process CPU sampled across overlapping scopes cannot be assigned to each scope without a stated attribution method. No maximum-worker or ≥2× parallelism acceptance gate.

Use stable phase/operation boundaries so the instrument survives #1456; document any per-thread attribution needed for overlapping regions. Reconcile whole ingest, operation totals and residuals, including publication (#1481). No inferred inherent serial fraction should be presented as measured. #1480 has supplied validated temporary diagnostics; carry its explicit missing-wakeup uncertainty into the permanent instrument rather than treating an unobserved wake as known blocked time.

### Step 3 · Bytes, ranked by attribution, judged in device units

No longer "an order of magnitude before CPU matters". The attribution to rank byte work exists (**measured**, S24, `application_io`):

| phase | read | write | share of construction I/O |
|---|---:|---:|---:|
| `shape_consume_reauthentication` | 196.3 GB | 115.7 GB | 60.4% |
| `encode_write_postwrite_authentication` | 71.0 GB | 13.4 GB | 16.3% |
| `append_merge` | 0 | 50.0 GB | 9.7% |
| `recovery_reauthentication` | 45.4 GB | 0 | 8.8% |
| `cas_install_read_write` | 11.4 GB | 11.4 GB | 4.4% |

`shape_consume_reauthentication` is the merge tree plus the Parquet writes — the core shaping work, not a redundant check despite the name. `recovery_reauthentication` reads 45 GB for zero writes; `encode_write_postwrite_authentication` reads 71 GB back (part of it removed by #1444); the `register-parquet` source copy is a full extra write and read of the input (§6). Owner: #1194; follow its live children. Historical attribution references include #1384, #1393, #1418 and #1442. Each item is a two-way door and bears on the historical S25 refusal, `io_reader_publication_headroom` — which, note, is computed against the same achieved-rate "capacity" F1 just measured to be 3–6× low; that projection is itself on the list to re-derive (#1433).

### Step 4 · Work removal, scoped from the integrated baseline

The ≤3.54 µs/edge target, mandatory 2.30× cut and 1/3.54 exchange rate are withdrawn. #1458 (landed, reported −33% validate user CPU), per-record allocation work and normalization (#1472) identify candidate mechanisms, not a proven remaining CPU target. Rank bounded changes by measured useful work removed and complete-ingest improvement under the same resource limits. Set prospective design allocations from §2.6 and validate them rather than transferring a concurrent-process ratio.

### Publication workstream · Attribute and budget publish (#1481)

Publication attribution accompanies the integrated baseline and uses #1477/#1480. It is not deferred until the engine swap. Include CSR construction, authentication/CAS, metadata, barriers and explicit residuals, according to the actual call path. Identify removable work or preparation that can move earlier/concurrently without changing atomic visibility, recovery or the ingest completion boundary. Record the publication allocation and remaining full-ingest budget at each observed scale.

A repair requires whole-ingest A/B evidence and the existing correctness/recovery tests. A no-change disposition requires evidence that publication fits the complete budget; historical ~42% consumption alone neither mandates a particular repair nor proves the floor impossible. #1481 is a native child and blocker of #1387, with no overlap in ownership with composite-transaction issues #1411/#1420.

### Step 5 · Overlap before decomposition (D2)

**Historical F5 proposal (disposition below supersedes the prototype requirement):** Nine roughly equal stages at ~1 core each; if they pipeline, wall tends toward the longest stage — **`canonical_encoding` at 18.0%**, not `after_chain`. Whether they *can* pipeline is a hypothesis: sorting, global validation, surrogate assignment and canonical publication may impose barriers, and a table of durations does not say which batches can overlap. So the experiment is small and gated: normalize ∥ append at S19, quiet host, **≥ 1.1× or pipelining is not the cheap win claimed** (F5). A pass justifies that overlap and nothing larger; the "~6× theoretical, 2–3× realistic" figures rev 11 quoted are withdrawn as forecasts. This is #1456 work item 3 ("overlap input preparation with append, needs no evidence change") and #1472 axis 2. Queue governed by bytes, not batch count; one process-wide CPU budget; the memory admission budget stays separate.

**F5 disposition, 2026-09-20:** #1472 landed bounded parallel normalization, not normalize/append overlap. The [quiet S19 measured-bound disposition](https://github.com/CurateLabs/graphforge/issues/1456#issuecomment-5747437710) substitutes a no-go for the remaining unchanged-work overlap prototype: granting all validate work outside the disjoint append and seal scopes for free, with recorded sampling uncertainty on both sides, gives optimistic bounds of **1.05818× baseline / 1.05920× candidate**, below the predeclared **1.1×** criterion. Do not build that pipeline merely to hide the measured preparation work. This is an explicit non-code disposition for #1456, not implemented overlap, measured overlap speedup, statistical confidence, or a universal limit on algorithms/cache effects that change append or seal work. The integrated S18–S22 baseline, direct floor, and subsequent work remain owned by #1387.

### Step 6 · Encode seam viability, then a bounded shaping decision (D3)

**Keep #1465 on encode.** Its deliverable is a time-boxed go/no-go on the reuse boundary under equal CPU/memory limits, preserving correctness, cancellation, authentication, publication, recovery and ADR 0038. Record encode and complete-ingest wall/CPU, bytes and memory. Remove the ≥2× parallelism gate rather than transferring it from shaping to encode.

A seam pass permits a subsequent bounded shaping experiment; it does not authorize wholesale migration or establish #1387's floor. Before that experiment, freeze the affected path, correctness/resource constraints and an end-to-end benefit/noise criterion. Adopt shaping only after representative evidence shows a worthwhile whole-ingest benefit under equal resources. Keep #1439 before skew-sensitive SortExec adoption; establish whether the encode spike reaches that surface rather than assuming the dependency.

The historical ~42% of validate in named sort/spill regions is not a proven limit on what execution changes can affect. The "stays ours" list preserves ownership of contracts, not an immutable performance boundary.

**Seam disposition and re-routing, 2026-09-20.** #1465 landed in #1496: the tested encode seam is **viable** (authentication, fsync/install, receipts and recovery retained; digests identical) and a **performance no-go** (three quiet matched S18 pairs 29.334 s vs 29.556 s complete-ingest median; about 7.6% higher encode wall/CPU with the adapter). The baseline was retained, so this step authorized **no** bounded shaping experiment. The same day the maintainer opened #1504, which evaluates library sorting/partitioning (#1506), spill/buffering/memory pools (#1507) and Tokio/DataFusion scheduling and cancellation (#1508) for construction under the #1505 protocol, with an ADR and decision matrix in #1509. On the current tree, shaping is 48% of validate at about 0.90 effective cores and encoding 31% at 0.94 (quiet S19, #1456 close evidence), so partition-level shaping parallelism (#1448) is the largest lever that is not byte removal. #1448 is re-scoped to shaping over partitions and is **natively blocked by #1506 and #1508**: a hand-rolled partition-lane scheduler is exactly the mechanism those spikes evaluate, and building it first would duplicate or prejudge their disposition. #1448 remains the #1387 child that owns shaping parallelism and implements on whichever facility they record (retain, adopt or hybrid). #1504 is not an M5 issue and does not gate #1387 or this plan; only its two mechanism spikes gate #1448.

**Continuation rule and boundaries (rev 14, 2026-09-21).** #1448 tests one hypothesis: moving a measured, row-proportional shaping operation from ordered coordinator consumption into independent partition tasks passes its predeclared 10% whole-ingest gate against a frozen baseline SHA. It removes or relocates no verification check, changes no admission, publication or recovery contract, and keeps recorded UUID splitters as the durable authority. After its A/B, recompute the remaining gap from the measured complete-ingest number before any further structural work; shaping alone is bounded below 1.92×. Revisit triggers before production adoption are recorded on #1448. Any need for a process-wide admission manager, manifest format or publication-protocol change stops the experiment and routes to #1509.

### Step 7 · Hardware last, as a reference-host demonstration only

A 32-core box changes neither bytes per edge nor the S24 drift; it makes the number appear on a demo. Reject as a primary lever. Accept one run on a defined reference host once steps 3–5 make the per-resource budgets pass on `OVHC-AGENCY`.

### What stays from earlier revisions

Phase 0b (#1463, implemented by #1466) is the precondition for any instrument measuring the engine rather than a two-worker facade. #1441 may dissolve into Arrow's columnar representation; verify rather than build twice. #1460 landed (#1468). #1464 owns the close/re-scope disposition of the proposal refuted at the cited scales; follow its evidence.

## 6 · Nothing sacred

Reopened by the maintainer's ruling; each to be argued on measurement, not reversed by default.

| invariant | what it costs (measured unless marked) | the question |
|---|---|---|
| The `register-parquet` source copy (closed 2026-09-17 as durability policy) | a full copy of the input in the transient peak; *hypothesised* to be the bulk of §3.3's unattributed 14–18% | a digest pinned at registration detects modification and refuses; it does **not** preserve import/resume after the source is edited or deleted. Is that semantic trade acceptable? Argue it as a trade, not as an equivalent |
| `shape_consume_reauthentication` | 60% of construction I/O at S24 | how many passes does shaping need? |
| Read/write amplification | 5.3× / 8.0× physical over logical at S24, harness units, whole rung | reconcile scope and units first (§2.2); then: a log-structured store on object storage measures 3–5×, what accounts for the rest? |
| **The durability barrier design** | 155,314 fsyncs at S22; 623,895 at S24; fsync service time on this array 0.18 → 5.9 ms mean under 16-way concurrency, thread time in `fsync()` 39% → 79% (F1); `seal` 96% → 32% CPU-busy at N = 16 (probe) | not only the count (#1452: 8 per spill on one directory inode; repricing landed as #1451) but the *placement*: which barriers sit on the critical path, and whether fresh-extent commits (F1 §4.3: 0.7–1.4 s tails) can be batched |
| `publish` | Historical 28.2 s at S22, 114.3 s at S24; approximately 0.42 µs/edge | #1481 attributes the integrated path including CSR and budgets publication inside full ingest (§2.5) |
| The identity B-tree | #1387 workstream 5: "the only component with a size-dependent constant" | candidate for the S24 drift; unmeasured since the ruling |
| The 2% serial budget | — | written against the old baseline; needs step 2's figures (2) and (3) before any number replaces it |
| The RSS growth gate (removed 2026-09-18, #1466) | refused S25 at 757 MiB against 4 GiB | settled: deleted from admission, fraction still reported; revisit once lanes exist. #1473 argues the rung peak it gated on tracks bytes moved, not memory held |

**Not on this list:** the open-time content sweep. Mutation testing with it removed shows a corrupted `topology/edges` Parquet accepted with queries returning results. "Nothing sacred" is licence to re-argue cost, not to trade correctness for throughput.

## 7 · Cautions that must survive into any design

1. Worker count, execution partitions and durable partition layout are three different numbers. Hash or round-robin repartitioning does not replace recorded UUID splitters.
2. Streaming does not eliminate blocking stages. Sorts must accumulate or spill.
3. Keep a separate total-memory admission budget. A framework memory pool accounts for registered consumers, not every flowing batch or writer buffer.
4. CPU work in async needs explicit admission and cooperative cancellation.
5. Retain GraphForge's filesystem authority. `tokio::fs` removes no fsync cost.
6. Coordinate pool concurrency so no two schedulers each claim all eight cores — and, per the agent brief, cap build jobs the same way.
7. *(corrected)* **Delivered speedup at N workers is not a pool size.** The real binary delivers 3.27× at 8 and 3.54× at 16; rev 11's proxies rose from 6.14× to 7.27× over the same range. Size pools by measuring the stage, not by reading a ceiling off a probe.
8. **Do not move work across the ingest boundary to move the metric.** "Edges/s during ingest" can be raised by relabelling (integrity checks behind `verify`, CSR build into `publish`). The lifecycle-time budget beside the rate (D1) is what prevents it.
9. *(new)* **N independent processes are not one parallel ingest.** Every scaling figure in §2.3 is the former. They characterize that concurrent-import workload; they neither bound the host generally nor forecast a redesigned single ingest.
10. *(new)* **Achieved rates are not capacities, and logical bytes are not device bytes.** Any byte argument states demand and capacity for the same work in the same units, or it is not an argument (§2.2).

## 8 · Research questions and evidence routing

These questions arise from the historical evidence, not a live list of unfinished work. Consult the owner for subsequent results and dispositions; do not repeat an answered experiment. Update this section only when a finding changes the plan's reasoning. Questions without a narrower owner remain with #1387's integrated baseline/attribution work.

| question | why it matters | experiment / evidence route | owner or historical finding |
|---|---|---|---|
| **Main contributors to N-process contention loss** | informs interventions; does not itself set a CPU budget | #1480 actual barrier/scheduler tracing, §2.3.1 | #1480; #1482 finding: sync blocking dominates; CPU duration/runnable delay also rise. Device/journal/lock separation and CPU mechanism remain open. |
| Device bytes per edge in the ingest phase, reconciled md vs members | the only form in which the 1.6 KB budget can be checked | phase-scoped device counters in the step 0 ladder | #1387; consult current evidence |
| GraphForge's I/O concurrency, offsets, cache setting, fsync placement | F1 shows the access setting moves capacity 6.5× (273 vs 1,786 MB/s on one thread) | integrated phase/device attribution | **partly answered**: #1482 records S18 validate cache state, sync type/count/phase placement and distributions. Full-ingest I/O concurrency/offsets and device serviceability remain open. |
| Is the rung peak RSS page cache? (F3) | if yes, "throughput rises with scale" was a cached-regime result and S24 is the first honest rung — a hypothesis, not a finding | cgroup `memory.stat` during one S22 rung | #1473 |
| The 4 GiB limit vs the 8.77 GB rung peak at S22 | #1387 comment: the per-rung limit and the observed rung peak "are evidently measuring different things"; the external review flags it as unresolved evidence | determine the cgroup hierarchy, enforcement, swap and accounting scope of the harness | #1387; consult current evidence |
| What bends at S24 | drift can consume floor headroom; the epic also has a separate shape requirement | per edge S22 → S24 (`f80f69fe`, measured): `seal` 6.42 → 6.84 µs (+6.6%), `append` 1.07 → 1.20 (+12%), unattributed 1.60 → 1.74 (+9%), `publish` 0.43 → 0.43 (flat) | answered in part: spread across three terms, located in none |
| What the 14–18% unattributed ingest wall is | the fastest-growing per-edge term (§3.3) | time `register-parquet` and session bookkeeping in the receipt (#1477) | #1477 instrumentation; #1387 full-path attribution |
| Effective parallelism at 8 balanced workers | fallback if lanes disappoint | rebase #1429, force `threads = 8`, `seal` wall at S19 quiet | #1387; consult current evidence |
| #1442 wall gain on a quiet host | fsync count measured (−65%); wall never measured quiet | `gf-s26plan/ab_s18.sh` when QUIET, 4 min | #1387; consult current evidence |
| Ladder-scale figures on the integrated tree | older ladder/phase tables predate the full integration; #1482 characterizes historical `fa4dec51` validate only | step 0 | #1387; `ab1a713e` summary rows (2026-09-21) are interim reference only; per-operation baseline needs the #1526-fixed rerun |
| Does an explicit-exchange, owned-artifact construction pipeline remove enough work or serialization to justify migration? | proposed 2026-09-21; a design essay, not a measurement; shares the barrier and publication substrate with the current path | one hypothesis row in #1509 under #1505 §5, same substrate both sides; #1448 tests the single shaping-ownership hypothesis first | #1509 (hypothesis), #1448 (experiment); not a plan |

## 9 · Decisions — decided 2026-09-19

Rev 11 recorded these as proposals. The maintainer ruled on all five; they are recorded here as decisions and the reasoning that survives is kept.

**D1 — resource disaggregation retained; rev 12's ≤3.54 µs derivation withdrawn.** The direct 1M floor at every supported scale remains the acceptance gate. Resource allocations are diagnostic/design aids, subject to matched-boundary measurement and the full deadline (§2.6).

| quantity | current disposition | evidence or limit |
|---|---|---|
| CPU per edge | **No new numerical necessity established.** Do not enforce ≤3.54 µs from the N-process probe. | Historical ingest CPU/edge is estimated; the cited validate-only CPU excludes publication and other work. Derive an allocation from the integrated candidate and deadline. |
| Physical I/O per edge | Retain the **≤1.6 KB policy allocation**, ingest-scoped device units; **serviceability is not yet established**. | F1's 1.62 GB/s is for an assumed access pattern/concurrency with little margin. Measure matching demand and actual barriers before claiming the allocation reaches the floor. |
| CPU concurrency / speedup | Report **C/W**, worker-count speedup and N-process throughput speedup separately. No ≥2× or max-worker gate. | 3.54× is observed N-process throughput, not an effective-core count or single-ingest ceiling. |
| Scaling | Wall exponent ≤1.02 and approximately flat CPU/edge remain diagnostic policy values. | They do not imply 1M at every rung or replace the separate no-degradation requirement. |
| S18 begin + resume + publish | Retain **≤0.84 s as a policy allocation for the named S18 operations**, not "fixed cost". | Publication/resume scale with input. #1481 re-measures and proposes allocations for the whole path; true startup cost is separate. |

#1476 must preserve same-workload regression protection and the direct floor gate, remove the circular validation claim and unsupported CPU threshold, and identify measured versus policy thresholds. Complete-ingest accounting already includes publication/setup; do not add them twice. Report lifecycle time alongside ingest.

**D2 — approved.** Bytes and work removal ahead of and alongside the seam spike; overlap before decomposition. §5 is the order. What it replaced: #1456 comment 5724135325's "foundation before optimisation" *as an ordering of code changes* — its rationale (the surrogate chain as the 48–50% serial remainder; hand-parallelising `seal` being work the refactor deletes) did not survive the rung-scale attribution. What it kept: the instrument first, #1439 before `SortExec`, the swap judged on a measurement.

**D3 — decided: encode.** Keep #1465 bounded to seam viability. Remove the ≥2× parallelism criterion on both surfaces; record useful-work performance and complete-ingest effects with equal resources and correctness. A pass authorizes a subsequent bounded shaping experiment, not engine replacement. Shaping adoption needs its own predeclared end-to-end benefit criterion and measured result (§5 step 6).

**D4 — approved: reusable diagnostics in stock release output.** Preserve per-operation CPU and expose region attribution under the corrected §5 step 2 contract. #1477 owns wiring and validation; consult its linked implementation and acceptance evidence for fulfillment. Historical test-only scopes and unused APIs explain the decision, not the current implementation state.

**D5 — approved**, with the added requirement that the gates keep tracking as performance improves. The historical `INGEST_FLOOR_EDGES_PER_SECOND = 15_000` and prose ratchet in `m6_storage_io.rs` motivated #1476, which owns the evolving regression gates and their reconciliation with #1387. Read its implementation/evidence for current thresholds. The external review's distinction is kept: a ratchet is an interim regression policy and can coexist with an unmet 1M acceptance gate; an unmet requirement is unfinished work, not wording to retire.

## 10 · Standards that do not move

TCK suite green (historical suite: 3,897 scenarios) (`cargo test -p graphforge-api --test bdd`) · G500 S26 admits *and completes* · GDC suites green · every feature retained · fail-closed publication · no silently accepted corruption.

**Explicitly spendable:** recoverability-without-restart · eager verification whose refusal is duplicated at a consuming boundary · in-memory representations, buffer sizes, process boundaries, phase counts · any constant priced for different hardware · wire formats, with a version bump, a read path and the determinism suite green · byte-identical intermediates, retired by ADR 0038 in favour of semantic equivalence at the publication boundary.

## 11 · Reversibility

| commitment | door | note |
|---|---|---|
| byte-cut work (#1444, #1450, #1451, #1418) | two-way | land freely; judge in device units |
| unpin resource policy (#1466, landed) | two-way | watch the reported `rss_growth_fraction` |
| removing the RSS growth gate (landed) | two-way | same |
| the contention characterization (step 1) | read-only | evidence: #1482; supports bounded #1452 barrier work, with explicit unresolved mechanisms |
| encode seam spike (#1465) | two-way | D3: seam viability on encode; a pass permits a bounded shaping experiment only |
| replacing sort/spill/memory accounting with DataFusion in shaping | **one-way in practice** | rewrites the evidence, spill lifecycle and admission budget; deserves the heaviest gate on this page |
| resource budgets beside the floor (D1) | decided with rev 13 corrections | #1476 preserves the direct floor and valid regression gates; no ≤3.54 µs derivation |

## 12 · Evidence

- **Completed #1480 characterization:** [merged report](https://github.com/CurateLabs/graphforge/blob/52f29ad03eb71c8a843c1ccf0f01d0b177c84b50/docs/development/evidence/ingest-contention-1480.md), [machine-readable evidence and final diagnostic sources](https://github.com/CurateLabs/graphforge/blob/52f29ad03eb71c8a843c1ccf0f01d0b177c84b50/docs/development/evidence/ingest-contention-1480.json), PR #1482, merge `52f29ad0`; raw traces, controls, failures and prepared projects at `/home/ubuntu/graphforge-1480/` on OVHC-AGENCY. Both PR and required merge-queue CI passed. This is S18 historical-build validate characterization, not the step 0 integrated ladder.

- **Lost on 2026-09-21 (rev 14):** every rung directory under `/home/ubuntu/graphforge-ladder/` listed in the next bullet, plus `clean-1955f17d-evidence/`, `clean-ab1a713e-evidence/` and `clean-4fbfe84c-evidence/`. The tables copied into this plan and #1387 are the record; controller summaries survive at `/home/ubuntu/gf-clean-ladder-ab1a713e.log` and `/home/ubuntu/gf-clean-ladder-4fbfe84c.log`; the source worktrees `/home/ubuntu/gf-ladder-src-<sha>/` (the frozen binaries under `gf-ladder-target-<sha>/` were removed in the same sweep, correction 2026-09-21 21:20 UTC; rebuild from the worktree). Retention: #1530.
- **Rungs (historical paths, no longer on disk):** `/home/ubuntu/graphforge-ladder/s18-s22-93c041df-evidence/` (S18–S22, quiet host, per-operation CPU; §2.1, §2.4, §3.2, §3.3 left columns); `clean-f80f69fe-evidence/` (S18–S24; the only S24; §2.1, §2.2 logical bytes, §3.3 right columns, §8); `s18-403fc02a-instrumented-evidence/` and #1387 comments 5734006616 / 5734073771 / 5734143331 (§3.1).
- **F1, the array:** `/home/ubuntu/graphforge-plan-review/f1-device-ceiling/` — `results.md`, `PROVENANCE.md`, `jobs/`, `raw/` (every fio output verbatim, host state, quiet-host log). Reproduce with `jobs/run-f1.sh`.
- **perf stat and the N-way probe:** `/home/ubuntu/graphforge-plan-review/perf-stat-validate/` — `results.md`, `summary-table.txt`, `jobs/run-perf.sh`, `jobs/run-scaling.sh`, `raw/` (every `perf stat` output and every probe receipt). `gf` from `fa4dec51`, release, sha256 prefix `5a86985c9abbfb04`.
- **Rev 11 proxies (superseded, kept):** `/home/ubuntu/gf-redteam-scratch/f2-*.log`, reproduced in `perf-stat-validate/jobs/f2-probe*.sh`.
- **Review packet:** `/home/ubuntu/graphforge-plan-review/` — `plan-rev11.md`, `rev10-critique.md`, `rev11-self-review.md`, `external-review-astra.md`, `verification-of-astra-review.md`, `epics/`.
- **Continuation log:** `/home/ubuntu/gf-ladder-continue-f80f69fe.log` (the S25 refusal).
- **Quiet-host guard:** `~/.claude/gf-quiet-host.sh`; never `pgrep -f` to detect a build.
- **Host:** OVHC-AGENCY, Ryzen 7 3800X (8 cores / 16 threads, Zen 2, 32 MiB L3), 125 GiB RAM, ext4 on md RAID1 over two PM983 960 GB NVMe. benchexec double-counts I/O on md RAID1 (reads ≈ 2×, writes ≈ 3×). Where the host was contended, CPU-seconds and phase fractions are reliable and absolute wall is not.
- **Live trackers:** §13; native hierarchy and dependencies are authoritative for current membership and state.
- **Related documents:** `docs/development/perf-g500-ladder.md` (the ladder), `docs/development/g500-certification.md` (the S26 claim), `docs/development/integrated-storage-1194.md` (#1194's evidence), ADR 0038 (determinism at the publication boundary).

## 13 · Live execution and closure views

Use GitHub's live issue state, sub-issue list/progress, **Blocked by / Blocking** panels and linked PRs at these entry points:

| Entry point | Role |
|---|---|
| [#1478 — this plan](https://github.com/CurateLabs/graphforge/issues/1478) | Workstream close gate; follow #1387. |
| [#1387 — ingest floor](https://github.com/CurateLabs/graphforge/issues/1387) | Canonical floor acceptance, integrated baseline evidence and live ingest children/blockers. |
| [#1456 — construction foundation](https://github.com/CurateLabs/graphforge/issues/1456) | Foundation acceptance and live construction children/blockers. |
| [#1194 — storage amplification](https://github.com/CurateLabs/graphforge/issues/1194) | Separate storage workstream and its live children/blockers. |
| [#1388 — query workstream](https://github.com/CurateLabs/graphforge/issues/1388) | Related query work; not an implied ingest execution prerequisite. |
| [#1504 — construction reuse evaluation](https://github.com/CurateLabs/graphforge/issues/1504) | Library sorting/partitioning, spill and scheduling evaluation under the #1505 protocol; its #1506/#1508 spikes gate #1448 only. |
| [#1526 — S22 control-record bound](https://github.com/CurateLabs/graphforge/issues/1526) | Blocks step 0: the current tree cannot complete S22 until it lands. |
| [#1530 — ladder evidence retention](https://github.com/CurateLabs/graphforge/issues/1530) | Archive rule for step 0 and later ladders; #1529 isolates the test suite from the ladder root. |
| [#735 — M5](https://github.com/CurateLabs/graphforge/issues/735) | Canonical milestone closing tracker. |
| [Open PR queue](https://github.com/CurateLabs/graphforge/pulls?q=is%3Apr+is%3Aopen) | Current implementation/merge work; apply repository WIP and exact-head CI policy. |

Follow nested children and dependencies to discover additions, closures and reopenings. Section 5 explains the ordering; these live views replace the static status diagram and child inventory.




