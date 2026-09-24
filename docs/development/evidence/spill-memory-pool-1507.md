# Construction spill, buffering, and memory-pool reuse (#1507)

> **Integrated follow-up (#1509):** this mechanism was combined with the other
> spikes' candidates and measured through complete ingest, publication, reopen
> and queries on a tree containing #1562. See
> [`construction-reuse-integrated-1509.md`](construction-reuse-integrated-1509.md).

Parent: [#1504](https://github.com/CurateLabs/graphforge/issues/1504). Follows the
binding protocol in
[`construction-reuse-inventory-protocol-1505.md`](../construction-reuse-inventory-protocol-1505.md).
Sibling mechanisms: sorting/partitioning (#1506), scheduling/cancellation
(#1508). The integrated ADR is #1509.

## Predeclared experiment (recorded before measurement)

| Item | Choice |
| --- | --- |
| Mechanism | Spill and memory admission for fixed-width shaping partitions |
| Candidate | DataFusion 54.1 external `SortExec` with a `GreedyMemoryPool` and a `DiskManager` (Arrow IPC spill runs, multi-level merge) |
| Hybrid | `GF_SHAPE_SPILL_SPIKE=datafusion`: partitions the baseline admits stay resident; a partition the recorded `max_partition_bytes` would refuse is sorted externally under a pool of that budget |
| Overhead probe | `GF_SHAPE_SPILL_SPIKE=datafusion-always`: every fixed-width partition goes external |
| Retained | Recorded splitters, routing, GraphForge's sealed spill segments and receipts, publication, recovery, the balance assertion, cancellation callback |
| Selection | Environment only; compiled under `cfg(test)` / `feature = "test-support"`; invalid values fail closed |
| Out of scope | Row (Arrow property) partitions, production default, splitter authority, scheduling changes (#1508) |

### Measurement protocol

- **Binary:** one release `gf` built with `graphforge-storage/test-support`; modes
  differ only by environment. Commit and binary SHA-256 recorded with results.
- **Input:** Graph500 S18 and S20, edge factor 16, seed
  `13907095936298285200`, the ladder profile's five `import-session`
  commands (begin, two `register-parquet`, validate, commit) into a fresh
  project per observation. Input SHA-256 recorded.
- **Modes:** `baseline`; `external` (`datafusion-always`, pool = recorded
  budget, 256 MiB default — operator cost without memory pressure);
  `external-tight` (`datafusion-always`, pool 64 KiB — forced spill in every
  partition, the tighter-memory stress case).
- **Repetitions:** three alternating triples per scale, mode order rotated per
  triple. A run starts after 12 consecutive QUIET samples (5 s apart) from
  the host guard and is sampled each second during execution; a run that saw
  BUSY is retained under `contended/`, excluded, and retried.
- **Metrics:** complete-ingest wall; process-tree CPU (user+sys via
  `wait4`); validate wall/CPU; peak RSS of the largest `gf` process; 512-byte
  blocks written (`ru_oublock`); host-wide `nr_dirtied`/`nr_written` page
  deltas (page cache vs storage writes). One separate instrumented run per
  external mode and scale records per-partition pool peak, spill count,
  spilled bytes/rows, peak spill disk bytes/files, and sort/merge wall; it is
  excluded from timing.
- **Report:** median and observed range; no significance claim from n=3; no
  invented speedup threshold. Published row counts must match baseline.
- **Correctness gate:** the tests below, plus the full storage library suite
  with every fixed partition external.

The driver is [`spill-memory-pool-1507/measure.py`](spill-memory-pool-1507/measure.py).

## Adapter design

`graph_construction/spill_spike.rs` is the only new execution code;
`partition_shaping.rs` gains a `LoadedPartition` enum so the coordinator can
consume either a resident partition or a streaming external one.

- **Input.** The load worker opens the partition's sealed GraphForge segments
  through the construction directory's descriptor authority, then hands the
  open readers to a `StreamingTableExec` source. Fixed records become
  `FixedSizeBinary(N)`; compact details become `Binary` wire bytes. Both sort
  byte-lexicographically, which is the baseline's order.
- **Sort phase (worker).** `SortExec` consumes every record under the pool,
  sorting and spilling runs as reservations fail. Input batches are sized to an
  eighth of the pool (64–8,192 records): DataFusion must admit a whole input
  batch, so an 8,192-record batch (~402 KB for 48-byte records) can never be
  admitted under a 16 KiB pool.
- **Merge phase (coordinator).** The first output batch returns to the
  coordinator; the rest stream while the coordinator writes the partition, so
  no fully sorted over-budget partition is ever resident. This moves merge work
  into the coordinator's ordered critical section.
- **Cancellation.** The source is cooperative, so the sort yields every tokio
  budget; a `select!` re-checks the worker stop flag at each yield and drops
  the stream, which is DataFusion's own cancellation and deletes the runs.
- **Integrity guard.** DataFusion does not detect changed spill payloads (see
  below). The adapter sums two differently seeded 64-bit checksums over the
  multiset of records fed in and compares them with the records emitted before
  the coordinator can publish. `GF_SHAPE_SPILL_GUARD=off` exists only so the
  tests can show what the library alone lets through.
- **Runtime.** One current-thread tokio runtime per external partition, with
  the blocking pool capped at two threads (DataFusion reads spilled runs with
  `spawn_blocking`).

## Library properties (independently reviewed)

A separate reviewer checked each claim against the pinned sources
(`datafusion-*` 54.1.0, `arrow-ipc` 58.4.0, `tokio` 1.53.1, `tempfile` 3.27.0).
Seven were confirmed; one was corrected.

| Property | Evidence |
| --- | --- |
| Spilled runs are read back with Arrow IPC validation disabled | `datafusion-physical-plan/src/spill/mod.rs:130-134`: `unsafe { StreamReader::try_new(file, None)?.with_skip_validation(true) }`, justified as "input guaranteed to be correct when written" |
| No checksum or digest on the spill write or read path | No such code in `spill/` or `disk_manager.rs` |
| Spill files are never fsynced | No `sync_all`/`sync_data` in `datafusion-execution` or `datafusion-physical-plan`; `flush()` flushes the in-process buffer only |
| Files are path-based `tempfile`s, deleted only on `Drop` | `disk_manager.rs:307-309`, `:363-374`; no startup sweep exists |
| Run-directory naming (corrected) | Configured directories get a `datafusion-XXXXXX` child (`disk_manager.rs:332`); the default `OsTmpDirectory` mode uses an unprefixed `.tmpXXXXXX` directory, which nothing identifies as DataFusion's after a crash |
| The disk limit is per `DiskManager` and checked after writing | `used_disk_space` is a per-instance field; `update_disk_usage` compares after the write. Separate `RuntimeEnv`s do not share a limit |
| The pool counts only explicit reservations | Spill writer and reader buffers are never reserved (`spill_manager.rs`, `in_progress_spill_file.rs`) |
| The multi-level merge re-spills | `sorts/multi_level_merge.rs:184-216` spills intermediate merges back to disk, so spilled rows can exceed input rows |
| `tokio::fs` only moves the blocking call | `tokio/src/fs/mod.rs:312-323` (`asyncify` = `spawn_blocking` over `std::fs`); no descriptor-relative open, checksum, quota, or crash cleanup |
| The source yields cooperatively | `streaming.rs:272` wraps the stream with `make_cooperative` |

## Correctness and failure evidence

The spike tests run each case as a complete construction (shape, encode,
publish, reopen, hydrate the adjacency index) in a fresh process, so the
environment switches never race the parent harness. The fixture has 1,024
nodes and 8,192 edges; half of all edges point at one hub, so the node-keyed
endpoint family concentrates in one partition (4,288 records, 141,504 bytes
resident) while every partition still owns a balanced share of distinct keys.
The recorded budget is 16 KiB.

```bash
TMPDIR=<ext4 dir> cargo test -p graphforge-storage --lib spill_spike -- --nocapture
```

| Case | Outcome |
| --- | --- |
| Baseline, default budget (control) | Publishes; reopened adjacency index holds 8,192 edges |
| Baseline, 16 KiB budget | Refuses: `partition materialization requires 141504 bytes, exceeds recorded budget 16384`; nothing published, no temps |
| Hybrid, 16 KiB budget and pool | Publishes shaped and encoded artifacts byte-identical to the control; one partition external, 50 spills, 570,536 bytes and 15,936 rows spilled (4.0x bytes, 3.7x rows), peak pool 12,672 of 16,384 bytes, 33 simultaneous run files; run directory empty afterwards |
| Every partition external, 16 KiB pool | Byte-identical to the control |
| Spill payload byte flipped | Refused by the guard before publication; no temps, no runs |
| Spill run truncated | Refused (`Arrow error: Io error: failed to fill whole buffer`) |
| Disk quota 4 KiB | Refused (`Resources exhausted ... exceeded the allowable limit`) |
| Pool 1 KiB | Refused (`Not enough memory to continue external sort`) |
| Byte flipped, guard off | DataFusion returns the corrupted record. The shape completes and installs a shaped artifact whose receipt covers the corrupted bytes; the encoder's edge-stream cross-check then refuses before publication. That catch is a property of the endpoint family, not of the spill |
| Cancelled after an external sort has spilled runs, then retried | The coordinator cancels while a spilled external partition awaits consumption; dropping it deletes its runs. No temps or runs remain, and the retry publishes the control's bytes. (Found in review: the first version of this case cancelled during partition sampling, before any external sort started.) |
| Process aborted after spilling, then resumed | Recovery resumes and publishes the control's bytes. Every orphaned run survives: GraphForge recovery ignores the unknown directory and the allocation ledger never charged it |

Only payload bytes are corrupted. Metadata and offset faults are not tested,
because with validation disabled they are not a safe test to run.

**Full storage suite with every fixed partition external** (64 KiB pool):
1,295 passed, 6 ignored, 1 failed. The failure is
`recorded_partition_refusal_survives_reopen_with_no_completed_shape`, which
asserts the baseline's refusal at a 1-byte budget; the external path shapes
that partition instead. That is the behaviour the hybrid exists to change, not
a defect.

## Measurements

**Setup.** Binary `gf` SHA-256 `ccc23c62…df790`, built on main `3b514cd8` plus this branch's spike commit (before the later clippy refactor and rebase) with
`cargo build --release -p graphforge-cli -p graphforge-storage --features graphforge-storage/test-support`.
Host OVHC-AGENCY: 16 logical CPUs (Ryzen 7 3800X), ext4 on mdadm RAID 1, no
cgroup limit. Generator SHA-256 `2637a106…a11e`. S18 inputs are nodes
`44c9dfd9…8475` and edges `f112ccbe…e9c4`, identical to #1481's S18 inputs;
#1465's S18 edges hash differs, because the generator's edge output changed
between those two issues. S20 inputs are nodes `5792da94…24aa` and edges
`3fb656aa…1086`. All 18 timed observations were accepted: three triples per
scale, mode order rotated. Three contended attempts were retained and
excluded. Raw observations, the instrumented runs and commit receipts are in
[`spill-memory-pool-1507.json`](spill-memory-pool-1507.json).

Medians, with the observed range in brackets:

| Scale / mode | Ingest wall (s) | Ingest CPU (s) | Validate wall (s) | Validate CPU (s) | Validate peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| S18 baseline | 31.6 [31.2–32.0] | 28.7 [28.6–29.0] | 28.6 | 27.2 | 244 [239–246] |
| S18 external | 33.5 [33.4–34.5] | 32.4 [32.4–32.6] | 30.4 | 30.9 | 268 [255–270] |
| S18 external, 64 KiB pool | 49.7 [49.3–50.3] | 66.2 [65.7–66.8] | 46.6 | 64.7 | 277 [259–286] |
| S20 baseline | 126.8 [126.8–128.0] | 117.9 [117.2–118.0] | 114.7 | 111.8 | 305 [302–314] |
| S20 external | 135.3 [135.2–136.5] | 133.9 [133.8–134.7] | 123.1 | 127.9 | 377 [339–380] |
| S20 external, 64 KiB pool | 204.0 [203.4–204.0] | 278.9 [278.8–279.3] | 191.9 | 272.9 | 365 [364–367] |

Relative to baseline, the external operator without memory pressure costs
+6–7% wall, +13–14% CPU and +10–24% validate RSS at both scales. Under
the 64 KiB pool it costs ×1.57–1.61 wall and ×2.31–2.37 CPU. The ranges
do not overlap. With n=3 this is an observed difference, not a significance
claim.

**Instrumented runs** (one per external mode and scale, untimed):

| Scale / mode | External partitions | Spilling | Spill runs | Spilled bytes | Byte amplification | Row amplification | Peak pool | Peak simultaneous run files | Peak run bytes per partition |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S18 external | 1,298 | 0 | 0 | 0 | — | — | 16.3 MB of 256 MiB | 0 | 0 |
| S18 64 KiB pool | 1,298 | 1,296 | 63,287 | 3.91 GB | 4.71x | 4.70x | 65,524 of 65,536 B | 117 | 2.95 MB |
| S20 external | 3,650 | 0 | 0 | 0 | — | — | 27.6 MB of 256 MiB | 0 | 0 |
| S20 64 KiB pool | 3,650 | 3,648 | 253,913 | 17.86 GB | 5.38x | 5.35x | 65,524 of 65,536 B | 344 | 8.67 MB |

Amplification is relative to the partitions' input wire bytes (830 MB at S18,
3.32 GB at S20). It grows with partition size, because the multi-level merge
re-spills intermediate runs.

**Where the bytes go.**

- **GraphForge's evidence is identical across modes.** At each scale, every
  accepted commit receipt reports the same input rows, fsync operations,
  GraphForge-accounted write bytes and chunk count: S18 4,456,448 rows, 872
  fsyncs, 781,593,496 bytes; S20 17,825,792 rows, 3,488 fsyncs,
  3,126,373,984 bytes. DataFusion adds no fsyncs, and none of its spill bytes
  appear in GraphForge's I/O evidence or allocation ledger.
- **The spill bytes still reach storage.** Host-wide page writeback rose from
  12.6 GB to 30.3 GB at S20, and from 3.1 GB to 7.0 GB at S18. The runs are
  not fsynced, but they are not absorbed by the page cache either.
- **Memory is bounded only where the pool reaches.** The pool stayed within
  its 64 KiB limit, but validate RSS rose above baseline (+13% at S18, +20% at
  S20) and above the no-pressure external mode at S18. The runtime, spill
  writer and reader buffers, and batches are outside the pool. Reservation
  bound, RSS and page cache are three different figures here.
- **Scratch is released.** No timed or instrumented run left a run file
  behind.

**Published answers are unchanged at S18.** I ran untimed S18 ingests in
baseline, a second baseline and 64 KiB-pool modes, then exported every node
(`MATCH (n) RETURN n`) and every edge with its endpoints
(`MATCH (a)-[r]->(b) RETURN a, r, b`). All three agree on 262,144 nodes,
4,194,304 edges and identical sorted-multiset digests. Output row order and
the object bytes differ even between the two baselines: row order is not
stable across ingests, and encoded objects carry the session clock. So the
byte-identity claim rests on the pinned-clock fixture tests, and the
Graph500-scale claim is equality of query answers.

## Candidate dispositions

| Candidate (protocol §4, spill/memory row) | Disposition |
| --- | --- |
| DataFusion external `SortExec` + `DiskManager` + `GreedyMemoryPool` | **Exercised** end to end (above) |
| DataFusion `MemoryPool` as GraphForge's total admission authority | **Measured, not sufficient.** The pool bounds operator reservations only; RSS grew while the pool held 64 KiB (§ Measurements; reviewed property "The pool counts only explicit reservations"). It can be a sub-budget inside GraphForge's admission, not a replacement for `max_partition_bytes` or `RowReservation` |
| `FairSpillPool` instead of `GreedyMemoryPool` | Not separately exercised. Each external partition has exactly one spillable consumer, so the choice only matters once pools are shared across workers, which is a #1508/#1509 question |
| Arrow IPC spill runs | Exercised as DataFusion's scratch format. Replacing GraphForge's durable fixed-width segment format with IPC is a separate format decision this spike does not test; GraphForge row spills already use IPC |
| DataFusion spill compression (LZ4/Zstd) | Not exercised. Could reduce the 4.7–5.4x spilled bytes at a CPU cost; open for #1509 |
| `tokio::fs` for spill I/O | **Inapplicable** (independently reviewed): it runs the same `std::fs` call on the blocking pool and adds no descriptor-relative open, checksum, quota or crash cleanup, so it cannot own spill files. The async API alone is not evidence of cheaper I/O |
| Row (Arrow property) partitions | Not exercised. They would need the same adapter with a key column; `RowReservation` admission is unchanged |

## Maintenance assessment

- **Code.** Added: 823 lines of adapter (`spill_spike.rs`) and a 67-line
  change to `partition_shaping.rs`. Tests: 75 lines of unit tests and 515 of
  subprocess tests. Deleted: nothing. The baseline has no external sort to
  replace: the merge tree was removed earlier, and over-budget partitions are
  refused. This is added capability, not replaced code.
- **Ownership.** GraphForge still owns splitters, segments, receipts,
  publication, recovery and total admission. DataFusion owns sort state and
  run files for the life of one partition. A production adapter would still
  have to supply:
  - a GraphForge-owned run directory created through the construction
    directory, rather than a path;
  - removal of that directory at recovery, because orphans survive a crash
    today;
  - one `DiskManager` shared across workers, because the disk limit is
    per instance;
  - a disk limit derived from the allocation ledger;
  - accounting of run bytes in construction evidence, because they are
    invisible today;
  - the multiset guard.
- **Invariants duplicated.** The guard repeats an integrity property
  GraphForge's own segments get from receipts. The batch-sizing rule encodes
  a DataFusion admission detail: a whole input batch must fit in the pool.
- **Dependency and API.** DataFusion is already a dependency, so there is no
  build or binary cost. The adapter touches `SortExec`, `StreamingTableExec`,
  `PartitionStream`, `DiskManagerBuilder`, `MemoryPool` and `MetricsSet`,
  which move between DataFusion majors. The disk-manager configuration API
  already changed in 48 (`DiskManagerConfig` deprecated). *Estimate:* a small
  upgrade touch per DataFusion major.
- **Diagnostics.** DataFusion errors surface as text inside `GfError::Storage`
  (e.g. "Not enough memory to continue external sort"). Spill metrics exist
  only through the adapter's instrumentation.
- **Upstream changes that would reduce adapter code.** A spill-file
  integrity option (checksum, or validated read-back) and a caller-supplied
  file factory (descriptor-relative creation), which would remove the
  guard and the path-based ownership gap.

## Decision (bounded to this mechanism and envelope)

- **Hybrid: viable as a correctness-preserving way to process partitions the
  recorded budget refuses.** Under the tested pools and conditions, it turns
  a safe refusal into a byte-identical publication. It is conditional on the
  adapter supplying what the library does not: the integrity guard, owned
  and recovered scratch, a shared disk limit, and evidence accounting.
  **Confidence: medium.** Tested on fixed-width families only, on the fixture
  hub and at forced-spill S18/S20.
- **External sort for partitions the baseline admits: retain the baseline.**
  Without memory pressure it adds 6–7% wall, 13–14% CPU and up to 24%
  validate RSS, and removes no custom code.
- **Spill files as recovery authority: never.** DataFusion's runs are
  unvalidated, unsynced, path-based scratch. GraphForge's sealed segments and
  receipts remain the only resume state; the crash test shows a correct
  resume that simply ignores the orphans.
- **Memory pool as total admission: retain GraphForge's budgets.** The pool
  can be a sub-budget inside them.

**Revisit triggers:**

- a production workload hits the partition-budget refusal (the hybrid's only
  benefit);
- DataFusion adds validated or checksummed spill reads, or a file-factory
  hook;
- #1508 introduces a shared worker pool, which changes the pool and
  disk-manager sharing question;
- #1506 changes the in-memory sort, which changes the no-pressure overhead.

**Not claimed:**

- that encode-only evidence (#1465) settles spill;
- any S26 qualification;
- anything about row partitions or other workloads;
- that this meets the #1387 ingest floor.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-23 | Predeclared experiment and measurement protocol. |
| 2026-09-23 | Before any timed run: the driver now refuses a binary built without the experiment. `cargo build -p graphforge-cli --features graphforge-storage/test-support` silently produced one; the S12 smoke run showed zero external partitions. The measured binary is built with `-p graphforge-cli -p graphforge-storage --features graphforge-storage/test-support`. Validate-step peak RSS added, because the largest `gf` process is not the validate step. |
| 2026-09-23 | After measurement: a clippy-only refactor of the spike and a ruff-only cleanup of the driver. The storage suite passed unchanged after the refactor, and an S12 smoke run of the cleaned driver reproduced the same partition counts. The results above come from the binary built before that refactor, and the driver as it was before the cleanup. The branch was then rebased onto a newer main. |
| 2026-09-23 | Review fixes (CodeRabbit, each verified against the code). The routing check read every partition's segment lengths in all modes; it now reads them only in the hybrid mode. The measured binary had that extra open and stat per partition in all three modes alike, so the comparisons are unaffected. Also: the cancellation case now cancels after a real spill; the crash check no longer assumes `/` separators; the fault hook reads DataFusion's directories when it fires, because the default OS-temp mode creates them lazily. |
| 2026-09-23 | Correction to the predeclared protocol, from the S12 smoke run before any timed run: a 64 KiB pool forces spill only in partitions whose input exceeds what the pool can buffer (51 of 1,250 at S12), not in every partition. The instrumented runs report the actual count. |
