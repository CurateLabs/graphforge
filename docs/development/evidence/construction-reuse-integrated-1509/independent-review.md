# Independent review of ADR 0046 (#1509)

A separate read-only agent checked every factual claim in the ADR draft against the
evidence it cites on 2026-09-24. It recomputed figures from the raw JSON and read the
code paths the ADR describes. It could not edit anything, and it ran no build while
the measurement host was in use.

## Confirmed

- Pins: `arrow` 58.4.0, `datafusion` 54.1.0, `tokio` 1.53.1, `rayon` 1.12.0.
- Partitioning: the six incompatibility proofs map to the retain decision.
- Sorting: the #1506 figures are byte-identical publication, ingest within about 1%, +21–45 MiB RSS and kernel 1.9–3.3×.
- Integrated wall: +2.7–4.7%, recomputed from the observations.
- Integrated RSS: +28–60 MiB, recomputed from the observations.
- Scheduler alone: within 0.4% of production, recomputed from the observations.
- Spill bytes are invisible to GraphForge evidence. The memory pool counts only explicit reservations.
- F15: 450 ms production cancellation latency.
- `rayon_ordered` is 62 lines. The production pool uses `std::thread::scope`.
- The hybrid's merge calls `block_on` on the coordinator (`spill_spike.rs`, `ExternalPartition::for_each_record`).
- The budget override is called at session open.
- The two maintainer-reserved decisions each change an existing requirement.
- The explicit-exchange hypothesis is correctly recorded as not prototyped.

## Problems found and how each was resolved

| Finding | Verified against | Resolution |
| --- | --- | --- |
| The hybrid CPU range's low end was +4.6%; S18 `hybrid` is +4.5% | `main/summary.json` | Corrected to +4.5–10.2% in the ADR, the evidence document and PR #1580 |
| The pool was called "120-line", the pre-#1564 figure; the measured module is 188 lines | `partition_load.rs`; the maintenance table | Now: an estimated 120 of the pool's 188 lines are what Rayon would replace |
| Adapter obligations were attributed to #1507 alone; the thread-scheduler requirement comes from #1509 | Evidence document, bounded conclusions | Now: the first five are named by #1507 and the last by #1509 |

## Omissions found and how each was resolved

| Finding | Resolution |
| --- | --- |
| Polling-candidate cancellation latency was "about 1 ms"; measured 0.7–1.4 ms | Stated as 0.7–1.4 ms |
| Hybrid cost lacked the share of partitions that went external | The matrix states the 1 MiB budget and the 9.2% (S18) and 7.0% (S20) shares |
| The cancellation repair had no issue number | Filed as #1581; retirement of experiment code as #1582 |
| #1448 says "the existing Rayon arrangement" on its retain path | The ADR states that the retained pool is scoped `std::thread`, and that Rayon today is `graphforge-exec`'s `ComputePool` |

## Reviewer verdict

All five #1509 acceptance criteria were substantially met. The gap was the missing
follow-up issue number, now filed. The three factual errors above were corrected
before the ADR was proposed for merge.
