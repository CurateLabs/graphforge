---
title: "ADR 0056: Shaping stays serial within stages until one sub-phase dominates"
adr: "0056"
status: "Accepted"
date: "2026-10-05"
superseded_by: null
revisit_when: "A stock region capture at S20 or S22 shows one shaping sub-phase above half of shaping wall, or shape_routing's calling-thread CPU above 20% of complete-ingest wall"
---

# ADR 0056: Shaping stays serial within stages until one sub-phase dominates

**Status:** Accepted

**Related:** ADR 0046 (construction reuse decisions), ADR 0047 (over-budget
partitions and the instance CPU budget), #1387 (ingest floor and core
scaling), #1429 and #1448 (earlier parallelism attempts), #1562 (durable
finish stages), #1572 (post-v0.6.0 deferral).

## Context

Complete ingest runs at 1.18, 1.22 and 1.25 effective cores at S18, S20 and
S22 on a 16-core host. #1387 asked whether parallelism *inside* the shaping stages could raise
that. The stages run in a fixed order, so any gain has to come from within a
stage.

A draft of this record proposed three tracers. Each one was gated on a
measurement showing that its target sub-phase dominates shaping wall:

1. **Parallel sort inside load lanes.** Split a partition into chunks, sort
   the chunks on leased sub-lanes, then k-way merge in key order. Targets the
   load and sort inside `shape_family_finish`.
2. **Admission-derived load window.** Replace the fixed
   `max_partition_bytes × 2` weight bound in
   `consume_in_partition_order_weighted` with a bound derived from the granted
   lanes. Same target as tracer 1; trades transient memory for overlap.
3. **Parallel routing lanes.** Shard `route_fixed_run`/`route_identity_run`
   across lanes, each writing its own spill segments per partition. This first
   needs the streaming run checksum split into per-chunk digests.

### Measurement

The measurement comes from the stock region capture that `gf import-session`
records in every receipt (#1477, #1623, #1632). It covers one quiet-window
S18–S22 ladder on `3eeedb9a` (2026-10-04), single run per rung. Complete
ingest is the sum of the five import commands. Process CPU covers every
thread; thread CPU covers the calling thread only.

| S22 region | wall s | share of shaping | effective cores | calling-thread CPU s |
| --- | ---: | ---: | ---: | ---: |
| `shape_routing` | 68.68 | 38.9% | 1.04 | 46.20 |
| `shape_family_finish` | 36.19 | 20.5% | 1.60 | 10.46 |
| `endpoint_resolution` | 35.36 | 20.0% | 1.41 | 25.91 |
| `surrogate_assignment` | 17.89 | 10.1% | 0.76 | — |
| `runtime_catalog` | 13.60 | 7.7% | 1.00 | — |
| `shape_planning` | 4.26 | 2.4% | 0.78 | — |
| **`shaping`** | **176.36** | 100% | 1.19 | — |

S20 has the same shape: routing is 39.2% of shaping, `shape_family_finish`
18.1%, `endpoint_resolution` 20.9%.

Complete ingest at S22 took 392.37 s for 67,108,864 edges, or 171,000
edges/s. The floor allows 67.1 s. Shaping is 45% of complete ingest. Outside
shaping, the largest regions are:

- `append_edges`: 80.71 s at 0.90 cores, 72.98 s of it calling-thread CPU.
- `canonical_encoding`: 66.00 s at 2.07 cores.
- `publish`: 30.58 s at 0.35 cores.

### Gate result

**No shaping sub-phase dominates.** The largest, routing, is 39% of shaping.
The tracers' best-case gains on S22 complete ingest are:

- **Tracers 1 and 2** target load and sort inside `shape_family_finish`. That
  region is a fifth of shaping and already runs at 1.60 cores. Eliminating it
  entirely would give 1.10×.
- **Tracer 3** parallelizes routing's 46.20 s of calling-thread CPU. Eight
  ideal lanes would remove 40.4 s, giving 1.11×. Reaching #1448's 10%
  whole-ingest adoption gate would need near-linear scaling. No construction
  region has shown that: the best measured are 2.07 cores (`canonical_encoding`)
  and 2.89 cores (normalization).

Making all of shaping free would give 1.82×. The floor needs 5.85×.

## Decision

Shaping sub-phases stay serial within each stage. Parallelism stays where it
already is: the bounded load lanes, the seal lanes, and the encoders, all
under ADR 0047's one CPU budget. None of the three tracers is built.

Any future stage-internal lane must keep two invariants. Both describe how the
current lanes already work:

1. **Determinism by total order.** Shaped records are totally ordered by their
   leading 16-byte UUID key (`load_fixed_partition` sorts on it). A widened
   phase may vary chunk boundaries and lane counts only if final
   materialization merges in key order. It must produce byte-identical
   output. `fixed_partition_finish_is_schedule_independent_across_worker_counts`
   in `partition_load/tests.rs` is the standing proof. Any new shard
   dimension extends that test.
2. **Attribution by commutative merge.** Lanes return local counters, as
   `PartitionLoadCounters` and the seal-lane receipts already do. The
   coordinator merges them after the lanes join. No lane mutates
   `GraphConstructionEvidence`. The single clone/mutate/write-back in
   `finish_with_segments` is not copied anywhere else.

## Consequences

- #1387's floor work goes to removing per-record work on the serial spine
  first: `append_edges` and `shape_routing` calling-thread CPU, together
  119 s of the 392 s at S22. This follows the epic's rule of removing work
  before structural change. Its parallelism outcome stays unmet and is not
  waived.
- The 2026-10-04 allocator/clone profile was taken on `gf portable import`, not
  on the `import-session` construction path. It does not identify
  construction work to remove. A construction-path profile has to come first.
- Removing serial work raises routing's share of complete ingest. The
  `revisit_when` trigger is that share, so this decision is reopened on a
  measurement, not by opinion.
- ADR 0047's posture is unchanged: imports may take longer beside interactive
  queries, and one CPU budget covers every construction phase.
