---
title: "ADR 0047: Over-budget construction partitions succeed; one CPU budget per instance"
adr: "0047"
status: "Accepted"
date: "2026-09-24"
superseded_by: null
---

# ADR 0047: Over-budget construction partitions succeed; one CPU budget per instance

**Status:** Accepted

**Implementation:** Pending. #1585 implements decision 1 and #1586 implements
decision 2. Until they land, production behaviour is unchanged.

**Build target:** v0.6.0

**Related:** ADR 0046 (construction reuse decisions; this record takes the two
decisions it reserved for maintainers), ADR 0038 (determinism at the
publication boundary), ADR 0045 (ingest authentication regime); #337
(per-instance execution resource policy); #1448 (shaping parallelism); #1504
(construction reuse epic).

## Context

ADR 0046 kept construction's own machinery and left two decisions to the
maintainers, because each changes an existing requirement:

1. Must an over-budget partition succeed rather than refuse?
2. Should concurrent imports share one CPU budget?

**The refusal.** Construction routes endpoint records by node UUID, two per
edge, so every record of one node lands in one partition. A partition whose
materialization exceeds the recorded `max_partition_bytes` refuses the whole
ingest before allocating; the default budget is 256 MiB. At 33 bytes per
endpoint record, one node with roughly 8.1 million edges fills the budget on
its own. More ranges cannot split it and more RAM does not raise the recorded
budget. The measurements behind this record
(`docs/development/evidence/partition-refusal-1584.md`) show:

- A 9,000,000-edge star graph, one hub with every other node linked to it, is
  refused: `partition materialization requires 297536415 bytes, exceeds
  recorded budget 268435456`. A 4,000,000-edge star publishes. A class node in
  a knowledge graph, linked to every entity of its type, has this shape. One
  such hub refuses an ingest of roughly the S19 Graph500 rung's edge count.
- Graph500 inputs grow their largest hub by 1.52× per scale step, measured
  from S18 to S26, as the generator's parameters predict. At S26, the
  certification target, the largest hub has 1,709,763 edges, and the modelled
  largest partition is 73.7 MB: 27% of the budget. The model fits the measured
  S18 and S20 partitions within 6%.
- The #1507 DataFusion external-sort adapter, run with its pool equal to the
  default budget, **failed** on the 9M star with DataFusion's own
  `Resources exhausted` during the merge. With 64 MiB and 8 MiB pools it
  published the same answers. Every earlier run that spilled had used a pool
  of 2 MiB or less, so a spilling sort under a large pool had never been
  tested. The cause inside DataFusion is not isolated.

**The CPU budget.** Each `GraphForge` instance sizes one private CPU pool from
`compute_threads` (#337). Query kernels run on it, and import normalization
has run on it since #1472, with nothing reserving a share for queries. The
finish-time partition loads use private threads outside any budget, and #1448
is about to add shaping lanes. #1508 F12 measured the trade: a shared budget of
two held two concurrent imports to two cores, at 35.5 ms against 19.4 ms on
private pools.

## Decision

### 1. An over-budget fixed-width partition is processed externally

A fixed-width construction partition whose materialization exceeds the
recorded `max_partition_bytes` is sorted with bounded memory and streamed to
its shaped output, instead of refusing the ingest. The budget keeps bounding
memory; it stops bounding which inputs are accepted.

Refusal remains, as structured errors that publish nothing, for:

- exhausted scratch disk under a recorded limit;
- cancellation;
- integrity failures in scratch or inputs.

The mechanism is not chosen here. #1585 compares the #1507 DataFusion adapter
with a native bounded external merge over GraphForge's sealed segments, under
the #1505 protocol, before measuring. Whichever wins must meet every
obligation below.

- **Recorded parameter.** External processing is a recorded construction
  parameter, so a resume validates it like any budget. A session recorded
  before it resumes under its recorded refusal contract.
- **Owned scratch.** Scratch is created through the construction directory and
  reclaimed at recovery. It is never recovery authority.
- **Bounded scratch.** A scratch disk limit derived from the allocation ledger
  refuses cleanly when exhausted, and scratch bytes are accounted in
  construction evidence.
- **Integrity.** Scratch is checksummed, or checked by the #1507 record-multiset
  guard, before anything derived from it is published.
- **Tested pool size.** Any library pool is a sub-budget whose size is chosen
  from tests at the partition sizes it will meet, not set equal to
  `max_partition_bytes`.
- **Thread-based coordinator.** The coordinator is a plain thread, because a
  streaming merge that blocks on its own runtime cannot run under a Tokio
  coordinator (#1509).
- **Unchanged resident path.** Partitions within budget keep today's resident
  path, and publication stays byte-stable under ADR 0038.

### 2. One CPU budget per instance, shared by queries and construction

An instance has one CPU budget, sized from `compute_threads`. All
CPU-parallel work in the instance draws from it:

- query kernels;
- import normalization;
- finish-time partition loads;
- #1448's shaping lanes.

Construction may hold at most `compute_threads - reserve` of it, with a reserve
of at least one, so queries always have a share while an import runs. Work
that blocks on file I/O runs on construction-owned threads holding admission,
never on the `ComputePool` workers that queries depend on. Admission is
scheduling only: published bytes and construction evidence must not depend on
the budget. Waiting for admission is cancellable.

#1586 implements this, chooses the default reserve from measured evidence, and
exposes it in the resource policy and its diagnostics. #1448's lanes draw
from it, so #1586 blocks #1448.

## Scope limits

- **Row partitions.** Arrow property-row partitions keep their refusal. They
  were never evaluated under an external path; the question stays open under
  #1504.
- **Instance, not process.** The budget is per instance, matching the
  resource policy's scope since #337. Two instances in one process each keep
  their own. A process-wide budget is not decided here.
- **Graph500 claim.** The Graph500 growth model predicts that the largest
  partition exceeds the default budget at S29, and that every endpoint
  partition exceeds it at S30, where the 4,096-range maximum stops growing.
  Both are model outputs beyond anything measured.

## Relation to ADR 0046

This record resolves the two decisions ADR 0046 reserved. It replaces that
record's "Over-budget partitions: retain refusal as the contract" row with
decision 1. Every other ADR 0046 decision stands, including the retained
production load pool; decision 2 constrains how that pool and #1448's lanes
are admitted, not which scheduler runs them.

## Consequences

- A graph with a hub of any degree ingests, bounded by scratch disk, once
  #1585 lands. Until then the refusal stands and the architecture page says so.
- #1582 no longer retires the #1507 adapter until #1585 chooses between it and
  a native merge.
- #1448's lanes are built against #1586's admission, not retrofitted to it.
- Imports can take longer beside interactive queries, by design.

## Revisit when

- #1585's comparison finds neither mechanism can meet the obligations above
  at an acceptable measured cost. The decision then returns to the
  maintainers with that evidence.
- Multi-instance processes become a supported deployment, which raises the
  process-wide budget question.
- Row partitions are evaluated under an external path.

## Evidence

| Evidence | What it establishes |
| --- | --- |
| `docs/development/evidence/partition-refusal-1584.md` | Graph500 hub growth S18–S26, the partition-size model and its fit, the star-graph refusal, and the #1507 adapter's failure under a large pool and success under smaller ones |
| `docs/development/evidence/construction-reuse-integrated-1509.md` | The hybrid's measured cost at a forced 1 MiB budget, and the Tokio-coordinator incompatibility |
| `docs/development/evidence/construction-scheduling-spike-1508.md` | F12 shared-admission measurement; F13 on the API runtime's blocking pool |
