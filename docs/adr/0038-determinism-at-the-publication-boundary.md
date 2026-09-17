# ADR 0038: Determinism belongs at the publication boundary

**Status:** Accepted

**Date:** 2026-09-17

**Build target:** v0.6.0 and later

**Related:** [ADR 0013](0013-storage-threat-model.md) (storage threat model);
issues #1416 (a wall clock is written into the runtime catalog), #1387 (ingest
throughput), #1429 and #1448 (construction concurrency attempts), #1456
(reuse Arrow/DataFusion where determinism and durability allow)

## Context

The construction path's determinism contract was written as: *within a fixed set
of recorded session parameters, identical logical input produces byte-identical
shaped and encoded artifacts, across separate sessions and separate project
directories, at every recorded partition count, and across an
interrupted-and-resumed run.*

Two things are wrong with that as a contract.

**We do not meet it.** #1416 records that shaped outputs are not byte-reproducible
across sessions on `main`, because `ConstructionShape::runtime_catalog_now_micros`
is `SystemTime::now()` at session open and is written into
`shaped-runtime-catalog.parquet`. The determinism suite passes by *pinning the
session clock*. Byte-identity across sessions is a test fixture working around a
defect, not a property the system holds.

**It is stronger than anything depends on.** Byte-identity of every *intermediate*
at every partition count is a cheap total check, which is why it was written. But
shaped intermediates are transient: `recover_shape_intent`
(`graph_construction/recovery.rs`) unlinks every `part-`, `staged-` and `shaped-`
name on an incomplete run. Nothing outside the construction session ever observes
them.

The cost of the overreach is concrete. Every attempt to parallelise construction
(#1429, #1448) has been constrained by the need to reproduce a byte stream under
an arbitrary schedule, and the evidence architecture that supports it — running
peaks over a global install-minus-unlink total, an ordered transition log — is
schedule-dependent by construction.

## Decision

**Determinism is asserted at the publication boundary, not on intermediates.**

Three properties must hold, and they are what the contract now says:

1. **A resumed run produces the same graph as an uninterrupted one.** Crash
   recovery correctness.
2. **The same logical input produces the same query answers.** The product
   guarantee.
3. **A content-addressed digest names the bytes it claims to name.** The CAS
   invariant, unchanged, and still SHA-256 wherever a digest is an identity.

Consequently:

- **Published artifacts** — topology, identities, the membership index — remain
  canonically ordered and byte-stable. `load_partition` already sorts by whole
  record before concatenation, so the canonical order exists; this ADR moves
  where the contract is drawn, not how the bytes are produced.
- **Shaped intermediates** may differ by execution schedule. Spill insertion
  order, partition completion order and worker count are scheduling decisions
  and are not recorded format parameters.
- **Recorded format parameters stay recorded.** Partition count and the UUID
  splitters are durable; worker count and execution partitioning are not. These
  are three distinct numbers and must not be conflated.

## The check that replaces byte-equality

Dropping intermediate digest comparison without replacing it would remove the
only total check on concurrency. **It is replaced, not dropped.**

Semantic equivalence at the publication boundary: identical node and edge sets,
identical adjacency, and identical answers for the ladder's recorded queries —
compared across forced worker counts, across recorded partition counts, and
across an interrupted-and-resumed run.

This is more expensive than a digest diff and checks the property rather than a
proxy for it. That trade is deliberate, and it follows three changes landed the
same day that made the same move: a growth assertion replaced by a conservation
law (`read_bytes == 2 * write_bytes + route_table_bytes`), a partition-balance
row-count proxy replaced by distinct keys per partition, and an observed call
floor re-derived from the contributors that remain.

**Assert the property, not the observation.**

## Consequences

**Enables.** Partition-local and family-local execution may reorder freely. The
evidence architecture can be split into performance accounting (order-free, and
a candidate for an execution framework's own metrics) and durability accounting
(small, and redefinable order-independently: a peak over a partitioned total, a
transition log keyed by artifact rather than ordered by time). Both are
prerequisites for #1456.

**Costs.** The determinism suite gets slower and more complex. A concurrency
defect that does not reach published bytes is no longer caught by a digest diff
and must be caught by the equivalence check or not at all. The suite must
therefore exercise forced worker counts explicitly rather than relying on
incidental scheduling.

**Does not change.** Authentication, receipts, recovery, fail-closed
publication, unsupported-format refusal, and collision resistance where a digest
names a content-addressed object. #1416 remains a defect worth fixing on its own
terms: a wall clock in a durable artifact is wrong regardless of where the
determinism contract is drawn.

**Superseded expectation.** Any test, comment or brief asserting that specific
shaped-artifact digests are fixed values is obsolete. The digest table in the
header comment of `construction_determinism_tests.rs` documents a measurement
taken on `13632d4b`, before the external merge tree was removed; two of its four
rows were already stale. No test asserted those values, and none should.
