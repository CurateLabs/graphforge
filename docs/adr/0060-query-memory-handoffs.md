---
title: "ADR 0060: Carry prepaid memory authority across reader handoffs"
adr: "0060"
status: "Accepted"
date: "2026-10-15"
superseded_by: null
revisit_when: "Either migrated caller requires duplicate payload charges, premature release, a spill-policy exception, or a vendor-only API in a published GraphForge crate"
---

# ADR 0060: Carry prepaid memory authority across reader handoffs

**Status:** Accepted

**Implementation:** Authenticated edge reads into single-hop execution and private
IPC spill reads into merge execution apply this contract at their existing
dependency boundaries.

## Context

The execution session already supplies one query memory pool. Its admission policy
includes sort headroom and aggregate floors and loans. Reader workspace, returned
Arrow buffers and downstream copies have different lifetimes, but a numeric
reservation held by the producer does not describe that handoff. Reconstructing a
source charge downstream can duplicate accounting; releasing the producer's charge
can leave aliases unaccounted for. Holding a whole read envelope through the last
output can also retain workspace and future-output credit after they are unused.

Pinned Arrow pool callbacks are infallible. They can attach previously admitted
authority to immutable buffers, but cannot implement fallible admission. Claiming a
buffer replaces its previous token, so unknown sponsorship cannot be overwritten.
Generic RecordBatch streams carry no ownership capability alongside their batches.

## Options considered

- Continue separate operator-specific handoff mechanisms. This limits each change,
  but repeats lifetime rules and reconstruction of source ownership.
- Use one ownership contract over existing reservations at controlled reader
  boundaries. This preserves policy and permits incremental migration, but still
  requires kernel-specific allocation bounds and explicit provenance.
- Replace the execution engine or allocator. This expands scope and does not by
  itself provide fallible admission for the current kernels.

## Decision

Use the second option. A protected allocation first acquires a checked reservation
from the existing pool. Transfer prepaid authority without releasing and reacquiring
it. A source alias retains its original physical backing charge; a fresh copy is
funded while its sources remain alive. Count full backing capacity, including
unused view payloads and slices. Keep metadata, active workspace and retained output
owners separate. Retire unused producer credit when actual production ends, while
escaped buffers retain only their own charges.

Storage owns the prepared authenticated file, reader metadata, projection,
selection and fallback description used by admission and execution. The initial
edge migration retains the existing conservative per-file decoder bound. It seals
returned ownership and releases that file's workspace before opening the next
file. A fallback is admitted before allocation and preserves authentication,
corruption checks, metrics and result semantics.

IPC uses a private synchronized credit bank with numeric tickets. Its control
metadata is separately admitted. While production is open, payload authority equals
available credit plus outstanding tickets. Tickets split
and return without pool callbacks. After the last reader and started blocking
worker finish, the bank releases unused credit and rejects new borrowing; returned
tickets then release their authority. Buffer tickets never retain producer permits.
Reader-root registration completes before prefetch. Partial construction, error,
cancellation and retained exhausted streams follow the same retirement rule.

Construction and resident ownership use separate phases. A parsed IPC schema or
decoded array keeps its complete preparation grant until construction ends, then
transfers only a checked resident bound. A merge copy admits source preparation,
the complete serial kernel peak and output reconciliation in the same reservation;
each transition preserves sources and earlier outputs. Qualified uncompressed IPC
frames use one full body allocation after a recursive descriptor check proves
that Arrow needs no alignment copies. Other frame layouts retain their existing
conservative bounds. Controlled Row copies require a checked payload bound before
executing an interleave kernel; unproved payload types fail with a named resource
refusal. Offset overflow still retries a smaller output prefix before allocation.

The controlled merge retains an input batch only while a future cursor or pending
row index needs it. It releases consumed inputs before refilling and after copying
emitted rows. Independent key cursors remain through their last comparison;
controlled Row streams release cached keys after actual input EOF. Escaped buffer
tokens retain their own lifetimes. Aggregates release the consumed input handle
when grouping ends, before state reservation or spilling; genuine state aliases
retain their own buffer tickets.

The qualified private Repartition FIFO donates one independently admitted,
nonspilling writer workspace after encoding and flush, before publishing the
batch. Later writes and reads borrow available credit and admit any fresh deficit;
they never merge more donors into that bank. One consumer permit spans file
rotations and started workers. Actual EOF, error or cancellation retires unused
credit, including when the exhausted outer stream remains alive. This does not
guarantee that concurrent writer and reader workspaces fit every constrained pool.

A buffered producer reserves an existing channel slot before polling its input.
That slot covers a queued or in-flight item, so a full queue cannot retain an
additional decoded batch awaiting send. Channel capacity, batch size, merge fan-in
and query-pool policy are unchanged. Dropping the receiver cancels the producer
through its existing task owner; started blocking work retains its own lease
until it exits.

Only a known nonspilling sponsor may enter the new IPC transfer path. Check this
before moving reservation authority. Spillable and unqualified sources retain their
existing protections: splitting changes reservation-local sizes observed by the
pool's sort-headroom and aggregate-release rules. Do not change those rules to
accommodate the ownership adapter.

The common abstraction is this conservation and lifetime contract over stock
DataFusion reservations. The storage integration and vendor IPC implementation stay
at their existing dependency boundaries. No new pool, crate, global pointer registry
or public vendor API is required. Published GraphForge crates declare their own
Arrow feature requirements and compile against stock registry DataFusion. Private
vendor behavior applies to builds selecting that vendor; packaging compatibility
does not claim that crates.io consumers receive the vendor patch.

## Consequences and limits

Local reconciliation may identify already sponsored buffers only while their source
owners are pinned and provenance remains sealed. It cannot infer sponsorship from
arbitrary pointer addresses or raw batches. Unknown paths keep their conservative
accounting. Dictionary reuse retains prior source owners through reconciliation;
ownership tokens must not retain the Arrow arrays that contain them.

Buffer tokens follow clones and slices. They cannot observe arbitrary public
RecordBatch/array wrapper drops, bufferless Null arrays or empty Structs. Internal
headers and bufferless results therefore require an explicit batch or queue owner;
this decision does not promise universal accounting of public wrapper clones.

Kernel bounds, growth overlap and codec workspace still need source-backed proofs.
The query pool is not a universal process RSS cap. Lower reservation counts or a
typed refusal do not satisfy required query completion. The original constrained
completion fixtures, exact-result controls, worker/alias cleanup tests, spill-policy
regressions and extracted-package builds remain the acceptance evidence.

## Related decisions

- [Read-plan resources](0026-read-plan-resources.md)
- [Lowering schema snapshot](0029-lowering-schema-snapshot.md)
- [Read-path selection](0050-read-path-fast-path-selection.md)
- [Execution resource policy](../development/execution-resource-policy.md)
