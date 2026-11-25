---
title: "ADR 0060: Carry prepaid memory authority across reader handoffs"
adr: "0060"
status: "Accepted"
date: "2026-10-15"
superseded_by: null
revisit_when: "A migrated caller requires duplicate payload charges, premature release, a spill-policy exception, or an API that stock DataFusion does not publish"
---

# ADR 0060: Carry prepaid memory authority across reader handoffs

**Status:** Accepted

**Implementation:** Authenticated edge reads into single-hop execution apply this
contract at their storage boundary. The IPC spill-read integration that a
vendored `datafusion-physical-plan` carried was retired with the move to stock
DataFusion 56 (#2042, #2357): DataFusion owns its spill readers, merges and
exchanges, and GraphForge holds their queries to the public outcomes listed
under consequences.

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

Spilling sorts, aggregates and exchanges run on stock DataFusion. Their evidence
is completion within the query budget, the exact answer of a large-budget run, a
typed refusal where the budget or spill cap cannot be met, and removal of their
spill files; the order and size of DataFusion's internal reservations is not a
GraphForge contract.

## Related decisions

- [Read-plan resources](0026-read-plan-resources.md)
- [Lowering schema snapshot](0029-lowering-schema-snapshot.md)
- [Read-path selection](0050-read-path-fast-path-selection.md)
- [Execution resource policy](../development/execution-resource-policy.md)
