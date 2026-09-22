---
title: "ADR 0039: Research Versions share Project publication authority"
adr: "0039"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0039: Research Versions share Project publication authority

**Implementation:** Storage foundation in #1535; selected physical retention in
#1536; public facade and bindings in #1537. #1350 remains the close gate.

## Context

Checkpoint names retain whole generations and checkpoint restoration replaces
the whole workspace. Neither behavior is a research Branch. Research needs
independent immutable content identities, independently advancing context heads,
and operation history which restoration cannot rewind. ADR 0018's one
publication authority and acknowledgement boundary still apply.

## Decision

Add the mandatory `research@1` capability and authenticated `registry@1` JSON
participant. It contains bounded immutable Version descriptors, context heads,
retention roots and durable operation receipts. Every change carries the full
current Project participant set through the existing writer-lock, validation,
publication and recovery protocol. There is no second commit pointer.

A Version identifies frozen content, not its storage generation, checkpoint
name, package digest or current head. Its content commitment includes the exact
selected participant descriptors and complete-versus-projected scope. A
projection identifies its source Version as provenance and has a distinct
identity; conflicting content under an existing Version identity is rejected.
The descriptor records producer and required capability identities. Unsupported
readers fail closed; no pre-v1 migration or perpetual reader compatibility is
promised.

Context heads select immutable Versions. Restoring a context publishes a new
Version and advances only that context's head. Other heads, retained Versions,
and current operation/acceptance history remain unchanged. These storage
contexts are foundation fixtures until the Branch lifecycle owns their public
meaning. Project-level restoration must explicitly replace Project research
content and retain current history; legacy whole-workspace checkpoint revert
must not rewind a research registry.

The Project-level scope, implemented by the public integration in #1537, is
the complete frozen Project participant set (graph, research metadata,
ontology composition, Sources/Artifacts and local change records). It must
preserve stable Project identity and current research registry, independent
context heads, receipts and accepted provenance. Context restoration in #1535
changes only that context head and its frozen content reference; it does not
replace the live Project's participant set.

The Source/Artifact owner supplies the exact evidence dependency closure to the
storage operation. Storage authenticates local digest/length references and
marks their objects during cleanup. External-only and unverifiable references
remain explicit frozen disclosures and are never fetched. This keeps domain
ledger interpretation out of storage; facade integration must collect the
complete owner-validated closure before invoking the primitive.

Receipts are outside Version-frozen content. Exact operation retries return the
original receipt after subsequent publications and restoration. Changed request
content under an operation identity returns `GF_IDEMPOTENCY_CONFLICT`.
Pre-linearization failures leave prior state; post-linearization errors retain
the existing truthful committed diagnostic and reconciliation through reopen.
Receipt retention lasts for the Project's lifetime, with an explicit finite
registry capacity and resource-limit refusal before mutation. There is no
implicit expiry or checkpoint-tombstone replay window. Receipt retention alone
does not claim perpetual availability of released Version payloads. Accepted
contribution deduplication remains a separately retained dependency owned by
the later Proposal implementation.

## Retention and staged implementation

Retained Versions and context heads are roots. Required references must exist
and agree with their immutable identity; provenance-only identifiers are not
additional roots. Deletion reports actual head/root blockers. Local evidence
and ontology participants are retained with graph content; external-only bytes
remain external and are never fetched as a historical replacement.

The foundation conservatively pins each Version's authenticated source
generation. This makes foundation publication/reopen safe but does **not** meet
#1350's bounded selected-retention criteria. #1536 must replace that physical
representation with selected closure and repacking, prove increasing-parent
and increasing-history retained bytes, and preserve these identity, history
and restoration rules. #1350 cannot close on the foundation alone.

## Alternatives and consequences

Reusing checkpoint names would conflate mutable naming with immutable identity
and whole-workspace restoration with context restoration. A separate Branch
container would introduce a second publication authority. Both conflict with
the existing requirements, so neither is used.

The bounded registry adds metadata publication cost. Explicit capacity refusal
is preferable to silently losing replay or accepted provenance. Later storage
representation changes must be versioned and must not weaken authentication,
root closure or truthful commit reporting.
