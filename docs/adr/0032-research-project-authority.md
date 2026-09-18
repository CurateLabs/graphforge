---
title: "ADR 0032: Research Branches share Project publication authority"
adr: "0032"
status: "Accepted"
date: "2026-09-16"
superseded_by: null
---

# ADR 0032: Research Branches share Project publication authority

**Implementation:** Accepted design; M11 implementation pending.

**Decider:** Project maintainer, through the approved #1346 repair plan

## Context

The M11 [analyst requirements](../engineering/analyst-ux.md) require independently
evolving Branches and selective parent integration with durable acceptance
history. Independent research does not establish whether each Branch is a
separate durable container. Without a single authority, a crash could publish
parent changes while losing the corresponding acceptance record.

ADRs [0013](0013-project-generation-protocol.md) and
[0018](0018-acknowledged-durability-isolation.md) already establish complete,
authenticated generations and `CURRENT` as the publication linearization point.
The decision must preserve that authority and its pre/post-commit failure
semantics, rather than promise rollback after a committed publication.

## Options considered

- Shared Project authority: publish parent changes and the acceptance receipt
  together using the existing container protocol. Branches remain independent
  research contexts, but share publication coordination.
- Independent Branch containers: give each Branch a separate durable root, but
  require additional cross-container acceptance and recovery coordination.

The maintainer selected shared Project authority. Forks and exports serve the
independent-copy use case without adding distributed transactions to Branches.

## Decision

Branches belong to their Project's durable container. Branch heads, immutable
research Version records, and acceptance receipts participate in complete
authenticated generations under that Project's sole `CURRENT` authority.
Acceptance publishes the selected parent change and durable receipt together;
the source Branch's accepted-contribution status derives from this receipt,
not a second independently committed write. Reuse existing writer locking,
idempotency, and recovery rules.

Failure before linearization preserves prior state. Failure after linearization
requires reconciliation and a committed outcome, never rollback. Exact retry
returns the original receipt; changed content under the same operation identity
fails without mutation. A Fork has its own Project authority.

## Consequences

Parent state and acceptance history cannot disagree through a partial
cross-store commit. Branch publication shares the Project's coordination and
contention boundary; this decision does not promise independent writer locks
or a new isolation level per Branch.

Storage participants and retention are implementation work, not already shipped
APIs. #1350 establishes the foundation, #1352 builds Branch behavior, and #1356
proves atomic acceptance and recovery. The
[workspace contract](../book/architecture/research-workspaces.md) specifies
selected retention closure, comparisons, and consumer projections. Existing
accepted ADRs remain unchanged; checkpoints are storage facilities, not research
Branches, and assertion supersession branches are not persistent workspaces.
