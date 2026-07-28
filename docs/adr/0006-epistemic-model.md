# ADR 0006: Append-only epistemic interpretation

**Status:** Accepted and implemented (epistemic model)

**Date:** 2026-06-07

**Reconciled:** 2026-07-25

**Build target:** v0.5.0

**Related:** ADR 0005, ADR 0012, ADR 0013

## Context

GraphForge must preserve how understanding changes, including rejected
interpretations and the evidence and reasoning behind them. That history cannot
be represented by mutable fields on graph rows or by rewriting an assertion.
It also must not alter Cypher, search, or analyst-algorithm semantics.

The knowledge layer supplies the immutable ledger: assertions, assertion-to-graph references,
confidence assessments, evidence links, provenance, and neutral algorithm-run
records. The epistemic layer adds interpretation as separate append-only records that reference
knowledge UUIDs. A knowledge assertion is valid without any epistemic status event.

## Decision

The epistemic model is an additive, append-only interpretation layer owned by `gf-knowledge`
and assembled through `gf-api`. It never changes a knowledge schema, row,
fingerprint, ordering rule, or identity.

The authoritative machine-readable contract is the generated
[Epistemic schema inventory](../reference/m21-schema-inventory.json). It is generated
only from the `gf-knowledge` registry; its checked SHA-256 digest makes
undocumented registry drift fail CI. All epistemic records use contract version `1`.

### Record families

| Record family | Purpose | Stable order |
| -------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------- |
| `assertion_status_events` | Explicit status for a knowledge assertion; an assertion may remain statusless | `(recorded_at, status_event_uuid)` |
| `reasoning` | Immutable reasoning content; amendments append a new row linked by `supersedes_reasoning_uuid` | `(recorded_at, reasoning_uuid)` |
| `assertion_supersessions` | Branch-preserving relation from a prior assertion to its replacement | `(recorded_at, supersession_uuid)` |
| `hypothesis_groups` | Immutable question identity | `(recorded_at, group_uuid)` |
| `hypothesis_membership_events` | Append-only add/remove membership events | `(recorded_at, membership_event_uuid)` |
| `hypothesis_selection_events` | Append-only selection events | `(recorded_at, selection_event_uuid)` |
| `assertion_validity_events` | Optional world-time intervals under `valid_time@1` | `(recorded_at, validity_event_uuid)` |
| `algorithm_interpretation_attachments` | Epistemic interpretation attached to an already durable knowledge-layer run | `(recorded_at, attachment_uuid)` |

The inventory, rather than prose tables copied into this ADR, is authoritative
for every field, Arrow type, nullability rule, capability version, record
version, fingerprint domain, and row limit.

### Status is explicit and optional

Status is never stored on `knowledge/assertions`. The current status at a
transaction cutoff is the maximum event at or before the cutoff by
`(recorded_at, status_event_uuid)`. No matching event means **statusless**, not
an implicit default. Resolution policy must explicitly reject, exclude, or
include statusless assertions.

Status changes append events. They do not update or retract the knowledge assertion.
Supersession is a separate atomic relation with its own linked terminal status
and reasoning identities. Multiple replacement leaves are preserved; a
projection must explicitly reject a branch or include all leaves.

### Reasoning and amendment

Reasoning rows are immutable. Correcting or extending reasoning appends a new
row whose `supersedes_reasoning_uuid` identifies the prior row. The prior
content remains queryable. Reusing an identity with different content is a
conflict, not an update.

### Competing hypotheses

A hypothesis group identifies a question. Membership and selection are
independent event streams. Their current values use the same deterministic
transaction rule: include records at or before the cutoff, then choose the
maximum event for each identity by `(recorded_at, event_uuid)`.

Storage does not silently pick among multiple groups or an unselected group.
Belief-projection policy must explicitly reject an absent selection or include
all current members. A selected assertion must be a current member.

### Transaction time and valid time are separate

`recorded_at` is mandatory transaction time on every record. It answers when
GraphForge recorded an event and is always reconstructed first.

Valid time is optional and uses the separate `valid_time@1` capability.
`assertion_validity_events` append half-open intervals `[valid_from, valid_to)`;
an omitted bound is open. Applying a world-time point occurs only after the
transaction snapshot has been reconstructed. Valid time may narrow that
snapshot, but it may never replace or discard transaction history.

The exact resolution order is:

1. pin one committed source generation;
2. reconstruct every knowledge/epistemic participant at the mandatory transaction cutoff;
3. resolve current status and supersession leaves with
   `(recorded_at, event_uuid)` tie-breaking, failing on ambiguity according to
   the caller's explicit statusless and branch policies;
4. apply optional `valid_time@1` eligibility to those assertions;
5. resolve hypothesis membership and selection, then apply the explicit
   hypothesis policy and reject contradictory multi-group votes;
6. materialize a graph-only projection and compute its content fingerprint;
7. recheck the pinned generation before returning.

The resolved projection records the canonical policy bytes and fingerprint,
snapshot fingerprint, optional valid-time fingerprint, graph-content
fingerprint, and sorted source-record UUIDs.

### Neutral algorithm execution and attachment

Epistemic resolution finishes before algorithm dispatch. The five analyst-verb families—`rank`,
`cluster`, `similar`, `paths`, and `analyze`—receive only the materialized
graph-native projection and their normal typed inputs. They never open
knowledge tables, and their Arrow schemas never acquire conditional epistemic
columns.

Successful execution first publishes the neutral knowledge-layer algorithm run. The epistemic layer then
publishes `algorithm_interpretation_attachments` referencing that run.
Attachment failure cannot roll back or erase the successful knowledge-layer run. Retrying
the same attachment identity and content is idempotent; conflicting reuse is
rejected. Cancellation after publication begins cannot promise rollback.

### Capability and security boundaries

- `epistemic@1` owns all epistemic families except validity events.
- `valid_time@1` is independently opt-in and depends on the compatible
  epistemic and knowledge capabilities.
- Graph-only projections contain topology, properties, and a filtered runtime
  catalog; they do not copy `knowledge/`, `provenance/`, search indexes,
  credentials, or unrelated sidecars.
- Resolution is bounded by the registry row limits and fails closed on
  malformed, dangling, cyclic, ambiguous, or conflicting records.
- Rust owns validation, ordering, canonical bytes, persistence, and execution.
  Python and Node expose thin projections of the same contract.

## Preservation-over-deletion

- knowledge-layer and epistemic records are never destructively updated or deleted.
- Statusless assertions remain valid ledger entries.
- Status, reasoning amendments, supersession, membership, selection, and valid
  time append new records.
- Rejected and superseded interpretations remain auditable.
- Optional world-time filtering never removes transaction-time history.

## Verification

The implementation contract is exercised at the public Rust API and through
the native Python and Node bindings:

- `gf-knowledge` tests validate each Arrow schema, canonical fingerprint,
  ordering rule—including identical-time UUID tie-breaking—idempotency rule,
  and conflict path.
- `gf-api::belief_projection` tests cover statusless assertions, retraction,
  supersession branches, competing hypothesis
  groups, half-open valid time, generation pinning, all five analyst-verb families,
  reopen determinism, graph-only isolation, and attachment retry.
- `crates/gf-bindings-py/tests/smoke.py` and
  `crates/gf-bindings-node/tests/belief-projection.test.mjs` execute the native
  binding examples against built artifacts.
- the repository-policy CI job verifies the generated epistemic inventory and digest.

## Consequences

GraphForge can reconstruct and analyze a deterministic belief state without
making belief mutable or contaminating graph computation. The cost is an
explicit policy surface and additional append-only tables; ambiguity is
reported rather than hidden behind defaults.

## References

- [ADR 0005: Layered Architecture](0005-layered-architecture.md)
- [ADR 0012: knowledge/epistemic Domain Ownership](0012-m20-domain-ownership.md)
- [Immutable knowledge ledger](../book/architecture/knowledge-ledger.md)
- [Algorithm Verbs](../book/architecture/algorithms.md#invocation-and-knowledge-layer-boundary)
- [Execution Model](../book/architecture/execution-model.md#reproducible-algorithm-execution)
- [Epistemic schema inventory](../reference/m21-schema-inventory.json)
