---
title: "ADR 0043: Proposal acceptance shares the Project publication owner"
adr: "0043"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0043: Proposal acceptance shares the Project publication owner

## Context

Issue #1356 requires immutable selected proposals, partial review and exact
contribution mappings that survive restoration. Branch publication, retained
selected content, comparison and contextual decisions already have native Rust
owners. Acceptance must not publish parent content before its receipt or import
unselected research. Correctness is regime A: deterministic native execution,
failure injection and durable reopen establish the contract.

## Decision

Extend the existing research publication through the Project's sole CURRENT.
Prepare selected frozen payloads and destination content privately under CAS
publication leases. Publish the accepted destination, review event, accepted
contribution mappings, retention roots and operation receipt in one generation.
For a Project destination, prepare from its pinned complete content and replace
its content participants in that publication, preserving current research history.
For a Branch destination, advance only that Branch's effective Version.

Frozen proposal payloads are selected projections in distinct contexts. They do
not advance an analyst Branch head and cannot masquerade as complete Branch or
Project restore sources. Original Branch and exact source Version remain explicit
provenance. Inserting authenticated retained content and advancing a context head
are separate operations. A proposal retains its selected closure, not the entire
evolving source. Accepted evidence has its own selected retention root, independent
of pending or rejected content.

Review binds the exact frozen selection and current destination revision. Later
destination changes require a fresh preview. Each selected item has an explicit
decision; accepted dependencies must be available in the destination or accepted
by the same review. Dependency discovery reports requirements without silently
adding private annotations, unselected claims or canonical authority. Native
domain owners apply selected changes and validate the prepared destination.

The restore-preserved research history owns proposals, immutable review events
and accepted mappings. Deduplication identifies a tagged destination authority,
stable contribution and exact typed value or deletion commitment. A stable
contribution identity alone does not distinguish later edits. Child-to-Branch
acceptance therefore cannot suppress that contribution's first acceptance into
the Project. Historical receipt and deduplication evidence remain independent of
whether an obsolete frozen payload is released. Capacity limits refuse new work
explicitly; cleanup never silently expires immutable identity evidence.

Integration and canonical promotion remain separate explicit decisions. Source
canonical status is provenance, not destination authority. Review actor and policy
metadata record the decision; Core does not authenticate remote reviewers.

Exact public intent is checked before stale-state checks and private preparation.
Before CURRENT replacement a failure preserves the old authority. After replacement
the facade reconciles committed content and receipt, including refreshed Project
graph state. A changed request under the same operation identity fails with
GF_IDEMPOTENCY_CONFLICT. Thin Python, Node and CLI surfaces transport this native
contract and its Arrow views.

## Alternatives

Publishing content and then recording acceptance would reuse two existing calls
but exposes an authoritative state without its durable review receipt. A separate
proposal database would add another recovery and restoration authority. Both are
rejected. Extending the existing transaction owner keeps one linearization point
and reuses its authenticated content, cancellation and recovery machinery.

## Consequences and proof

Registry transitions must preserve earlier committed review and mapping records.
Accepted units and evidence must agree with the prepared destination. Selection
must filter field baselines as precisely as payload fields. Retention tests must
measure actual successive facade proposals, released roots and compacted/reopened
bytes, rather than substituting storage fixtures for lifecycle evidence.

Implementation proceeds through a native frozen submission and one atomic partial
acceptance, then dependency, deduplication, restoration and lifecycle regressions.
A real two-story/shared-character facade example precedes interchange work #1357.
This ADR records the design; issue closure still requires its acceptance tests,
public contract and binding parity, review and the repository merge gate.
