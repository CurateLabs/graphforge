---
title: "ADR 0041: Branch state publishes through Project CURRENT"
adr: "0041"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0041: Branch state publishes through Project CURRENT

## Context

Issue #1352 requires independently evolving Branch research with shared selected
retention, exact lineage and scoped restoration in one Project container.

## Decision

A Branch is an independently advancing research context inside one Project. Its current immutable Version commits the complete effective Branch research state: selected graph rows, exact ontology composition, selected domain/evidence closure and Branch-local changes. Cypher and analyst operations use that effective graph. There is no alternate binding engine and no mutable read through an upstream parent.

The Project's authenticated Branch metadata participant records identity, immediate parent/base Version, ultimate Project lineage, creator/time, creation selection commitment and lifecycle policy. The existing research registry owns the Branch context head, immutable Version records, required roots and operation receipts. Duplicate mutable head fields are avoided; readers resolve the head from the registry.

Origin/baseline/change records are Parquet, committed by the corresponding immutable Branch Version. Each object/field records immutable original source context/Version, the incorporated revision/value commitment and a stable contribution identity where changed. Origin never advances when a baseline later advances. Contribution identity survives nested acceptance; destination-scoped acceptance is a later operation-history mapping, never a replacement for contribution identity. Suppression is a recorded change with an effective graph/ledger result, not deletion of origin identity.

Creation from a frozen Slice authenticates its source Version and commitments, checks all selected identities, and derives required closure using domain owners. The capsule's digest is not a signature. The initial Branch keeps selected baseline bytes and selected immutable payloads in Project CAS; source Version genealogy alone does not retain the complete source. Creating a whole-Project Branch intentionally selects the whole recorded state. Creating a child Branch freezes the immediate parent's exact Version and carries ultimate Project lineage separately.

Preparation materializes a retained Branch Version into a private process-owned native facade, applies existing Rust mutation/ontology methods there, and derives the new effective state. This temporary container has no durable authority and no externally visible Branch head. A bounded publication installs required immutable objects in Project CAS while holding object-publication leases, then atomically publishes the new Version, Branch metadata/baselines, retention changes and operation receipt through the existing writer/admission/recovery path. The parent graph remains the current Project graph. A prepared temporary generation UUID must never become a durable source-generation root.

Pre-CURRENT failure preserves old authoritative state; unreferenced installed objects remain ordinary cleanup candidates. Post-CURRENT failure preserves the committed state and original error classification, reconciles the facade, and exposes the receipt on exact retry. Operation identity binds request semantics and relevant preconditions; a changed request under the same identity conflicts. Independent handles use optimistic expected-CURRENT checks; no blind last-writer-wins behavior.

Restore creates a fresh Version under the same Branch context and moves only that head. It does not rewrite the immutable source, Project research, another Branch head or Project operation/acceptance history. Branch lifecycle/history participants must be classified as current history for complete Project restoration as well. Later #1356 acceptance mappings remain outside frozen research and must survive restoration.

Reference records an outside origin locator and never imports active content. Bring requires explicitly retained source history, authenticates selected objects and closure, preserves graph UUIDs and origins, and publishes only that expansion. If source history is unavailable, refuse instead of reading current parent bytes. Local ontology extensions use exact composition IDs and scoped validation; runtime catalog IDs never stand in for ontology IDs.

Limits apply to Branch count, operation receipts, retained Versions, selected objects/rows, prepared bytes and returned Arrow pages. No time-based replay expiry or silent receipt eviction is introduced. Baseline roots retain selected state needed for future comparison. Deliberately retained named historical Versions are separate roots. Changes to any capability/record version are documented before implementation and unsupported prior formats fail closed under the pre-v1 policy.

Acceptance evidence must include actual Rust-facade parent/A/B edits and scoped A restoration after independent parent/B advances and reopen; graph-versus-assertion suppression; Reference versus Bring; nested origin identity; Branch-only ontology; fault/retry/conflict behavior; and fixed-selection parent-growth measurements after release/compaction/reopen. Record source scope and actual copied/reused/retained bytes separately; do not infer constant creation work from constant retained bytes. Python, Node and CLI must execute the same Rust behavior.

## Capability amendment

Research capability and registry revision 3 add immutable Branch creation records
to the authenticated current registry. Context heads remain the sole mutable
head mapping. Branch research state uses an authenticated Parquet participant
for object/field origin, baseline and local change records. Registry revision 2
is not read as revision 3; the pre-v1 policy requires explicit refusal instead
of migration or fallback. Producer identity names revision 3. Branch registry
records cannot be removed or rewritten by unrelated publishers or restoration.

The metadata record stores only control identities and commitments. Data-bearing
selection/baseline/contribution rows remain Parquet and their public projections
remain Arrow. No new directory outside the existing authenticated Project state
and CAS becomes an authoritative store.

## Consequences

Branch reads and edits may materialize their selected closure into private
process storage. This is not a zero-copy read claim. Selected creation must
measure actual source scope and copy/reuse work as well as retained payload.
Version/root/receipt limits remain explicit; receipt cleanup cannot silently
weaken exact replay or later destination-scoped contribution deduplication.

This decision precedes implementation. Real Branch acceptance tests, including
two-Branch restoration and reopen, are required before #1352 closes.
