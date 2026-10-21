---
title: "ADR 0044: Research interchange preserves content identity separately from authority"
adr: "0044"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
revisit_when: "A round trip fails to stay complete, disjoint, and redacted, or a Fork loses its required independence from installed authority"
---

# ADR 0044: Research interchange preserves content identity separately from authority

## Context

Issue #1357 requires complete and selected research interchange, explicit Fork
governance, and citations that distinguish a Branch head from an immutable
Version. Existing native owners validate selected content, immutable identities,
contribution mappings, portable components, and Project publication. Correctness
is regime A: execution, hostile-input refusal, failure injection, and reopen.

A research registry is also an operational authority: its Branch heads and
retention roots keep local research active. Copying the whole registry would
include unrelated history and retain ancestors outside the export selection.
Copying only graph bytes would lose Version identity and research provenance.

## Decision

Use the existing portable manifest and authenticated versioned components.
Register a research component containing native immutable content commitments
and an explicit bounded closure of required payloads. Complete materialization
preserves the original Version record and identity commitment. Every subset or
redaction receives a distinct projection identity and cites its source Version.
Package and transport digests remain separate from research identities.

Record the original Project citation for each retained Version. A Fork can create
local research while retaining acceptance evidence from another Project; those
Project identities and genealogy remain distinct. Conflicting citations for one
immutable Version are rejected.

Ordinary portable imports carry selected genealogy and acceptance evidence as
historical provenance, not live Branch heads or destination governance decisions.
A discovery-verified same-Project Branch clone additionally publishes one explicit
native continuation transition: its exact original Branch record, UUID and
selected head become live. Immutable-Version selection stays detached. Other
published refs are durable read-only remote citations, not native Branch creation
records or heads. They grant no retained payload, access or publication authority. Historical
ancestor references need identity metadata; they do not require full ancestor
payloads. Interchange contract 2 separates identity-verified ancestor records
from the exact `required_versions` payload closure. Imported metadata is inspectable
but cannot be opened as retained content. A transient Project capture deliberately
released by root Branch creation can remain an identity-only boundary anchor;
its native Branch origin and permanent identity ledger must prove the boundary.
No author, committer or parent record is invented. A missing ordinary intermediate
commit is refused before upload. Research registry revision 8 stores remote citations;
revision 6/7 reads preserve old Version digests and cannot contain this facet.
The selected Version's incorporated field baselines travel with its
native content. Missing historical expansion remains explicitly unavailable.

A live Branch reference resolves its current head once. An immutable Version
reference never follows a later head. Both expose the exact Version, original
base, origin, and derivative authorship without assigning a hosting URL.

Import verifies the research component, native schema and producer requirements,
content closure, and identity commitments before publication. It cannot accept
conflicting content under an already known immutable identity. Native retained
Version registration and historical provenance are separate from creating an
active local Branch. The explicit Fork operation supplies independent Project
metadata, access/governance declarations, and ontology disposition; source
acceptance evidence does not become a local promotion or policy decision.

Use one existing Project CURRENT publication for imported content and its durable
receipt. Before replacement, refusal leaves prior authority intact. After
replacement, report and reconcile the committed outcome, including reopen or
acknowledgement failures. Retry uses the durable operation identity and exact
request commitment. Cleanup cannot turn supported replay into another import.

## Alternatives

Copying an entire operational registry is simpler to serialize but violates
selection privacy, independent governance, and bounded ancestor retention.
Exporting a graph snapshot alone fits existing portable imports but cannot meet
immutable Version, baseline, or accepted-contribution preservation.
A second archive format would duplicate the existing verifier and transport
identity rules. Extending registered portable components keeps one verifier.

## Consequences

Interchange must validate historical provenance without installing it as live
local authority. Required research schema/capability changes fail closed for
unsupported readers; no pre-v1 migration promise is introduced. Expanded and
bundled packages share semantic identity and differ only in transport identity.
The first implementation must prove complete, disjoint, and redacted round trips,
selected history without ancestor expansion, and explicit Fork independence.

## Receipt-bound remote fetch (#2123)

`fetch_research_branch` fully verifies the selected discovery package before native
CAS admission. It preserves the local head/workspace and stores the existing
immutable interchange archive and native operation receipt. This is a native owner
API; it adds no fetch porcelain or alternate interchange/history engine.

Research registry revision 9 adds the optional authenticated remote binding to an
immutable upstream review. Revisions 6, 7 and 8 remain readable; they cannot carry
this revision-9 review field. Unsupported readers refuse the required revision
before decoding it. Omission preserves ordinary review serialization and old
Version digests; no author or commit identity is rewritten. Historical reviews
authenticate their original archive/receipt, whereas new reviews and commits also
require the current remote citation and retained endpoint payload.
