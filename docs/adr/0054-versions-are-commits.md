---
title: "ADR 0054: Research Versions are commits"
adr: "0054"
status: "Proposed"
date: "2026-10-03"
superseded_by: null
revisit_when: "A head-moving operation needs more than one merge parent, or a host needs identity it cannot express as free-form author and committer signatures"
---

# ADR 0054: Research Versions are commits

**Status:** Proposed

**Implementation:** #1771 is the close gate and stays Proposed until it completes.
Slice 1 (#1773, storage) records parents, author and committer on every new
Version, keeps the permanent ancestry ledger, and moves the research format to
revision 7. Round trip, merge, erase and credit display are later slices of #1771.

**Related:** #1771, #1772, ADR 0039 (research Version publication), ADR 0041
(Branch publication), ADR 0043 (Proposal acceptance), ADR 0044 (research
interchange), ADR 0051–0053 (discovery and Hub publish).

## Context

A research Version identified frozen content and nothing else. It recorded no
parent, so a Branch head could be replaced by an unrelated Version without
anyone noticing, intermediate history could not travel, and a second committer
had nothing to fast-forward. Credit for the work lived nowhere in the Version;
creator and actor UUIDs on Branch and Proposal records are opaque identifiers.

## Decision

**A Version is a commit.** Each new `ResearchVersionRecord` records:

- `parents`: its parent Versions, first parent the prior head of its context;
- `author`: who wrote the change;
- `committer`: who recorded it;
- `provenance`: where restored or brought content came from, when that source is
  not a parent.

Signatures are free-form `ResearchSignature { name, email?, orcid? }`. They are
credit, not authentication: nothing reads them to grant or prove permission.

The Version identity digest is unchanged in form: SHA-256 of the record's
struct-order JSON with physical placement (`generation_uuid`,
`manifest_sha256`) zeroed. The new fields are part of that record, so the digest
commits to parents, author, committer and provenance. Removing credit later is a
history rewrite. Each new field is omitted when empty, so a record without them
encodes, and therefore digests, exactly as a revision 6 record did. A golden
vector (a real research/6 record and the digest its producer committed) pins
this.

### Parents per operation

Storage enforces these parents at publication; a head never moves to a Version
that does not name the prior head as its first parent.

| Operation | Parents | Also recorded |
| --- | --- | --- |
| Project capture (`Register`), graph projection | prior head, or none for a context's first Version | |
| Branch creation | origin Version | |
| Execute, ontology, claim, suppress, reference | prior head | |
| Restore (Branch, context or Project) | prior head | `provenance: restored` with the source Version |
| Bring | prior head | `provenance: brought` with the Slice source Version; field baselines already record it per field |
| Upstream incorporation | prior head, upstream Version | |
| Whole-Proposal acceptance | prior head (if any), Proposal source Version | |
| Partial Proposal acceptance | prior head (if any) | the review's accepted items and mappings |

Proposal payloads and acceptance proofs are frozen selections, not commits; they
carry no parents. An origin captured for Branch creation or Project upstream does
not move a head and carries no parents.

### Bounds

- At most `MAX_PARENTS` (8) parents, each non-nil, distinct, not the Version
  itself, and already in the permanent `identities` ledger.
- Signature name: 1–256 UTF-8 bytes, no surrounding whitespace, no control
  characters. Email: at most 254 bytes, `local@domain` with a 1–64 byte local
  part, a 1–253 byte domain with no empty labels, and no whitespace, control
  characters or angle brackets; syntax only, never delivered to. ORCID: the bare
  `0000-0000-0000-000X` form with its ISO 7064 MOD 11-2 check digit verified.
- Requests reject invalid signatures with a validation error before preparing
  content; storage refuses a registry holding one as corrupt.

### Ancestry ledger

`ResearchRegistry.ancestry` maps every Version recorded with parents to its
parent list. Like `identities` it is permanent: releasing a Version's payload
keeps its entry, every publisher must carry every entry unchanged, and it is
bounded by `MAX_RECEIPTS` (4,096) with a resource-limit refusal. A retained
Version's parents must equal its ledger entry; the ledger must be acyclic and
name only known identities. Descent of a released Version stays walkable.

A research interchange manifest carries the ancestry of its closure: every
entry reachable from its Versions, with the identities of every ancestor cited.
An imported archive's ancestry must agree with the importing registry's ledger.

### Credit and access

Credit lives in the project; access lives on the host. Authors and committers
are part of history and are public by default; opt-in redaction is applied
client-side at export and publish (#1771, slice 5). Permission to see or change
anything belongs to the Hub or other host and never appears in project,
package, lineage or summary formats.

### Format

Research capability and registry revision 7 (`graphforge-research-registry/7`,
producer `research/7`). As with revisions 2–6 there is no migration and no
reader compatibility: a revision 6 registry or package is refused with
`GF_UNSUPPORTED_CAPABILITY_VERSION`. Identities already committed by revision 6
remain valid identities: the same record digests to the same value under
revision 7, and a revision 7 registry may hold Versions without parents.

Author and committer are optional on requests in this slice, so existing callers
keep working and record none. Branch `creator_uuid` and Proposal and review
`actor_uuid` stay on their records until #1772 supplies default signatures;
new Version credit uses `author` and `committer`.

## Alternatives

- **Parents only in a side ledger.** Rejected: the identity would not commit to
  them, so a publisher could rewrite history without changing any identity.
- **Authenticated identities.** Rejected: authentication is the host's job, and
  committing account identifiers would put access control into history.
- **Reader compatibility for revision 6.** Rejected for the same reasons as the
  earlier revisions: it would need dual acceptance in every reader of the
  research capability (registry, canonical decisions, checkpoint diff, portable
  packages) for a format with no deployed Hub and no real packages.

## Consequences

- A head that does not descend from the prior head is refused at publication,
  which is what lets publish and clone fast-forward (#1771, slice 2).
- Every new Version is larger by its parent list and signatures; the ledger adds
  at most one entry per Version identity.
- Local research Projects written before revision 7 must be recreated; there
  is no migration.
