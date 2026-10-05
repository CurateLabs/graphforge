---
title: "ADR 0055: Research Versions are commits"
adr: "0055"
status: "Proposed"
date: "2026-10-03"
superseded_by: null
revisit_when: "A head-moving operation needs more than one merge parent, a host needs identity that free-form signatures cannot express, or revision 6 Projects no longer need to be read"
---

# ADR 0055: Research Versions are commits

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

### Format and compatibility

Research capability and registry revision 7 (`graphforge-research-registry/7`,
producer `research/7`). Revision 6 research layouts remain readable; revisions
before 6 are refused with `GF_UNSUPPORTED_CAPABILITY_VERSION`, as before.
This layout compatibility does not override the separate interchange producer
check: an archive's outer `graphforge-storage/<version>;research-interchange/1`
producer must match the reader's exact package version. Pre-v1 package-version
changes can therefore reject an older package, or reject research-history access
to a Project containing such an archive, even when its revision 6 layout is
readable. Opening the Project alone does not establish that its archived
research history is admissible.

Layout compatibility needs no translation because every field revision 7 adds is
omitted when empty: revision 6 registry bytes are valid revision 7 bytes and
decode, validate and digest identically.

- **Reading.** A revision 6 registry (`registry/6`) is read as-is. Its Versions
  are parentless legacy roots with no author, committer or provenance; its
  ancestry ledger is empty; their identity digests are unchanged. A registry
  labelled revision 6 that holds any revision 7 commit data is refused.
- **Upgrade on the first research write.** Publishers that do not write
  research (graph writes, checkpoints) carry revision 6 unchanged. The first
  research write (any registry operation, or research decisions) relabels the
  research capability and its participants to revision 7 in that same
  publication, so the upgrade is atomic and crash-safe: a crash leaves CURRENT at
  either the revision 6 or the revision 7 generation. No record's bytes are
  rewritten. The new Version's first parent is the prior head even when that
  head is a revision 6 Version. Once a Project is revision 7, no publication may
  label it revision 6 again.
- **Packages.** Revision 6 research packages that satisfy the current producer
  contract are admitted and import at revision 7 by the same relabelling; their
  imported Versions and archive stay legacy roots and keep `research_capability_version`
  6 in the archive. Every export is written at revision 7, including a
  whole-Project export of an admissible Project imported at revision 6. This
  does not promise import or re-export of archives from a different pre-v1
  package version.
- **Readers of research participants.** Canonical decisions written at
  revision 6 read unchanged. Checkpoint summary diffs span the upgrade;
  record-level checkpoint diff still has no research registry adapter at any
  revision.

Author and committer are optional on requests in this slice, so existing callers
keep working and record none. Branch `creator_uuid` and Proposal and review
`actor_uuid` stay on their records until #1772 supplies default signatures;
new Version credit uses `author` and `committer`.

## Alternatives

- **Parents only in a side ledger.** Rejected: the identity would not commit to
  them, so a publisher could rewrite history without changing any identity.
- **Authenticated identities.** Rejected: authentication is the host's job, and
  committing account identifiers would put access control into history.
- **Migrating revision 6 records.** Rejected: giving old Versions parents or
  signatures would change their identities. They stay parentless roots.
- **A separate upgrade command.** Rejected: the first research write already
  publishes the whole registry atomically, so the upgrade needs no extra step.

## Consequences

- A head that does not descend from the prior head is refused at publication,
  which is what lets publish and clone fast-forward (#1771, slice 2).
- Every new Version is larger by its parent list and signatures; the ledger adds
  at most one entry per Version identity.
- Every reader of the research capability accepts two revisions until
  revision 6 support is withdrawn by a later decision.
