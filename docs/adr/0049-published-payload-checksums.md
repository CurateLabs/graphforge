---
title: "ADR 0049: Versioned checksums for published graph payload admission"
adr: "0049"
status: "Accepted"
date: "2026-09-29"
superseded_by: null
revisit_when: "The same-identity adversary assumption changes, or published graph payloads move to a substrate with authoritative data checksums"
---

# ADR 0049: Versioned checksums for published graph payload admission

## Context

Issue [#1617](https://github.com/CurateLabs/graphforge/issues/1617) removes
payload SHA-256 from default reads while preserving corruption refusal.
Removing the check entirely previously allowed same-inode, same-length
topology corruption to reach a query (#1435). The reference ext4 substrate
does not checksum file data. Payload length, path containment and retained
file identity alone cannot detect this mutation.

[ADR 0045](0045-ingest-authentication-regime.md) separates SHA-256 naming from
XXH64 corruption detection. Published graph inventories need a durable,
versioned checksum before their admission can use this separation. Their
existing versions carry only SHA-256 names. The maintainer selected a
versioned checksum format for this boundary. Because the product is pre-v1,
the maintainer also chose to retire legacy graph formats and the standalone
full-verification command. This supersedes ADR 0045’s administrative
`graphforge verify` requirement; its producer and trust-boundary
authentication decisions remain in force.

## Decision

New published graph payload entries carry `content_xxh64`, a seed-zero XXH64
checksum encoded as exactly 16 lowercase hexadecimal digits. Fixed width
keeps manifest sizes and allocation accounting independent of the checksum
value. Capture and CAS installation compute it in the
same streamed read pass as the existing SHA-256 digest. SHA-256 continues to
name CAS objects, authenticate control metadata and identify receipts.
XXH64 never becomes an address or a receipt identity.

The graph/files participant and schema explicitly distinguish these formats:

| Routes         | Expanded inventory | Compact root | Payload metadata                |
| -------------- | ------------------ | ------------ | ------------------------------- |
| Retired raw    | 1                  | 2            | Refused                         |
| Retired mapped | 3                  | 4            | Refused                         |
| Current raw    | 5                  | 6            | SHA-256 name and required XXH64 |
| Current mapped | 7                  | 8            | SHA-256 name and required XXH64 |

Compact roots use existing version-3 Patricia branches and version-4 buckets
whose entries require the checksum. The root version constrains every
resolved entry, including targeted lookups. Descriptor and payload versions
must agree. Missing checksums, mixed legacy/current metadata, malformed
values and unsupported versions fail closed; they never select a weaker
admission path.

Current-format graph-inventory admission checks exact length and XXH64 on
the retained payload handle. It preserves path, link, identity, filesystem
and atomic-publication checks. It performs no payload SHA-256 pass.
Opening a compact generation does not read payload content. The open
authenticates the manifest and route table, checks every payload for presence
and exact length, and hard-links payloads into the private workspace; it
checksums only the small sidecars and the small identity controls (manifests,
receipt, lock, tombstones), which it copies into single-link files and verifies
as it copies. The node-linear forward and ordinal identity runs are hard-linked
read-only and opened without reading a byte: each lookup checks the required
XXH64 of every ordinal or tombstone block it returns a value from, forward runs
(read only by writers) are authenticated whole before a writer builds on them,
and the fact that UUID order matches ordinal order is recorded by the publisher in
the authenticated manifest ([ADR 0038](0038-determinism-at-the-publication-boundary.md));
a manifest published before the field is proven by reading every ordinal block
once, on the first ordered fast-path query. Bulk payloads (nodes, edges, property fragments and search segments)
are checked for XXH64 on the first read of each object, memoized for that
hydration, so a project that never reads an object never pays for it. A
refusal is memoized as well. Every consumer that would otherwise bless a
payload's current bytes (inventory capture for publication, rewrite baselines,
appends) admits it first, so corruption cannot be republished under a fresh
digest. The explicit full-admission API remains for callers that want a whole
generation checked.

Retired graph/files formats 1–4 and persisted graph/snapshot Arrow records
are refused with an unsupported-format error. Their compatibility readers,
SHA-256 read fallback and publication upgrade path are removed. Projects
using these pre-v1 formats must be recreated. Current expanded inventories
may still be converted to compact manifests; this changes representation,
not the accepted format policy. Current updates retain bounded path-copy
publication.

Retire the standalone `gf verify` / `graphforge verify` command, its public
facade, reports and whole-store forensic scanner. No replacement full audit
or diagnostics sweep is introduced. Ordinary reads retain checksum corruption
refusal; producer installation, control metadata and portable package trust
boundaries retain required authentication. Internal snapshots used for live
construction rollback remain an implementation detail.

The same mandatory-checksum policy applies to the other persisted payload
readers: CSR shard manifests use version 3, UUID membership manifests version 7,
the logical ordinal-v4 facet descriptor uses wire version 6, GFDR run envelopes
and records use version 2, and embedding generation manifests use version 2.
These versions bind exact payload lengths and required file/block checksums;
older descriptors are refused without a SHA read fallback. Publication names,
control authentication, and canonical identity commitments remain SHA-256.

Private encoded construction inventories use wire version 2 with required
whole-artifact XXH64 for new and retained entries. Their checkpoint, generation,
shape and current-parent controls authorize those expected checksums before
replay or publication. UUID wire 7 and ordinal wire 6 retire unused per-block
SHA fields while preserving whole-artifact SHA names, checksum fences, counts
and semantic mapping commitments. Unsupported inventory and index versions
are refused before decoding current required fields; retired block fields
are refused rather than silently ignored.

Final writer SHA/XXH64/length captures are forwarded into the inventory,
including already captured CSR shards. Private publication opens one retained
encoded source at a time, binds its native identity and allocation to checkpoint
authority, and checksums the actual copied bytes. Same-inode mutate/read/restore
therefore refuses at the consuming boundary. Existing CAS objects use this
shortcut only when an installation lease already binds their exact native
identity; unknown objects and concurrent winners retain genuine SHA trust
checks. Public untrusted installation and orphan deletion authority retain
SHA authentication. This changes SHA work, not the checksum I/O required for
corruption refusal. Reclaim retains its existing zero-payload-read behavior.

The reproduction commands and current source/test hashes are in the
[ingest diagnostics method](../development/ingest-region-diagnostics.md).

The project generation manifest uses wire version 2 with mandatory participant
checksums. Arrow/Parquet participant reads use checksum/length admission, while
JSON control participant authentication remains cryptographic. The project
`FORMAT` and `CURRENT` schemas are unchanged and retain version 1.

Private replay and materialization inventories carry read-only lengths and
checksums. They are not serializable publication descriptors and cannot create
CAS names. A bounded semantic-route control table retains its actual SHA-256
authentication; no placeholder payload digest substitutes for one. Zero
cryptographic payload hashing on ordinary reads permits mandatory checksum work.
Once-per-payload publication/export and optional observability remain separate
acceptance outcomes of the complete #1617 policy.

## Consequences

Ordinary payload admission still detects accidental corruption while avoiding
cryptographic payload hashing. It continues to read payload bytes for the
checksum; eliminating that I/O requires a separate trusted storage mechanism.
Each entry gains one fixed-width checksum, and newly written versions require a
reader that understands them. Retired formats are rejected rather than
rewritten or migrated in place.

XXH64 has neither cryptographic collision resistance nor adversarial
authentication. This decision inherits ADR 0013 and ADR 0045's exclusion of
an active same-identity adversary. The product offers no whole-store proof
that unread retained bytes still match their cryptographic CAS names. A
change to the threat boundary requires revisiting the admission algorithm.

Counter tests enforce SHA-256 capture and admission costs; corruption tests
exercise same-inode, same-length mutation; refusal tests enforce format
pairing and reject checksum downgrade. Test results belong on #1637 and its
PR, with #1617 remaining the complete policy's close gate.
