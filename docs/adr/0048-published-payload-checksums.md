---
title: "ADR 0048: Versioned checksums for published graph payload admission"
adr: "0048"
status: "Accepted"
date: "2026-09-29"
superseded_by: null
revisit_when: "The same-identity adversary assumption changes, or published graph payloads move to a substrate with authoritative data checksums"
---

# ADR 0048: Versioned checksums for published graph payload admission

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
versioned checksum format for this boundary.

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
| Legacy raw     | 1                  | 2            | SHA-256 name only               |
| Legacy mapped  | 3                  | 4            | SHA-256 name only               |
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
Explicit legacy formats remain readable with their existing SHA-256 check.
Publication upgrades retained legacy entries only after authenticating their
SHA-256 identity, and rebuilds their manifest metadata once. Subsequent
current-format updates retain bounded path-copy publication.

The explicit `graphforge verify` command retains SHA-256 verification of CAS
object names and scans retained objects beyond those selected by an ordinary
read. It is an administrative audit and runs in no normal read or query path.
This format foundation does not claim that all other read-path digest sites
or the complete #1617 policy have been converted.

## Consequences

Ordinary payload admission still detects accidental corruption while avoiding
cryptographic payload hashing. It continues to read payload bytes for the
checksum; eliminating that I/O requires a separate trusted storage mechanism.
Each entry gains one fixed-width checksum, and newly written versions require a
reader that understands them. Existing generation bytes remain unchanged.

XXH64 has neither cryptographic collision resistance nor adversarial
authentication. This decision inherits ADR 0013 and ADR 0045's exclusion of
an active same-identity adversary. An operator who needs proof that bytes
still match their cryptographic CAS names uses explicit verification. A
change to the threat boundary requires revisiting the admission algorithm.

Counter tests enforce SHA-256 capture and admission costs; corruption tests
exercise same-inode, same-length mutation; compatibility tests enforce format
pairing and reject checksum downgrade. Test results belong on #1637 and its
PR, with #1617 remaining the complete policy's close gate.
