---
title: "ADR 0045: Ingest authentication regime — hash once on write, verify at trust boundaries"
adr: "0045"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0045: Ingest authentication regime — hash once on write, verify at trust boundaries

**Build target:** v0.6.0

**Related:** ADR 0013 (project-generation protocol; the threat-model boundary),
ADR 0018 (acknowledged durability and isolation), ADR 0038 (determinism at the
publication boundary)

## Context

Measured at the 67,108,864-edge rung on `710c6c64`, ingest spent 310.1 GB of
its 153.6 GB/310.1 GB read/write envelope almost entirely on authentication:
99.1% of ingest reads existed to re-read and re-hash bytes this process had
just written. Six of nine tracked storage I/O phases were authentication or
verification. That is 17.3 times the final data size read purely to
authenticate it, flat across a sixteen-fold scale range — a structural
constant, not a scaling artifact. An external survey of eight comparable
Arrow/Parquet/DataFusion systems (InfluxDB 3, GreptimeDB, Lance, Databend,
SlateDB, ParadeDB, Iceberg/Delta, arrow-rs) found no system that reads data
back in order to hash it; where verification exists it is a non-cryptographic
checksum computed inline by the pass that produces the bytes.

The maintainer decision of record (2026-09-17, issue #1384) reverses the
burden of proof: every authentication pass must name a failure a user would
notice that nothing else catches, or it is removed.

One constraint shapes the result and is specific to how GraphForge deploys:
the reference substrate is ext4 on an mdadm RAID 1 pair. ext4's
`metadata_csum` covers inodes, directory blocks and the journal — it does not
checksum file data, and RAID 1 mirrors blocks without checksums. Unlike the
surveyed systems, GraphForge does not sit on an object store that verifies
every put and get underneath it. So GraphForge verifies **once** rather than
never; the cost being attacked is repetition and read-back, not the act of
hashing each byte a single time.

## Decision

1. **Hash once, on write, streaming.** Every digest is produced by the same
   pass that writes the bytes. No pass reads a payload back in order to hash
   it.
2. **Content-addressed naming stays cryptographic.** Where a digest names an
   object — CAS graph objects, artifact receipts, authority digests — it is
   SHA-256 and remains SHA-256. "Same name means same bytes" is a correctness
   requirement for identity and deduplication; collision resistance is not
   negotiable there. (BLAKE3 is additionally foreclosed for this role by FIPS
   compliance obligations.)
3. **Corruption detection uses a fast non-cryptographic checksum** for bytes
   that are already named and whose producing pass is trusted: XXH64, seed 0,
   implemented dependency-free in `crates/graphforge-storage/src/
   corruption_checksum.rs` and recorded in `ArtifactReceipt::xxh64` beside
   `sha256`. The threat is byte mutation, torn or partial writes, and silent
   media corruption — the threat class of Parquet page checksums and ZFS
   checksums — not a forged digest.
4. **Prove-the-store-is-intact work runs nowhere in ingest.** It is reachable
   through the explicit administrative `graphforge verify` command, which
   validates the store on demand, as ParadeDB validates segment checksums on
   demand.
5. **The assumptions are recorded beside the code that depends on them** (in
   `corruption_checksum.rs`, quoted below), not only in this ADR.

### The two assumptions this rests on

1. **The threat model excludes an active same-identity adversary**, per
   ADR 0013's operational boundary. A non-cryptographic checksum is trivially
   forgeable; it defends against accidental mutation, torn writes and media
   corruption — not an attacker who can rewrite the receipt alongside the
   payload. Untrusted shared storage, multi-tenant hosts or supply-chain
   threats invalidate it.
2. **The storage substrate does not checksum file data.** On a substrate that
   does (ZFS, btrfs) the corruption check could relax further; on a weaker
   one it must strengthen.

If either stops holding, the decision reverts and the corruption checksum
must be replaced by a cryptographic digest.

### Determinism

Digests stay reproducible for identical logical input: digest values are a
pure function of the bytes each producing pass writes, and on-disk layout
remains governed by ADR 0038. Removing read-back passes changes no digest
value.

## Surviving authentication boundaries

Every authentication read that remains in the lifecycle, the failure it
uniquely catches, and its measured cost at the 67,108,864-edge rung
(4,194,304-edge rung in parentheses; application-I/O receipts, retained
`b6ffb088` archive plus #1552):

| Boundary | Mechanism | Failure it uniquely catches | Cost per edge |
| --- | --- | --- | --- |
| Producer write | SHA-256 computed inline by the pass writing the bytes (staged chunks, shaped outputs, canonical Parquet, CSR shards, CAS objects, receipts) | Digest is the product of the write; names the object | 0 extra reads (rides the write) |
| Consume spool | SHA-256 fused into the authenticated source spool during encode | Shaped source mutated between shape completion and encode consumption | 0 extra reads (rides the consume) |
| Shape replay/recovery | Identity + link count + length + XXH64 payload checksum (`authenticate_shaped_output`) | Same-inode, same-length mutation of a completed shape output — the #1269/#1392 class, regression-tested | One read of shaped outputs on replay paths only; 0 on a first ingest |
| Resume recovery | `authenticate_artifact` over retained payloads at the checkpoint/resume boundary | Retained payload mutated while construction was interrupted — the #1269 class | 68.6 B/edge (62 B/edge) |
| CAS install | Streaming copy re-derives SHA-256 and refuses a mismatch before the object becomes addressable | Torn or mutated artifact entering the content-addressed store; also names the object | Rides the copy (4.61 GB read at S22 is bytes moved into the store) |
| CAS hydration (open time) | Streamed digest verification against the object's address | Store object no longer matches its name — silent media corruption | 5.1 B/edge (4.8 B/edge), charged at open, not ingest |
| Shard read admission | Manifest digest + `codec::preflight` before Arrow decodes a CSR shard | Oversized or corrupt shard presented to the query path | Read-path work, outside ingest |

`graphforge verify` re-derives every retained object's digest on demand and
runs in no lifecycle path.

## Removed passes

The following read-back and re-hash passes were removed under #1384 and its
focused repairs, each after recording why no surviving boundary needs it:
the full SHA-256 re-hash of every consumed payload before supersession unlink;
the postwrite re-read of encoded artifacts (#1444); the triple re-hash of the
encoded inventory per ingest (#1450); the CSR shard write read-back (#1552);
whole-graph re-verification on every property mutation (#1423);
already-validated row re-hashing on provenance merge (#1419); per-object
re-hashing in the storage-attribution walk (#1443); and the open-path
generation inventory sweep re-hash (#1425).

## Accounting note

`ShapeConsumeReauthentication` and `EncodeWritePostwriteAuthentication` are
**region names**, not authentication quantities: they are the thread I/O
scopes of the shaping and encode regions respectively, and their reads are
the partitioner's and encoder's real data movement. Authentication costs are
attributed by the boundary table above, not by those phase names.

## Consequences

- Ingest application reads at the 67,108,864-edge rung fall from 310.1 GB
  (authentication-dominated, 4,621 B/edge) to 68.5 GB
  (data-movement-dominated), of which approximately 4.94 GB — 74 B/edge,
  about 28% of the retained 265 B/edge — are authentication read-backs after
  #1552 (5.85 GB including the 0.906 GB CSR shard read-back that #1552
  removed). The shortfall against the issue's ~25 GB hashed-bytes target is
  exactly the boundary table above, each entry named, justified and measured.
- The #1269 corruption refusal holds unchanged, proven by its own regression
  test, and is now established deliberately at the boundaries that consume
  retained bytes.
- Crash recovery, cancellation, corruption refusal and fail-closed publication
  contracts are unchanged; all corruption, crash-boundary and recovery tests
  pass unweakened.
- ADR 0013 and ADR 0018 are unchanged; this record operates inside their
  threat model and durability contract.

## Evidence

- Baseline measurement and survey: issue #1384 (2026-09-17).
- Retained rung evidence: `docs/development/evidence/ladder/`
  (the `b6ffb088` S18–S22 archive) and
  `docs/development/evidence/authentication-regime-1384.md` for the
  integrated-tree measurement that accompanied this record.
