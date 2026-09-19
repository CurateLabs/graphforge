---
title: "ADR 0037: Derived adjacency is published with the generation"
adr: "0037"
status: "Accepted"
date: "2026-09-17"
superseded_by: null
---

# ADR 0037: Derived adjacency is published with the generation

**Status:** Accepted

**Date:** 2026-09-17

**Build target:** v0.6.0

**Implementation:** Accepted for the #1388 implementation. Initial
construction and complete portable import publish the index; appends keep the
lazy rebuild and are the recorded follow-up.

**Related:** #1388, [ADR 0004](0004-adjacency-index.md), [ADR 0013](0013-project-generation-protocol.md)

## Context

ADR 0004 made the adjacency CSR a derived, rebuildable artifact under
`indexes/adjacency/`. The persistent provider serves it when its manifest
matches the project's `topology_generation` and rebuilds it lazily otherwise.
Host-scale ingest never created it: every query process rebuilt the entire CSR
into a per-process temporary directory and deleted it at exit. At S20 that is
about 16 s of CPU per process, paid four times per ladder rung; at S26 it was
0.58 h, the largest non-ingest term. The rebuild bypasses the read-path
counters, so the `adjacency` storage category reported zero bytes while a
one-hop query read gigabytes.

## Decision

The generation that publishes canonical topology also publishes its derived
adjacency CSR, as ordinary graph-files inventory entries.

- **Construction.** The canonical encoder builds `indexes/adjacency/` from the
  exact edge tables it has just encoded, stamps the generation it is about to
  bind, and records every file as a SHA-256-declared artifact of the encoded
  inventory. The existing publisher installs those artifacts into the project
  object store like every other file. Only an initial construction
  (`parent_topology_generation == 0`) is covered: an append carries the parent's
  files forward without re-reading them, so its index would need the parent's
  edge tables too. Until that lands, an append's carried-forward manifest reads
  as stale and is never served; the provider rebuilds lazily as before.
- **Portable import.** A complete package of a generation carries its index
  and imports it unchanged. Subset exports exclude `Index`-role files and older
  packages predate the index; for those a complete import builds the CSR into
  the verified package tree before the compact object-store append, so the
  imported generation ships it. The v1 inventory contract, which is verified
  file-for-file against the package tree, keeps the lazy rebuild.
- **Reading.** No read-path change. Hydration materializes the entries into
  the workspace, the provider finds a fresh manifest and opens the CSR
  presence-only (#1094), and a project without one rebuilds exactly as before.

## Consequences

- **Not a durable-format change.** The on-disk CSR format, the manifest, and
  `Index`-role inventory entries all pre-date this record; explicit
  `index("adjacency")` already published them. What changes is when the
  artifact is produced. No version bump and no compatibility read path.
- **Determinism is preserved.** CSR bytes derive from `topology/` alone, the
  shard directory takes its content digest as its name, and the manifest's
  build time is the session's recorded clock. The encoded inventory authority
  therefore stays reproducible across sessions and resume; the determinism
  suite covers the new artifacts because it compares every encoded artifact.
- **Authentication.** Every published CSR object is digest-verified by the
  open-time sweep like Topology, and shard payloads are additionally
  authenticated on first row touch. The shard manifest (`*.csr.json`) and
  `index_manifest.parquet` are covered by the sweep only; the provider treats
  any disagreement as a stale index and rebuilds privately rather than serving.
- **Cost moves to publish.** One build per generation instead of one per
  process, plus the artifact bytes in the object store and in every open-time
  sweep. The union `_all` pair duplicates the per-relation pair for a
  single-relation graph; sharing them is a possible later saving.
- **Follow-ups.** Append constructions; tombstoning a parent's stale index on
  append; attributing the rebuild's I/O to the read-path counters, which #1422
  currently misses.
