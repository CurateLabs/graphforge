---
title: "ADR 0052: Discovery carries a digest-addressed research lineage document"
adr: "0052"
status: "Accepted"
date: "2026-10-02"
superseded_by: null
revisit_when: "Clone or publish paths need identities this document cannot express, or Hub moderation requires cross-owner Proposal submission semantics"
---

# ADR 0052: Discovery carries a digest-addressed research lineage document

**Status:** Accepted

**Implementation:** #1748: the wire contract (`graphforge-discovery`, discovery v1.1); registry-derived lineage (`GraphForge::build_research_lineage_for_discovery`); verification-first Version admission (`verify_discovered_research_version`); and `gf clone --ref <branch>` / `--version-uuid <uuid>`, proven end to end against an in-memory Hub serving a real Fork with two Branches and a Proposal. Publishing and upload remain #1749.

**Related:** #1748, #1749, #906, ADR 0044 (research interchange), ADR 0051 (Project summary).

## Context

Hub consumers need read-only research lineage: Branch heads, immutable Versions, Fork origin citations, Slice creation commitments, and frozen Proposal projections. Discovery v1 names only the complete Project portable package. Research identities (`version_uuid`, `branch_uuid`, projection vs complete) must stay distinct from repository snapshot digests and package digests.

## Decision

Add optional manifest field `lineage { format, lineage_digest, object_digest }` selecting a `graphforge-research-lineage/1` object (`application/vnd.graphforge.research-lineage+json`). The document lists Branches (with `ref_name`, genealogy, and `selection_sha256`), Versions (`complete` or `projection` with optional per-Version package references), Fork origin citations, and published Proposals. Unknown required capabilities fail `unsupported_future` before content is read.

`DiscoveryManifest::bind_lineage` requires repository and `immutable_version` agreement, digest match, and every Branch `ref_name` in the refs snapshot with a target equal to the manifest's `immutable_version`: one lineage document describes the Branch heads of exactly one repository snapshot, so a ref that targets another snapshot fails closed. `research_version_object` selects a Version's portable object separately from the Project package object; research packages obey the same byte rules as the Project package.

The lineage is derived from the research registry: Branch genealogy and heads, Version identities and kinds, Proposal records, and the Fork origin (origin Project, Version, and identity from the Fork record). The publisher supplies only hosting facts: ref names, per-Version packages, which Proposals to publish, and the origin repository. A Version is `complete` only when it is Project research or a Branch state; a frozen Proposal payload or any other derivative of a source Version is a `projection` that cites its source.

Cloning a Version verifies the package, then requires its research registry to carry the selected Version with the lineage identity digest and kind before any destination exists. The import operation, and so the generation identity, is derived from the repository snapshot plus the selected Version UUID and identity, so a Branch-head clone and an immutable-Version clone of one snapshot never share a generation. Plain Project clones keep their historical derivation.

## Consequences

- Hubs can render lineage without graph I/O; clone consumers still ignore `lineage` until they opt in.
- Partial projections are labeled `projection` and must cite `source_version_uuid`; Proposals reference projection payload Versions only.
- Publish and resumable upload remain #1749; this ADR does not define write paths.
