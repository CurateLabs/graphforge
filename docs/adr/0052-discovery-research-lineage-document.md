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

**Implementation:** Wire contract only (`graphforge-discovery`, discovery v1.1). Hub derivation, `gf clone --ref`, publish, and in-memory Hub tests are follow-ups of #1748 and #1749.

**Related:** #1748, #1749, #906, ADR 0044 (research interchange), ADR 0051 (Project summary).

## Context

Hub consumers need read-only research lineage: Branch heads, immutable Versions, Fork origin citations, Slice creation commitments, and frozen Proposal projections. Discovery v1 names only the complete Project portable package. Research identities (`version_uuid`, `branch_uuid`, projection vs complete) must stay distinct from repository snapshot digests and package digests.

## Decision

Add optional manifest field `lineage { format, lineage_digest, object_digest }` selecting a `graphforge-research-lineage/1` object (`application/vnd.graphforge.research-lineage+json`). The document lists Branches (with `ref_name`, genealogy, and `selection_sha256`), Versions (`complete` or `projection` with optional per-Version package references), Fork origin citations, and published Proposals. Unknown required capabilities fail `unsupported_future` before content is read.

`DiscoveryManifest::bind_lineage` requires repository and `immutable_version` agreement, digest match, branch `ref_name` presence in the refs snapshot, and resolved-ref target agreement when the resolved ref names a branch. `research_version_object` selects a Version's portable object separately from the Project package object.

## Consequences

- Hubs can render lineage without graph I/O; clone consumers still ignore `lineage` until they opt in.
- Partial projections are labeled `projection` and must cite `source_version_uuid`; Proposals reference projection payload Versions only.
- Publish and resumable upload remain #1749; this ADR does not define write paths.
