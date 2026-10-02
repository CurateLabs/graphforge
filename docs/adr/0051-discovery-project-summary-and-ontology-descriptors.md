---
title: "ADR 0051: Discovery carries a digest-addressed Project summary and exact ontology descriptors"
adr: "0051"
status: "Accepted"
date: "2026-10-02"
superseded_by: null
revisit_when: "A summary field needs required interpretation by readers, module packages must become independent of the publishing Project (which requires a portable-v2 manifest change), or a Hub needs summary data that cannot be derived from a verified package"
---

# ADR 0051: Discovery carries a digest-addressed Project summary and exact ontology descriptors

**Status:** Accepted

**Implementation:** Wire contract only (`graphforge-discovery`, discovery v1.1). Package-side derivation and module resolution, the Hub fixture, and any CLI fetch are follow-up sub-issues of #1732.

**Related:** #1732 (parent), #1743 (this contract), #906 (discovery v1), #1348 (research metadata), ADR 0021 (portable project v2), ADR 0022 (portable multi-ontology compatibility), ADR 0023 (composable ontology modules).

## Context

A Hub renders a Project page (title, license, ontologies, counts) and an ontology page, and a consumer resolves one exact ontology module from any Project that publishes it. Neither needs graph data. Discovery v1 names only the portable-v2 package, so every such need would download the package. The discovery wire shape is unreleased, so additive fields break no shipped reader.

Three things look alike and are not:

- **Package identity.** `package_digest`: the portable-v2 semantic identity of one exported package. It includes `source_generation`, so it differs between two Projects exporting the same content.
- **Module identity.** `(id, version, content_digest)`: the exact ontology module or bridge set. `content_digest` is the domain-separated canonical digest of the module document. It is the same wherever the module is published.
- **Composition digest.** `composition_digest`: the identity of the whole set of modules, bridge sets and activation in one Project version.

Treating any of these as another would let a reader accept the wrong bytes, or reject the right ones.

## Decision

**Summary is a separate object.** A `graphforge-project-summary/1` document (media type `application/vnd.graphforge.project-summary+json`) is listed in the manifest's `objects`. An optional manifest field `summary { format, summary_digest, object_digest }` selects it by `object_digest`, the same way `package` selects the Project package. `summary_digest` is the canonical digest of the document; `object_digest` is the digest of the transported bytes. The manifest stays small, the summary caches by digest, and a reader fetches it with no package I/O.

**The summary binds to one version.** It embeds `repository`, `immutable_version` and `package.package_digest`. `DiscoveryManifest::bind_summary` requires all three to equal the manifest's, requires the canonical digest to equal `summary.summary_digest`, and requires the summary's ontology composition to match the manifest's `ontology` inventory (both absent, or identical).

**The summary has its own closed requirements.** Only `project-summary@1` is understood; any other required capability, or an unknown format major, fails `unsupported_future` during parsing, before any other content is read. Optional `capabilities` stay free-form. Unknown fields fail `malformed_response`, as everywhere in discovery v1.

**Ontology descriptors are identities, not packages.** An optional manifest field `ontology { composition_digest, modules[], bridge_sets[] }` lists exact identities in strictly ascending order. A module may carry a `package` reference `{ format, package_digest, object_digest }` to a portable-v2 package for that one module. That object must be a `application/vnd.graphforge.project` object other than the Project package object, within `max_module_package_bytes`. Its `package_digest` identifies that package and legitimately differs per publishing Project. Bridge sets are listed by identity only.

**Public-safe projection.** Summary `metadata` includes every #1348 research metadata field except `access.collaborators`, `extensions` and `discovery_facets`; the record's `contract_version` is replaced by the summary `format`. `access.visibility` and `access.access_policy` stay as consumer metadata, not enforcement. Counts are the declared `corpus_size`, never computed from graph data. `facts` holds only what a verified package yields without graph payload: ontology mode, a component-kind histogram, `payload_bytes`, research and evidence presence, and the ontology composition. The summary has no location field.

**Locations are not identity.** Locations live only in `objects[].locations`. Summary bytes, `summary_digest` and the `summary` and `ontology` descriptors are invariant under location changes; only the manifest's own canonical digest changes.

**Versioning.** `ProtocolVersion::CURRENT` becomes 1.1. Both manifest fields are optional, so clone consumers need neither. A reader that predates them rejects a 1.1 manifest that uses them under `deny_unknown_fields`; none shipped, so version 1.0 is retired.

**Bounds.** `DiscoveryLimits` gains `max_summary_bytes` (1 MiB), `max_module_package_bytes` (64 MiB) and `max_ontology_entries` (4096). Summary strings and lists keep the #1348 bounds.

## Consequences

- A Hub can render a Project page from one bounded object. A consumer can fetch one module package without the Project package.
- Two Projects that publish the same module serve different module package digests and the same module identity. Verifiers compare module identity, not package digest, across Projects.
- A module package is `component-selective`, not `ontology-only`, because relabeling exact-composition selections would change frozen interchange ledgers.
- A publisher whose package carries an ontology composition must advertise `ontology` in the manifest, because `bind_summary` requires the two to agree (both absent, or identical).
- `bind_summary` and `ontology_module_object` are pure; verifying a package against its descriptors stays with the portable verifier and the API layer.
- Every new #1348 field requires an explicit projection decision before it reaches a summary.
- The summary schema is stricter than the Rust parser on omitted optional keys: the schema describes the canonical shape, and Rust normalizes on parse.
