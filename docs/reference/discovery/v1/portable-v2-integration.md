# Discovery v1 to portable-v2 verification

This document defines the trust boundary between repository discovery and a
downloaded portable-v2 package.

## Required order

1. Parse and validate manifest and refs bytes with `graphforge-discovery`, using
   explicit response limits.
2. Require both documents to name the repository identity requested by the
   caller.
3. Bind `resolved_ref` through the refs snapshot and require its target to equal
   the manifest's `immutable_version`.
4. Select the inventory entry whose transport `digest` equals
   `package.object_digest`. The reference MUST resolve to exactly one object and
   that object MUST use `application/vnd.graphforge.project`; clients never
   select the first object or guess by media type.
5. Download that selected object using a caller-owned HTTP transport. Redirect,
   host, and credential policy remain transport responsibilities.
6. Pass the complete local package to `graphforge-storage`'s portable-v2
   verifier. Only that verifier decides package integrity, compatibility, and
   authenticity.
7. Require the verifier's semantic `package_digest` to equal the discovery
   manifest's `package.package_digest` before returning an accepted selection.

Failure at any step MUST return no accepted repository/package result. Discovery
`package.object_digest` and inventory object digests protect downloaded object
bytes; they do not replace `package.package_digest`, the portable-v2 semantic
identity established by the storage verifier. Likewise, an immutable repository
version identifies a repository snapshot and is not a portable package identity.

`graphforge_api::verify_discovered_portable_v2`
(`crates/graphforge-api/src/discovery_portable_v2.rs`) implements this sequence
over the `graphforge-storage` portable-v2 verifier. It does not publish or
materialize a project, so a failed cross-contract check cannot leave partially
accepted project state.

## Summary read

A Hub reads a Project summary without package I/O:

1. Parse and validate manifest and refs bytes with `graphforge-discovery`, using
   explicit response limits; require the requested repository identity and bind
   `resolved_ref` through refs, exactly as in steps 1-3 above.
2. Select the summary object with `DiscoveryManifest::summary_object()`. It
   resolves `summary.object_digest` to exactly one inventory entry with media type
   `application/vnd.graphforge.project-summary+json`. A manifest without `summary`
   has no summary to read.
3. Download that object using a caller-owned HTTP transport, bounded by
   `max_summary_bytes`. Require the downloaded bytes to hash to `object_digest`.
4. Parse with `ProjectSummary::from_json`. An unknown required capability or
   format major fails `unsupported_future` here, before any metadata is read.
5. Call `DiscoveryManifest::bind_summary`. It requires the summary's repository,
   `immutable_version`, and `package_digest` to equal the manifest's, its
   canonical digest to equal `summary.summary_digest`, and its ontology
   composition to match the manifest's `ontology` inventory.

Failure at any step returns no summary. Nothing in this sequence reads the Project
package or any graph data.

Publishers derive summary bytes with
`graphforge_api::summarize_verified_portable_v2`
(`crates/graphforge-api/src/discovery_project_summary.rs`). It fully verifies the
package, then reads only the `workspace/research_metadata` and
`workspace/configuration` participants through the storage-owned authenticated
reader (`PortableV2PackageIndex`), so a verifier that runs it on the same package
obtains the same canonical bytes for a bundle and for an expanded directory. The
public-safe projection is one exhaustive destructuring function: a new Project
metadata field does not compile until a maintainer decides whether it is public.
`access.collaborators`, `extensions`, and `discovery_facets` are never included,
and local paths and Project identity are never consulted.

## Lineage read

A Hub reads research lineage without Project package I/O:

1. Parse and validate manifest and refs bytes with `graphforge-discovery`, using
   explicit response limits; require the requested repository identity and bind
   `resolved_ref` through refs, exactly as in steps 1-3 above.
2. Select the lineage object with `DiscoveryManifest::lineage_object()`. It
   resolves `lineage.object_digest` to exactly one inventory entry with media type
   `application/vnd.graphforge.research-lineage+json`. A manifest without `lineage`
   has no lineage to read.
3. Download that object using a caller-owned HTTP transport, bounded by
   `max_lineage_bytes`. Require the downloaded bytes to hash to `object_digest`.
4. Parse with `ResearchLineage::from_json`. An unknown required capability or
   format major fails `unsupported_future` here, before any Version entry is read.
5. Call `DiscoveryManifest::bind_lineage` with the refs snapshot. It requires
   repository and `immutable_version` agreement, digest match, and every branch
   `ref_name` to appear in refs with a `target` equal to the manifest's
   `immutable_version`. One lineage document describes every Branch head of one
   repository snapshot, so a Branch ref that targets another snapshot fails
   `integrity_failure` instead of resolving to this snapshot's head.

`ResearchLineage::from_json` also enforces the cross-entry rules JSON Schema
cannot express: a `projection` never cites itself as `source_version_uuid`; a
Proposal's payload Version is listed, is a `projection`, has the Proposal's
`source_version_uuid` as its source, and (when it carries a package) carries the
Proposal's package; and a Proposal's `source_branch_uuid` names a listed Branch.

Failure at any step returns no lineage. Listing Branches, Versions, Fork origins,
and Proposals uses only this document plus refs; it never reads the Project
package or any graph data. A `projection` Version is not a `complete` Version.

## Research Version fetch

A consumer clones one exact research Version (Branch head or immutable Version UUID)
without using the Project package object:

1. Steps 1-5 of the lineage read sequence.
2. Select the Version with `DiscoveryManifest::research_version_object(&lineage,
   version_uuid)`. It requires a per-Version `package` reference and resolves its
   object to a `application/vnd.graphforge.project` entry other than the Project
   package object.
3. Download that object under the same rules as the Project package (it counts
   toward `max_cumulative_object_bytes`, and `gf clone` applies its Project
   bundle bound), and require the bytes to hash to the object's `digest`.
4. Pass the complete local package to the portable-v2 verifier. Require the
   semantic `package_digest` to equal the Version's `package.package_digest`.
5. Require the package to carry research interchange whose registry holds the
   selected Version with the lineage `identity_digest` (both the registry
   commitment and the record's recomputed identity) and the same kind and
   `source_version_uuid`. A projection package therefore never verifies as its
   source Version, and a package without research never verifies as any Version.

`graphforge_api::verify_discovered_research_version` implements this sequence. It
does not publish or materialize a project, so a failed cross-contract check cannot
leave partially accepted project state. `gf clone --ref <branch>` and
`gf clone --version-uuid <uuid>` run it before importing, then derive the import
operation from the repository snapshot, the selected Version UUID, and its
identity digest.

## Exact module fetch

A consumer resolves one exact ontology module from any publishing Project:

1. Steps 1-3 of the required order, then build the `ExactIdentity`
   `(id, version, content_digest)` the caller wants.
2. Select the module package with
   `DiscoveryManifest::ontology_module_object(&identity)`. It requires the module
   to be advertised with a `package`, and its object to be a
   `application/vnd.graphforge.project` object other than the Project package
   object. It never selects by position, host, or media type alone.
3. Download that object, bounded by `max_module_package_bytes`, and require the
   bytes to hash to the object's `digest`.
4. Pass the complete local package to the portable-v2 verifier. Require the
   semantic `package_digest` to equal the module descriptor's
   `package.package_digest`, and require the verified ontology composition to
   contain a module with the requested exact identity.
5. Accept the module bytes only after their canonical content digest equals the
   requested `content_digest`.

The module package's `package_digest` identifies that package and differs between
Projects that publish the same module. Module identity, not package digest, is
what two Projects have in common. The Project package is never downloaded.

`gf ontology module fetch OWNER/REPOSITORY --ontology-id ID --version VERSION
--digest HEX --output FILE [--hub URL]` runs this sequence from the command line
with the same transport safety as `gf clone` (HTTPS only, public network only,
bounded downloads, private no-follow staging) and writes the verified module
document to a new file. It makes exactly three requests: refs, manifest, and the
module object. The CLI hashes the downloaded bytes against the object's
`digest` and `length` before step 4 and bounds the object by
`max_module_package_bytes`. See `packages/cli/README.md` for its flags, receipt
(`graphforge-hub-module-fetch/1`), and `hub.*` error codes.

`graphforge_api::resolve_discovered_ontology_module`
(`crates/graphforge-api/src/discovery_ontology_module.rs`) implements steps 1, 2,
4 and 5 over a downloaded package: discovery parsing, repository and refs binding,
and descriptor selection complete before any package path is read. It returns the
module identity, the carrying `package_digest`, the module document bytes, and the
document's file SHA-256 (the same for every Project that publishes the module).
The document is read through the manifest-authenticated storage reader and its
domain-separated canonical digest is recomputed, never trusted from the package.

## Hub and TypeScript consumption

The Hub may serve the versioned files in this directory and package them as
static TypeScript assets. It may use `manifest.schema.json` and
`refs.schema.json` for early structural diagnostics and `conformance.json` to
test its HTTP adapter. These files are generated and byte-checked by Rust.

The Hub MUST NOT maintain hand-written TypeScript protocol types or validation
rules as a competing authority. If TypeScript declarations are useful, generate
them from the versioned schema during the Hub build, keep them disposable, and
still treat a Rust validation/verification result as authoritative for protocol
acceptance.
