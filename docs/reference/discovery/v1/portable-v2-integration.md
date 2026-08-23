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

`graphforge-storage::verify_discovered_portable_v2` implements this sequence.
It does not publish or materialize a project, so a failed cross-contract check
cannot leave partially accepted project state.

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
