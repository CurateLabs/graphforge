# ADR 0021: Portable project v2 package layout and identity

**Status:** Proposed  
**Date:** 2026-08-19  
**Build target:** v0.5.x (M5 portable project v2)  
**Related:** #783, parent #740; consumers #741, #742, #784–#787, #744

## Context

Portable v1 is a fully buffered proprietary envelope. It cannot express a
billion-edge file-backed project without its 16 GiB envelope ceiling and does
not give independent implementations a standard inspectable representation or
a representation-independent identity.

The logical package must have an expanded form for inspection and a single,
incrementally writable bundle form. Identity must describe selected project
meaning, not host paths, archive metadata, or a particular transport.

## Options considered

1. **BagIt 1.0 profile plus a canonical bundle.** RFC 8493 supplies a simple,
   content-addressed expanded layout and payload/tag manifests. A narrow
   GraphForge profile can close BagIt's intentionally extensible surface while
   retaining standard tooling.
2. **OCI image layout.** OCI gives digest-addressed blobs and useful registry
   transport, but an image-layout directory is not an inspectable project
   package, permits descriptor graphs and annotations beyond this semantic
   model, and does not define deterministic layer bytes. OCI promotion remains
   an optional later mapping of the canonical bundle.
3. **Raw tar or ZIP.** Both are widely implemented, but neither supplies the
   semantic inventory, dependency closure, payload manifests, or closed-world
   profile. ZIP also has multiple filename/timestamp/extra-field encodings.
4. **New proprietary framing.** It could be streamable, but would duplicate
   mature archive parsing and security work and create another ecosystem-only
   envelope.

## Decision

Adopt the closed BagIt-compatible expanded profile and deterministic PAX/ustar
bundle defined by the normative
[portable project v2 specification](../book/architecture/portable-project-v2.md).
The expanded extension is `.gfproject/`; the uncompressed bundle extension is
`.gfpb` with media type `application/vnd.graphforge.project.v2+tar`. Compression
is deliberately absent in v2 so that bytes are reproducible and readers can
enforce limits while streaming.

The semantic manifest is closed JSON with media type
`application/vnd.graphforge.project.manifest.v2+json`, canonicalized using RFC
8785 JCS. Package identity is SHA-256 over a domain-separated preimage containing
the canonical semantic manifest after its self-referential `package_digest`
field is omitted. Component descriptors bind every logical payload by portable
path, byte length, SHA-256, kind, stable identity, dependencies, and selection
state. Expanded and bundled representations therefore have the same package
digest and inventory while retaining distinct transport digests.

## Consequences

- Export, verification, import, bindings, and optional OCI promotion consume one
  schema and fixture corpus rather than inventing format rules.
- Full-memory buffering is forbidden. Counts and per-entry lengths are bounded;
  payload bytes are hashed and copied incrementally.
- BagIt's general extensions are narrowed: only enumerated regular files are
  allowed, all payload and tag files are manifested, and extra files fail.
- Authenticity is not inferred from integrity. Signatures and trust policy are
  optional evidence over the package digest and belong to later issues.
- v1 remains readable only through the legacy v1 reader and is never emitted by
  a v2 writer. Unsupported future semantics fail before project mutation.

## Required verification

The machine-readable schema and golden corpus under
`tests/fixtures/portable-v2/` are normative. Their validator must cover every
package class, expanded/bundle equivalence, JCS bytes, bundle ordering and
headers, hostile paths and entries, truncation, closed-world inventory,
limits, v1 disposition, and future-version/capability rejection. A structural
billion-edge vector proves streaming bounds without committing the dataset.

## References

- [RFC 8493: The BagIt File Packaging Format (V1.0)](https://www.rfc-editor.org/rfc/rfc8493)
- [RFC 8785: JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
- [OCI Image Layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md)
- [POSIX pax archive format](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/pax.html)
