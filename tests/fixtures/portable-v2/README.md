# Portable project v2 conformance fixtures

These vectors are normative inputs to #741, #742, #784–#787, and #744. The
manifest schema is `docs/contracts/graphforge-project-v2.schema.json`; the byte
and security rules are in `docs/book/architecture/portable-project-v2.md`.

`cases.json` is a closed test ledger. Each implementation MUST materialize both
`expanded` and `bundle` for every positive package class and assert identical
canonical semantic bytes, package digest, and component inventory. It MUST
apply every listed mutation independently and return the named typed result.
Bundle vectors specify the exact header/ordering mutation so permissive tar
libraries cannot accidentally define conformance.

The `billion-edge-structural` case uses declared descriptor sizes and counts,
not a committed payload. A streaming implementation supplies deterministic
zero/read generators, verifies 64-bit accounting, and asserts peak resident
format state is bounded by the configured manifest/entry/copy buffers.

Fixture updates are format changes: review them with ADR 0021 and regenerate
canonical bytes/digests in every supported language before acceptance.

`multi-ontology-vectors.json` and ADR 0022 define the M9 extension through the
existing authenticated `compatibility` kind and a new required capability. The
fixture freezes composition identity inputs/exclusions, closure for all four
package classes, older-reader behavior, typed negative results, and operational
non-mutation invariants. It deliberately contains no runtime identity or TCK
result in the composition identity.
