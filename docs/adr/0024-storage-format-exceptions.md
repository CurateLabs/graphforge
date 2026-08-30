# ADR 0024: Storage format exceptions for GFDR and compiled ontologies

**Status:** Accepted
**Date:** 2026-08-30
**Build target:** v0.5.x
**Related:** ADR 0019 (authoritative graph delta journal), issue #1022

## Context

The default storage rule is intentionally simple: graph data uses
Arrow/Parquet, while ontology definitions and metadata use JSON or YAML. Two
shipped formats do not fit that shorthand:

1. authoritative graph delta runs are custom binary `.gfdr` containers whose
   mutation payloads are JSON; and
2. a compiled ontology runtime can be persisted as eight Parquet tables.

Leaving these as undocumented implementation details makes the default rule
misleading. It also obscures which bytes are authority, where compatibility
versions live, and whether an incompatible representation must be migrated or
can be rebuilt.

## Decision

These are permanent, named exceptions to the default storage rule. “Permanent”
means that each is an intentional architecture category, not a temporary rule
violation. It does not freeze today's byte layout: every persisted exception
must have an explicit compatibility boundary, fail closed outside that
boundary, and carry its migration or rebuild policy.

### GFDR is an authoritative, versioned graph-data exception

An inventory-listed `graph/deltas/run_*.gfdr` file is authoritative graph state
when the selected generation represents its graph as canonical Parquet base
files plus an ordered delta chain. The `.gfdr` extension alone is not a format
marker. Version 1 is identified and framed as follows; all integers are little
endian:

| Scope | Current marker | Authoritative content |
| --- | --- | --- |
| File | four bytes `GFDR`, then `u32` run-format version `1` | run sequence, run UUID, transaction UUID, record count, records, and SHA-256 framing checksums |
| Record | `u16` record version `1` | operation UUID and sequence, closed mutation kind, payload schema ID, payload length and bytes, and record checksum |
| Payload | `u16` schema ID `1` | `serde_json` encoding of the tagged Rust `GraphDeltaPayload`; typed property values are `serde_json` encodings of `IrLiteral` carried in the payload's value field |

The JSON payload does not make GFDR a JSON sidecar. The checked binary framing,
ordered records, and schema-qualified payload together are the authoritative
mutation contract. Unknown run-format versions fail with
`GF_UNSUPPORTED_PROJECT_FORMAT`; malformed framing, lengths, order, or digests
fail as corruption. Replay never guesses from an extension, partial prefix, or
unversioned payload.

GFDR's migration debt is explicit. The pre-release routing-free and
string-only payload prototype is not losslessly migratable and remains
rejected. A future framing or payload version must add a bounded reader and an
explicit generation migration/compaction path, or remain unsupported. It must
not silently reinterpret old bytes. This specializes, rather than replaces,
the authority and publication rules in [ADR 0019](0019-authoritative-graph-delta-journal.md).

### Ontology Parquet is a derived runtime-persistence exception

An adopted ontology's canonical `OntologyDoc` JSON inside the CURRENT-selected
workspace participant is durable ontology authority. YAML and JSON files are
authoring/import inputs; a file elsewhere in the project tree cannot override
the committed participant. Startup compiles the committed document into Arrow
runtime tables. `graphforge-ontology` may also persist the compiled runtime as
eight Parquet files (`ontology_meta`, `entity_types`, `relation_types`,
`property_types`, `type_constraints`,
`semantic_flags`, `cardinality_rules`, and `aliases`) to avoid repeating parse,
validation, and compilation work.

Those Parquet files are derived runtime data, not a second ontology-definition
authority. They carry the ontology ID, ontology version, source checksum, and
writer version in Arrow schema metadata. When a caller supplies the expected
ontology checksum, a mismatch means the snapshot was compiled from a different
document and must be discarded and recompiled from the committed JSON.
Corrupt, missing, or incompatible compiled tables likewise do not authorize
changes to that document.

The compiled snapshot's migration debt is rebuildable rather than semantic:
the current writer records `graphforge.writer_version = 0.5.0`, but the loader
does not yet enforce that marker as a closed snapshot-format version. Until an
enforced compiled-snapshot version gate exists, cross-release reuse must be
treated as unsupported unless the current reader accepts the tables and their
ontology checksum matches the committed document. Recompilation from that
authoritative JSON document is the upgrade path; in-place inference or repair
of unknown Parquet schemas is not.

## Consequences

- The concise storage rule must name both exceptions wherever it is presented
  as an architecture constraint.
- GFDR receives the same review rigor as any authoritative graph format:
  versioning, bounds, checksums, compatibility failures, and explicit migration
  are correctness concerns.
- Compiled ontology Parquet may be deleted and rebuilt without changing
  ontology meaning, provided the authoritative committed JSON document is
  retained.
- A `.gfdr` run may not be deleted or rebuilt independently of its selected
  generation; compaction must publish an equivalent new generation through the
  normal authority protocol.

## Rejected alternatives

| Alternative | Reason |
| --- | --- |
| Describe GFDR as JSON metadata | Its binary framing and ordered payloads are authoritative graph mutations, not metadata or a sidecar. |
| Encode each small mutation as Parquet | Reintroduces the rewrite amplification ADR 0019 was designed to avoid. |
| Treat compiled ontology Parquet as source authority | Creates two competing ontology definitions and makes checksum mismatch ambiguous. |
| Promise transparent pre-v1 migration | The old GFDR payload is not losslessly recoverable, and the compiled snapshot lacks an enforced format-version gate. |
