# ADR 0025: Storage values have a compiler-independent contract

**Status:** Accepted (decision; extraction not yet implemented)
**Date:** 2026-09-04
**Build target:** v0.6.0
**Related:** issues #1013, #1011, #1012; ADRs 0008–0011, 0013, 0017, 0024

## Context: the current dependency graph

The compiler pipeline is a flow of work, not the Cargo dependency graph.
`graphforge-storage` currently has production dependencies on both
`graphforge-ir` and `graphforge-ontology`. In particular:

- `graphforge-ir/src/expr.rs` owns `IrLiteral` and its custom serde encoding.
  Storage's writer, property overlay, and GFDR journal use those values.
- `graphforge-ir/src/catalog.rs` owns runtime IDs, the tag-bit conversion into
  `graphforge_core::TypeId`, and `RuntimeCatalog`'s Arrow persistence schema.
  Storage reads and writes that catalog and stores tagged IDs in topology.
- Rel, exec, and storage independently construct or interpret heterogeneous
  Arrow values (`__het_tag` and their variant payloads).
- Storage also uses ontology handles and IR composition-binding adapters.
  Moving value definitions alone will not remove every storage → IR edge.

`IrVersion::CURRENT` is attached to result metadata in `graphforge-api`.
It is not checked by `resolve_project_generation` on project open. Making that
compiler version the durable-value compatibility gate would couple unrelated
plan and storage evolution.

## Decision and dependency graph

Choose a small Rust **`graphforge-value`** contract crate. It depends on
`graphforge-core`, Arrow, and serialization/error support; it must not depend
on AST, IR, ontology, rel, exec, storage, or DataFusion. It is a leaf relative
to those engine layers. Core remains below it and must not depend on it.

The target **value-contract** edges (arrow means “depends on”) are:

```text
graphforge-ir       ─┐
graphforge-ontology ─┤
graphforge-rel      ─┤
graphforge-exec     ─┼──> graphforge-value ──> graphforge-core
graphforge-storage ─┤           │
graphforge-api      ─┘           └──> Arrow + serde + error support
```

This graph does not replace the compiler pipeline or show every crate edge.
Ontology-to-value conversion stays in ontology-facing adapters; conversion
between DataFusion `ScalarValue` and the shared Arrow/value contract stays in
rel/exec adapters. Neither may duplicate tag interpretation. Storage may still
consume ontology/composition adapters where it validates semantic bindings;
this ADR does not turn those APIs into compiler-independent APIs by assertion.

### Ownership

| Contract | Owner after extraction | Boundary |
| --- | --- | --- |
| Literal value variants currently named `IrLiteral`, including nested values and custom non-finite-float serde encoding | `graphforge-value` | A neutral value type; IR may re-export it as `IrLiteral` for source compatibility. `IrExpr`, arenas, operators, binder policy, and `IrVersion` stay in IR. Moving the Rust type must preserve serialized names and bytes. |
| Ontology IDs versus runtime entity/relation IDs and their tagged carrier | Primitive ontology identity stays in core; checked tagged carrier/codec and runtime ID types live in `graphforge-value` | Private fields and checked construction prevent substitution; one encoder/decoder owns every tag bit. Ontology assignment validates its range before conversion. |
| Heterogeneous Arrow values | `graphforge-value` | Construct, recognize, validate, encode, and decode the existing constant, nested, and per-expression dynamic layouts. This includes field names, types, null rules, tag selection, and schema-version recognition, not just constants. |
| Persisted runtime catalog/value schemas | `graphforge-value` | Schema definitions, serialized carriers, and validation are shared. Runtime observation/interning policy can remain in IR, consuming those carriers. Storage owns Parquet I/O and generation publication, not a second schema definition. |
| Topology tables, GFDR envelopes, generation manifests and participant inventory | `graphforge-storage` | Storage owns physical framing and open/replay admission, using the shared ID/value codecs. A contract crate does not own filesystem paths or publication. |
| Canonical ontology documents, composition semantics and compiled ontology snapshots | `graphforge-ontology` and the existing workspace adapters | Durable source remains committed JSON; compiled Parquet is derived under ADR 0024. They consume checked identity/value types without moving ontology policy into the leaf. |

The schema helpers must distinguish the existing layouts: dynamic list tags
are per-expression payload indexes, not a global enum of value kinds. Preserve
ADRs 0008–0011, including nested layouts and null semantics. Centralization must
not accidentally assign one universal meaning to tags used by different schemas.

## Versioning and compatibility

Public package versions remain coordinated under ADR 0017. Four other version
boundaries remain distinct:

| Boundary | Rule |
| --- | --- |
| Graph IR / `IrVersion` | Describes the compiler-plan contract and result annotation. Changing it neither upgrades nor admits durable projects. |
| Shared ID/value/catalog schemas | The contract crate names and validates each supported encoding independently of IR and package versions. Existing encodings are the initial contract; no new mandatory field or version metadata is added merely for extraction. |
| Project container and participants | Storage validates the exact supported `FORMAT`, `CURRENT`, generation manifest and participant format contracts. Participant/schema compatibility cannot be inferred from a matching container alone. |
| GFDR framing and typed payload | Preserve ADR 0024's run version, record version and payload schema ID. Moving `IrLiteral` must not silently alter the JSON encoded under the existing payload schema ID. |

For #1011 and #1012, the chosen implementation is **encoding preserving**.
Existing ontology IDs occupy the untagged low range below `2^30`; runtime
entity IDs use bit 30 and runtime relation IDs use bit 31, with a local ID below
`2^30`. Both tag bits set is invalid. The new carrier retains these integers,
rejects invalid/out-of-range construction and decoding, and never renumbers
catalog entries. Check the kind against the catalog/ontology at semantic
admission; a syntactically valid integer alone does not prove a valid reference.

Golden vectors must pin boundaries and invalid IDs, each heterogeneous layout
and nested variant, and any moved literal/catalog serialization. Public Arrow
results, Python/Node adapters, and receipts must retain their representations.
Changing a Rust module path is not a reason to rewrite a committed generation.

If an implementation cannot preserve an encoding, stop that refactor and make
an explicit compatibility decision: assign a new applicable schema/format
version and reject unsupported inputs, or separately specify and test a bounded
migration through atomic generation publication. Never reinterpret old bytes
under the same version. This ADR authorizes no migration feature.

### Existing project-open and migration policy

The [pre-v1 project compatibility policy](../book/architecture/project-format-compatibility.md)
continues to apply. The current container marker is exactly
`graphforge-project/v1\n`; its `v1` is a format marker, not product v1.0.
The existing v0.5 generation protocol selects the canonical `CURRENT` record,
authenticates its named manifest, validates paths, and holds the selected
generation lease. It does not elect a generation by scanning directories.
Unsupported layouts fail with `GF_UNSUPPORTED_PROJECT_FORMAT` before graph
records are opened or project files are mutated. Malformed supported authority
uses the existing corruption errors. An explicitly empty directory may be
initialized; an unrecognized non-empty root may not.

There is no pre-v1 importer, automatic upgrade, read-only compatibility view,
identity conversion, or migration API for unsupported project layouts.
Unsupported data must be re-created through supported construction/ingest.
Within supported generations, the documented canonical Parquet bases,
immutable full-snapshot fragments and typed GFDR payloads remain supported.
The old routing-free/string-only GFDR prototype remains unsupported. Derived
compiled ontology tables may be rebuilt from committed JSON; this is not a
migration of authoritative graph data.

### Required implementation admission checks

The generic generation resolver remains a bounded container/manifest resolver;
it must not become a full graph scan. Once the selected participant is opened,
storage must use the shared schema recognizers and checked ID/value decoders at
catalog, topology/property and GFDR decode boundaries. Catalog admission must
reject invalid tags, duplicate/conflicting identities and unsupported carrier
schemas before exposing them to query execution. Deferred/batched reads must
validate values before returning or replaying each batch, not trust an open-time
container check as proof of every row. Portable import must use the same checks
before publication.

Unknown value tags and schema drift need stable typed contract errors, mapped
at storage boundaries to the existing unsupported-format or corruption error
as appropriate. A malformed supported value is not a request to try an older
codec. Persisted input must not panic, silently become null, or be repaired on
open. Valid reopen/recovery tests and invalid-input tests must demonstrate that
rejection leaves committed project authority unchanged. These are requirements
for implementation, not claims that the current readers already enforce all of
them.

## Bounded implementation and evidence

The decision unblocks existing work; it does not close that implementation:

- **#1011** introduces the checked ID carrier in the leaf crate, replaces tag
  logic in rel/storage/API, enforces ontology assignment bounds, and validates
  persisted catalog/topology identity decoding. Its existing golden-vector and
  public-representation criteria include valid reopen and invalid-ID rejection
  without rewriting the project. Catalog carrier/schema changes needed for this
  work must preserve the current encoding.
- **#1012** owns the shared heterogeneous schema and codecs in that crate,
  including the neutral literal representation needed by shared encode/decode.
  This supersedes its original tentative “core or IR” location. Its existing
  end-to-end write-back criterion must cross rel → exec → storage for every
  supported variant, then reopen durable values; golden vectors pin the layouts
  and any moved literal serde encoding. Unknown-tag/schema-drift cases exercise
  the same helpers used by storage and import.
- **#1008** consumes the resulting logical value/ID types for plan purity; it
  does not move runtime environment authority into the value contract.

No broad storage/ontology adapter rewrite is implied by these issues. Any
remaining edge must be described honestly until separately justified work
removes it. In particular, #1013 completes the architecture decision, not a
claim that Cargo already implements the target value boundary. Its evidence is
the inspected crate manifests and concrete reader/writer locations above,
aligned architecture documentation, and the docs build/link gates. The follow-up
issues require actual Rust/facade/binding evidence appropriate to their changed
surface; logical plans alone are insufficient.

## Alternatives rejected

Formally making IR the durable contract would entrench the existing compiler
and storage coupling and invite misuse of `IrVersion` on open. Putting all
Arrow schemas in core would broaden the foundational crate's dependency surface
and mix base identities with the evolving value wire contract. Duplicating
storage-only DTOs with ad hoc conversions would keep multiple incompatible
schema owners. The dedicated leaf keeps a single checked contract while
allowing compiler plans and durable values to evolve independently.
