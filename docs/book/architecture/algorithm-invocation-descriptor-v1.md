# Neutral algorithm invocation descriptor v1

**Status:** Frozen for v0.5.0  
**Contract:** `graphforge-algorithm-invocation/1`  
**Canonical encoding:** [`graphforge-canonical/1`](canonical-fingerprints-v1.md)  

This contract gives every live `rank`, `cluster`, `paths`, `analyze`, and
`similar` algorithm one knowledge-neutral identity. It is owned by `graphforge-api`.
`graphforge-exec` continues to own graph projection and algorithm execution and never
opens provenance or knowledge records.

## Descriptor schema

An invocation descriptor has the same logical fields as one row of the
registered `invocation_descriptor@1` schema:

| Field | Arrow type | Null |
|---|---|---|
| `descriptor_version` | `UInt32` | no |
| `verb` | `Utf8` | no |
| `algorithm` | `Utf8` | no |
| `algorithm_version` | `UInt32` | no |
| `projection_fingerprint` | `FixedSizeBinary(32)` | no |
| `parameters` | `List<Struct<...>>` | no |
| `result_schema_version` | `UInt32` | no |
| `result_schema_fingerprint` | `FixedSizeBinary(32)` | no |

The `parameters` child struct is:

| Field | Arrow type | Null |
|---|---|---|
| `name` | `Utf8` | no |
| `value_kind` | `Utf8` | no |
| `bool_value` | `Boolean` | yes |
| `u64_value` | `UInt64` | yes |
| `u64_list` | `List<UInt64>` | yes |
| `f64_value` | `Float64` | yes |
| `utf8_value` | `Utf8` | yes |
| `uuid_value` | `FixedSizeBinary(16)` | yes |
| `uuid_list` | `List<FixedSizeBinary(16)>` | yes |
| `utf8_list` | `List<Utf8>` | yes |
| `f64_list` | `List<Float64>` | yes |

`value_kind` is the closed registry
`bool | u64 | u64_list | f64 | utf8 | uuid | uuid_list | utf8_list |
f64_list`. Exactly one corresponding value field is present. Parameter names are closed per
algorithm/version and rows are sorted by raw UTF-8 name bytes. Duplicate,
unknown, missing, or inapplicable parameters are structural errors.

The schema metadata contains only:

```text
graphforge.contract.id=invocation_descriptor
graphforge.contract.version=1
```

The public descriptor is encoded directly as this single logical record by
`graphforge-canonical/1` and fingerprinted with
`graphforge/invocation-descriptor`; `algorithm_run@1` persists those canonical
bytes in its registered descriptor field. No JSON, language-native map,
display string, or binding-specific serialization participates.

## Normalized inputs

Every descriptor contains the effective values consumed by dispatch, including
defaults. Optional inputs that do not apply to the selected algorithm are
omitted; supplying one is an error rather than an ignored value.

- `rank`: label, relationship selector, effective direction.
- `cluster`: label, effective direction, relationship selector or vector
  property as required by the selected family.
- `paths`: resolved source/target UUIDs when applicable, relationship selector,
  effective direction, `k`, weight/capacity/cost/heuristic properties,
  random-walk length and seed, sorted-distinct Steiner terminal UUIDs, and prize
  property when applicable.
- `analyze`: optional label, relationship selector, effective direction,
  weight, `k`, partition property, and every typed embedding option including
  normalized seed and ordered feature-property lists.
- `similar`: label, `k`, relationship selector or vector property as required
  by the selected family.

Aliases normalize to their canonical catalog value before encoding. Ordered
lists remain ordered except sets explicitly declared by the public algorithm
contract, such as Steiner terminals, which are sorted and deduplicated during
normalization.

`write_property` is not descriptor state. It is a separate graph mutation with
its own operation UUID and provenance transaction. A descriptor builder rejects
rank or cluster options containing `write_property`; recorded execution never
hides a graph write inside an algorithm-run lifecycle.

## Projection fingerprint

`projection_fingerprint` is not a project-generation digest. It identifies the
exact logical graph input consumed by the selected kernel so irrelevant project
files, Parquet layout, row groups, and generation UUIDs do not change an
otherwise identical invocation.

`graphforge-exec` resolves a registered `algorithm_projection@1` table containing:

| Field | Arrow type | Null |
|---|---|---|
| `record_kind` | `Utf8` | no |
| `subject_uuid` | `FixedSizeBinary(16)` | no |
| `source_uuid` | `FixedSizeBinary(16)` | yes |
| `target_uuid` | `FixedSizeBinary(16)` | yes |
| `name` | `Utf8` | yes |
| `value_kind` | `Utf8` | yes |
| `bool_value` | `Boolean` | yes |
| `i64_value` | `Int64` | yes |
| `f64_value` | `Float64` | yes |
| `utf8_value` | `Utf8` | yes |
| `f64_list` | `List<Float64>` | yes |
| `ordinal` | `UInt64` | no |

Closed `record_kind` values are `node`, `edge`, `node_value`, and
`edge_value`. Node and edge rows preserve UUID topology. Value rows contain
only graph properties actually consumed by the invocation after the kernel's
documented missing/default behavior has been applied. Exactly one typed value
field is present for a value row. Unit-weight edges use the reserved name
`$unit_weight` and an exact `1.0`; this cannot collide with a graph property.

Rows sort by the closed record-kind ordinal, then `subject_uuid`, `name`,
`ordinal`, `source_uuid`, and `target_uuid`. The canonical table fingerprint
uses `graphforge/graph-projection`. Graph surrogates, row offsets, physical
Arrow buffers, Parquet details, knowledge records, and execution timestamps
never participate.

## Preparation and dispatch

`graphforge-api` exposes typed descriptor preparation for the five analyst verbs.
Preparation validates and normalizes inputs, resolves the projection through
the same `graphforge-exec` projection adapter used by execution, and returns the typed
descriptor plus canonical bytes and fingerprint. It does not run the kernel or
write project state.

Descriptor dispatch performs these deterministic steps:

1. require descriptor, algorithm, parameter-registry, and result-schema
   versions supported by this binary;
2. reconstruct typed dispatch inputs without parsing display strings;
3. resolve the projection again through the shared adapter;
4. compare its full 32-byte fingerprint to the descriptor;
5. dispatch through the same executor used by the direct analyst method; and
6. validate the produced Arrow schema against the descriptor's registered
   result schema.

A changed projection returns `GF_PROJECTION_CHANGED` before kernel execution.
Unknown versions return `GF_UNSUPPORTED_CONTRACT_VERSION`; malformed parameters
return `GF_DESCRIPTOR_INVALID`; a schema mismatch returns
`GF_SCHEMA_MISMATCH`. Diagnostics contain versions, algorithm identity, and
safe mismatch categories only.

Python and Node builders and dispatch methods are thin adapters over these Rust
operations. They neither construct canonical bytes nor reimplement the
parameter registry. The same canonical bytes and full fingerprints must be
observable from all three surfaces.

## Exhaustiveness and evolution

The descriptor registry is generated from the five `graphforge-core` algorithm
catalogs. A catalog value without:

- a parameter contract;
- a semantic algorithm version;
- projection dependency declaration;
- result schema version/fingerprint; and
- direct/descriptor dispatch mapping

fails the deterministic exhaustiveness test.

Version 1 never changes meaning. A field, parameter, normalization, projection
dependency, algorithm semantic, or result schema change requires an explicit
new version. Existing descriptor bytes and fingerprints remain reproducible.
