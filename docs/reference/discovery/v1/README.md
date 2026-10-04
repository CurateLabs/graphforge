# Discovery v1 contract artifacts

This directory and the `graphforge-discovery` crate are the single discovery
wire authority. GraphForge API, CLI, and Hub adapters consume these types and
artifacts; they must not define a second manifest/refs model, media-type table,
or error taxonomy. The older `graphforge-hub/1` API-local draft and its fixtures
were removed because their different field names and semantics could not be a
compatible projection of this contract.

## Documents

| Document | Schema | Media type |
| --- | --- | --- |
| Manifest | `manifest.schema.json` | adapter-defined |
| Refs | `refs.schema.json` | adapter-defined |
| Project summary (`graphforge-project-summary/1`) | `summary.schema.json` | `application/vnd.graphforge.project-summary+json` |
| Research lineage (`graphforge-research-lineage/1`) | `lineage.schema.json` | `application/vnd.graphforge.research-lineage+json` |

These checked-in JSON Schemas describe the wire shape of GraphForge discovery
manifest, refs, and Project summary documents. `conformance.json` supplies valid
and invalid examples with stable Rust error results. Semantic rules that JSON Schema cannot
express—canonical ordering, cumulative bounds, safe URLs, capability
negotiation, and duplicate JSON members—remain authoritative in
`graphforge-discovery` and are exercised by the corpus.

Regenerate all four artifacts deterministically from the Rust test source:

```bash
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 \
  cargo test -p graphforge-discovery --test contract_artifacts
```

Running the same test without that environment variable compares generated
bytes with the checked-in files and validates every corpus case through the
public Rust parser. CI therefore fails when artifacts drift.

Downstream TypeScript may package these JSON files, use a JSON Schema validator
for early structural feedback, and run the conformance corpus against an HTTP
adapter. It must not transcribe the schema as hand-written TypeScript protocol
types or reimplement validation semantics. Rust remains the authority; generated
TypeScript declarations, if needed, must be treated as disposable build output
derived from these versioned artifacts.

There is intentionally no checked-in hand-written TypeScript discovery model.
Schema-derived Hub artifacts consume the required `package.object_digest` field
from `manifest.schema.json`; regenerating the Rust-owned artifacts updates that
versioned input and the conformance cases together.

## Project summary and ontology descriptors (protocol 1.1)

`ProtocolVersion::CURRENT` is 1.1. Version 1.0 was never released and is retired:
the manifest gained two optional fields, and readers reject unknown fields, so a
reader that predates them rejects a manifest that uses them. A manifest that
carries neither field is still valid, and clone consumers need neither.

- `summary { format, summary_digest, object_digest }` selects a Project summary
  object from `objects` by `object_digest`, the same way `package` selects the
  Project package. The object must use
  `application/vnd.graphforge.project-summary+json` and be at most
  `max_summary_bytes` (default 1 MiB).
- `ontology { composition_digest, modules[], bridge_sets[] }` lists exact
  identities in strictly ascending `(id, version, content_digest)` order, at most
  `max_ontology_entries` (default 4096) of each. A module may carry
  `package { format, package_digest, object_digest }` referencing a portable-v2
  package for that one module. That object must use
  `application/vnd.graphforge.project`, must not be the Project package object,
  and must be at most `max_module_package_bytes` (default 64 MiB). Bridge sets
  are listed by identity only.

The summary document embeds `repository`, `immutable_version`, and the package
reference (`format`, `package_digest`, `package_class`). It carries its own
closed `requirements`: only `project-summary@1` is understood. An unknown
required capability or an unknown format major fails `unsupported_future` during
parsing, before any other content is read. Unknown optional `capabilities` are
accepted; unknown fields fail `malformed_response`.

`metadata` is the public-safe projection of Project research metadata: every
field except `access.collaborators`, `extensions`, and `discovery_facets`. The
record's `contract_version` is not carried; the summary `format` versions the
document instead.
`access.visibility` and `access.access_policy` are consumer metadata, not
enforcement. Counts are the declared `corpus_size`. Strings are non-empty, at
most 4096 bytes, and free of ASCII control characters; lists hold at most 256
strictly ascending entries. `facts` holds verified package facts that need no
graph payload: `ontology_mode`, a component-kind histogram (only kinds with at
least one component), `payload_bytes`, `research_present`, `evidence_present`,
and an optional ontology composition. The presence flags must agree with the
histogram: `research_present` means the package carries a `research`-kind
component (the research-interchange registry), and `evidence_present` an
`evidence`-kind component. Research *metadata* presence shows up as non-null
`metadata` fields, not as `research_present`.

`DiscoveryManifest::bind_summary` requires a summary to name the manifest's
repository, `immutable_version`, and `package_digest`, to hash to
`summary.summary_digest`, and to carry an ontology composition equal to the
manifest's `ontology` inventory (both absent, or identical). A publisher whose
package carries an ontology composition must therefore also advertise
`ontology` in the manifest.

`summary.schema.json` describes the canonical shape and requires every
`metadata` and `facts` key, with `null` for absent values. The Rust parser also
accepts omitted optional keys and normalizes them, so every Rust-produced
summary is schema-valid, but a hand-written summary that omits a key can parse
in Rust and still fail schema validation. Rust stays authoritative.

### Three identities

These are distinct fields. None substitutes for another.

| Field | Identifies | Stable across publishing Projects |
| --- | --- | --- |
| `package_digest` | One exported portable-v2 package, including its source generation | No |
| `(id, version, content_digest)` | One exact ontology module or bridge set | Yes |
| `composition_digest` | The whole composition of one Project version | Only for identical compositions |

A module package's `package_digest` therefore differs between two Projects that
publish the same module, while the module's `content_digest` does not. Module
packages use the `component-selective` package class.

`gf ontology module fetch` is the command-line client of this exact-module
selection; it never requests the Project package.

### Locations are transport

The summary has no location field. Locations appear only in
`objects[].locations`, so summary bytes, `summary_digest`, and the `summary` and
`ontology` descriptors do not change when only locations change. Only the
manifest's own canonical digest does.

See [portable-v2-integration.md](portable-v2-integration.md) for the normative
validation order and the boundary between discovery and package verification.
