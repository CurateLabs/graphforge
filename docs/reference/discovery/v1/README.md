# Discovery v1 contract artifacts

This directory and the `graphforge-discovery` crate are the single discovery
wire authority. GraphForge API, CLI, and Hub adapters consume these types and
artifacts; they must not define a second manifest/refs model, media-type table,
or error taxonomy. The older `graphforge-hub/1` API-local draft and its fixtures
were removed because their different field names and semantics could not be a
compatible projection of this contract.

## Documents

| Document                                           | Schema                 | Media type                                         |
| -------------------------------------------------- | ---------------------- | -------------------------------------------------- |
| Manifest                                           | `manifest.schema.json` | adapter-defined                                    |
| Refs                                               | `refs.schema.json`     | adapter-defined                                    |
| Permanent item (`graphforge-item/1`)               | `item.schema.json`     | `application/vnd.graphforge.item+json`             |
| Project summary (`graphforge-project-summary/1`)   | `summary.schema.json`  | `application/vnd.graphforge.project-summary+json`  |
| Research lineage (`graphforge-research-lineage/1`) | `lineage.schema.json`  | `application/vnd.graphforge.research-lineage+json` |

These checked-in JSON Schemas describe the wire shape of GraphForge discovery
manifest, refs, and Project summary documents. `conformance.json` supplies valid
and invalid examples with stable Rust error results. Semantic rules that JSON Schema cannot
express—canonical ordering, cumulative bounds, safe URLs, capability
negotiation, and duplicate JSON members—remain authoritative in
`graphforge-discovery` and are exercised by the corpus.

Regenerate manifest, refs, summary, and lineage artifacts from the Rust test source:

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

A TypeScript Hub that needs the validators themselves rather than the schemas
uses `@curatelabs/graphforge-protocol-wasm` (`packages/protocol-wasm`): the
`graphforge-discovery` and `graphforge-hub-publish` validators built to
WebAssembly, with TypeScript declarations generated from the Rust types. CI
replays this corpus through it.

There is intentionally no checked-in hand-written TypeScript discovery model.
Schema-derived Hub artifacts consume the required `package.object_digest` field
from `manifest.schema.json`; regenerating the Rust-owned artifacts updates that
versioned input and the conformance cases together.

## Digest-free exact authored-version resolution

`/<id>/<authored_version>` preserves exact `(id, version, content_digest)`
identity. The authored version is opaque: nonempty, at most 4096 UTF-8 bytes by
default, compared exactly without ordering, trimming, normalization or SemVer
parsing. It does not become immutable at publication. The URL adapter represents
it as one encoded path segment and must preserve the decoded value exactly.

Core's `resolve_item_version` takes the closed internal host inventory
`{id, version, complete, state, repositories, candidates: ItemDescriptor[]}`.
The ID is canonical lowercase; every candidate names that ID and exact opaque
version. The host supplies complete admitted, retained, currently readable
publications, filtering authorization and retirement **before input construction
and bounds**. Completeness and current authorization are host attestations, never
permission to truncate. These inputs must never come from an HTTP caller.

There is **one designated stable repository per permanent ID, for every version**.
First owning publication establishes it. Later versions published elsewhere,
owner updates and foreign exact reuse preserve it; only explicit current item-owner
rehome changes it. `ItemSourceDesignation` carries the ID, `StableRepositoryId`
and source CAS revision. Stable repository identity survives repository rename
and transfer; it is independent of mutable owner/name, item ownership and Project
identity. No internal repository key is added to public descriptors or URLs.

`repositories` contains exactly one fresh `ItemRepositoryFact` per candidate
repository: stable key, current canonical path and host binding revision. Both
keys and paths are unique. Candidate paths must match that table exactly, with
no missing or extra rows. No row is required solely for an unavailable designated
repository. The [inventory schema](item-resolution.schema.json) and
[source schema](item-source.schema.json) describe these native-owned shapes.

After validating every lifecycle, designation, repository and candidate record,
Core groups eligible candidates by content digest. Multiple distinct digests
refuse ambiguity even when no designated-source witness is available. Repeated
triples across repositories, snapshots or package digests are not ambiguity.
With one digest, an eligible witness in the designated stable repository is
required; an exact mirror elsewhere never substitutes. There is no newest,
lexical-digest, repository-order winner or digest-qualified lookup.

The closed `state` distinguishes established `absent_or_hidden`, currently visible
permanent-item `tombstoned`, and known visible `published { designation }`.
Absent/hidden and tombstoned states have empty candidates and repository facts.
Published state with zero eligible candidates is unavailable, not absence.
Unknown lifecycle or missing designation/completeness facts fail closed.

`bind_item_version_source` repeats resolution, requires full nominated-descriptor
membership **within the designated stable repository**, and calls
`ItemDescriptor::bind_snapshot` with that witness's actual retained refs and
manifest. Any complete equivalent witness within that repository may bind;
changed snapshot pins or package digests do not require item rehome. Snapshot
binding and repository designation are separate facts. It returns the nominated
descriptor unchanged: never splice repository, refs, manifest or package facts.
Module fetching still verifies the package from that manifest; bridges remain
descriptive. Source transitions use the native
[publication/rehome planner](../../hub-publish/v1/README.md#permanent-item-source-transitions).

The fixed candidate bound is 4096, **before deduplication**. The aggregate encoded
input budget is `DiscoveryLimits.max_response_bytes` (16 MiB by default), including
inventory (including repository facts), nomination, refs and manifest for source binding. Existing string and
per-document limits still apply. Native typed inputs use the same bounded
serialization budget without allocating a full encoding. JSON entrypoints reject
duplicate and unknown members. Incomplete inventories, invalid records, overflows
and stale/mismatched bindings fail closed without a descriptor.

`DiscoveryError::item_resolution_http_response` is the total Core HTTP projection:

- Established absence/all-hidden: HTTP 404, exact body `{"code":"missing_object"}`.
- Proven ambiguity: HTTP 409, exact body `{"code":"ambiguous_identity"}`.
- Currently visible permanent-item tombstone: HTTP 410, exact body `{"code":"gone"}`.
- Unavailable designated source, parsing, invalid inventory, completeness, bounds and binding failures: HTTP 503,
  **empty body**. This means valid resolution is unavailable; it does not promise
  malformed state will repair itself. Existing client retries remain bounded.

Refusal JSON uses `application/json`; its generated shape is
`item-resolution-error.schema.json`. A 404 requires **both** `missing_object` and
reserved field `item_version_inventory.candidates`, emitted only for validated,
complete, explicitly absent/hidden inventory. A 409 requires **both** `ambiguous_identity`
and reserved field `item_version_inventory.candidates.content_digest`, emitted
only for proven distinct digests. A 410 requires both `gone` and reserved field
`item_version_inventory.state`, emitted for a currently visible tombstone. Nested `MissingObject` fields such as `objects`
and `ontology.modules.package.object_digest` remain operational failures (503).
A raw code or message cannot establish a logical refusal. Unknown, missing
or malformed code/path yields empty 503. Refusals expose no candidate counts,
digests, owners, repositories or authored diagnostic text.

WASM exposes `resolve_item_version`, `bind_item_version_source` and the same total
`item_resolution_error_response` projection over its existing `ProtocolError`
shape (`field` is preserved as `path`). Both diagnostic projection input and CLI
refusal bodies are bounded to 1 KiB. The CLI recognizes only the closed 409
ambiguity document on an item-descriptor request, with `hub.ambiguous_identity`,
`GF_VALIDATION`, exit 2, before metadata/package fetch or output. Existing
`--digest` remains a verification pin and cannot override canonical ambiguity.

Hidden publications cannot change visible identity, ambiguity, bounds or error
details. Absent and all-hidden yield identical 404 responses. The host must
repeat current authorization, lifecycle, designation, retention and resolution before GET, HEAD and
conditional/304 responses, using a coherent view; a cached former success cannot
justify 304 after access or resolution changes. HEAD uses the same decision and
status without a body. These are host obligations: Core conformance fixtures
exercise supplied eligibility facts, not a production host's enumeration or auth.

Regenerate resolution artifacts from the native test authority:

```bash
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 \
  cargo test -p graphforge-discovery --test item_resolution
```

`item-resolution-conformance.json` is replayed by native Rust and the actual
protocol-WASM package. The native and WASM source-binding tests additionally
exercise retained snapshots, nominations, splicing and nested failure provenance.

## Permanent item descriptors

Tools request `application/vnd.graphforge.item+json` from `GET {hub}/<id>` for
the current item version, or `GET {hub}/<id>/<authored_version>` for an exact
authored version. The negotiated document has this closed shape:

```json
{
  "format": "graphforge-item/1",
  "id": "ont_7k2m9x4q8r3t5v1w",
  "kind": "ontology_module",
  "repository": { "owner": "opendata", "repository": "openalex" },
  "module": {
    "id": "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
    "version": "2026.01",
    "content_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "discovery": {
    "refs_revision": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "manifest_validator": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
}
```

`id` is always a canonical lowercase bare permanent ID. `kind` is
`ontology_module` for an `ont_` ID or `bridge_set` for a `brg_` ID. The
`module: ExactIdentity` field carries that ID's canonical URI, an opaque authored
version, and its canonical content digest. The ID, kind, and URI must agree;
case normalization belongs to client input, not to wire descriptors. The
repository uses the existing canonical `owner/repository` rules.

`discovery` identifies the retained refs snapshot and manifest by their canonical
SHA-256 digests. `ItemDescriptor::bind_snapshot` recomputes both digests, binds the
repository, resolved ref, manifest validator and immutable target, and requires
the exact module identity in that manifest's ontology inventory. A current refs
snapshot cannot replace the named snapshot after refs advance.

The client reads fixed paths relative to the descriptor's repository:
`/.gf/refs?revision=<refs_revision>` followed by
`/.gf/manifest?validator=<manifest_validator>`. Query values are the canonical
`sha256:` digests (URL-encoded for transport). An immutable authored version or
Project target is not a metadata selector. HTTP ETags alone are insufficient,
and missing retained metadata never falls back to the current documents.

A server retains the actual admitted RefSet and manifest atomically, with
registry reachability pins while it serves their descriptor. These reads remain
repository-scoped: current visibility and authorization apply before GET, HEAD,
and conditional/304 responses. Descriptor and snapshot retirement must fail
consistently; this contract introduces no indefinite retention requirement for
unreferenced snapshots. Repository moves atomically publish a fresh descriptor
with pins for the actual rehomed metadata. Stale pins fail closed.

Bridge descriptors are descriptive. Bridge inventory has no package reference,
so `gf ontology module fetch` rejects bridge kinds and does not claim bridge
package fetch support.

`ItemDescriptor::from_json` is the Rust-owned parser and validator. It refuses
duplicate JSON members, unknown members, unsupported formats, inconsistent
identities, and configured response/string bounds. Its generated TypeScript
type and `validate_item_descriptor` WebAssembly export are the same contract.
The schema describes structural constraints; equality of the bare ID and the
URI's token remains a native validation rule. `item-conformance.json` exercises
both kinds and the stable failure codes through Rust and WebAssembly.

The descriptor carries no package location or native package bytes. A client
still binds repository refs to a manifest, selects the exact module package
advertised there, and runs the existing package and module verification path.

Regenerate these two artifacts with:

```bash
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 \
  cargo test -p graphforge-discovery --test item_descriptor
```

## Project summary and ontology descriptors (protocol 1.2)

`ProtocolVersion::CURRENT` is 1.2. Versions 1.0 and 1.1 were never released and
are retired: the manifest gained two optional fields (1.1) and every package
reference gained the required `producing_core_version` (1.2), and readers reject
unknown fields and missing required fields. A manifest that carries neither
optional field is still valid, and clone consumers need neither.

- `summary { format, summary_digest, object_digest }` selects a Project summary
  object from `objects` by `object_digest`, the same way `package` selects the
  Project package. The object must use
  `application/vnd.graphforge.project-summary+json` and be at most
  `max_summary_bytes` (default 1 MiB).
- `ontology { composition_digest, modules[], bridge_sets[] }` lists exact
  identities in strictly ascending `(id, version, content_digest)` order, at most
  `max_ontology_entries` (default 4096) of each. A module may carry
  `package { format, package_digest, object_digest }` referencing a portable
  package for that one module. That object must use
  `application/vnd.graphforge.project`, must not be the Project package object,
  and must be at most `max_module_package_bytes` (default 64 MiB). Bridge sets
  are listed by identity only.

Every package reference (`package` in the manifest, module packages, lineage
Versions and Proposals, and the summary) declares `producing_core_version`: the
exact GraphForge Core version that wrote the package. It is an opaque exact
label (non-empty, bounded, no control characters) that readers compare for
equality and never order or parse. A Hub reads it from the manifest, or from the
summary, without downloading the package; the verifier requires the package's
own manifest to declare the same value.

The summary document embeds `repository`, `immutable_version`, and the package
reference (`format`, `package_digest`, `producing_core_version`, `package_class`). It carries its own
closed `requirements`: only `project-summary@1` is understood. An unknown
required capability or an unknown format major fails `unsupported_future` during
parsing, before any other content is read. Unknown optional `capabilities` are
accepted; unknown fields fail `malformed_response`.

`metadata` is the public-safe projection of Project research metadata: every
field except `extensions` and `discovery_facets`. The record's
`contract_version` is not carried; the summary `format` versions the document
instead. `authors` is user-authored display text, published as written. No
access field exists in any format; the host enforces access. `people` lists the
authors and committers of the published research lineage's Versions (each
distinct signature with its `roles`), derived and checked by
`ProjectSummary::bind_lineage`; credit is public and permanent. Counts are the
declared `corpus_size`. Strings are non-empty, at
most 4096 bytes, and free of ASCII control characters; lists hold at most 256
strictly ascending entries. `facts` holds verified package facts that need no
graph payload: `ontology_mode`, a component-kind histogram (only kinds with at
least one component), `payload_bytes`, `research_present`, `evidence_present`,
and an optional ontology composition. The presence flags must agree with the
histogram: `research_present` means the package carries a `research`-kind
component (the research-interchange registry), and `evidence_present` an
`evidence`-kind component. Research _metadata_ presence shows up as non-null
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

| Field                           | Identifies                                                     | Stable across publishing Projects |
| ------------------------------- | -------------------------------------------------------------- | --------------------------------- |
| `package_digest`                | One exported portable package, including its source generation | No                                |
| `(id, version, content_digest)` | One exact ontology module or bridge set                        | Yes                               |
| `composition_digest`            | The whole composition of one Project version                   | Only for identical compositions   |

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

See [portable-integration.md](portable-integration.md) for the normative
validation order and the boundary between discovery and package verification.

## Repository moves and re-homing

A moved repository returns HTTP **308** for its old `.gf/refs`, `.gf/manifest`,
and `.gf/publish` endpoints (including publish subpaths). `Location` names the
same endpoint under the new canonical owner/repository on the **same HTTPS
origin**. The bounded JSON body uses `moved.schema.json`:

```json
{
  "format": "graphforge-repository-moved/1",
  "moved_from": { "owner": "curate", "repository": "claims" },
  "moved_to": { "owner": "research", "repository": "renamed" }
}
```

Clients accept one identity move only, require refs and manifest to agree, and
validate the returned documents against the new identity. Publish resolves this
identity before acquiring its repository-scoped credential. Writes are never
forwarded through redirects. Cross-origin, downgraded, chained, ambiguous, or
inconsistent identity moves fail closed. CLI JSON adds `moved_from` and
`moved_to` only after a verified move; human output reports the same transition.
Ordinary object-location redirects retain their existing credential isolation.

Before a moved clone contacts either new metadata endpoint, it verifies both
old metadata markers. The read adapter then replaces only its current metadata
identity and cached repository grant. It retains the same origin-bound user
session, reapplies configured protection policy only to fresh verified-current
metadata requests, and acquires a new repository grant through normal auth.
It never treats an old repository grant as authorization for the new name.
Object requests retain their separate origin and redirect restrictions.

`rehome_repository` is the Rust derivation authority for a Hub transaction. It
accepts the current ref set and exactly the bounded closure of stored manifests
referenced by that set, with their exact advertised summary and lineage bytes.
It verifies every original manifest validator, immutable ref target, control
object digest/length, summary binding, and intrinsic lineage/package binding
before deriving replacements. The current default snapshot also retains the
normal strict ref/lineage check. Historical snapshots are not checked against
invented historical ref sets.

The result replaces repository identity in manifests, summaries and lineage,
then derives control-object digests, manifest validators and the refs revision.
A validated explicit `ObjectLocationTemplate` supplies newly derived control
locations (`{digest}` or `{hex}`); old paths are never inferred by string
replacement. Native Project/module bytes, package identities and their object
locations remain unchanged. Fork origin citations remain immutable published
facts. The Hub must retain old object routes and commit the complete replacement
set atomically with an unreusable old-name tombstone. Repeated administrative
moves flatten old tombstones directly to the current name, rather than exposing
redirect chains. Re-homing does not migrate an in-flight publish operation.

The move schema and `moved-conformance.json` are generated and replayed with:

```bash
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 \
  cargo test -p graphforge-discovery --test repository_contract
```
