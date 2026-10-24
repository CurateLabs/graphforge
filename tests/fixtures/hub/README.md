# Rust-owned Hub fixture

This directory is the single GraphForge authority for the synthetic OpenAlex-themed Hub
example. `openalex-source/` is the complete, synthetic, expanded portable
source state of the `opendata/openalex` Project. `generated/v1/` contains the
deterministic bundle, Project summary, per-module package, and canonical
discovery manifest and refs emitted by Rust.

## Commands

Check drift (CI runs the same check in `crates/graphforge-cli/tests/hub_fixture.rs`):

```bash
cargo run -p graphforge-cli --example generate_hub_fixture
```

Regenerate `generated/v1/` after an intentional source or contract change:

```bash
cargo run -p graphforge-cli --example generate_hub_fixture -- --update
```

Rebuild `openalex-source/` itself, then regenerate (the generated artifacts bind
the source tree, so always follow `--rebuild-source` with `--update`):

```bash
cargo run -p graphforge-cli --example generate_hub_fixture -- --rebuild-source
cargo run -p graphforge-cli --example generate_hub_fixture -- --update
```

Generate and check artifacts for a public HTTPS preview data origin:

```bash
cargo run -p graphforge-cli --example generate_hub_fixture -- --update \
  --location-base https://preview-data.example.org/objects/sha256/ --output-dir /tmp/hub-preview
cargo run -p graphforge-cli --example generate_hub_fixture -- \
  --location-base https://preview-data.example.org/objects/sha256/ --output-dir /tmp/hub-preview
```

Both modes require the same base. It must be a canonical, credential-free HTTPS
URL with a public host, no query or fragment, and a trailing `/`. Check mode
rejects a different base or a changed location, even if the manifest digest is
recomputed. No flags still checks the production fixture at the default base.

The source, bundle, summary and manifest each declare the Core version that built
them (`producing_core_version`, from `graphforge_core::manifest::GRAPHFORGE_VERSION`),
so any change of the workspace version requires `--rebuild-source` followed by
`--update` and regenerated receipts. Never edit the version by hand.

After the artifacts change, regenerate the shared verification receipts as
described in `tests/fixtures/portable/README.md`. The fixture also supplies inputs
to native-generated conformance corpora. Regenerate these in order, so the source
transition corpus consumes the new admission corpus rather than the old one:

```bash
GRAPHFORGE_UPDATE_ITEM_ADMISSION_ARTIFACTS=1 cargo test -p graphforge-hub-publish \
  --test item_admission native_admission_oracle_and_checked_in_wasm_corpus
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 cargo test -p graphforge-hub-publish \
  --test item_source generated_transitions_preserve_sources_and_cas_preconditions
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 cargo test -p graphforge-discovery \
  --test item_resolution generated_contract_and_native_resolution_agree
GRAPHFORGE_UPDATE_PROTOCOL_WASM_EXPECTED=1 cargo test -p graphforge-protocol-wasm \
  --test corpus --test hub_documents
```

Run the native tests again without the update variables and replay the matching
corpora through the WASM tests. Review generated differences before accepting
them; never change an expected verdict by hand to make a candidate pass.

## The source

`--rebuild-source` builds a durable Project through the public GraphForge
facade, with fixed UUIDv7 operation and graph identities. Its handle-local
`with_write_clock` callback supplies `2026-01-01T00:00:00Z` (UTC microseconds),
matching the synthetic metadata dates. Composite topology and runtime-catalog
observations share that captured construction time; ordinary reopened handles
use the system clock. It adopts the existing
advisory ontology module, then commits two invented `Author` nodes, three
invented `Work` nodes and four `AUTHORED` relationships through a native graph
transaction. `Ada Example` authored `A Synthetic Atlas of Ideas`; `Basil Sample`
authored `Invented Networks for Learning`; both authored `Shared Paths in Example
Graphs`. There are no `Institution` records.

This is GraphForge-generated content, dedicated under CC0-1.0 and maintained by
GraphForge. It contains no real OpenAlex records and has no official OpenAlex or
OurResearch affiliation. Its native title is "Synthetic OpenAlex-themed
GraphForge example". Research metadata counts are measured from the committed
graph with native inspection, not entered as fixture constants. Unsupported
source/artifact counts are absent. Summary metadata copies these measured
declarations from the verified package; summary component and byte facts have a
different scope and do not independently count graph rows.

The ontology retains permanent identity
`https://graphforge.sh/ont_drqf2xw8w33cxaxj`, with authored version `2026.02`.
It preserves the existing `Work`, `Author`, `Institution` entity types and
`AUTHORED` from Author to Work, and adds only nullable, single-valued UTF-8
properties `Author.name` and `Work.title`. The exact native content digest is
recorded in `fixture.json` and the manifest. Composition ownership requires
these declarations for native transaction construction and useful Cypher
queries even in advisory mode. Native tests execute these traversals after
package import and durable reopen.

The earlier authored version `2026.01`, content digest
`sha256:64cbfecfb43f55bc433b0d06ecd8a75946be20ca8b7f8792b5ceed9aa7a83f11`,
is retained in immutable publication history and Git history; its empty
property declarations are not redefined.

After cloning `opendata/openalex`, try:

```cypher
MATCH (a:Author)-[:AUTHORED]->(w:Work)
RETURN a.name, w.title ORDER BY a.name, w.title
```

```cypher
MATCH (a:Author)-[:AUTHORED]->(w:Work)
RETURN a.name, count(w) AS works ORDER BY a.name
```

```cypher
MATCH (a:Author)-[:AUTHORED]->(w:Work)<-[:AUTHORED]-(b:Author)
WHERE a.name < b.name
RETURN a.name, b.name, w.title
```

The new source tree is fully verified and swapped in only when complete.
Nothing under `openalex-source/` is edited by hand; a unit test rebuilds it
independently and compares every byte with the checked-in tree.

`opendata/openalex` remains the repository identity, and `generated/v1/` remains
the fixture protocol path. Changed graph content produces a new immutable
Project package identity and new transport/summary/manifest/refs bindings.
Previously published package bytes remain immutable; Git history retains the
previous fixture. This local regeneration does not publish or replace live Hub
content. Hub owns downstream repinning, publication history and live acceptance.

## The generated artifacts

`generated/v1/` is exactly these files:

| File | Contents |
| --- | --- |
| `manifest.json` | canonical discovery manifest, including `summary` and `ontology` |
| `refs.json` | canonical refs; the `main` validator is the manifest digest |
| `objects/opendata-openalex.gfpb` | the complete Project package (bundle) |
| `objects/opendata-openalex.summary.json` | the canonical `graphforge-project-summary/1` document |
| `objects/ontology-module-<content digest>.gfpb` | a component-selective package carrying exactly one ontology module |
| `fixture.json` | provenance and the digests that bind the files above |

The generator fully verifies the source, imports and reopens it through the
public GraphForge facade with a fixed operation identity, exports and fully
verifies the canonical bundle, derives the summary from that verified bundle
(`summarize_verified_portable`, never from hand-entered metadata), exports one
package per advertised module, then constructs and validates manifest and refs
through `graphforge-discovery`. The manifest advertises the summary
(`summary.object_digest`) and each module (`ontology.modules[].package`) as
digest-addressed objects, and `bind_summary` proves the summary matches the
manifest. The check also verifies that every module package resolves, through
`resolve_discovered_ontology_module`, to exactly the advertised module identity.

`fixture.json` binds the checked-in source tree, the compiled generator source,
the selected location base, and the package, transport, summary, module, manifest, and byte-length
identities. It does not accept a caller-supplied Git commit. The invoking
checkout commit is the review and release provenance evidence.

Object locations are transport, never identity. `generate` takes a location base
(default `https://data.graphforge.sh/objects/sha256/`); a different base changes
`objects[].locations`, the manifest and refs digests that cover them, and fixture
metadata: the summary bytes and the `summary` and `ontology` descriptors are
identical.

## What downstream may copy

Downstream TypeScript may copy or import the six files in `generated/v1/` as
opaque versioned artifacts. It must not reconstruct protocol fields, validators,
digests, or compatibility rules, and it must not read `openalex-source/`.
