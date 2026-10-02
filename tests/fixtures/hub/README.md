# Rust-owned Hub fixture

This directory is the single GraphForge authority for the public Hub closeout
fixture. `openalex-source/` is the complete, synthetic, expanded portable-v2
source state of the `openalex/openalex` Project. `generated/v1/` contains the
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

After the artifacts change, regenerate the shared verification receipts as
described in `tests/fixtures/portable-v2/README.md`.

## The source

`--rebuild-source` builds the source through the public GraphForge facade: a
durable Project with fixed operation identities, synthetic research metadata
written by `update_research_metadata` (title "OpenAlex", license "CC0-1.0",
public/open access, declared corpus size 1000 nodes and 2500 relationships),
one adopted ontology module (`https://openalex.org/ontology/works` version
`2026.01`: `Work`, `Author`, `Institution`, one `AUTHORED` relationship) with an
advisory activation profile, then a complete expanded export. The new tree is
fully verified and swapped in only when complete. The values are fixture
content, not product behavior. Nothing under `openalex-source/` is edited by
hand; running `--rebuild-source` twice produces byte-identical trees, and a unit
test asserts that the rebuild equals the checked-in tree.

## The generated artifacts

`generated/v1/` is exactly these files:

| File | Contents |
| --- | --- |
| `manifest.json` | canonical discovery manifest, including `summary` and `ontology` |
| `refs.json` | canonical refs; the `main` validator is the manifest digest |
| `objects/openalex-openalex.gfpb` | the complete Project package (bundle) |
| `objects/openalex-openalex.summary.json` | the canonical `graphforge-project-summary/1` document |
| `objects/ontology-module-<content digest>.gfpb` | a component-selective package carrying exactly one ontology module |
| `fixture.json` | provenance and the digests that bind the files above |

The generator fully verifies the source, imports and reopens it through the
public GraphForge facade with a fixed operation identity, exports and fully
verifies the canonical bundle, derives the summary from that verified bundle
(`summarize_verified_portable_v2`, never from hand-entered metadata), exports one
package per advertised module, then constructs and validates manifest and refs
through `graphforge-discovery`. The manifest advertises the summary
(`summary.object_digest`) and each module (`ontology.modules[].package`) as
digest-addressed objects, and `bind_summary` proves the summary matches the
manifest. The check also verifies that every module package resolves, through
`resolve_discovered_ontology_module`, to exactly the advertised module identity.

`fixture.json` binds the checked-in source tree, the compiled generator source,
and the package, transport, summary, module, manifest, and byte-length
identities. It does not accept a caller-supplied Git commit. The invoking
checkout commit is the review and release provenance evidence.

Object locations are transport, never identity. `generate` takes a location base
(default `https://data.graphforge.sh/objects/sha256/`); a different base changes
only `objects[].locations`, the manifest and refs digests that cover them, and
nothing else: the summary bytes and the `summary` and `ontology` descriptors are
identical.

## What downstream may copy

Downstream TypeScript may copy or import the six files in `generated/v1/` as
opaque versioned artifacts. It must not reconstruct protocol fields, validators,
digests, or compatibility rules, and it must not read `openalex-source/`.
