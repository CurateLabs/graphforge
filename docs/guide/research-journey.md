# Follow a research question across two stories

This journey follows Ada, a character shared by *Mystery* and *Voyage*. Focus on
one story, work independently, review selected changes, and continue after
acceptance. Rust owns the behavior; Python, Node and the CLI expose the same
research operations. An agent or application can explain each step using the
returned fields.

The executable corpus is
[`analyst-journey-v1`](https://github.com/CurateLabs/graphforge/tree/main/tests/fixtures/analyst-journey-v1).
It contains synthetic scan and OCR inputs and an analyst assertion. Supplying
OCR text does not execute an OCR engine. The supporting
[acceptance matrix](../engineering/TESTING.md#analyst-ux-acceptance) covers richer
lineage, competing claims, ontology changes, failures and retention through the
owning native regression tests.

## Explore and focus

Discover the Project by its title, then select *Mystery* with a one-hop traversal.
Ada is included because of the `FEATURES` relationship. *Voyage* appears as
outside context: sharing Ada does not silently add the second story to the
selection. Evidence dependencies have their own output and do not become active
graph membership.

| Question | Native result | Fields to inspect |
| --- | --- | --- |
| What did I open? | `discovery.arrow`, `project-metadata.json` | `title` and Project metadata |
| What is selected? | `included.arrow` | `object_kind`, `object_uuid` |
| What remains outside? | `boundary.arrow` | Outside object identity and boundary context |
| Why is Ada here? | `explanations.arrow` | `reason`, `root_uuid`, `predecessor_uuid`, `via_edge_uuid`, `depth` |
| What supports the claim? | `source.arrow`, `ocr-artifact.arrow`, `assertion.arrow`, `claim-evidence.arrow` | Source/Artifact/assertion identities and explicit evidence links |
| Is this evidence or a judgment? | `claim-context.arrow` | Explicit classification, status and context; these are separate from raw graph membership |

Create a frozen Slice from one exact Version, then create Branch A for *Mystery*
and Branch B for *Voyage*. Both preserve Ada's identity, but each has an
independent current Version. Local edits in A do not modify B or the Project.
The native fixtures read the selected scan/OCR bytes from immutable Versions.
The Rust capture also selects an external-only Artifact. `external-artifact.arrow`
and `imported-external-artifact.arrow` expose its native availability and reference
metadata. Reading its historical payload fails with `GF_RESULT_NOT_RETAINED`
before and after import and cleanup: Core does not fetch replacement bytes.
This supporting Rust case adds the external limitation to the shared stage
sequence; the thin runners exercise the local scan/OCR route.

## Analyze, compare and review

In A, set Ada's score to 1 and ending to `detective`. The Project independently
changes `x` and `y`. Inspect `upstream-preview.arrow`, select only `property:x`,
and commit against that preview. A now has score 1, x 1 and y 0. The unselected
y update remains unapplied. `branch-analysis.arrow` is the actual query result;
`upstream-receipt.json` records the native operation.

`comparison.arrow` exposes changed object/field units. Submit only `property:score`
and `property:ending` from A's frozen Version. `proposal-preview.arrow` identifies
each review item with `item_uuid`, `field` and `preview_sha256`. Accept the score
and defer the ending. The Project's score becomes 1; its ending and private
annotation stay unchanged. The private annotation is absent from the submitted
field set and review preview.

`acceptance-receipt.json` and `accepted-history.arrow` identify the committed
operation and exact accepted contributions. `decision-history.arrow` records
research decisions. `canonical-choices-after-acceptance.arrow` remains empty:
acceptance does not implicitly promote a claim to canonical status. Explicit
promotion and pre-existing canonical choices have separate owner regressions
listed in the acceptance matrix.

## Continue and revisit

Continue A to score 2, B to score 73, and the Project to score 99. The
`continued-live-reference.json` resolves A's new head. The earlier
`immutable-reference.json` identifies the frozen source Version and its content
identity. A later resolution may report a different `resolved_generation_uuid`;
that is current resolution authority, not a change to frozen content.

Restore A's accepted Version. A returns to score 1, while B stays 73 and the
Project stays 99. Repeating the original review returns its receipt; reproposing
already accepted work does not apply it again. Reusing the operation identity
with changed content fails with `GF_IDEMPOTENCY_CONFLICT`. The journey repeats receipt/conflict and graph/ontology/evidence readbacks after
cleanup and reopen; the Proposal restoration owner test also reproposes after
cleanup. `restoration-outcome.json` is a
derived assertion summary, not a public response schema.

## Share with explicit scope

Restoration creates a fresh Version. `export-source-reference.json` captures
that Version separately from the earlier accepted source. A complete Version
export preserves the restored Version’s immutable identity and historical genealogy. `complete-verification.json` describes the verified package;
`imported-reference.json` identifies the imported Version. Importing historical
Branch genealogy does not create a live Branch with that identity.

A score-only export creates a distinct projected Version.
`selected-reference.json` distinguishes its content identity from
`version.content.source_version`, which records provenance. The private field
is absent from the imported graph. Disjoint exports and identity/content
conflicts are covered by the interchange owner tests.

A Fork creates an independent Project with explicit governance and metadata.
`fork-reference.json` distinguishes the new Project from its origin. Editing the
Fork leaves A unchanged. Governance and access-policy metadata describe intent;
a hosted application owns authentication and access enforcement.

## Run and inspect the evidence

From a source checkout with the pinned toolchain and an admitted durable
filesystem, run the native facade journey:

```sh
CARGO_TARGET_DIR=/path/to/isolated-target \
  cargo test -p graphforge-api --test research_journey -- --nocapture
```

To capture a fresh set of results, also set
`GRAPHFORGE_JOURNEY_CAPTURE_DIR=/path/to/new-output-directory`. The manifest lists
file sizes, SHA-256 digests, reader qualification and generated identity roles.
Arrow files preserve native schemas and values; JSON files serialize native
control results except the explicitly labeled assertion summaries. Generated
UUIDs and temporary discovery paths differ between runs. They are not stable
identifiers for another Project.

The same corpus is executed by:

```sh
uv run python crates/graphforge-bindings-py/tests/research_journey.py
node --test crates/graphforge-bindings-node/tests/research-journey.test.mjs
cargo test -p graphforge-cli --test research_journey -- --nocapture
```

Build native bindings from the same source revision before running the Python
and Node commands. The CLI test invokes its same-build binary. It uses a facade
setup call for the evidence link because that constructor has no CLI command;
the research stages themselves execute through the CLI.

These fixtures establish Core behavior and consumer-visible contracts. They do
not establish application rendering quality, observed human comprehension,
remote access enforcement, or perpetual compatibility with future readers.
