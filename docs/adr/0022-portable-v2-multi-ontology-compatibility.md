# ADR 0022: Multi-ontology semantics in portable project v2

- Status: Accepted
- Date: 2026-08-19
- Issue: #835
- Supersedes: none

## Decision

M9 uses decision-gate option 1: it is representable through portable-v2's
existing authenticated, versioned compatibility semantics. It does not add a
component kind, change `graphforge-project/2`, or require portable v3.

Every M9 package contains one `compatibility` component whose participant ID is
`graphforge-ontology-composition`. Its one canonical JSON file is
`data/components/compatibility/graphforge-ontology-composition/composition.json`,
with media type `application/vnd.graphforge.ontology-composition+json`. The
manifest declares the required capability `ontology-composition@1`. This token
is intentionally unknown to pre-M9 readers, which already return
`unsupported_future` while validating manifest requirements and before reading
component payloads, materializing staging, or mutating a project.

The control document contract is `graphforge-ontology-composition/1`. It owns:

- the canonically ordered ontology inventory and each module's stable ID,
  exact version, canonical content digest, and dialect/profile;
- canonically ordered bridge-set identities, versions, canonical digests, and
  exact module endpoints;
- the activation profile and its exact active module and bridge-set identities;
- the composition digest over the preceding semantic fields; and
- required and optional feature tokens.

Its closed machine-readable shape is
`docs/contracts/graphforge-ontology-composition-v1.schema.json`. Arrays MUST be
in ascending UTF-8 identity order despite JSON Schema being unable to express
order; the Rust decoder enforces order and uniqueness before semantic use.

The manifest authenticates the control document's path, length, and digest and
therefore includes it in `package_digest`. The composition digest is
`sha256("graphforge-ontology-composition/1\0" || JCS(document without
composition_digest))`. Exact module and bridge payloads remain ordinary
`ontology` and `schema` components and are dependencies of the compatibility
component. TCK reports, validation transcripts, and benchmarks may be
`evidence` or `provenance` components, but the compatibility component MUST NOT
depend on them and their identities MUST NOT enter the composition digest.

Runtime catalog IDs, host paths, parser/library versions, machine configuration,
session state, credentials, and TCK outcomes are forbidden in the control
document. Interpretation and validation are Rust-owned; bindings and CLI only
project typed requests and reports.

## Why this is compatible v2

The current schema already closes component kinds while permitting versioned
required capability tokens. `compatibility` and `ontology` are existing kinds;
`requirements.capabilities` permits `ontology-composition@1`; and the existing
verifier rejects an unsupported token before payload verification. Both
expanded and bundle representations authenticate the same semantic manifest
and component bytes, so they retain one package digest.

The current verifier recognizes only `compatibility@1` and the closed runtime
generation map. It therefore correctly rejects an M9 package today rather than
silently importing semantics it cannot interpret. #841 must add the new closed
control schema and capability to the Rust verifier before it can accept M9.

## Current-v2 inventory reviewed

This decision was checked against
`docs/contracts/graphforge-project-v2.schema.json` and
`crates/graphforge-storage/src/project_portable_v2.rs` at `68a1655`.

| Surface | Existing v2 contract | M9 use or constraint |
|---|---|---|
| semantic component kinds | `ontology`, `schema`, `migration`, `settings`, `graph-data`, `derived-artifact`, `evidence`, `provenance`, `compatibility` | reuse `ontology`, `schema`, and `compatibility`; add no kind |
| compatibility fields | manifest `requirements.capabilities`, `dependency_rule`; state `compatibility`; authenticated compatibility component files | require `ontology-composition@1`; retain `required-transitive-closure/1` |
| schema extension points | versioned capability tokens, media types, participant IDs, files and dependency edges inside closed manifest fields | closed composition control is a versioned compatibility payload, not an unknown manifest member |
| semantic identity inputs | format/class, source generation, selection, components and file digests, requirements, states | composition control and exact ontology payload descriptors are included through existing component identity |
| excluded transport identity | representation headers/tags and `transport_digest` | expanded/bundle transport differences remain excluded |
| existing runtime control | `graphforge-runtime-generation-map/1`: capabilities, participants, graph placement, encoding, schema fingerprint, row count | remains separate; M9 semantics never enter runtime catalog IDs or host placement |
| verifier compatibility rule | closed supported capability list; unknown capability/dependency rule/major returns `unsupported_future` | pre-M9 rejection point for every M9 package |
| verifier schema rule | duplicate-free canonical JSON, closed manifest/runtime-map fields, closed kinds/classes, ordered unique IDs and paths | #841 adds an equally closed canonical composition decoder after capability recognition |
| verifier integrity rule | manifest/package digest and every path/length/content digest authenticated | composition and module/bridge corruption fail before authority change |
| expanded form | closed BagIt-compatible tree and manifests | composition path is an ordinary authenticated data component file |
| bundled form | canonical uncompressed PAX/ustar stream over the same files | byte transport differs; semantic manifest and query composition do not |

The schema's capability pattern is the intentional compatibility extension
point. Adding `ontology-composition@1` to a package is valid schema but not
supported semantics for the current reader, exactly the distinction represented
by `PortableV2ErrorCode::UnsupportedFuture` and
`PortableV2Compatibility::UnsupportedFuture`.

## Closure rules

- `complete`: all committed modules, bridge sets, activation profile, exact
  composition control, and graph/data components are included.
- `ontology-only`: all active modules, bridge sets, their schema authorities,
  and composition control are included; graph data is excluded.
- `component-selective`: selected modules or data require the transitive module,
  bridge, schema, activation, and composition closure needed to interpret them.
  Strict selection refuses implicit widening.
- `graph-data-subset`: the generic #786 graph selector applies, followed by the
  same exact ontology/bridge/schema composition closure as graph data.

No profile may contain a dangling module endpoint, bridge endpoint, activation
member, or composition input. Omissions are explicit stable portable participant
IDs. Inventory order never selects authority or resolves ambiguity.

## Reader and failure matrix

| Input | Pre-M9 v2 reader | M9-aware v2 reader | Mutation |
|---|---|---|---|
| valid M9 package | `unsupported_future` on `ontology-composition@1` | verify and report exact composition | none during verify/inspect |
| malformed control JSON/schema/order | normally `unsupported_future`; payload is not read | typed incompatible/invalid structure | none |
| component or composition digest mismatch | normally `unsupported_future`; payload is not read | typed digest mismatch | none |
| unknown optional feature | normally `unsupported_future`; M9 itself is unknown | ignore only when declared optional and semantics-neutral | none |
| unknown required feature/version | `unsupported_future` | `unsupported_future` | none |

Import never adopts, replaces, removes, activates, deactivates, or clears an
ontology module or bridge as a side effect of verify, inspect, or preview. A
separate explicit authority-change operation must validate and atomically
publish the already verified closure.

## Operational conformance

The generic portable-v2 limits, streaming copy buffer, cancellation checks,
source identity checks, private staging, cleanup, and no-replace publication
rules apply unchanged. The composition document is bounded by
`max_manifest_bytes`; inventory collections are bounded by `max_components`;
all arithmetic is checked. Cancellation or any structural, compatibility,
integrity, authority, or I/O failure removes private residue and leaves the
previous project generation authoritative.

Normative semantic vectors are in
`tests/fixtures/portable-v2/multi-ontology-vectors.json`. They cover every
package class, representation equivalence, exact identity inputs, malformed and
unknown features, closure, cancellation, cleanup, and authority non-mutation.

## Consequences

M9 implementation can proceed in #836 and #841 without inventing a v2 dialect
or flattening ontologies. Adding a field with required interpretation requires a
new required feature token or `ontology-composition@2`; changing the generic
manifest/component contract would require a deliberate portable format decision.
