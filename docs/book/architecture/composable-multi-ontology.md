# Composable multi-ontology contract

This document is normative. **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, and
**MAY** have their RFC 2119 meanings. ADR 0023 records the decision.

## Stable identities and references

`OntologyModuleId` is `{ ontology_id, authored_version, canonical_digest }`.
`ontology_id` is a globally unique, NFC-normalized URI; `authored_version` is an
opaque NFC string interpreted only by that ontology's declared version scheme;
`canonical_digest` is lowercase SHA-256 over the domain-separated canonical
module document. Two modules are the same only when all three fields match.
The exact `/1` sorted-JSON grammar and intentional separation from the core
binary fingerprint envelope are specified in the
[ontology document compatibility boundary](canonical-fingerprints-v1.md#ontology-document-compatibility-boundary).

`QualifiedSymbol` is `{ module, kind, local_id }`, where `kind` is exactly
`entity`, `relation`, `property`, `constraint`, or `migration`, and `local_id`
is NFC-normalized and unique for its kind within the exact module. A runtime
catalog ID, host path, load order, parser version, or machine setting MUST NOT
appear in either identity. Unqualified lookup succeeds only with one candidate;
otherwise `resolution.ambiguous` reports a sorted, bounded candidate list.

`BridgeSetId` is `{ bridge_id, authored_version, canonical_digest }`. Each
assertion names exact qualified endpoints, a relation (`equivalent`,
`subsumes`, `maps_to`, or `evidence_for`), provenance, and optional validity.
Bridge ownership is independent: activating, updating, or deleting a bridge
does not rewrite a module or imply reciprocal/equivalent meaning.

### Creating permanent item IDs

The Rust facade's `create_ontology_module` and `create_ontology_bridge` MUST mint
an ID for a document without `ontology_id` or `bridge_id`, respectively, and
return the completed document in the candidate. They use the shared
`graphforge-discovery::ItemId` grammar and the operating system CSPRNG: 80 random
bits encoded as 16 lowercase Crockford base32 characters. Modules use
`https://graphforge.sh/ont_…`; bridge sets use `https://graphforge.sh/brg_…`.
An existing document ID MUST be retained. Authors MUST save the completed
document before adopting it; adopt, update, and module migration MUST reject a
missing ID with a create-first hint and MUST NOT mint one.

For any ID whose scheme and host are `https://graphforge.sh`, validation MUST
require the exact canonical lowercase URI and the correct item kind. Other IRIs
and existing short or legacy IDs (including `core` and `legacy:<digest>`) retain
their authored values. Authors using their own IRI SHOULD control its domain.

`gf ontology module|bridge create --input FILE` emits the completed JSON document
to stdout, or writes a new `--output FILE`. It MUST NOT rewrite the input.
`--json` emits the candidate receipt with the minted ID and completed document.
`--id` supplies a permanent token/URI or own-domain IRI, canonicalizes permanent
ID input, and MUST fail if it disagrees with the document.

### Regenerating a refused permanent ID

`gf ontology regenerate-id <id> --operation-uuid UUID [--yes]` MUST invoke the
Rust facade's `GraphForge::regenerate_item_id`. The CLI MUST obtain an explicit
affirmative (`y` or `yes`) before invoking the mutation, unless `--yes` is set.
EOF, an empty or invalid answer, and a negative answer MUST NOT invoke it.
Non-interactive stdin MUST require `--yes`; piped input MUST NOT confirm it.
Prompts MUST go to stderr so JSON stdout remains the native
`ItemIdRegeneration { id, version_uuid }` receipt, whose `id` is a bare ID.

Regeneration MUST require the local `id_taken` receipt stored by `gf publish`
for that exact permanent GraphForge ID. A missing receipt MUST fail; an
own-domain IRI MUST NOT be regenerated. If the receipt says `publish_as_owner`,
Core MUST refuse and point to publication as that owner. Regenerating an owned
item would create a fork, which this operation does not implement. Ownership
MUST NOT be inferred from the document, a caller-supplied owner, or a new Hub
lookup in place of the receipt.

Core MUST mint a fresh ID of the same kind and atomically publish the rewrite
as a new Version and head. Every adopted version under the refused permanent
ID MUST receive that same replacement ID in one operation, preserving each
authored version and the exact digest-derived identity of every reference.
A module rewrite MUST reuse the module-migration
candidate path, permit only the `ontology_id` change, and preserve local symbol
IDs through an identity symbol map. It MUST rewrite the module document and
canonical digest, dependent modules, bridge source/target modules and assertion
symbols, cascading bridge digests and dependent bridges' exact dependencies,
activation subjects, stored semantic bindings and derived storage routes,
workspace research metadata `ontologies`, and the composition fingerprint.
A bridge rewrite MUST replace `bridge_id`, dependent bridges' exact
dependencies, and activation subjects, recomputing affected bridge digests and
the composition fingerprint.

Earlier Versions and their content MUST remain unchanged. Publication SHOULD
start from the regenerated Version; the new ID does not replace the old ID in
historical Versions. Replaying the same operation UUID MUST return the original
replacement receipt without minting another ID or recording another Version.
A preparation whose source generation loses a write race MUST NOT rebase or
resample its replacement ID. An explicitly new operation MAY prepare from the
current workspace only after serialized native recovery proves the earlier
operation did not commit and its source generation can no longer publish.
The earlier operation binding MUST remain immutable and continue to refuse as
stale; committed or uncertain publication evidence MUST remain protected.

## Inventory, closure, and composition identity

An inventory contains unique exact module IDs and bridge IDs. Required module
dependencies name exact module identities. Closure is obtained by checked graph
traversal and then sorted by the UTF-8 bytes of
`(ontology_id, authored_version, canonical_digest)`; bridges are sorted the same
way by bridge identity. Input order, file order, registration order, and map
iteration order confer no precedence. Missing, duplicate, self, and cyclic
dependencies fail before activation.

`CompositionFingerprint` is lowercase SHA-256 of:

```text
"graphforge-ontology-composition/1\0" || ontology_sorted_json_v1({
  modules: [exact module identities in closure order],
  bridges: [exact bridge identities in identity order],
  activation: [sorted scoped activation records]
})
```

It excludes project generation, runtime catalog IDs, paths, parser versions,
TCK results, and machine configuration. A committed generation records the
fingerprint as authority provenance, but generation is not part of the digest.
Logical plans bind both generation identity and composition fingerprint.
Arrow schema and Parquet file metadata use
`graphforge.ontology.composition_fingerprint`; qualified columns additionally
use `graphforge.ontology.symbol`. Reopen MUST recompute and compare the
fingerprint before query or write. A mismatch is `coherence.fingerprint`.

## Project authority and enforcement

### Branch authority (M11)

The [research workspace contract](research-workspaces.md) requires a Branch
to inherit exact ontology composition and enforcement context at its base
Version. Local extensions and reclassification affect only that Branch.
Upstream updates and Proposals preview ontology dependencies before adoption;
deferring a required extension also prevents acceptance of dependent research.
Competing classifications coexist only when valid under the selected authority.
Module identity, composition identity, and runtime catalog identity remain
distinct. Native Branch, update and Proposal APIs implement this lifecycle;
[acceptance evidence](../../engineering/TESTING.md#analyst-ux-acceptance) identifies
the ontology isolation, selected adoption and dependency-valid acceptance tests.

### Current project authority

Reusable modules contain definitions, never activation policy. Project
authority owns an `ActivationProfile` with a default and exact overrides for
modules and bridges. Each scope is `exploratory`, `advisory`, or `strict`:

- exploratory accepts unknown runtime observations and records them only in
  the disjoint RuntimeCatalog;
- advisory preserves the operation and returns bounded structured warnings;
- strict rejects unresolved or violating operations atomically.

The most specific exact scope applies. There is no inventory-order fallback.
A bridge may make an explicit qualified resolution available, but matching
local names never creates a bridge. Plans retain the profile digest so a stale
plan cannot silently execute after authority changes.

## Lifecycle state machine

Modules and bridges move through `candidate -> validated -> adopted ->
superseded|removed`. `create/register` and `import` produce candidates;
`validate` and `preview_update` are non-mutating. `adopt`, `update`, and
dependency-aware `delete` require the preview's source generation and publish
the whole inventory, bridge inventory, activation profile, migrations, and
fingerprint atomically as one new generation. A stale source is
`inventory.generation_conflict`. Failure or cancellation publishes nothing.

`list`, `get`, and `inspect` return deterministic identity order. Export names
exact selected identities and complete dependency closure. Import verifies and
stages bytes but MUST NOT activate, replace, or delete authority implicitly.
Deleting an in-use module fails `dependency.in_use` with bounded dependants.
Forced deletion does not exist; callers explicitly update dependants/bridges
and preview again. Migration steps name exact qualified migration references,
are ordered by declared dependency, and roll back with the authority change.

Legacy projects with one adopted ontology reopen as a virtual one-module
inventory. The legacy canonical ontology digest is retained; a deterministic
`legacy:<digest>` ontology ID and authored version `legacy-v1` are recorded only
when an explicit migration publishes the first M9 authority generation. Until
then export remains legacy and no implicit format upgrade occurs.

## Query binding and explain receipts

`CompositionBindingContext` is the immutable Rust authority supplied to the
Binder and `GraphForge::execute_with_composition`. Resolution tries an exact
module qualifier first and permits shorthand only for one candidate. Adopted
bridge assertions retain their authored direction and bounded predicate;
symmetric predicates may be traversed in reverse, while conflicting or
non-computable required semantics fail deterministically.

Activation remains scoped. A module or bridge override determines the effective
exploratory, advisory, or strict policy for that decision; the composition
default is used only when no exact override exists. Advisory and exploratory
fallbacks intern a separately tagged RuntimeCatalog identity, while strict,
ambiguous, wrong-owner, invalid-endpoint, and conflicting binds fail without
publishing the staged catalog snapshot.

Every successful composed resolution adds a bounded `BindingExplainReceipt` to
the `GraphPlan`. It contains the exact composition fingerprint, effective mode,
ordered qualified/unique/bridge/runtime decisions, and attributable advisory
diagnostics with remediation. The plan-local numeric semantic projection is
derived only from identity-sorted qualified symbols; it is never persisted or
presented as an ontology or runtime-catalog identity.

## Diagnostics and resource contract

Every failure is `{ code, phase, message, subjects, candidates, limit }`.
Messages are explanatory but clients branch only on stable codes. Subjects and
candidates are identity-sorted, deduplicated, path-free, and capped by caller
limits. The required code families are:

| Phase       | Stable codes                                                                                         |
| ----------- | ---------------------------------------------------------------------------------------------------- |
| inventory   | `inventory.duplicate`, `inventory.not_found`, `inventory.generation_conflict`                        |
| dependency  | `dependency.missing`, `dependency.cycle`, `dependency.in_use`                                        |
| collision   | `collision.qualified_duplicate`, `collision.metadata`                                                |
| bridge      | `bridge.endpoint_missing`, `bridge.contradiction`, `bridge.provenance_missing`                       |
| coherence   | `coherence.fingerprint`, `coherence.plan_stale`, `coherence.storage_metadata`                        |
| enforcement | `enforcement.unknown`, `enforcement.violation`                                                       |
| migration   | `migration.path_missing`, `migration.failed`, `migration.rollback_failed`                            |
| interchange | `interchange.unsupported_future`, `interchange.integrity`, `interchange.selection`                   |
| resolution  | `resolution.ambiguous`, `resolution.not_found`, `resolution.kind_mismatch`                           |
| resource    | `resource.modules`, `resource.bridges`, `resource.symbols`, `resource.diagnostics`, `resource.bytes` |
| lifecycle   | `lifecycle.cancelled`, `lifecycle.invalid_transition`                                                |

Callers MUST provide finite maxima for module, bridge, symbol, dependency-edge,
diagnostic, migration-step, manifest-byte, and copy-buffer counts. Arithmetic is
checked. Cancellation is observed before document parsing, each closure edge,
each validation/bridge/migration unit, each streaming buffer, and publication.
Errors never include ontology payloads, host paths, credentials, or unbounded
candidate sets. Canonicalization rejects duplicate JSON keys, invalid Unicode,
non-NFC identifiers, unknown required fields, digest mismatch, and dependency
cycles before project mutation.

## Interchange boundary

Portable identity MUST preserve exact module/bridge identities, closure,
activation, and composition fingerprint while excluding runtime IDs and host
state. Expanded and bundled forms MUST retain one package digest and query
semantics; complete, selective, ontology-only, and graph-subset selections must
round trip without implicit adoption. Unknown required M9 features fail
`interchange.unsupported_future` before mutation. TCK output is evidence, not
semantic identity. Export/import is bounded, streaming, cancellable, and
atomic.

Portable encoding is decided by [ADR 0022](../../adr/0022-multi-ontology-compatibility.md)
(#835): records live in the authenticated compatibility component
`graphforge-ontology-composition` with media type
`application/vnd.graphforge.ontology-composition+json` and required capability
`ontology-composition@1`. #841 MUST consume that contract and MUST NOT invent a
new portable component kind.

Hub discovery advertises the same identities without a package download
([ADR 0051](../../adr/0051-discovery-project-summary-and-ontology-descriptors.md)).
A discovery manifest's optional `ontology` inventory lists each module as the
exact identity `(id, version, content_digest)` defined here, and a Project
summary reports the composition digest. A module may also reference a per-module
portable package; that package's `package_digest` is source-dependent and is a
different identity from the module's `content_digest` and from the composition
digest, none of which substitutes for another. See
[discovery v1](../../reference/discovery/v1/README.md).

## Worked composition and failures

The canonical fixture composes six modules: research owns `Study`, documents
own `Document`, genealogy owns `Person`, science owns `Specimen`, provenance
owns `Activity`, and evidence owns `Claim`. Explicit bridges connect a study to
documents, a person to evidence, and a specimen to provenance. Reversing the
input inventory produces the same closure and fingerprint.

The fixture oracle also requires these failures:

- `Person` in genealogy and provenance makes unqualified `Person` fail
  `resolution.ambiguous`; qualified lookup succeeds.
- An undeclared same-name `Claim` never becomes equivalent.
- A bridge whose endpoint digest is upgraded fails `bridge.endpoint_missing`
  until explicitly migrated.
- A cycle and a missing exact dependency fail before adoption.
- Removing evidence while research or a bridge depends on it fails
  `dependency.in_use`.
- Strict science rejects an unknown property; advisory documents emits a
  bounded warning; exploratory research records a runtime-only observation.
- A stale preview, over-limit inventory, malformed digest, cancellation, and
  unsupported interchange feature leave generation and fingerprint unchanged.

Machine-readable positive and adversarial examples live in
[`tests/fixtures/multi-ontology-v1`](../../../tests/fixtures/multi-ontology-v1/README.md).
