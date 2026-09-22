# Research workspace semantics

**Status: Designed — M11.** This is the implementation-facing contract for the
[analyst research experience](../../engineering/analyst-ux.md). Existing
checkpoints, knowledge records, and portable selection provide foundations;
they do not already implement this lifecycle.

## Ownership and authority

Rust owns Project, Slice, Branch, Version, Fork, comparison, update, and Proposal
behavior. The public facade composes domain records and graph/storage operations.
Python, Node, and CLI expose equivalent results and errors. Tabular content is
Arrow; lifecycle, metadata, explanation, and handles follow the existing
control-plane contract.

Associated projects such as XYG and graphforge-nextjs, applications, and peer
extensions consume these operations. They own navigation, rendering, hosting,
authentication, and authorization. They must not reconstruct Branch semantics
or invent a second research-state authority in a client.

Research lineage is distinct from a physical storage manifest. A Branch behaves
as a version-pinned overlay, but that is a semantic contract, not an instruction
to introduce a chain of mutable parent pointers or copy an entire corpus.
Branches share their Project's durable container and publication authority;
independent research does not mean an independent storage root. Branch heads,
Version records, parent changes, and acceptance receipts are authenticated
participants in complete Project generations under one `CURRENT`. Reuse the
existing writer locking and recovery protocol, not a second Branch commit or
cross-store transaction. Forks and exports provide independent copies.
This decision is recorded in [ADR 0032](../../adr/0032-research-project-authority.md).
Shared immutable payloads and bounded selection may avoid copying;
reconstruction must never consult the parent's current state implicitly.

## Identity and state

| Identity or state    | Required contract                                                                                                                                               |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Project              | Stable research identity with metadata, ontologies, Versions, and research lineage.                                                                             |
| Graph object         | Inherited nodes/edges retain public UUID identity; runtime catalog and ontology IDs remain distinct.                                                            |
| Slice                | A selection definition and its explainable boundary, not an independent Project.                                                                                |
| Frozen Slice         | Exact membership, boundary references, required evidence/ontology context, and source Version.                                                                  |
| Branch base          | Immutable immediate parent identity/Version, ultimate Project lineage, initial membership/selection, creator/time, evidence dependencies, and ontology context. |
| Branch current state | Base plus explicit local changes and deliberately incorporated content; every imported object records its source Version.                                       |
| Version              | Immutable state identity, distinct from a mutable Branch handle, a checkpoint name, storage generation counter, and package digest.                             |
| Proposal             | Exact source Branch Version, destination parent, selected changes, required dependencies, and review/acceptance history.                                        |
| Fork                 | New independently governed Project identity; original graph-object identity and derivation remain inspectable.                                                  |

Use existing UUID and domain registration rules. Do not introduce binding-owned
records, silently reinterpret existing frozen wire fields, or substitute storage
generation numbers for analyst Version identities.

## Selection and boundary closure

Dynamic Slices evaluate explicit search, filter, query, traversal, or direct
selection against current state. Frozen Slices resolve membership against one
Version. Return included objects, boundary references, inclusion explanations,
counts, and required evidence/ontology dependencies as separately inspectable
information. Dependencies do not silently expand active research.

For the Three Apples example, a shared character can be included while other
appearances remain outside the Slice. Explain whether each object came from
direct selection, story containment, evidence dependency, or a traversal rule.
Expansion/contraction previews must preserve those explanations.

Creating a Branch freezes the complete starting context, whether the input was
a whole Project, a dynamic/frozen Slice, a Branch, or a historical Version.
Operations must be bounded, cancellable, deterministic at a pinned Version,
and must not materialize an unrelated full corpus to create a small Branch.
Freeze the selected dependency closure, not the whole ancestor just because its
Version identifies the origin. Boundary references explain outside content but
do not promise that its historical bytes are retained. Historical expansion
outside the closure requires separately retained source history; otherwise
report unavailability and allow explicit selection of another available Version.

## Local changes and interpretation

Local operations are add, modify, suppress, replace, reclassify, and challenge.
Suppressing inherited graph content removes it from the active Branch graph
while preserving origin and historical state. Suppressing a knowledge assertion
removes it from the active Branch knowledge view; it is not permission to delete
every graph object that assertion references.

Queries and analyst verbs evaluate the selected Branch graph, including explicit
graph suppressions. Canonicality, confidence, hypothesis choice, and epistemic
status never silently filter it. Belief/interpretation filtering remains an
explicit operation. Canonicality is scoped acceptance, not a synonym for the
existing epistemic status “supported.”
Integration and canonical promotion are separate explicit recorded decisions.
They may share one reviewed publication, but accepting an alternative without
promotion preserves the target's canonical choices. Source-context canonicality
is provenance, not authority to promote in the destination.

Changing knowledge appends a successor/change event under the existing immutable
ledger model. Stable conceptual identity does not authorize mutation of an old
assertion row. Inherited content, local revisions, replacements, and competing
claims must remain distinguishable and connected by provenance.

Branch-local ontology extensions use exact module/composition identities and
scoped enforcement. Reclassification or importing an extension cannot silently
change parent authority. Retaining competing classifications is permitted only
when representable under the selected ontology policy; otherwise explain the
required extension or unresolved validation conflict.

## Evolution and comparison

Opening an existing Branch or Version never advances its inherited state.
Discover changes that affect selected objects and their evidence/ontology
dependencies; report unrelated parent changes separately from relevant updates.

Compare Branch/parent, Branch/Branch, Version/Version, Project Versions, and
current Branch/previously accepted Version. Report semantic additions,
modifications, suppressions, challenged canonical assertions, Sources,
annotations, and ontology changes with provenance.

The immutable original base is genealogy, not a permanently fixed merge base.
Retain an incorporated baseline at the selected object/field granularity: the
exact upstream revision/value last deliberately incorporated for each selected
unit. Unselected units keep their prior baselines. Stable graph-object UUIDs,
immutable revision identities, and operation/contribution identities remain
distinct. Partial acceptance records a mapping from accepted source
contributions to destination revisions; it never marks the whole source Version
accepted. Comparisons consume that mapping, including subsets accepted from
different Versions, to distinguish new divergence from already integrated work.

For example, from `x=0, y=0`, incorporating only upstream `x=1` advances the `x`
baseline to 1 and leaves the `y` baseline at 0. Against upstream `x=2, y=2`, an
unchanged local `x=1` is incorporated content, not a local edit. A subsequent
local `x=3` conflicts with upstream `x=2`. A local suppression remains explicit:
an upstream modification cannot silently resurrect the suppressed object;
review must resolve that conflict. An unselected update never advances a
baseline or discards a local decision.

An upstream-update preview identifies local, base, and proposed upstream states,
dependency consequences, and conflicts. The analyst selects compatible changes,
individual assertions, or source/ontology-only updates. Conflict outcomes are
keep local, adopt upstream, preserve both where valid, or add a claim explaining
their relationship. Commit only the reviewed valid selection and retain its
origin Version. A changed endpoint invalidates the preview before mutation.

“Reference” records an outside reference without importing a neighborhood.
“Bring into Branch” adds explicitly selected content and its required context
from a pinned source Version. Both retain provenance; only the latter enlarges
active research. Child Branches identify the immediate parent Version, retain
their selected base/dependency closure, and preserve ultimate Project genealogy.

## Proposal and acceptance

A proposal is frozen at submission. Later edits or upstream updates do not
rewrite its content. Select nodes, edges, claims, annotations, Sources,
ontology additions, or analytical results and preview required dependencies.
Do not silently add private annotations or unselected experimental research.

Review explains changes, motivation, evidence, impacted existing knowledge,
conflicts, and required ontology changes. Each item can be accepted, rejected,
or deferred. An accepted subset must have valid dependency closure: accepting
a relationship whose required ontology addition is deferred must either resolve
that dependency explicitly or leave the dependent relationship unaccepted.

Acceptance publishes the validated parent change atomically with provenance to
the exact Branch Version, analyst, evidence, and accepted item identities. The
same Project publication includes the durable acceptance receipt and any
explicitly reviewed canonical decision. Branch acceptance status derives from
that receipt; there is no second independently committed status write.
Retain contribution-to-destination mappings so a later proposal using a new
operation identity cannot apply the same contribution twice.

Updates and acceptance follow ADR 0018's linearization contract:

- Cancellation, validation failure, or stale review before `CURRENT` replacement
  leaves prior state unchanged (`committed: false`).
- A failure after `CURRENT` replacement does not roll back. Reconcile validated
  `CURRENT` under the writer lock and report `committed: true` with the committed
  outcome/receipt. Crash or I/O ambiguity resolves through supported reopen;
  never invent an intermediate state or claim rollback from a lost response.
- An exact retry returns the original receipt/outcome without applying changes
  again. Reusing an operation identity with changed content returns
  `GF_IDEMPOTENCY_CONFLICT` without mutation.

The Branch survives acceptance. Its state records local-only, proposed,
accepted, rejected/deferred, and superseded contributions. Further local changes
to accepted work produce a visible divergence and can form another proposal.
Parent publication never follows the live Branch automatically.

## Versions, retention, restoration, and interchange

[ADR 0039](../../adr/0039-research-version-publication.md) defines the
`research@1` storage foundation (#1535): authenticated Version records, context
heads, explicit dependency roots and permanent bounded receipts under the same
`CURRENT`. Its tests use storage context/root fixtures, including real Parquet
objects and local Artifact bytes. They are not Branch/Proposal or public-facade
proof. Source-generation retention is conservative until #1536; #1537 owns the
public Version surface. #1350 remains open until all original outcomes pass.

Retained Versions preserve graph, local changes, Sources/references, local
Artifact bytes, ontology composition, assertions, and research metadata.
Branch bases, child Branches, Proposals, and accepted-provenance dependencies
are retention roots for their selected content, required evidence/ontology
dependencies, and comparison baselines. An origin Version identifier alone is
not a whole-ancestor pin. Explicitly retaining a whole Project Version does pin
its whole state. Cleanup must preserve the required closure; explicit history
deletion reports blockers and never breaks retained content references.

Logical selection and physical retention must both be bounded. Shared payloads
may temporarily contain unrelated rows, but compaction/repacking must permit
reclamation of unrelated ancestor content once other roots release it. Prove
retained storage as well as copy cost with a fixed selection and increasing
unrelated parent data, including parent evolution, cleanup, and reopen. Merely
avoiding copies while permanently pinning a complete ancestor is insufficient.

External references retain their selected identity and integrity metadata.
Remote unavailability or unverifiable content is visible; readers cannot fetch
new bytes and present them as historical evidence. A reference alone is not a
promise that GraphForge has archived the external source.

Creating/restoring a Version uses the existing atomic publication discipline;
restore creates new current state and never rewrites historical state.
Checkpoints remain named retention/read facilities, not mutable research Branches.
Retained content survives reopen, supported recovery, and compaction with a
compatible reader. Preserve the format/capability and producer identity needed
to identify that requirement; reproducibility is not a promise of perpetual
latest-reader compatibility. Pre-v1 format evolution may reject incompatible
containers explicitly; it must never silently reinterpret a retained Version
or imply migration support.

Portable selection/import remain transport operations, not Proposal acceptance.
Research interchange must preserve selected Version identities, evidence and
ontology closure, Branch genealogy, and acceptance provenance, or reject an
unsupported package before mutation. Genealogy metadata must not silently widen
the export or retention closure to the complete ancestor. A Fork deliberately
creates a new Project identity and governance; package import alone does not
imply that decision.
Version-addressed sharing must distinguish a current Branch reference from an
immutable Version reference independently of the consumer's URL scheme.

## Consumer interaction contract

Define these projections in #1346 before runtime implementation. Each owning
issue delivers its real Rust and thin-surface evidence; #1358 certifies their
composition rather than defining the contract for the first time. These are
semantic requirements, not claims that new methods or wire fields already ship.

| Interaction                              | Core result required before consumer rendering                                                                                                                                                                                   |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Open current Branch or immutable Version | Project/Branch identity, immediate origin and original base, selected and current Version identities, derivative authorship, and live-versus-immutable reference kind.                                                           |
| Inspect or expand a Slice                | Included objects, outside boundary references, inclusion explanations, typed counts, required dependencies, and historical availability; no implicit import of outside content.                                                  |
| Compare or review upstream               | Exact endpoints, incorporated baselines, local/upstream/conflicting changes, and valid explicit resolution choices, including suppression conflicts.                                                                             |
| Review a Proposal or acceptance          | Frozen source Version, selected contributions, dependency consequences, accepted/rejected/deferred outcomes, canonical decision separately, and durable receipt/commit status. Unselected private annotations are absent.        |
| Continue a paged result                  | Deterministic order tied to pinned endpoints and selection; bounded pages and explicit completion. Reject invalid/stale continuations instead of mixing Versions; current-head advancement cannot silently change a pinned page. |
| Inspect evidence or recover an operation | Retained-local versus external-only evidence, unavailable/unverified status, structured failure phase and commitment status, and exact retry/reconciliation outcome without credentials or raw private content in diagnostics.   |

For the two-story fixture, a consumer can render a shared character's outside
appearances without importing them, cite a historical Branch Version while the
current Branch advances, review an alternative without canonical promotion, and
recover a lost acceptance response without submitting the contribution twice.
No browser, URL scheme, authentication provider, or application deployment is
required for Core certification. Policy metadata must not be presented as
enforced authorization.

## Existing contracts and implementation boundaries

- [Storage](storage.md) and ADRs 0013/0014/0018 govern atomic publication,
  checkpoints, retention, and durable acknowledgement.
- [Knowledge ledger](knowledge-ledger.md) and ADR 0006 govern immutable records,
  explicit interpretation, and provenance; research canonicality and scoped
  suppression extend these foundations. An assertion supersession “branch” is
  not a persistent research Branch. Analyst Evidence/Knowledge/Research/Lineage
  categories do not replace ADR 0005's Rust ownership layers.
- [Composable ontologies](composable-multi-ontology.md) govern inherited and
  locally extended ontology authority.
- [Portable projects](../../guide/portable-projects.md) govern bounded verified
  interchange; packaging is not semantic integration.
- [Testing](../../engineering/TESTING.md#analyst-ux-acceptance) defines the
  acceptance evidence needed to call this capability implemented.

### Selected storage retention

The storage primitive uses `research@2` and `registry@2`. Explicit compaction
moves exact selected participant commitments into shared CAS placement without
changing Version identity. Required graph manifests/payloads and local Artifact
bytes remain roots; the physical source generation becomes provenance only.
Graph projection has a separate immutable selector/content identity and repacks
shared Parquet units. Genealogy alone cannot retain an ancestor or expand a
historical view. Whole-Version retention remains an explicit complete-state
control. These are storage fixtures for later Branch/Proposal consumers, not
implemented Branch or Proposal lifecycles.

Root release removes only releasable dependencies. Accepted-provenance roots
remain permanent and operation receipts retain their separate lifetime limits.
CAS placement materialization allows at most 256 MiB of participant bytes per
Version (graph payloads are separately streamed and bounded by graph manifest
limits); explicit graph selectors allow at most one million identities.
Cleanup authenticates all retained CAS content before removing generations. A
busy CAS lifecycle guard returns `GF_WRITER_BUSY` for research cleanup without
mutation. Revision-1 readers refuse revision 2; there is no migration command.

### Native public Version lifecycle

The Rust facade exposes `prepare_research_version`,
`commit_research_version_operation`, `research_version`,
`list_research_versions`, `research_version_retention` and
`open_research_version`. Preparation freezes the exact source generation and
obtains the complete Artifact closure from the Source/Artifact owner. Keep the
returned operation unchanged: commit validates it, exact retries return its
permanent receipt, and changed content under its operation UUID returns
`GF_IDEMPOTENCY_CONFLICT`. A retry does not recapture today's Project or restore
old state again. Preparation is read-only and does not reserve a future commit.

A complete capture contains all authenticated non-history Project research
participants: graph, research metadata, ontology/configuration/composition,
provenance and enabled domain ledgers. It records locally verified Artifact
identities/digests/lengths and explicit external-only or unverifiable references.
The frozen Artifact table distinguishes missing-local from unverifiable status.
Complete means the complete recorded research state; it does not promise that
external or already-missing bytes were archived. Historical payload reads never
fetch external resources and are bounded to 256 MiB per returned Artifact.

`ResearchVersionView` runs real read-only native Cypher and reads frozen
ontology, metadata and Artifact records/bytes. Materialization authenticates
retained CAS content into a private temporary Project, including after source
generations were reclaimed. The Version record remains its original immutable
citation; temporary generation identity is not a replacement research identity.
Writes through a historical view are refused. Storage-created projections retain
their distinct identity and cannot expand beyond their retained closure. The
public complete-Project capture refuses raw participant/projection registration
without domain-owner closure; Slice and Branch owners supply those semantics in
their own implementation issues.

`RestoreProject` explicitly replaces frozen Project research and creates a fresh
Version under the source's owner context. It retains current research receipts,
identity tombstones, retention roots, other context heads and restoration
history. It rejects a projection as a complete Project source. `Restore` is the
separate context-only primitive for future Branch consumers; it does not replace
Project graph state. Both use the same CURRENT publication as their receipt.
After a committed error or replay on a stale facade, graph and metadata authority
are refreshed consistently; an unrecoverable refresh makes that facade unusable
until reopen rather than mixing generations.

`RetainRoot`, `ReleaseRoot`, `DeleteVersion` and `Compact` use explicit operation
identities and expected generation preconditions. Deletion reports current
head/root/required-Version blockers. Released payload does not erase the
operation's receipt or allow its identity to be reused. The existing registry
limits (1,024 retained Versions, 4,096 receipts/identities and roots, 256 contexts,
8 MiB canonical registry) are hard limits, not silent expiry or eviction.

Python exposes the corresponding snake_case methods; Node uses camelCase and
returns an asynchronous commit with optional `AbortSignal`. Python commits
accept a native cancellation token. Both expose historical query, ontology,
metadata, Artifact metadata and payload methods. Tabular results are Arrow
(Python tables, Node IPC); operation/identity/retention records are control JSON.
The CLI mirrors these under `gf research version`: `prepare --file`,
`commit --file`, `list`, `show`, `retention`, `query`, `ontology`, `metadata`,
`artifact` and `artifact-payload`. Requests are Rust-validated JSON and CLI files
are bounded to 1 MiB.

For example, a Python caller keeps the prepared operation for both publication
and any retry:

```python
prepared = graph.prepare_research_version({
    "operation_uuid": operation_uuid,
    "version_uuid": version_uuid,
    "context_uuid": project_context_uuid,
    "label": "Before annotation review",
    "description": None,
    "created_at": created_at_microseconds,
    "required_versions": [],
})
receipt = graph.commit_research_version_operation(prepared)
historical = graph.query_research_version(
    version_uuid, "MATCH (n:Person) RETURN n.node_uuid"
)
```

These APIs establish the Version foundation. They do not implement Branch edits,
Proposal acceptance, or interchange lifecycle. Their real composition remains
owned by #1352, #1356 and #1357; #1350 remains subject to its full acceptance audit.
