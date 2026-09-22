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

## Implemented Branch facade (#1352)

`create_research_branch` creates an independent research context from current
Project research, an exact retained Version, a Branch head, or a frozen Slice.
It publishes one immutable base and head through the owning Project's `CURRENT`.
`open_research_branch` returns the effective read-only native graph for Cypher,
analyst and domain reads. `execute_research_branch` applies native graph changes
to a private prepared view and publishes a fresh Branch Version. Parent and
sibling heads do not advance. Graph suppression uses ordinary native deletion;
`suppress_research_branch_assertion` removes the assertion and its owned
interpretation state without deleting referenced graph objects.

`research_branch_selection` exposes the permanent base's `exact_membership/1`
selector as Arrow `object_kind`, `object_uuid`, and `role` rows. This is the
retained selection definition, independent of later edits or parent cleanup.
Schema metadata identifies the origin and base Versions and the original source
selector digest. That digest is provenance; it does not recover an original
query predicate. Required context remains required in whole-Branch and historical
Branch-Version children unless explicitly incorporated as active research.

The view's `fields()` Arrow result exposes immutable original Version/value,
incorporated Version/value, current value, stable contribution identity, role,
and inherited/local/suppressed status. UUID identity is not a revision counter.
`record()` exposes frozen genealogy and creation metadata; `version_uuid()` names
the opened head. Existing Version listing supplies retained names and labels.
The base is a retention root. Origin genealogy alone does not pin complete
ancestor payload, and expansion beyond selected content requires explicitly
retained source history (`GF_RESULT_NOT_RETAINED` otherwise).

`reference_research_branch` adds a Version-qualified citation; `references()`
returns Arrow citations without expanding active research or pinning source
payload. `bring_research_branch` authenticates a frozen Slice and incorporates
its selected graph/domain closure with original UUIDs and source baselines.
Conflicting existing object values refuse before publication. Bring requires
identical ontology and composition; `change_research_branch_ontology` performs
an explicit exact-composition change with native stored-data validation first.
Selected unsupported domain dependencies refuse rather than drop state or import
unselected content. Automatic conflict resolution, updates and Proposal
acceptance belong to subsequent M11 issues.

`restore_research_branch` requires a retained Version of that same Branch and
publishes a new Version under only its context. It preserves parent research,
other Branch heads and Project operation history. Every mutation accepts a
stable operation UUID, an exact expected `CURRENT`, and cooperative cancellation.
Exact retry returns the original receipt and reconciles the local facade with
live authority; changed content under an operation UUID conflicts.

Python exposes matching snake-case methods with dict controls and Arrow tables.
Node exposes asynchronous camel-case methods with metadata or Arrow IPC results.
Both use explicit Branch UUID read adapters for info, query, fields, references
and creation selection. `gf research branch` provides create, execute, restore,
ontology, reference, bring, suppress-assertion, info, selection, fields,
references and query commands. Mutation `--file` inputs contain the native JSON
request. Read data defaults to Arrow IPC; `--json` uses the existing CLI renderer.
The versioned contract is `tests/contracts/branch-api-v1.json`.

Requests are bounded to 1 MiB canonical JSON (including encoded capsule bytes).
Native query/selection and semantic-field preparation use 64 MiB working bounds;
prepared immutable participant transfer is bounded to 256 MiB. Baseline reads
preflight uncompressed Parquet and field allocation before decoding/expansion.
These are bounded operations, not streaming arbitrary-sized whole-Project
Branch creation. Materializing a selected view can copy its selected closure;
there is no zero-copy claim. The native retention regression grows unrelated
parent content, measures selected payload and retained CAS after parent evolution
and cleanup, releases the source Version, then reopens the Branch and its frozen
selection. Creation time can still include parent metadata scans.

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

### Implemented Slice facade (#1351)

`preview_slice(request, kind, page)` accepts `current` or an explicit immutable
`version` source and a `direct`, `filter`, `query`, `search` or `traverse` selector.
The separate Arrow page kinds are `included`, `boundary`, `explanations`,
`dependencies` and `counts`. Graph selection queries return canonical
`node_uuid` and/or `edge_uuid` columns. Filters bind scalar values independently
of identifiers. Text search is local native retrieval, without provider calls.
Traversal has a maximum of 64 hops and canonical UUID ordering, with one root,
predecessor and edge per included object rather than duplicated full paths.

`include` and `exclude` revise active membership. Required endpoints, evidence,
Sources, Artifact derivation inputs and ontology context remain separate; missing
required local objects fail explicitly. Boundary references disclose outside
relationships and source-Version genealogy without importing content.
`ontology_context` identifies the chosen source context; it is not an ontology
ID. Counts distinguish object families and active graph labels such as Passage.

`freeze_slice` requires an explicitly retained Version and returns an Arrow
membership capsule. Serialize the returned Python table with `pyarrow.ipc`;
Node already returns IPC bytes. `inspect_frozen_slice` reads exact membership
without rerunning its selector, even after the Version payload is released.
`revise_frozen_slice` accepts exact additions/removals and an optional explicit
`source_version` for outside-history expansion. An unavailable original or
outside Version returns `GF_RESULT_NOT_RETAINED`; current parent bytes are never
substituted. Capsules bind Version, selector, ontology and Artifact commitments,
but establish no new retention root. They are not self-contained graph packages
or signed authorization. Future Branch creation must retain and validate its
chosen source. See [ADR 0040](../../adr/0040-frozen-slice-membership.md).

Limits bound decoded source/selector rows, active objects, required references,
boundaries, collection bytes and final IPC bytes including schema metadata.
The query memory pool uses the requested working budget; its budget and Slice
collections are separate bounded allocations. `scanned_rows` does not count
physical query-operator work. Cancellation is cooperative during query streaming,
search, traversal and closure processing. Query response pages never return a
partial selection after a bound is exceeded. Page tokens bind source, request,
selection, page kind and frozen context; changed current heads, mismatched
selectors and malformed tokens are rejected.

Python uses the four snake-case facade names; Node uses their camel-case names
and promises with optional `AbortSignal`. CLI parity is:

```sh
gf --project ./research research slice preview --file selection.json --kind boundary
gf --project ./research research slice freeze --file version-selection.json > slice.arrow
gf --project ./research research slice inspect --capsule slice.arrow --kind included
gf --project ./research research slice revise --capsule slice.arrow --file revision.json > revised.arrow
```

Freeze/revise write Arrow IPC and reject `--json`; previews and inspections may
use the existing JSON display option. Input contracts and ceilings are pinned
in `tests/contracts/slice-api-v1.json`. Selection alone creates no independent
research state. The remaining Branch, comparison, update and Proposal lifecycle
below remains designed work.

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

The native contextual claim contract is implemented by the Rust facade and thin
Python, Node and CLI transports. `create_research_claim` creates an immutable
assertion with a research category; evidence stays in the existing Source,
Artifact and evidence-link owners. `relate_research_claims` records alternatives,
contradictions, disputes, refinements, supersession or support. Neither operation
promotes a claim. Machine extraction requires an existing producer run.

`change_research_branch_claim` publishes one Branch Version for creation,
challenge, revision or knowledge suppression. Revision appends a successor and
native supersession history; prior claim bytes and parent state remain unchanged.
`inspect_research_claims` selects an explicit Project or Branch context and
community, returning Arrow rows with category, creator/run, origin Branch/Version,
canonical status, suppression and local/inherited/modified state. Statusless
assertions remain visible. Set `include_suppressed` to inspect hidden claims;
`research_claim_history` returns their classification, relation, suppression,
status, reasoning or evidence owner history.

`record_research_decisions` records explicit `integrate`, `promote` or `revoke`
decisions for typed node, edge or assertion subjects. A bounded batch can record
integration and promotion together, but neither implies the other. Current
choices and append-only history are available through `research_canonical_choices`
and `research_decision_history`. Decisions belong to current Project authority
and survive restoration; frozen claim content belongs to the selected Version.
A restored Project can revoke a historical decision even if its subject is absent.
Community identity is scope metadata, not an access-control mechanism.

Python accepts native request dictionaries and returns Arrow tables. Node accepts
the same snake-case request objects and asynchronously returns Arrow IPC for data
and a durable receipt for Branch changes. Python Branch changes also return
receipts. CLI
commands use `gf --project PATH research claim
create|relate|change-branch|decide|inspect|history|decisions|canonical --file REQUEST.json`.
Mutations require an operation UUID and expected CURRENT generation. Exact retry
returns the original result; changed requests or stale first publications fail.
The concrete limits and transport contract are in
`tests/contracts/research-claims-api-v1.json`. Proposal review is described below.

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

**Implemented (#1354):** `GraphForge::compare_research` returns native Arrow
changes or summary indicators for Project, Branch, and exact retained Version
endpoints. Python `compare_research`, Node `compareResearch`, and
`gf research compare --file request.json` transport the same closed
[request contract](../../../tests/contracts/research-comparison-api-v1.json).
The left endpoint is local/earlier research; the right is upstream/later research.

Rows identify the native object and field, local/upstream/conflict disposition,
value commitments, immutable origin, incorporated Version, contribution UUID,
and any explicitly accepted source/destination Versions. Annotation and challenge
history uses the existing assertion owners. Ontology rows use semantic identities,
not runtime catalog IDs. Missing evidence and unretained citations are explicit
`dependency_unavailable` rows. Immediate-parent reads select the Branch's known
objects and citations; unrelated corpus additions do not become Branch updates.
Workspace ontology remains shared context authority.

Comparisons never publish research. Accepted mappings are explicit per-field
inputs validated against exact retained source/destination content and native
contribution identity; they do not replace the Proposal acceptance ledger.
Several fields may cite different accepted Versions. Canonical comparisons require
explicit context/community and optional decision-sequence cutoffs; frozen Version
timestamps never imply canonical promotion. Native Proposal review applies
explicit acceptance; upstream update application remains #1355 work.

`max_fields`, `max_bytes`, and `page_size` bound admitted semantic state and output;
native domain decoding additionally uses its existing hard bounds. Cancellation
returns `GF_CANCELLED`. Cursors bind request, pinned endpoints and deterministic
semantic rows, including dependency availability. A changed live endpoint or
released referenced payload returns `GF_PAGE_SNAPSHOT_GONE`; unrelated current
publications do not invalidate an otherwise unchanged exact-Version comparison.

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

### Native upstream request contract

`preview_research_upstream`, `update_research_branch`, and
`research_upstream_history` use the closed
[`research-upstream-api-v1.json`](../../../tests/contracts/research-upstream-api-v1.json)
contract. The preview scope is existing Branch research, Sources, ontology, or
1–256 explicit native fields. Its Arrow metadata binds the owning CURRENT,
original base, local head, immediate upstream and preview commitment. Required
fields and evidence acknowledgements are visible before publication. Explicit
field scope may be widened and previewed again to include those dependencies.

An update supplies that commitment, expected CURRENT, a fresh Version and stable
operation identity. `all_compatible` selects only reviewed nonconflicting changes
with adoptable prerequisites. `selected` records each field's explicit resolution.
Keep-local advances its reviewed upstream baseline while preserving the local
value. Explain also creates the caller-supplied immutable claim, including its
native graph references, in the same Branch publication. Retain-both applies only
to existing compatible list properties: it concatenates the local and upstream
sequences without losing their order or repetitions, then uses native property
and ontology validation. It never casts scalar conflicts into lists.

Adopting an ontology dependency uses native composition publication. Retained
graph files are reauthenticated for the new composition and published with its
bindings; old Versions retain their original files. Read-only field inspection
keeps runtime name observations local to the read.

Source preference comparison commits the effective preferred Artifact UUID,
not the preference event UUID. Its append-only history retains every referenced
Artifact and provenance dependency. A preference resolution does not rerun a
historical OCR/extraction or change its input references. Native preference
publication orders a new choice after prior events even when an imported event
has a later clock timestamp. Clearing a preference has no native event
representation and is refused explicitly rather than deleting history. Importing
a new OCR Artifact does not require adopting its Source's preferred choice:
that choice remains separately reviewable, including in nested Branches.

Imported fields preserve their upstream origin and contribution identity. The
complete Project capture used to authenticate an upstream citation is only a
publication validation witness; permanent review metadata does not copy the
unselected parent Artifact inventory or keep its payloads alive.

Review records and operation receipts live in the owning Project's research
registry, outside restorable Branch content. Cleanup may release obsolete
Version payloads; it cannot erase retained review decisions or turn exact retry
into a second application. The registry has its existing finite capacity and
refuses new operations when full. `research@5` rejects earlier incompatible
registries rather than guessing missing incorporated preference history.

The three transports are Python methods of the same snake-case names, Node
`previewResearchUpstream` / `updateResearchBranch` / `researchUpstreamHistory`,
and `gf research upstream preview|update|history --file request.json`. Preview
and history return native Arrow data; update returns its native receipt.

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

[ADR 0039](../../adr/0039-research-version-publication.md) defines authenticated
Version records, context heads, explicit dependency roots and permanent bounded
receipts under the same CURRENT. #1350's merged work supplies selected CAS
retention and the public Version facade. Proposal lifecycle proof adds actual
submission/review/root-release/compaction/reopen measurements; storage-root
fixtures alone do not establish those outcomes.

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

These APIs establish the Version foundation used by native Branch edits and
Proposal acceptance. Portable interchange is implemented by the native contract below.

### Native selective Proposal contract (#1356)

`submit_research_proposal` takes an authenticated frozen Slice from an exact
Branch Version and up to 256 explicit native field identities. The immediate
parent is derived from Branch genealogy. Its selected payload has a separate,
headless Version and a `FrozenProposal` retention root. Submission does not
advance the Branch. Node and edge properties are filtered individually;
immutable domain records require all their native fields explicitly selected.
Structural graph and ontology context is inspectable dependency evidence, not
implicit permission to accept another contribution.

`preview_research_proposal` returns Arrow rows containing frozen source identity,
item/contribution identities, baseline/proposed/destination commitments,
motivation, conflicts, required items, unavailable dependencies and evidence
gaps. A review supplies the exact generation and preview digest, one decision
per item, explicit use-proposed conflict resolutions, and exact acknowledgement
of external/unverifiable evidence context. It cannot accept an item while
rejecting or deferring its required unavailable dependency. A stale generation
requires a new preview. Preview does not modify authority.

`review_research_proposal` publishes selected parent changes, ordered immutable
review history, exact contribution mappings, accepted proof roots and its
receipt through one CURRENT. Integration records are distinct from optional
explicit canonical promotions. Parent authority determines promotion scope;
source canonical status grants no destination authority. The stable Branch
remains active. `research_proposal_history` returns bounded Arrow item,
ordered-review or accepted-mapping views; continuations bind the exact CURRENT.
A later Branch edit is visible as superseded proposal content.

Deduplication keys include tagged Project/Branch destination, native field,
contribution UUID and exact typed value or deletion. A new value may be accepted;
a new operation identity cannot reapply an already accepted value. Nested
Child→Branch acceptance preserves the contribution for its first Branch→Project
acceptance. Live Branch comparisons use the latest accepted mapping for the
current destination, authenticated by retained selected proof. Exact historical
Version comparisons do not inherit later decisions automatically; explicit
accepted mappings remain available for that purpose.

`release_research_proposal` releases an obsolete frozen payload root. Explicit
Version deletion and compaction/cleanup can then reclaim its selected bytes.
Unreleased deferred or rejected content remains rooted until the caller releases
it. Accepted selected proof has an independent `AcceptedProvenance` root and
retains required evidence even when original source and proposal payload Versions
are deleted. Original source/destination Version citations remain immutable
identity commitments. Releasing a payload ends its preview/review availability;
exact operation replay and history inspection remain supported.

The `research@5` registry bounds all history to 8 MiB, 1,024 retained Versions,
4,096 receipts/identity commitments, 4,096 roots, and 16,384 accepted mappings.
It refuses capacity overflow rather than expiring receipts or deduplication
history. Receipt replay and duplicate prevention are separate permanent bounded
obligations. Restore never rewinds either. Preview dependency working data is
bounded to 8 MiB and rendered output to 16 MiB; history uses pages of 1–1,000 rows.

Python exposes the same snake-case methods and native request dictionaries;
Node exposes their camel-case asynchronous equivalents with AbortSignal; CLI
uses `gf research proposal submit|preview|review|release|history --file request.json`.
Preview/history are Arrow; mutation results are control receipts. No binding
implements review policy. See [ADR 0043](../../adr/0043-atomic-research-proposal-acceptance.md)
for the publication and provenance boundaries.

Analytical results must have an explicit retained representation before submission:
selected graph properties or an owned Artifact containing result Arrow IPC with
its media type and recorded provenance. A transient query/algorithm result is not
implicitly archived or proposed. The result regression executes a query, persists
its exact Arrow output as `ArtifactKind::Other`, selects that Artifact and Source,
and verifies accepted bytes after reopen while excluding a private result. An
independently selected external reference requires explicit acknowledgement;
acknowledgement never invents an analytical derivation or a retained remote payload.

Requests reject unknown fields. Invalid selection, incomplete decisions, unresolved
conflicts, missing dependencies, and unacknowledged evidence fail before publication.
Stale generation or preview commitments require a fresh preview; changed requests
under an existing operation UUID return `GF_IDEMPOTENCY_CONFLICT`. Cancellation
returns `GF_CANCELLED`. Publication errors retain the native `committed:false` /
`committed:true` distinction around CURRENT replacement. After an ambiguous response,
reopen and retry the exact operation to recover its durable receipt. Native tests
exercise both sides of that boundary without rolling back committed parent content.

### Native research interchange contract (#1357)

`research_reference` resolves a live Branch head once or an exact immutable
Version, including its identity commitment, original base/origin, and derivative
authorship. Hosting URLs remain an application concern.

`export_research` packages one retained Version or a distinct selected projection.
The versioned research component authenticates required content and selected
acceptance proofs separately from historical ancestor identities. It preserves
field baselines and exact accepted-contribution mappings without copying active
heads, proposal receipts, or unrelated ancestor data. Native selected-field
freezing physically materializes effective property rows so deletion overlays
cannot leave private values in exported Parquet. Expanded and bundled forms
have equal semantic identity. Re-export preserves immutable proof identities
only after checking the exact selected native content closure.

Existing `verify_portable_v2` and `import_portable_v2` validate archived native
domains before destination admission. Imported genealogy supports citation and
explicit Version comparison, but is not local Branch or acceptance authority.
Unavailable ancestor payloads stay unavailable after reopen and cleanup.
`fork_research` adds explicit independent Project identity, workspace metadata,
governance, and ontology adoption. Its durable exact-intent receipt supports
replay without resetting later destination changes or requiring source payloads.

Portable errors after CURRENT replacement carry `committed_import` with the
operation, generation, manifest, and package identities. This reports a committed
outcome, not rollback; an absent receipt alone does not prove rollback. See
[ADR 0044](../../adr/0044-research-interchange-authority.md) and
[portable publication guarantees](portable-project-v2.md).
