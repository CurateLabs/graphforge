# Analyst research experience

**Status: Designed — M11 requirements for v0.6.0.** This page integrates the
maintainer's Core Analyst UX and Branch and Slice Semantics specifications.
It defines intended product behavior; it does not certify implementation.
The [architecture summary](ARCHITECTURE.md) distinguishes existing foundations
from the work required here.

M11 delivers Rust-owned capabilities through the public facade and thin Python,
Node, and CLI surfaces, plus a consumer UX contract. Associated projects such as
XYG and graphforge-nextjs, applications, and peer extensions own their interfaces,
rendering, hosting, authentication, and access enforcement. Core exposes
provenance, policy metadata, and reviewable state; recording an access policy
does not itself enforce access. These consumers are not required dependencies
of Core, and their application implementation is not an M11 Core close gate.

GraphForge is an analytical environment for exploring, structuring, deriving,
and publishing knowledge from heterogeneous evidence. Its primary journey is
**Explore → Focus → Branch → Analyze → Compare → Propose → Integrate**.
Progressive ontology development remains part of that journey, rather than
being the whole workflow.

From any meaningful analytical object, the analyst must be able to determine:
what it is; where it came from; which Version is being inspected; and what
uses, depends on, disagrees with, or derives from it.

## Research vocabulary

The vocabulary describes the complete system. An analyst does not need to
learn every concept before obtaining a useful result. Introduce a concept
when the current task needs it; assess understanding at that step rather than
requiring a vocabulary lesson during setup.

| Concept   | Required meaning                                                                                                                                                                                       |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Project   | A durable research universe containing or referencing evidence, knowledge, metadata, ontologies, research Branches, Versions, collaborators, and access policies. A Project is not one graph snapshot. |
| Source    | An original unit of evidence with stable identity: an edition, manuscript, EPUB, PDF, web resource, photograph, recording, or database export.                                                         |
| Artifact  | One representation or processing derivative of a Source, with its own identity and derivation history. Improving a representation does not replace Source identity.                                    |
| Slice     | A selection obtained through search, filtering, traversal, query, or direct selection. It focuses attention without creating independent research.                                                     |
| Branch    | Persistent, independently evolving research derived from a Project, Slice, or Branch, with an immutable starting context and navigable origin.                                                         |
| Version   | An immutable state of a Project or Branch, usable for inspection, comparison, citation, reproducibility, and continued research.                                                                       |
| Fork      | A new Project with independent identity, metadata, governance, access policy, branches, ontology decisions, and history; derivation from its origin remains provenance.                                |
| Assertion | A claim whose authorship, evidence, provenance, and status can be inspected.                                                                                                                           |
| Canonical | Currently accepted in a specified Project/community context; it does not mean objectively true.                                                                                                        |
| Lineage   | Derivation, processing, authorship, Version, acceptance, and dependency relationships.                                                                                                                 |
| Proposal  | Selected research offered upstream from one exact Branch Version. Later Branch edits cannot change the proposal.                                                                                       |

Use Branch, Version, Compare, Propose, Accept, Review upstream changes, Update
Branch, Reference, and Bring into Branch as analyst actions. Storage choices
and Git commands are not prerequisites for doing research.

## Audience and entry journeys

The core product audience is a **nontechnical analyst working with an agent**.
The analyst brings a question, source material and research judgment; the agent
helps operate the supported tools. Technical analysts and developers can also
work directly in notebooks, scripts and applications. v0.6.0 may initially
serve this technical path more completely while associated applications develop
the agent-assisted experience. That delivery stage does not redefine the core
audience or establish nontechnical usability by itself.

Expected first-use environments are VS Code with the GraphForge extension and
an agent, other agent-led workflows, and Jupyter notebooks, including Kaggle and
Google Colab where qualified. These are target journeys, not a claim that every
environment or M11 operation is already supported.

| Entry                           | First useful outcome                                                                                         | Required guidance and proof                                                                                                                                                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| VS Code extension with an agent | Open or construct a small project, ask a research question, inspect the result and its source context.       | Packaged extension/native/XYG versions work together; the agent uses published operations and the analyst can inspect the result independently of its explanation.                                                              |
| Agent-led setup and analysis    | Describe a task and obtain a supported query or analysis with an inspectable result and a clear next action. | Installed skills/tool schemas match the candidate; no repository checkout, invented API or undocumented setup intervention is required.                                                                                         |
| Jupyter notebook                | Run a short sequence of cells from installation to an understandable table or visualization.                 | A fresh kernel can reproduce exact results; simple result display and construction do not require handwritten UUID generation or native IPC knowledge. Qualify Kaggle/Colab separately and record any unsupported combinations. |

Start with a small deterministic fixture and an in-memory path where supported.
Explain when state is temporary, what a kernel/session reset loses, and the
supported way to retain or export work. Durable projects require the documented
filesystem admission; a hosted notebook is not assumed to support them. Do not
weaken admission or silently upload evidence to make setup appear successful.

The assisting agent may translate intent into operations, but Core owns graph
and research semantics. The analyst must be able to inspect selected scope,
evidence and proposed effects before a consequential research decision. An
agent's assertion that an operation succeeded is not a substitute for its real
result or receipt. Applications own interaction and access enforcement.

## Journey questions and user stories

Use the two-story/shared-character fixture throughout delivery. Ask the
following questions when the relevant action first appears; do not present
them all as an onboarding questionnaire. Answers must come from visible state
and evidence, in language the analyst understands, without requiring storage
or version-control implementation knowledge.

| Analyst story / moment                      | Question the experience must answer                                                                  | Observable answer                                                                                                       |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| I open a Project to investigate a question. | What am I looking at, what can I investigate, and what is my next useful action?                     | Project summary, sources, available entry actions, and the selected research context.                                   |
| I focus on one story.                       | Why is this object included, and what remains outside my selection?                                  | Inclusion rule/path, typed counts, boundary references and required dependencies shown separately.                      |
| I start independent research.               | What does my Branch contain, where did it come from, and will my edits change the parent?            | Frozen starting context, origin and inherited/local state; parent research remains unchanged by local edits.            |
| I inspect an apparent fact.                 | Is this source evidence, a machine extraction or an analyst claim, and what supports or disputes it? | Source/Artifact lineage, creator or processing run, evidence and competing assertions.                                  |
| I revisit or cite a result.                 | Which Version am I seeing, and can someone inspect the same evidence?                                | Live-versus-immutable reference, exact retained context and explicit external-evidence limitations.                     |
| I notice upstream changes.                  | What changed, does it affect my research, and what happens if I adopt it?                            | Relevant semantic comparison, dependency effects, valid choices and conflicts in a read-only preview.                   |
| I propose a contribution.                   | Exactly what am I sharing, what stays private/local, and can later edits change this proposal?       | Selected contributions and dependencies, omitted private content, and a frozen source Version.                          |
| I review a contribution.                    | What will acceptance change, what remains deferred or rejected, and does it make a claim canonical?  | Item-level outcomes and an explicit canonical decision separate from integration.                                       |
| I continue after acceptance.                | What was accepted, can I keep working, and will a retry apply it twice?                              | Durable receipt, accepted-contribution mapping, continued Branch state and supported replay outcome.                    |
| I save or share my work.                    | What survives closing/resetting this environment, and what will another reader receive?              | Temporary-versus-retained state, supported save/export path, complete-versus-selected identity and reader requirements. |

The [acceptance strategy](TESTING.md#analyst-journey-comprehension) separates
Core correctness, consumer presentation and observed human comprehension.
Each implementation owner supplies evidence for its questions as the capability
lands; final journey certification composes it. A misleading agent explanation
or a user misunderstanding becomes a concrete finding, not a vocabulary failure
inferred solely from the size of this specification.

## Project entry and discovery

Opening a Project must explain its contents, sources, scale, ontology context,
research Branches, descriptive metadata, and latest update. Graph visualization
is one entry point; readers must also be able to enter through text, sources,
entities, relationships, stories/documents, events, locations, ontology types,
linguistic properties, traversal, structured queries, and existing Branches.

Structured metadata supports title, description, authors/maintainers, subjects,
languages, geographic and temporal coverage, source types, corpus size,
ontologies and knowledge standards, license, visibility and access policy,
tags, originating and related Projects, canonical and external identifiers,
and creation/update dates. Communities can add fields without changing the core
schema. Discovery supports free text and structured filters without loading
the complete graph.

Representative discovery questions include literary corpora covering 800–1500
CE, Projects using a narrative-events ontology, Projects derived from a named
translation, and Projects containing manuscript sources.

## Explore and focus

The reference corpus is the complete _Book of the Thousand Nights and a Night_,
including ten primary volumes, supplemental volumes, scans, transcripts,
passages, stories, characters, locations, events, linguistic features,
extractions, annotations, hypotheses, external sources, and ontology context.
The same Project supports corpus-wide linguistic analysis and detailed research
on _The Tale of the Three Apples_ without disconnected copies.

Search for the story, inspect connected material, and expand characters,
events, passages, Sources, Artifacts, and assertions into a current Slice.
Always show what is selected, what lies outside, why each object is included,
and how the selection relates to the Project. Counts distinguish nodes,
relationships, passages, and Artifacts. The supplied example of 1,842 nodes,
4,291 relationships, 173 passages, and 12 Artifacts is illustrative, not a
benchmark target.

A shared character may be included while its other appearances remain boundary
references. “Why is this here?” explains the selection path or rule, even for
large traversal results. Expand/contract and preview the boundary before
creating a Branch. A Dynamic Slice reevaluates a selection against current
state; a Frozen Slice records exact selected content at one Version. Creating
a Branch freezes its starting context.

## Independent research

Create a Branch from a whole Project, a Slice, or another Branch. Version
history must also support restoration and branching from historical Versions.
Request lightweight context such as a name and description, not storage or
synchronization settings.

Branches evolve independently within their Project's durable container and
publication authority. A Fork, not an ordinary Branch, creates a separately
governed Project and independent storage authority.

Display immediate origin, base Version, initial Slice where applicable,
creator, creation time, current Version, and research summary. Preserve inherited
object identity. A Branch's local additions, modifications, suppressions,
replacements, reclassifications, and challenges do not silently change its
parent. Competing interpretations may coexist.

Distinguish inherited, added here, modified here, suppressed here, changed
upstream, proposed, accepted, rejected, local-only, and superseded research.
“Reference” lets the analyst inspect outside content without importing its
neighborhood. “Bring into Branch” expands active research and records the
origin Version of the added content. Initial Slice boundaries do not restrict
the Branch permanently.
For a Slice Branch, retain the selected content and required dependencies, not
the entire historical ancestor. Expansion outside that retained closure may
require separately retained parent history; explain when it is unavailable.

Branches form navigable research genealogy: immediate parent, ultimate Project,
creator, date, base/current Versions, accepted contributions, and child
Branches. Analysts can name significant Versions, describe research milestones,
cite and compare them, and continue from historical work without editing it.

## Evidence and disagreement

Keep original evidence, transformed representations, machine extraction,
analyst assertions, interpretations, and theories/hypotheses distinguishable.
These are conceptual Evidence, Knowledge, Research, and Lineage layers for
analysts; their relation to Rust ownership is documented in
[the architecture summary](ARCHITECTURE.md#problem-model-and-terminology).

Canonicality applies to assertions and relationships as well as entities and is
scoped to the accepting Project/community. Show who or what created an assertion,
its supporting evidence, originating Branch, canonical status, competing
assertions, and status history. Alternatives may support, contradict, refine,
supersede, or dispute one another. Confidence or automated extraction alone
does not establish canonicality.
Integrating an alternative and promoting it to canonical are separate recorded
decisions. They may occur in one reviewed operation, but integration alone does
not change existing canonical choices or transfer another context's authority.

“Show Lineage” traverses backward from an entity/assertion through extraction,
normalized passage, OCR, processed scan, original scan, and Source, and forward
from evidence to derived assertions, entities/relationships, analyst claims,
Branches, and theories. The [knowledge ledger](../book/architecture/knowledge-ledger.md)
owns the evidence and interpretation requirements.

Improving a Source adds a preferred Artifact while preserving old Artifacts,
processing runs, derived knowledge, and the date/reason preference changed.
Report affected downstream research. The analyst can inspect the new
representation, retain the old one, deliberately adopt an update, rerun
processing, and compare derivations.

## Compare, update, propose, integrate

Compare Branch with parent, Branch with Branch, current Branch with an accepted
Version, Version with Version, and Project Version with Project Version.
Describe semantic changes to entities, relationships, assertions, canonical
claims, annotations, ontology definitions, and Sources; allow inspection of
every change and its provenance.

Relevant upstream additions, modifications, source replacements, and ontology
changes must be discoverable without mutating Branch research. Preview selected
updates or all compatible updates before applying them. When parent and Branch
independently change an object, present the conflict and the alternatives:
retain local interpretation, adopt upstream, keep both where valid, or explain
their relationship with an assertion.
Keep the original base as origin history while advancing comparison baselines
only for deliberately incorporated objects/fields. A later upstream update must
not mislabel previously incorporated content as a local edit, or silently
resurrect locally suppressed content.

Proposals may select nodes, relationships, assertions, Sources, ontology
additions, annotations, or other analytical results. Review must explain what
changed, why, the supporting evidence, affected knowledge, conflicts, and
required ontology changes. Parent acceptance may be partial, with accepted,
rejected, and deferred items recorded separately.
Record exact accepted contributions across source Versions rather than treating
partial acceptance as acceptance of an entire Version. Parent changes and the
acceptance receipt publish together; continued research reads that one outcome.
If a response fails after commitment, report the committed result and support
safe replay instead of claiming the old state was restored.

Accepted knowledge identifies its originating Branch Version, analyst, and
evidence. Acceptance neither terminates the Branch nor forwards later edits.
Compare later research with previously accepted work, show divergence, and
allow a new proposal. See [research workspace semantics](../book/architecture/research-workspaces.md)
for the required state transitions.

## Ontology, sharing, and reproducibility

Projects declare independently versioned ontologies and knowledge standards.
Branches inherit exact ontology context and can extend it locally without
changing their parent. Proposing useful ontology extensions uses the same
evidence-aware review process as other research, subject to the
[multi-ontology contract](../book/architecture/composable-multi-ontology.md).

Sharing may identify a Branch's current state or one immutable Version. The
viewer must see origin, original base, current/selected Version, and derivative
authorship immediately. The supplied web addresses illustrate addressability;
they do not themselves establish a hosting implementation.

A historical Version must reconstruct the same graph state, source references,
Artifact versions, ontology context, assertions, and local changes. Discovering
new upstream information from that view never changes the view itself.

Retained Versions pin all required graph, ontology, comparison-baseline, and
locally stored Artifact content for their selected scope. Genealogy alone does
not pin the whole ancestor; explicitly retaining a whole Project Version does.
Bound both Branch creation cost and retained storage after parent evolution and
cleanup. Reproduction requires a compatible reader; it does not promise pre-v1
migration or permanent compatibility with the latest reader.
External-only evidence retains its identity/reference and integrity
information where available; unavailable or unverifiable bytes must be disclosed,
not substituted from a newer URL response. Deleting retained history is explicit
and cannot invalidate a Version still required by a Branch or Proposal.

The [consumer interaction contract](../book/architecture/research-workspaces.md#consumer-interaction-contract)
defines context, lineage, selection explanations, review outcomes, pagination,
and failure semantics before implementation. Associated applications render
these Core results; their UI work is not postponed into, or required by, Core's
final journey certification.

## Delivery evidence

[M11 tracker #1347](https://github.com/CurateLabs/graphforge/issues/1347) owns the
implementation breakdown and is a native blocker of release readiness #1096.
Its native sub-issues and blocked-by relationships define implementation order.
Documentation integration is tracked separately in
[#1346](https://github.com/CurateLabs/graphforge/issues/1346).

These requirements supersede the older product framing of a Project as only
a portable graph workspace and of exploration-to-formalization as the complete
analyst journey. Existing documented APIs remain implementation evidence only
for their actual behavior. A checkpoint, export selection, or assertion
supersession is not proof of a research Branch, Slice, or Proposal.

[Testing](TESTING.md#analyst-ux-acceptance) maps both supplied specifications to
implementation acceptance scenarios. M11 completion requires those outcomes;
documentation alone does not satisfy them.

### Core Proposal walkthrough evidence

The native `research_proposals::tests::two_stories` scenario starts two independent
story Branches with the same Character UUID. Mystery changes Ada's age and ending;
Voyage keeps a separate alternative. Native comparison shows Mystery's two local
fields. A Proposal freezes that Version, accepts age, and defers ending: the
Project age changes, its ending stays undecided, and Voyage's Version stays fixed.
Mystery then changes age again. Proposal history remains tied to the submitted
Version, comparison cites the prior accepted contribution, and the new exact value
can form a second Proposal. This bounded Rust facade example is prerequisite
proof for interchange; #1358 still owns the complete cross-surface consumer journey.

### Core research interchange evidence

Native interchange regressions cover complete and disjoint/redacted Version
round trips, explicit Fork governance and exact replay, and nested accepted
lineage through import and re-export. They inspect physical exported Parquet,
including content-addressed proof objects, to exclude private selected fields
and unrelated ancestor content. Imported provenance remains separate from live
heads and local acceptance. Full cross-surface journey certification remains
#1358's close gate.
