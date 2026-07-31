# Command map

Every command below is a stable ID, invokable from the Command Palette or programmatically via
`vscode.commands.executeCommand("<id>", ...)`. Commands marked **Node-only** require the
`@graphforge/node` runtime — they aren't backed by the Python bridge yet (see
[`install.md`](install.md)).

## Setup

| Command | ID | What it does |
|---|---|---|
| GraphForge: Check Environment | `graphforge.checkEnvironment` | Reports both runtimes' status, which one is active, and the single next step. Accepts optional `{ silent: true }`. |
| GraphForge: Setup Native Binding | `graphforge.setupNativeBinding` | QuickPick to link/browse/install `@graphforge/node`. |
| GraphForge: Setup Python Binding | `graphforge.setupPythonBinding` | QuickPick to select a Python interpreter or install `graphforge` via `uv`. |
| GraphForge: Initialize Project Here | `graphforge.initializeProjectHere` | Creates a new GraphForge project in an empty/uninitialized folder. |
| GraphForge: Open Project | `graphforge.openProject` | Opens an existing `FORMAT`-marked project; accepts an optional path string arg. |
| GraphForge: Refresh Explorer | `graphforge.refreshExplorer` | Refreshes the Projects Activity Bar view. |
| GraphForge: Get Started | `graphforge.getStarted` | Opens the Get Started sidebar (Welcome mode picker on first use, then the runtime → project → query checklist). |
| GraphForge: Choose Experience Mode… | `graphforge.chooseExperienceMode` | Reopens the Welcome mode picker (Guided/Autonomous) inside Get Started. |
| GraphForge: Settings | `graphforge.openSettings` | Opens the GraphForge Settings panel — left-nav categories (Runtime / Experience / Advanced) over the same `graphforge.*` settings as the VS Code Settings UI. |

## Cypher

| Command | ID | What it does |
|---|---|---|
| GraphForge: Run Query | `graphforge.runQuery` | Runs Cypher from the editor selection, the whole document, or an input box; accepts `{ cypher?, params? }` to skip prompts. |
| GraphForge: Run Query with Parameters… | `graphforge.runQueryWithParams` | Same as Run Query, plus a JSON parameters prompt (or pass `{ cypher, params }` directly). |

## Analyst verbs

Each verb has a plain command (walks a QuickPick chain: label, then algorithm) and an
`…Advanced…` variant (adds via/directed/write-property prompts where applicable).

| Verb | Plain | Advanced |
|---|---|---|
| Rank | `graphforge.rank` | `graphforge.rankAdvanced` |
| Cluster | `graphforge.cluster` | `graphforge.clusterAdvanced` |
| Paths | `graphforge.paths` | `graphforge.pathsAdvanced` |
| Analyze | `graphforge.analyze` | `graphforge.analyzeAdvanced` |
| Similar | `graphforge.similar` | `graphforge.similarAdvanced` |
| Find | `graphforge.find` | — |

## Indexing **(Node-only)**

| Command | ID |
|---|---|
| GraphForge: Index Text… | `graphforge.indexText` |
| GraphForge: Index Vector… | `graphforge.indexVector` |
| GraphForge: Inspect Text Index… | `graphforge.inspectTextIndex` |
| GraphForge: Index Adjacency | `graphforge.indexAdjacency` |
| GraphForge: Inspect Adjacency Index | `graphforge.inspectAdjacency` |
| GraphForge: Rebuild Adjacency Index | `graphforge.rebuildAdjacency` |

## Checkpoints **(Node-only, ADR 0014)**

| Command | ID |
|---|---|
| GraphForge: Create Checkpoint… | `graphforge.createCheckpoint` |
| GraphForge: List Checkpoints | `graphforge.listCheckpoints` |
| GraphForge: Open Checkpoint… | `graphforge.openCheckpoint` |
| GraphForge: Diff Checkpoints… | `graphforge.diffCheckpoints` |
| GraphForge: Delete Checkpoint… | `graphforge.deleteCheckpoint` |
| GraphForge: Revert to Checkpoint… | `graphforge.revertToCheckpoint` |

## Embedding spaces **(Node-only)**

| Command | ID |
|---|---|
| GraphForge: Embedding Spaces | `graphforge.embeddingSpaces` |
| GraphForge: Publish Caller Embeddings… | `graphforge.publishCallerEmbeddings` |
| GraphForge: Bind Embedding Space Alias… | `graphforge.bindEmbeddingSpaceAlias` |
| GraphForge: Set Default Embedding Space… | `graphforge.setDefaultEmbeddingSpace` |
| GraphForge: Delete Embedding Space… | `graphforge.deleteEmbeddingSpace` |
| GraphForge: Inspect Embedding Space Freshness… | `graphforge.inspectEmbeddingSpaceFreshness` |

## Write mode & transactions **(Node-only, ADR 0015)**

| Command | ID |
|---|---|
| GraphForge: Enable Capability… | `graphforge.enableCapability` |
| GraphForge: Open with Write Mode… | `graphforge.openWithWriteMode` |
| GraphForge: Export Invocation Descriptor… | `graphforge.exportInvocationDescriptor` |
| GraphForge: List Algorithm Runs | `graphforge.listAlgorithmRuns` |
| GraphForge: Publish Composite Transaction… (Advanced) | `graphforge.publishCompositeTransaction` |

## Ontology

| Command | ID |
|---|---|
| GraphForge: Show Ontology Viewer | `graphforge.showOntology` |
| GraphForge: Load Ontology… | `graphforge.loadOntology` |
| GraphForge: Open ontology.json | `graphforge.openOntologyFile` |
| GraphForge: Explain Ontology Mode | `graphforge.explainOntologyMode` |

## Knowledge ledger

| Command | ID |
|---|---|
| GraphForge: List Assertions | `graphforge.listAssertions` |
| GraphForge: Create Assertion… | `graphforge.createAssertion` |
| GraphForge: Show Assertion… | `graphforge.showAssertion` |
| GraphForge: Show Assertion on Graph… | `graphforge.showAssertionOnGraph` |
| GraphForge: Attach Evidence… (Advanced) | `graphforge.attachEvidence` |
| GraphForge: Assess Confidence… (Advanced) | `graphforge.assessConfidence` |
| GraphForge: Record Assertion Status… (Advanced) | `graphforge.recordAssertionStatus` |

Identity UUIDs for assertions/evidence/confidence/status events are minted client-side as
UUIDv7 (engine-enforced). Knowledge-ledger writes require the Node runtime.

## Result views

| Command | ID |
|---|---|
| GraphForge: Show Result Graph | `graphforge.showResultGraph` |
| GraphForge: Result Graph (Advanced)… | `graphforge.showResultGraphAdvanced` |
| GraphForge: Show Project Capabilities | `graphforge.showCapabilities` |

See [`agent-interop.md`](agent-interop.md) for which of these accept structured arguments and
return structured results for programmatic (agent) callers, and the source-of-truth test
(`src/test/extension.test.ts`) that asserts every command above is registered.
