# Source module decomposition (#1016)

The canonical close gate is [#1016](https://github.com/CurateLabs/graphforge/issues/1016).
Extract cohesive domains inside the settled crate boundaries. Keep production
behavior, public exports, resource ownership, and current-format semantics intact.
Each coherent batch has a native child issue, independent review, and a green PR.
The maintainer requested larger PRs: complete source modules or related module
families within a batch, retaining private domain boundaries and the three-change
work-in-progress limit.

## Completion policy

The approved default is 3,000 physical lines for every tracked file beneath
`crates/*/src`, including comments, blank lines, and embedded tests. Larger files
require an accepted ADR with an exact path, finite bound, and cohesion rationale.
The planned policy ADR grants 3,500-line limits only to storage's `adjacency.rs`
and `project_generation.rs`; those exemptions are not yet recorded as accepted ADRs.
Strict CI enforcement lands after the remaining extractions. No temporary blanket
exemptions or unchanged-tree release certification are required.

## Landed and active ownership

- Relational aggregates, scalar conversion, value comparison, temporal UDFs,
  list execution, and map/property access have their own expression modules.
- `expr/temporal_lowering.rs` owns temporal constructors, projection, truncation,
  field extraction, and epoch lowering. `ExprLowerer` retains shared clock state.
- `expr/spatial_lowering.rs` owns point construction and distance lowering.
- Both lowering modules reuse existing scalar/temporal adapters. Dispatch and
  public lowering tests remain in the parent expression module.
- [#1299](https://github.com/CurateLabs/graphforge/pull/1299) merged the temporal
  and spatial extraction and closed #1296, with body-equivalence and validation evidence.
- `temporal/duration.rs` owns duration construction, parsing/rendering, arithmetic,
  and between operations. Explicit reexports preserve the temporal API paths.
  [#1297](https://github.com/CurateLabs/graphforge/issues/1297) records the extraction;
  the parent and new duration module both fit the default bound.
- [#1293](https://github.com/CurateLabs/graphforge/pull/1293) merged storage
  construction catalog extraction and closed #1292.
- [#1303](https://github.com/CurateLabs/graphforge/pull/1303) merged the API
  root decomposition in one batch: `query_execution`, `result_shaping`,
  `workspace_hydration`, `graph_publication`, and `runtime_ownership`. Direct tests
  follow those owners; public query integration tests remain at the facade layer.
  The root and every extracted source/test file fit the default bound. Public
  methods and the `RuntimeGuard` export retain their existing paths.
- `expr/list_values.rs` owns heterogeneous-list construction, tagged-value
  assembly, list concatenation, and element promotion. Its direct tests live in
  `expr/list_values/tests.rs`; both reuse the existing scalar and value codecs.
  [#1307](https://github.com/CurateLabs/graphforge/pull/1307) merged this
  extraction and closed #1300. Expression dispatch and graph-shape compatibility remain in the parent.
- The same #1300 batch completes the expression root: `expr/graph_values.rs`
  owns whole graph values, graph metadata, and neutral path hydration descriptors;
  `expr/scalar_execution.rs` owns scalar builtin dispatch and execution UDFs.
  Public hydration and row-marker inspection paths remain explicit reexports.
  Cross-domain lowering tests and shared fixtures live in `expr/tests.rs` under
  the unchanged `expr::tests` module path. Every new file and the root fit 3,000 lines.

- [#1309](https://github.com/CurateLabs/graphforge/pull/1309) merged #1301 portable
  storage ownership: `graph_projection/logical_fingerprint.rs` owns canonical
  logical encoding; `project_portable_v2/materialization.rs` and
  `semantic_validation.rs` own extraction and semantic checks; export's
  `planning.rs` and `transport.rs` own manifest planning and transport writing.
  The parents retain projection materialization, canonical transport authentication,
  export orchestration, final verification, leases, and platform admission.
  All moved tests retain their assertions; native workflow selectors stay in place.

- [#1316](https://github.com/CurateLabs/graphforge/pull/1316) merged #1306, extracting the four
  algorithm adapter roots together. Rank and clustering algorithms own individual
  modules and direct tests. Analysis families own matching/partition, coloring,
  paths/cycles/DAG, structural, and embedding adapters. Path flow/cut and Steiner
  adapters have separate owners. Existing kernels remain single-owned; parents
  retain registration, invocation controls, and shared projection helpers.

- [#1317](https://github.com/CurateLabs/graphforge/pull/1317) merged the six API
  domain roots together and closed #1308. Bulk `normalization` and `publication` keep shared
  request types and membership admission in the parent. Knowledge `assertions`,
  `supporting`, and `ledger` own operations and shared publication encoding.
- Repository `configuration` owns definition/configuration validation; `skills`
  owns bundle verification, installation, and recovery. Discovery, synchronization,
  receipts, and shared durable filesystem helpers remain in the parent.
- Checkpoint `view` owns the pinned read-only facade and `diff` owns logical-record
  comparison and Arrow rendering. Lifecycle, revert validation, and shared paging
  remain in the parent. The recovery gate selects `checkpoints::` so direct child
  tests and retained integration tests all execute.
- Multi-ontology `candidate` owns pure module/bridge transformations and
  `diagnostics` owns bounded diagnostic projection. Composite `property_routes`
  and `rebase` own routing and compatibility. Facade publication, locking,
  reconciliation, and cross-domain tests remain with their existing coordinators.
  Public paths remain explicit reexports; direct tests follow their domain owners.

- [#1318](https://github.com/CurateLabs/graphforge/pull/1318) merged the
  query-pipeline roots together and closed #1310. `binder/{patterns,writes,projection,expressions}`
  owns binding domains while the parent retains state, scopes and clause orchestration.
  `lowerer/{scans,traversal,nested_queries,writes}` owns relational construction while
  the parent retains shared read authority and plan orchestration.
- Executor `create_exec`, `write_exec`, `expand_exec`, `row_exec` and `session`
  own physical operators and session planning, with explicit public root reexports.
  The session evidence stream retains its original field/drop ordering. Mixed tests
  remain under the original root test module; direct tests follow each owner.
- `write_driver/{create_merge,mutation_phases}` owns statement phases. The parent
  retains context, frontier, ordered phase dispatch, final staging and cleanup.
  Existing test bodies and assertions move intact with their owning domains.

- [#1319](https://github.com/CurateLabs/graphforge/pull/1319) merged #1311, decomposing
  UUID identity and construction together. UUID `probing` owns authenticated snapshots
  and lookup; `construction` owns index encoding and merge cursors;
  `ordinal_artifacts` owns artifact writers and their publication guards;
  `topology_delta` owns topology preparation and commit; `ordinal_compaction`
  owns compaction; `rebuild` and `maintenance` own rebuilds and orphan cleanup.
  Shared format records, authentication primitives, snapshot fields, and hooks
  remain in the parent. `identity_codec` remains the format codec owner.
- Construction `intake` owns chunks and receipts; `shape` owns validation and
  surrogate assignment; `encoding_publication` owns encoding and publication
  coordination; `recovery` owns authenticated recovery and cleanup; `controls`
  owns bounded serialization; `io_evidence` owns counted I/O and resource evidence.
  Session state, lifecycle, lock ownership, and format records remain in the
  parent. Existing `catalog`, `shaping_merge`, `supersession`, and `diagnostics`
  owners stay in place, consuming the same shared implementations.
- Direct tests follow these owners. Shared fixtures and cross-domain recovery
  tests retain `uuid_membership::tests` and `graph_construction::tests`, including
  construction's `compact_details` and `lifecycle_budget` modules. Their
  subprocess selectors and native-platform workflow selections remain unchanged.

- [#1320](https://github.com/CurateLabs/graphforge/pull/1320) merged #1312, completing
  property storage in one batch. Writer `replay_topology`, `replay_properties`,
  `property_codec`, and `property_mutation` own stream inputs, encoding, and
  authenticated staging. `GraphWriter` retains batching, topology commits and
  shared work accounting; replay cleanup guards stay with their resources.
- Property-overlay owners separate inventory admission, projected and targeted
  reads, Parquet budgets, and newest-snapshot merging. Shared formats, metrics,
  limits and resource ownership remain explicit. The source-contract gate follows
  declared production modules and verifies root exports and moved evidence tests.
- Catalog `filtered_parquet`, `property_readers`, and `providers` own pruning,
  property decoding and DataFusion adapters. Registration and graph authority
  remain in the parent; shared observer and test fixtures have one owner.
- Semantic `migration/planning` and `migration/materialization` own retained-data
  planning and private candidate construction. `legacy_routes` owns installation
  and rollback guards. Binding identity, validation, schemas and inventory
  authentication remain in the parent. Public paths use explicit reexports.
- Existing direct assertions follow each owner; cross-domain tests retain their
  original layer. Writer subprocess targets remain under `writer::tests`.
  The current staging conflict test is
  `writer::property_mutation::tests::staged_property_mutation_conflicts_after_intervening_project_publication`;
  historical execution evidence keeps its recorded source revision and selector.

- [#1327](https://github.com/CurateLabs/graphforge/pull/1327) merged #1313, extracting
  storage lifecycle and filesystem ownership together. Object-store `manifest_tree`,
  `materialization`, `installation`, and `gc` own their operations and direct tests;
  the parent retains read/publication leases and lifecycle locks.
- Publication `participants` owns validation/staging and `control` owns bounded
  journal serialization and atomic-file publication. Commit sequencing, `CURRENT`
  replacement, reconciliation, and mutation locks remain in the parent.
- Checkpoint `registry` owns authenticated pair commit/recovery and `restoration`
  owns revert participant construction. Lifecycle methods, shared authority,
  retention/read locks, and subprocess test helpers remain in the parent.
- Filesystem `cache_io`, `platform`, `windows`, and `windows_cas` own cache-release
  I/O and platform implementations. Capability types and public root exports
  retain their interfaces and field/drop order. Windows backend admission and
  unsafe-code boundaries are unchanged. Direct tests follow their owners.

- [#1314](https://github.com/CurateLabs/graphforge/issues/1314) extracts knowledge
  ledgers and both native language surfaces together. Knowledge `algorithm_run`
  and `confidence` own records, validation and direct tests; shared schema/reader
  authority and registry remain in the root.
- Python and Node each have private `conversions`, `assertions`, `epistemic`,
  `recorded`, `analyst`, `ontology`, `providers`, `query`, `construction`, and
  `lifecycle` owners. Complete annotated methods, wrappers and async tasks move
  together. Facade identity, runtime ownership, constructors and registration stay
  in the roots; public paths and field/drop order remain unchanged.
- Pinned PyO3 uses `multiple-pymethods`, with Cargo/Bazel dependency state updated
  together. Python stub, parity and GIL source checks follow declared production
  modules. This corrects the old scanner's omission of eight already-registered
  import-session members without changing the actual API or frozen Rust digests.
  The expanded GIL check also requires two existing certification-report inputs
  to become owned strings before the unchanged detached native call.

## Remaining domain sequence

Follow live child dependencies and finish queued work before starting more.

| Area | Remaining completion work |
| --- | --- |
| Size policy | Accepted ADR with two bounded exemptions; strict checker in Repository Policy and `make pre-push-fast`; clean merged inventory |

## Measured source inventory

This snapshot records the working tree during #1314; recompute before canonical
closure. Files above the default bound remain pending unless an accepted exemption applies.

| Source | Physical lines |
| --- | ---: |
| `crates/graphforge-storage/src/adjacency.rs` | 3,017 |
| `crates/graphforge-storage/src/project_generation.rs` | 3,014 |

## Evidence requirements

Compare moved production bodies, preserve all test assertions, and verify test
inventory after module renames. Update exact test selectors and source-inspection
checks with each move. Real facade/binding execution proves pipeline behavior;
logical plans alone do not. Storage claims require admitted-filesystem reopen,
fault, cancellation, authentication, and budget evidence. Preserve telemetry names,
error fields, lock/drop order, cache release, seeds, and deterministic reductions.

CI's required status remains `github-status/CI Gate` at the PR's exact head.
The final size checker belongs to Repository Policy and `make pre-push-fast`.
