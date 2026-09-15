# Source module decomposition (#1016)

The canonical close gate is [#1016](https://github.com/CurateLabs/graphforge/issues/1016).
Extract cohesive domains inside the settled crate boundaries. Keep production
behavior, public exports, resource ownership, and current-format semantics intact.
Each extraction has a native child issue, independent review, and its own green PR.

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
- [#1296](https://github.com/CurateLabs/graphforge/issues/1296) tracks the temporal
  and spatial extraction; its PR records body-equivalence and validation evidence.
- [#1292](https://github.com/CurateLabs/graphforge/issues/1292) tracks storage
  construction catalog extraction. Check live issue state before further work.

## Remaining domain sequence

Follow live child dependencies and finish queued work before starting more.

| Area | Remaining ownership extractions |
| --- | --- |
| Relational expressions | Heterogeneous lists; graph/path values; scalar UDFs and builtin dispatch |
| Relational plan lowering | Scans/property joins; traversal; nested/optional queries; writes |
| Temporal values | Duration parsing, arithmetic, between operations, and direct tests |
| IR binding | Patterns/paths; writes; projection/aggregation; expression/property binding |
| API root | Query/result shaping; hydration/read authority; graph publication; runtime |
| API domains | Bulk normalization/publication; knowledge ledgers; repository definitions/skills; checkpoint views/diffs; ontology candidates; composite property routing/rebase |
| Executor | CREATE; DELETE/SET/REMOVE; traversal/ordinal identity; optional/UNWIND; session/planner |
| Write driver | CREATE/MERGE; property/label/delete phases |
| Algorithms | Rank families; analysis families/embeddings; clustering families; flow/cut/Steiner adapters |
| UUID membership | Probing/snapshots; construction encoding; ordinal writers/publication; topology deltas; compaction; rebuild/maintenance |
| Construction | Intake/receipts; shape validation/surrogates; encoding/publication; recovery/cleanup; bounded control; I/O evidence |
| Property storage | Replay topology/properties; property codecs/staging; authenticated inventories; projected reads/budgets; newest-snapshot merges |
| Catalog/bindings/projection | Filtered readers; property readers; table providers; semantic migration; canonical fingerprint encoding |
| Storage lifecycle | CAS manifest/materialization/install/GC; publication staging/control; checkpoint registry/revert; portable planning/transport/materialization/validation |
| Filesystem | Cache release; platform capabilities; Windows CAS sealing |
| Knowledge core | Algorithm-run and confidence ledgers |
| Bindings | Conversions, domain methods/tasks/types, lifecycle/construction; preserve registration and signatures |

## Measured source inventory

This snapshot records the working tree during #1296; recompute before canonical
closure. A listed file is still pending unless an accepted exemption says otherwise.

| Source | Physical lines |
| --- | ---: |
| `crates/graphforge-api/src/bulk_construction.rs` | 4,646 |
| `crates/graphforge-api/src/checkpoints.rs` | 3,807 |
| `crates/graphforge-api/src/composite_publish.rs` | 3,188 |
| `crates/graphforge-api/src/knowledge.rs` | 5,073 |
| `crates/graphforge-api/src/lib.rs` | 5,740 |
| `crates/graphforge-api/src/multi_ontology.rs` | 3,226 |
| `crates/graphforge-api/src/repository.rs` | 4,457 |
| `crates/graphforge-bindings-node/src/lib.rs` | 8,122 |
| `crates/graphforge-bindings-py/src/lib.rs` | 7,070 |
| `crates/graphforge-exec/src/algorithm_analyze.rs` | 7,161 |
| `crates/graphforge-exec/src/algorithm_cluster.rs` | 4,457 |
| `crates/graphforge-exec/src/algorithm_paths.rs` | 3,402 |
| `crates/graphforge-exec/src/algorithm_rank.rs` | 10,408 |
| `crates/graphforge-exec/src/lib.rs` | 8,533 |
| `crates/graphforge-exec/src/write_driver.rs` | 4,861 |
| `crates/graphforge-filesystem/src/lib.rs` | 5,488 |
| `crates/graphforge-ir/src/binder.rs` | 9,579 |
| `crates/graphforge-knowledge/src/lib.rs` | 3,442 |
| `crates/graphforge-rel/src/expr.rs` | 8,340 |
| `crates/graphforge-rel/src/lowerer.rs` | 7,217 |
| `crates/graphforge-rel/src/temporal.rs` | 3,005 |
| `crates/graphforge-storage/src/adjacency.rs` | 3,017 |
| `crates/graphforge-storage/src/catalog.rs` | 5,467 |
| `crates/graphforge-storage/src/graph_construction.rs` | 13,084 |
| `crates/graphforge-storage/src/graph_object_store.rs` | 6,304 |
| `crates/graphforge-storage/src/graph_projection.rs` | 3,078 |
| `crates/graphforge-storage/src/project_checkpoints.rs` | 4,091 |
| `crates/graphforge-storage/src/project_generation.rs` | 3,014 |
| `crates/graphforge-storage/src/project_portable_v2.rs` | 3,483 |
| `crates/graphforge-storage/src/project_portable_v2_export.rs` | 4,434 |
| `crates/graphforge-storage/src/project_publication.rs` | 4,805 |
| `crates/graphforge-storage/src/property_overlay.rs` | 6,677 |
| `crates/graphforge-storage/src/semantic_bindings.rs` | 4,043 |
| `crates/graphforge-storage/src/uuid_membership.rs` | 13,650 |
| `crates/graphforge-storage/src/writer.rs` | 10,432 |

## Evidence requirements

Compare moved production bodies, preserve all test assertions, and verify test
inventory after module renames. Update exact test selectors and source-inspection
checks with each move. Real facade/binding execution proves pipeline behavior;
logical plans alone do not. Storage claims require admitted-filesystem reopen,
fault, cancellation, authentication, and budget evidence. Preserve telemetry names,
error fields, lock/drop order, cache release, seeds, and deterministic reductions.

CI's required status remains `github-status/CI Gate` at the PR's exact head.
The final size checker belongs to Repository Policy and `make pre-push-fast`.
