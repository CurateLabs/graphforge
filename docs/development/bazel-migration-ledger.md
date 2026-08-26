# Bazel migration ledger (freeze)

Checked-in inventory for Bazel-migration issue [#12](https://github.com/CurateLabs/graphforge/issues/12) / canonical [#1](https://github.com/CurateLabs/graphforge/issues/1) step 1.

Orchestration contract: [bazel-migration-orchestration.md](bazel-migration-orchestration.md).
Performance baseline: [bazel-migration-baseline.md](bazel-migration-baseline.md).

## Freeze metadata

| Field | Value |
| --- | --- |
| Freeze date (UTC) | 2026-08-06 |
| Inventory SHA | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| Authoritative source | `cargo metadata --format-version=1 --no-deps` |
| Workspace packages | 18 |
| Cargo metadata targets | **101** |
| Bazel modeling claimed complete? | **Yes** — all 101 Cargo targets mapped (#10–#6 + #338 + #336 + #752 + #753 + #779); retained tools justified in exceptions |
| Machine-readable map | `tools/bazel/parity/migration_target_map.json` (fail-closed via `scripts/ci/bazel-migration-ledger-check.py`) |
| Bootstrap note | See [bazel-bootstrap.md](bazel-bootstrap.md); parity evidence [bazel-migration-parity.md](bazel-migration-parity.md) |

Issue #1 historically cited ~71 Cargo targets / ~53 integration-test binaries.
This freeze uses the **current authoritative** metadata count (**101** targets;
**66** integration-test binaries). Later slices must update rows,
not silently ignore new targets.

## Target class summary

| Class | Count | Notes |
| --- | ---: | --- |
| `lib` | 16 | First-party libraries (unit/doctest surface rides these targets under `cargo test --lib` / doctests) |
| `integration-test` | 66 | `tests/*.rs` integration binaries |
| `cdylib` | 2 | PyO3 + napi-rs native libs |
| `bin` | 1 | CLI (`gf`) |
| `custom-build` | 2 | `build.rs` scripts |
| `example` | 11 | API examples mapped as `//crates/graphforge-api:<name>` (#6) |
| **Total** | **101** | |

### Unit tests and doctests

Cargo metadata does **not** emit separate targets for `#[cfg(test)]` unit modules or
doctests. For migration accounting and CI:

- **Unit tests** → owned with the package `lib` (or `bin`) row; prove under Bazel via
  `gf_rust_test` / `rust_test(crate = ...)` (covers `#[cfg(test)]` modules only).
- **Doctests** → **not** covered by `rust_test(crate = ...)`. Crates with runnable
  rustdoc examples must declare an explicit `gf_rust_doc_test` /
  `rust_doc_test` target and include it in `//:ci_rust_tests` (via
  `//:first_party_lib_tests`). Today: `//crates/graphforge-ir:graphforge_ir_doc_test`
  and `//crates/graphforge-ontology:graphforge_ontology_doc_test`.

## Cargo target ledger

Columns `bazel_label` and `status` are filled by modeling slices (#10–#6).
Authoritative machine-readable map: `tools/bazel/parity/migration_target_map.json`.

| Package | Target | Class | Source | Bazel label | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `graphforge-api` | `graphforge_api` | `lib` | `crates/graphforge-api/src/lib.rs` | `//crates/graphforge-api:graphforge_api` | `mapped` | #9; unit tests `//crates/graphforge-api:graphforge_api_test` |
| `graphforge-api` | `atomic_recovery_workflow` | `example` | `crates/graphforge-api/examples/atomic_recovery_workflow.rs` | `//crates/graphforge-api:atomic_recovery_workflow` | `mapped` | #6 |
| `graphforge-api` | `correction_churn_workflow` | `example` | `crates/graphforge-api/examples/correction_churn_workflow.rs` | `//crates/graphforge-api:correction_churn_workflow` | `mapped` | #6 |
| `graphforge-api` | `cyber_intrusion_workflow` | `example` | `crates/graphforge-api/examples/cyber_intrusion_workflow.rs` | `//crates/graphforge-api:cyber_intrusion_workflow` | `mapped` | #6 |
| `graphforge-api` | `derived_state_freshness_workflow` | `example` | `crates/graphforge-api/examples/derived_state_freshness_workflow.rs` | `//crates/graphforge-api:derived_state_freshness_workflow` | `mapped` | #6 |
| `graphforge-api` | `finance_fraud_workflow` | `example` | `crates/graphforge-api/examples/finance_fraud_workflow.rs` | `//crates/graphforge-api:finance_fraud_workflow` | `mapped` | #6 |
| `graphforge-api` | `knowledge_evolution_workflow` | `example` | `crates/graphforge-api/examples/knowledge_evolution_workflow.rs` | `//crates/graphforge-api:knowledge_evolution_workflow` | `mapped` | #6 |
| `graphforge-api` | `ontology_emergence_strict_handoff` | `example` | `crates/graphforge-api/examples/ontology_emergence_strict_handoff.rs` | `//crates/graphforge-api:ontology_emergence_strict_handoff` | `mapped` | #6 |
| `graphforge-api` | `probate_genealogy_workflow` | `example` | `crates/graphforge-api/examples/probate_genealogy_workflow.rs` | `//crates/graphforge-api:probate_genealogy_workflow` | `mapped` | #6 |
| `graphforge-api` | `release_load_probe` | `example` | `crates/graphforge-api/examples/release_load_probe.rs` | `//crates/graphforge-api:release_load_probe` | `mapped` | #6; release certification probe |
| `graphforge-api` | `sna_intelligence_workflow` | `example` | `crates/graphforge-api/examples/sna_intelligence_workflow.rs` | `//crates/graphforge-api:sna_intelligence_workflow` | `mapped` | #6 |
| `graphforge-api` | `strict_add_node_fixture` | `example` | `crates/graphforge-api/examples/strict_add_node_fixture.rs` | `//crates/graphforge-api:strict_add_node_fixture` | `mapped` | #6 |
| `graphforge-api` | `bdd` | `integration-test` | `crates/graphforge-api/tests/bdd/main.rs` | `//crates/graphforge-api:bdd` | `mapped` | #8 |
| `graphforge-api` | `bdd_timing` | `integration-test` | `crates/graphforge-api/tests/bdd_timing.rs` | `//crates/graphforge-api:bdd_timing` | `mapped` | #8 |
| `graphforge-api` | `belief_subject_contract` | `integration-test` | `crates/graphforge-api/tests/belief_subject_contract.rs` | `//crates/graphforge-api:belief_subject_contract` | `mapped` | #8 |
| `graphforge-api` | `bind_error_spans` | `integration-test` | `crates/graphforge-api/tests/bind_error_spans.rs` | `//crates/graphforge-api:bind_error_spans` | `mapped` | #8 |
| `graphforge-api` | `clear` | `integration-test` | `crates/graphforge-api/tests/clear.rs` | `//crates/graphforge-api:clear` | `mapped` | #8 |
| `graphforge-api` | `conductance` | `integration-test` | `crates/graphforge-api/tests/conductance.rs` | `//crates/graphforge-api:conductance` | `mapped` | #8 |
| `graphforge-api` | `create_scaling` | `integration-test` | `crates/graphforge-api/tests/create_scaling.rs` | `//crates/graphforge-api:create_scaling` | `mapped` | #8 |
| `graphforge-api` | `e2e_baseline` | `integration-test` | `crates/graphforge-api/tests/e2e_baseline.rs` | `//crates/graphforge-api:e2e_baseline` | `mapped` | #8 |
| `graphforge-api` | `existential_subquery` | `integration-test` | `crates/graphforge-api/tests/existential_subquery.rs` | `//crates/graphforge-api:existential_subquery` | `mapped` | #8 |
| `graphforge-api` | `facade_methods` | `integration-test` | `crates/graphforge-api/tests/facade_methods.rs` | `//crates/graphforge-api:facade_methods` | `mapped` | #8 |
| `graphforge-api` | `fixed_hop_limit` | `integration-test` | `crates/graphforge-api/tests/fixed_hop_limit.rs` | `//crates/graphforge-api:fixed_hop_limit` | `mapped` | #8 |
| `graphforge-api` | `graph_internal_metadata` | `integration-test` | `crates/graphforge-api/tests/graph_internal_metadata.rs` | `//crates/graphforge-api:graph_internal_metadata` | `mapped` | #8 |
| `graphforge-api` | `inference_provenance` | `integration-test` | `crates/graphforge-api/tests/inference_provenance.rs` | `//crates/graphforge-api:inference_provenance` | `mapped` | #8 |
| `graphforge-api` | `knowledge_isolation` | `integration-test` | `crates/graphforge-api/tests/knowledge_isolation.rs` | `//crates/graphforge-api:knowledge_isolation` | `mapped` | #8 |
| `graphforge-api` | `list_semantics` | `integration-test` | `crates/graphforge-api/tests/list_semantics.rs` | `//crates/graphforge-api:list_semantics` | `mapped` | #8 |
| `graphforge-api` | `multi_ontology_certification` | `integration-test` | `crates/graphforge-api/tests/multi_ontology_certification.rs` | `//crates/graphforge-api:multi_ontology_certification` | `mapped` | #843 retained multi-ontology migration certification |
| `graphforge-api` | `algorithm_public_surface` | `integration-test` | `crates/graphforge-api/tests/algorithm_public_surface.rs` | `//crates/graphforge-api:algorithm_public_surface` | `mapped` | #8 |
| `graphforge-api` | `search_public_surface` | `integration-test` | `crates/graphforge-api/tests/search_public_surface.rs` | `//crates/graphforge-api:search_public_surface` | `mapped` | #8 |
| `graphforge-api` | `scale_g500_scale20` | `integration-test` | `crates/graphforge-api/tests/scale_g500_scale20.rs` | `//crates/graphforge-api:scale_g500_scale20` | `mapped` | #710 |
| `graphforge-api` | `scale_g500_ladder` | `integration-test` | `crates/graphforge-api/tests/scale_g500_ladder.rs` | `//crates/graphforge-api:scale_g500_ladder` | `mapped` | #736 |
| `graphforge-api` | `provider_public_surface` | `integration-test` | `crates/graphforge-api/tests/provider_public_surface.rs` | `//crates/graphforge-api:provider_public_surface` | `mapped` | #8 |
| `graphforge-api` | `m4_entry_baseline` | `integration-test` | `crates/graphforge-api/tests/m4_entry_baseline.rs` | `//crates/graphforge-api:m4_entry_baseline` | `mapped` | #8 |
| `graphforge-api` | `file_backed_graph_generation` | `integration-test` | `crates/graphforge-api/tests/file_backed_graph_generation.rs` | `//crates/graphforge-api:file_backed_graph_generation` | `mapped` | #338 |
| `graphforge-api` | `adjacency_scale_evidence` | `integration-test` | `crates/graphforge-api/tests/adjacency_scale_evidence.rs` | `//crates/graphforge-api:adjacency_scale_evidence` | `mapped` | #336 |
| `graphforge-api` | `file_backed_scale_evidence` | `integration-test` | `crates/graphforge-api/tests/file_backed_scale_evidence.rs` | `//crates/graphforge-api:file_backed_scale_evidence` | `mapped` | #338 densified |
| `graphforge-api` | `max_bipartite_matching` | `integration-test` | `crates/graphforge-api/tests/max_bipartite_matching.rs` | `//crates/graphforge-api:max_bipartite_matching` | `mapped` | #8 |
| `graphforge-api` | `max_cardinality_matching` | `integration-test` | `crates/graphforge-api/tests/max_cardinality_matching.rs` | `//crates/graphforge-api:max_cardinality_matching` | `mapped` | #8 |
| `graphforge-api` | `max_weight_matching` | `integration-test` | `crates/graphforge-api/tests/max_weight_matching.rs` | `//crates/graphforge-api:max_weight_matching` | `mapped` | #8 |
| `graphforge-api` | `minimum_k_spanning_tree` | `integration-test` | `crates/graphforge-api/tests/minimum_k_spanning_tree.rs` | `//crates/graphforge-api:minimum_k_spanning_tree` | `mapped` | #8 |
| `graphforge-api` | `modularity` | `integration-test` | `crates/graphforge-api/tests/modularity.rs` | `//crates/graphforge-api:modularity` | `mapped` | #8 |
| `graphforge-api` | `multi_label_scaling` | `integration-test` | `crates/graphforge-api/tests/multi_label_scaling.rs` | `//crates/graphforge-api:multi_label_scaling` | `mapped` | #8 |
| `graphforge-api` | `pattern_comprehension` | `integration-test` | `crates/graphforge-api/tests/pattern_comprehension.rs` | `//crates/graphforge-api:pattern_comprehension` | `mapped` | #8 |
| `graphforge-api` | `percentile_aggregates` | `integration-test` | `crates/graphforge-api/tests/percentile_aggregates.rs` | `//crates/graphforge-api:percentile_aggregates` | `mapped` | #8 |
| `graphforge-api` | `provider_session` | `integration-test` | `crates/graphforge-api/tests/provider_session.rs` | `//crates/graphforge-api:provider_session` | `mapped` | #8 |
| `graphforge-api` | `public_facade_remaining_conformance` | `integration-test` | `crates/graphforge-api/tests/public_facade_remaining_conformance.rs` | `//crates/graphforge-api:public_facade_remaining_conformance` | `mapped` | #8 |
| `graphforge-api` | `public_lifecycle_conformance` | `integration-test` | `crates/graphforge-api/tests/public_lifecycle_conformance.rs` | `//crates/graphforge-api:public_lifecycle_conformance` | `mapped` | #8 |
| `graphforge-api` | `release_load_construction` | `integration-test` | `crates/graphforge-api/tests/release_load_construction.rs` | `//crates/graphforge-api:release_load_construction` | `mapped` | #8 |
| `graphforge-api` | `strict_runtime_properties` | `integration-test` | `crates/graphforge-api/tests/strict_runtime_properties.rs` | `//crates/graphforge-api:strict_runtime_properties` | `mapped` | #8 |
| `graphforge-api` | `varlen_empty_seed` | `integration-test` | `crates/graphforge-api/tests/varlen_empty_seed.rs` | `//crates/graphforge-api:varlen_empty_seed` | `mapped` | #8 |
| `graphforge-api` | `value_access_semantics` | `integration-test` | `crates/graphforge-api/tests/value_access_semantics.rs` | `//crates/graphforge-api:value_access_semantics` | `mapped` | #8 |
| `graphforge-api` | `value_semantics` | `integration-test` | `crates/graphforge-api/tests/value_semantics.rs` | `//crates/graphforge-api:value_semantics` | `mapped` | #8 |
| `graphforge-api` | `with_aggregation` | `integration-test` | `crates/graphforge-api/tests/with_aggregation.rs` | `//crates/graphforge-api:with_aggregation` | `mapped` | #8 |
| `graphforge-api` | `xor_scaling` | `integration-test` | `crates/graphforge-api/tests/xor_scaling.rs` | `//crates/graphforge-api:xor_scaling` | `mapped` | #8 |
| `graphforge-ast` | `graphforge_ast` | `lib` | `crates/graphforge-ast/src/lib.rs` | `//crates/graphforge-ast:graphforge_ast` | `mapped` | #10; unit tests `//crates/graphforge-ast:graphforge_ast_test` |
| `graphforge-bindings-node` | `graphforge_bindings_node` | `cdylib` | `crates/graphforge-bindings-node/src/lib.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node` | `mapped` | #7; packaging `//:node_package_smoke` |
| `graphforge-bindings-node` | `build-script-build` | `custom-build` | `crates/graphforge-bindings-node/build.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node_build_script` | `mapped` | #7; `napi-build` via `gf_cargo_build_script` |
| `graphforge-bindings-py` | `graphforge_bindings_py` | `cdylib` | `crates/graphforge-bindings-py/src/lib.rs` | `//crates/graphforge-bindings-py:graphforge_bindings_py` | `mapped` | #7; packaging `//:python_wheel_smoke` |
| `graphforge-cli` | `graphforge_cli` | `lib` | `crates/graphforge-cli/src/lib.rs` | `//crates/graphforge-cli:graphforge_cli` | `mapped` | #7/#8; unit `//crates/graphforge-cli:graphforge_cli_test` |
| `graphforge-cli` | `gf` | `bin` | `crates/graphforge-cli/src/main.rs` | `//crates/graphforge-cli:gf` | `mapped` | #8 |
| `graphforge-cli` | `filesystem_admission` | `integration-test` | `crates/graphforge-cli/tests/filesystem_admission.rs` | `//crates/graphforge-cli:filesystem_admission` | `mapped` | #780 |
| `graphforge-cli` | `checkpoints` | `integration-test` | `crates/graphforge-cli/tests/checkpoints.rs` | `//crates/graphforge-cli:checkpoints` | `mapped` | #8 |
| `graphforge-cli` | `portable` | `integration-test` | `crates/graphforge-cli/tests/portable.rs` | `//crates/graphforge-cli:portable` | `mapped` | #8 |
| `graphforge-cli` | `repository` | `integration-test` | `crates/graphforge-cli/tests/repository.rs` | `//crates/graphforge-cli:repository` | `mapped` | #8 |
| `graphforge-cli` | `build-script-build` | `custom-build` | `crates/graphforge-cli/build.rs` | `//crates/graphforge-cli:graphforge_cli_build_script` | `mapped` | #7/#8; RT-cli-build-script closed |
| `graphforge-core` | `graphforge_core` | `lib` | `crates/graphforge-core/src/lib.rs` | `//crates/graphforge-core:graphforge_core` | `mapped` | #10; unit tests `//crates/graphforge-core:graphforge_core_test` |
| `graphforge-core` | `canonical` | `bench` | `crates/graphforge-core/benches/canonical.rs` | — | `exception` | RT-codspeed-bench; CodSpeed divan benchmark |
| `graphforge-cypher` | `graphforge_cypher` | `lib` | `crates/graphforge-cypher/src/lib.rs` | `//crates/graphforge-cypher:graphforge_cypher` | `mapped` | #10; unit tests `//crates/graphforge-cypher:graphforge_cypher_test` |
| `graphforge-cypher` | `corpus` | `integration-test` | `crates/graphforge-cypher/tests/corpus.rs` | `//crates/graphforge-cypher:corpus` | `mapped` | #8 |
| `graphforge-cypher` | `compile` | `bench` | `crates/graphforge-cypher/benches/compile.rs` | — | `exception` | RT-codspeed-bench; CodSpeed divan benchmark |
| `graphforge-discovery` | `graphforge_discovery` | `lib` | `crates/graphforge-discovery/src/lib.rs` | `//crates/graphforge-discovery:graphforge_discovery` | `mapped` | #908; unit tests `//crates/graphforge-discovery:graphforge_discovery_test` |
| `graphforge-discovery` | `contract_artifacts` | `integration-test` | `crates/graphforge-discovery/tests/contract_artifacts.rs` | `//crates/graphforge-discovery:contract_artifacts` | `mapped` | #910; deterministic schema and conformance fixture parity |
| `graphforge-exec` | `graphforge_exec` | `lib` | `crates/graphforge-exec/src/lib.rs` | `//crates/graphforge-exec:graphforge_exec` | `mapped` | #9; unit tests `//crates/graphforge-exec:graphforge_exec_test` |
| `graphforge-exec` | `adjacency_expand` | `integration-test` | `crates/graphforge-exec/tests/adjacency_expand.rs` | `//crates/graphforge-exec:adjacency_expand` | `mapped` | #8 |
| `graphforge-exec` | `bench_traversal_scaling` | `integration-test` | `crates/graphforge-exec/tests/bench_traversal_scaling.rs` | `//crates/graphforge-exec:bench_traversal_scaling` | `mapped` | #8 |
| `graphforge-exec` | `create_execution` | `integration-test` | `crates/graphforge-exec/tests/create_execution.rs` | `//crates/graphforge-exec:create_execution` | `mapped` | #8 |
| `graphforge-exec` | `create_input_driven` | `integration-test` | `crates/graphforge-exec/tests/create_input_driven.rs` | `//crates/graphforge-exec:create_input_driven` | `mapped` | #8 |
| `graphforge-exec` | `differential_traversal` | `integration-test` | `crates/graphforge-exec/tests/differential_traversal.rs` | `//crates/graphforge-exec:differential_traversal` | `mapped` | #8 |
| `graphforge-exec` | `explain_snapshots` | `integration-test` | `crates/graphforge-exec/tests/explain_snapshots.rs` | `//crates/graphforge-exec:explain_snapshots` | `mapped` | #8 |
| `graphforge-exec` | `optional_match` | `integration-test` | `crates/graphforge-exec/tests/optional_match.rs` | `//crates/graphforge-exec:optional_match` | `mapped` | #8 |
| `graphforge-exec` | `persistent_adjacency` | `integration-test` | `crates/graphforge-exec/tests/persistent_adjacency.rs` | `//crates/graphforge-exec:persistent_adjacency` | `mapped` | #8 |
| `graphforge-exec` | `read_execution` | `integration-test` | `crates/graphforge-exec/tests/read_execution.rs` | `//crates/graphforge-exec:read_execution` | `mapped` | #8 |
| `graphforge-exec` | `unwind` | `integration-test` | `crates/graphforge-exec/tests/unwind.rs` | `//crates/graphforge-exec:unwind` | `mapped` | #8 |
| `graphforge-exec` | `var_len_expand` | `integration-test` | `crates/graphforge-exec/tests/var_len_expand.rs` | `//crates/graphforge-exec:var_len_expand` | `mapped` | #8 |
| `graphforge-exec` | `write_statement` | `integration-test` | `crates/graphforge-exec/tests/write_statement.rs` | `//crates/graphforge-exec:write_statement` | `mapped` | #8 |
| `graphforge-io` | `graphforge_io` | `lib` | `crates/graphforge-io/src/lib.rs` | `//crates/graphforge-io:graphforge_io` | `mapped` | #9; unit tests `//crates/graphforge-io:graphforge_io_test` |
| `graphforge-ir` | `graphforge_ir` | `lib` | `crates/graphforge-ir/src/lib.rs` | `//crates/graphforge-ir:graphforge_ir` | `mapped` | #10; unit tests `//crates/graphforge-ir:graphforge_ir_test` |
| `graphforge-ir` | `golden` | `integration-test` | `crates/graphforge-ir/tests/golden.rs` | `//crates/graphforge-ir:golden` | `mapped` | #8 |
| `graphforge-knowledge` | `graphforge_knowledge` | `lib` | `crates/graphforge-knowledge/src/lib.rs` | `//crates/graphforge-knowledge:graphforge_knowledge` | `mapped` | #9; unit tests `//crates/graphforge-knowledge:graphforge_knowledge_test` |
| `graphforge-observability` | `graphforge_observability` | `lib` | `crates/graphforge-observability/src/lib.rs` | `//crates/graphforge-observability:graphforge_observability` | `mapped` | #886; unit tests `//crates/graphforge-observability:graphforge_observability_test` |
| `graphforge-observability` | `disabled_allocations` | `integration-test` | `crates/graphforge-observability/tests/disabled_allocations.rs` | `//crates/graphforge-observability:disabled_allocations` | `mapped` | #886; disabled hot-path allocation proof |
| `graphforge-ontology` | `graphforge_ontology` | `lib` | `crates/graphforge-ontology/src/lib.rs` | `//crates/graphforge-ontology:graphforge_ontology` | `mapped` | #10; unit tests `//crates/graphforge-ontology:graphforge_ontology_test` |
| `graphforge-ontology` | `integration` | `integration-test` | `crates/graphforge-ontology/tests/integration.rs` | `//crates/graphforge-ontology:integration` | `mapped` | #8 |
| `graphforge-ontology` | `composition_inventory` | `integration-test` | `crates/graphforge-ontology/tests/composition_inventory.rs` | `//crates/graphforge-ontology:composition_inventory` | `mapped` | #836 |
| `graphforge-ontology` | `bridge_sets` | `integration-test` | `crates/graphforge-ontology/tests/bridge_sets.rs` | `//crates/graphforge-ontology:bridge_sets` | `mapped` | #838 |
| `graphforge-ontology` | `inventory_crud` | `integration-test` | `crates/graphforge-ontology/tests/inventory_crud.rs` | `//crates/graphforge-ontology:inventory_crud` | `mapped` | #837 |
| `graphforge-plan` | `graphforge_plan` | `lib` | `crates/graphforge-plan/src/lib.rs` | `//crates/graphforge-plan:graphforge_plan` | `mapped` | #10; unit tests `//crates/graphforge-plan:graphforge_plan_test` |
| `graphforge-provenance` | `graphforge_provenance` | `lib` | `crates/graphforge-provenance/src/lib.rs` | `//crates/graphforge-provenance:graphforge_provenance` | `mapped` | #10; unit tests `//crates/graphforge-provenance:graphforge_provenance_test` |
| `graphforge-rel` | `graphforge_rel` | `lib` | `crates/graphforge-rel/src/lib.rs` | `//crates/graphforge-rel:graphforge_rel` | `mapped` | #10; unit tests `//crates/graphforge-rel:graphforge_rel_test` |
| `graphforge-rel` | `expression_lowering_matrix` | `integration-test` | `crates/graphforge-rel/tests/expression_lowering_matrix.rs` | `//crates/graphforge-rel:expression_lowering_matrix` | `mapped` | #8 |
| `graphforge-rel` | `logical_plan_golden` | `integration-test` | `crates/graphforge-rel/tests/logical_plan_golden.rs` | `//crates/graphforge-rel:logical_plan_golden` | `mapped` | #8 |
| `graphforge-search` | `graphforge_search` | `lib` | `crates/graphforge-search/src/lib.rs` | `//crates/graphforge-search:graphforge_search` | `mapped` | #9; unit tests `//crates/graphforge-search:graphforge_search_test` |
| `graphforge-storage` | `graphforge_storage` | `lib` | `crates/graphforge-storage/src/lib.rs` | `//crates/graphforge-storage:graphforge_storage` | `mapped` | #10 early / #9; Bazel enables `test-failpoints` for api subprocess unification; unit tests `//crates/graphforge-storage:graphforge_storage_test` |
| `graphforge-storage` | `m6_storage` | `bench` | `crates/graphforge-storage/benches/m6_storage.rs` | — | `exception` | RT-codspeed-bench; #782 diagnostic simulation/hardware evidence |
| `graphforge-storage` | `m6_storage_io` | `bench` | `crates/graphforge-storage/benches/m6_storage_io.rs` | — | `exception` | RT-codspeed-bench; #782 stable-runner walltime evidence |
| `graphforge-storage` | `adjacency_delta_write` | `integration-test` | `crates/graphforge-storage/tests/adjacency_delta_write.rs` | `//crates/graphforge-storage:adjacency_delta_write` | `mapped` | #8 |
| `graphforge-storage` | `discovery_portable_v2` | `integration-test` | `crates/graphforge-storage/tests/discovery_portable_v2.rs` | `//crates/graphforge-storage:discovery_portable_v2` | `mapped` | #909; discovery reference to storage verifier boundary |
| `graphforge-storage` | `filtered_read` | `integration-test` | `crates/graphforge-storage/tests/filtered_read.rs` | `//crates/graphforge-storage:filtered_read` | `mapped` | #8 |
| `graphforge-storage` | `graph_delta_journal` | `integration-test` | `crates/graphforge-storage/tests/graph_delta_journal.rs` | `//crates/graphforge-storage:graph_delta_journal` | `mapped` | #8 / #752 |
| `graphforge-storage` | `graph_delta_compaction` | `integration-test` | `crates/graphforge-storage/tests/graph_delta_compaction.rs` | `//crates/graphforge-storage:graph_delta_compaction` | `mapped` | #8 / #753 |
| `graphforge-storage` | `graph_writer` | `integration-test` | `crates/graphforge-storage/tests/graph_writer.rs` | `//crates/graphforge-storage:graph_writer` | `mapped` | #8 |
| `graphforge-storage` | `io_stats` | `integration-test` | `crates/graphforge-storage/tests/io_stats.rs` | `//crates/graphforge-storage:io_stats` | `mapped` | #8 |
| `graphforge-storage` | `property_overlay_scale` | `integration-test` | `crates/graphforge-storage/tests/property_overlay_scale.rs` | `//crates/graphforge-storage:property_overlay_scale` | `mapped` | #940; bounded property-overlay scale qualification |

## Retained-tool exceptions

Justified retained Cargo/ecosystem tools after #6. Stub status is forbidden; the
ledger check fails closed on `stub` or missing justification.

| ID | Tool / surface | Why Bazel may not replace cleanly | Owning follow-up | Status |
| --- | --- | --- | --- | --- |
| RT-fuzz | `cargo fuzz` (`fuzz/` workspace, workflow `fuzz.yml`) | cargo-fuzz driver + corpus workflow outside ordinary `rules_rust` test graph | keep Cargo | justified |
| RT-publish-crates | `cargo publish` / crates.io authorize flows | Ecosystem publication metadata and registry auth | keep Cargo | justified |
| RT-maturin-assemble | `maturin build` / `maturin sdist` packaging assembly | Bazel handoff: `//:python_wheel_smoke` + `assemble_bazel_binding_packages.py` consume Bazel cdylibs (no silent `maturin build` recompile). Maturin may still sign/publish later. | #7 handoff | handoff |
| RT-napi-assemble | `napi build` / `napi artifacts` / `napi pre-publish` | Bazel handoff: `//:node_package_smoke` consumes Bazel cdylib (no silent `napi build` recompile). napi may still assemble/sign/publish later. | #7 handoff | handoff |
| RT-cli-build-script | `graphforge-cli` lib (`build.rs` → embedded `project-skills`) | Mapped via `cargo_build_script` + `//:project_skills_bundle`; bin/tests mapped | #8 complete | closed |
| RT-bindings-cdylib | `graphforge-bindings-py` / `graphforge-bindings-node` packages | Mapped as `rust_shared_library` cdylibs + packaging smoke targets | #7 | mapped |
| RT-examples | `graphforge-api` examples (11) | All 11 example binaries mapped under `//crates/graphforge-api:*` | #6 | closed |
| RT-codspeed-bench | `cargo codspeed` / divan benches (`crates/*/benches/*.rs`, workflow `codspeed.yml`) | Benchmarks are a Cargo diagnostics surface measured by CodSpeed, not a correctness signal compiled or tested by `//:ci_rust_tests` | keep Cargo | justified |
| RT-mobile | Swift / Kotlin / UniFFI / XCFramework / JVM AAR | **Abandoned for Bazel migration** — not a deliverable; do not inventory as required targets | excluded | excluded |

## Cross-platform release platforms (#6)

Checked-in model: `tools/bazel/release/release_platforms.json` + `//platforms:*`.
Must cover every Binding RC target in
`tests/contracts/binding-release-candidate-targets.json` and every
`package.json` `napi.targets` triple (including `aarch64-unknown-linux-gnu`
cross-target). Host-native release bins aggregate: `//:release_bins`.

## CI / release build command sites

Frozen scan of `.github/workflows/`, `scripts/`, and `Makefile` for `cargo`,
`maturin`, and `napi` build/test command invocations: **120** sites across
**27** files. Representative required path is `CI Gate` via
`.github/workflows/test.yml` on Blacksmith runners.

### Sticky Cargo `target/` disks (#4 cutover)

After [#4](https://github.com/CurateLabs/graphforge/issues/4), Test Suite
(`.github/workflows/test.yml`) no longer mounts PR job-isolated Cargo sticky
disks. Retained sticky workflows (packaging / retained tools):
- `.github/workflows/binding-release-candidate.yml`
- `.github/workflows/release-certification.yml`
- `.github/workflows/fuzz.yml`

Retired PR sticky key pattern (do not reintroduce without rollback docs):
`${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1` → `target/`.

### Sites by file

| File | Sites | Role |
| --- | ---: | --- |
| `.github/workflows/binding-release-candidate.yml` | 11 | Binding RC wheels/addons |
| `.github/workflows/checkpoint-recovery-gate.yml` | 6 | Checkpoint recovery gate |
| `.github/workflows/concurrency-stress-gate.yml` | 1 | Concurrency stress |
| `.github/workflows/fuzz.yml` | 6 | cargo-fuzz (retained-tool candidate) |
| `.github/workflows/release-certification.yml` | 4 | Release load certification |
| `.github/workflows/non-cypher-surface-gate.yml` | 4 | Non-Cypher surface gate |
| `.github/workflows/test.yml` | 6 | Required CI Gate (Bazel authority + Cargo lint/bindings; #4 cutover) |
| `.github/workflows/visualization-limits-stress.yml` | 1 | Visualization stress |
| `Makefile` | 17 | Developer/CI mirrors |
| `scripts/ci/clean-env-verify.py` | 1 | Build/test/package command site |
| `scripts/ci/crate-publish-plan.py` | 4 | Build/test/package command site |
| `scripts/ci/release-certification.py` | 2 | Build/test/package command site |
| `scripts/ci/test-binding-release-candidate.py` | 19 | Build/test/package command site |
| `scripts/ci/test-checkpoint-recovery-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-ci-storage-policy.py` | 3 | Build/test/package command site |
| `scripts/ci/test-crate-publish-plan.py` | 2 | Build/test/package command site |
| `scripts/ci/test-release-certification.py` | 10 | Build/test/package command site |
| `scripts/ci/test-knowledge-contract-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-epistemic-contract-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-pre-push-validation.py` | 1 | Build/test/package command site |
| `scripts/ci/test-publish-track.py` | 3 | Build/test/package command site |
| `scripts/ci/test-release-publish-preflight.py` | 1 | Build/test/package command site |
| `scripts/coverage-rust.sh` | 2 | Coverage builds (maturin/napi + cargo) |
| `scripts/pre_push_validation.py` | 1 | Build/test/package command site |
| `scripts/publish_crates.py` | 4 | Build/test/package command site |
| `scripts/publish_dry_run.py` | 5 | Build/test/package command site |
| `scripts/verify_package_licenses.py` | 3 | Build/test/package command site |

<details>
<summary>Full command-site listing (path:line)</summary>

| Path | Line | Snippet |
| --- | ---: | --- |
| `.github/workflows/non-cypher-surface-gate.yml` | 41 | `cargo test -p graphforge-api --lib --no-fail-fast` |
| `.github/workflows/non-cypher-surface-gate.yml` | 42 | `cargo test -p graphforge-api \` |
| `.github/workflows/non-cypher-surface-gate.yml` | 135 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `.github/workflows/non-cypher-surface-gate.yml` | 136 | `cargo test -p graphforge-api --test knowledge_isolation --test public_lifecycle_conformance --test public_facade_remaining_conformance --test algorithm_public_surface --test search_public_surface --test provider_public_surface --test provider_session --no-fail-fast` |
| `.github/workflows/binding-release-candidate.yml` | 93 | `uses: PyO3/maturin-action@v1` |
| `.github/workflows/binding-release-candidate.yml` | 102 | `- name: Reclaim sticky-disk ownership after maturin` |
| `.github/workflows/binding-release-candidate.yml` | 324 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release` |
| `.github/workflows/binding-release-candidate.yml` | 365 | `pnpm exec napi create-npm-dirs` |
| `.github/workflows/binding-release-candidate.yml` | 366 | `pnpm exec napi artifacts --output-dir artifacts --npm-dir npm` |
| `.github/workflows/binding-release-candidate.yml` | 543 | `uv run maturin sdist` |
| `.github/workflows/binding-release-candidate.yml` | 558 | `pnpm exec napi build --platform --release --target x86_64-unknown-linux-gnu` |
| `.github/workflows/binding-release-candidate.yml` | 561 | `pnpm exec napi create-npm-dirs` |
| `.github/workflows/binding-release-candidate.yml` | 562 | `pnpm exec napi artifacts --output-dir artifacts --npm-dir npm` |
| `.github/workflows/binding-release-candidate.yml` | 565 | `pnpm exec napi pre-publish -t npm --skip-optional-publish --no-gh-release` |
| `.github/workflows/binding-release-candidate.yml` | 612 | `cargo package "${package_args[@]}" --allow-dirty --no-verify` |
| `.github/workflows/test.yml` | 437 | `run: cargo fmt --all -- --check` |
| `.github/workflows/test.yml` | 440 | `run: cargo clippy --workspace -- -D warnings` |
| `.github/workflows/test.yml` | 477 | `run: cargo test --workspace --no-fail-fast` |
| `.github/workflows/test.yml` | 529 | `uvx maturin build` |
| `.github/workflows/test.yml` | 682 | `run: pnpm --filter @curatelabs/graphforge exec napi build --platform` |
| `.github/workflows/test.yml` | 851 | `cargo test -p graphforge-storage project_generation::tests:: --lib` |
| `.github/workflows/release-certification.yml` | 137 | `uses: PyO3/maturin-action@v1` |
| `.github/workflows/release-certification.yml` | 145 | `- name: Reclaim sticky-disk ownership after maturin` |
| `.github/workflows/release-certification.yml` | 166 | `cargo build --release -p graphforge-api --example release_load_probe` |
| `.github/workflows/release-certification.yml` | 171 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release \` |
| `.github/workflows/visualization-limits-stress.yml` | 59 | `uvx maturin build \` |
| `.github/workflows/fuzz.yml` | 60 | `cargo fmt --check` |
| `.github/workflows/fuzz.yml` | 61 | `cargo clippy --all-targets -- -D warnings` |
| `.github/workflows/fuzz.yml` | 69 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_parse corpus/fuzz_parse seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 73 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_bind corpus/fuzz_bind seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 77 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_ontology corpus/fuzz_ontology seeds/ontology -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 81 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_exec corpus/fuzz_exec seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/concurrency-stress-gate.yml` | 54 | `uvx maturin build \` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 29 | `cargo test -p graphforge-storage --lib project_checkpoints::tests --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 30 | `cargo test -p graphforge-api --lib checkpoints::tests --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 31 | `cargo test -p graphforge-cli --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 32 | `cargo build -p graphforge-cli` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 84 | `uv run --with maturin maturin build --manifest-path crates/graphforge-bindings-py/Cargo.toml --out dist` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 127 | `pnpm --filter @curatelabs/graphforge exec napi build --platform` |
| `scripts/coverage-rust.sh` | 139 | `uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml` |
| `scripts/coverage-rust.sh` | 140 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release` |
| `scripts/publish_crates.py` | 11 | `for every ``cargo publish`` attempt.` |
| `scripts/publish_crates.py` | 13 | `The token is normalized before ``cargo publish``: leading/trailing whitespace` |
| `scripts/publish_crates.py` | 207 | `"""Run ``cargo publish`` for one crate, sleeping through bounded 429 waits.` |
| `scripts/publish_crates.py` | 314 | `raise RuntimeError(f"cargo package did not create {archive}")` |
| `scripts/verify_package_licenses.py` | 6 | `- Cargo: ``cargo package --list`` includes LICENSE and NOTICE` |
| `scripts/verify_package_licenses.py` | 8 | `- Python: maturin/pyproject ``license-files`` exist and declare Apache-2.0` |
| `scripts/verify_package_licenses.py` | 88 | `errors.append(f"cargo package -p {name} --list failed: {detail}")` |
| `scripts/pre_push_validation.py` | 355 | `(("uv", "run", "maturin", "--version"), "run: uv sync --all-extras"),` |
| `scripts/publish_dry_run.py` | 5 | `- cargo-package: ``cargo package --list --no-verify`` per crates.io plan order` |
| `scripts/publish_dry_run.py` | 6 | `- cargo-publish: ``cargo publish --dry-run`` (heavy; optional)` |
| `scripts/publish_dry_run.py` | 9 | `- python: ``maturin sdist`` (local packaging; TestPyPI upload is separate/manual)` |
| `scripts/publish_dry_run.py` | 206 | `"maturin",` |
| `scripts/publish_dry_run.py` | 227 | `help="When surface=all, skip heavy cargo publish --dry-run",` |
| `scripts/ci/test-checkpoint-recovery-gate.py` | 63 | `skipped["command_groups"]["rust-storage"] = "cargo test -- --ignored"` |
| `scripts/ci/release-certification.py` | 176 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/release-certification.py` | 177 | `"cargo test -p graphforge-api --test knowledge_isolation "` |
| `scripts/ci/test-crate-publish-plan.py` | 81 | `assert commands[0].startswith("cargo publish -p graphforge-core ")` |
| `scripts/ci/test-crate-publish-plan.py` | 82 | `assert commands[-1].startswith("cargo publish -p graphforge-cli ")` |
| `scripts/ci/test-release-certification.py` | 112 | `self.assertNotIn("cargo build", validation_job)` |
| `scripts/ci/test-release-certification.py` | 113 | `self.assertNotIn("maturin-action", validation_job)` |
| `scripts/ci/test-release-certification.py` | 129 | `self.assertIn("Reclaim sticky-disk ownership after maturin", load_job)` |
| `scripts/ci/test-release-certification.py` | 138 | `reclaim_step = load_job.index("- name: Reclaim sticky-disk ownership after maturin")` |
| `scripts/ci/test-release-certification.py` | 147 | `self.assertNotIn("cargo build", load_job[wrapper_step:artifact_step])` |
| `scripts/ci/test-release-certification.py` | 151 | `rust_build = artifact_build.index("cargo build")` |
| `scripts/ci/test-release-certification.py` | 152 | `node_build = artifact_build.index("napi build")` |
| `scripts/ci/test-release-certification.py` | 219 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/test-release-certification.py` | 220 | `"cargo test -p graphforge-api --test knowledge_isolation --test "` |
| `scripts/ci/test-release-certification.py` | 320 | `bad_rust_commands["commands"][0] = "cargo test --workspace"` |
| `scripts/ci/test-pre-push-validation.py` | 324 | `self.assertFalse(any(command[0] == "uv" and "maturin" in command for command in commands))` |
| `scripts/ci/clean-env-verify.py` | 540 | `result.commands.append("cargo check")` |
| `scripts/ci/test-epistemic-contract-gate.py` | 60 | `forbidden["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
| `scripts/ci/test-ci-storage-policy.py` | 14 | `cache without GitHub-backed maturin sccache).` |
| `scripts/ci/test-ci-storage-policy.py` | 373 | `for step in action_steps(text, "PyO3/maturin-action@"):` |
| `scripts/ci/test-ci-storage-policy.py` | 374 | `assert field(step, "uses") == "PyO3/maturin-action@v1", "unapproved Maturin action"` |
| `scripts/ci/test-release-publish-preflight.py` | 168 | `for forbidden in ("npm publish", "uv publish", "cargo publish", "release:\n"):` |
| `scripts/ci/crate-publish-plan.py` | 100 | `"""Return crate → path deps that lack version= (blocks cargo publish)."""` |
| `scripts/ci/crate-publish-plan.py` | 130 | `f"{name}: path dependencies missing version= for cargo publish: " + ", ".join(deps)` |
| `scripts/ci/crate-publish-plan.py` | 152 | `print(f"cargo publish -p {name} --dry-run --locked")` |
| `scripts/ci/crate-publish-plan.py` | 167 | `help="Print cargo publish --dry-run commands when unblocked",` |
| `scripts/ci/test-publish-track.py` | 81 | `"cargo publish",` |
| `scripts/ci/test-publish-track.py` | 92 | `assert "PyO3/maturin-action" not in publish` |
| `scripts/ci/test-publish-track.py` | 93 | `assert "napi build" not in publish` |
| `scripts/ci/test-knowledge-contract-gate.py` | 51 | `forbidden_command["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
| `scripts/ci/test-binding-release-candidate.py` | 27 | `ARTIFACT_COMMAND = "pnpm exec napi artifacts --output-dir artifacts --npm-dir npm"` |
| `scripts/ci/test-binding-release-candidate.py` | 142 | `_, maturin_found, post_maturin = python_job.partition("uses: PyO3/maturin-action@v1")` |
| `scripts/ci/test-binding-release-candidate.py` | 143 | `assert maturin_found, "missing maturin build marker"` |
| `scripts/ci/test-binding-release-candidate.py` | 263 | `"pnpm --filter @curatelabs/graphforge exec napi build --platform --release",` |
| `scripts/ci/test-binding-release-candidate.py` | 509 | `"uses: PyO3/maturin-action@v1",` |
| `scripts/ci/test-binding-release-candidate.py` | 527 | `post_maturin_python = rc_workflow_text.split("uses: PyO3/maturin-action@v1", 1)[1].split(` |
| `scripts/ci/test-binding-release-candidate.py` | 551 | `assert "cargo test --release -p graphforge-storage" not in python_job` |
| `scripts/ci/test-binding-release-candidate.py` | 554 | `assert "uses: PyO3/maturin-action@v1" in python_job` |
| `scripts/ci/test-binding-release-candidate.py` | 566 | `"cargo test -p graphforge-storage project_generation::tests:: --lib",` |
| `scripts/ci/test-binding-release-candidate.py` | 597 | `assert "Reclaim sticky-disk ownership after maturin" in rc_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 618 | `"pnpm exec napi build --platform --release --target x86_64-unknown-linux-gnu"` |
| `scripts/ci/test-binding-release-candidate.py` | 628 | `assert 'cargo package "${package_args[@]}" --allow-dirty --no-verify' in (release_candidate_job)` |
| `scripts/ci/test-binding-release-candidate.py` | 629 | `assert 'cargo package -p "$crate"' not in release_candidate_job` |
| `scripts/ci/test-binding-release-candidate.py` | 654 | `assert "PyO3/maturin-action" not in publish_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 655 | `assert "napi build" not in publish_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 783 | `assert "exec napi build --platform --release" in workflow_text, (` |
| `scripts/ci/test-binding-release-candidate.py` | 804 | `assert "napi artifacts --dir" not in workflow_text, (` |
| `scripts/ci/test-binding-release-candidate.py` | 805 | `f"{workflow.name} uses the unsupported napi artifacts --dir option"` |
| `scripts/ci/test-binding-release-candidate.py` | 809 | `assert "exec napi build --platform --release" not in publish_text` |
| `Makefile` | 39 | `publish-dry-run-python:  ## Local maturin sdist packaging check (not TestPyPI upload)` |
| `Makefile` | 41 | `publish-dry-run-cargo:  ## cargo package --list for all 16 crates.io packages in plan order` |
| `Makefile` | 66 | `cargo test -p graphforge-core --test bdd` |
| `Makefile` | 84 | `echo "   maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml"; \` |
| `Makefile` | 104 | `coverage-python:  ## Run unit tests with Python wrapper coverage (requires maturin develop)` |
| `Makefile` | 250 | `cargo build --workspace` |
| `Makefile` | 253 | `cargo test --workspace` |
| `Makefile` | 271 | `cargo test -p graphforge-exec --release --test bench_traversal_scaling -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 274 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_fixed_hop_limit_1m_10m -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 278 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_livejournal_fixed_hop_limits -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 292 | `cargo test -p graphforge-api --release --test m4_entry_baseline large_manual_matrix_emits_hardware_dataset_evidence -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 326 | `cargo check --workspace` |
| `Makefile` | 329 | `cargo clippy --workspace -- -D warnings` |
| `Makefile` | 332 | `cargo fmt --all` |
| `Makefile` | 335 | `cargo fmt --all -- --check` |
| `Makefile` | 362 | `echo "   Build first: pnpm --filter @curatelabs/graphforge exec napi build --platform --release"; \` |
| `Makefile` | 385 | `cargo check --workspace` |
| `Makefile` | 389 | `cargo build --workspace` |

</details>

## Blacksmith runner path

- Required CI jobs use `blacksmith-*-ubuntu-*` / Blacksmith-hosted runners (see
  `.github/workflows/test.yml` and related gates).
- **#5 cache evidence:** Bazel Build Caching is enabled; remote hits observed and
  ≥10 cold/warm pairs checked in under
  `docs/development/bazel-migration-evidence/perf-sample.json` (strict evaluate
  passes). See [bazel-migration-perf.md](bazel-migration-perf.md).
- Do not configure repository `--remote_cache`; Blacksmith injects cache for Bazel jobs.

## Update rules

1. Modeling PRs must update `bazel_label` / `status` (markdown +
   `migration_target_map.json`) for touched rows in the same change.
2. New Cargo targets require a new map/ledger row; unmapped rows fail
   `scripts/ci/bazel-migration-ledger-check.py`.
3. Unjustified retained exceptions (`stub` or empty justification) fail the ledger.
4. Mobile bindings stay `excluded` — never promote to required Bazel-migration targets.
5. Release platform additions must update `release_platforms.json` and
   `//platforms:*` together with the Binding RC contract.
