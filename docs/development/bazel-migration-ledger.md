# Bazel migration ledger (freeze)

Checked-in inventory for M2 issue [#12](https://github.com/CurateLabs/graphforge/issues/12) / canonical [#1](https://github.com/CurateLabs/graphforge/issues/1) step 1.

Orchestration contract: [bazel-migration-orchestration.md](bazel-migration-orchestration.md).
Performance baseline: [bazel-migration-baseline.md](bazel-migration-baseline.md).

## Freeze metadata

| Field | Value |
| --- | --- |
| Freeze date (UTC) | 2026-08-06 |
| Inventory SHA | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| Authoritative source | `cargo metadata --format-version=1 --no-deps` |
| Workspace packages | 17 |
| Cargo metadata targets | **90** |
| Bazel modeling claimed complete? | **No** — #10/#9/#7 libs+cdylibs mapped (15/15 lib rows + 2 cdylibs); CLI bin + tests/resources remain (#8) |
| Bootstrap note | See [bazel-bootstrap.md](bazel-bootstrap.md); crate_universe + foundation/runtime/binding labels from #10/#9/#7 |

Issue #1 historically cited ~71 Cargo targets / ~53 integration-test binaries.
This freeze uses the **current authoritative** metadata count (**90** targets;
**59** integration-test binaries). Later slices must update rows,
not silently ignore new targets.

## Target class summary

| Class | Count | Notes |
| --- | ---: | --- |
| `lib` | 15 | First-party libraries (unit/doctest surface rides these targets under `cargo test --lib` / doctests) |
| `integration-test` | 59 | `tests/*.rs` integration binaries |
| `cdylib` | 2 | PyO3 + napi-rs native libs |
| `bin` | 1 | CLI (`gf`) |
| `custom-build` | 2 | `build.rs` scripts |
| `example` | 11 | API examples (map or justify retained exception in later slices) |
| **Total** | **90** | |

### Unit tests and doctests

Cargo metadata does **not** emit separate targets for `#[cfg(test)]` unit modules or
doctests. For migration accounting:

- **Unit tests** → owned with the package `lib` (or `bin`) row; prove under Bazel via
  that crate's test configuration in #8/#10/#9.
- **Doctests** → same attachment, or a documented equivalent if Bazel doctest support
  differs (must not silently drop coverage).

## Cargo target ledger

Columns `bazel_label` and `status` are filled by modeling slices (#10–#7).
Integration-test / example / cdylib / bin rows stay `unmapped` until #8/#7/#9.

| Package | Target | Class | Source | Bazel label | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `graphforge-api` | `graphforge_api` | `lib` | `crates/graphforge-api/src/lib.rs` | `//crates/graphforge-api:graphforge_api` | `mapped` | #9; unit tests `//crates/graphforge-api:graphforge_api_test` |
| `graphforge-api` | `atomic_recovery_workflow` | `example` | `crates/graphforge-api/examples/atomic_recovery_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `correction_churn_workflow` | `example` | `crates/graphforge-api/examples/correction_churn_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `cyber_intrusion_workflow` | `example` | `crates/graphforge-api/examples/cyber_intrusion_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `derived_state_freshness_workflow` | `example` | `crates/graphforge-api/examples/derived_state_freshness_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `finance_fraud_workflow` | `example` | `crates/graphforge-api/examples/finance_fraud_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `knowledge_evolution_workflow` | `example` | `crates/graphforge-api/examples/knowledge_evolution_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `ontology_emergence_strict_handoff` | `example` | `crates/graphforge-api/examples/ontology_emergence_strict_handoff.rs` | — | `unmapped` | |
| `graphforge-api` | `probate_genealogy_workflow` | `example` | `crates/graphforge-api/examples/probate_genealogy_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `release_load_probe` | `example` | `crates/graphforge-api/examples/release_load_probe.rs` | — | `unmapped` | |
| `graphforge-api` | `sna_intelligence_workflow` | `example` | `crates/graphforge-api/examples/sna_intelligence_workflow.rs` | — | `unmapped` | |
| `graphforge-api` | `strict_add_node_fixture` | `example` | `crates/graphforge-api/examples/strict_add_node_fixture.rs` | — | `unmapped` | |
| `graphforge-api` | `bdd` | `integration-test` | `crates/graphforge-api/tests/bdd/main.rs` | — | `unmapped` | |
| `graphforge-api` | `bdd_timing` | `integration-test` | `crates/graphforge-api/tests/bdd_timing.rs` | — | `unmapped` | |
| `graphforge-api` | `belief_subject_contract` | `integration-test` | `crates/graphforge-api/tests/belief_subject_contract.rs` | — | `unmapped` | |
| `graphforge-api` | `bind_error_spans` | `integration-test` | `crates/graphforge-api/tests/bind_error_spans.rs` | — | `unmapped` | |
| `graphforge-api` | `clear` | `integration-test` | `crates/graphforge-api/tests/clear.rs` | — | `unmapped` | |
| `graphforge-api` | `conductance` | `integration-test` | `crates/graphforge-api/tests/conductance.rs` | — | `unmapped` | |
| `graphforge-api` | `create_scaling` | `integration-test` | `crates/graphforge-api/tests/create_scaling.rs` | — | `unmapped` | |
| `graphforge-api` | `e2e_baseline` | `integration-test` | `crates/graphforge-api/tests/e2e_baseline.rs` | — | `unmapped` | |
| `graphforge-api` | `existential_subquery` | `integration-test` | `crates/graphforge-api/tests/existential_subquery.rs` | — | `unmapped` | |
| `graphforge-api` | `facade_methods` | `integration-test` | `crates/graphforge-api/tests/facade_methods.rs` | — | `unmapped` | |
| `graphforge-api` | `fixed_hop_limit` | `integration-test` | `crates/graphforge-api/tests/fixed_hop_limit.rs` | — | `unmapped` | |
| `graphforge-api` | `graph_internal_metadata` | `integration-test` | `crates/graphforge-api/tests/graph_internal_metadata.rs` | — | `unmapped` | |
| `graphforge-api` | `inference_provenance` | `integration-test` | `crates/graphforge-api/tests/inference_provenance.rs` | — | `unmapped` | |
| `graphforge-api` | `knowledge_isolation` | `integration-test` | `crates/graphforge-api/tests/knowledge_isolation.rs` | — | `unmapped` | |
| `graphforge-api` | `list_semantics` | `integration-test` | `crates/graphforge-api/tests/list_semantics.rs` | — | `unmapped` | |
| `graphforge-api` | `m22_m18_public_surface` | `integration-test` | `crates/graphforge-api/tests/m22_m18_public_surface.rs` | — | `unmapped` | |
| `graphforge-api` | `m22_m19_public_surface` | `integration-test` | `crates/graphforge-api/tests/m22_m19_public_surface.rs` | — | `unmapped` | |
| `graphforge-api` | `m22_provider_public_surface` | `integration-test` | `crates/graphforge-api/tests/m22_provider_public_surface.rs` | — | `unmapped` | |
| `graphforge-api` | `max_bipartite_matching` | `integration-test` | `crates/graphforge-api/tests/max_bipartite_matching.rs` | — | `unmapped` | |
| `graphforge-api` | `max_cardinality_matching` | `integration-test` | `crates/graphforge-api/tests/max_cardinality_matching.rs` | — | `unmapped` | |
| `graphforge-api` | `max_weight_matching` | `integration-test` | `crates/graphforge-api/tests/max_weight_matching.rs` | — | `unmapped` | |
| `graphforge-api` | `minimum_k_spanning_tree` | `integration-test` | `crates/graphforge-api/tests/minimum_k_spanning_tree.rs` | — | `unmapped` | |
| `graphforge-api` | `modularity` | `integration-test` | `crates/graphforge-api/tests/modularity.rs` | — | `unmapped` | |
| `graphforge-api` | `multi_label_scaling` | `integration-test` | `crates/graphforge-api/tests/multi_label_scaling.rs` | — | `unmapped` | |
| `graphforge-api` | `pattern_comprehension` | `integration-test` | `crates/graphforge-api/tests/pattern_comprehension.rs` | — | `unmapped` | |
| `graphforge-api` | `percentile_aggregates` | `integration-test` | `crates/graphforge-api/tests/percentile_aggregates.rs` | — | `unmapped` | |
| `graphforge-api` | `provider_session` | `integration-test` | `crates/graphforge-api/tests/provider_session.rs` | — | `unmapped` | |
| `graphforge-api` | `public_facade_remaining_conformance` | `integration-test` | `crates/graphforge-api/tests/public_facade_remaining_conformance.rs` | — | `unmapped` | |
| `graphforge-api` | `public_lifecycle_conformance` | `integration-test` | `crates/graphforge-api/tests/public_lifecycle_conformance.rs` | — | `unmapped` | |
| `graphforge-api` | `release_load_construction` | `integration-test` | `crates/graphforge-api/tests/release_load_construction.rs` | — | `unmapped` | |
| `graphforge-api` | `strict_runtime_properties` | `integration-test` | `crates/graphforge-api/tests/strict_runtime_properties.rs` | — | `unmapped` | |
| `graphforge-api` | `value_access_semantics` | `integration-test` | `crates/graphforge-api/tests/value_access_semantics.rs` | — | `unmapped` | |
| `graphforge-api` | `value_semantics` | `integration-test` | `crates/graphforge-api/tests/value_semantics.rs` | — | `unmapped` | |
| `graphforge-api` | `with_aggregation` | `integration-test` | `crates/graphforge-api/tests/with_aggregation.rs` | — | `unmapped` | |
| `graphforge-api` | `xor_scaling` | `integration-test` | `crates/graphforge-api/tests/xor_scaling.rs` | — | `unmapped` | |
| `graphforge-ast` | `graphforge_ast` | `lib` | `crates/graphforge-ast/src/lib.rs` | `//crates/graphforge-ast:graphforge_ast` | `mapped` | #10; unit tests `//crates/graphforge-ast:graphforge_ast_test` |
| `graphforge-bindings-node` | `graphforge_bindings_node` | `cdylib` | `crates/graphforge-bindings-node/src/lib.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node` | `mapped` | #7; packaging `//:node_package_smoke` |
| `graphforge-bindings-node` | `build-script-build` | `custom-build` | `crates/graphforge-bindings-node/build.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node_build_script` | `mapped` | #7; `napi-build` via `gf_cargo_build_script` |
| `graphforge-bindings-py` | `graphforge_bindings_py` | `cdylib` | `crates/graphforge-bindings-py/src/lib.rs` | `//crates/graphforge-bindings-py:graphforge_bindings_py` | `mapped` | #7; packaging `//:python_wheel_smoke` |
| `graphforge-cli` | `graphforge_cli` | `lib` | `crates/graphforge-cli/src/lib.rs` | `//crates/graphforge-cli:graphforge_cli` | `mapped` | #7 link dep for bindings; bin/tests remain #8 |
| `graphforge-cli` | `gf` | `bin` | `crates/graphforge-cli/src/main.rs` | — | `unmapped` | #8 |
| `graphforge-cli` | `checkpoints` | `integration-test` | `crates/graphforge-cli/tests/checkpoints.rs` | — | `unmapped` | #8 |
| `graphforge-cli` | `portable` | `integration-test` | `crates/graphforge-cli/tests/portable.rs` | — | `unmapped` | #8 |
| `graphforge-cli` | `repository` | `integration-test` | `crates/graphforge-cli/tests/repository.rs` | — | `unmapped` | #8 |
| `graphforge-cli` | `build-script-build` | `custom-build` | `crates/graphforge-cli/build.rs` | `//crates/graphforge-cli:graphforge_cli_build_script` | `mapped` | #7 (embeds `project-skills`); #8 owns bin/tests |
| `graphforge-core` | `graphforge_core` | `lib` | `crates/graphforge-core/src/lib.rs` | `//crates/graphforge-core:graphforge_core` | `mapped` | #10; unit tests `//crates/graphforge-core:graphforge_core_test` |
| `graphforge-cypher` | `graphforge_cypher` | `lib` | `crates/graphforge-cypher/src/lib.rs` | `//crates/graphforge-cypher:graphforge_cypher` | `mapped` | #10; unit tests `//crates/graphforge-cypher:graphforge_cypher_test` |
| `graphforge-cypher` | `corpus` | `integration-test` | `crates/graphforge-cypher/tests/corpus.rs` | — | `unmapped` | #8 |
| `graphforge-exec` | `graphforge_exec` | `lib` | `crates/graphforge-exec/src/lib.rs` | `//crates/graphforge-exec:graphforge_exec` | `mapped` | #9; unit tests `//crates/graphforge-exec:graphforge_exec_test` |
| `graphforge-exec` | `adjacency_expand` | `integration-test` | `crates/graphforge-exec/tests/adjacency_expand.rs` | — | `unmapped` | |
| `graphforge-exec` | `bench_traversal_scaling` | `integration-test` | `crates/graphforge-exec/tests/bench_traversal_scaling.rs` | — | `unmapped` | |
| `graphforge-exec` | `create_execution` | `integration-test` | `crates/graphforge-exec/tests/create_execution.rs` | — | `unmapped` | |
| `graphforge-exec` | `create_input_driven` | `integration-test` | `crates/graphforge-exec/tests/create_input_driven.rs` | — | `unmapped` | |
| `graphforge-exec` | `differential_traversal` | `integration-test` | `crates/graphforge-exec/tests/differential_traversal.rs` | — | `unmapped` | |
| `graphforge-exec` | `explain_snapshots` | `integration-test` | `crates/graphforge-exec/tests/explain_snapshots.rs` | — | `unmapped` | |
| `graphforge-exec` | `optional_match` | `integration-test` | `crates/graphforge-exec/tests/optional_match.rs` | — | `unmapped` | |
| `graphforge-exec` | `persistent_adjacency` | `integration-test` | `crates/graphforge-exec/tests/persistent_adjacency.rs` | — | `unmapped` | |
| `graphforge-exec` | `read_execution` | `integration-test` | `crates/graphforge-exec/tests/read_execution.rs` | — | `unmapped` | |
| `graphforge-exec` | `unwind` | `integration-test` | `crates/graphforge-exec/tests/unwind.rs` | — | `unmapped` | |
| `graphforge-exec` | `var_len_expand` | `integration-test` | `crates/graphforge-exec/tests/var_len_expand.rs` | — | `unmapped` | |
| `graphforge-exec` | `write_statement` | `integration-test` | `crates/graphforge-exec/tests/write_statement.rs` | — | `unmapped` | |
| `graphforge-io` | `graphforge_io` | `lib` | `crates/graphforge-io/src/lib.rs` | `//crates/graphforge-io:graphforge_io` | `mapped` | #9; unit tests `//crates/graphforge-io:graphforge_io_test` |
| `graphforge-ir` | `graphforge_ir` | `lib` | `crates/graphforge-ir/src/lib.rs` | `//crates/graphforge-ir:graphforge_ir` | `mapped` | #10; unit tests `//crates/graphforge-ir:graphforge_ir_test` |
| `graphforge-ir` | `golden` | `integration-test` | `crates/graphforge-ir/tests/golden.rs` | — | `unmapped` | #8 |
| `graphforge-knowledge` | `graphforge_knowledge` | `lib` | `crates/graphforge-knowledge/src/lib.rs` | `//crates/graphforge-knowledge:graphforge_knowledge` | `mapped` | #9; unit tests `//crates/graphforge-knowledge:graphforge_knowledge_test` |
| `graphforge-ontology` | `graphforge_ontology` | `lib` | `crates/graphforge-ontology/src/lib.rs` | `//crates/graphforge-ontology:graphforge_ontology` | `mapped` | #10; unit tests `//crates/graphforge-ontology:graphforge_ontology_test` |
| `graphforge-ontology` | `integration` | `integration-test` | `crates/graphforge-ontology/tests/integration.rs` | — | `unmapped` | #8 |
| `graphforge-plan` | `graphforge_plan` | `lib` | `crates/graphforge-plan/src/lib.rs` | `//crates/graphforge-plan:graphforge_plan` | `mapped` | #10; unit tests `//crates/graphforge-plan:graphforge_plan_test` |
| `graphforge-provenance` | `graphforge_provenance` | `lib` | `crates/graphforge-provenance/src/lib.rs` | `//crates/graphforge-provenance:graphforge_provenance` | `mapped` | #10; unit tests `//crates/graphforge-provenance:graphforge_provenance_test` |
| `graphforge-rel` | `graphforge_rel` | `lib` | `crates/graphforge-rel/src/lib.rs` | `//crates/graphforge-rel:graphforge_rel` | `mapped` | #10; unit tests `//crates/graphforge-rel:graphforge_rel_test` |
| `graphforge-rel` | `expression_lowering_matrix` | `integration-test` | `crates/graphforge-rel/tests/expression_lowering_matrix.rs` | — | `unmapped` | #8 |
| `graphforge-rel` | `logical_plan_golden` | `integration-test` | `crates/graphforge-rel/tests/logical_plan_golden.rs` | — | `unmapped` | #8 |
| `graphforge-search` | `graphforge_search` | `lib` | `crates/graphforge-search/src/lib.rs` | `//crates/graphforge-search:graphforge_search` | `mapped` | #9; unit tests `//crates/graphforge-search:graphforge_search_test` |
| `graphforge-storage` | `graphforge_storage` | `lib` | `crates/graphforge-storage/src/lib.rs` | `//crates/graphforge-storage:graphforge_storage` | `mapped` | #10 early / #9; Bazel enables `test-failpoints` for api subprocess unification; unit tests `//crates/graphforge-storage:graphforge_storage_test` |
| `graphforge-storage` | `adjacency_delta_write` | `integration-test` | `crates/graphforge-storage/tests/adjacency_delta_write.rs` | — | `unmapped` | |
| `graphforge-storage` | `filtered_read` | `integration-test` | `crates/graphforge-storage/tests/filtered_read.rs` | — | `unmapped` | |
| `graphforge-storage` | `graph_writer` | `integration-test` | `crates/graphforge-storage/tests/graph_writer.rs` | — | `unmapped` | |
| `graphforge-storage` | `io_stats` | `integration-test` | `crates/graphforge-storage/tests/io_stats.rs` | — | `unmapped` | |

## Retained-tool exception stubs

These are **not** claimed as Bazel-complete at freeze. Each needs a justification
before #6 parity can pass with the exception still open.

| ID | Tool / surface | Why Bazel may not replace cleanly | Owning follow-up | Status |
| --- | --- | --- | --- | --- |
| RT-fuzz | `cargo fuzz` (`fuzz/` workspace, workflow `fuzz.yml`) | cargo-fuzz driver + corpus workflow outside ordinary `rules_rust` test graph | #8/#6 justify or map | stub |
| RT-publish-crates | `cargo publish` / crates.io authorize flows | Ecosystem publication metadata and registry auth | keep Cargo; ledger must remain explicit | stub |
| RT-maturin-assemble | `maturin build` / `maturin sdist` packaging assembly | Bazel handoff: `//:python_wheel_smoke` + `assemble_bazel_binding_packages.py` consume Bazel cdylibs (no silent `maturin build` recompile). Maturin may still sign/publish later. | #7 handoff | handoff |
| RT-napi-assemble | `napi build` / `napi artifacts` / `napi pre-publish` | Bazel handoff: `//:node_package_smoke` consumes Bazel cdylib (no silent `napi build` recompile). napi may still assemble/sign/publish later. | #7 handoff | handoff |
| RT-cli-build-script | `graphforge-cli` lib (`build.rs` → embedded `project-skills`) | Lib + build script mapped for #7 binding link; bin/tests still #8 | #8 bin/tests; #7 mapped lib | partial |
| RT-bindings-cdylib | `graphforge-bindings-py` / `graphforge-bindings-node` packages | Mapped as `rust_shared_library` cdylibs + packaging smoke targets | #7 | mapped |
| RT-examples | `graphforge-api` examples (11) | May be CI/release probes vs developer samples; map or except per #8/#6 | #8/#6 | stub |
| RT-mobile | Swift / Kotlin / UniFFI / XCFramework / JVM AAR | **Abandoned for M2** — not a deliverable; do not inventory as required targets | excluded | excluded |

## CI / release build command sites

Frozen scan of `.github/workflows/`, `scripts/`, and `Makefile` for `cargo`,
`maturin`, and `napi` build/test command invocations: **120** sites across
**27** files. Representative required path is `CI Gate` via
`.github/workflows/test.yml` on Blacksmith runners.

### Sticky Cargo `target/` disks (retire only after #4 evidence)

Workflows using `useblacksmith/stickydisk` at freeze:
- `.github/workflows/binding-release-candidate.yml`
- `.github/workflows/test.yml`
- `.github/workflows/m1-release-certification.yml`
- `.github/workflows/fuzz.yml`

Primary `test.yml` sticky key pattern:
`${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1` → `target/`.

### Sites by file

| File | Sites | Role |
| --- | ---: | --- |
| `.github/workflows/binding-release-candidate.yml` | 11 | Binding RC wheels/addons |
| `.github/workflows/checkpoint-recovery-gate.yml` | 6 | Checkpoint recovery gate |
| `.github/workflows/concurrency-stress-gate.yml` | 1 | Concurrency stress |
| `.github/workflows/fuzz.yml` | 6 | cargo-fuzz (retained-tool candidate) |
| `.github/workflows/m1-release-certification.yml` | 4 | Release load certification |
| `.github/workflows/non-cypher-surface-gate.yml` | 4 | Non-Cypher surface gate |
| `.github/workflows/test.yml` | 6 | Required CI Gate compile/test/bindings |
| `.github/workflows/visualization-limits-stress.yml` | 1 | Visualization stress |
| `Makefile` | 17 | Developer/CI mirrors |
| `scripts/ci/clean-env-verify.py` | 1 | Build/test/package command site |
| `scripts/ci/crate-publish-plan.py` | 4 | Build/test/package command site |
| `scripts/ci/m1-release-certification.py` | 2 | Build/test/package command site |
| `scripts/ci/test-binding-release-candidate.py` | 19 | Build/test/package command site |
| `scripts/ci/test-checkpoint-recovery-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-ci-storage-policy.py` | 3 | Build/test/package command site |
| `scripts/ci/test-crate-publish-plan.py` | 2 | Build/test/package command site |
| `scripts/ci/test-m1-release-certification.py` | 10 | Build/test/package command site |
| `scripts/ci/test-m20-contract-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-m21-contract-gate.py` | 1 | Build/test/package command site |
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
| `.github/workflows/non-cypher-surface-gate.yml` | 136 | `"cargo test -p graphforge-api --test knowledge_isolation --test public_lifecycle_conformance --test public_facade_remaining_conformance --test m22_m18_public_su` |
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
| `.github/workflows/m1-release-certification.yml` | 137 | `uses: PyO3/maturin-action@v1` |
| `.github/workflows/m1-release-certification.yml` | 145 | `- name: Reclaim sticky-disk ownership after maturin` |
| `.github/workflows/m1-release-certification.yml` | 166 | `cargo build --release -p graphforge-api --example release_load_probe` |
| `.github/workflows/m1-release-certification.yml` | 171 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release \` |
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
| `scripts/ci/m1-release-certification.py` | 176 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/m1-release-certification.py` | 177 | `"cargo test -p graphforge-api --test knowledge_isolation "` |
| `scripts/ci/test-crate-publish-plan.py` | 81 | `assert commands[0].startswith("cargo publish -p graphforge-core ")` |
| `scripts/ci/test-crate-publish-plan.py` | 82 | `assert commands[-1].startswith("cargo publish -p graphforge-cli ")` |
| `scripts/ci/test-m1-release-certification.py` | 112 | `self.assertNotIn("cargo build", validation_job)` |
| `scripts/ci/test-m1-release-certification.py` | 113 | `self.assertNotIn("maturin-action", validation_job)` |
| `scripts/ci/test-m1-release-certification.py` | 129 | `self.assertIn("Reclaim sticky-disk ownership after maturin", load_job)` |
| `scripts/ci/test-m1-release-certification.py` | 138 | `reclaim_step = load_job.index("- name: Reclaim sticky-disk ownership after maturin")` |
| `scripts/ci/test-m1-release-certification.py` | 147 | `self.assertNotIn("cargo build", load_job[wrapper_step:artifact_step])` |
| `scripts/ci/test-m1-release-certification.py` | 151 | `rust_build = artifact_build.index("cargo build")` |
| `scripts/ci/test-m1-release-certification.py` | 152 | `node_build = artifact_build.index("napi build")` |
| `scripts/ci/test-m1-release-certification.py` | 219 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/test-m1-release-certification.py` | 220 | `"cargo test -p graphforge-api --test knowledge_isolation --test "` |
| `scripts/ci/test-m1-release-certification.py` | 320 | `bad_rust_commands["commands"][0] = "cargo test --workspace"` |
| `scripts/ci/test-pre-push-validation.py` | 324 | `self.assertFalse(any(command[0] == "uv" and "maturin" in command for command in commands))` |
| `scripts/ci/clean-env-verify.py` | 540 | `result.commands.append("cargo check")` |
| `scripts/ci/test-m21-contract-gate.py` | 60 | `forbidden["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
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
| `scripts/ci/test-m20-contract-gate.py` | 51 | `forbidden_command["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
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
| `Makefile` | 41 | `publish-dry-run-cargo:  ## cargo package --list for all 15 crates.io packages in plan order` |
| `Makefile` | 66 | `cargo test -p graphforge-core --test bdd` |
| `Makefile` | 84 | `echo "   maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml"; \` |
| `Makefile` | 104 | `coverage-python:  ## Run unit tests with Python wrapper coverage (requires maturin develop)` |
| `Makefile` | 250 | `cargo build --workspace` |
| `Makefile` | 253 | `cargo test --workspace` |
| `Makefile` | 271 | `cargo test -p graphforge-exec --release --test bench_traversal_scaling -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 274 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_fixed_hop_limit_1m_10m -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 278 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_livejournal_fixed_hop_limits -- --ignored --nocapture --test-threads=1` |
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
- **Org-admin dependency for #5:** enable **Bazel Build Caching** for this repository
  in Blacksmith. Ledger freeze does **not** wait on that enablement.
- Do not configure repository `--remote_cache`; Blacksmith injects cache for Bazel jobs.

## Update rules

1. Modeling PRs (#11–#7) must update `bazel_label` / `status` for touched rows in the
   same change.
2. New Cargo targets require a new ledger row before #6 parity can pass.
3. Unjustified retained exceptions fail the migration at #6.
4. Mobile bindings stay `excluded` — never promote to required M2 targets.

