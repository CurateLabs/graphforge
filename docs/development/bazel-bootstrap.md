# Bazelisk / Bzlmod bootstrap (#11–#8, #7 bindings)

Minimal Bazel workspace for M2 issues
[#11](https://github.com/CurateLabs/graphforge/issues/11)–[#7](https://github.com/CurateLabs/graphforge/issues/7).
Canonical contract: [#1](https://github.com/CurateLabs/graphforge/issues/1).
Orchestration: [bazel-migration-orchestration.md](bazel-migration-orchestration.md).

## What landed

| Piece | Location |
| --- | --- |
| Bazelisk pin | `.bazelversion` (`9.2.0`) |
| Bzlmod module | `MODULE.bazel` (`rules_rust` `0.73.0`, Rust `1.96.0`, edition `2024`) |
| crate_universe | `crate.from_cargo` → `@crates` + checked-in `cargo-bazel-lock.json` |
| Repo flags | `.bazelrc` (Bzlmod on; **no** `--remote_cache`) |
| Smoke library/test | `//tools/bazel/smoke:smoke` / `:smoke_test` |
| Foundation/compiler libs | `//:foundation_compiler_libs` / `//:foundation_compiler_tests` |
| Runtime libs | `//:runtime_libs` / `//:runtime_lib_tests` |
| All modeled libs (incl. CLI) | `//:first_party_libs` / `//:first_party_lib_tests` |
| Binding cdylibs | `//:binding_cdylibs` (`graphforge_bindings_py` / `graphforge_bindings_node`) |
| Packaging handoff | `//:python_wheel_smoke` / `//:node_package_smoke` |
| CI target groups | `//:unit_tests`, `//:integration_tests`, `//:snapshot_tests`, `//:bdd_tests`, `//:cli_tests` |
| Resource filegroups | `//:resource_inputs` (skills via `//:project_skills_bundle`, TCK, features, goldens, notebooks, contracts) |
| Bootstrap smoke suite | `//:bazel_test_graph_smoke` |
| Shared rust macros | `tools/bazel/gf_rust.bzl` (`gf_rust_library` / `gf_rust_test` / `gf_rust_integration_test` / `gf_rust_binary` / `gf_rust_shared_library` / `gf_cargo_build_script`) |
| Cargo feature fingerprint | `tools/bazel/drift/cargo_feature_fingerprint.json` |
| Drift check | `scripts/ci/cargo-bazel-drift-check.py` |
| Fail-closed drift test | `scripts/ci/test-cargo-bazel-drift-check.py` |
| Binding packaging assembler | `scripts/ci/assemble_bazel_binding_packages.py` |
| CI | `Bazel Bootstrap` job in `.github/workflows/test.yml` (path-classified) |

## Foundation / compiler slice (#10)

| Crate | Library label | Unit-test label |
| --- | --- | --- |
| `graphforge-core` | `//crates/graphforge-core:graphforge_core` | `:graphforge_core_test` |
| `graphforge-ast` | `//crates/graphforge-ast:graphforge_ast` | `:graphforge_ast_test` |
| `graphforge-ontology` | `//crates/graphforge-ontology:graphforge_ontology` | `:graphforge_ontology_test` |
| `graphforge-provenance` | `//crates/graphforge-provenance:graphforge_provenance` | `:graphforge_provenance_test` |
| `graphforge-ir` | `//crates/graphforge-ir:graphforge_ir` | `:graphforge_ir_test` |
| `graphforge-plan` | `//crates/graphforge-plan:graphforge_plan` | `:graphforge_plan_test` |
| `graphforge-storage` | `//crates/graphforge-storage:graphforge_storage` | `:graphforge_storage_test` |
| `graphforge-rel` | `//crates/graphforge-rel:graphforge_rel` | `:graphforge_rel_test` |
| `graphforge-cypher` | `//crates/graphforge-cypher:graphforge_cypher` | `:graphforge_cypher_test` |

## Runtime slice (#9)

| Crate | Library label | Unit-test label |
| --- | --- | --- |
| `graphforge-exec` | `//crates/graphforge-exec:graphforge_exec` | `:graphforge_exec_test` |
| `graphforge-search` | `//crates/graphforge-search:graphforge_search` | `:graphforge_search_test` |
| `graphforge-knowledge` | `//crates/graphforge-knowledge:graphforge_knowledge` | `:graphforge_knowledge_test` |
| `graphforge-api` | `//crates/graphforge-api:graphforge_api` | `:graphforge_api_test` |
| `graphforge-io` | `//crates/graphforge-io:graphforge_io` | `:graphforge_io_test` |

`graphforge-storage` remains under the foundation aggregate (modeled in #10) and is
also listed in `//:runtime_libs` for the runtime slice view. Bazel enables the
`test-failpoints` crate feature on storage so api subprocess recovery unit tests
see the same env-gated hooks Cargo gets via feature unification.

## Binding cdylibs + packaging (#7)

| Target | Label | Notes |
| --- | --- | --- |
| PyO3 cdylib | `//crates/graphforge-bindings-py:graphforge_bindings_py` | abi3-py310 / extension-module via crate_universe |
| napi-rs cdylib | `//crates/graphforge-bindings-node:graphforge_bindings_node` | includes `napi-build` build script |
| Python wheel smoke | `//:python_wheel_smoke` | assembles wheel from Bazel `.so`/`.dylib`; no `maturin build` |
| Node package smoke | `//:node_package_smoke` | assembles zip from Bazel cdylib; no `napi build` |

Credentials / OIDC stay outside cacheable Bazel actions (publish workflows unchanged).

## Test / CLI / resource slice (#8)

| Surface | Labels |
| --- | --- |
| CLI lib + `build.rs` + `gf` bin | `//crates/graphforge-cli:graphforge_cli`, `:graphforge_cli_build_script`, `:gf` |
| CLI tests | `//crates/graphforge-cli:cli_tests` |
| Integration suite | `//:integration_tests` (59 Cargo integration binaries) |
| Snapshot / golden | `//:snapshot_tests` (IR, logical plan, explain) |
| BDD (API + TCK) | `//:bdd_tests` → `//crates/graphforge-api:bdd` |
| Resources | `//:resource_inputs` (`//:project_skills_bundle`, TCK, features, goldens, notebooks, contracts) |

`project-skills/` remains a pure distribution tree (no `BUILD.bazel` payload);
skills are declared via root `//:project_skills_bundle` / `//:project-skills/manifest.json`.

### Package coverage (17 workspace members)

| Class | Count | Status after #8 |
| --- | ---: | --- |
| Ordinary `lib` mapped | 15 | foundation + runtime + CLI |
| Binding cdylibs mapped | 2 | PyO3 + napi-rs (#7) |
| Integration-test mapped | 59 | `//:integration_tests` (+ BDD harness) |
| CLI `bin` mapped | 1 | `//crates/graphforge-cli:gf` |
| CLI `custom-build` mapped | 1 | `cargo_build_script` + `//:project_skills_bundle` |
| Example binaries mapped | 11 | `//crates/graphforge-api:*` + `//:release_bins` (#6) |
| Justified Cargo tools | 2 | `RT-fuzz`, `RT-publish-crates` |
| Release platforms | 5 OS/arch + 8 Binding RC surfaces | `//platforms:*` + `tools/bazel/release/release_platforms.json` |

### Residual gaps (justified)

- Workspace Clippy/lint policy from `Cargo.toml` `[workspace.lints]` is not yet
  mirrored as Bazel `rustc_flags` / Clippy aspects (Cargo remains authoritative
  for lint CI until a later slice).
- Doctests are not separate Bazel targets (documented equivalent: coverage attaches
  to crate unit-test targets; same policy as #10/#9).
- Blacksmith `Bazel Bootstrap` runs authoritative `//:ci_rust_tests`, release bins,
  binding smokes, and diagnostic #6 dual-build parity (one release cycle after #4).
  Full `//:integration_tests` and `//:bdd_tests` remain executable under Bazel for
  local/full runs.
- Cross-OS Binding RC still produces macOS/Windows natives on those runners;
  Bazel models every certified platform and builds host-native release artifacts.
- Bazel storage always enables `test-failpoints` (env-gated no-ops); Cargo release
  builds keep the const no-op body — documented dual-build parity surface.

## Local commands

```bash
# Pin via Bazelisk
bazelisk version   # must report 9.2.0 from .bazelversion

# Smoke + modeled first-party libraries/tests/CLI/resources
bazelisk test //:bazel_test_graph_smoke
bazelisk build //:first_party_libs //:cli_bins //:resource_inputs

# Binding cdylibs + packaging handoff (no maturin/napi recompile)
bazelisk build //:binding_cdylibs //:python_wheel_smoke //:node_package_smoke
python3 scripts/ci/test-assemble-bazel-binding-packages.py

# Release bins + dual-build parity (#6)
bazelisk build //:release_bins
python3 scripts/ci/bazel-migration-ledger-check.py
python3 scripts/ci/cargo-bazel-parity-check.py --mode all \
  --write-evidence dist/cargo-bazel-parity-evidence.json

# Full mapped suites (longer)
bazelisk test //:unit_tests //:integration_tests //:snapshot_tests //:cli_tests
bazelisk test //:bdd_tests

# Cargo ↔ fingerprint drift (host cargo metadata; fail-closed)
python3 scripts/ci/cargo-bazel-drift-check.py
python3 scripts/ci/test-cargo-bazel-drift-check.py

# After intentional Cargo dependency/feature changes:
python3 scripts/ci/cargo-bazel-drift-check.py --write
CARGO_BAZEL_REPIN=1 bazelisk build --repo_env=CARGO_BAZEL_REPIN=1 //:first_party_libs
```

## Blacksmith / cache

- CI runs on Blacksmith runners when `bazel` path classification is true.
- Do **not** add `--remote_cache` in `.bazelrc` or workflow steps. Blacksmith
  injects repository Bazel caching (org-admin enablement complete; see #5).
- Policy + measurement harness: `scripts/ci/bazel-cache-perf.py` (see
  [bazel-migration-perf.md](bazel-migration-perf.md)).
- After [#4](https://github.com/CurateLabs/graphforge/issues/4), `Bazel Bootstrap`
  is authoritative under `CI Gate` (`//:ci_rust_tests`).

## Developer entrypoint

Day-to-day install, extending targets, packaging handoff, troubleshooting, and
CI/release runbooks: [bazel.md](bazel.md).
#1 close-readiness evidence map: [bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md).
