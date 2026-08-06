# Bazelisk / Bzlmod bootstrap (#11–#9, #7 bindings)

Minimal Bazel workspace for M2 issues
[#11](https://github.com/CurateLabs/graphforge/issues/11),
[#10](https://github.com/CurateLabs/graphforge/issues/10),
[#9](https://github.com/CurateLabs/graphforge/issues/9), and
[#7](https://github.com/CurateLabs/graphforge/issues/7). Canonical contract:
[#1](https://github.com/CurateLabs/graphforge/issues/1). Orchestration:
[bazel-migration-orchestration.md](bazel-migration-orchestration.md).

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
| All modeled libs | `//:first_party_libs` / `//:first_party_lib_tests` |
| Binding cdylibs | `//:binding_cdylibs` (`graphforge_bindings_py` / `graphforge_bindings_node`) |
| Packaging handoff | `//:python_wheel_smoke` / `//:node_package_smoke` |
| Shared rust macros | `tools/bazel/gf_rust.bzl` (`gf_rust_library` / `gf_rust_test` / `gf_rust_shared_library` / `gf_cargo_build_script`) |
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
| CLI lib (link dep) | `//crates/graphforge-cli:graphforge_cli` | lib + skill-bundle `build.rs` only; bin/tests → #8 |
| Python wheel smoke | `//:python_wheel_smoke` | assembles wheel from Bazel `.so`/`.dylib`; no `maturin build` |
| Node package smoke | `//:node_package_smoke` | assembles zip from Bazel cdylib; no `napi build` |

Credentials / OIDC stay outside cacheable Bazel actions (publish workflows unchanged).

### Package coverage (17 workspace members)

| Class | Count | Status after #7 |
| --- | ---: | --- |
| Ordinary `lib` mapped | 15 | foundation + runtime + CLI lib (link dep) |
| Binding cdylibs mapped | 2 | PyO3 + napi-rs |
| Remaining for #8 | bin + integration/BDD/CLI tests + resources | |

### Residual gaps (justified)

- Workspace Clippy/lint policy from `Cargo.toml` `[workspace.lints]` is not yet
  mirrored as Bazel `rustc_flags` / Clippy aspects (Cargo remains authoritative
  for lint CI until a later slice).
- Doctests are not separate Bazel targets yet (same attachment note as the
  ledger unit-test policy).
- Integration / snapshot / BDD / CLI tests and the `gf` binary are out of scope (#8).
- Full cross-platform binding/release matrix remains #6.
- Bazel storage always enables `test-failpoints` (env-gated no-ops); Cargo release
  builds keep the const no-op body — track under #6 parity if needed.

## Local commands

```bash
# Pin via Bazelisk
bazelisk version   # must report 9.2.0 from .bazelversion

# Smoke + all modeled first-party libraries (rules_rust; no Cargo shell-out)
bazelisk test //tools/bazel/smoke:smoke_test //:first_party_lib_tests
bazelisk build //:bazel_smoke //:first_party_libs //:runtime_libs

# Binding cdylibs + packaging handoff (no maturin/napi recompile)
bazelisk build //:binding_cdylibs //:python_wheel_smoke //:node_package_smoke
python3 scripts/ci/test-assemble-bazel-binding-packages.py

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
  injects repository Bazel caching when org-admin enablement lands (#5).

## Next

1. [#8](https://github.com/CurateLabs/graphforge/issues/8) — test/BDD/CLI/resource
   graph (CLI bin + tests; may extend the CLI lib row already mapped for #7).
2. [#6](https://github.com/CurateLabs/graphforge/issues/6) — cross-platform release
   targets and Cargo/Bazel parity evidence (blocked by #7 and #8).
