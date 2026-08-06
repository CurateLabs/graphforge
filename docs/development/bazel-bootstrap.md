# Bazelisk / Bzlmod bootstrap (#11), foundation (#10), runtime libs (#9)

Minimal Bazel workspace for M2 issues
[#11](https://github.com/CurateLabs/graphforge/issues/11),
[#10](https://github.com/CurateLabs/graphforge/issues/10), and
[#9](https://github.com/CurateLabs/graphforge/issues/9). Canonical contract:
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
| Shared rust macros | `tools/bazel/gf_rust.bzl` (`gf_rust_library` / `gf_rust_test`) |
| Cargo feature fingerprint | `tools/bazel/drift/cargo_feature_fingerprint.json` |
| Drift check | `scripts/ci/cargo-bazel-drift-check.py` |
| Fail-closed drift test | `scripts/ci/test-cargo-bazel-drift-check.py` |
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

### Package coverage (17 workspace members)

| Class | Count | Status after #9 |
| --- | ---: | --- |
| Ordinary `lib` mapped | 14 | foundation + runtime above |
| Explicit retained exception | 3 | CLI lib (`RT-cli-build-script` → #8); bindings cdylibs (`RT-bindings-cdylib` → #7) |

### Residual gaps (justified)

- Workspace Clippy/lint policy from `Cargo.toml` `[workspace.lints]` is not yet
  mirrored as Bazel `rustc_flags` / Clippy aspects (Cargo remains authoritative
  for lint CI until a later slice).
- Doctests are not separate Bazel targets yet (same attachment note as the
  ledger unit-test policy).
- Integration / snapshot / BDD / CLI tests are out of scope (#8).
- CLI library `build.rs` skill-bundle embedding is `RT-cli-build-script` (#8).
- Binding cdylib packaging is #7.
- Bazel storage always enables `test-failpoints` (env-gated no-ops); Cargo release
  builds keep the const no-op body — track under #6 parity if needed.

## Local commands

```bash
# Pin via Bazelisk
bazelisk version   # must report 9.2.0 from .bazelversion

# Smoke + all modeled first-party libraries (rules_rust; no Cargo shell-out)
bazelisk test //tools/bazel/smoke:smoke_test //:first_party_lib_tests
bazelisk build //:bazel_smoke //:first_party_libs //:runtime_libs

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

## Next (#8 / #7, parallel)

1. [#8](https://github.com/CurateLabs/graphforge/issues/8) — test/BDD/CLI/resource
   graph (includes CLI lib + `build.rs`).
2. [#7](https://github.com/CurateLabs/graphforge/issues/7) — PyO3/napi cdylibs and
   packaging handoff.
