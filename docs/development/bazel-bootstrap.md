# Bazelisk / Bzlmod bootstrap (#11) and foundation libs (#10)

Minimal Bazel workspace for M2 issues
[#11](https://github.com/CurateLabs/graphforge/issues/11) and
[#10](https://github.com/CurateLabs/graphforge/issues/10). Canonical contract:
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
| Shared rust macros | `tools/bazel/gf_rust.bzl` (`gf_rust_library` / `gf_rust_test`) |
| Cargo feature fingerprint | `tools/bazel/drift/cargo_feature_fingerprint.json` |
| Drift check | `scripts/ci/cargo-bazel-drift-check.py` |
| Fail-closed drift test | `scripts/ci/test-cargo-bazel-drift-check.py` |
| CI | `Bazel Bootstrap` job in `.github/workflows/test.yml` (path-classified) |

## Foundation / compiler slice (#10)

Modeled first-party libraries (ordinary compilation via rules_rust; no Cargo
shell-out):

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

`graphforge-storage` is modeled in #10 because `graphforge-rel` depends on it.
Issue [#9](https://github.com/CurateLabs/graphforge/issues/9) still owns exec,
search, knowledge, API, and io libraries. Integration-test binaries remain for
[#8](https://github.com/CurateLabs/graphforge/issues/8).

### Residual gaps (justified)

- Workspace Clippy/lint policy from `Cargo.toml` `[workspace.lints]` is not yet
  mirrored as Bazel `rustc_flags` / Clippy aspects (Cargo remains authoritative
  for lint CI until a later slice).
- Doctests are not separate Bazel targets yet (same attachment note as the
  ledger unit-test policy).
- Integration / snapshot / BDD / CLI tests are out of scope for #10.

## Local commands

```bash
# Pin via Bazelisk
bazelisk version   # must report 9.2.0 from .bazelversion

# Smoke + foundation/compiler libraries (rules_rust; no Cargo shell-out)
bazelisk test //tools/bazel/smoke:smoke_test //:foundation_compiler_tests
bazelisk build //:bazel_smoke //:foundation_compiler_libs

# Cargo ↔ fingerprint drift (host cargo metadata; fail-closed)
python3 scripts/ci/cargo-bazel-drift-check.py
python3 scripts/ci/test-cargo-bazel-drift-check.py

# After intentional Cargo dependency/feature changes:
python3 scripts/ci/cargo-bazel-drift-check.py --write
CARGO_BAZEL_REPIN=1 bazelisk build --repo_env=CARGO_BAZEL_REPIN=1 //:foundation_compiler_libs
```

## Blacksmith / cache

- CI runs on Blacksmith runners when `bazel` path classification is true.
- Do **not** add `--remote_cache` in `.bazelrc` or workflow steps. Blacksmith
  injects repository Bazel caching when org-admin enablement lands (#5).

## Next (#9)

1. Model remaining first-party libraries (exec, search, knowledge, api, io).
2. Keep drift check + `cargo-bazel-lock.json` green across new deps/features.
3. Update [bazel-migration-ledger.md](bazel-migration-ledger.md) labels/status.
