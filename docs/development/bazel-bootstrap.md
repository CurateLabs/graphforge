# Bazelisk / Bzlmod bootstrap (#11)

Minimal Bazel workspace for M2 issue
[#11](https://github.com/CurateLabs/graphforge/issues/11). Canonical contract:
[#1](https://github.com/CurateLabs/graphforge/issues/1). Orchestration:
[bazel-migration-orchestration.md](bazel-migration-orchestration.md).

## What landed

| Piece | Location |
| --- | --- |
| Bazelisk pin | `.bazelversion` (`9.2.0`) |
| Bzlmod module | `MODULE.bazel` (`rules_rust` `0.73.0`, Rust `1.96.0`, edition `2024`) |
| Repo flags | `.bazelrc` (Bzlmod on; **no** `--remote_cache`) |
| Smoke library/test | `//tools/bazel/smoke:smoke` / `:smoke_test` |
| Cargo feature fingerprint | `tools/bazel/drift/cargo_feature_fingerprint.json` |
| Drift check | `scripts/ci/cargo-bazel-drift-check.py` |
| Fail-closed drift test | `scripts/ci/test-cargo-bazel-drift-check.py` |
| crate_universe fragment | `tools/bazel/crate_universe.MODULE.bazel.fragment` (activate in #10) |
| CI | `Bazel Bootstrap` job in `.github/workflows/test.yml` (path-classified) |

This slice does **not** claim first-party GraphForge library modeling (#10/#9).

## Local commands

```bash
# Pin via Bazelisk
bazelisk version   # must report 9.2.0 from .bazelversion

# Ordinary compilation (rules_rust; no Cargo shell-out)
bazelisk test //tools/bazel/smoke:smoke_test
bazelisk build //:bazel_smoke

# Cargo ↔ fingerprint drift (host cargo metadata; fail-closed)
python3 scripts/ci/cargo-bazel-drift-check.py
python3 scripts/ci/test-cargo-bazel-drift-check.py

# After intentional Cargo dependency/feature changes:
python3 scripts/ci/cargo-bazel-drift-check.py --write
```

## Blacksmith / cache

- CI runs on Blacksmith runners when `bazel` path classification is true.
- Do **not** add `--remote_cache` in `.bazelrc` or workflow steps. Blacksmith
  injects repository Bazel caching when org-admin enablement lands (#5).

## Next (#10)

1. Activate `crate.from_cargo` from the checked-in fragment (generate
   `cargo-bazel-lock.json`).
2. Model foundation/compiler-layer crates as real `rust_library` targets.
3. Update [bazel-migration-ledger.md](bazel-migration-ledger.md) labels/status.
