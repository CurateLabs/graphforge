#!/usr/bin/env bash
# Cheap whole-repository checks shared by `make check` and the CI lint job:
# lock files, licenses, and the few structural rules that are not a compiler
# or linter's job. Nothing here builds or runs product code.
set -euo pipefail
cd "$(dirname "$0")/../.."

uv lock --check
cargo metadata --locked --manifest-path benchmarks/Cargo.toml --format-version 1 >/dev/null
python3 scripts/license_check.py
python3 scripts/source_size_policy.py
python3 scripts/ci/check-domain-dependencies.py
python3 scripts/ci/python-build-mode-check.py
python3 scripts/ci/docs-tree-policy.py check
python3 scripts/ci/adr-index.py check
python3 scripts/ci/gate-registry.py validate
