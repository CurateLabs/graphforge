#!/usr/bin/env bash
# Revalidate the same-SHA per-surface Rust coverage ledger.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LEDGER="${COVERAGE_RUST_LEDGER:-$ROOT/build/coverage-rust/ledger.json}"

python3 "$ROOT/scripts/coverage_rust_ledger.py" \
  --root "$ROOT" \
  --ledger "$LEDGER" \
  --core-floor "${COVERAGE_FAIL_UNDER_RUST:-85}" \
  --python-floor "${COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER:-80}" \
  --node-floor "${COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER:-80}"
