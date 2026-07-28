#!/usr/bin/env bash
# Run workspace Rust tests under cargo-llvm-cov and emit term + lcov + HTML.
# Prerequisites:
#   cargo install cargo-llvm-cov
#   rustup component add llvm-tools-preview
#
# Outputs (gitignored via build/):
#   build/coverage-rust/lcov.info
#   build/coverage-rust/html/index.html
#   build/coverage-rust/summary.txt
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${COVERAGE_RUST_DIR:-build/coverage-rust}"
LCOV_PATH="${OUT_DIR}/lcov.info"
# cargo-llvm-cov appends /html under --output-dir
HTML_DIR="${OUT_DIR}/html"
SUMMARY_PATH="${OUT_DIR}/summary.txt"
# Default: whole workspace. Override for focused runs, e.g. COVERAGE_RUST_ARGS='-p gf-api'
COVERAGE_RUST_ARGS="${COVERAGE_RUST_ARGS:---workspace}"

if ! command -v cargo-llvm-cov >/dev/null 2>&1 && ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "❌ cargo-llvm-cov not found."
  echo "   Install with:  cargo install cargo-llvm-cov"
  echo "   Also require:  rustup component add llvm-tools-preview"
  exit 1
fi

if ! rustup component list --installed 2>/dev/null | grep -Eq '^llvm-tools'; then
  echo "❌ rustup llvm-tools component not installed."
  echo "   Install with:  rustup component add llvm-tools-preview"
  exit 1
fi

mkdir -p "${OUT_DIR}"
rm -rf "${HTML_DIR}" "${LCOV_PATH}" "${SUMMARY_PATH}"

echo "━━━ Rust coverage (cargo llvm-cov ${COVERAGE_RUST_ARGS}) ━━━━━━━━━━━━━━━"
echo "   CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-<default target/>}"
echo "   output: ${OUT_DIR}/"
echo

# shellcheck disable=SC2086
cargo llvm-cov clean ${COVERAGE_RUST_ARGS}

# Instrument + run once; generate reports from the collected data (lcov/html are mutually exclusive flags).
# shellcheck disable=SC2086
cargo llvm-cov ${COVERAGE_RUST_ARGS} --no-report

echo
echo "━━━ Generating reports ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cargo llvm-cov report 2>&1 | tee "${SUMMARY_PATH}"
cargo llvm-cov report --lcov --output-path "${LCOV_PATH}"
# --output-dir receives the parent; llvm-cov writes <dir>/html/index.html
cargo llvm-cov report --html --output-dir "${OUT_DIR}"

if [[ ! -s "${LCOV_PATH}" ]]; then
  echo "❌ Rust coverage produced empty or missing lcov at ${LCOV_PATH}"
  exit 1
fi

if ! grep -q '^SF:' "${LCOV_PATH}"; then
  echo "❌ Rust coverage lcov has no source file records (SF:) at ${LCOV_PATH}"
  exit 1
fi

if [[ ! -f "${HTML_DIR}/index.html" ]]; then
  echo "❌ Rust coverage HTML report missing at ${HTML_DIR}/index.html"
  exit 1
fi

if [[ ! -s "${SUMMARY_PATH}" ]]; then
  echo "❌ Rust coverage term summary is empty at ${SUMMARY_PATH}"
  exit 1
fi

if ! grep -q '^TOTAL' "${SUMMARY_PATH}"; then
  echo "❌ Rust coverage term summary has no TOTAL row at ${SUMMARY_PATH}"
  exit 1
fi

lines=$(grep -c '^SF:' "${LCOV_PATH}" || true)
bytes=$(wc -c < "${LCOV_PATH}" | tr -d ' ')
echo
echo "✅ Rust coverage report ready"
echo "   lcov:    ${LCOV_PATH} (${bytes} bytes, ${lines} source files)"
echo "   html:    ${HTML_DIR}/index.html"
echo "   summary: ${SUMMARY_PATH}"

# Optional fail-under (coordinator aggregation). Empty/0 skips the check here;
# `make check-coverage-rust` always enforces COVERAGE_FAIL_UNDER_RUST.
if [[ -n "${COVERAGE_FAIL_UNDER_RUST:-}" && "${COVERAGE_FAIL_UNDER_RUST}" != "0" ]]; then
  COVERAGE_RUST_SUMMARY="${SUMMARY_PATH}" \
    COVERAGE_FAIL_UNDER_RUST="${COVERAGE_FAIL_UNDER_RUST}" \
    bash "${ROOT}/scripts/check-coverage-rust.sh"
fi
