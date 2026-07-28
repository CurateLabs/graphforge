#!/usr/bin/env bash
# Parse cargo-llvm-cov term summary and enforce a line-coverage floor.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUMMARY="${COVERAGE_RUST_SUMMARY:-$ROOT/build/coverage-rust/summary.txt}"
MIN="${COVERAGE_FAIL_UNDER_RUST:-85}"

if [[ ! -f "${SUMMARY}" ]]; then
  echo "❌ Missing Rust coverage summary at ${SUMMARY}"
  exit 1
fi

# Match the TOTAL row from llvm-cov's text table, e.g.:
# TOTAL  1234  100  200  50  800  100  85.00%
line_pct="$(
  awk '
    /^TOTAL/ {
      for (i = NF; i >= 1; i--) {
        if ($i ~ /%$/) {
          gsub(/%/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "${SUMMARY}"
)"

if [[ -z "${line_pct}" ]]; then
  echo "❌ Could not parse TOTAL line coverage from ${SUMMARY}"
  exit 1
fi

echo "Rust lines: ${line_pct}%"
awk -v pct="${line_pct}" -v min="${MIN}" 'BEGIN {
  if (pct + 0 < min + 0) {
    printf("❌ Rust coverage below %s%% (got %s%%)\n", min, pct)
    exit 1
  }
  printf("✅ Rust coverage meets %s%% threshold\n", min)
}'
