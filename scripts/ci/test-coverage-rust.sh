#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNNER="$ROOT/scripts/coverage-rust.sh"

bash -n "$RUNNER"

grep -Fq 'CORE_COVERAGE_ARGS="${COVERAGE_RUST_ARGS} --release"' "$RUNNER"
grep -Fq 'cargo llvm-cov ${CORE_COVERAGE_ARGS} --no-report' "$RUNNER"
grep -Fq 'if [[ "$HTML_REPORT" == "1" ]]; then' "$RUNNER"
grep -Fq 'xargs -0 -n 1 -P "$PYTHON_BINDING_WORKERS"' "$RUNNER"
grep -Fq 'run_python_acceptance' "$RUNNER"
grep -Fq 'run_node_acceptance' "$RUNNER"
grep -Fq 'wait "$python_pid"' "$RUNNER"
grep -Fq 'wait "$node_pid"' "$RUNNER"
grep -Fq 'stamp_matches()' "$RUNNER"
grep -Fq 'write_stamp()' "$RUNNER"

if rg -n --glob '*.{yml,yaml}' 'coverage-rust|make coverage|make pre-push' "$ROOT/.github"; then
  echo "Rust coverage must remain outside PR CI" >&2
  exit 1
fi

echo "rust coverage runner policy: ok"
