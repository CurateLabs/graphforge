#!/usr/bin/env bash
# Measure core Rust plus the Rust code executed by real Python and Node acceptance.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${COVERAGE_RUST_DIR:-build/coverage-rust}"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
PROFILE_DIR="$OUT_DIR/profiles"
CORE_LCOV="$OUT_DIR/core.lcov.info"
PYTHON_LCOV="$OUT_DIR/python-adapter.lcov.info"
NODE_LCOV="$OUT_DIR/node-adapter.lcov.info"
WORKSPACE_LCOV="$OUT_DIR/lcov.info"
LEDGER="$OUT_DIR/ledger.json"
EVIDENCE="$OUT_DIR/evidence.json"
SUMMARY="$OUT_DIR/summary.txt"
COVERAGE_RUST_ARGS="${COVERAGE_RUST_ARGS:---workspace}"
PYTHON_FLOOR="${COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER:-80}"
NODE_FLOOR="${COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER:-80}"
CORE_FLOOR="${COVERAGE_FAIL_UNDER_RUST:-85}"

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "Rust coverage evidence error: cargo-llvm-cov is not installed" >&2
  exit 1
fi
if ! rustup component list --installed 2>/dev/null | grep -Eq '^llvm-tools'; then
  echo "Rust coverage evidence error: rustup llvm-tools is not installed" >&2
  exit 1
fi
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$OUT_DIR/native-target}"
CORE_TARGET_DIR="$CARGO_TARGET_DIR/llvm-cov-target"

rm -rf "$PROFILE_DIR" "$OUT_DIR/core-html"
rm -f "$CORE_LCOV" "$PYTHON_LCOV" "$NODE_LCOV" "$WORKSPACE_LCOV" \
  "$LEDGER" "$EVIDENCE" "$SUMMARY" "$OUT_DIR/python.profdata" "$OUT_DIR/node.profdata"
mkdir -p "$PROFILE_DIR"

echo "━━━ Core Rust coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
# shellcheck disable=SC2086
cargo llvm-cov clean ${COVERAGE_RUST_ARGS}
# shellcheck disable=SC2086
cargo llvm-cov ${COVERAGE_RUST_ARGS} --no-report
cargo llvm-cov report --lcov --output-path "$CORE_LCOV"
cargo llvm-cov report --html --output-dir "$OUT_DIR/core-html"

# Export the exact instrumentation environment used by cargo-llvm-cov. Native
# artifacts are built once, sequentially, and then reused by all acceptance.
eval "$(cargo llvm-cov show-env --sh)"

echo "━━━ Instrumented native artifacts ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
pnpm --filter @curatelabs/graphforge exec napi build --platform --release

find_one() {
  local description="$1"
  shift
  local matches=()
  while IFS= read -r match; do
    matches+=("$match")
  done < <(find "$@" -print | sort)
  if [[ "${#matches[@]}" -ne 1 ]]; then
    echo "Rust coverage evidence error: expected exactly one $description, got ${#matches[@]}" >&2
    printf '%s\n' "${matches[@]}" >&2
    exit 1
  fi
  printf '%s\n' "${matches[0]}"
}

PYTHON_OBJECT="$(find_one 'Python adapter object' "$CARGO_TARGET_DIR" -type f ! -path '*/deps/*' \( -name 'libgraphforge_bindings_py.so' -o -name 'libgraphforge_bindings_py.dylib' -o -name 'graphforge_bindings_py.dll' \))"
NODE_OBJECT="$(find_one 'Node adapter object' "$CARGO_TARGET_DIR" -type f ! -path '*/deps/*' \( -name 'libgraphforge_bindings_node.so' -o -name 'libgraphforge_bindings_node.dylib' -o -name 'graphforge_bindings_node.dll' \))"
PYTHON_RUNTIME="$(uv run --no-sync python -c 'from graphforge import _graphforge_rs; print(_graphforge_rs.__file__)')"
NODE_RUNTIME="$(find_one 'Node runtime addon' crates/graphforge-bindings-node -maxdepth 1 -type f -name 'graphforge.*.node')"
for artifact in "$PYTHON_OBJECT" "$NODE_OBJECT" "$PYTHON_RUNTIME" "$NODE_RUNTIME"; do
  if [[ -z "$artifact" || ! -s "$artifact" ]]; then
    echo "Rust coverage evidence error: expected instrumented native artifact is missing" >&2
    exit 1
  fi
done

hash_file() {
  python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$1"
}
python_hash="$(hash_file "$PYTHON_OBJECT")"
node_hash="$(hash_file "$NODE_OBJECT")"
if [[ "$(hash_file "$PYTHON_RUNTIME")" != "$python_hash" ]]; then
  echo "Rust coverage evidence error: Python did not import the measured artifact" >&2
  exit 1
fi
if [[ "$(hash_file "$NODE_RUNTIME")" != "$node_hash" ]]; then
  echo "Rust coverage evidence error: Node did not load the measured artifact" >&2
  exit 1
fi

echo "━━━ Python native acceptance ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
export LLVM_PROFILE_FILE="$PROFILE_DIR/python-%p-%10m.profraw"
for test_file in crates/graphforge-bindings-py/tests/*.py; do
  CARGO_TARGET_DIR="$CORE_TARGET_DIR" uv run --no-sync python "$test_file"
done
# The publication dry-run invokes pnpm publish hooks that replace the measured
# addon; #365 tracks separating that integration check from adapter acceptance.
uv run --no-sync pytest tests/unit tests/integration tests/features \
  --ignore=tests/unit/test_publish_dry_run.py \
  -n "${PYTEST_WORKERS:-4}" --tb=short

echo "━━━ Node native acceptance ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
export LLVM_PROFILE_FILE="$PROFILE_DIR/node-%p-%10m.profraw"
pnpm --filter @curatelabs/graphforge test:smoke
pnpm --filter @curatelabs/graphforge test
(
  cd tests/features/node
  node node_modules/@cucumber/cucumber/bin/cucumber.js \
    "../api/**/*.feature" \
    --require-module ts-node/register \
    --require "step_definitions/**/*.ts" \
    --tags "not @excluded-api-bdd and not @excluded-node-api-bdd" \
    --format summary
)

TOOLS_DIR="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin"
shopt -s nullglob
python_profiles=("$PROFILE_DIR"/python-*.profraw)
node_profiles=("$PROFILE_DIR"/node-*.profraw)
shopt -u nullglob
if [[ "${#python_profiles[@]}" -eq 0 || "${#node_profiles[@]}" -eq 0 ]]; then
  echo "Rust coverage evidence error: an acceptance suite produced no instrumented profiles" >&2
  exit 1
fi
"$TOOLS_DIR/llvm-profdata" merge -sparse "${python_profiles[@]}" -o "$OUT_DIR/python.profdata"
"$TOOLS_DIR/llvm-profdata" merge -sparse "${node_profiles[@]}" -o "$OUT_DIR/node.profdata"
"$TOOLS_DIR/llvm-cov" export "$PYTHON_OBJECT" \
  -instr-profile="$OUT_DIR/python.profdata" -format=lcov > "$PYTHON_LCOV"
"$TOOLS_DIR/llvm-cov" export "$NODE_OBJECT" \
  -instr-profile="$OUT_DIR/node.profdata" -format=lcov > "$NODE_LCOV"

export GF_COVERAGE_ROOT="$ROOT"
GF_COVERAGE_SOURCE_SHA="$(git rev-parse HEAD)"
GF_COVERAGE_RUSTC="$(rustc --version)"
GF_COVERAGE_LLVM_COV="$(cargo llvm-cov --version)"
export GF_COVERAGE_SOURCE_SHA GF_COVERAGE_RUSTC GF_COVERAGE_LLVM_COV
export GF_COVERAGE_PYTHON_OBJECT="$PYTHON_OBJECT"
export GF_COVERAGE_PYTHON_RUNTIME="$PYTHON_RUNTIME"
export GF_COVERAGE_PYTHON_HASH="$python_hash"
export GF_COVERAGE_NODE_OBJECT="$NODE_OBJECT"
export GF_COVERAGE_NODE_RUNTIME="$NODE_RUNTIME"
export GF_COVERAGE_NODE_HASH="$node_hash"
export GF_COVERAGE_PROFILE_DIR="$PROFILE_DIR"
export GF_COVERAGE_EVIDENCE="$EVIDENCE"
python3 - <<'PY'
import glob
import json
import os
from pathlib import Path

profiles = Path(os.environ["GF_COVERAGE_PROFILE_DIR"])
data = {
    "source_sha": os.environ["GF_COVERAGE_SOURCE_SHA"],
    "rustc": os.environ["GF_COVERAGE_RUSTC"],
    "cargo_llvm_cov": os.environ["GF_COVERAGE_LLVM_COV"],
    "surfaces": {
        "python_adapter": {
            "artifact": os.environ["GF_COVERAGE_PYTHON_OBJECT"],
            "runtime_artifact": os.environ["GF_COVERAGE_PYTHON_RUNTIME"],
            "artifact_sha256": os.environ["GF_COVERAGE_PYTHON_HASH"],
            "profiles": sorted(glob.glob(str(profiles / "python-*.profraw"))),
        },
        "node_adapter": {
            "artifact": os.environ["GF_COVERAGE_NODE_OBJECT"],
            "runtime_artifact": os.environ["GF_COVERAGE_NODE_RUNTIME"],
            "artifact_sha256": os.environ["GF_COVERAGE_NODE_HASH"],
            "profiles": sorted(glob.glob(str(profiles / "node-*.profraw"))),
        },
    },
}
Path(os.environ["GF_COVERAGE_EVIDENCE"]).write_text(
    json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

echo "━━━ Rust coverage ledger ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 scripts/coverage_rust_ledger.py \
  --root "$ROOT" \
  --ledger "$LEDGER" \
  --build \
  --core-lcov "$CORE_LCOV" \
  --python-lcov "$PYTHON_LCOV" \
  --node-lcov "$NODE_LCOV" \
  --workspace-lcov "$WORKSPACE_LCOV" \
  --evidence "$EVIDENCE" \
  --core-floor "$CORE_FLOOR" \
  --python-floor "$PYTHON_FLOOR" \
  --node-floor "$NODE_FLOOR" | tee "$SUMMARY"

echo "✅ Rust coverage ledger ready: $LEDGER"
