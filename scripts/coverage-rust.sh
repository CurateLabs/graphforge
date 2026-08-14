#!/usr/bin/env bash
# Measure core Rust plus the Rust code executed by real Python and Node acceptance.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${COVERAGE_RUST_DIR:-build/coverage-rust}"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
PROFILE_DIR="$OUT_DIR/profiles"
STAMP_DIR="$OUT_DIR/stamps"
CORE_LCOV="$OUT_DIR/core.lcov.info"
CORE_HTML_INDEX="$OUT_DIR/core-html/index.html"
PYTHON_LCOV="$OUT_DIR/python-adapter.lcov.info"
NODE_LCOV="$OUT_DIR/node-adapter.lcov.info"
WORKSPACE_LCOV="$OUT_DIR/lcov.info"
LEDGER="$OUT_DIR/ledger.json"
EVIDENCE="$OUT_DIR/evidence.json"
SUMMARY="$OUT_DIR/summary.txt"
COVERAGE_TIMING="$OUT_DIR/timing.log"
COVERAGE_RUST_ARGS="${COVERAGE_RUST_ARGS:---workspace}"
CORE_COVERAGE_ARGS="${COVERAGE_RUST_ARGS} --release"
RESUME="${COVERAGE_RUST_RESUME:-0}"
HTML_REPORT="${COVERAGE_RUST_HTML:-0}"
PYTHON_BINDING_WORKERS="${PYTHON_BINDING_WORKERS:-4}"
PYTHON_FLOOR="${COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER:-80}"
NODE_FLOOR="${COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER:-80}"
CORE_FLOOR="${COVERAGE_FAIL_UNDER_RUST:-95}"
CRATE_FLOOR="${COVERAGE_FAIL_UNDER_RUST_CRATE:-80}"
PATCH_FLOOR="${COVERAGE_FAIL_UNDER_RUST_PATCH:-90}"
PATCH_BASE="${COVERAGE_RUST_PATCH_BASE:-origin/main}"
SOURCE_SHA="$(git rev-parse HEAD)"

if [[ "$RESUME" != "0" && "$RESUME" != "1" ]]; then
  echo "Rust coverage evidence error: COVERAGE_RUST_RESUME must be 0 or 1" >&2
  exit 1
fi
if [[ "$HTML_REPORT" != "0" && "$HTML_REPORT" != "1" ]]; then
  echo "Rust coverage evidence error: COVERAGE_RUST_HTML must be 0 or 1" >&2
  exit 1
fi

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

if [[ "$RESUME" != "1" ]]; then
  rm -rf "$PROFILE_DIR" "$STAMP_DIR" "$OUT_DIR/core-html"
  rm -f "$CORE_LCOV" "$PYTHON_LCOV" "$NODE_LCOV" "$WORKSPACE_LCOV" \
    "$LEDGER" "$EVIDENCE" "$SUMMARY" "$COVERAGE_TIMING" \
    "$OUT_DIR/python.profdata" "$OUT_DIR/node.profdata"
fi
mkdir -p "$PROFILE_DIR" "$STAMP_DIR"

hash_file() {
  python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$1"
}

stamp_payload() {
  local phase="$1"
  local input="$2"
  shift 2
  printf 'source_sha=%s\nphase=%s\ninput=%s\n' "$SOURCE_SHA" "$phase" "$input"
  for output in "$@"; do
    printf 'output_sha256=%s path=%s\n' "$(hash_file "$output")" "$output"
  done
}

stamp_matches() {
  local phase="$1"
  local input="$2"
  shift 2
  local stamp="$STAMP_DIR/$phase.stamp"
  [[ "$RESUME" == "1" && -f "$stamp" ]] || return 1
  for required in "$@"; do
    [[ -s "$required" ]] || return 1
  done
  cmp -s <(stamp_payload "$phase" "$input" "$@") "$stamp"
}

write_stamp() {
  local phase="$1"
  local input="$2"
  shift 2
  local stamp="$STAMP_DIR/$phase.stamp"
  local temporary="$stamp.tmp.$$"
  stamp_payload "$phase" "$input" "$@" >"$temporary"
  mv "$temporary" "$stamp"
}

record_timing() {
  local phase="$1"
  local started="$2"
  printf 'source_sha=%s phase=%s seconds=%s\n' \
    "$SOURCE_SHA" "$phase" "$((SECONDS - started))" >>"$COVERAGE_TIMING"
}

echo "━━━ Core Rust coverage ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
core_input="${CORE_COVERAGE_ARGS} html=${HTML_REPORT}"
core_outputs=("$CORE_LCOV")
if [[ "$HTML_REPORT" == "1" ]]; then
  core_outputs+=("$CORE_HTML_INDEX")
fi
if stamp_matches core "$core_input" "${core_outputs[@]}"; then
  echo "Resuming verified core coverage from $CORE_LCOV"
else
  phase_started=$SECONDS
  # shellcheck disable=SC2086
  cargo llvm-cov clean ${COVERAGE_RUST_ARGS}
  # shellcheck disable=SC2086
  cargo llvm-cov ${CORE_COVERAGE_ARGS} --no-report
  cargo llvm-cov report --release --lcov --output-path "$CORE_LCOV"
  if [[ "$HTML_REPORT" == "1" ]]; then
    cargo llvm-cov report --release --html --output-dir "$OUT_DIR/core-html"
  fi
  write_stamp core "$core_input" "${core_outputs[@]}"
  record_timing core "$phase_started"
fi

# Export the exact instrumentation environment used by cargo-llvm-cov. Native
# artifacts are built once, sequentially, and then reused by all acceptance.
COVERAGE_ENV="$(cargo llvm-cov show-env --sh)"
eval "$COVERAGE_ENV"

echo "━━━ Instrumented native artifacts ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
native_input="$(printf '%s\n' "$CORE_COVERAGE_ARGS" "$COVERAGE_ENV" | shasum -a 256)"
if stamp_matches native-artifacts "$native_input"; then
  echo "Resuming verified native artifact build"
else
  phase_started=$SECONDS
  uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
  pnpm --filter @curatelabs/graphforge exec napi build --platform --release
  write_stamp native-artifacts "$native_input"
  record_timing native-artifacts "$phase_started"
fi

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

run_python_acceptance() {
  export LLVM_PROFILE_FILE="$PROFILE_DIR/python-%p-%10m.profraw"
  shopt -s nullglob
  local test_files=(crates/graphforge-bindings-py/tests/*.py)
  if [[ "${#test_files[@]}" -eq 0 ]]; then
    echo "Rust coverage evidence error: no Python binding acceptance tests found" >&2
    return 1
  fi
  printf '%s\0' "${test_files[@]}" | xargs -0 -n 1 -P "$PYTHON_BINDING_WORKERS" \
    env CARGO_TARGET_DIR="$CORE_TARGET_DIR" uv run --no-sync python
  uv run --no-sync pytest tests/unit tests/integration tests/features \
    -n "${PYTEST_WORKERS:-4}" --tb=short
}

run_node_acceptance() {
  export LLVM_PROFILE_FILE="$PROFILE_DIR/node-%p-%10m.profraw"
  pnpm --filter @curatelabs/graphforge test:smoke
  pnpm --filter @curatelabs/graphforge test
  (
    cd tests/features/node
    node node_modules/@cucumber/cucumber/bin/cucumber.js \
      "../api/**/*.feature" \
      --require-module tsx/cjs \
      --require "step_definitions/**/*.ts" \
      --tags "not @excluded-api-bdd and not @excluded-node-api-bdd" \
      --format summary
  )
}

python_input="artifact_sha256=$python_hash"
node_input="artifact_sha256=$node_hash"
echo "━━━ Parallel native acceptance ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if stamp_matches python-acceptance "$python_input" "${PROFILE_DIR}"/python-*.profraw; then
  echo "Resuming verified Python native acceptance"
else
  (
    phase_started=$SECONDS
    run_python_acceptance
    write_stamp python-acceptance "$python_input" "${PROFILE_DIR}"/python-*.profraw
    record_timing python-acceptance "$phase_started"
  ) &
  python_pid=$!
fi
if stamp_matches node-acceptance "$node_input" "${PROFILE_DIR}"/node-*.profraw; then
  echo "Resuming verified Node native acceptance"
else
  (
    phase_started=$SECONDS
    run_node_acceptance
    write_stamp node-acceptance "$node_input" "${PROFILE_DIR}"/node-*.profraw
    record_timing node-acceptance "$phase_started"
  ) &
  node_pid=$!
fi

acceptance_status=0
if [[ -n "${python_pid:-}" ]] && ! wait "$python_pid"; then
  acceptance_status=1
fi
if [[ -n "${node_pid:-}" ]] && ! wait "$node_pid"; then
  acceptance_status=1
fi
if [[ "$acceptance_status" -ne 0 ]]; then
  echo "Rust coverage evidence error: native acceptance failed" >&2
  exit 1
fi

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
  --crate-floor "$CRATE_FLOOR" \
  --patch-floor "$PATCH_FLOOR" \
  --patch-base "$PATCH_BASE" \
  --python-floor "$PYTHON_FLOOR" \
  --node-floor "$NODE_FLOOR" | tee "$SUMMARY"

echo "✅ Rust coverage ledger ready: $LEDGER"
