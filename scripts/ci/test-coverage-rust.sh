#!/usr/bin/env bash
# Policy: coverage-rust Node acceptance uses tsx, and Cucumber loader flags stay
# aligned with tests/features/node package dependencies (#722).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNNER="$ROOT/scripts/coverage-rust.sh"
CUCUMBER_JS="$ROOT/tests/features/node/cucumber.js"
NODE_PACKAGE="$ROOT/tests/features/node/package.json"
WORKFLOW="$ROOT/.github/workflows/test.yml"

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

# Node acceptance must load TypeScript via tsx, not ts-node.
grep -Fq -- 'tsx/cjs' "$RUNNER"
if grep -Fq -- 'ts-node' "$RUNNER"; then
  echo "coverage-rust.sh must not reference ts-node" >&2
  exit 1
fi

# Full Rust coverage measurement stays outside PR CI; the policy test itself may run.
if matches="$(rg -n --glob '*.{yml,yaml}' \
  -e 'make coverage-rust\b' \
  -e 'make coverage\b' \
  -e 'scripts/coverage-rust\.sh' \
  -e 'make pre-push\b' \
  "$ROOT/.github")"; then
  printf '%s\n' "$matches"
  echo "Rust coverage must remain outside PR CI" >&2
  exit 1
fi

# Package dependencies and Cucumber loader flags must not drift across entry points.
python3 - "$ROOT" "$RUNNER" "$CUCUMBER_JS" "$NODE_PACKAGE" "$WORKFLOW" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

root, runner_path, cucumber_path, package_path, workflow_path = map(Path, sys.argv[1:])

SUPPORTED = "tsx/cjs"
FORBIDDEN_PACKAGE = "ts-node"


def package_name(loader: str) -> str:
    return loader.split("/", 1)[0]


def flag_loaders(text: str, *, source: str) -> list[str]:
    loaders = re.findall(r"--require-module\s+(\S+)", text)
    if not loaders:
        raise SystemExit(f"{source}: expected at least one --require-module")
    return loaders


def cucumber_loaders(text: str) -> list[str]:
    match = re.search(
        r"requireModule\s*:\s*\[(.*?)\]",
        text,
        flags=re.DOTALL,
    )
    if not match:
        raise SystemExit(f"{cucumber_path}: missing requireModule array")
    loaders = re.findall(r'["\']([^"\']+)["\']', match.group(1))
    if not loaders:
        raise SystemExit(f"{cucumber_path}: requireModule must name a loader")
    return loaders


runner = runner_path.read_text(encoding="utf-8")
cucumber = cucumber_path.read_text(encoding="utf-8")
workflow = workflow_path.read_text(encoding="utf-8")
package = json.loads(package_path.read_text(encoding="utf-8"))
deps = {
    **package.get("dependencies", {}),
    **package.get("devDependencies", {}),
}

entry_loaders: dict[str, list[str]] = {
    str(runner_path.relative_to(root)): flag_loaders(runner, source=str(runner_path)),
    str(cucumber_path.relative_to(root)): cucumber_loaders(cucumber),
    str(workflow_path.relative_to(root)): flag_loaders(workflow, source=str(workflow_path)),
}

for source, loaders in entry_loaders.items():
    for loader in loaders:
        if loader != SUPPORTED:
            raise SystemExit(
                f"{source}: unsupported Cucumber loader {loader!r}; expected {SUPPORTED!r}"
            )
        name = package_name(loader)
        if name not in deps:
            raise SystemExit(
                f"{source}: loader {loader!r} requires package {name!r} "
                f"in {package_path.relative_to(root)} dependencies/devDependencies"
            )

if FORBIDDEN_PACKAGE in deps:
    raise SystemExit(
        f"{package_path.relative_to(root)} must not declare {FORBIDDEN_PACKAGE}"
    )

runner_loader = entry_loaders[str(runner_path.relative_to(root))][0]
cucumber_loader = entry_loaders[str(cucumber_path.relative_to(root))][0]
if runner_loader != cucumber_loader:
    raise SystemExit(
        "coverage-rust.sh --require-module and cucumber.js requireModule drifted: "
        f"{runner_loader!r} vs {cucumber_loader!r}"
    )

print("cucumber loader/package drift policy: ok")
PY

echo "rust coverage runner policy: ok"
