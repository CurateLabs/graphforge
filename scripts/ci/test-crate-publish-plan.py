#!/usr/bin/env python3
"""Tests for scripts/ci/crate-publish-plan.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("crate-publish-plan.py")


def load_module():
    spec = importlib.util.spec_from_file_location("crate_publish_plan", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


mod = load_module()

# Synthetic topo: leaf libraries only.
synthetic = {
    "graphforge-core": set(),
    "graphforge-ast": {"graphforge-core"},
    "graphforge-api": {"graphforge-core", "graphforge-ast"},
    "graphforge-bindings-py": {"graphforge-api"},
    "graphforge-cli": {"graphforge-api"},
}
order = mod.topological_publish_order(synthetic)
assert order == [
    "graphforge-core",
    "graphforge-ast",
    "graphforge-api",
    "graphforge-cli",
], order
assert "graphforge-bindings-py" not in order
assert "graphforge-cli" in order

cycle = {"a": {"b"}, "b": {"a"}, "graphforge-bindings-node": {"a"}}
# Exclude bindings; cycle remains among graphforge-a/graphforge-b.
cycle = {"graphforge-a": {"graphforge-b"}, "graphforge-b": {"graphforge-a"}}
try:
    mod.topological_publish_order(cycle)
    raise AssertionError("expected cycle to fail")
except SystemExit as exc:
    assert "cycle" in str(exc).lower()

listed = run("list")
assert listed.returncode == 0, listed.stderr
names = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
assert names[0] == "graphforge-core", names
assert names[-1] == "graphforge-cli", names
assert "graphforge-bindings-py" not in names
assert "graphforge-cli" in names
# Relative order samples
assert names.index("graphforge-ast") < names.index("graphforge-ir")
assert names.index("graphforge-filesystem") < names.index("graphforge-storage")
assert names.index("graphforge-storage") < names.index("graphforge-api")

checked = run("check")
assert checked.returncode == 0, checked.stderr
assert "16 crates" in checked.stdout

dry = run("dry-run-commands")
assert dry.returncode == 0, dry.stderr
commands = [line for line in dry.stdout.splitlines() if line]
assert len(commands) == 16, commands
assert commands[0].startswith("cargo publish -p graphforge-core ")
assert commands[-1].startswith("cargo publish -p graphforge-cli ")

print("crate-publish-plan tests passed")
