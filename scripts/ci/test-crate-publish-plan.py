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
    "gf-core": set(),
    "gf-ast": {"gf-core"},
    "gf-api": {"gf-core", "gf-ast"},
    "gf-bindings-py": {"gf-api"},
    "gf-cli": {"gf-api"},
}
order = mod.topological_publish_order(synthetic)
assert order == ["gf-core", "gf-ast", "gf-api"], order
assert "gf-bindings-py" not in order
assert "gf-cli" not in order

cycle = {"a": {"b"}, "b": {"a"}, "gf-bindings-node": {"a"}}
# Exclude bindings; cycle remains among a/b — but a/b are not gf-* workspace
# names. Use gf-* names:
cycle = {"gf-a": {"gf-b"}, "gf-b": {"gf-a"}}
try:
    mod.topological_publish_order(cycle)
    raise AssertionError("expected cycle to fail")
except SystemExit as exc:
    assert "cycle" in str(exc).lower()

listed = run("list")
assert listed.returncode == 0, listed.stderr
names = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
assert names[0] == "gf-core", names
assert names[-1] == "gf-api", names
assert "gf-bindings-py" not in names
assert "gf-cli" not in names
# Relative order samples
assert names.index("gf-ast") < names.index("gf-ir")
assert names.index("gf-storage") < names.index("gf-api")

checked = run("check")
assert checked.returncode == 1, checked.stdout
assert "gf-core" in checked.stderr
assert "name conflict" in checked.stderr
assert "version=" in checked.stderr or "missing version" in checked.stderr

dry = run("dry-run-commands")
assert dry.returncode == 1, dry.stdout
assert "name conflict" in dry.stderr.lower() or "Refusing" in dry.stderr

print("crate-publish-plan tests passed")
