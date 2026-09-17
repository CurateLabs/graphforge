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


def load_named_module(path: Path):
    """Import a scripts/ module by path (names contain hyphens)."""
    name = path.stem.replace("-", "_")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
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
    "graphforge-observability": set(),
    "graphforge-api": {
        "graphforge-core",
        "graphforge-ast",
        "graphforge-observability",
    },
    "graphforge-bindings-py": {"graphforge-api"},
    "graphforge-cli": {"graphforge-api"},
}
order = mod.topological_publish_order(synthetic)
assert order == [
    "graphforge-core",
    "graphforge-observability",
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
assert "graphforge-discovery" in names
assert "graphforge-value" in names
# Relative order samples
assert names.index("graphforge-core") < names.index("graphforge-portable-oci")
assert names.index("graphforge-portable-oci") < names.index("graphforge-api")
assert names.index("graphforge-core") < names.index("graphforge-value")
for consumer in ("graphforge-ontology", "graphforge-ir", "graphforge-storage"):
    assert names.index("graphforge-value") < names.index(consumer)
assert names.index("graphforge-ast") < names.index("graphforge-ir")
assert names.index("graphforge-filesystem") < names.index("graphforge-storage")
assert names.index("graphforge-storage") < names.index("graphforge-api")
assert names.index("graphforge-observability") < names.index("graphforge-api")

checked = run("check")
assert checked.returncode == 0, checked.stderr
assert "20 crates" in checked.stdout

dry = run("dry-run-commands")
assert dry.returncode == 0, dry.stderr
commands = [line for line in dry.stdout.splitlines() if line]
assert len(commands) == 20, commands
assert any(command.startswith("cargo publish -p graphforge-value ") for command in commands)
assert any(command.startswith("cargo publish -p graphforge-observability ") for command in commands)
assert commands[0].startswith("cargo publish -p graphforge-core ")
assert commands[-1].startswith("cargo publish -p graphforge-cli ")

# --- Release inventories must not diverge from the publish plan (#1373) -------
# Every hand-maintained crates.io inventory is compared against the plan here so
# that adding a crate cannot silently leave a release gate behind.
INVENTORIES = (
    (ROOT / "scripts" / "ci" / "release_candidate_manifest.py", "CRATES"),
    (ROOT / "scripts" / "ci" / "clean-env-verify.py", "DEFAULT_CRATES"),
    (ROOT / "scripts" / "verify_package_licenses.py", "CARGO_PUBLISH_CRATES"),
)

for script, attribute in INVENTORIES:
    inventory_module = load_named_module(script)
    inventory = tuple(getattr(inventory_module, attribute))
    assert inventory == tuple(names), (
        f"{script.relative_to(ROOT)}:{attribute} diverges from "
        f"crate-publish-plan.py list; expected {list(names)}, found {list(inventory)}"
    )

license_check = load_named_module(ROOT / "scripts" / "license_check.py")
license_dirs = {path.name for path in license_check.CARGO_PACKAGE_DIRS}
missing_license_dirs = sorted(set(names) - license_dirs)
assert not missing_license_dirs, (
    f"scripts/license_check.py:CARGO_PACKAGE_DIRS lacks publishable crates: {missing_license_dirs}"
)

missing_notice = sorted(name for name in names if not (ROOT / "crates" / name / "NOTICE").is_file())
assert not missing_notice, f"publishable crates without a NOTICE file: {missing_notice}"


print("crate-publish-plan tests passed")
