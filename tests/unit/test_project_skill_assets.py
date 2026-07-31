"""Distribution parity checks for project-local GraphForge skills."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "project-skills"
PYTHON_COPY = ROOT / "crates" / "gf-bindings-py" / "python" / "graphforge" / "_project_skills"
NPM_COPY = ROOT / "packages" / "cli" / "project-skills"


def test_generated_distribution_assets_are_current() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "sync_project_skills.py")],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_manifest_names_host_discoverable_skills() -> None:
    manifest = json.loads((SOURCE / "manifest.json").read_text())
    assert manifest["schema_version"] == 1
    assert manifest["bundle_version"] == 1
    assert manifest["skills"] == [
        "graphforge-bootstrap",
        "graphforge-build-knowledge",
    ]
    for skill in manifest["skills"]:
        assert (SOURCE / skill / "SKILL.md").is_file()


def test_python_and_npm_payloads_are_exact_copies() -> None:
    canonical = {
        path.relative_to(SOURCE): path.read_bytes() for path in SOURCE.rglob("*") if path.is_file()
    }
    for destination in (PYTHON_COPY, NPM_COPY):
        packaged = {
            path.relative_to(destination): path.read_bytes()
            for path in destination.rglob("*")
            if path.is_file()
        }
        assert packaged == canonical


def test_sync_script_is_importable_without_mutating_assets() -> None:
    spec = importlib.util.spec_from_file_location(
        "sync_project_skills", ROOT / "scripts" / "sync_project_skills.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.check() == []
