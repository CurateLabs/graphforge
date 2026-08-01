"""Tests for multi-surface release version alignment."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "set_release_version.py"
SPEC = importlib.util.spec_from_file_location("set_release_version", SCRIPT)
assert SPEC and SPEC.loader
set_release_version = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(set_release_version)


def test_parse_release_and_dev() -> None:
    assert set_release_version.parse_base("0.5.0") == ("0.5.0", False)
    assert set_release_version.parse_base("0.5.0-dev") == ("0.5.0", True)
    assert set_release_version.parse_base("0.5.0.dev0") == ("0.5.0", True)
    assert set_release_version.parse_base("0.5.0-dev.0") == ("0.5.0", True)


def test_expected_mapping() -> None:
    release = set_release_version.expected_for("0.5.0", dev=False)
    assert release == {
        "cargo": "0.5.0",
        "python": "0.5.0",
        "node": "0.5.0",
        "cli": "0.5.0",
        "skills": "0.5.0",
    }
    dev = set_release_version.expected_for("0.5.0", dev=True)
    assert dev["cargo"] == "0.5.0-dev"
    assert dev["python"] == "0.5.0.dev0"
    assert dev["node"] == "0.5.0-dev.0"
    assert dev["cli"] == "0.5.0-dev.0"
    assert dev["skills"] == "0.5.0-dev.0"


def test_current_tree_is_aligned() -> None:
    assert len(set_release_version.cargo_lock_versions()) == 17
    assert set_release_version.check_aligned() == []
    compatibility = json.loads(
        set_release_version.SKILLS_COMPATIBILITY.read_text(encoding="utf-8")
    )
    current = set_release_version.read_current()
    assert compatibility["package_version"] == current["skills"]
    assert compatibility["graphforge_release"] == current["skills"]
    for path in set_release_version.native_npm_packages():
        meta = json.loads(path.read_text(encoding="utf-8"))
        assert meta["version"] == current["node"]


def test_dry_run_does_not_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cargo = tmp_path / "Cargo.toml"
    cargo.write_text('[workspace.package]\nversion = "0.5.0-dev"\n', encoding="utf-8")
    monkeypatch.setattr(set_release_version, "CARGO_TOML", cargo)
    before = cargo.read_text(encoding="utf-8")
    mapping = set_release_version.apply_version("0.5.0", dev=False, dry_run=True)
    assert mapping["cargo"] == "0.5.0"
    assert cargo.read_text(encoding="utf-8") == before
