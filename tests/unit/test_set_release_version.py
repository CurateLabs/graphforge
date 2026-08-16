"""Tests for multi-surface release version alignment."""

import importlib.util
import json
from pathlib import Path

import pytest
import tomllib

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
    lock_versions = set_release_version.cargo_lock_versions()
    assert len(lock_versions) == 18
    manifest_packages = {
        tomllib.loads(path.read_text(encoding="utf-8"))["package"]["name"]
        for path in set_release_version.crate_manifests()
    }
    assert set(lock_versions) == manifest_packages
    assert set_release_version.check_aligned() == []
    compatibility = json.loads(set_release_version.SKILLS_COMPATIBILITY.read_text(encoding="utf-8"))
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


def test_path_version_pins_match_root() -> None:
    """Every first-party path+version dep must match workspace.package.version."""
    current = set_release_version.read_current()
    pins = set_release_version.path_version_pins()
    assert pins, "expected first-party path+version dependency pins"
    assert all(version == current["cargo"] for _, _, version in pins)


def test_check_aligned_rejects_stale_path_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale path+version pins must fail --check before Binding RC rehearsal."""
    current = set_release_version.read_current()
    root = current["cargo"]
    assert root  # must be the live workspace root version
    pins = set_release_version.path_version_pins()
    assert pins
    path, dependency, _version = pins[0]
    stale = "0.0.0"
    monkeypatch.setattr(
        set_release_version,
        "path_version_pins",
        lambda: [(path, dependency, stale)],
    )
    errors = set_release_version.check_aligned()
    assert any(dependency in error and stale in error and root in error for error in errors), errors


def test_apply_version_rewrites_path_pins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cargo = tmp_path / "Cargo.toml"
    cargo.write_text('[workspace.package]\nversion = "0.5.0"\n', encoding="utf-8")
    lock = tmp_path / "Cargo.lock"
    lock.write_text('name = "graphforge-core"\nversion = "0.5.0"\n', encoding="utf-8")
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nversion = "0.5.0"\n', encoding="utf-8")
    crates = tmp_path / "crates"
    crates.mkdir()
    manifest = crates / "graphforge-api" / "Cargo.toml"
    manifest.parent.mkdir()
    manifest.write_text(
        '[dependencies]\ngraphforge-core = { version = "0.5.0", path = "../graphforge-core" }\n',
        encoding="utf-8",
    )
    for path, content in (
        (tmp_path / "node" / "package.json", '{"version":"0.5.0"}\n'),
        (tmp_path / "cli" / "package.json", '{"version":"0.5.0"}\n'),
        (
            tmp_path / "skills" / "package.json",
            '{"version":"0.5.0","graphforgeCompatibility":{"release":"0.5.0"}}\n',
        ),
        (
            tmp_path / "skills" / "compatibility.json",
            '{"package_version":"0.5.0","graphforge_release":"0.5.0"}\n',
        ),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    monkeypatch.setattr(set_release_version, "ROOT", tmp_path)
    monkeypatch.setattr(set_release_version, "CARGO_TOML", cargo)
    monkeypatch.setattr(set_release_version, "CARGO_LOCK", lock)
    monkeypatch.setattr(set_release_version, "PYPROJECT", pyproject)
    monkeypatch.setattr(set_release_version, "NODE_PACKAGE", tmp_path / "node" / "package.json")
    monkeypatch.setattr(set_release_version, "CLI_PACKAGE", tmp_path / "cli" / "package.json")
    monkeypatch.setattr(set_release_version, "SKILLS_PACKAGE", tmp_path / "skills" / "package.json")
    monkeypatch.setattr(
        set_release_version, "SKILLS_COMPATIBILITY", tmp_path / "skills" / "compatibility.json"
    )
    monkeypatch.setattr(set_release_version, "native_npm_packages", list)
    monkeypatch.setattr(
        set_release_version, "crate_manifests", lambda: sorted(crates.glob("*/Cargo.toml"))
    )

    set_release_version.apply_version("0.5.1", dev=False, dry_run=False)
    text = manifest.read_text(encoding="utf-8")
    assert 'version = "0.5.1"' in text
    assert 'version = "0.5.0"' not in text


def test_apply_version_rejects_missing_pins_without_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail closed before writing when no path+version pins are discovered."""
    cargo = tmp_path / "Cargo.toml"
    before = '[workspace.package]\nversion = "0.5.0"\n'
    cargo.write_text(before, encoding="utf-8")
    lock = tmp_path / "Cargo.lock"
    lock.write_text('name = "graphforge-core"\nversion = "0.5.0"\n', encoding="utf-8")
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nversion = "0.5.0"\n', encoding="utf-8")
    for path, content in (
        (tmp_path / "node" / "package.json", '{"version":"0.5.0"}\n'),
        (tmp_path / "cli" / "package.json", '{"version":"0.5.0"}\n'),
        (
            tmp_path / "skills" / "package.json",
            '{"version":"0.5.0","graphforgeCompatibility":{"release":"0.5.0"}}\n',
        ),
        (
            tmp_path / "skills" / "compatibility.json",
            '{"package_version":"0.5.0","graphforge_release":"0.5.0"}\n',
        ),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    monkeypatch.setattr(set_release_version, "ROOT", tmp_path)
    monkeypatch.setattr(set_release_version, "CARGO_TOML", cargo)
    monkeypatch.setattr(set_release_version, "CARGO_LOCK", lock)
    monkeypatch.setattr(set_release_version, "PYPROJECT", pyproject)
    monkeypatch.setattr(set_release_version, "NODE_PACKAGE", tmp_path / "node" / "package.json")
    monkeypatch.setattr(set_release_version, "CLI_PACKAGE", tmp_path / "cli" / "package.json")
    monkeypatch.setattr(set_release_version, "SKILLS_PACKAGE", tmp_path / "skills" / "package.json")
    monkeypatch.setattr(
        set_release_version, "SKILLS_COMPATIBILITY", tmp_path / "skills" / "compatibility.json"
    )
    monkeypatch.setattr(set_release_version, "native_npm_packages", list)
    monkeypatch.setattr(set_release_version, "crate_manifests", list)

    with pytest.raises(ValueError, match=r"path\+version"):
        set_release_version.apply_version("0.5.1", dev=False, dry_run=False)
    assert cargo.read_text(encoding="utf-8") == before
