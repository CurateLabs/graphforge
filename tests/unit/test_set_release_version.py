"""Tests for multi-surface release version alignment."""

import importlib.util
import json
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on supported Python 3.10
    import tomli as tomllib

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "set_release_version.py"
SPEC = importlib.util.spec_from_file_location("set_release_version", SCRIPT)
assert SPEC and SPEC.loader
set_release_version = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(set_release_version)


def test_parse_release_and_dev() -> None:
    assert set_release_version.parse_base("0.5.2") == ("0.5.2", False, None)
    assert set_release_version.parse_base("0.6.0") == ("0.6.0", False, None)
    assert set_release_version.parse_base("0.5.0-dev") == ("0.5.0", True, None)
    assert set_release_version.parse_base("0.5.0.dev0") == ("0.5.0", True, None)
    assert set_release_version.parse_base("0.5.0-dev.0") == ("0.5.0", True, None)


def test_parse_prerelease() -> None:
    assert set_release_version.parse_base("0.6.0-rc.1") == ("0.6.0", False, "rc.1")
    assert set_release_version.parse_base("0.6.0-rc1") == ("0.6.0", False, "rc1")
    assert set_release_version.parse_base("1.0.0-beta.2") == ("1.0.0", False, "beta.2")


@pytest.mark.parametrize(
    "version",
    [
        "",
        "0.6",
        "1.2.3.4",
        "banana",
        # The PEP 440 spelling is an output, never an input: the root version is
        # the cargo/SemVer spelling (ADR 0033).
        "0.6.0rc1",
        # A prerelease with no PEP 440 meaning must not reach a registry writer.
        "0.6.0-foo",
        # A prerelease development build is not a shape this project publishes.
        "0.6.0-rc.1-dev",
    ],
)
def test_parse_rejects_malformed(version: str) -> None:
    with pytest.raises(ValueError):
        set_release_version.parse_base(version)


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


def test_expected_mapping_prerelease() -> None:
    """One version, spelled per ecosystem: cargo/npm verbatim, Python PEP 440."""
    prerelease = set_release_version.expected_for("0.6.0", dev=False, pre="rc.1")
    assert prerelease == {
        "cargo": "0.6.0-rc.1",
        "python": "0.6.0rc1",
        "node": "0.6.0-rc.1",
        "cli": "0.6.0-rc.1",
        "skills": "0.6.0-rc.1",
    }


@pytest.mark.parametrize(
    ("version", "python"),
    [
        ("0.5.2", "0.5.2"),
        ("0.6.0", "0.6.0"),
        ("0.6.0-rc.1", "0.6.0rc1"),
        ("0.6.0-rc1", "0.6.0rc1"),
        ("0.6.0-dev", "0.6.0.dev0"),
        ("1.0.0-beta.2", "1.0.0b2"),
        ("1.0.0-alpha.3", "1.0.0a3"),
    ],
)
def test_python_spelling(version: str, python: str) -> None:
    assert set_release_version.python_spelling(version) == python


def test_current_tree_is_aligned() -> None:
    lock_versions = set_release_version.cargo_lock_versions()
    manifest_packages = {
        tomllib.loads(path.read_text(encoding="utf-8"))["package"]["name"]
        for path in set_release_version.crate_manifests()
    }
    assert len(lock_versions) == len(manifest_packages)
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


def test_apply_prerelease_rewrites_every_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prerelease reaches every surface in that surface's canonical spelling."""
    cargo = tmp_path / "Cargo.toml"
    cargo.write_text('[workspace.package]\nversion = "0.5.0"\n', encoding="utf-8")
    lock = tmp_path / "Cargo.lock"
    lock.write_text('name = "graphforge-core"\nversion = "0.5.0"\n', encoding="utf-8")
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nversion = "0.5.0"\n', encoding="utf-8")
    crates = tmp_path / "crates"
    manifest = crates / "graphforge-api" / "Cargo.toml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        '[dependencies]\ngraphforge-core = { version = "0.5.0", path = "../graphforge-core" }\n',
        encoding="utf-8",
    )
    native = tmp_path / "npm" / "graphforge-linux-x64-gnu" / "package.json"
    native.parent.mkdir(parents=True)
    native.write_text('{"version":"0.5.0"}\n', encoding="utf-8")
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
    monkeypatch.setattr(set_release_version, "native_npm_packages", lambda: [native])
    monkeypatch.setattr(
        set_release_version, "crate_manifests", lambda: sorted(crates.glob("*/Cargo.toml"))
    )

    base, dev, pre = set_release_version.parse_base("0.6.0-rc.1")
    mapping = set_release_version.apply_version(base, dev=dev, pre=pre, dry_run=False)
    assert mapping["cargo"] == "0.6.0-rc.1"
    assert mapping["python"] == "0.6.0rc1"

    assert 'version = "0.6.0-rc.1"' in cargo.read_text(encoding="utf-8")
    assert 'version = "0.6.0-rc.1"' in lock.read_text(encoding="utf-8")
    assert 'version = "0.6.0rc1"' in pyproject.read_text(encoding="utf-8")
    assert 'graphforge-core = { version = "0.6.0-rc.1"' in manifest.read_text(encoding="utf-8")
    assert json.loads(native.read_text(encoding="utf-8"))["version"] == "0.6.0-rc.1"
    for path in (
        tmp_path / "node" / "package.json",
        tmp_path / "cli" / "package.json",
        tmp_path / "skills" / "package.json",
    ):
        assert json.loads(path.read_text(encoding="utf-8"))["version"] == "0.6.0-rc.1"
    skills = json.loads((tmp_path / "skills" / "package.json").read_text(encoding="utf-8"))
    assert skills["graphforgeCompatibility"]["release"] == "0.6.0-rc.1"
    compatibility = json.loads(
        (tmp_path / "skills" / "compatibility.json").read_text(encoding="utf-8")
    )
    assert compatibility == {
        "package_version": "0.6.0-rc.1",
        "graphforge_release": "0.6.0-rc.1",
    }

    # The tree it just wrote must satisfy --check without further edits.
    assert set_release_version.check_aligned() == []


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
