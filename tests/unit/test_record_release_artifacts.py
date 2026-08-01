"""Tests for release artifact checksum recording."""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "record_release_artifacts.py"
SPEC = importlib.util.spec_from_file_location("record_release_artifacts", SCRIPT)
assert SPEC and SPEC.loader
record_release_artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(record_release_artifacts)


def test_classify_and_hash(tmp_path: Path) -> None:
    wheel = tmp_path / "python" / "graphforge-0.5.0-py3-none-any.whl"
    wheel.parent.mkdir()
    wheel.write_bytes(b"fake-wheel")
    record = record_release_artifacts.build_record(
        version="0.5.0",
        dist_dir=tmp_path,
        notes="test",
    )
    assert record["schema"] == "graphforge-release-candidate-v2"
    assert record["version"] == "0.5.0"
    assert record["tag"] == "v0.5.0"
    assert len(record["commit_sha"]) == 40
    assert set(record["publication_states"]) == {
        "not_attempted",
        "absent",
        "accepted_pending_visibility",
        "verified",
        "conflict",
        "indeterminate",
        "failed",
    }
    assert [group["id"] for group in record["artifact_groups"]] == [
        "python",
        "npm",
        "crates",
        "evidence",
    ]
    assert record["artifacts"][0]["class"] == "python-wheel"
    assert record["artifacts"][0]["surface"] == "pypi"
    assert record["artifacts"][0]["name"] == "graphforge"
    assert record["artifacts"][0]["version"] == "0.5.0"
    assert record["artifacts"][0]["filename"] == wheel.name
    assert len(record["artifacts"][0]["sha256"]) == 64
    serialized = json.dumps(record)
    for retired_tracker in ("#742", "#2783", "#2793", "#2794", "#2799"):
        assert retired_tracker not in serialized


def test_cli_writes_json(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    (dist / "npm").mkdir(parents=True)
    (dist / "npm" / "pkg.tgz").write_bytes(b"npm")
    out = tmp_path / "out.json"
    assert (
        record_release_artifacts.main(
            ["--version", "0.5.0", "--dist-dir", str(dist), "--out", str(out)]
        )
        == 0
    )
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["artifacts"][0]["class"] == "npm-tarball"
    assert payload["artifacts"][0]["surface"] == "npm"


def test_crate_artifact_uses_crates_surface(tmp_path: Path) -> None:
    archive = tmp_path / "crates" / "graphforge-core-0.5.0.crate"
    archive.parent.mkdir()
    archive.write_bytes(b"crate")
    record = record_release_artifacts.build_record(
        version="0.5.0",
        dist_dir=tmp_path,
        notes="crate test",
    )
    artifact = record["artifacts"][0]
    assert artifact["class"] == "rust-crate"
    assert artifact["surface"] == "crates"
    assert artifact["name"] == "graphforge-core"


def test_owned_scope_npm_artifacts_keep_their_public_identity(tmp_path: Path) -> None:
    expected = {
        "curatelabs-graphforge-0.5.0.tgz": "@curatelabs/graphforge",
        "curatelabs-graphforge-linux-x64-gnu-0.5.0.tgz": ("@curatelabs/graphforge-linux-x64-gnu"),
        "curatelabs-graphforge-cli-0.5.0.tgz": "@curatelabs/graphforge-cli",
        "curatelabs-graphforge-agent-skills-0.5.0.tgz": ("@curatelabs/graphforge-agent-skills"),
    }
    for filename in expected:
        path = tmp_path / "npm" / filename
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(filename.encode())

    record = record_release_artifacts.build_record(
        version="0.5.0",
        dist_dir=tmp_path,
        notes="npm identity test",
    )
    observed = {artifact["filename"]: artifact["name"] for artifact in record["artifacts"]}
    assert observed == expected
