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
    wheel = tmp_path / "graphforge-0.5.0-py3-none-any.whl"
    wheel.write_bytes(b"fake-wheel")
    record = record_release_artifacts.build_record(
        version="0.5.0",
        dist_dir=tmp_path,
        notes="test",
    )
    assert record["schema"] == "graphforge-release-record-v1"
    assert record["version"] == "0.5.0"
    assert record["tag"] == "v0.5.0"
    assert len(record["commit_sha"]) == 40
    assert record["licenses"]["first_party_spdx"] == "Apache-2.0"
    assert record["licenses"]["related_issues"] == ["#218", "#200"]
    assert record["links"]["parent_tracker"] == "#192"
    assert record["links"]["execution_tracker"] == "#194"
    assert record["contents_summary"]["total_artifacts"] == 1
    assert record["artifacts"][0]["class"] == "python-wheel"
    assert record["artifacts"][0]["surface"] == "pypi"
    assert record["artifacts"][0]["name"] == "graphforge"
    assert record["artifacts"][0]["version"] == "0.5.0"
    assert record["artifacts"][0]["filename"] == wheel.name
    assert len(record["artifacts"][0]["sha256"]) == 64
    assert "same_tagged_commit_policy" in record
    serialized = json.dumps(record)
    for retired_tracker in ("#742", "#2783", "#2793", "#2794", "#2799"):
        assert retired_tracker not in serialized


def test_cli_writes_json(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "pkg.tgz").write_bytes(b"npm")
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
    archive = tmp_path / "graphforge-core-0.5.0.crate"
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
