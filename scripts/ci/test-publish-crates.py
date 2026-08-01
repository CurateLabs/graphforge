#!/usr/bin/env python3
"""Deterministic tests for the checksum-safe crates.io publisher."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

SCRIPT = Path(__file__).parents[1] / "publish_crates.py"


def load_module():
    spec = importlib.util.spec_from_file_location("publish_crates", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = load_module()
assert mod.VERSION == "0.5.0"

commands: list[list[str]] = []
mod.package_checksum = lambda _name, expected=None: expected or "abc123"
mod.owner_logins = lambda _name: {"DecisionNerd"}
mod.run = commands.append

mod.version_record = lambda _name: {"checksum": "abc123"}
assert (
    mod.publish_one("graphforge-core", expected_checksum="abc123")
    == "already published; checksum and owner match"
)
assert commands == []

mod.version_record = lambda _name: None
assert (
    mod.publish_one("graphforge-core")
    == "accepted; public checksum and owner verification required"
)
assert commands == [["cargo", "publish", "-p", "graphforge-core", "--locked"]]

commands.clear()
assert (
    mod.publish_authorized("graphforge-core", "abc123")
    == "accepted; public checksum and owner verification required"
)
assert commands == [["cargo", "publish", "-p", "graphforge-core", "--locked"]]

mod.version_record = lambda _name: {"checksum": "different"}
try:
    mod.publish_one("graphforge-core")
    raise AssertionError("expected an existing-version checksum mismatch")
except RuntimeError as exc:
    assert "refusing to resume" in str(exc)

mod.version_record = lambda _name: {"checksum": "abc123"}
mod.owner_logins = lambda _name: {"someone-else"}
try:
    mod.publish_one("graphforge-core")
    raise AssertionError("expected the owner assertion to fail")
except RuntimeError as exc:
    assert "DecisionNerd is not an owner" in str(exc)

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    artifacts = root / "artifacts"
    artifacts.mkdir()
    archive = artifacts / "graphforge-core-0.5.0.crate"
    archive.write_bytes(b"certified crate")
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    record = {
        "schema": "graphforge-release-record-v1",
        "version": "0.5.0",
        "tag": "v0.5.0",
        "commit_sha": "release-sha",
        "artifacts": [
            {
                "surface": "crates",
                "name": "graphforge-core",
                "version": "0.5.0",
                "path": archive.name,
                "sha256": sha,
            }
        ],
    }
    record_path = root / "record.json"
    original_run = subprocess.run

    def release_sha(*args, **_kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout="release-sha\n")

    mod.subprocess.run = release_sha
    record_path.write_text(json.dumps(record), encoding="utf-8")
    assert mod.release_record_checksums(record_path, artifacts) == {"graphforge-core": sha}

    for escaped in ("../outside.crate", "nested/../../outside.crate", "/etc/passwd"):
        record["artifacts"][0]["path"] = escaped
        record_path.write_text(json.dumps(record), encoding="utf-8")
        try:
            mod.release_record_checksums(record_path, artifacts)
            raise AssertionError("expected escaped artifact path to fail")
        except RuntimeError as exc:
            assert "escapes artifact root" in str(exc)

    record["artifacts"][0]["path"] = archive.name
    record["artifacts"][0]["version"] = "0.5.1"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    try:
        mod.release_record_checksums(record_path, artifacts)
        raise AssertionError("expected artifact version mismatch to fail")
    except RuntimeError as exc:
        assert "version mismatch" in str(exc)
    mod.subprocess.run = original_run

print("publish crates tests passed")
