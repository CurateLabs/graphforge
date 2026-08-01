#!/usr/bin/env python3
"""Deterministic contract for the authorized v0.5.0 npm amendment."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile

SCRIPT = Path(__file__).parents[1] / "amend_npm_main_artifact.py"
SPEC = importlib.util.spec_from_file_location("amend_npm_main_artifact", SCRIPT)
assert SPEC and SPEC.loader
amend = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(amend)

source_manifest = amend.load_json(
    Path(__file__).parents[2] / "crates" / "graphforge-bindings-node" / "package.json"
)
assert "*.node" not in source_manifest["files"]

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    source = root / "source" / "package"
    source.mkdir(parents=True)
    manifest = {
        "name": amend.PACKAGE,
        "version": amend.VERSION,
        "files": ["index.js", "*.node"],
        "optionalDependencies": amend.OPTIONAL_DEPENDENCIES,
    }
    (source / "package.json").write_text(json.dumps(manifest), encoding="utf-8")
    (source / "index.js").write_text("module.exports = {}\n", encoding="utf-8")
    for name in amend.NATIVE_FILES:
        (source / name).write_bytes(name.encode())

    artifacts = root / "artifacts"
    original = artifacts / "npm" / "curatelabs-graphforge-0.5.0.tgz"
    original.parent.mkdir(parents=True)
    with tarfile.open(original, "w:gz") as bundle:
        bundle.add(source, arcname="package")
    record = {
        "schema": "graphforge-release-record-v1",
        "version": "0.5.0",
        "tag": "v0.5.0",
        "commit_sha": "a" * 40,
        "artifacts": [
            {
                "path": "npm/curatelabs-graphforge-0.5.0.tgz",
                "surface": "npm",
                "name": amend.PACKAGE,
                "version": amend.VERSION,
                "bytes": original.stat().st_size,
                "sha256": amend.sha256_file(original),
            }
        ],
    }
    record_path = root / "record.json"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    archive_out = root / "curatelabs-graphforge-0.5.0-amended.tgz"
    supplement = root / "v0.5.0-npm-amendment.json"

    amend.create(record_path, artifacts, archive_out, supplement)
    updated = amend.load_json(record_path)
    item = amend.main_item(updated)
    assert item["sha256"] == amend.sha256_file(archive_out)
    assert item["bytes"] < record["artifacts"][0]["bytes"]
    assert updated["amendments"][0]["issue"] == "#287"
    details = amend.load_json(supplement)
    assert set(details["excluded_files"]) == amend.NATIVE_FILES

    extracted = root / "extracted"
    amend.safe_extract(archive_out, extracted)
    assert not list((extracted / "package").glob("*.node"))
    amended_manifest = amend.load_json(extracted / "package" / "package.json")
    assert "*.node" not in amended_manifest["files"]
    assert amended_manifest["optionalDependencies"] == amend.OPTIONAL_DEPENDENCIES

print("npm main amendment tests passed")
