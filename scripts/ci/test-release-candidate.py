#!/usr/bin/env python3
"""Mutation-sensitive tests for the immutable M1 release-candidate bundle."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile

SCRIPT = Path(__file__).with_name("release-candidate.py")
SPEC = importlib.util.spec_from_file_location("release_candidate", SCRIPT)
assert SPEC and SPEC.loader
release_candidate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_candidate)


def _artifact(root: Path, relative: str, surface: str, name: str, kind: str) -> dict[str, object]:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(relative.encode())
    return {
        "path": relative,
        "class": kind,
        "surface": surface,
        "name": name,
        "version": "0.5.0",
        "filename": path.name,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _fixture(root: Path) -> tuple[Path, Path]:
    artifacts: list[dict[str, object]] = []
    for platform in ("linux", "macos", "windows"):
        artifacts.append(
            _artifact(
                root, f"python/graphforge-{platform}.whl", "pypi", "graphforge", "python-wheel"
            )
        )
    for target in ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64", "win32-x64"):
        artifacts.append(
            _artifact(root, f"node-addons/graphforge.{target}.node", "github", target, "node-addon")
        )
    artifacts.append(
        _artifact(root, "python/graphforge-0.5.0.tar.gz", "pypi", "graphforge", "python-sdist")
    )
    for index, name in enumerate(release_candidate.NPM_PACKAGES):
        artifacts.append(_artifact(root, f"npm/{index}.tgz", "npm", name, "npm-tarball"))
    for name in release_candidate.CRATES:
        artifacts.append(
            _artifact(root, f"crates/{name}-0.5.0.crate", "crates", name, "rust-crate")
        )
    record = {
        "schema": release_candidate.SCHEMA,
        "version": "0.5.0",
        "tag": "v0.5.0",
        "commit_sha": "a" * 40,
        "artifacts": artifacts,
    }
    record_path = root.parent / "record.json"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    return record_path, root


def main() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "artifacts"
        record_path, artifacts_dir = _fixture(root)
        record = release_candidate.validate(record_path, artifacts_dir, "a" * 40, "0.5.0")
        assert len(release_candidate.npm_paths(record)) == 8
        assert release_candidate.npm_paths(record)[-3:] == ["npm/5.tgz", "npm/6.tgz", "npm/7.tgz"]

        target = artifacts_dir / record["artifacts"][0]["path"]
        target.write_bytes(b"mutated")
        try:
            release_candidate.validate(record_path, artifacts_dir, "a" * 40, "0.5.0")
        except release_candidate.CandidateError as error:
            assert "checksum mismatch" in str(error)
        else:
            raise AssertionError("checksum mutation should fail")

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "artifacts"
        record_path, artifacts_dir = _fixture(root)
        record = json.loads(record_path.read_text(encoding="utf-8"))
        npm_item = next(item for item in record["artifacts"] if item["surface"] == "npm")
        npm_item["version"] = "0.5.1"
        record_path.write_text(json.dumps(record), encoding="utf-8")
        try:
            release_candidate.validate(record_path, artifacts_dir, "a" * 40, "0.5.0")
        except release_candidate.CandidateError as error:
            assert "artifact version mismatch" in str(error)
        else:
            raise AssertionError("ADR 0017 forbids npm/core version divergence")

    print("release-candidate tests: ok")


if __name__ == "__main__":
    main()
