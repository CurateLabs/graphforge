#!/usr/bin/env python3
"""Deterministic tests for checksum-safe npm publication."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
from pathlib import Path
import tempfile

SCRIPT = Path(__file__).parents[1] / "publish_npm_artifacts.py"
SPEC = importlib.util.spec_from_file_location("publish_npm_artifacts", SCRIPT)
assert SPEC and SPEC.loader
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    archive = root / "package.tgz"
    archive.write_bytes(b"candidate")
    item = {
        "name": "@curatelabs/graphforge",
        "version": "0.5.0",
        "path": archive.name,
        "sha256": "abc123",
    }
    published: list[Path] = []
    publisher.publish_archive = published.append
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(b"candidate").digest()).decode()
    publisher.published_integrity = lambda _name, _version: integrity
    assert publisher.publish_one(item, root, 1) == "already published; integrity matches"
    assert published == []

    responses = iter((None, integrity))
    publisher.published_integrity = lambda _name, _version: next(responses)
    assert publisher.publish_one(item, root, 1) == "published"
    assert published == [archive]

    different = "sha512-" + base64.b64encode(hashlib.sha512(b"different").digest()).decode()
    publisher.published_integrity = lambda _name, _version: different
    try:
        publisher.publish_one(item, root, 1)
    except RuntimeError as error:
        assert "refusing to resume" in str(error)
    else:
        raise AssertionError("checksum drift should fail")

    assert publisher.archive_matches_integrity(archive, f"md5-AAAA {integrity}")
    try:
        publisher.archive_matches_integrity(archive, "md5-AAAA")
    except RuntimeError as error:
        assert "no supported" in str(error)
    else:
        raise AssertionError("unsupported integrity should fail")

assert publisher.GROUPS["native"] == slice(0, 6)
assert publisher.GROUPS["cli"] == slice(6, 7)
assert publisher.GROUPS["skills"] == slice(7, 8)
print("publish npm artifact tests passed")
