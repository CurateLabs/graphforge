#!/usr/bin/env python3
"""Deterministic tests for checksum-safe npm publication."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import types

SCRIPT = Path(__file__).parents[1] / "publish_npm_artifacts.py"
SPEC = importlib.util.spec_from_file_location("publish_npm_artifacts", SCRIPT)
assert SPEC and SPEC.loader
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)

# A release version takes npm's default channel; every semver prerelease is diverted.
for version, expected_tag in {
    "0.5.2": "latest",
    "0.6.0": "latest",
    "1.0.0": "latest",
    "0.6.0+build.5": "latest",
    "0.6.0-rc.1": "next",
    "0.6.0-rc1": "next",
    "0.6.0-dev": "next",
    "0.6.0-dev.0": "next",
    "0.6.0-beta.2": "next",
    "1.0.0-0": "next",
}.items():
    assert publisher.dist_tag_for(version) == expected_tag, version

# Anything unclassifiable refuses rather than inheriting npm's 'latest' default.
# '0.6.0rc1' is the PEP 440 spelling of the same release and is not npm semver.
for malformed in ("", "   ", "0.6.0rc1", "v0.6.0", "0.6", "0.6.0.1", "latest", "0.6.0-", None, 60):
    try:
        publisher.dist_tag_for(malformed)
    except publisher.DistTagError:
        continue
    raise AssertionError(f"{malformed!r} must not be classified")

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
    published: list[tuple[Path, str]] = []
    real_publish_archive = publisher.publish_archive
    publisher.publish_archive = lambda path, dist_tag: published.append((path, dist_tag))
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(b"candidate").digest()).decode()
    publisher.published_integrity = lambda _name, _version: integrity
    assert publisher.publish_one(item, root, "latest") == "already published; integrity matches"
    assert published == []

    publisher.published_integrity = lambda _name, _version: None
    assert publisher.publish_one(item, root, "latest") == "accepted; public verification required"
    assert published == [(archive, "latest")]

    # A prerelease tarball is never publishable under the release channel.
    prerelease = {**item, "version": "0.6.0-rc.1"}
    try:
        publisher.publish_one(prerelease, root, "latest")
    except RuntimeError as error:
        assert "refusing to publish" in str(error)
    else:
        raise AssertionError("a prerelease must not publish under 'latest'")
    assert published == [(archive, "latest")]

    published.clear()
    assert (
        publisher.publish_one(prerelease, root, "next") == "accepted; public verification required"
    )
    assert published == [(archive, "next")]

    different = "sha512-" + base64.b64encode(hashlib.sha512(b"different").digest()).decode()
    publisher.published_integrity = lambda _name, _version: different
    try:
        publisher.publish_one(item, root, "latest")
    except RuntimeError as error:
        assert "refusing to resume" in str(error)
    else:
        raise AssertionError("checksum drift should fail")

    # publish_archive always hands npm an explicit --tag.
    argv: list[list[str]] = []
    publisher.subprocess = types.SimpleNamespace(run=lambda cmd, **_kwargs: argv.append(cmd))
    real_publish_archive(archive, "next")
    assert argv[0][:2] == ["npm", "publish"]
    assert argv[0][-2:] == ["--tag", "next"]
    assert "--provenance" in argv[0]

    assert publisher.archive_matches_integrity(archive, f"md5-AAAA {integrity}")
    try:
        publisher.archive_matches_integrity(archive, "md5-AAAA")
    except RuntimeError as error:
        assert "no supported" in str(error)
    else:
        raise AssertionError("unsupported integrity should fail")

# The command line cannot talk a prerelease onto 'latest', and an unclassifiable
# version stops before any registry contact.
assert publisher.main(["--version", "0.6.0-rc.1", "--print-dist-tag"]) == 0
assert publisher.main(["--version", "0.6.0", "--print-dist-tag"]) == 0
assert publisher.main(["--version", "nonsense", "--print-dist-tag"]) == 2
assert (
    publisher.main(
        [
            "--version",
            "0.6.0-rc.1",
            "--dist-tag",
            "latest",
            "--release-record",
            "manifest.json",
            "--artifacts-dir",
            "artifacts",
            "--expected-sha",
            "a" * 40,
            "--package",
            "@curatelabs/graphforge",
        ]
    )
    == 2
)
assert (
    publisher.main(
        [
            "--version",
            "0.6.0-rc.1",
            "--release-record",
            "manifest.json",
            "--artifacts-dir",
            "artifacts",
            "--expected-sha",
            "a" * 40,
        ]
    )
    == 2
)

assert publisher.GROUPS["native"] == slice(0, 6)
assert publisher.GROUPS["cli"] == slice(6, 7)
assert publisher.GROUPS["skills"] == slice(7, 8)
source = SCRIPT.read_text(encoding="utf-8")
assert "time.sleep" not in source
assert "while " not in source
assert "--provenance" in source
# Every publication carries an explicit dist-tag derived from the version.
assert '"--tag",' in source
assert "def dist_tag_for(" in source
assert 'RELEASE_DIST_TAG = "latest"' in source
assert 'PRERELEASE_DIST_TAG = "next"' in source
assert "publish_archive(path, dist_tag)" in source
assert "NODE_AUTH_TOKEN is required" not in source
# --package and --group both resume through publish_one (no direct publish_archive bypass).
assert "for name in names:" in source
assert "publish_one(by_name[name], args.artifacts_dir, dist_tag)" in source
assert "if args.package:" not in source or "publish_archive(args.artifacts_dir" not in source
print("publish npm artifact tests passed")
