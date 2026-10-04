#!/usr/bin/env python3
"""Deterministic tests for the npm publisher."""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile

SCRIPT = Path(__file__).parents[1] / "publish_npm.py"


def load_module():
    spec = importlib.util.spec_from_file_location("publish_npm", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pack(directory: Path, filename: str, name: str, version: str) -> None:
    payload = json.dumps({"name": name, "version": version}).encode()
    info = tarfile.TarInfo("package/package.json")
    info.size = len(payload)
    with tarfile.open(directory / filename, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(payload))


mod = load_module()
version = mod.workspace_version()

# A prerelease never becomes `latest`; an unclassifiable version is refused.
assert mod.dist_tag_for("0.6.0") == "latest"
assert mod.dist_tag_for("0.6.0-rc.1") == "next"
assert mod.dist_tag_for("1.0.0+build.5") == "latest"


def refused(candidate: str) -> bool:
    try:
        mod.dist_tag_for(candidate)
    except ValueError:
        return True
    return False


assert all(refused(bad) for bad in ("", "0.6", "v0.6.0", "0.6.0rc1", "latest"))

with tempfile.TemporaryDirectory() as raw:
    root = Path(raw)
    pack(root, "curatelabs-graphforge-cli-x.tgz", "@curatelabs/graphforge-cli", version)
    pack(root, "curatelabs-graphforge-x.tgz", "@curatelabs/graphforge", version)
    pack(
        root,
        "curatelabs-graphforge-linux-x64-gnu-x.tgz",
        "@curatelabs/graphforge-linux-x64-gnu",
        version,
    )
    pack(
        root,
        "curatelabs-graphforge-darwin-arm64-x.tgz",
        "@curatelabs/graphforge-darwin-arm64",
        version,
    )

    # A missing dependent package is an incomplete release.
    try:
        mod.publish_order(root)
        raise AssertionError("expected the missing agent-skills package to fail")
    except RuntimeError as error:
        assert "graphforge-agent-skills" in str(error)

    pack(
        root,
        "curatelabs-graphforge-agent-skills-x.tgz",
        "@curatelabs/graphforge-agent-skills",
        version,
    )
    names = [name for _, name, _ in mod.publish_order(root)]
    assert names == [
        "@curatelabs/graphforge-darwin-arm64",
        "@curatelabs/graphforge-linux-x64-gnu",
        "@curatelabs/graphforge",
        "@curatelabs/graphforge-cli",
        "@curatelabs/graphforge-agent-skills",
    ], names

    # A re-run publishes only what the registry lacks, natives before the main
    # package, always with the derived dist-tag.
    commands: list[list[str]] = []
    mod.subprocess.run = lambda command, **_kwargs: commands.append(command)
    mod.is_published = lambda name, _version: name == "@curatelabs/graphforge-darwin-arm64"
    tag = mod.dist_tag_for(version)
    mod.publish(root, tag)
    assert [Path(command[2]).name for command in commands] == [
        "curatelabs-graphforge-linux-x64-gnu-x.tgz",
        "curatelabs-graphforge-x.tgz",
        "curatelabs-graphforge-cli-x.tgz",
        "curatelabs-graphforge-agent-skills-x.tgz",
    ], commands
    assert all(command[-2:] == ["--tag", tag] and "--provenance" in command for command in commands)

    # The wrong dist-tag is refused before anything is published.
    commands.clear()
    wrong = "next" if tag == "latest" else "latest"
    try:
        mod.publish(root, wrong)
        raise AssertionError("expected a contradictory dist-tag to fail")
    except RuntimeError as error:
        assert "contradicts" in str(error)
    assert commands == []

    # A tarball packed at another version stops the release before any upload.
    pack(root, "curatelabs-graphforge-cli-x.tgz", "@curatelabs/graphforge-cli", "0.0.1")
    try:
        mod.publish(root, tag)
        raise AssertionError("expected a stale tarball to fail")
    except RuntimeError as error:
        assert "expected" in str(error)
    assert commands == []

print("publish npm tests passed")
