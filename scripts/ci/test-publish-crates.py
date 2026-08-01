#!/usr/bin/env python3
"""Deterministic tests for the checksum-safe crates.io publisher."""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
waits: list[tuple[str, str, int]] = []
mod.package_checksum = lambda _name: "abc123"
mod.owner_logins = lambda _name: {"DecisionNerd"}
mod.run = commands.append
mod.wait_for_version = lambda name, checksum, timeout: waits.append((name, checksum, timeout))

mod.version_record = lambda _name: {"checksum": "abc123"}
assert (
    mod.publish_one("graphforge-core", timeout_seconds=30) == "already published; checksum matches"
)
assert commands == []
assert waits == []

mod.version_record = lambda _name: None
assert mod.publish_one("graphforge-core", timeout_seconds=30) == "published"
assert commands == [["cargo", "publish", "-p", "graphforge-core", "--locked"]]
assert waits == [("graphforge-core", "abc123", 30)]

mod.version_record = lambda _name: {"checksum": "different"}
try:
    mod.publish_one("graphforge-core", timeout_seconds=30)
    raise AssertionError("expected an existing-version checksum mismatch")
except RuntimeError as exc:
    assert "refusing to resume" in str(exc)

mod.version_record = lambda _name: {"checksum": "abc123"}
mod.owner_logins = lambda _name: {"someone-else"}
try:
    mod.publish_one("graphforge-core", timeout_seconds=30)
    raise AssertionError("expected the owner assertion to fail")
except RuntimeError as exc:
    assert "DecisionNerd is not an owner" in str(exc)

print("publish crates tests passed")
