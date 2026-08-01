#!/usr/bin/env python3
"""Tests for native npm legal-inventory preparation."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

SCRIPT = Path(__file__).with_name("prepare-napi-packages.py")
SPEC = importlib.util.spec_from_file_location("prepare_napi_packages", SCRIPT)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    npm = root / "npm"
    package = npm / "darwin-arm64"
    package.mkdir(parents=True)
    (package / "package.json").write_text(
        json.dumps({"name": "@curatelabs/graphforge-darwin-arm64", "files": ["addon.node"]}),
        encoding="utf-8",
    )
    (package / "addon.node").write_bytes(b"native")
    legal = root / "legal"
    legal.mkdir()
    for name in module.LEGAL_FILES:
        (legal / name).write_text(name, encoding="utf-8")

    module.prepare(npm, legal)
    manifest = json.loads((package / "package.json").read_text(encoding="utf-8"))
    assert manifest["files"] == ["addon.node", *module.LEGAL_FILES]
    for name in module.LEGAL_FILES:
        assert (package / name).read_text(encoding="utf-8") == name

    (legal / "NOTICE").unlink()
    try:
        module.prepare(npm, legal)
    except ValueError as error:
        assert "legal source is missing" in str(error)
    else:
        raise AssertionError("missing legal input was accepted")

print("prepare-napi-packages tests passed")
