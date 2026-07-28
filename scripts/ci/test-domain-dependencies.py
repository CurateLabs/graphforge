#!/usr/bin/env python3
"""Regression tests for the ADR 0014 dependency checker."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
from typing import Any

CHECKER = Path(__file__).with_name("check-domain-dependencies.py")


def package(name: str, dependencies: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "dependencies": [{"name": dependency} for dependency in dependencies],
    }


def run(packages: list[dict[str, Any]]) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as directory:
        metadata = Path(directory) / "metadata.json"
        metadata.write_text(json.dumps({"packages": packages}), encoding="utf-8")
        return subprocess.run(
            [str(CHECKER), "--metadata", str(metadata)],
            capture_output=True,
            text=True,
            check=False,
        )


base = [
    package("gf-core", []),
    package("gf-storage", ["gf-core"]),
    package("gf-exec", ["gf-core", "gf-storage"]),
    package("gf-provenance", ["gf-core"]),
    package("gf-knowledge", ["gf-core"]),
    package(
        "gf-api",
        ["gf-core", "gf-storage", "gf-exec", "gf-provenance", "gf-knowledge"],
    ),
    package("gf-bindings-py", ["gf-api"]),
    package("gf-bindings-node", ["gf-api"]),
    package("gf-cli", ["gf-api"]),
]

assert run(base).returncode == 0

for consumer, dependency in [
    ("gf-storage", "gf-knowledge"),
    ("gf-exec", "gf-provenance"),
    ("gf-knowledge", "gf-storage"),
    ("gf-provenance", "gf-knowledge"),
    ("gf-bindings-py", "gf-knowledge"),
    ("gf-bindings-node", "gf-provenance"),
    ("gf-cli", "gf-knowledge"),
]:
    changed = [
        package(
            item["name"],
            [value["name"] for value in item["dependencies"]]
            + ([dependency] if item["name"] == consumer else []),
        )
        for item in base
    ]
    result = run(changed)
    assert result.returncode == 1, (consumer, dependency, result.stdout, result.stderr)
    assert consumer in result.stderr
    assert dependency in result.stderr

print("ADR 0014 domain dependency checker tests passed")
