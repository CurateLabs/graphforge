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
    package("graphforge-core", []),
    package("graphforge-filesystem", []),
    package("graphforge-storage", ["graphforge-core", "graphforge-filesystem"]),
    package("graphforge-exec", ["graphforge-core", "graphforge-storage"]),
    package("graphforge-provenance", ["graphforge-core"]),
    package("graphforge-knowledge", ["graphforge-core"]),
    package(
        "graphforge-api",
        [
            "graphforge-core",
            "graphforge-storage",
            "graphforge-exec",
            "graphforge-provenance",
            "graphforge-knowledge",
        ],
    ),
    package("graphforge-bindings-py", ["graphforge-api"]),
    package("graphforge-bindings-node", ["graphforge-api"]),
    package("graphforge-cli", ["graphforge-api"]),
]

assert run(base).returncode == 0

for consumer, dependency in [
    ("graphforge-storage", "graphforge-knowledge"),
    ("graphforge-exec", "graphforge-provenance"),
    ("graphforge-knowledge", "graphforge-storage"),
    ("graphforge-provenance", "graphforge-knowledge"),
    ("graphforge-bindings-py", "graphforge-knowledge"),
    ("graphforge-bindings-node", "graphforge-provenance"),
    ("graphforge-cli", "graphforge-knowledge"),
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
