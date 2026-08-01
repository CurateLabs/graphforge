#!/usr/bin/env python3
"""Tests for scripts/ci/crate-authorize-refresh-nodes.py."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile

SCRIPT = Path(__file__).with_name("crate-authorize-refresh-nodes.py")


def run(manifest: dict, node: str) -> list[str]:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "manifest.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(path), "--node", node],
            check=True,
            capture_output=True,
            text=True,
        )
    return [line for line in completed.stdout.splitlines() if line]


def main() -> None:
    manifest = {
        "dependencies": [
            {"from": "crates:graphforge-api", "requires": "crates:graphforge-core"},
            {"from": "crates:graphforge-api", "requires": "crates:graphforge-search"},
            {"from": "crates:graphforge-api", "requires": "npm:@curatelabs/graphforge"},
            {"from": "crates:graphforge-cli", "requires": "crates:graphforge-api"},
        ]
    }
    assert run(manifest, "crates:graphforge-api") == [
        "crates:graphforge-api",
        "crates:graphforge-core",
        "crates:graphforge-search",
    ]
    assert run(manifest, "crates:graphforge-cli") == [
        "crates:graphforge-cli",
        "crates:graphforge-api",
    ]
    assert run({"dependencies": []}, "crates:graphforge-core") == ["crates:graphforge-core"]
    print("crate-authorize-refresh-nodes tests passed")


if __name__ == "__main__":
    main()
