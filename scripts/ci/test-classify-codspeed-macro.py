#!/usr/bin/env python3
"""Regression tests for CodSpeed Macro changed-path classification."""

from __future__ import annotations

from pathlib import Path
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[2]
CLASSIFIER = ROOT / "scripts/ci/classify-codspeed-macro.py"
WORKFLOW = ROOT / ".github/workflows/codspeed.yml"


def classify(*paths: str) -> str:
    result = subprocess.run(
        ["python3", str(CLASSIFIER)],
        input="\n".join(paths),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


assert classify("crates/graphforge-storage/src/project.rs") == "true"
assert classify("crates/graphforge-filesystem/src/lib.rs") == "true"
assert classify("crates/graphforge-core/src/lib.rs") == "true"
assert classify(".cargo/config.toml") == "true"
assert classify("Cargo.lock") == "true"
assert classify("Cargo.toml") == "true"
assert classify("rust-toolchain.toml") == "true"
assert classify("docs/reference/storage.md") == "false"
assert classify(".github/workflows/codspeed.yml") == "false"
assert classify("scripts/ci/classify-codspeed-macro.py") == "false"
assert classify("crates/graphforge-ontology/src/lib.rs") == "false"
assert classify("docs/guide.md", "crates/graphforge-storage/src/lib.rs") == "true"
assert classify() == "false"
assert classify("new-runtime-surface/config.bin") == "true"
assert classify(" docs/change.rs") == "true"
assert classify("unknown.md ") == "true"

workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
jobs = workflow["jobs"]
macro_changes = jobs["macro-changes"]
assert macro_changes["outputs"]["required"] == "${{ steps.classify.outputs.required }}"
assert any(step.get("id") == "classify" for step in macro_changes["steps"])

walltime = jobs["m6-walltime"]
assert walltime["needs"] == "macro-changes"
assert walltime["if"] == (
    "github.event_name != 'pull_request' || needs.macro-changes.outputs.required == 'true'"
)

print("CodSpeed Macro path classification verified")
