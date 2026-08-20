#!/usr/bin/env python3
"""Regression tests for CodSpeed Macro changed-path classification."""

from __future__ import annotations

from pathlib import Path
import subprocess

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
assert classify("Cargo.lock") == "true"
assert classify("docs/reference/storage.md") == "false"
assert classify(".github/workflows/codspeed.yml") == "false"
assert classify("scripts/ci/classify-codspeed-macro.py") == "false"
assert classify("crates/graphforge-ontology/src/lib.rs") == "false"
assert classify("docs/guide.md", "crates/graphforge-storage/src/lib.rs") == "true"
assert classify() == "false"
assert classify("new-runtime-surface/config.bin") == "true"

workflow = WORKFLOW.read_text(encoding="utf-8")
assert "macro-changes:" in workflow
assert "needs: macro-changes" in workflow
assert "github.event_name != 'pull_request'" in workflow
assert "needs.macro-changes.outputs.required == 'true'" in workflow

print("CodSpeed Macro path classification verified")
