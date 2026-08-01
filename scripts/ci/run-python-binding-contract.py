#!/usr/bin/env python3
"""Execute every shipped Python native acceptance file in a clean interpreter."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
tests = sorted((ROOT / "crates/graphforge-bindings-py/tests").glob("*.py"))
if not tests:
    raise SystemExit("no Python binding acceptance tests discovered")
for test in tests:
    completed = subprocess.run([sys.executable, str(test)], cwd=ROOT, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)
