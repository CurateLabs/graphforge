#!/usr/bin/env python3
"""Prove the Cargo↔Bazel drift check fails closed on intentional divergence."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/cargo-bazel-drift-check.py"


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="graphforge-drift-") as tmp:
        tmp_path = Path(tmp)
        good = tmp_path / "good.json"
        bad = tmp_path / "bad.json"

        subprocess.check_call(
            ["python3", str(CHECK), "--write", "--fingerprint", str(good)],
            cwd=ROOT,
        )
        payload = json.loads(good.read_text(encoding="utf-8"))
        # Intentional divergence: drop a dependency feature entry.
        assert payload["entries"], "fingerprint must contain workspace packages"
        payload["entries"][0]["dependencies"] = payload["entries"][0]["dependencies"][:-1] or [
            {
                "name": "intentionally-missing-dep",
                "req": "1.0.0",
                "features": ["drift"],
                "optional": False,
                "uses_default_features": True,
                "kind": None,
                "target": None,
            }
        ]
        # Keep a stale sha so either sha or entries mismatch fails closed.
        bad.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        ok = subprocess.run(
            ["python3", str(CHECK), "--fingerprint", str(good)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if ok.returncode != 0:
            raise SystemExit(f"expected matching fingerprint to pass:\n{ok.stderr}")

        drifted = subprocess.run(
            ["python3", str(CHECK), "--fingerprint", str(bad)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if drifted.returncode == 0:
            raise SystemExit("expected intentional divergence to fail closed")
        if "drifted" not in drifted.stderr.lower() and "drift" not in drifted.stderr.lower():
            raise SystemExit(f"unexpected failure output:\n{drifted.stderr}")

    print("cargo-bazel drift check tests passed")


if __name__ == "__main__":
    main()
