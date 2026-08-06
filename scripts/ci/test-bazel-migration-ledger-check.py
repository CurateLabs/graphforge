#!/usr/bin/env python3
"""Prove the migration ledger check fails closed on unmapped / stub exceptions."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/bazel-migration-ledger-check.py"
MAP = ROOT / "tools/bazel/parity/migration_target_map.json"
LEDGER = ROOT / "docs/development/bazel-migration-ledger.md"


def main() -> None:
    ok = subprocess.run(
        ["python3", str(CHECK), "--map", str(MAP), "--ledger", str(LEDGER)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if ok.returncode != 0:
        raise SystemExit(
            "expected current ledger map to pass:\n"
            f"{ok.stdout}\n{ok.stderr}"
        )

    with tempfile.TemporaryDirectory(prefix="gf-ledger-") as tmp:
        tmp_path = Path(tmp)
        bad_map = tmp_path / "bad-map.json"
        bad_ledger = tmp_path / "bad-ledger.md"
        shutil.copy2(LEDGER, bad_ledger)

        payload = json.loads(MAP.read_text(encoding="utf-8"))
        # Force an unmapped row and a stub exception.
        payload["targets"][0]["status"] = "unmapped"
        payload["targets"][0]["bazel_label"] = None
        for exc in payload["exceptions"]:
            if exc["id"] == "RT-fuzz":
                exc["status"] = "stub"
                break
        bad_map.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        bad_ledger.write_text(
            bad_ledger.read_text(encoding="utf-8")
            + "\n| `graphforge-api` | `fake` | `example` | `x.rs` | — | `unmapped` | |\n",
            encoding="utf-8",
        )

        drifted = subprocess.run(
            ["python3", str(CHECK), "--map", str(bad_map), "--ledger", str(bad_ledger)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if drifted.returncode == 0:
            raise SystemExit("expected mutated ledger to fail closed")
        combined = (drifted.stdout + drifted.stderr).lower()
        if "unmapped" not in combined:
            raise SystemExit(f"expected unmapped failure:\n{drifted.stderr}")
        if "stub" not in combined and "unjustified" not in combined:
            raise SystemExit(f"expected stub/unjustified failure:\n{drifted.stderr}")

    print("bazel migration ledger check tests passed")


if __name__ == "__main__":
    main()
