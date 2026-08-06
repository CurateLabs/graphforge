#!/usr/bin/env python3
"""Unit tests for the Cargo/Bazel parity inventory gate (#6)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/cargo-bazel-parity-check.py"
PLATFORMS = ROOT / "tools/bazel/release/release_platforms.json"
MAP = ROOT / "tools/bazel/parity/migration_target_map.json"
RC = ROOT / "tests/contracts/binding-release-candidate-targets.json"


def main() -> None:
    ok = subprocess.run(
        [
            "python3",
            str(CHECK),
            "--mode",
            "inventory",
            "--skip-label-query",
            "--map",
            str(MAP),
            "--platforms",
            str(PLATFORMS),
            "--rc-contract",
            str(RC),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if ok.returncode != 0:
        raise SystemExit(f"expected inventory parity check to pass:\n{ok.stdout}\n{ok.stderr}")

    with tempfile.TemporaryDirectory(prefix="gf-parity-") as tmp:
        tmp_path = Path(tmp)
        bad_platforms = tmp_path / "bad-platforms.json"
        payload = json.loads(PLATFORMS.read_text(encoding="utf-8"))
        # Drop a certified Binding RC target from the Bazel model.
        payload["platforms"] = [
            entry for entry in payload["platforms"] if entry["id"] != "python-windows"
        ]
        bad_platforms.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        failed = subprocess.run(
            [
                "python3",
                str(CHECK),
                "--mode",
                "inventory",
                "--skip-label-query",
                "--map",
                str(MAP),
                "--platforms",
                str(bad_platforms),
                "--rc-contract",
                str(RC),
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if failed.returncode == 0:
            raise SystemExit("expected missing release platform to fail closed")
        if "python-windows" not in (failed.stdout + failed.stderr):
            raise SystemExit("expected python-windows missing-platform error:\n" + failed.stderr)

    print("cargo-bazel parity check tests passed")


if __name__ == "__main__":
    main()
