#!/usr/bin/env python3
"""Unit tests for the Cargo/Bazel parity inventory gate (#6 / #440)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/cargo-bazel-parity-check.py"
PLATFORMS = ROOT / "tools/bazel/release/release_platforms.json"
MAP = ROOT / "tools/bazel/parity/migration_target_map.json"
RC = ROOT / "tests/contracts/binding-release-candidate-targets.json"


def _load_parity_module():
    spec = importlib.util.spec_from_file_location("cargo_bazel_parity_check", CHECK)
    if spec is None or spec.loader is None:
        raise SystemExit(f"unable to load {CHECK}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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

    parity = _load_parity_module()
    mapped_tests = parity.mapped_labels(MAP, classes=parity.TEST_CLASSES)
    if not mapped_tests:
        raise SystemExit("expected mapped integration-test labels in migration_target_map.json")
    if "//crates/graphforge-api:clear" not in mapped_tests:
        raise SystemExit("expected //crates/graphforge-api:clear among mapped test-class labels")

    membership_ok = parity.check_suite_membership(mapped_tests, mapped_tests)
    if membership_ok:
        raise SystemExit(f"expected full suite coverage to pass: {membership_ok}")

    missing = "//crates/graphforge-api:clear"
    suite_without = set(mapped_tests) - {missing}
    membership_fail = parity.check_suite_membership(suite_without, mapped_tests)
    if not membership_fail:
        raise SystemExit("expected suite-membership check to fail closed when a mapped test is absent")
    if missing not in membership_fail[0]:
        raise SystemExit(f"expected missing-label error for {missing}: {membership_fail}")
    if "tests(//:ci_rust_tests)" not in membership_fail[0]:
        raise SystemExit(f"expected ci_rust_tests suite wording: {membership_fail}")

    print("cargo-bazel parity check tests passed")


if __name__ == "__main__":
    main()
