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

    # Suite-membership helper: a mapped integration-test outside //:ci_rust_tests
    # must fail closed (unit-test the predicate without requiring a full query of
    # a mutated workspace — inject a synthetic map + stub suite set).
    from importlib import util as import_util

    spec = import_util.spec_from_file_location("parity_check", CHECK)
    assert spec and spec.loader
    mod = import_util.module_from_spec(spec)
    # Load only the pure helpers we need by executing the module.
    spec.loader.exec_module(mod)

    with tempfile.TemporaryDirectory(prefix="gf-parity-suite-") as tmp:
        tmp_path = Path(tmp)
        map_path = tmp_path / "map.json"
        map_payload = {
            "targets": [
                {
                    "package": "graphforge-api",
                    "target": "orphan_test",
                    "class": "integration-test",
                    "status": "mapped",
                    "bazel_label": "//crates/graphforge-api:orphan_test",
                }
            ]
        }
        map_path.write_text(json.dumps(map_payload), encoding="utf-8")

        real_run = mod.run

        def fake_run(cmd, cwd=None):
            if len(cmd) >= 3 and cmd[0] == "bazelisk" and cmd[1] == "query":
                # Empty suite → orphan label is missing.
                return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
            return real_run(cmd, cwd=cwd)

        mod.run = fake_run  # type: ignore[method-assign]
        try:
            suite_errors = mod.check_suite_membership(ROOT, map_path)
        finally:
            mod.run = real_run  # type: ignore[method-assign]

        if not suite_errors or "orphan_test" not in suite_errors[0]:
            raise SystemExit(
                "expected suite-membership check to report orphan_test, got:\n" + repr(suite_errors)
            )

    print("cargo-bazel parity check tests passed")


if __name__ == "__main__":
    main()
