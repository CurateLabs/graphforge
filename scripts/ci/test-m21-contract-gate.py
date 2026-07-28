#!/usr/bin/env python3
"""Regression tests for the M21 closure-ledger validator."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/m21-contract-gate.py"
SPEC = importlib.util.spec_from_file_location("m21_contract_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load M21 contract gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def expect_gate_error(matrix: dict[str, object]) -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory, "matrix.json")
        path.write_text(json.dumps(matrix), encoding="utf-8")
        original = GATE.MATRIX_PATH
        GATE.MATRIX_PATH = path
        try:
            try:
                GATE.validate_matrix()
            except GATE.GateError:
                return
            raise AssertionError("invalid matrix was accepted")
        finally:
            GATE.MATRIX_PATH = original


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    validated = GATE.validate_matrix()
    require(len(validated["matrix"]["cases"]) == 16, "expected 16 gate cases")

    missing = copy.deepcopy(validated["matrix"])
    missing["cases"].pop()
    expect_gate_error(missing)

    stale_test = copy.deepcopy(validated["matrix"])
    stale_test["cases"][0]["tests"][0]["symbol"] = "absent_contract_test"
    expect_gate_error(stale_test)

    wrong_baseline = copy.deepcopy(validated["matrix"])
    wrong_baseline["m20_baseline_sha"] = "0" * 40
    expect_gate_error(wrong_baseline)

    forbidden = copy.deepcopy(validated["matrix"])
    forbidden["command_groups"]["rust"][0] = "cargo test -- --ignored"
    expect_gate_error(forbidden)

    missing_surface = copy.deepcopy(validated["matrix"])
    for case in missing_surface["cases"]:
        case["surfaces"] = [surface for surface in case["surfaces"] if surface != "analyze"]
    expect_gate_error(missing_surface)

    with tempfile.TemporaryDirectory() as directory:
        fragments = Path(directory, "fragments")
        fragments.mkdir()
        for group in sorted(GATE.GROUPS):
            log = fragments / f"{group}.log"
            log.write_text("verified\n", encoding="utf-8")
            (fragments / f"{group}.json").write_text(
                json.dumps(
                    {
                        "group": group,
                        "status": "success",
                        "commands": [
                            {"command": command, "exit_code": 0}
                            for command in validated["matrix"]["command_groups"][group]
                        ],
                        "log_sha256": GATE.sha256(log),
                    }
                ),
                encoding="utf-8",
            )
        output = Path(directory, "report")
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        GATE.build_report(sha, fragments, output)
        report = json.loads((output / "m21-contract-gate-report.json").read_text())
        require(report["commit_sha"] == sha, "report SHA mismatch")
        require(
            report["m20_baseline_sha"] == GATE.M20_BASELINE_SHA,
            "M20 baseline SHA mismatch",
        )
        require(
            report["summary"] == {"total": 16, "passed": 16, "failed": 0},
            "report summary mismatch",
        )
        require(
            (output / "m20-baseline-schema-inventory.json").is_file(),
            "M20 schema evidence missing",
        )
        require(
            (output / "m21-schema-inventory.json").is_file(),
            "M21 schema evidence missing",
        )

        stale = json.loads((fragments / "node.json").read_text(encoding="utf-8"))
        stale["commands"][0]["command"] = "stale command"
        (fragments / "node.json").write_text(json.dumps(stale), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, output)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted commands outside the checked matrix")

    print("M21 contract gate tests passed")


if __name__ == "__main__":
    main()
