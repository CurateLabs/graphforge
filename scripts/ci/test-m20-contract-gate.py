#!/usr/bin/env python3
"""Regression tests for the M20 closure-ledger validator."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/m20-contract-gate.py"
SPEC = importlib.util.spec_from_file_location("m20_contract_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load M20 contract gate")
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


def main() -> None:
    validated = GATE.validate_matrix()
    assert len(validated["matrix"]["cases"]) == 16

    missing = copy.deepcopy(validated["matrix"])
    missing["cases"].pop()
    expect_gate_error(missing)

    stale_test = copy.deepcopy(validated["matrix"])
    stale_test["cases"][0]["tests"][0]["symbol"] = "nonexistent_contract_test"
    expect_gate_error(stale_test)

    forbidden_command = copy.deepcopy(validated["matrix"])
    forbidden_command["command_groups"]["rust"][0] = "cargo test -- --ignored"
    expect_gate_error(forbidden_command)

    with tempfile.TemporaryDirectory() as directory:
        fragments = Path(directory, "fragments")
        fragments.mkdir()
        for group in sorted(GATE.GROUPS):
            log_path = fragments / f"{group}.log"
            log_path.write_text("verified\n", encoding="utf-8")
            (fragments / f"{group}.json").write_text(
                json.dumps(
                    {
                        "group": group,
                        "status": "success",
                        "commands": [
                            {"command": command, "exit_code": 0}
                            for command in validated["matrix"]["command_groups"][group]
                        ],
                        "log_sha256": GATE.sha256(log_path),
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
        report = json.loads((output / "m20-contract-gate-report.json").read_text())
        assert report["commit_sha"] == sha
        assert report["summary"] == {"total": 16, "passed": 16, "failed": 0}
        assert all(case["outcome"] == "success" for case in report["cases"])

        stale_fragment = json.loads((fragments / "node.json").read_text(encoding="utf-8"))
        stale_fragment["commands"][0]["command"] = "stale command"
        (fragments / "node.json").write_text(json.dumps(stale_fragment), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, output)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted commands that differ from the checked matrix")

    print("M20 contract gate tests passed")


if __name__ == "__main__":
    main()
