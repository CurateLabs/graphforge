#!/usr/bin/env python3
"""Regression tests for the checkpoint recovery acceptance-ledger validator."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/checkpoint-recovery-gate.py"
SPEC = importlib.util.spec_from_file_location("checkpoint_recovery_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load checkpoint recovery gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def run_matrix(matrix: dict[str, object]) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory, "matrix.json")
        path.write_text(json.dumps(matrix), encoding="utf-8")
        original = GATE.MATRIX_PATH
        GATE.MATRIX_PATH = path
        try:
            return GATE.validate_matrix()
        finally:
            GATE.MATRIX_PATH = original


def expect_gate_error(matrix: dict[str, object]) -> None:
    try:
        run_matrix(matrix)
    except GATE.GateError:
        return
    raise AssertionError("invalid checkpoint matrix was accepted")


def main() -> None:
    validated = GATE.validate_matrix()
    matrix = validated["matrix"]
    assert len(matrix["cases"]) == 13

    missing = copy.deepcopy(matrix)
    missing["cases"].pop()
    expect_gate_error(missing)

    stale = copy.deepcopy(matrix)
    stale["cases"][0]["tests"][0]["symbol"] = "absent_checkpoint_test"
    expect_gate_error(stale)

    unsafe = copy.deepcopy(matrix)
    unsafe["cases"][0]["tests"][0]["path"] = "../outside.rs"
    expect_gate_error(unsafe)

    unsupported = copy.deepcopy(matrix)
    unsupported["cases"][0]["tests"][0]["kind"] = "shell"
    expect_gate_error(unsupported)

    skipped = copy.deepcopy(matrix)
    skipped["command_groups"]["rust-storage"] = "cargo test -- --ignored"
    expect_gate_error(skipped)

    missing_shape = copy.deepcopy(matrix)
    for case in missing_shape["cases"]:
        case["workspace_shapes"] = [
            shape for shape in case["workspace_shapes"] if shape != "strict"
        ]
    expect_gate_error(missing_shape)

    missing_expected_contract = copy.deepcopy(matrix)
    del missing_expected_contract["cases"][0]["expected_errors"]
    expect_gate_error(missing_expected_contract)

    unproved_contract = copy.deepcopy(matrix)
    unproved_contract["cases"][0]["expected_errors"].append("GF_NOT_ASSERTED")
    expect_gate_error(unproved_contract)

    incomplete_evidence = copy.deepcopy(matrix)
    incomplete_evidence["cases"][-1]["expected_artifacts"].pop()
    expect_gate_error(incomplete_evidence)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", dir=ROOT, delete=False, encoding="utf-8"
    ) as fixture:
        fixture.write("def unrelated():\n    sleep(1)\n\ndef main():\n    return None\n\nmain()\n")
        fixture_path = Path(fixture.name)
    try:
        scoped_timing = copy.deepcopy(matrix)
        scoped_timing["cases"][-1]["tests"] = [
            {
                "kind": "python-call",
                "path": str(fixture_path.relative_to(ROOT)),
                "symbol": "main",
            }
        ]
        run_matrix(scoped_timing)
        fixture_path.write_text("def main():\n    sleep(1)\n\nmain()\n", encoding="utf-8")
        expect_gate_error(scoped_timing)
    finally:
        fixture_path.unlink()

    print("checkpoint recovery gate tests passed")


if __name__ == "__main__":
    main()
