#!/usr/bin/env python3
"""Mutation tests for the opt-in bulk-construction conformance gate (#2552)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/bulk-construction-conformance.py"
SPEC = importlib.util.spec_from_file_location("bulk_construction_conformance", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load bulk construction conformance gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def reject(matrix: dict) -> None:
    original = GATE.MATRIX_PATH.read_text(encoding="utf-8")
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            path.write_text(json.dumps(matrix), encoding="utf-8")
            GATE.MATRIX_PATH = path
            try:
                GATE.validate_matrix()
            except GATE.GateError:
                return
            raise AssertionError("invalid bulk conformance matrix was accepted")
    finally:
        GATE.MATRIX_PATH = ROOT / "tests/contracts/bulk-construction-conformance.json"
        assert GATE.MATRIX_PATH.read_text(encoding="utf-8") == original


def assert_persistent_admission_lock_contract() -> None:
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        nested = work / "case"
        nested.mkdir()
        admission_lock = nested / f".graphforge-admission-{'a' * 64}.lock"
        admission_lock.touch()
        assert GATE.unexpected_lock_artifacts(work) == []

        uppercase_dir = work / "uppercase"
        uppercase_dir.mkdir()
        unexpected = [
            nested / "writer.lock",
            nested / f".graphforge-admission-{'a' * 63}.lock",
            uppercase_dir / f".graphforge-admission-{'A' * 64}.lock",
            nested / ".graphforge-admission-not-a-digest.lock",
        ]
        for path in unexpected:
            path.touch()

        expected = sorted(str(path.relative_to(work)) for path in unexpected)
        assert GATE.unexpected_lock_artifacts(work) == expected

        symlink = nested / f".graphforge-admission-{'b' * 64}.lock"
        try:
            symlink.symlink_to(admission_lock)
        except (NotImplementedError, OSError):
            pass
        else:
            assert GATE.unexpected_lock_artifacts(work) == sorted(
                [*expected, str(symlink.relative_to(work))]
            )


def main() -> None:
    matrix = GATE.validate_matrix()
    assert matrix["issue"] == 2552
    assert matrix["parent_issue"] == 2519
    assert {case["surface"] for case in matrix["cases"]} == {"rust", "python", "node"}

    bad = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    bad["parent_issue"] = 0
    reject(bad)

    missing_node = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    missing_node["cases"] = [case for case in missing_node["cases"] if case["surface"] != "node"]
    reject(missing_node)

    mutated = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    for case in mutated["cases"]:
        if case["id"] == "python-bulk-acceptance":
            case["argv"] = ["python", "crates/graphforge-bindings-py/tests/missing.py"]
            break
    else:
        raise AssertionError("python-bulk-acceptance case missing")
    reject(mutated)

    assert_persistent_admission_lock_contract()

    print("bulk construction conformance mutation tests passed")


if __name__ == "__main__":
    main()
