#!/usr/bin/env python3
"""Mutation tests for the required short concurrency matrix gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/concurrency-short-gate.py"
SPEC = importlib.util.spec_from_file_location("concurrency_short_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load concurrency short gate")
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
            raise AssertionError("invalid short concurrency matrix was accepted")
    finally:
        GATE.MATRIX_PATH = ROOT / "tests/contracts/concurrency-short-matrix.json"
        assert GATE.MATRIX_PATH.read_text(encoding="utf-8") == original


def assert_persistent_admission_lock_contract() -> None:
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        nested = work / "case"
        nested.mkdir()
        admission_lock = nested / f".graphforge-admission-{'a' * 64}.lock"
        admission_lock.touch()
        rewrite_lock = nested / ".graphforge-rewrite.lock"
        rewrite_lock.touch()
        assert GATE.unexpected_lock_artifacts(work) == []

        uppercase_dir = work / "uppercase"
        uppercase_dir.mkdir()
        unexpected = [
            nested / "writer.lock",
            nested / f".graphforge-admission-{'a' * 63}.lock",
            uppercase_dir / f".graphforge-admission-{'A' * 64}.lock",
            nested / ".graphforge-admission-not-a-digest.lock",
            nested / ".graphforge-rewrite-extra.lock",
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

        rewrite_symlink = uppercase_dir / ".graphforge-rewrite.lock"
        try:
            rewrite_symlink.symlink_to(rewrite_lock)
        except (NotImplementedError, OSError):
            pass
        else:
            expected_symlinks = [str(rewrite_symlink.relative_to(work))]
            if symlink.is_symlink():
                expected_symlinks.append(str(symlink.relative_to(work)))
            assert GATE.unexpected_lock_artifacts(work) == sorted([*expected, *expected_symlinks])


def main() -> None:
    matrix = GATE.validate_matrix()
    assert matrix["issue"] == 2417
    assert any(case["surface"] == "python" for case in matrix["cases"])
    assert any(case["surface"] == "node" for case in matrix["cases"])

    bad = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    bad["required_ci_job"] = "Test Suite / CI Gate"
    reject(bad)

    missing_node = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    missing_node["cases"] = [case for case in missing_node["cases"] if case["surface"] != "node"]
    reject(missing_node)

    mutated_argv = json.loads(GATE.MATRIX_PATH.read_text(encoding="utf-8"))
    for case in mutated_argv["cases"]:
        if case["id"] == "rust-same-instance":
            case["argv"] = [
                "cargo",
                "test",
                "-p",
                "graphforge-api",
                "--lib",
                "unrelated_ok_test",
                "--",
                "--exact",
            ]
            break
    else:
        raise AssertionError("required rust-same-instance case missing from matrix")
    reject(mutated_argv)

    assert_persistent_admission_lock_contract()

    print("concurrency short gate mutation tests passed")


if __name__ == "__main__":
    main()
