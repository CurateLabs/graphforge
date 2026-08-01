#!/usr/bin/env python3
"""Opt-in same-SHA Rust/Python/Node bulk-construction conformance gate (#2552).

This is release/conformance evidence, not a required pull-request CI Gate job.
Build or install current-commit native Python and Node artifacts before `run`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/bulk-construction-conformance.json"
PYTHON_TEST = ROOT / "crates/graphforge-bindings-py/tests/bulk_construction.py"
NODE_TEST = ROOT / "crates/graphforge-bindings-node/tests/bulk_construction.test.mjs"
PARITY_TEST = ROOT / "scripts/ci/bulk-construction-parity.py"
CASE_TIMEOUT_SECONDS = 900

REQUIRED_CASES: dict[str, tuple[str, list[str]]] = {
    "rust-bulk-construction-lib": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "bulk_construction::",
            "--",
            "--nocapture",
        ],
    ),
    "rust-release-load-bulk": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--test",
            "release_load_construction",
            "--",
            "--nocapture",
        ],
    ),
    "python-bulk-acceptance": (
        "python",
        ["python", "crates/graphforge-bindings-py/tests/bulk_construction.py"],
    ),
    "node-bulk-acceptance": (
        "node",
        [
            "pnpm",
            "--filter",
            "@curatelabs/graphforge",
            "exec",
            "node",
            "--test",
            "tests/bulk_construction.test.mjs",
        ],
    ),
    "cross-binding-bulk-parity": (
        "python",
        ["python", "scripts/ci/bulk-construction-parity.py"],
    ),
}


class GateError(RuntimeError):
    """Bulk-construction conformance validation or execution failure."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise GateError(f"{path} root must be an object")
    return value


def validate_matrix() -> dict[str, Any]:
    matrix = load_json(MATRIX_PATH)
    if (
        matrix.get("schema_version") != 1
        or matrix.get("contract") != "graphforge-bulk-construction-conformance/1"
        or matrix.get("issue") != 2552
        or matrix.get("parent_issue") != 2519
    ):
        raise GateError("bulk conformance matrix schema or issue mapping changed")
    if not isinstance(matrix.get("max_runtime_seconds"), int) or matrix["max_runtime_seconds"] <= 0:
        raise GateError("max_runtime_seconds must be a positive integer")
    for path in (PYTHON_TEST, NODE_TEST, PARITY_TEST):
        if not path.is_file():
            raise GateError(f"required acceptance/parity file missing: {path}")
    coverage = matrix.get("coverage")
    if not isinstance(coverage, list) or not coverage:
        raise GateError("coverage must be a non-empty array")
    required_coverage = {
        "empty",
        "single-row",
        "multi-row",
        "mixed-property",
        "identity",
        "endpoint",
        "malformed-input",
        "atomicity",
        "retry-conflict",
        "receipt",
        "reopen",
    }
    if set(coverage) != required_coverage:
        raise GateError(f"coverage must equal {sorted(required_coverage)}")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or not cases:
        raise GateError("cases must be a non-empty array")
    seen: set[str] = set()
    surfaces: set[str] = set()
    by_id: dict[str, dict[str, Any]] = {}
    for case in cases:
        if not isinstance(case, dict):
            raise GateError("each case must be an object")
        case_id = case.get("id")
        surface = case.get("surface")
        argv = case.get("argv")
        if not isinstance(case_id, str) or not case_id or case_id in seen:
            raise GateError("case ids must be unique non-empty strings")
        if surface not in {"rust", "python", "node"}:
            raise GateError(f"{case_id}: surface must be rust, python, or node")
        if (
            not isinstance(argv, list)
            or not argv
            or not all(isinstance(item, str) for item in argv)
        ):
            raise GateError(f"{case_id}: argv must be a non-empty string array")
        seen.add(case_id)
        surfaces.add(surface)
        by_id[case_id] = case
    if surfaces != {"rust", "python", "node"}:
        raise GateError("matrix must cover rust, python, and node surfaces")
    for required_id, (required_surface, required_argv) in REQUIRED_CASES.items():
        case = by_id.get(required_id)
        if case is None:
            raise GateError(f"required case missing: {required_id}")
        if case["surface"] != required_surface:
            raise GateError(
                f"{required_id}: surface must be {required_surface}, got {case['surface']}"
            )
        if case["argv"] != required_argv:
            raise GateError(f"{required_id}: argv must match the declared bulk command")
    # Omission check: every declared coverage category must remain grounded in
    # native acceptance/parity sources. Tokens may be category names or the
    # stable synonyms those sources already use for the same behavior.
    coverage_source_tokens = {
        "empty": ("empty",),
        "single-row": ("single-row",),
        "multi-row": ("multi-row",),
        "mixed-property": ("mixed-property",),
        "identity": ("identity", "entity_uuid"),
        "endpoint": ("endpoint",),
        "malformed-input": ("malformed-input", "malformed"),
        "atomicity": ("atomicity", "atomic"),
        "retry-conflict": ("retry-conflict", "idempotency"),
        "receipt": ("receipt",),
        "reopen": ("reopen",),
    }
    if set(coverage_source_tokens) != required_coverage:
        raise GateError("coverage omission token map must match required_coverage")
    for path in (PYTHON_TEST, NODE_TEST, PARITY_TEST):
        source = path.read_text(encoding="utf-8").lower()
        for category, tokens in coverage_source_tokens.items():
            if not any(token in source for token in tokens):
                raise GateError(
                    f"{path.relative_to(ROOT)}: omission check failed for "
                    f"coverage {category!r} (tokens={list(tokens)!r})"
                )
    return matrix


def git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return "unknown"
    return (completed.stdout or "").strip() or "unknown"


def run_case(
    case: dict[str, Any],
    env: dict[str, str],
    work: Path,
    log_dir: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    case_id = case["id"]
    argv = list(case["argv"])
    if argv[0] == "python":
        argv[0] = sys.executable
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise GateError(f"{case_id}: exceeded case timeout {timeout_seconds:.0f}s") from error
    duration_ms = int((time.monotonic() - started) * 1000)
    log_path = log_dir / f"{case_id}.log"
    log_path.write_text(
        (completed.stdout or "") + (completed.stderr or ""),
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise GateError(f"{case_id}: command failed exit={completed.returncode} log={log_path}")
    leftover_locks = sorted(path.name for path in work.rglob("*.lock") if path.is_file())
    if leftover_locks:
        raise GateError(f"{case_id}: leaked lock files under work root: {leftover_locks}")
    return {
        "id": case_id,
        "surface": case["surface"],
        "argv": argv,
        "exit_code": completed.returncode,
        "duration_ms": duration_ms,
        "log": str(log_path),
        "log_sha256": sha256(log_path),
        "outcome": "ok",
    }


def run_matrix(output: Path) -> int:
    matrix = validate_matrix()
    output.mkdir(parents=True, exist_ok=True)
    log_dir = output / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="gf-bulk-conformance-", dir=str(output)))
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "never")
    env["TMPDIR"] = str(work)
    env["TEMP"] = str(work)
    env["TMP"] = str(work)
    env["GF_BULK_CONFORMANCE_SHA"] = git_head()
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    failure: str | None = None
    try:
        for case in matrix["cases"]:
            results.append(run_case(case, env, work, log_dir, CASE_TIMEOUT_SECONDS))
            if time.monotonic() - started > matrix["max_runtime_seconds"]:
                raise GateError("matrix exceeded max_runtime_seconds")
    except (GateError, OSError) as error:
        failure = str(error)
    report = {
        "gate": "Bulk Construction Conformance",
        "schema_version": 1,
        "contract": matrix["contract"],
        "issue": 2552,
        "parent_issue": 2519,
        "commit": git_head(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "matrix_sha256": sha256(MATRIX_PATH),
        "cases": results,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "summary": {"cases": len(results), "passed": failure is None},
        "failure": failure,
    }
    (output / "bulk-construction-conformance-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if failure is not None:
        print(failure, file=sys.stderr)
        return 1
    print(json.dumps({"commit": report["commit"], "passed": True, "cases": len(results)}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="Validate the checked-in bulk conformance contract")
    run = sub.add_parser("run", help="Execute the same-SHA bulk conformance matrix")
    run.add_argument(
        "--output",
        type=Path,
        default=ROOT / "build" / "bulk-construction-conformance",
        help="Directory for logs and the JSON report",
    )
    args = parser.parse_args()
    if args.command == "validate":
        matrix = validate_matrix()
        print(
            json.dumps(
                {
                    "contract": matrix["contract"],
                    "issue": matrix["issue"],
                    "cases": len(matrix["cases"]),
                    "ok": True,
                }
            )
        )
        return 0
    return run_matrix(args.output.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
