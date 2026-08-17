#!/usr/bin/env python3
"""Required short deterministic concurrency matrix for pull requests (#2417)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/concurrency-short-matrix.json"
LEDGER_PATH = ROOT / "tests/contracts/concurrency-recovery-matrix.json"
PYTHON_TEST = ROOT / "crates/graphforge-bindings-py/tests/concurrency_parity.py"
NODE_TEST = ROOT / "crates/graphforge-bindings-node/tests/concurrency-parity.test.mjs"
CASE_TIMEOUT_SECONDS = 300
FORBIDDEN_TIMING = re.compile(r"\b(?:time\.sleep|asyncio\.sleep|setTimeout)\s*\(")
PERSISTENT_ADMISSION_LOCK_NAME = re.compile(r"\.graphforge-admission-[0-9a-f]{64}\.lock\Z")

# Required id → (surface, argv). Additional cases are allowed; these must match exactly.
REQUIRED_CASES: dict[str, tuple[str, list[str]]] = {
    "rust-same-instance": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "same_process_concurrency_tests::independent_instances_and_one_instance_reads_are_deterministic",
            "--",
            "--exact",
        ],
    ),
    "rust-cross-session": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "same_process_concurrency_tests::cross_session_reads_are_canonically_equal",
            "--",
            "--exact",
        ],
    ),
    "rust-nested-runtime": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "same_process_concurrency_tests::synchronous_calls_complete_inside_existing_tokio_runtime",
            "--",
            "--exact",
        ],
    ),
    "rust-stream-drop": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "stream_cancellation_isolation_tests::early_stream_drop_does_not_truncate_concurrent_peer",
            "--",
            "--exact",
        ],
    ),
    "rust-token-cancellation": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "stream_cancellation_isolation_tests::cooperative_token_cancellation_does_not_cancel_concurrent_peer",
            "--",
            "--exact",
        ],
    ),
    "rust-shared-read-write": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "shared_directory_semantics_tests::shared_directory_reads_pin_complete_generations_and_reopen_sees_commit",
            "--",
            "--exact",
        ],
    ),
    "rust-competing-writer": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "shared_directory_semantics_tests::competing_writer_fails_before_its_staging_or_publication",
            "--",
            "--exact",
        ],
    ),
    "rust-killed-writer": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "multi_process_publication_tests::killed_staged_child_releases_lock_and_recovers_without_partial_generation",
            "--",
            "--exact",
        ],
    ),
    "rust-published-child": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "multi_process_publication_tests::published_child_is_visible_only_to_fresh_current_reader",
            "--",
            "--exact",
        ],
    ),
    "rust-composite-kill-reopen": (
        "rust",
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "composite_recovery_tests::composite_kill_reopen_matrix_never_exposes_mixed_state",
            "--",
            "--exact",
        ],
    ),
    "python-concurrency-parity": (
        "python",
        ["python", "crates/graphforge-bindings-py/tests/concurrency_parity.py"],
    ),
    "node-concurrency-parity": (
        "node",
        [
            "pnpm",
            "--filter",
            "@curatelabs/graphforge",
            "exec",
            "node",
            "--test",
            "tests/concurrency-parity.test.mjs",
        ],
    ),
}


class GateError(RuntimeError):
    """Short concurrency gate validation or execution failure."""


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
        or matrix.get("contract") != "graphforge-concurrency-short-gate/1"
        or matrix.get("issue") != 2417
        or matrix.get("parent_issue") != 1720
    ):
        raise GateError("short concurrency matrix schema or issue mapping changed")
    if not isinstance(matrix.get("max_runtime_seconds"), int) or matrix["max_runtime_seconds"] <= 0:
        raise GateError("max_runtime_seconds must be a positive integer")
    if matrix.get("required_ci_job") != "Test Suite / Concurrency Matrix":
        raise GateError("required CI job identity changed")
    if not LEDGER_PATH.is_file():
        raise GateError("Rust concurrency recovery ledger is absent")
    if not PYTHON_TEST.is_file() or not NODE_TEST.is_file():
        raise GateError("Python or Node concurrency parity tests are absent")
    for path in (PYTHON_TEST, NODE_TEST):
        source = path.read_text(encoding="utf-8")
        if FORBIDDEN_TIMING.search(source):
            raise GateError(f"{path.relative_to(ROOT)}: sleep-based timing is forbidden")
        if "DEADLINE" not in source and "deadline" not in source:
            raise GateError(f"{path.relative_to(ROOT)}: bounded deadline evidence is required")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or not cases:
        raise GateError("short matrix cases must be a non-empty array")
    seen: set[str] = set()
    surfaces = set()
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
        raise GateError("short matrix must cover rust, python, and node surfaces")
    for required_id, (required_surface, required_argv) in REQUIRED_CASES.items():
        case = by_id.get(required_id)
        if case is None:
            raise GateError(f"required case missing: {required_id}")
        if case["surface"] != required_surface:
            raise GateError(
                f"{required_id}: surface must be {required_surface}, got {case['surface']}"
            )
        if case["argv"] != required_argv:
            raise GateError(f"{required_id}: argv must match the declared concurrency command")
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


def unexpected_lock_artifacts(work: Path) -> list[str]:
    """Return lock artifacts other than durable lifecycle rendezvous files."""
    unexpected = []
    for path in work.rglob("*.lock"):
        is_file = path.is_file()
        is_symlink = path.is_symlink()
        if not is_file and not is_symlink:
            continue
        if is_file and not is_symlink and PERSISTENT_ADMISSION_LOCK_NAME.fullmatch(path.name):
            continue
        unexpected.append(str(path.relative_to(work)))
    return sorted(unexpected)


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
    leftover_locks = unexpected_lock_artifacts(work)
    leftover_staging = sorted(
        str(path.relative_to(work))
        for path in work.rglob("*")
        if path.is_dir() and path.name in {"transactions", "generations"}
    )
    if leftover_locks or leftover_staging:
        raise GateError(
            f"{case_id}: leaked transient lock/staging under work root "
            f"locks={leftover_locks} staging={leftover_staging}"
        )
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


def write_failure_report(
    output: Path,
    *,
    results: list[dict[str, Any]],
    failure: str,
    started: float,
    matrix: dict[str, Any],
) -> None:
    report = {
        "gate": "Concurrency Short Matrix",
        "schema_version": 1,
        "contract": matrix["contract"],
        "commit": git_head(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "matrix_sha256": sha256(MATRIX_PATH),
        "ledger_sha256": sha256(LEDGER_PATH),
        "cases": results,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "summary": {"cases": len(results), "passed": False},
        "failure": failure,
    }
    (output / "concurrency-short-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = ["# Reproduce failed short concurrency matrix", f"# failure: {failure}"]
    for case in results:
        argv = case.get("argv")
        if isinstance(argv, list):
            lines.append(" ".join(str(item) for item in argv))
        elif case.get("id"):
            lines.append(str(case["id"]))
    lines.append(f"python3 scripts/ci/concurrency-short-gate.py run --output {output}")
    (output / "reproduction.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_matrix(output: Path) -> int:
    matrix = validate_matrix()
    output.mkdir(parents=True, exist_ok=True)
    log_dir = output / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="gf-concurrency-short-", dir=str(output)))
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "never")
    # Direct tempfile-backed workloads into the scanned work root.
    env["TMPDIR"] = str(work)
    env["TEMP"] = str(work)
    env["TMP"] = str(work)
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    failure: str | None = None
    try:
        for case in matrix["cases"]:
            remaining_seconds = matrix["max_runtime_seconds"] - (time.monotonic() - started)
            if remaining_seconds <= 0:
                raise GateError("short matrix exceeded bounded runtime")
            results.append(
                run_case(
                    case,
                    env,
                    work,
                    log_dir,
                    min(float(CASE_TIMEOUT_SECONDS), remaining_seconds),
                )
            )
        report = {
            "gate": "Concurrency Short Matrix",
            "schema_version": 1,
            "contract": matrix["contract"],
            "commit": git_head(),
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "matrix_sha256": sha256(MATRIX_PATH),
            "ledger_sha256": sha256(LEDGER_PATH),
            "cases": results,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "summary": {"cases": len(results), "passed": True},
        }
        report_path = output / "concurrency-short-report.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"concurrency short gate passed: {len(results)} cases in {report['duration_ms']}ms")
        return 0
    except GateError as error:
        failure = str(error)
        write_failure_report(
            output, results=results, failure=failure, started=started, matrix=matrix
        )
        raise
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("validate")
    run = subparsers.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "run":
            return run_matrix(args.output)
        validate_matrix()
    except GateError as error:
        print(f"concurrency short gate failed: {error}", file=sys.stderr)
        return 1
    print("concurrency short matrix valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
