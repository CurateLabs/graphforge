#!/usr/bin/env python3
"""Validate and execute the finite M20 contract-gate ledger."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/m20-contract-matrix.json"
SCHEMA_INVENTORY = ROOT / "docs/reference/m20-schema-inventory.json"
GROUPS = {"rust", "python", "node"}
REQUIRED_CASES = {
    "graph-only-no-knowledge",
    "knowledge-enabled-empty",
    "knowledge-populated",
    "corrupt-knowledge",
    "future-knowledge",
    "persistent-reopen",
    "publication-failpoints",
    "idempotent-and-conflicting-uuid",
    "unsupported-pre-v1-rust",
    "unsupported-pre-v1-python",
    "unsupported-pre-v1-node",
    "m18-exhaustive-isolation",
    "m19-find-knowledge-states",
    "descriptor-direct-arrow-equivalence",
    "canonical-arrow-fingerprint",
    "cross-binding-contract",
}
FORBIDDEN_EXECUTION_WORDS = {"skip", "ignored", "ignore", "quarantine", "manual"}
CATALOG_FRAGMENTS = {
    "RankAlgorithm::ALL",
    "ClusterAlgorithm::ALL",
    "SimilarAlgorithm::ALL",
    "PathAlgorithm::ALL",
    "AnalyzeAlgorithm::ALL",
}


class GateError(RuntimeError):
    """A deterministic contract-gate validation failure."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_matrix() -> dict[str, Any]:
    try:
        value = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read M20 matrix: {error}") from error
    if not isinstance(value, dict):
        raise GateError("M20 matrix root must be an object")
    return value


def require_relative_file(value: object) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or Path(value).is_absolute()
        or ".." in Path(value).parts
    ):
        raise GateError(f"test path must be a safe repository-relative path: {value!r}")
    path = ROOT / value
    if not path.is_file():
        raise GateError(f"test source does not exist: {value}")
    return path


def validate_rust_test(source: str, symbol: str, label: str) -> None:
    match = re.search(
        rf"(?P<attrs>(?:#\[[^\]]+\]\s*)+)"
        rf"(?:pub\s+)?(?:async\s+)?fn\s+{re.escape(symbol)}\s*\(",
        source,
    )
    if match is None:
        raise GateError(f"{label}: Rust test function is absent")
    attributes = match.group("attrs")
    if "#[test]" not in attributes and "#[tokio::test]" not in attributes:
        raise GateError(f"{label}: Rust function is not a test")
    if "#[ignore" in attributes:
        raise GateError(f"{label}: ignored Rust tests cannot prove closure")


def validate_python_test(source: str, symbol: str, label: str, direct_call: bool) -> None:
    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        raise GateError(f"{label}: Python source does not parse: {error}") from error
    functions = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if symbol not in functions:
        raise GateError(f"{label}: Python test function is absent")
    if direct_call and len(re.findall(rf"\b{re.escape(symbol)}\s*\(", source)) < 2:
        raise GateError(f"{label}: direct smoke check is not invoked")


def validate_node_test(source: str, symbol: str, label: str, direct_call: bool) -> None:
    if direct_call:
        if re.search(rf"\bfunction\s+{re.escape(symbol)}\s*\(", source) is None:
            raise GateError(f"{label}: Node check function is absent")
        if re.search(rf"\btest\([^,\n]+,\s*{re.escape(symbol)}\s*\)", source) is None:
            raise GateError(f"{label}: Node check is not registered with node:test")
    elif re.search(rf"\btest\(\s*[\"']{re.escape(symbol)}[\"']", source) is None:
        raise GateError(f"{label}: Node test title is absent")
    if re.search(rf"\btest\.(?:skip|todo)\(\s*[\"']{re.escape(symbol)}[\"']", source):
        raise GateError(f"{label}: skipped Node tests cannot prove closure")


def validate_test(test: object, case_id: str) -> str:
    if not isinstance(test, dict):
        raise GateError(f"{case_id}: every test reference must be an object")
    kind = test.get("kind")
    symbol = test.get("symbol")
    if kind not in {"rust", "python", "python-call", "node", "node-call"}:
        raise GateError(f"{case_id}: unsupported test kind {kind!r}")
    if not isinstance(symbol, str) or not symbol:
        raise GateError(f"{case_id}: test symbol is required")
    path = require_relative_file(test.get("path"))
    source = path.read_text(encoding="utf-8")
    label = f"{case_id}:{path.relative_to(ROOT)}:{symbol}"
    if kind == "rust":
        validate_rust_test(source, symbol, label)
    elif kind in {"python", "python-call"}:
        validate_python_test(source, symbol, label, kind == "python-call")
    else:
        validate_node_test(source, symbol, label, kind == "node-call")
    return f"{path.relative_to(ROOT)}::{symbol}"


def validate_matrix() -> dict[str, Any]:
    matrix = load_matrix()
    if matrix.get("schema_version") != 1 or matrix.get("gate") != "M20":
        raise GateError("matrix must declare M20 schema_version 1")
    commands = matrix.get("command_groups")
    if not isinstance(commands, dict) or set(commands) != GROUPS:
        raise GateError("matrix command groups must be exactly rust, python, and node")
    for group, entries in commands.items():
        if not isinstance(entries, list) or not entries:
            raise GateError(f"{group}: command group must be non-empty")
        for command in entries:
            if not isinstance(command, str) or not command.strip():
                raise GateError(f"{group}: commands must be non-empty strings")
            lowered = set(re.findall(r"[a-z]+", command.lower()))
            if lowered & FORBIDDEN_EXECUTION_WORDS:
                raise GateError(f"{group}: closure commands cannot skip or quarantine tests")

    cases = matrix.get("cases")
    if not isinstance(cases, list):
        raise GateError("matrix cases must be an array")
    ids = [case.get("id") for case in cases if isinstance(case, dict)]
    if len(ids) != len(cases) or len(set(ids)) != len(ids):
        raise GateError("matrix case IDs must be present and unique")
    if set(ids) != REQUIRED_CASES:
        missing = sorted(REQUIRED_CASES - set(ids))
        extra = sorted(set(ids) - REQUIRED_CASES)
        raise GateError(f"matrix case omission/drift: missing={missing}, extra={extra}")

    surfaces: set[str] = set()
    test_ids: dict[str, list[str]] = {}
    for case in cases:
        case_id = case["id"]
        groups = case.get("command_groups")
        if not isinstance(groups, list) or not groups or not set(groups) <= GROUPS:
            raise GateError(f"{case_id}: invalid or empty command_groups")
        case_surfaces = case.get("surfaces")
        if not isinstance(case_surfaces, list) or not case_surfaces:
            raise GateError(f"{case_id}: surfaces must be non-empty")
        surfaces.update(case_surfaces)
        tests = case.get("tests")
        if not isinstance(tests, list) or not tests:
            raise GateError(f"{case_id}: exact test IDs are required")
        test_ids[case_id] = [validate_test(test, case_id) for test in tests]

    if not {"rust", "python", "node"} <= surfaces:
        raise GateError("matrix omits a public binding surface")

    isolation = (ROOT / "crates/gf-api/tests/knowledge_isolation.rs").read_text(encoding="utf-8")
    missing_catalog = sorted(
        fragment for fragment in CATALOG_FRAGMENTS if fragment not in isolation
    )
    if missing_catalog:
        raise GateError(f"M18 catalog omission check is incomplete: {missing_catalog}")
    partition_test = "typed_catalog_partition_is_unique_exhaustive_and_probes_unavailable_handlers"
    if partition_test not in isolation:
        raise GateError("M18 catalog partition test is absent")

    if not SCHEMA_INVENTORY.is_file():
        raise GateError("checked M20 schema inventory is absent")
    expected_digest = (
        (ROOT / "docs/reference/m20-schema-inventory.sha256").read_text(encoding="utf-8").split()[0]
    )
    if sha256(SCHEMA_INVENTORY) != expected_digest:
        raise GateError("checked M20 schema inventory digest does not match")
    return {"matrix": matrix, "test_ids": test_ids}


def run_command(command: str, log: Any) -> int:
    log.write(f"$ {command}\n")
    log.flush()
    completed = subprocess.run(
        ["bash", "-lc", command],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log.write(f"[exit {completed.returncode}]\n")
    log.flush()
    return completed.returncode


def run_group(group: str, output: Path) -> int:
    validated = validate_matrix()
    commands = validated["matrix"]["command_groups"][group]
    output.mkdir(parents=True, exist_ok=True)
    log_path = output / f"{group}.log"
    results: list[dict[str, Any]] = []
    with log_path.open("w", encoding="utf-8") as log:
        for command in commands:
            code = run_command(command, log)
            results.append({"command": command, "exit_code": code})
            if code != 0:
                break
    status = (
        "success"
        if len(results) == len(commands) and all(result["exit_code"] == 0 for result in results)
        else "failure"
    )
    fragment = {
        "group": group,
        "status": status,
        "commands": results,
        "log_sha256": sha256(log_path),
    }
    (output / f"{group}.json").write_text(
        json.dumps(fragment, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if status == "success" else 1


def tool_version(command: list[str]) -> str:
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    value = (completed.stdout or completed.stderr).strip().splitlines()
    return value[0] if value else "unavailable"


def build_report(sha: str, fragments: Path, output: Path) -> None:
    validated = validate_matrix()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise GateError("report SHA must be a full lowercase Git commit")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    if head != sha:
        raise GateError(f"report SHA {sha} does not match checkout {head}")

    group_reports: dict[str, Any] = {}
    for group in sorted(GROUPS):
        path = fragments / f"{group}.json"
        log_path = fragments / f"{group}.log"
        if not path.is_file():
            raise GateError(f"missing {group} result fragment")
        if not log_path.is_file():
            raise GateError(f"missing {group} command log")
        value = json.loads(path.read_text(encoding="utf-8"))
        if value.get("group") != group or value.get("status") != "success":
            raise GateError(f"{group} command group did not pass")
        command_results = value.get("commands")
        if not isinstance(command_results, list):
            raise GateError(f"{group} command results are absent")
        actual_commands = [result.get("command") for result in command_results]
        if actual_commands != validated["matrix"]["command_groups"][group]:
            raise GateError(f"{group} executed commands do not match the checked matrix")
        if any(result.get("exit_code") != 0 for result in command_results):
            raise GateError(f"{group} contains a failed command")
        if value.get("log_sha256") != sha256(log_path):
            raise GateError(f"{group} command log digest does not match")
        group_reports[group] = value

    matrix = validated["matrix"]
    cases = []
    for case in matrix["cases"]:
        cases.append(
            {
                "id": case["id"],
                "criterion": case["criterion"],
                "outcome": "success",
                "command_groups": case["command_groups"],
                "surfaces": case["surfaces"],
                "test_ids": validated["test_ids"][case["id"]],
            }
        )
    report = {
        "gate": "M20 Contract Gate",
        "schema_version": 1,
        "commit_sha": sha,
        "matrix_sha256": sha256(MATRIX_PATH),
        "schema_inventory_sha256": sha256(SCHEMA_INVENTORY),
        "toolchain": {
            "rust": tool_version(["rustc", "--version"]),
            "cargo": tool_version(["cargo", "--version"]),
            "python": tool_version(["python3", "--version"]),
            "node": tool_version(["node", "--version"]),
        },
        "command_groups": group_reports,
        "cases": cases,
        "summary": {"total": len(cases), "passed": len(cases), "failed": 0},
    }
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "m20-contract-gate-report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = sha256(report_path)
    (output / "m20-contract-gate-report.sha256").write_text(
        f"{digest}  {report_path.name}\n",
        encoding="utf-8",
    )


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    subcommands = value.add_subparsers(dest="command", required=True)
    subcommands.add_parser("validate")
    run = subcommands.add_parser("run-group")
    run.add_argument("--group", choices=sorted(GROUPS), required=True)
    run.add_argument("--output", type=Path, required=True)
    report = subcommands.add_parser("report")
    report.add_argument("--sha", required=True)
    report.add_argument("--fragments", type=Path, required=True)
    report.add_argument("--output", type=Path, required=True)
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "validate":
            validated = validate_matrix()
            print(f"M20 contract matrix valid: {len(validated['matrix']['cases'])} cases")
            return 0
        if args.command == "run-group":
            return run_group(args.group, args.output)
        build_report(args.sha, args.fragments, args.output)
        print("M20 contract report generated")
        return 0
    except (GateError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"M20 contract gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
