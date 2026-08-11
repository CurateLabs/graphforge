#!/usr/bin/env python3
"""Validate and execute the finite SHA-bound epistemic contract gate."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/epistemic-contract-matrix.json"
KNOWLEDGE_INVENTORY = ROOT / "docs/reference/knowledge-schema-inventory.json"
EPISTEMIC_INVENTORY = ROOT / "docs/reference/epistemic-schema-inventory.json"
GROUPS = {"rust", "python", "node"}
KNOWLEDGE_BASELINE_SHA = "8101c2c52246b903a39ff502dc325915974e4d69"
KNOWLEDGE_BASELINE_INVENTORY_SHA256 = (
    "ac69d81108121f3510390d619904fa49cddf08b92d3a26afd65ab870f1ae30b2"
)
REQUIRED_CASES = {
    "knowledge-frozen-baseline",
    "status-events",
    "reasoning-amendments",
    "supersession",
    "hypotheses",
    "confidence-never-selects",
    "transaction-snapshots",
    "valid-time",
    "capability-fail-closed",
    "atomic-reopen-recovery",
    "algorithm-resolved-equivalence",
    "projection-fingerprint",
    "attachment-failure-preserves-run",
    "cross-binding-contract",
    "graph-and-knowledge-preservation",
    "search-knowledge-isolation",
}
REQUIRED_FAMILIES = {
    "algorithm_interpretation_attachments",
    "assertion_status_events",
    "assertion_supersessions",
    "assertion_validity_events",
    "hypothesis_groups",
    "hypothesis_membership_events",
    "hypothesis_selection_events",
    "reasoning",
}
REQUIRED_ALGORITHM_SURFACES = {"rank", "cluster", "similar", "paths", "analyze"}
FORBIDDEN_WORDS = {"skip", "ignored", "ignore", "quarantine", "manual"}


class GateError(RuntimeError):
    """Deterministic epistemic gate validation failure."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read {label}: {error}") from error


def safe_source(value: object) -> tuple[Path, str]:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise GateError(f"unsafe test path: {value!r}")
    relative = Path(value)
    if ".." in relative.parts:
        raise GateError(f"unsafe test path: {value!r}")
    path = ROOT / relative
    if not path.is_file():
        raise GateError(f"test source does not exist: {value}")
    return path, path.read_text(encoding="utf-8")


def validate_test(test: object, case_id: str) -> str:
    if not isinstance(test, dict):
        raise GateError(f"{case_id}: test reference must be an object")
    kind = test.get("kind")
    symbol = test.get("symbol")
    if kind not in {"rust", "python-call", "node"}:
        raise GateError(f"{case_id}: unsupported test kind {kind!r}")
    if not isinstance(symbol, str) or not symbol:
        raise GateError(f"{case_id}: test symbol is required")
    path, source = safe_source(test.get("path"))
    label = f"{case_id}:{path.relative_to(ROOT)}:{symbol}"
    if kind == "rust":
        match = re.search(
            rf"(?P<attrs>(?:#\[[^\]]+\]\s*)+)(?:pub\s+)?(?:async\s+)?fn\s+"
            rf"{re.escape(symbol)}\s*\(",
            source,
        )
        if match is None or (
            "#[test]" not in match.group("attrs") and "#[tokio::test]" not in match.group("attrs")
        ):
            raise GateError(f"{label}: Rust test is absent")
        if "#[ignore" in match.group("attrs"):
            raise GateError(f"{label}: ignored tests cannot prove closure")
    elif kind == "python-call":
        if re.search(rf"^def\s+{re.escape(symbol)}\s*\(", source, re.MULTILINE) is None:
            raise GateError(f"{label}: Python check is absent")
        if len(re.findall(rf"\b{re.escape(symbol)}\s*\(", source)) < 2:
            raise GateError(f"{label}: Python check is not invoked")
    else:
        registered = re.search(rf"\btest\(\s*[\"']{re.escape(symbol)}[\"']", source)
        if registered is None:
            raise GateError(f"{label}: Node test title is absent")
        if re.search(rf"\btest\.(?:skip|todo)\(\s*[\"']{re.escape(symbol)}[\"']", source):
            raise GateError(f"{label}: skipped tests cannot prove closure")
    return f"{path.relative_to(ROOT)}::{symbol}"


def validate_matrix() -> dict[str, Any]:
    matrix = load_json(MATRIX_PATH, "epistemic matrix")
    if not isinstance(matrix, dict):
        raise GateError("epistemic matrix root must be an object")
    if matrix.get("schema_version") != 1 or matrix.get("gate") != "epistemic":
        raise GateError("matrix must declare epistemic schema_version 1")
    if matrix.get("knowledge_baseline_sha") != KNOWLEDGE_BASELINE_SHA:
        raise GateError("matrix knowledge baseline SHA drifted")
    commands = matrix.get("command_groups")
    if not isinstance(commands, dict) or set(commands) != GROUPS:
        raise GateError("command groups must be exactly rust, python, and node")
    for group, entries in commands.items():
        if not isinstance(entries, list) or not entries:
            raise GateError(f"{group}: command group must be non-empty")
        for command in entries:
            if not isinstance(command, str) or not command.strip():
                raise GateError(f"{group}: commands must be non-empty strings")
            if set(re.findall(r"[a-z]+", command.lower())) & FORBIDDEN_WORDS:
                raise GateError(f"{group}: closure commands cannot skip or quarantine tests")

    cases = matrix.get("cases")
    if not isinstance(cases, list):
        raise GateError("matrix cases must be an array")
    ids = [case.get("id") for case in cases if isinstance(case, dict)]
    if len(ids) != len(cases) or len(set(ids)) != len(ids):
        raise GateError("matrix case IDs must be present and unique")
    if set(ids) != REQUIRED_CASES:
        raise GateError(
            "matrix case omission/drift: "
            f"missing={sorted(REQUIRED_CASES - set(ids))}, "
            f"extra={sorted(set(ids) - REQUIRED_CASES)}"
        )

    surfaces: set[str] = set()
    test_ids: dict[str, list[str]] = {}
    for case in cases:
        case_id = case["id"]
        criterion = case.get("criterion")
        if not isinstance(criterion, str) or not criterion.strip():
            raise GateError(f"{case_id}: criterion is required")
        groups = case.get("command_groups")
        if not isinstance(groups, list) or not groups or not set(groups) <= GROUPS:
            raise GateError(f"{case_id}: invalid command_groups")
        case_surfaces = case.get("surfaces")
        if not isinstance(case_surfaces, list) or not case_surfaces:
            raise GateError(f"{case_id}: surfaces are required")
        surfaces.update(case_surfaces)
        tests = case.get("tests")
        if not isinstance(tests, list) or not tests:
            raise GateError(f"{case_id}: exact test IDs are required")
        test_ids[case_id] = [validate_test(test, case_id) for test in tests]
    if not {"rust", "python", "node"} <= surfaces:
        raise GateError("matrix omits a public binding surface")
    if not surfaces >= REQUIRED_ALGORITHM_SURFACES:
        raise GateError("matrix omits one or more public algorithm families")
    algorithm_case = next(case for case in cases if case["id"] == "algorithm-resolved-equivalence")
    if not set(algorithm_case["surfaces"]) >= REQUIRED_ALGORITHM_SURFACES:
        raise GateError("algorithm-resolved-equivalence omits a public algorithm family")

    if sha256(KNOWLEDGE_INVENTORY) != KNOWLEDGE_BASELINE_INVENTORY_SHA256:
        raise GateError("frozen knowledge inventory differs from the closed knowledge baseline")
    checked_knowledge = (
        (ROOT / "docs/reference/knowledge-schema-inventory.sha256").read_text().split()[0]
    )
    checked_epistemic = (
        (ROOT / "docs/reference/epistemic-schema-inventory.sha256").read_text().split()[0]
    )
    if (
        sha256(KNOWLEDGE_INVENTORY) != checked_knowledge
        or sha256(EPISTEMIC_INVENTORY) != checked_epistemic
    ):
        raise GateError("checked schema inventory digest mismatch")
    inventory = load_json(EPISTEMIC_INVENTORY, "epistemic inventory")
    families = {record.get("record_family") for record in inventory.get("records", [])}
    if families != REQUIRED_FAMILIES:
        raise GateError(
            f"epistemic family omission/drift: missing={sorted(REQUIRED_FAMILIES - families)}, "
            f"extra={sorted(families - REQUIRED_FAMILIES)}"
        )
    return {"matrix": matrix, "test_ids": test_ids}


def run_group(group: str, output: Path) -> int:
    validated = validate_matrix()
    output.mkdir(parents=True, exist_ok=True)
    log_path = output / f"{group}.log"
    results = []
    with log_path.open("w", encoding="utf-8") as log:
        for command in validated["matrix"]["command_groups"][group]:
            log.write(f"$ {command}\n")
            log.flush()
            completed = subprocess.run(
                ["bash", "-lc", command],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
            results.append({"command": command, "exit_code": completed.returncode})
            if completed.returncode:
                break
    success = len(results) == len(validated["matrix"]["command_groups"][group]) and all(
        result["exit_code"] == 0 for result in results
    )
    fragment = {
        "group": group,
        "status": "success" if success else "failure",
        "commands": results,
        "log_sha256": sha256(log_path),
    }
    (output / f"{group}.json").write_text(
        json.dumps(fragment, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if success else 1


def tool_version(command: list[str]) -> str:
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    lines = (completed.stdout or completed.stderr).strip().splitlines()
    return lines[0] if lines else "unavailable"


def build_report(sha: str, fragments: Path, output: Path) -> None:
    validated = validate_matrix()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise GateError("report SHA must be a full lowercase commit")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True
    ).stdout.strip()
    if head != sha:
        raise GateError(f"report SHA {sha} does not match checkout {head}")
    reports = {}
    for group in sorted(GROUPS):
        fragment_path = fragments / f"{group}.json"
        log_path = fragments / f"{group}.log"
        if not fragment_path.is_file() or not log_path.is_file():
            raise GateError(f"missing {group} evidence")
        fragment = load_json(fragment_path, f"{group} fragment")
        expected = validated["matrix"]["command_groups"][group]
        if (
            fragment.get("status") != "success"
            or [row.get("command") for row in fragment.get("commands", [])] != expected
            or any(row.get("exit_code") != 0 for row in fragment.get("commands", []))
            or fragment.get("log_sha256") != sha256(log_path)
        ):
            raise GateError(f"{group} evidence did not pass exact matrix commands")
        reports[group] = fragment

    output.mkdir(parents=True, exist_ok=True)
    shutil.copy2(KNOWLEDGE_INVENTORY, output / "knowledge-baseline-schema-inventory.json")
    shutil.copy2(EPISTEMIC_INVENTORY, output / "epistemic-schema-inventory.json")
    cases = [
        {
            "id": case["id"],
            "criterion": case["criterion"],
            "outcome": "success",
            "command_groups": case["command_groups"],
            "surfaces": case["surfaces"],
            "test_ids": validated["test_ids"][case["id"]],
        }
        for case in validated["matrix"]["cases"]
    ]
    report = {
        "gate": "Epistemic Contract Gate",
        "schema_version": 1,
        "commit_sha": sha,
        "knowledge_baseline_sha": KNOWLEDGE_BASELINE_SHA,
        "matrix_sha256": sha256(MATRIX_PATH),
        "knowledge_schema_inventory_sha256": sha256(KNOWLEDGE_INVENTORY),
        "epistemic_schema_inventory_sha256": sha256(EPISTEMIC_INVENTORY),
        "toolchain": {
            "rust": tool_version(["rustc", "--version"]),
            "cargo": tool_version(["cargo", "--version"]),
            "python": tool_version(["python3", "--version"]),
            "node": tool_version(["node", "--version"]),
        },
        "command_groups": reports,
        "cases": cases,
        "summary": {"total": len(cases), "passed": len(cases), "failed": 0},
    }
    report_path = output / "epistemic-contract-gate-report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "epistemic-contract-gate-report.sha256").write_text(
        f"{sha256(report_path)}  {report_path.name}\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate")
    run = commands.add_parser("run-group")
    run.add_argument("--group", choices=sorted(GROUPS), required=True)
    run.add_argument("--output", type=Path, required=True)
    report = commands.add_parser("report")
    report.add_argument("--sha", required=True)
    report.add_argument("--fragments", type=Path, required=True)
    report.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate":
            validated = validate_matrix()
            print(f"epistemic contract matrix valid: {len(validated['matrix']['cases'])} cases")
            return 0
        if args.command == "run-group":
            return run_group(args.group, args.output)
        build_report(args.sha, args.fragments, args.output)
        print("epistemic contract report generated")
        return 0
    except (GateError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"epistemic contract gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
