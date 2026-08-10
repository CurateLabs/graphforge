#!/usr/bin/env python3
"""Validate the finite deterministic checkpoint recovery acceptance ledger."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/checkpoint-recovery-matrix.json"
REQUIRED_LAYERS = {"rust-storage", "rust-api", "python", "node", "cli"}
REQUIRED_SHAPES = {
    "graph-only",
    "ontology-free",
    "emergent",
    "advisory",
    "strict",
    "knowledge",
    "epistemic",
}
REQUIRED_CASES = {
    "checkpoint-registry-lifecycle",
    "checkpoint-pin-lease-cleanup",
    "complete-workspace-shapes",
    "pinned-view-current-advance",
    "diff-pagination-cancellation-bounds",
    "revert-publication-failpoints",
    "registry-publication-failpoints",
    "corrupt-future-dangling-rejection",
    "revert-identity-and-visibility",
    "python-same-sha-surface",
    "node-same-sha-surface",
    "cli-same-sha-surface",
    "same-sha-artifact-evidence",
}
FORBIDDEN_COMMAND_WORDS = {"ignore", "ignored", "skip", "quarantine"}


class GateError(RuntimeError):
    """A deterministic checkpoint-ledger validation failure."""


def load_matrix() -> dict[str, Any]:
    try:
        value = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read checkpoint matrix: {error}") from error
    if not isinstance(value, dict):
        raise GateError("checkpoint matrix root must be an object")
    return value


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


def braced_scope(source: str, opening: int, label: str) -> str:
    """Return one brace-delimited definition, ignoring braces inside strings."""
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(opening, len(source)):
        character = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {'"', "'", "`"}:
            quote = character
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return source[opening : index + 1]
    raise GateError(f"{label}: test definition has no closing brace")


def indented_scope(source: str, start: int) -> str:
    lines = source[start:].splitlines(keepends=True)
    header_indent = len(lines[0]) - len(lines[0].lstrip())
    end = 1
    while end < len(lines):
        line = lines[end]
        if line.strip() and len(line) - len(line.lstrip()) <= header_indent:
            break
        end += 1
    return "".join(lines[:end])


def validate_test(test: object, case_id: str) -> tuple[str, str]:
    if not isinstance(test, dict):
        raise GateError(f"{case_id}: test reference must be an object")
    kind = test.get("kind")
    symbol = test.get("symbol")
    if kind not in {"rust", "python-call", "node", "workflow-job"}:
        raise GateError(f"{case_id}: unsupported test kind {kind!r}")
    if not isinstance(symbol, str) or not symbol:
        raise GateError(f"{case_id}: exact test symbol is required")
    path, source = safe_source(test.get("path"))
    label = f"{case_id}:{path.relative_to(ROOT)}:{symbol}"
    if kind == "rust":
        match = re.search(
            rf"(?P<attrs>(?:#\[[^\]]+\]\s*)+)(?:pub\s+)?(?:async\s+)?fn\s+{re.escape(symbol)}\s*\(",
            source,
        )
        if match is None or not any(
            marker in match.group("attrs") for marker in ("#[test]", "#[tokio::test]")
        ):
            raise GateError(f"{label}: Rust test is absent")
        if "#[ignore" in match.group("attrs"):
            raise GateError(f"{label}: ignored helpers cannot be acceptance evidence")
        opening = source.find("{", match.end())
        if opening < 0:
            raise GateError(f"{label}: Rust test has no body")
        scope = match.group("attrs") + braced_scope(source, opening, label)
    elif kind == "python-call":
        match = re.search(rf"^def\s+{re.escape(symbol)}\s*\(", source, re.MULTILINE)
        if match is None:
            raise GateError(f"{label}: Python check is absent")
        if len(re.findall(rf"\b{re.escape(symbol)}\s*\(", source)) < 2:
            raise GateError(f"{label}: Python check is not invoked")
        scope = indented_scope(source, match.start())
    elif kind == "node":
        match = re.search(rf"\btest\(\s*[\"']{re.escape(symbol)}[\"']", source)
        if match is None:
            raise GateError(f"{label}: Node test title is absent")
        if re.search(rf"\btest\.(?:skip|todo)\(\s*[\"']{re.escape(symbol)}[\"']", source):
            raise GateError(f"{label}: skipped Node test cannot prove closure")
        opening = source.find("{", match.end())
        if opening < 0:
            raise GateError(f"{label}: Node test has no body")
        scope = braced_scope(source, opening, label)
    else:
        match = re.search(rf"^  {re.escape(symbol)}:\s*$", source, re.MULTILINE)
        if match is None:
            raise GateError(f"{label}: workflow evidence job is absent")
        scope = indented_scope(source, match.start())
    if re.search(r"\b(?:sleep|setTimeout)\s*\(", scope):
        raise GateError(f"{label}: timing sleeps cannot prove deterministic closure")
    return f"{path.relative_to(ROOT)}::{symbol}", scope


def validate_matrix() -> dict[str, Any]:
    matrix = load_matrix()
    if (
        matrix.get("schema_version") != 1
        or matrix.get("contract") != "graphforge-checkpoint-recovery/1"
        or matrix.get("issue") != 2481
    ):
        raise GateError("matrix must declare checkpoint recovery schema 1 for issue 2481")
    commands = matrix.get("command_groups")
    if not isinstance(commands, dict) or set(commands) != REQUIRED_LAYERS:
        raise GateError("command groups must be exactly the five required layers")
    for layer, command in commands.items():
        if not isinstance(command, str) or not command.strip():
            raise GateError(f"{layer}: command must be a non-empty string")
        if set(re.findall(r"[a-z]+", command.lower())) & FORBIDDEN_COMMAND_WORDS:
            raise GateError(f"{layer}: command cannot skip or quarantine acceptance tests")

    cases = matrix.get("cases")
    if not isinstance(cases, list):
        raise GateError("matrix cases must be an array")
    ids = [case.get("id") for case in cases if isinstance(case, dict)]
    if len(ids) != len(cases) or len(set(ids)) != len(ids) or set(ids) != REQUIRED_CASES:
        raise GateError("matrix case IDs are missing, duplicated, or outside the frozen ledger")

    all_shapes: set[str] = set()
    all_layers: set[str] = set()
    test_ids: dict[str, list[str]] = {}
    for case in cases:
        case_id = case["id"]
        layer = case.get("layer")
        if layer not in REQUIRED_LAYERS:
            raise GateError(f"{case_id}: invalid layer")
        all_layers.add(layer)
        shapes = case.get("workspace_shapes")
        if not isinstance(shapes, list) or not shapes or not set(shapes) <= REQUIRED_SHAPES:
            raise GateError(f"{case_id}: workspace shapes are absent or invalid")
        all_shapes.update(shapes)
        if not isinstance(case.get("criterion"), str) or not case["criterion"].strip():
            raise GateError(f"{case_id}: deterministic criterion is required")
        for field in ("expected_columns", "expected_errors"):
            values = case.get(field)
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise GateError(f"{case_id}: {field} must be an array of strings")
        if case_id == "same-sha-artifact-evidence":
            artifacts = case.get("expected_artifacts")
            if not isinstance(artifacts, list) or len(artifacts) != 5:
                raise GateError(f"{case_id}: exact SHA-bound artifacts are required")
        tests = case.get("tests")
        if not isinstance(tests, list) or not tests:
            raise GateError(f"{case_id}: exact tests are required")
        validated_tests = [validate_test(test, case_id) for test in tests]
        test_ids[case_id] = [test_id for test_id, _ in validated_tests]
        scoped_source = "\n".join(scope for _, scope in validated_tests)
        for field in ("expected_columns", "expected_errors"):
            missing = [value for value in case[field] if value not in scoped_source]
            if missing:
                raise GateError(f"{case_id}: {field} are not asserted by scoped tests: {missing}")
    if all_layers != REQUIRED_LAYERS:
        raise GateError("matrix omits a required public or Rust layer")
    if all_shapes != REQUIRED_SHAPES:
        raise GateError("matrix omits a required workspace shape")
    return {"matrix": matrix, "test_ids": test_ids}


def main() -> int:
    try:
        validated = validate_matrix()
    except GateError as error:
        print(f"checkpoint recovery gate failed: {error}", file=sys.stderr)
        return 1
    print(f"checkpoint recovery matrix valid: {len(validated['matrix']['cases'])} cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
