#!/usr/bin/env python3
"""Compare real multi-ontology semantic reports from all four public surfaces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any

CONTRACT = "graphforge-multi-ontology-parity-result/1"
SURFACES = ("rust", "python", "node", "cli")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
DIGEST = re.compile(r"^(?:sha256:)?[0-9a-f]{64}$")


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: report must be an object")
    return value


def _runtime_specific(value: Any) -> bool:
    if isinstance(value, str):
        return UUID.fullmatch(value) is not None or DIGEST.fullmatch(value) is not None
    if isinstance(value, list):
        return any(_runtime_specific(item) for item in value)
    if isinstance(value, dict):
        return any(_runtime_specific(item) for item in value.values())
    return False


def compare(report_paths: dict[str, Path], ledger_path: Path) -> list[str]:
    errors: list[str] = []
    ledger = _load(ledger_path)
    expected_cases = set(ledger.get("case_evidence", {}))
    reports: dict[str, dict[str, Any]] = {}
    for surface in SURFACES:
        path = report_paths[surface]
        try:
            report = _load(path)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"{surface}: cannot load report: {error}")
            continue
        if set(report) != {"contract", "cases"} or report.get("contract") != CONTRACT:
            errors.append(f"{surface}: invalid report envelope")
            continue
        cases = report.get("cases")
        if not isinstance(cases, dict) or set(cases) != expected_cases:
            errors.append(f"{surface}: report case set does not match the parity ledger")
            continue
        if _runtime_specific(cases):
            errors.append(f"{surface}: report contains runtime-specific UUID or digest values")
            continue
        reports[surface] = report
    if len(reports) == len(SURFACES):
        authority = reports["rust"]["cases"]
        for surface in SURFACES[1:]:
            if reports[surface]["cases"] != authority:
                errors.append(f"{surface}: semantic results differ from Rust authority")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    for surface in SURFACES:
        parser.add_argument(f"--{surface}", required=True, type=Path)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("tests/contracts/multi-ontology-surface-v1.json"),
    )
    args = parser.parse_args()
    paths = {surface: getattr(args, surface) for surface in SURFACES}
    errors = compare(paths, args.ledger)
    if errors:
        print("multi-ontology semantic parity: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("multi-ontology semantic parity: PASS (Rust, Python, Node, CLI)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
