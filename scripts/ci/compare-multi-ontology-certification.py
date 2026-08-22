#!/usr/bin/env python3
"""Compare real #843 certification reports against Rust authority."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any

CONTRACT = "graphforge-multi-ontology-certification-result/1"
SURFACES = ("rust", "python", "node", "cli")
REPORT_KEYS = {
    "contract",
    "surface",
    "composition_before",
    "composition_after",
    "migration_plan_digest",
    "module_ids",
    "bridge_ids",
    "retained_data",
    "cases",
}
DIGEST = re.compile(r"^(?:sha256:)?[0-9a-f]{64}$")


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("report must be an object")
    return value


def _validate(surface: str, report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if set(report) != REPORT_KEYS:
        errors.append(f"{surface}: report keys do not match the closed contract")
    if report.get("contract") != CONTRACT or report.get("surface") != surface:
        errors.append(f"{surface}: invalid report contract or surface")
    for field in ("composition_before", "composition_after", "migration_plan_digest"):
        value = report.get(field)
        if not isinstance(value, str) or DIGEST.fullmatch(value) is None:
            errors.append(f"{surface}: {field} must be a lowercase SHA-256 digest")
    if report.get("composition_before") == report.get("composition_after"):
        errors.append(f"{surface}: migration did not change composition identity")
    for field in ("module_ids", "bridge_ids"):
        values = report.get(field)
        if (
            not isinstance(values, list)
            or not values
            or not all(isinstance(value, str) and value for value in values)
            or values != sorted(set(values), key=lambda value: value.encode("utf-8"))
        ):
            errors.append(f"{surface}: {field} must be a non-empty sorted unique list")
    retained = report.get("retained_data")
    if not isinstance(retained, dict) or set(retained) != {
        "rows_scanned",
        "name",
        "birth_year",
    }:
        errors.append(f"{surface}: retained_data must be an exact closed outcome")
    elif (
        not isinstance(retained["rows_scanned"], int)
        or isinstance(retained["rows_scanned"], bool)
        or retained["rows_scanned"] < 1
        or not isinstance(retained["name"], str)
        or not retained["name"]
        or not isinstance(retained["birth_year"], int)
        or isinstance(retained["birth_year"], bool)
    ):
        errors.append(f"{surface}: retained_data values are invalid")
    cases = report.get("cases")
    expected_cases = {
        "authority_reopened": {"composition_fingerprint": report.get("composition_after")},
        "bridge_set_retained": {"bridge_ids": report.get("bridge_ids")},
        "module_set_retained": {"module_ids": report.get("module_ids")},
        "retained_data_query": report.get("retained_data"),
    }
    if cases != expected_cases:
        errors.append(
            f"{surface}: cases must bind exact authority, inventory, "
            "and retained-query observations"
        )
    return errors


def compare(paths: dict[str, Path]) -> list[str]:
    errors: list[str] = []
    reports: dict[str, dict[str, Any]] = {}
    for surface in SURFACES:
        try:
            report = _load(paths[surface])
        except (OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"{surface}: cannot load report: {error}")
            continue
        errors.extend(_validate(surface, report))
        reports[surface] = report

    rust = reports.get("rust")
    if rust is not None:
        authority = {key: value for key, value in rust.items() if key != "surface"}
        for surface in SURFACES[1:]:
            report = reports.get(surface)
            if report is None:
                continue
            candidate = {key: value for key, value in report.items() if key != "surface"}
            if candidate != authority:
                errors.append(f"{surface}: certification outcome differs from Rust authority")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    for surface in SURFACES:
        parser.add_argument(f"--{surface}", required=True, type=Path)
    args = parser.parse_args()
    errors = compare({surface: getattr(args, surface) for surface in SURFACES})
    if errors:
        print("multi-ontology certification parity: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("multi-ontology certification parity: PASS (Rust, Python, Node, CLI)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
