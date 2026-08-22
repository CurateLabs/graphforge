#!/usr/bin/env python3
"""Compare observed multi-ontology behavior from all four public surfaces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

CONTRACT = "graphforge-multi-ontology-parity-result/1"
SURFACES = ("rust", "python", "node", "cli")


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: report must be an object")
    return value


def _exact(surface: str, name: str, value: Any, keys: set[str], errors: list[str]) -> bool:
    if not isinstance(value, dict) or set(value) != keys:
        errors.append(f"{surface}: {name} must contain exact observed fields {sorted(keys)}")
        return False
    return True


def _validate(surface: str, cases: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    value = cases["cancellation"]
    if _exact(
        surface, "cancellation", value, {"error_code", "before_modules", "after_modules"}, errors
    ) and (
        value["error_code"] != "GF_CANCELLED" or value["before_modules"] != value["after_modules"]
    ):
        errors.append(
            f"{surface}: cancellation must preserve separately captured module inventories"
        )
    value = cases["idempotent_replay"]
    if _exact(
        surface,
        "idempotent_replay",
        value,
        {"first_receipt", "replay_receipt", "conflict_code"},
        errors,
    ) and (
        value["first_receipt"] != value["replay_receipt"]
        or value["conflict_code"] != "GF_IDEMPOTENCY_CONFLICT"
    ):
        errors.append(
            f"{surface}: replay must contain equal observed receipts and the real conflict code"
        )
    value = cases["no_partial_import_or_authority_change"]
    keys = {"before_entries", "after_entries", "authority_before", "authority_after"}
    if _exact(surface, "no_partial_import_or_authority_change", value, keys, errors) and (
        value["before_entries"] != value["after_entries"]
        or value["authority_before"] != value["authority_after"]
    ):
        errors.append(
            f"{surface}: failed import must preserve observed target and authority snapshots"
        )
    value = cases["deterministic_path_free_cli_json"]
    keys = {"first_serialized", "second_serialized", "forbidden_path"}
    if _exact(surface, "deterministic_path_free_cli_json", value, keys, errors):
        first, second, forbidden = (
            value["first_serialized"],
            value["second_serialized"],
            value["forbidden_path"],
        )
        if not isinstance(first, str) or not first or first != second:
            errors.append(
                f"{surface}: determinism requires two equal non-empty serialized observations"
            )
        if (
            not isinstance(forbidden, str)
            or not forbidden
            or forbidden in first
            or forbidden in second
        ):
            errors.append(
                f"{surface}: deterministic observations expose the forbidden runtime path"
            )
    value = cases["packaged_clean_install"]
    if _exact(
        surface,
        "packaged_clean_install",
        value,
        {"package_origin", "operation", "module_count"},
        errors,
    ):
        if not isinstance(value["package_origin"], str) or not value["package_origin"]:
            errors.append(f"{surface}: packaged execution must identify its loaded artifact")
        if (
            value["operation"] != "ontology_modules"
            or isinstance(value["module_count"], bool)
            or not isinstance(value["module_count"], int)
            or value["module_count"] < 0
        ):
            errors.append(
                f"{surface}: packaged execution must report an actual ontology_modules result"
            )
    return errors


def _semantics(cases: dict[str, Any]) -> dict[str, Any]:
    names = (
        "positive_crud_import_export",
        "exact_identity_and_ambiguity",
        "dependency_blocked_deletion",
        "unsupported_future_portability",
        "bounded_structured_diagnostics",
    )
    return {name: cases[name] for name in names} | {
        "cancellation_error": cases["cancellation"]["error_code"],
        "replay_conflict": cases["idempotent_replay"]["conflict_code"],
        "packaged_operation": cases["packaged_clean_install"]["operation"],
    }


def compare(report_paths: dict[str, Path], ledger_path: Path) -> list[str]:
    errors: list[str] = []
    expected = set(_load(ledger_path).get("case_evidence", {}))
    reports: dict[str, dict[str, Any]] = {}
    for surface in SURFACES:
        try:
            report = _load(report_paths[surface])
        except (OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"{surface}: cannot load report: {error}")
            continue
        if set(report) != {"contract", "cases"} or report.get("contract") != CONTRACT:
            errors.append(f"{surface}: invalid report envelope")
            continue
        cases = report.get("cases")
        if not isinstance(cases, dict) or set(cases) != expected:
            errors.append(f"{surface}: report case set does not match the parity ledger")
            continue
        observed_errors = _validate(surface, cases)
        errors.extend(observed_errors)
        if not observed_errors:
            reports[surface] = report
    if len(reports) == len(SURFACES):
        authority = _semantics(reports["rust"]["cases"])
        for surface in SURFACES[1:]:
            if _semantics(reports[surface]["cases"]) != authority:
                errors.append(f"{surface}: semantic results differ from Rust authority")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    for surface in SURFACES:
        parser.add_argument(f"--{surface}", required=True, type=Path)
    parser.add_argument(
        "--ledger", type=Path, default=Path("tests/contracts/multi-ontology-surface-v1.json")
    )
    args = parser.parse_args()
    errors = compare({surface: getattr(args, surface) for surface in SURFACES}, args.ledger)
    if errors:
        print("multi-ontology semantic parity: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("multi-ontology semantic parity: PASS (Rust, Python, Node, CLI)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
