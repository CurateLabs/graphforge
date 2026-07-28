#!/usr/bin/env python3
"""Aggregate complete, same-SHA cross-platform binding RC evidence."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

TARGET_SCHEMA = "graphforge-binding-rc-target/1"
CONTRACT_SCHEMA = "graphforge-binding-rc-targets/1"
AGGREGATE_SCHEMA = "graphforge-binding-rc-aggregate/1"
SUPPORTED_CLASSIFICATION_SCHEMAS = {
    "node": {1},
    "python": {"graphforge-python-non-cypher-parity/1"},
}


def load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON report {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"report must be a JSON object: {path}")
    return value


def validate(
    reports: list[dict[str, object]], contract: dict[str, object], expected_sha: str
) -> dict[str, object]:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ValueError("expected SHA must be exactly 40 lowercase hexadecimal characters")
    if contract.get("schema") != CONTRACT_SCHEMA:
        raise ValueError("unsupported target contract schema")
    targets = contract.get("targets")
    if not isinstance(targets, dict) or not targets:
        raise ValueError("target contract must declare targets")

    identities = [report.get("target") for report in reports]
    duplicates = sorted(
        str(identity) for identity, count in Counter(identities).items() if count > 1
    )
    if duplicates:
        raise ValueError(f"duplicate target reports: {duplicates}")
    actual = set(identities)
    expected = set(targets)
    missing = sorted(expected - actual)
    extra = sorted(str(item) for item in actual - expected)
    if missing or extra:
        raise ValueError(f"target report mismatch: missing={missing}, extra={extra}")

    versions: defaultdict[str, set[str]] = defaultdict(set)
    for report in reports:
        target = report["target"]
        target_contract = targets[target]
        if report.get("schema") != TARGET_SCHEMA:
            raise ValueError(f"{target}: unsupported report schema")
        if report.get("source_sha") != expected_sha:
            raise ValueError(f"{target}: source SHA drift")
        language = report.get("language")
        if language != target_contract.get("language"):
            raise ValueError(f"{target}: language does not match target contract")
        version = report.get("package_version")
        if not isinstance(version, str) or not version:
            raise ValueError(f"{target}: missing package version")
        versions[str(language)].add(version)
        artifact = report.get("artifact")
        if not isinstance(artifact, dict) or not re.fullmatch(
            r"[0-9a-f]{64}", str(artifact.get("sha256", ""))
        ):
            raise ValueError(f"{target}: missing artifact SHA-256")
        classification = report.get("classification")
        if not isinstance(classification, dict):
            raise ValueError(f"{target}: classification must be an object")
        if not re.fullmatch(r"[0-9a-f]{64}", str(classification.get("sha256", ""))):
            raise ValueError(f"{target}: missing classification SHA-256")
        classification_schema = classification.get("schema")
        if classification_schema not in SUPPORTED_CLASSIFICATION_SCHEMAS[str(language)]:
            raise ValueError(f"{target}: unsupported classification schema")
        execution = report.get("execution")
        if not isinstance(execution, dict) or execution.get("mode") != target_contract.get(
            "execution_mode"
        ):
            raise ValueError(f"{target}: execution mode does not match target contract")
        rationale = execution.get("rationale")
        if execution["mode"] == "package-validation" and not (
            isinstance(rationale, str) and rationale.strip()
        ):
            raise ValueError(f"{target}: cross-built target requires a rationale")
        if execution["mode"] == "native" and rationale is not None:
            raise ValueError(f"{target}: native target must not have a rationale")
        if report.get("fallback_execution") is not False:
            raise ValueError(f"{target}: fallback execution is not permitted")
        parity = report.get("sanitized_parity_diff")
        if parity != []:
            raise ValueError(f"{target}: parity differences are non-empty")
        cases = report.get("cases")
        if not isinstance(cases, list) or not cases:
            raise ValueError(f"{target}: no classified cases")
        case_ids: set[str] = set()
        for case in cases:
            if not isinstance(case, dict):
                raise ValueError(f"{target}: invalid case entry")
            identity = case.get("identity")
            if not isinstance(identity, str) or not identity or identity in case_ids:
                raise ValueError(f"{target}: missing or duplicate case identity")
            case_ids.add(identity)
            if case.get("outcome") != "passed" or case.get("sanitized_error") is not None:
                raise ValueError(f"{target}: case did not pass: {identity}")

    drift = {language: sorted(values) for language, values in versions.items() if len(values) != 1}
    if drift:
        raise ValueError(f"mixed package versions: {drift}")
    return {
        "schema": AGGREGATE_SCHEMA,
        "source_sha": expected_sha,
        "status": "passed",
        "package_versions": {
            language: next(iter(values)) for language, values in sorted(versions.items())
        },
        "targets": sorted(reports, key=lambda report: str(report["target"])),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reports: list[dict[str, object]] = []
    try:
        paths = sorted(args.reports.rglob("*.json"))
        reports = [load_json(path) for path in paths]
        aggregate = validate(reports, load_json(args.contract), args.expected_sha)
    except ValueError as error:
        aggregate = {
            "schema": AGGREGATE_SCHEMA,
            "source_sha": args.expected_sha,
            "status": "failed",
            "sanitized_failure": str(error)[:500],
            "targets_received": sorted(
                str(report.get("target", "<missing>")) for report in reports
            ),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
        raise SystemExit(f"release-candidate evidence rejected: {error}") from error
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
