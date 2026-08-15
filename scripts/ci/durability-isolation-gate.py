#!/usr/bin/env python3
"""Validate the acknowledged durability and isolation contract (#748)."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "tests/contracts/durability-isolation-matrix.json"
CONTRACT = "graphforge-durability-isolation/1"
REQUIRED_WRITE_MODES = {
    "single_writer",
    "queued_writer",
    "optimistic_multi_writer",
}
REQUIRED_COVERAGE = {"covered", "documented", "partial", "deferred"}
REQUIRED_ANOMALIES = {
    "dirty_read",
    "non_repeatable_read_within_pinned_facade",
    "lost_update_same_property",
    "write_skew",
    "phantom_read_across_fresh_opens",
}
REQUIRED_CRASH_PHASES = {
    "before_current_replace",
    "after_current_replace",
    "post_linearization_api_error",
    "lost_root_directory_flush_power_loss",
    "torn_current_or_manifest_bytes",
    "recovery_on_open_interrupted_transaction",
}
REQUIRED_BDD = {
    "acknowledged-success",
    "write-skew-honesty",
    "unsupported-filesystem-fail-closed",
}
REQUIRED_VOCABULARY = {
    "stage",
    "validate",
    "durable generation",
    "linearize",
    "acknowledge",
    "publish",
    "abort",
    "recover",
}
FORBIDDEN_POSITIVE_PATTERNS = [
    re.compile(r"\bACID\b"),
    re.compile(r"\bSSI\b"),
    re.compile(r"\bserializable isolation\b", re.IGNORECASE),
    re.compile(r"\bprovides serializability\b", re.IGNORECASE),
    re.compile(r"\bserializable snapshot isolation\b", re.IGNORECASE),
]
DENIAL_CONTEXT = re.compile(
    r"(not|never|without|no|does not|do not|must not|cannot|forbid|"
    r"rather than|instead of|outside|out of scope|unsupported|"
    r"explicitly|honest|denied|reject)",
    re.IGNORECASE,
)


class GateError(RuntimeError):
    """Deterministic contract validation failure."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_matrix(path: Path = MATRIX_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read durability matrix: {error}") from error
    if not isinstance(value, dict):
        raise GateError("durability matrix root must be an object")
    return value


def require_repo_file(value: object, label: str) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or Path(value).is_absolute()
        or ".." in Path(value).parts
    ):
        raise GateError(f"{label}: unsafe repository-relative path {value!r}")
    path = ROOT / value
    if not path.is_file():
        raise GateError(f"{label}: missing file {value}")
    return path


def validate_rust_symbol(path: Path, symbol: str, label: str) -> None:
    source = path.read_text(encoding="utf-8")
    match = re.search(
        rf"(?P<attrs>(?:#\[[^\]]+\]\s*)+)"
        rf"(?:pub\s+)?(?:async\s+)?fn\s+{re.escape(symbol)}\s*\(",
        source,
    )
    if match is None:
        raise GateError(f"{label}: Rust symbol {symbol!r} is absent in {path}")
    attributes = match.group("attrs")
    if "#[test]" not in attributes and "#[tokio::test]" not in attributes:
        raise GateError(f"{label}: Rust symbol {symbol!r} is not a test")
    if "#[ignore" in attributes:
        raise GateError(f"{label}: ignored Rust tests cannot prove coverage")


def validate_doc_symbol(path: Path, symbol: str, label: str) -> None:
    text = path.read_text(encoding="utf-8")
    if symbol not in text:
        raise GateError(f"{label}: documentation anchor {symbol!r} missing in {path}")


def validate_evidence(entry: object, label: str) -> None:
    if not isinstance(entry, dict):
        raise GateError(f"{label}: evidence entries must be objects")
    kind = entry.get("kind")
    symbol = entry.get("symbol")
    if kind not in {"rust", "doc"}:
        raise GateError(f"{label}: unsupported evidence kind {kind!r}")
    if not isinstance(symbol, str) or not symbol:
        raise GateError(f"{label}: evidence symbol is required")
    path = require_repo_file(entry.get("path"), label)
    if kind == "rust":
        validate_rust_symbol(path, symbol, label)
    else:
        validate_doc_symbol(path, symbol, label)


def line_is_denial(line: str) -> bool:
    return DENIAL_CONTEXT.search(line) is not None


def scan_forbidden_claims(path: Path, label: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("<!--"):
            continue
        for pattern in FORBIDDEN_POSITIVE_PATTERNS:
            match = pattern.search(stripped)
            if match is None:
                continue
            window = " ".join(lines[max(0, index - 1) : min(len(lines), index + 2)])
            if line_is_denial(stripped) or line_is_denial(window):
                continue
            raise GateError(
                f"{label}:{path.relative_to(ROOT)}:{index + 1}: forbidden positive claim "
                f"{match.group(0)!r}"
            )


def require_phrases(path: Path, phrases: list[str], label: str) -> None:
    text = path.read_text(encoding="utf-8")
    missing = [phrase for phrase in phrases if phrase not in text]
    if missing:
        raise GateError(f"{label}: missing required phrases {missing} in {path}")


def validate_coverage_cell(
    cell: dict[str, Any],
    label: str,
    deferred_owners: set[int],
) -> None:
    coverage = cell.get("coverage")
    if coverage not in REQUIRED_COVERAGE:
        raise GateError(f"{label}: invalid coverage {coverage!r}")
    evidence = cell.get("evidence", [])
    if coverage in {"covered", "documented", "partial"}:
        if not isinstance(evidence, list) or not evidence:
            raise GateError(f"{label}: {coverage} cells require evidence")
        for index, entry in enumerate(evidence):
            validate_evidence(entry, f"{label}.evidence[{index}]")
    if coverage in {"deferred", "partial", "documented"}:
        owner = cell.get("owner_issue")
        if coverage == "documented" and owner is None:
            return
        if not isinstance(owner, int) or owner not in deferred_owners:
            raise GateError(
                f"{label}: owner_issue must be one of sorted deferred M6 issues, got {owner!r}"
            )
    if coverage == "covered" and "owner_issue" in cell:
        raise GateError(f"{label}: covered cells must not defer to owner_issue")


def validate_matrix(path: Path = MATRIX_PATH) -> dict[str, Any]:
    matrix = load_matrix(path)
    if matrix.get("schema_version") != 1 or matrix.get("contract") != CONTRACT:
        raise GateError(f"matrix must declare {CONTRACT} schema_version 1")
    if matrix.get("issue") != 748 or matrix.get("parent_issue") != 747:
        raise GateError("matrix must bind issues 748 / 747")

    deferred_owners = matrix.get("deferred_owner_issues")
    if not isinstance(deferred_owners, list) or set(deferred_owners) != set(range(749, 757)):
        raise GateError("deferred_owner_issues must be exactly #749-#756")
    deferred_owner_set = set(deferred_owners)

    m5 = matrix.get("m5_consumer_issues")
    if not isinstance(m5, list) or set(m5) != {738, 742, 745}:
        raise GateError("m5_consumer_issues must be exactly #738, #742, #745")

    adr = require_repo_file(matrix.get("adr"), "adr")
    architecture = require_repo_file(matrix.get("architecture_doc"), "architecture_doc")
    api_doc = require_repo_file(matrix.get("api_doc"), "api_doc")
    reconciled = matrix.get("reconciled_adrs")
    if not isinstance(reconciled, list) or len(reconciled) != 3:
        raise GateError("reconciled_adrs must list ADR 0013-0015 paths")
    for entry in reconciled:
        require_repo_file(entry, "reconciled_adrs")

    acknowledgement = matrix.get("acknowledgement")
    if not isinstance(acknowledgement, dict):
        raise GateError("acknowledgement block is required")
    if acknowledgement.get("name") != "acknowledged-durable":
        raise GateError("acknowledgement.name must be acknowledged-durable")
    requires = acknowledgement.get("requires")
    if not isinstance(requires, list) or "project_root_directory_flush" not in requires:
        raise GateError("acknowledgement must require project_root_directory_flush")
    if acknowledgement.get("linearization_point") != "current_atomic_replace_or_create":
        raise GateError("linearization_point must be current_atomic_replace_or_create")

    filesystem = matrix.get("filesystem_scope")
    if not isinstance(filesystem, dict):
        raise GateError("filesystem_scope is required")
    if filesystem.get("error") != "GF_UNSUPPORTED_FILESYSTEM":
        raise GateError("filesystem_scope.error must be GF_UNSUPPORTED_FILESYSTEM")
    if filesystem.get("best_effort_allowed") is not False:
        raise GateError("filesystem_scope.best_effort_allowed must be false")

    recovery = matrix.get("recovery_authority")
    if not isinstance(recovery, dict) or recovery.get("sole") != "exact_valid_CURRENT":
        raise GateError("recovery_authority.sole must be exact_valid_CURRENT")

    write_modes = matrix.get("write_modes")
    if not isinstance(write_modes, dict) or set(write_modes) != REQUIRED_WRITE_MODES:
        raise GateError(f"write_modes must be exactly {sorted(REQUIRED_WRITE_MODES)}")
    optimistic = write_modes["optimistic_multi_writer"]
    if (
        optimistic.get("ssi_claimed") is not False
        or optimistic.get("serializable_claimed") is not False
    ):
        raise GateError("optimistic mode must not claim SSI or serializability")
    for mode, body in write_modes.items():
        conflicts = body.get("conflicts")
        if not isinstance(conflicts, list) or not conflicts:
            raise GateError(f"{mode}: isolation/conflict table is required")

    vocabulary = matrix.get("publication_vocabulary")
    if not isinstance(vocabulary, list) or set(vocabulary) != REQUIRED_VOCABULARY:
        raise GateError(f"publication_vocabulary must equal {sorted(REQUIRED_VOCABULARY)}")

    bdd = matrix.get("bdd_scenarios")
    if not isinstance(bdd, list):
        raise GateError("bdd_scenarios must be an array")
    bdd_ids = {item.get("id") for item in bdd if isinstance(item, dict)}
    if bdd_ids != REQUIRED_BDD:
        raise GateError(f"bdd_scenarios drift: {sorted(bdd_ids)} != {sorted(REQUIRED_BDD)}")

    crash_phases = matrix.get("crash_phases")
    if not isinstance(crash_phases, list):
        raise GateError("crash_phases must be an array")
    crash_ids = {item.get("id") for item in crash_phases if isinstance(item, dict)}
    if crash_ids != REQUIRED_CRASH_PHASES:
        raise GateError(
            f"crash_phases drift: missing/extra {sorted(REQUIRED_CRASH_PHASES ^ crash_ids)}"
        )
    for phase in crash_phases:
        validate_coverage_cell(phase, f"crash_phases.{phase['id']}", deferred_owner_set)

    anomalies = matrix.get("anomalies")
    if not isinstance(anomalies, list):
        raise GateError("anomalies must be an array")
    anomaly_ids = {item.get("id") for item in anomalies if isinstance(item, dict)}
    if anomaly_ids != REQUIRED_ANOMALIES:
        raise GateError(f"anomalies drift: {sorted(anomaly_ids ^ REQUIRED_ANOMALIES)}")
    write_skew = next(item for item in anomalies if item["id"] == "write_skew")
    if write_skew["modes"].get("optimistic_multi_writer") != "allowed_documented_not_ssi":
        raise GateError("write_skew must classify optimistic mode as allowed_documented_not_ssi")
    for anomaly in anomalies:
        modes = anomaly.get("modes")
        if not isinstance(modes, dict) or set(modes) != REQUIRED_WRITE_MODES:
            raise GateError(f"anomalies.{anomaly['id']}: modes must cover every write mode")
        validate_coverage_cell(anomaly, f"anomalies.{anomaly['id']}", deferred_owner_set)

    lifecycle = matrix.get("lifecycle")
    if not isinstance(lifecycle, list) or not lifecycle:
        raise GateError("lifecycle cells are required")
    for item in lifecycle:
        validate_coverage_cell(item, f"lifecycle.{item.get('id')}", deferred_owner_set)

    filesystem_evidence = matrix.get("filesystem_evidence")
    if not isinstance(filesystem_evidence, list) or not filesystem_evidence:
        raise GateError("filesystem_evidence is required")
    for index, entry in enumerate(filesystem_evidence):
        validate_evidence(entry, f"filesystem_evidence[{index}]")

    require_phrases(
        adr,
        [
            "acknowledged-durable",
            "project-root directory flush",
            "GF_UNSUPPORTED_FILESYSTEM",
            "Write-skew witness",
            "exact, valid `CURRENT`",
            "Publication vocabulary",
        ],
        "adr",
    )
    require_phrases(
        architecture,
        [
            "acknowledged-durable",
            "Write-skew witness",
            "GF_UNSUPPORTED_FILESYSTEM",
            "single_writer",
            "queued_writer",
            "optimistic_multi_writer",
            "graphforge-durability-isolation/1",
        ],
        "architecture_doc",
    )
    require_phrases(
        api_doc,
        [
            "ADR 0018",
            "acknowledged-durable",
            "write-skew",
        ],
        "api_doc",
    )
    for doc in (adr, architecture, api_doc):
        scan_forbidden_claims(doc, "normative_docs")

    m5_guide = ROOT / "docs/guides/repository-integration.md"
    if not m5_guide.is_file():
        raise GateError("repository integration guide is missing")
    guide_text = m5_guide.read_text(encoding="utf-8")
    for term in ("acknowledge", "linearize", "stage", "publish"):
        if term not in guide_text:
            raise GateError(f"M5 interchange guide must reuse publication vocabulary term {term!r}")

    return matrix


def emit_report(matrix: dict[str, Any], output: Path) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    covered = []
    deferred = []
    for section in ("crash_phases", "anomalies", "lifecycle"):
        for cell in matrix.get(section, []):
            record = {
                "section": section,
                "id": cell.get("id"),
                "coverage": cell.get("coverage"),
                "owner_issue": cell.get("owner_issue"),
            }
            if cell.get("coverage") == "covered":
                covered.append(record)
            else:
                deferred.append(record)
    report = {
        "contract": CONTRACT,
        "issue": 748,
        "matrix_sha256": sha256(MATRIX_PATH),
        "covered_cells": covered,
        "non_covered_cells": deferred,
        "m5_consumer_issues": matrix.get("m5_consumer_issues"),
    }
    report_path = output / "durability-isolation-report.json"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    report_path.write_text(payload, encoding="utf-8")
    (output / "durability-isolation-report.sha256").write_text(
        hashlib.sha256(payload.encode("utf-8")).hexdigest() + "\n",
        encoding="utf-8",
    )
    return report_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        nargs="?",
        default="validate",
        choices=["validate", "report"],
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "dist" / "durability-isolation",
    )
    args = parser.parse_args(argv)
    try:
        matrix = validate_matrix()
        if args.command == "report":
            path = emit_report(matrix, args.output)
            print(path)
        else:
            print(f"{CONTRACT} ok ({MATRIX_PATH.relative_to(ROOT)})")
        return 0
    except GateError as error:
        print(f"durability isolation gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
