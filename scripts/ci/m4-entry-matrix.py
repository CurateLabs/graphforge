#!/usr/bin/env python3
"""Validate and summarize the M4 embedded performance entry contract (#334)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "tests/contracts/m4-entry-matrix.json"
SCHEMA = "graphforge-m4-entry-matrix/1"
EVIDENCE_SCHEMA = "graphforge-m4-entry-evidence/1"
REQUIRED_WORKLOAD_IDS = {
    "fixed-hop-limit",
    "scan-count",
    "aggregate-top-n",
    "pagerank",
    "exact-cosine-knn",
    "node2vec",
}
REQUIRED_DEFERRED_IDS = {
    "threads-1",
    "threads-2",
    "threads-4",
    "threads-8",
    "threads-automatic",
}
REQUIRED_PARITY = {
    "canonical_arrow_schema",
    "row_ordering",
    "result_fingerprint",
    "structured_errors",
    "cancellation_outcome",
    "resource_limit_behavior",
}


def load(path: Path = CONTRACT) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def contract_errors(path: Path = CONTRACT) -> list[str]:
    errors: list[str] = []
    try:
        data = load(path)
    except (OSError, json.JSONDecodeError) as exc:
        return [f"unreadable contract: {exc}"]

    if data.get("schema") != SCHEMA:
        errors.append(f"schema must be {SCHEMA}")
    if data.get("issue") != 334:
        errors.append("issue must be 334")
    if data.get("parity_owner_issue") != 337:
        errors.append("parity_owner_issue must be 337")
    if data.get("exit_owner_issue") != 345:
        errors.append("exit_owner_issue must be 345")
    if data.get("public_persistence_owner_issue") != 338:
        errors.append("public_persistence_owner_issue must be 338")

    current = data.get("current_runtime")
    if not isinstance(current, dict):
        errors.append("current_runtime must be an object")
    else:
        if current.get("status") != "supported":
            errors.append("current_runtime.status must be supported")
        if current.get("tokio_worker_threads") != 2:
            errors.append("current_runtime.tokio_worker_threads must be 2")
        if current.get("public_resource_policy") is not False:
            errors.append("current_runtime.public_resource_policy must be false")

    deferred = data.get("deferred_runtime_configurations")
    if not isinstance(deferred, list):
        errors.append("deferred_runtime_configurations must be a list")
    else:
        ids = {item.get("id") for item in deferred if isinstance(item, dict)}
        if ids != REQUIRED_DEFERRED_IDS:
            errors.append(
                "deferred_runtime_configurations ids must equal "
                + ",".join(sorted(REQUIRED_DEFERRED_IDS))
            )
        for item in deferred:
            if not isinstance(item, dict):
                errors.append("deferred configuration entries must be objects")
                continue
            if item.get("status") != "deferred":
                errors.append(f"{item.get('id')}: status must be deferred")
            if item.get("owner_issue") != 337:
                errors.append(f"{item.get('id')}: owner_issue must be 337")

    parity = data.get("parity_assertions")
    if not isinstance(parity, list) or set(parity) != REQUIRED_PARITY:
        errors.append("parity_assertions must exactly match the #337 evidence contract")

    workloads = data.get("workloads")
    if not isinstance(workloads, list):
        errors.append("workloads must be a list")
    else:
        ids = {item.get("id") for item in workloads if isinstance(item, dict)}
        if ids != REQUIRED_WORKLOAD_IDS:
            errors.append("workloads must exactly cover " + ",".join(sorted(REQUIRED_WORKLOAD_IDS)))
        for item in workloads:
            if not isinstance(item, dict):
                continue
            if item.get("short_ci") is not True:
                errors.append(f"{item.get('id')}: short_ci must be true")

    matrices = data.get("matrices")
    if not isinstance(matrices, dict):
        errors.append("matrices must be an object")
    else:
        short = matrices.get("short_ci")
        if not isinstance(short, dict):
            errors.append("matrices.short_ci must be an object")
        else:
            forbidden = set(short.get("forbidden") or [])
            for required in (
                "timing_absolute_thresholds",
                "fabricated_deferred_runtime_results",
            ):
                if required not in forbidden:
                    errors.append(f"short_ci.forbidden must include {required}")
            if short.get("runtime") != "fixed-two-worker":
                errors.append("short_ci.runtime must be fixed-two-worker")
        large = matrices.get("large_manual")
        if not isinstance(large, dict) or not large.get("requires_documented_commands"):
            errors.append("large_manual must require documented commands")

    discovery = data.get("discovery_evidence")
    if not isinstance(discovery, list) or not discovery:
        errors.append("discovery_evidence must be a non-empty list")
    else:
        found = False
        for item in discovery:
            if not isinstance(item, dict):
                continue
            if item.get("id") == "lower-level-8m-128m":
                found = True
                if item.get("classification") != "discovery_not_public_facade_baseline":
                    errors.append("8M/128M evidence must be discovery_not_public_facade_baseline")
                if item.get("public_facade_owner_issue") != 338:
                    errors.append("8M/128M public_facade_owner_issue must be 338")
        if not found:
            errors.append("missing lower-level-8m-128m discovery evidence entry")

    evidence = data.get("evidence_artifact")
    if not isinstance(evidence, dict) or evidence.get("schema") != EVIDENCE_SCHEMA:
        errors.append(f"evidence_artifact.schema must be {EVIDENCE_SCHEMA}")

    return errors


def evidence_errors(payload: dict[str, Any], contract: dict[str, Any] | None = None) -> list[str]:
    """Validate an M4 entry evidence artifact against the contract shape."""
    errors: list[str] = []
    contract = contract or load()
    required = set(contract["evidence_artifact"]["required_fields"])
    missing = sorted(required - set(payload))
    if missing:
        errors.append("missing evidence fields: " + ",".join(missing))
    if payload.get("schema") != EVIDENCE_SCHEMA:
        errors.append(f"evidence schema must be {EVIDENCE_SCHEMA}")
    if payload.get("contract_schema") != SCHEMA:
        errors.append(f"contract_schema must be {SCHEMA}")

    runtime = payload.get("runtime_configuration")
    if isinstance(runtime, dict):
        status = runtime.get("status")
        if status not in {"supported", "deferred", "unavailable"}:
            errors.append("runtime_configuration.status must be supported|deferred|unavailable")
        if status == "supported" and runtime.get("tokio_worker_threads") != 2:
            errors.append("supported runtime must report tokio_worker_threads=2")
        if status == "deferred" and runtime.get("owner_issue") != 337:
            errors.append("deferred runtime must cite owner_issue 337")
        if status != "deferred" and runtime.get("requested_workers") in {1, 4, 8, "automatic"}:
            # Fabricating unsupported requested configs as supported /
            # unavailable-without-owner is forbidden.
            if status == "supported":
                errors.append("must not claim unsupported thread requests as supported")
    else:
        errors.append("runtime_configuration must be an object")

    deferred = payload.get("deferred_configurations")
    if not isinstance(deferred, list):
        errors.append("deferred_configurations must be a list")
    else:
        for item in deferred:
            if not isinstance(item, dict):
                continue
            if item.get("status") != "deferred":
                errors.append(f"{item.get('id')}: evidence must keep deferred status")
            if item.get("executed") is True:
                errors.append(f"{item.get('id')}: must not claim deferred configs executed")

    discovery = payload.get("discovery_evidence")
    if isinstance(discovery, list):
        for item in discovery:
            if not isinstance(item, dict):
                continue
            if item.get("id") == "lower-level-8m-128m" and item.get("classification") != (
                "discovery_not_public_facade_baseline"
            ):
                errors.append("8M/128M discovery classification drift")

    workloads = payload.get("workloads")
    if not isinstance(workloads, list):
        errors.append("workloads must be a list")
    else:
        for item in workloads:
            if not isinstance(item, dict):
                continue
            gates = item.get("structural_gates")
            timing = item.get("timing_observation")
            if gates is None:
                errors.append(f"{item.get('id')}: missing structural_gates")
            if timing is not None and item.get("timing_is_pass_fail") is True:
                errors.append(f"{item.get('id')}: wall timing must not be a pass/fail gate")

    return errors


def validate() -> int:
    errors = contract_errors()
    if errors:
        print("M4 entry matrix contract errors:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(f"OK {CONTRACT.relative_to(ROOT)} ({SCHEMA})")
    return 0


def summarize() -> int:
    errors = contract_errors()
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    data = load()
    print(
        json.dumps(
            {
                "schema": data["schema"],
                "issue": data["issue"],
                "current_runtime": data["current_runtime"]["id"],
                "deferred": [item["id"] for item in data["deferred_runtime_configurations"]],
                "workloads": [item["id"] for item in data["workloads"]],
                "short_ci_fixture": data["matrices"]["short_ci"]["fixture"],
                "parity_owner_issue": data["parity_owner_issue"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("validate", "summarize"),
        help="validate the checked-in contract, or print a short summary",
    )
    args = parser.parse_args(argv)
    if args.command == "validate":
        return validate()
    return summarize()


if __name__ == "__main__":
    raise SystemExit(main())
