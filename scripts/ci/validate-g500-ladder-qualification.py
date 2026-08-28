#!/usr/bin/env python3
"""Fail-closed semantic validator for #951 disk attribution and S26 projection."""

from __future__ import annotations

import argparse
from itertools import pairwise
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/development/evidence/g500-ladder-qualification.schema.json"
REQUIRED_CATEGORIES = {
    "canonical_node_topology", "canonical_edge_topology", "properties",
    "uuid_surrogate_indexes", "adjacency_csr", "catalog_manifests",
    "construction_staging_spill",
    "portable_package",
    "clean_imported_project",
}
REQUIRED_PHASES = {
    "append_merge", "seal_authentication", "shape_consume_reauthentication",
    "encode_write_postwrite_authentication", "publication_preauthentication",
    "cas_install_read_write", "hydration_verification", "fsync_synchronization",
    "recovery_reauthentication",
}
S26_EDGES = 1 << 30  # SCALE=26, edgefactor=16 raw target; conservative live denominator.
S26_NODES = 1 << 26


class EvidenceError(ValueError):
    pass


def ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def validate_schema(evidence: dict[str, Any]) -> None:
    try:
        contract = json.loads(SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(contract)
        Draft202012Validator(contract).validate(evidence)
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        raise EvidenceError(f"committed schema is invalid: {error}") from error
    except ValidationError as error:
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        raise EvidenceError(f"schema violation at {location}: {error.message}") from error


def validate(evidence: dict[str, Any]) -> None:
    validate_schema(evidence)
    rungs = evidence["rungs"]
    if len(rungs) < 2:
        raise EvidenceError("at least two adjacent observations are required")
    scales = [rung["scale"] for rung in rungs]
    if scales != sorted(set(scales)) or any(b - a != 2 for a, b in pairwise(scales)):
        raise EvidenceError("rungs must be unique, ordered, and adjacent")

    for rung in rungs:
        if rung["id"] != f"S{rung['scale']}":
            raise EvidenceError("rung id and scale disagree")
        categories = [artifact["category"] for artifact in rung["artifacts"]]
        if set(categories) != REQUIRED_CATEGORIES or len(categories) != len(set(categories)):
            raise EvidenceError("artifact categories must be complete and unique")
        phases = rung["phases"]
        phase_names = [phase["phase"] for phase in phases]
        if set(phase_names) != REQUIRED_PHASES or len(phase_names) != len(set(phase_names)):
            raise EvidenceError("application I/O phases must be complete and unique")
        phase_fields = ("read_bytes", "write_bytes", "read_calls", "write_calls", "object_count", "block_count", "fsync_calls")
        for phase in phases:
            observed = any(phase[field] != 0 for field in phase_fields)
            if phase["applicable"] != observed:
                raise EvidenceError("phase applicability contradicts source-owned counters")
            if phase["phase"] != "recovery_reauthentication" and not phase["applicable"]:
                raise EvidenceError("required lifecycle phase has a fake-zero observation")
            if (phase["read_bytes"] == 0) != (phase["read_calls"] == 0):
                raise EvidenceError("phase read bytes and calls disagree")
            if (phase["write_bytes"] == 0) != (phase["write_calls"] == 0):
                raise EvidenceError("phase write bytes and calls disagree")
        if any(artifact["physical_objects"] > artifact["logical_references"] for artifact in rung["artifacts"]):
            raise EvidenceError("physical identities must be deduplicated from logical references")
        logical = sum(artifact["logical_bytes"] for artifact in rung["artifacts"])
        allocated = sum(artifact["allocated_bytes"] for artifact in rung["artifacts"])
        retained_views = sum(artifact["current_retained_bytes"] for artifact in rung["artifacts"])
        retained = rung["totals"]["current_retained_bytes"]
        if retained != rung["workspace_current_allocated_bytes"]:
            raise EvidenceError("workspace numerator disagrees with retained identity union")
        # Category peaks are diagnostics, not a total: categories coexist.
        # The total is an independently observed phase-boundary union high-water
        # mark and must not be reconstructed as max(category).
        transient_peak = rung["totals"]["transient_peak_allocated_bytes"]
        phase_totals = {f"phase_{field}": sum(phase[field] for phase in phases) for field in phase_fields}
        expected_totals = {"logical_bytes": logical, "allocated_bytes": allocated, **phase_totals}
        if {key: rung["totals"][key] for key in expected_totals} != expected_totals:
            raise EvidenceError("artifact or phase totals do not reconcile")
        if transient_peak < max(
            artifact["transient_peak_allocated_bytes"] for artifact in rung["artifacts"]
        ):
            raise EvidenceError("lifecycle peak is below a category peak")
        if retained > retained_views or retained < max(
            artifact["current_retained_bytes"] for artifact in rung["artifacts"]
        ):
            raise EvidenceError("native retained union is inconsistent with owner views")
        if any(item["current_retained_bytes"] > item["allocated_bytes"] for item in rung["artifacts"]):
            raise EvidenceError("retained allocation exceeds category allocation")
        if transient_peak < retained:
            raise EvidenceError("lifecycle peak is below current retained allocation")
        source_project = rung["source_project_current_allocated_bytes"]
        selected_source = sum(
            item["allocated_bytes"]
            for item in rung["artifacts"]
            if item["source"] == "storage_owned_snapshot"
        )
        if source_project < selected_source:
            raise EvidenceError("source project union is below its selected generation")
        if source_project > retained:
            raise EvidenceError("source project union exceeds the workspace union")
        live, nodes = rung["live_edges"], rung["live_nodes"]
        by_category = {item["category"]: item for item in rung["artifacts"]}
        expected = {
            "canonical_node_bytes_per_live_node": {"numerator_bytes": by_category["canonical_node_topology"]["logical_bytes"], "denominator_count": nodes},
            "canonical_edge_bytes_per_live_edge": {"numerator_bytes": by_category["canonical_edge_topology"]["logical_bytes"], "denominator_count": live},
            "authoritative_project_bytes_per_live_edge": {
                "numerator_bytes": source_project,
                "denominator_count": live,
            },
            "full_lifecycle_peak_bytes_per_live_edge": {"numerator_bytes": transient_peak, "denominator_count": live},
        }
        if rung["ratios"] != expected:
            raise EvidenceError("ratios must preserve exact reproducible denominators")

    rate = evidence["projection"]["rate"]
    rn, rd = rate["numerator_bytes"], rate["denominator_count"]
    for low, high in pairwise(rungs):
        delta_edges = high["live_edges"] - low["live_edges"]
        delta_bytes = high["totals"]["transient_peak_allocated_bytes"] - low["totals"]["transient_peak_allocated_bytes"]
        if delta_edges <= 0:
            raise EvidenceError("live-edge denominator must increase across adjacent rungs")
        if delta_bytes > 0 and rn * delta_edges < delta_bytes * rd:
            raise EvidenceError("projection rate is below an observed adjacent-rung slope")
        if rn * high["live_edges"] < high["totals"]["transient_peak_allocated_bytes"] * rd:
            raise EvidenceError("projection rate is below the latest observed peak ratio")

    projected = ceil_ratio(rn * S26_EDGES, rd)
    projection = evidence["projection"]
    if projection["source_rungs"] != [rungs[-2]["id"], rungs[-1]["id"]]:
        raise EvidenceError("projection must cite the newest adjacent source rungs")
    if projection["projected_lifecycle_peak_bytes"] != projected:
        raise EvidenceError("S26 projected peak is not reproducible from the declared rate")
    latest_categories = {item["category"]: item for item in rungs[-1]["artifacts"]}
    canonical_node_projected = ceil_ratio(
        latest_categories["canonical_node_topology"]["current_retained_bytes"]
        * S26_NODES,
        rungs[-1]["live_nodes"],
    )
    canonical_edge_projected = ceil_ratio(
        latest_categories["canonical_edge_topology"]["current_retained_bytes"]
        * S26_EDGES,
        rungs[-1]["live_edges"],
    )
    if projection["projected_canonical_node_bytes"] != canonical_node_projected:
        raise EvidenceError("S26 canonical node projection is not reproducible")
    if projection["projected_canonical_edge_bytes"] != canonical_edge_projected:
        raise EvidenceError("S26 canonical edge projection is not reproducible")
    if projected > projection["volume_bytes"]:
        expected_headroom = 0
    else:
        expected_headroom = projection["volume_bytes"] - projected
    if projection["headroom_bytes"] != expected_headroom:
        raise EvidenceError("headroom does not reconcile")
    expected_decision = "refuse"
    if projected <= projection["volume_bytes"] and expected_headroom >= projection["reserved_headroom_bytes"]:
        expected_decision = "admit"
    if projection["decision"] != expected_decision:
        raise EvidenceError("S26 admission decision contradicts projected headroom")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    try:
        value = json.loads(args.evidence.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise EvidenceError("evidence root must be an object")
        validate(value)
    except (OSError, json.JSONDecodeError, EvidenceError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
