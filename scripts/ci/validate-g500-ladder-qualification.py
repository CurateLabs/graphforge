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
    "generator_spill",
    "canonical_generation",
    "derived_adjacency",
    "portable_package",
    "clean_import",
}
S26_EDGES = 1 << 30  # SCALE=26, edgefactor=16 raw target; conservative live denominator.


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
        if any(
            artifact["physical_objects"] > artifact["logical_references"]
            for artifact in rung["artifacts"]
        ):
            raise EvidenceError("physical identities must be deduplicated from logical references")
        logical = sum(artifact["logical_bytes"] for artifact in rung["artifacts"])
        allocated = sum(artifact["allocated_bytes"] for artifact in rung["artifacts"])
        if rung["totals"] != {"logical_bytes": logical, "allocated_bytes": allocated}:
            raise EvidenceError("artifact totals do not reconcile")
        live = rung["live_edges"]
        expected = {
            "logical_bytes_per_live_edge": {
                "numerator_bytes": logical,
                "denominator_edges": live,
            },
            "allocated_bytes_per_live_edge": {
                "numerator_bytes": allocated,
                "denominator_edges": live,
            },
        }
        if rung["ratios"] != expected:
            raise EvidenceError("ratios must preserve exact reproducible denominators")
        if rung["phase_peak_allocated_bytes"] < max(
            artifact["allocated_bytes"] for artifact in rung["artifacts"]
        ):
            raise EvidenceError("phase peak is below an observed artifact allocation")

    rate = evidence["projection"]["rate"]
    rn, rd = rate["numerator_bytes"], rate["denominator_edges"]
    for low, high in pairwise(rungs):
        delta_edges = high["live_edges"] - low["live_edges"]
        delta_bytes = high["phase_peak_allocated_bytes"] - low["phase_peak_allocated_bytes"]
        if delta_edges <= 0:
            raise EvidenceError("live-edge denominator must increase across adjacent rungs")
        if delta_bytes > 0 and rn * delta_edges < delta_bytes * rd:
            raise EvidenceError("projection rate is below an observed adjacent-rung slope")
        if rn * high["live_edges"] < high["phase_peak_allocated_bytes"] * rd:
            raise EvidenceError("projection rate is below the latest observed peak ratio")

    projected = ceil_ratio(rn * S26_EDGES, rd)
    projection = evidence["projection"]
    if projection["projected_canonical_lifecycle_peak_bytes"] != projected:
        raise EvidenceError("S26 projected peak is not reproducible from the declared rate")
    if projected > projection["volume_bytes"]:
        expected_headroom = 0
    else:
        expected_headroom = projection["volume_bytes"] - projected
    if projection["headroom_bytes"] != expected_headroom:
        raise EvidenceError("headroom does not reconcile")
    expected_decision = (
        "admit" if expected_headroom >= projection["reserved_headroom_bytes"] else "refuse"
    )
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
