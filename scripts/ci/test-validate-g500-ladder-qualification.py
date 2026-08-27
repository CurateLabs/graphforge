from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("validate-g500-ladder-qualification.py")
SPEC = importlib.util.spec_from_file_location("ladder_qualification", SCRIPT)
assert SPEC and SPEC.loader
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)

CATEGORIES = (
    ("generator_spill", "generator_exact_descriptors"),
    ("canonical_generation", "storage_owned_snapshot"),
    ("derived_adjacency", "storage_owned_snapshot"),
    ("portable_package", "portable_exact_descriptor"),
    ("clean_import", "clean_import_snapshot"),
)


def rung(scale: int, live: int, unit: int) -> dict:
    artifacts = [
        {
            "category": category,
            "logical_bytes": unit * (index + 1),
            "allocated_bytes": unit * (index + 2),
            "logical_references": index + 2,
            "physical_objects": index + 1,
            "source": source,
        }
        for index, (category, source) in enumerate(CATEGORIES)
    ]
    logical = sum(item["logical_bytes"] for item in artifacts)
    allocated = sum(item["allocated_bytes"] for item in artifacts)
    return {
        "id": f"S{scale}",
        "scale": scale,
        "live_edges": live,
        "artifacts": artifacts,
        "totals": {"logical_bytes": logical, "allocated_bytes": allocated},
        "ratios": {
            "logical_bytes_per_live_edge": {
                "numerator_bytes": logical,
                "denominator_edges": live,
            },
            "allocated_bytes_per_live_edge": {
                "numerator_bytes": allocated,
                "denominator_edges": live,
            },
        },
        "phase_peak_allocated_bytes": allocated,
    }


def evidence() -> dict:
    low = rung(20, 10_000, 1_000)
    high = rung(22, 40_000, 4_000)
    # 140000/40000 = 3.5 bytes/edge, exactly matching both observations.
    numerator, denominator = 140_000, 40_000
    projected = VALIDATOR.ceil_ratio(numerator * VALIDATOR.S26_EDGES, denominator)
    volume = 5_000_000_000
    return {
        "schema": "graphforge-g500-ladder-qualification/2",
        "rungs": [low, high],
        "projection": {
            "target": "S26",
            "rate": {
                "numerator_bytes": numerator,
                "denominator_edges": denominator,
            },
            "projected_canonical_lifecycle_peak_bytes": projected,
            "volume_bytes": volume,
            "reserved_headroom_bytes": 500_000_000,
            "headroom_bytes": volume - projected,
            "decision": "admit",
        },
    }


def test_accepts_reconciled_adjacent_rungs_and_conservative_projection():
    VALIDATOR.validate(evidence())


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("missing_category", "schema violation"),
        ("duplicate_category", "complete and unique"),
        ("undeduplicated", "physical identities must be deduplicated"),
        ("logical_total", "totals do not reconcile"),
        ("allocated_total", "totals do not reconcile"),
        ("denominator", "reproducible denominators"),
        ("one_rung", "schema violation"),
        ("nonadjacent", "ordered, and adjacent"),
        ("understated_slope", "below an observed"),
        ("projection", "not reproducible"),
        ("headroom", "does not reconcile"),
        ("unsafe_admit", "contradicts projected headroom"),
        ("peak_below_artifact", "below an observed artifact"),
    ],
)
def test_rejects_goal_seeking_or_incomplete_evidence(mutation: str, match: str):
    value = copy.deepcopy(evidence())
    if mutation == "missing_category":
        value["rungs"][0]["artifacts"].pop()
    elif mutation == "duplicate_category":
        value["rungs"][0]["artifacts"][4] = copy.deepcopy(value["rungs"][0]["artifacts"][0])
    elif mutation == "undeduplicated":
        value["rungs"][0]["artifacts"][0]["physical_objects"] = 3
    elif mutation == "logical_total":
        value["rungs"][0]["totals"]["logical_bytes"] += 1
    elif mutation == "allocated_total":
        value["rungs"][0]["totals"]["allocated_bytes"] += 1
    elif mutation == "denominator":
        value["rungs"][0]["ratios"]["allocated_bytes_per_live_edge"]["denominator_edges"] += 1
    elif mutation == "one_rung":
        value["rungs"].pop()
    elif mutation == "nonadjacent":
        value["rungs"][1]["id"], value["rungs"][1]["scale"] = "S24", 24
    elif mutation == "understated_slope":
        value["projection"]["rate"]["numerator_bytes"] = 1
    elif mutation == "projection":
        value["projection"]["projected_canonical_lifecycle_peak_bytes"] += 1
    elif mutation == "headroom":
        value["projection"]["headroom_bytes"] += 1
    elif mutation == "unsafe_admit":
        value["projection"]["reserved_headroom_bytes"] = value["projection"]["headroom_bytes"] + 1
    elif mutation == "peak_below_artifact":
        value["rungs"][0]["phase_peak_allocated_bytes"] = 0
    with pytest.raises(VALIDATOR.EvidenceError, match=match):
        VALIDATOR.validate(value)


def test_refuses_when_projection_does_not_leave_reserved_headroom():
    value = evidence()
    value["projection"]["reserved_headroom_bytes"] = value["projection"]["headroom_bytes"] + 1
    value["projection"]["decision"] = "refuse"
    VALIDATOR.validate(value)
