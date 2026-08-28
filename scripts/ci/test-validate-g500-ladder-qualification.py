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
    ("canonical_node_topology", "storage_owned_snapshot"),
    ("canonical_edge_topology", "storage_owned_snapshot"),
    ("properties", "storage_owned_snapshot"),
    ("uuid_surrogate_indexes", "storage_owned_snapshot"),
    ("adjacency_csr", "storage_owned_snapshot"),
    ("catalog_manifests", "storage_owned_snapshot"),
    ("construction_staging_spill", "construction_receipts"),
    ("portable_package", "exact_descriptor"),
    ("clean_imported_project", "clean_import_snapshot"),
)
PHASES = ("append_merge", "seal_authentication", "shape_consume_reauthentication", "encode_write_postwrite_authentication", "publication_preauthentication", "cas_install_read_write", "hydration_verification", "fsync_synchronization", "recovery_reauthentication")


def rung(scale: int, live: int, unit: int) -> dict:
    artifacts = [
        {
            "category": category,
            "logical_bytes": unit * (index + 1),
            "allocated_bytes": unit * (index + 2),
            "current_retained_bytes": unit * (index + 1),
            "transient_peak_allocated_bytes": unit * 100 if index == 0 else unit,
            "logical_references": index + 2,
            "physical_objects": index + 1,
            "source": source,
        }
        for index, (category, source) in enumerate(CATEGORIES)
    ]
    logical = sum(item["logical_bytes"] for item in artifacts)
    allocated = sum(item["allocated_bytes"] for item in artifacts)
    retained = sum(item["current_retained_bytes"] for item in artifacts)
    # Independent union high-water observation; deliberately larger than any
    # one category peak because categories coexist at lifecycle boundaries.
    peak = sum(item["transient_peak_allocated_bytes"] for item in artifacts)
    phases = [{"phase": phase, "read_bytes": unit, "write_bytes": unit, "read_calls": 1, "write_calls": 1, "object_count": 1, "block_count": 1, "fsync_calls": 1} for phase in PHASES]
    return {
        "id": f"S{scale}",
        "scale": scale,
        "live_nodes": live // 16,
        "live_edges": live,
        "artifacts": artifacts,
        "phases": phases,
        "totals": {"logical_bytes": logical, "allocated_bytes": allocated, "current_retained_bytes": retained, "transient_peak_allocated_bytes": peak, "phase_read_bytes": unit * 9, "phase_write_bytes": unit * 9, "phase_read_calls": 9, "phase_write_calls": 9, "phase_object_count": 9, "phase_block_count": 9, "phase_fsync_calls": 9},
        "ratios": {
            "canonical_node_bytes_per_live_node": {"numerator_bytes": artifacts[0]["logical_bytes"], "denominator_count": live // 16},
            "canonical_edge_bytes_per_live_edge": {"numerator_bytes": artifacts[1]["logical_bytes"], "denominator_count": live},
            "authoritative_project_bytes_per_live_edge": {"numerator_bytes": retained, "denominator_count": live},
            "full_lifecycle_peak_bytes_per_live_edge": {"numerator_bytes": peak, "denominator_count": live},
        },
    }


def evidence() -> dict:
    low = rung(20, 10_000, 1_000)
    high = rung(22, 40_000, 4_000)
    numerator, denominator = high["totals"]["transient_peak_allocated_bytes"], 40_000
    projected = VALIDATOR.ceil_ratio(numerator * VALIDATOR.S26_EDGES, denominator)
    volume = 50_000_000_000
    return {
        "schema": "graphforge-g500-ladder-qualification/3",
        "rungs": [low, high],
        "projection": {
            "target": "S26",
            "source_rungs": ["S20", "S22"],
            "rate": {
                "numerator_bytes": numerator,
                "denominator_count": denominator,
            },
            "projected_canonical_node_bytes": VALIDATOR.ceil_ratio(
                high["artifacts"][0]["current_retained_bytes"]
                * VALIDATOR.S26_NODES,
                high["live_nodes"],
            ),
            "projected_canonical_edge_bytes": VALIDATOR.ceil_ratio(
                high["artifacts"][1]["current_retained_bytes"]
                * VALIDATOR.S26_EDGES,
                high["live_edges"],
            ),
            "projected_lifecycle_peak_bytes": projected,
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
        ("peak_below_artifact", "below a category peak"),
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
        value["rungs"][0]["ratios"]["authoritative_project_bytes_per_live_edge"]["denominator_count"] += 1
    elif mutation == "one_rung":
        value["rungs"].pop()
    elif mutation == "nonadjacent":
        value["rungs"][1]["id"], value["rungs"][1]["scale"] = "S24", 24
    elif mutation == "understated_slope":
        value["projection"]["rate"]["numerator_bytes"] = 1
    elif mutation == "projection":
        value["projection"]["projected_lifecycle_peak_bytes"] += 1
    elif mutation == "headroom":
        value["projection"]["headroom_bytes"] += 1
    elif mutation == "unsafe_admit":
        value["projection"]["reserved_headroom_bytes"] = value["projection"]["headroom_bytes"] + 1
    elif mutation == "peak_below_artifact":
        value["rungs"][0]["totals"]["transient_peak_allocated_bytes"] = 0
    with pytest.raises(VALIDATOR.EvidenceError, match=match):
        VALIDATOR.validate(value)


def test_refuses_when_projection_does_not_leave_reserved_headroom():
    value = evidence()
    value["projection"]["reserved_headroom_bytes"] = value["projection"]["headroom_bytes"] + 1
    value["projection"]["decision"] = "refuse"
    VALIDATOR.validate(value)


def test_refuses_volume_overflow_even_with_zero_reserved_headroom():
    value = evidence()
    value["projection"]["volume_bytes"] = (
        value["projection"]["projected_lifecycle_peak_bytes"] - 1
    )
    value["projection"]["reserved_headroom_bytes"] = 0
    value["projection"]["headroom_bytes"] = 0
    value["projection"]["decision"] = "refuse"
    VALIDATOR.validate(value)


def test_canonical_projection_excludes_package_and_import_copies():
    value = evidence()
    value["projection"]["projected_canonical_edge_bytes"] = VALIDATOR.ceil_ratio(
        value["rungs"][-1]["totals"]["current_retained_bytes"]
        * VALIDATOR.S26_EDGES,
        value["rungs"][-1]["live_edges"],
    )
    with pytest.raises(VALIDATOR.EvidenceError, match="canonical edge projection"):
        VALIDATOR.validate(value)
