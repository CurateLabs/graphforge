"""Routine-CI coverage for the #299 visualization stress harness (no full matrix)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STRESS_ROOT = ROOT / "examples" / "visualization" / "stress"
sys.path.insert(0, str(STRESS_ROOT))

from harness.contract import (  # noqa: E402
    load_edge_list_dataset,
    projection_from_edges,
    sample_subgraph,
)
from harness.schema import OPTIONS, empty_result, validate_result_record  # noqa: E402


def test_size_ladder_frozen_before_run() -> None:
    ladder = json.loads((STRESS_ROOT / "size_ladder.json").read_text(encoding="utf-8"))
    assert ladder["seed"] == 29901
    assert ladder["stopping"]["per_option_timeout_seconds"] == 120
    assert ladder["stopping"]["ladder_stop_on_first_failure_or_timeout"] is True
    assert [step["target_nodes"] for step in ladder["steps"]] == [
        10,
        20,
        34,
        100,
        250,
        500,
        1000,
        2000,
        4039,
    ]


def test_dispatch_only_workflow_has_no_pr_push_schedule() -> None:
    workflow = (
        ROOT / ".github" / "workflows" / "visualization-limits-stress.yml"
    ).read_text(encoding="utf-8")
    assert "workflow_dispatch" in workflow
    # Guard against accidentally wiring this into routine CI.
    for forbidden in ("pull_request:", "push:", "schedule:"):
        assert forbidden not in workflow


def test_result_schema_and_five_options() -> None:
    assert set(OPTIONS) == {"plotly", "jaal", "pyvis", "cytoscape", "sigma"}
    record = empty_result(
        option="sigma",
        runtime="node",
        step_id="xs",
        node_count=10,
        edge_count=9,
        seed=29901,
        status="success",
    )
    record["graphforge_projection_seconds"] = 0.1
    record["viz_prep_seconds"] = 0.2
    record["renderer_init_seconds"] = 0.3
    record["peak_rss_mb"] = 50.0
    record["payload_bytes"] = 99
    assert validate_result_record(record) == []


def test_deterministic_karate_sampling() -> None:
    edges, provenance = load_edge_list_dataset("karate")
    assert provenance["node_count"] == 34
    a_nodes, a_edges = sample_subgraph(edges, 12, 29901)
    b_nodes, b_edges = sample_subgraph(edges, 12, 29901)
    assert a_nodes == b_nodes
    assert a_edges == b_edges
    projection = projection_from_edges(
        a_edges,
        dataset_id=provenance["dataset"],
        checksum=provenance["checksum_sha256"],
        provenance=provenance,
    )
    assert len(projection.nodes) == 12
    assert projection.to_dict()["projection_id"] == "karate-member-friend-v1"
    assert projection.layout_seed == 42
