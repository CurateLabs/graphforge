"""Schema and fixture-selection unit tests (no full stress matrix)."""

from __future__ import annotations

import json
from pathlib import Path
import sys

STRESS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STRESS_ROOT))

from harness.contract import (  # noqa: E402
    load_edge_list_dataset,
    projection_from_edges,
    sample_subgraph,
)
from harness.schema import (  # noqa: E402
    OPTIONS,
    RESULT_SCHEMA_VERSION,
    empty_result,
    validate_result_record,
)


def test_result_schema_accepts_valid_record() -> None:
    record = empty_result(
        option="plotly",
        runtime="python",
        step_id="xs",
        node_count=10,
        edge_count=12,
        seed=29901,
        status="success",
    )
    record["graphforge_projection_seconds"] = 0.01
    record["viz_prep_seconds"] = 0.02
    record["renderer_init_seconds"] = 0.03
    record["peak_rss_mb"] = 100.0
    record["payload_bytes"] = 1234
    assert validate_result_record(record) == []


def test_result_schema_rejects_unknown_option() -> None:
    record = empty_result(
        option="plotly",
        runtime="python",
        step_id="xs",
        node_count=1,
        edge_count=0,
        seed=1,
        status="success",
    )
    record["option"] = "not-a-library"
    assert any("unknown option" in err for err in validate_result_record(record))


def test_all_options_listed() -> None:
    assert set(OPTIONS) == {
        "plotly",
        "plotly_js",
        "jaal",
        "pyvis",
        "cytoscape",
        "sigma",
    }
    assert RESULT_SCHEMA_VERSION == "1.0.0"


def test_size_ladder_spec_is_complete() -> None:
    ladder = json.loads((STRESS_ROOT / "size_ladder.json").read_text(encoding="utf-8"))
    assert ladder["seed"] == 29901
    assert "stopping" in ladder
    assert ladder["stopping"]["per_option_timeout_seconds"] > 0
    assert ladder["steps"]
    assert ladder["steps"][0]["target_nodes"] < ladder["steps"][-1]["target_nodes"]


def test_deterministic_subgraph_selection() -> None:
    edges, _prov = load_edge_list_dataset("karate")
    a_nodes, a_edges = sample_subgraph(edges, 15, seed=29901)
    b_nodes, b_edges = sample_subgraph(edges, 15, seed=29901)
    assert a_nodes == b_nodes
    assert a_edges == b_edges
    assert len(a_nodes) == 15
    # Same seed must be stable across calls; different seed must change the walk.
    c_nodes, _ = sample_subgraph(edges, 15, seed=7)
    assert c_nodes != a_nodes


def test_projection_contract_fields() -> None:
    edges, provenance = load_edge_list_dataset("karate")
    _nodes, sub = sample_subgraph(edges, 10, 29901)
    projection = projection_from_edges(
        sub,
        dataset_id=provenance["dataset"],
        checksum=provenance["checksum_sha256"],
        provenance=provenance,
    )
    payload = projection.to_dict()
    assert payload["projection_id"] == "karate-member-friend-v1"
    assert len(payload["nodes"]) == 10
    for node in payload["nodes"]:
        assert set(node) >= {"id", "label", "club_id"}
    for edge in payload["edges"]:
        assert set(edge) >= {"source", "target", "type", "directed"}
