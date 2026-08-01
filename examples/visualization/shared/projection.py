"""Shared GraphForge projection for visualization examples.

Loads Mark Newman's karate-club GML through GraphForge's public Python API and
returns a deterministic node/edge projection consumed by each library adapter.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataset.fetch import fetch_dataset, load_manifest  # noqa: E402

CONTRACT_PATH = Path(__file__).resolve().parent / "contract.json"


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def parse_gml_undirected_edges(gml_text: str) -> list[tuple[int, int]]:
    """Parse undirected edges from Newman's karate.gml (source < target)."""
    edges: set[tuple[int, int]] = set()
    for match in re.finditer(
        r"edge\s*\[\s*source\s+(\d+)\s+target\s+(\d+)",
        gml_text,
    ):
        left, right = int(match.group(1)), int(match.group(2))
        if left == right:
            continue
        edges.add((left, right) if left < right else (right, left))
    return sorted(edges)


def build_graphforge(dataset_dir: Path | None = None):
    """Construct an in-memory GraphForge graph from the verified dataset."""
    from graphforge import GraphForge

    manifest = load_manifest()
    extract_dir = dataset_dir or fetch_dataset()
    gml_path = extract_dir / "karate.gml"
    edges = parse_gml_undirected_edges(gml_path.read_text(encoding="utf-8"))

    expected_nodes = manifest["graph"]["node_count"]
    expected_edges = manifest["graph"]["edge_count"]
    if len(edges) != expected_edges:
        raise RuntimeError(f"Expected {expected_edges} undirected edges, found {len(edges)}")

    forge = GraphForge()
    handles: dict[int, Any] = {}
    low, high = manifest["graph"]["node_id_range"]
    for club_id in range(low, high + 1):
        handles[club_id] = forge.add_node(
            manifest["graph"]["node_label"],
            club_id=club_id,
            label=f"M{club_id}",
        )
    if len(handles) != expected_nodes:
        raise RuntimeError(f"Expected {expected_nodes} nodes, built {len(handles)}")

    rel = manifest["graph"]["relationship_type"]
    for source, target in edges:
        forge.add_edge(handles[source], rel, handles[target])

    return forge


def project(forge=None, dataset_dir: Path | None = None) -> dict[str, Any]:
    """Return the shared projection dict via GraphForge public execute()."""
    contract = load_contract()
    engine = forge or build_graphforge(dataset_dir)

    nodes = engine.execute(contract["query"]["nodes"]).to_pylist()
    edges = engine.execute(contract["query"]["edges"]).to_pylist()

    projection = {
        "projection_id": contract["projection_id"],
        "directed": contract["edge"]["directed"],
        "layout_seed": contract["layout"]["seed"],
        "style": {
            "node_color": contract["node"]["color"],
            "node_size": contract["node"]["size"],
            "edge_color": contract["edge"]["color"],
            "edge_width": contract["edge"]["width"],
        },
        "nodes": [
            {
                "id": int(row["club_id"]),
                "label": str(row["label"]),
                "club_id": int(row["club_id"]),
            }
            for row in nodes
        ],
        "edges": [
            {
                "source": int(row["source"]),
                "target": int(row["target"]),
            }
            for row in edges
        ],
    }
    return projection


def write_projection_json(path: Path, projection: dict[str, Any] | None = None) -> Path:
    payload = projection or project()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    out = ROOT / "output" / "projection.json"
    write_projection_json(out)
    print(out)
