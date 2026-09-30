"""Native-wheel acceptance for the visualization example's GraphForge path (#1675).

The documented example builds the karate-club graph through the public Python
API and projects it with ``execute()``. Nothing ran it, so an API change broke
it unnoticed. This check runs that exact path against the installed wheel with
no network and no visualization library: the archive is replaced by a generated
GML that has the manifest's node range and edge count.
"""

from __future__ import annotations

import itertools
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "examples" / "visualization"))

from dataset.fetch import load_manifest  # noqa: E402
from shared.projection import project  # noqa: E402


def write_generated_gml(directory: Path) -> list[tuple[int, int]]:
    """Write a GML with the manifest's identifier range and undirected edge count."""
    graph = load_manifest()["graph"]
    low, high = graph["node_id_range"]
    pairs = list(itertools.combinations(range(low, high + 1), 2))[: graph["edge_count"]]
    assert len(pairs) == graph["edge_count"], "manifest edge count exceeds a simple graph"
    body = "".join(f"  node\n  [\n    id {node}\n  ]\n" for node in range(low, high + 1))
    body += "".join(
        f"  edge\n  [\n    source {source}\n    target {target}\n  ]\n" for source, target in pairs
    )
    (directory / "karate.gml").write_text(f"graph\n[\n{body}]\n", encoding="utf-8")
    return pairs


def main() -> None:
    graph = load_manifest()["graph"]
    low, high = graph["node_id_range"]
    with tempfile.TemporaryDirectory() as directory:
        dataset = Path(directory)
        pairs = write_generated_gml(dataset)
        projection = project(dataset_dir=dataset)
    assert [node["id"] for node in projection["nodes"]] == list(range(low, high + 1))
    assert [node["label"] for node in projection["nodes"]] == [
        f"M{member}" for member in range(low, high + 1)
    ]
    assert sorted((edge["source"], edge["target"]) for edge in projection["edges"]) == pairs
    assert projection["directed"] is graph["directed"]
    print("visualization example projection ok")


if __name__ == "__main__":
    main()
