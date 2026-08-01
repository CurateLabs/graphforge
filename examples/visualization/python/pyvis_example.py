#!/usr/bin/env python3
"""PyVis visualization over the shared GraphForge karate projection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.projection import project  # noqa: E402


def build_network(projection: dict):
    from pyvis.network import Network

    net = Network(
        height="700px",
        width="100%",
        bgcolor="#ffffff",
        font_color="#1a202c",
        directed=projection["directed"],
    )
    # PyVis/vis.js accepts a numeric seed for reproducible physics layout.
    net.set_options(
        json.dumps(
            {
                "physics": {
                    "enabled": True,
                    "barnesHut": {"gravitationalConstant": -8000},
                    "stabilization": {"iterations": 100},
                },
                "layout": {"randomSeed": projection["layout_seed"]},
                "interaction": {"hover": True},
            }
        )
    )
    for node in projection["nodes"]:
        net.add_node(
            node["id"],
            label=node["label"],
            color=projection["style"]["node_color"],
            size=projection["style"]["node_size"],
        )
    for edge in projection["edges"]:
        net.add_edge(
            edge["source"],
            edge["target"],
            color=projection["style"]["edge_color"],
            width=projection["style"]["edge_width"],
        )
    return net


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output",
        help="Directory for HTML artifacts",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Also open the HTML in a browser (local use only)",
    )
    args = parser.parse_args()

    projection = project()
    net = build_network(projection)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    html_path = args.output_dir / "pyvis_karate.html"
    net.write_html(str(html_path), open_browser=args.show, notebook=False)
    print(html_path)


if __name__ == "__main__":
    main()
