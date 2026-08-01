#!/usr/bin/env python3
"""Plotly visualization over the shared GraphForge karate projection."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.projection import project  # noqa: E402


def _seeded_layout(node_ids: list[int], seed: int) -> dict[int, tuple[float, float]]:
    """Deterministic circular layout (Plotly has no graph layout seed of its own)."""
    count = len(node_ids)
    positions: dict[int, tuple[float, float]] = {}
    for index, node_id in enumerate(sorted(node_ids)):
        angle = (2.0 * math.pi * index / count) + (seed % 360) * math.pi / 180.0
        positions[node_id] = (math.cos(angle), math.sin(angle))
    return positions


def build_figure(projection: dict):
    import plotly.graph_objects as go

    positions = _seeded_layout(
        [node["id"] for node in projection["nodes"]],
        projection["layout_seed"],
    )
    edge_x: list[float | None] = []
    edge_y: list[float | None] = []
    for edge in projection["edges"]:
        x0, y0 = positions[edge["source"]]
        x1, y1 = positions[edge["target"]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    node_x = [positions[node["id"]][0] for node in projection["nodes"]]
    node_y = [positions[node["id"]][1] for node in projection["nodes"]]
    labels = [node["label"] for node in projection["nodes"]]

    fig = go.Figure(
        data=[
            go.Scatter(
                x=edge_x,
                y=edge_y,
                mode="lines",
                line={
                    "width": projection["style"]["edge_width"],
                    "color": projection["style"]["edge_color"],
                },
                hoverinfo="none",
                name="edges",
            ),
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                text=labels,
                textposition="top center",
                marker={
                    "size": projection["style"]["node_size"],
                    "color": projection["style"]["node_color"],
                },
                name="nodes",
            ),
        ],
        layout=go.Layout(
            title="Zachary karate club (Plotly / GraphForge projection)",
            showlegend=False,
            hovermode="closest",
            xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        ),
    )
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output",
        help="Directory for HTML/JSON artifacts",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Open the interactive figure in a browser (local use only)",
    )
    args = parser.parse_args()

    projection = project()
    fig = build_figure(projection)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    html_path = args.output_dir / "plotly_karate.html"
    json_path = args.output_dir / "plotly_karate.json"
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)
    json_path.write_text(json.dumps(fig.to_plotly_json(), indent=2) + "\n", encoding="utf-8")
    print(html_path)
    print(json_path)
    if args.show:
        fig.show()


if __name__ == "__main__":
    main()
