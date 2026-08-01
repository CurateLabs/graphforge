"""Plotly scatter/network adapter over the shared projection."""

from __future__ import annotations

import json
import time
from typing import Any

from ..contract import GraphProjection


def render(projection: GraphProjection) -> dict[str, Any]:
    prep_started = time.perf_counter()
    # Deterministic circular layout (shared seed) — Plotly has no built-in graph layout.
    import math

    n = len(projection.nodes)
    positions: dict[str, tuple[float, float]] = {}
    for i, node in enumerate(projection.nodes):
        angle = (2 * math.pi * i / max(n, 1)) + (projection.layout_seed % 360) * math.pi / 180
        positions[node.id] = (math.cos(angle), math.sin(angle))

    node_x = [positions[node.id][0] for node in projection.nodes]
    node_y = [positions[node.id][1] for node in projection.nodes]
    node_text = [f"{node.label} (club_id={node.club_id})" for node in projection.nodes]

    edge_x: list[float | None] = []
    edge_y: list[float | None] = []
    for edge in projection.edges:
        x0, y0 = positions[edge.source]
        x1, y1 = positions[edge.target]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    prep_seconds = time.perf_counter() - prep_started

    init_started = time.perf_counter()
    import plotly.graph_objects as go

    fig = go.Figure(
        data=[
            go.Scatter(
                x=edge_x,
                y=edge_y,
                mode="lines",
                line={"width": 0.5, "color": "#888"},
                hoverinfo="none",
                name="edges",
            ),
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers",
                marker={"size": 8},
                text=node_text,
                hoverinfo="text",
                name="nodes",
            ),
        ],
        layout=go.Layout(
            title="GraphForge visualization stress — Plotly",
            showlegend=False,
            xaxis={"showgrid": False, "zeroline": False, "visible": False},
            yaxis={"showgrid": False, "zeroline": False, "visible": False},
        ),
    )
    payload = fig.to_json()
    init_seconds = time.perf_counter() - init_started
    return {
        "viz_prep_seconds": prep_seconds,
        "renderer_init_seconds": init_seconds,
        "payload_bytes": len(payload.encode("utf-8")),
        "artifact_kind": "plotly_json",
        "divergence_notes": (
            "Plotly has no native force-directed layout; uses deterministic circular "
            "coordinates derived from the shared layout seed."
        ),
        "payload_preview": payload[:200],
    }
