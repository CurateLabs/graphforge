"""Jaal (Dash/vis.js) adapter over the shared projection."""

from __future__ import annotations

import time
from typing import Any

from ..contract import GraphProjection


def render(projection: GraphProjection) -> dict[str, Any]:
    prep_started = time.perf_counter()
    import pandas as pd

    edge_df = pd.DataFrame(
        {
            "from": [e.source for e in projection.edges],
            "to": [e.target for e in projection.edges],
        }
    )
    node_df = pd.DataFrame(
        {
            "id": [n.id for n in projection.nodes],
            "title": [n.label for n in projection.nodes],
            "group": [str(n.club_id) for n in projection.nodes],
        }
    )
    prep_seconds = time.perf_counter() - prep_started

    init_started = time.perf_counter()
    from jaal import Jaal

    # Construct the Jaal object and its Dash app without calling .plot() (server).
    viz = Jaal(edge_df, node_df)
    app = viz.create()
    # Serialize a compact readiness probe: dataframe shapes + app readiness.
    payload = {
        "nodes": int(node_df.shape[0]),
        "edges": int(edge_df.shape[0]),
        "app_ready": app is not None,
        "layout_seed": projection.layout_seed,
    }
    import json

    payload_text = json.dumps(payload, sort_keys=True)
    init_seconds = time.perf_counter() - init_started
    return {
        "viz_prep_seconds": prep_seconds,
        "renderer_init_seconds": init_seconds,
        "payload_bytes": len(payload_text.encode("utf-8")),
        "artifact_kind": "jaal_dashboard_probe",
        "divergence_notes": (
            "Jaal wraps vis.js via Dash; measurement constructs the dashboard with "
            "Jaal.create() and does not call plot()/bind a network port or open a browser."
        ),
        "payload_preview": payload_text,
    }
