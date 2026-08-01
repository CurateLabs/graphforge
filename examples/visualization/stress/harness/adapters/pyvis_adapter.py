"""PyVis adapter over the shared projection."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

from ..contract import GraphProjection


def render(projection: GraphProjection) -> dict[str, Any]:
    prep_started = time.perf_counter()
    from pyvis.network import Network

    net = Network(height="750px", width="100%", directed=projection.directed)
    net.barnes_hut()
    for node in projection.nodes:
        net.add_node(
            node.id,
            label=node.label,
            group=str(node.club_id),
            title=node.label,
        )
    for edge in projection.edges:
        net.add_edge(edge.source, edge.target)
    prep_seconds = time.perf_counter() - prep_started

    init_started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="gf-pyvis-") as tmp:
        out = Path(tmp) / "graph.html"
        # write_html generates the self-contained HTML artifact without opening a browser.
        net.write_html(str(out), open_browser=False, notebook=False)
        payload = out.read_bytes()
    init_seconds = time.perf_counter() - init_started
    return {
        "viz_prep_seconds": prep_seconds,
        "renderer_init_seconds": init_seconds,
        "payload_bytes": len(payload),
        "artifact_kind": "pyvis_html",
        "divergence_notes": (
            "PyVis uses vis.js Barnes-Hut; layout is engine-internal and only loosely "
            "tied to the shared layout seed (seed recorded in methodology, not injected "
            "into vis.js physics)."
        ),
        "payload_preview": payload[:200].decode("utf-8", errors="replace"),
    }
