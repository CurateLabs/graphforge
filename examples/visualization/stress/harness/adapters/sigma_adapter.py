"""Sigma.js adapter — delegates graphology graph construction to Node."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from ..contract import GraphProjection

NODE_DIR = Path(__file__).resolve().parents[2] / "node"


def render(projection: GraphProjection) -> dict[str, Any]:
    prep_started = time.perf_counter()
    request = {
        "nodes": [
            {"id": n.id, "label": n.label, "club_id": n.club_id}
            for n in projection.nodes
        ],
        "edges": [
            {
                "id": f"e-{e.source}-{e.target}",
                "source": e.source,
                "target": e.target,
                "type": e.type,
            }
            for e in projection.edges
        ],
        "layout_seed": projection.layout_seed,
        "directed": projection.directed,
    }
    prep_seconds = time.perf_counter() - prep_started

    init_started = time.perf_counter()
    script = NODE_DIR / "sigma_render.mjs"
    proc = subprocess.run(
        ["node", str(script)],
        input=json.dumps(request),
        capture_output=True,
        text=True,
        cwd=str(NODE_DIR),
        check=False,
    )
    init_seconds = time.perf_counter() - init_started
    if proc.returncode != 0:
        raise RuntimeError(
            f"sigma_render.mjs failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    response = json.loads(proc.stdout)
    return {
        "viz_prep_seconds": prep_seconds,
        "renderer_init_seconds": init_seconds + float(response.get("construct_seconds", 0)),
        "payload_bytes": int(response["payload_bytes"]),
        "artifact_kind": "graphology_serialized",
        "divergence_notes": response.get(
            "divergence_notes",
            "Headless graphology graph for Sigma.js; WebGL renderer not started.",
        ),
        "payload_preview": response.get("payload_preview", ""),
    }
