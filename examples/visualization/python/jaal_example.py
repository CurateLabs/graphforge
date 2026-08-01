#!/usr/bin/env python3
"""Jaal visualization over the shared GraphForge karate projection.

Limitation: Jaal is a Dash dashboard. Its public interactive path is
``Jaal(...).plot()`` (or ``create()`` + a WSGI server). This example constructs
the Jaal app through ``create()`` and writes a browser-ready payload descriptor
without starting a server in CI.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.projection import project  # noqa: E402


def to_jaal_frames(projection: dict):
    import pandas as pd

    edge_df = pd.DataFrame(
        [
            {
                "from": edge["source"],
                "to": edge["target"],
                "weight": 1,
            }
            for edge in projection["edges"]
        ]
    )
    node_df = pd.DataFrame(
        [
            {
                "id": node["id"],
                "label": node["label"],
                "club_id": node["club_id"],
            }
            for node in projection["nodes"]
        ]
    )
    return edge_df, node_df


def build_jaal_app(projection: dict):
    from jaal import Jaal

    edge_df, node_df = to_jaal_frames(projection)
    # Jaal/vis.js layout seeding is not exposed as a first-class create() argument.
    # Documented compromise: use default physics; seed is recorded in the payload.
    return Jaal(edge_df, node_df).create(directed=projection["directed"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output",
        help="Directory for JSON payload artifacts",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start Jaal's Dash server for local interactive viewing",
    )
    args = parser.parse_args()

    projection = project()
    edge_df, node_df = to_jaal_frames(projection)
    app = build_jaal_app(projection)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "library": "jaal",
        "projection_id": projection["projection_id"],
        "layout_seed_requested": projection["layout_seed"],
        "layout_seed_applied": None,
        "limitation": (
            "Jaal.create()/plot() does not accept a layout seed; interactive viewing "
            "requires a Dash server (use --serve locally)."
        ),
        "node_count": len(node_df),
        "edge_count": len(edge_df),
        "nodes": node_df.to_dict(orient="records"),
        "edges": edge_df.to_dict(orient="records"),
        "app_constructed": app is not None,
        "app_type": type(app).__name__,
    }
    payload_path = args.output_dir / "jaal_karate_payload.json"
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(payload_path)

    if args.serve:
        from jaal import Jaal

        Jaal(edge_df, node_df).plot(directed=projection["directed"])


if __name__ == "__main__":
    main()
