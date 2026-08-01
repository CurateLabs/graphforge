"""Focused checks for Python visualization examples (#298).

These are example-suite tests, not required CI / release gates. They construct
each visualizer far enough to write an artifact or browser-ready payload without
opening an interactive browser.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output" / "test-python"

pytest.importorskip("graphforge")
pytest.importorskip("plotly")
pytest.importorskip("pyvis")
pytest.importorskip("jaal")
pytest.importorskip("pandas")


@pytest.fixture(scope="module")
def projection():
    sys.path.insert(0, str(ROOT))
    from shared.projection import project

    payload = project()
    assert len(payload["nodes"]) == 34
    assert len(payload["edges"]) == 78
    assert payload["projection_id"] == "karate-member-friend-v1"
    return payload


def test_plotly_writes_html_and_json(projection, tmp_path):
    from python.plotly_example import build_figure

    fig = build_figure(projection)
    html_path = tmp_path / "plotly_karate.html"
    json_path = tmp_path / "plotly_karate.json"
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)
    json_path.write_text(json.dumps(fig.to_plotly_json()), encoding="utf-8")
    assert html_path.is_file() and html_path.stat().st_size > 100
    assert "plotly" in html_path.read_text(encoding="utf-8").lower()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert "data" in payload and len(payload["data"]) >= 2


def test_pyvis_writes_html(projection, tmp_path):
    from python.pyvis_example import build_network

    net = build_network(projection)
    html_path = tmp_path / "pyvis_karate.html"
    net.write_html(str(html_path), open_browser=False, notebook=False)
    text = html_path.read_text(encoding="utf-8")
    assert "vis" in text.lower() or "network" in text.lower()
    assert "M1" in text


def test_jaal_constructs_app_and_payload(projection, tmp_path):
    from python.jaal_example import build_jaal_app, to_jaal_frames

    edge_df, node_df = to_jaal_frames(projection)
    assert len(node_df) == 34
    assert len(edge_df) == 78
    app = build_jaal_app(projection)
    assert app is not None

    payload = {
        "app_constructed": True,
        "app_type": type(app).__name__,
        "node_count": len(node_df),
        "edge_count": len(edge_df),
    }
    path = tmp_path / "jaal_karate_payload.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert json.loads(path.read_text())["node_count"] == 34


def test_example_scripts_cli(tmp_path):
    """Smoke each example script end-to-end with GraphForge + artifact output."""
    out = tmp_path / "cli"
    out.mkdir()
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    scripts = [
        ROOT / "python" / "plotly_example.py",
        ROOT / "python" / "pyvis_example.py",
        ROOT / "python" / "jaal_example.py",
    ]
    for script in scripts:
        subprocess.run(
            [sys.executable, str(script), "--output-dir", str(out)],
            check=True,
            cwd=str(ROOT),
            env=env,
        )
    assert (out / "plotly_karate.html").is_file()
    assert (out / "pyvis_karate.html").is_file()
    assert (out / "jaal_karate_payload.json").is_file()
