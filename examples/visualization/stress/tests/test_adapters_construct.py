"""Adapter construction-path tests without the full stress ladder."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

STRESS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STRESS_ROOT))

from harness.adapters import ADAPTERS  # noqa: E402
from harness.contract import build_step_projection  # noqa: E402


@pytest.fixture(scope="module")
def small_projection():
    projection, _elapsed = build_step_projection(12, seed=29901, use_graphforge=False)
    assert len(projection.nodes) == 12
    return projection


def test_plotly_construct(small_projection) -> None:
    pytest.importorskip("plotly")
    outcome = ADAPTERS["plotly"][1](small_projection)
    assert outcome["payload_bytes"] > 0
    assert outcome["artifact_kind"] == "plotly_json"


def test_plotly_js_construct(small_projection) -> None:
    if shutil.which("node") is None:
        pytest.skip("node not installed")
    outcome = ADAPTERS["plotly_js"][1](small_projection)
    assert outcome["payload_bytes"] > 0
    assert outcome["artifact_kind"] == "plotly_js_json"


def test_pyvis_construct(small_projection) -> None:
    pytest.importorskip("pyvis")
    outcome = ADAPTERS["pyvis"][1](small_projection)
    assert outcome["payload_bytes"] > 1000
    assert "html" in outcome["artifact_kind"]


def test_jaal_construct(small_projection) -> None:
    pytest.importorskip("jaal")
    outcome = ADAPTERS["jaal"][1](small_projection)
    assert outcome["payload_bytes"] > 0
    assert json.loads(outcome["payload_preview"])["app_ready"] is True


def test_cytoscape_construct(small_projection) -> None:
    if shutil.which("node") is None:
        pytest.skip("node not installed")
    node_dir = STRESS_ROOT / "node"
    if not (node_dir / "node_modules" / "cytoscape").exists():
        subprocess.run(["npm", "install", "--no-fund", "--no-audit"], cwd=node_dir, check=True)
    outcome = ADAPTERS["cytoscape"][1](small_projection)
    assert outcome["payload_bytes"] > 0


def test_sigma_construct(small_projection) -> None:
    if shutil.which("node") is None:
        pytest.skip("node not installed")
    node_dir = STRESS_ROOT / "node"
    if not (node_dir / "node_modules" / "graphology").exists():
        subprocess.run(["npm", "install", "--no-fund", "--no-audit"], cwd=node_dir, check=True)
    outcome = ADAPTERS["sigma"][1](small_projection)
    assert outcome["payload_bytes"] > 0
