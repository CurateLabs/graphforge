"""Visualization adapters for the #299 stress harness."""

from __future__ import annotations

from typing import Any, Callable

from ..contract import GraphProjection
from . import cytoscape_adapter, jaal_adapter, plotly_adapter, pyvis_adapter, sigma_adapter

AdapterFn = Callable[[GraphProjection], dict[str, Any]]

ADAPTERS: dict[str, tuple[str, AdapterFn]] = {
    "plotly": ("python", plotly_adapter.render),
    "jaal": ("python", jaal_adapter.render),
    "pyvis": ("python", pyvis_adapter.render),
    "cytoscape": ("node", cytoscape_adapter.render),
    "sigma": ("node", sigma_adapter.render),
}
