"""Machine-readable result schema for visualization stress runs (#299)."""

from __future__ import annotations

from typing import Any

RESULT_SCHEMA_VERSION = "1.0.0"

REQUIRED_RESULT_KEYS = frozenset(
    {
        "schema_version",
        "option",
        "runtime",
        "step_id",
        "node_count",
        "edge_count",
        "seed",
        "status",
        "graphforge_projection_seconds",
        "viz_prep_seconds",
        "renderer_init_seconds",
        "peak_rss_mb",
        "payload_bytes",
        "error",
        "divergence_notes",
    }
)

VALID_STATUSES = frozenset({"success", "failure", "timeout", "resource_limit", "skipped"})

OPTIONS = (
    "plotly",
    "jaal",
    "pyvis",
    "cytoscape",
    "sigma",
)


def validate_result_record(record: dict[str, Any]) -> list[str]:
    """Return a list of schema violations (empty means valid)."""
    errors: list[str] = []
    missing = REQUIRED_RESULT_KEYS - set(record)
    if missing:
        errors.append(f"missing keys: {sorted(missing)}")
    if record.get("schema_version") != RESULT_SCHEMA_VERSION:
        errors.append(
            f"schema_version must be {RESULT_SCHEMA_VERSION!r}, got {record.get('schema_version')!r}"
        )
    if record.get("option") not in OPTIONS:
        errors.append(f"unknown option: {record.get('option')!r}")
    if record.get("status") not in VALID_STATUSES:
        errors.append(f"invalid status: {record.get('status')!r}")
    for key in (
        "node_count",
        "edge_count",
        "seed",
        "payload_bytes",
    ):
        value = record.get(key)
        if value is not None and not isinstance(value, int):
            errors.append(f"{key} must be int|null, got {type(value).__name__}")
    for key in (
        "graphforge_projection_seconds",
        "viz_prep_seconds",
        "renderer_init_seconds",
        "peak_rss_mb",
    ):
        value = record.get(key)
        if value is not None and not isinstance(value, (int, float)):
            errors.append(f"{key} must be number|null, got {type(value).__name__}")
    return errors


def empty_result(
    *,
    option: str,
    runtime: str,
    step_id: str,
    node_count: int,
    edge_count: int,
    seed: int,
    status: str,
) -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "option": option,
        "runtime": runtime,
        "step_id": step_id,
        "node_count": node_count,
        "edge_count": edge_count,
        "seed": seed,
        "status": status,
        "graphforge_projection_seconds": None,
        "viz_prep_seconds": None,
        "renderer_init_seconds": None,
        "peak_rss_mb": None,
        "payload_bytes": None,
        "error": None,
        "divergence_notes": None,
    }
