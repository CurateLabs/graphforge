"""Canonical Graph500 progressive qualification query shapes (#900, #904)."""

from __future__ import annotations

ONE_HOP_ORDERED_LIMIT = (
    "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000"
)
TWO_HOP_ORDERED_LIMIT = (
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000"
)

CANONICAL_QUERY_PHASES = (
    ("query", ONE_HOP_ORDERED_LIMIT, TWO_HOP_ORDERED_LIMIT),
    ("reopen_proof", ONE_HOP_ORDERED_LIMIT, TWO_HOP_ORDERED_LIMIT),
)
