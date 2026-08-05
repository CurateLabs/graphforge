"""graphforge.recipes — standalone helpers composing ``forge.execute()``.

Each recipe is thin Python over the native engine and returns a pyarrow.Table.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

    from graphforge import GraphForge

__all__ = ["neighbourhood"]

# A label / property name is interpolated into Cypher: the language can't bind
# labels or property keys as parameters, so allowlist them to bare identifiers
# to keep the query structure injection-proof.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(name: str, kind: str) -> str:
    """Validate a Cypher label or property identifier before interpolation."""
    if not isinstance(name, str) or not _IDENTIFIER.match(name):
        raise ValueError(f"{kind} must be a valid identifier, got {name!r}")
    return name


def _empty_neighbourhood_table(canonical_prop: str) -> pa.Table:
    """Typed empty Arrow table matching the neighbourhood result schema."""
    import pyarrow as pa

    labels = pa.list_(pa.string())
    if canonical_prop == "name":
        schema = pa.schema([("name", pa.string()), ("labels", labels)])
    else:
        schema = pa.schema(
            [
                (canonical_prop, pa.string()),
                ("name", pa.string()),
                ("labels", labels),
            ]
        )
    return pa.Table.from_pydict(
        {field.name: [] for field in schema},
        schema=schema,
    )


def _neighbourhood_return_clause(canonical_prop: str) -> str:
    """Build RETURN columns without duplicating the canonical property as ``name``."""
    if canonical_prop == "name":
        return "RETURN DISTINCT neighbour.name AS name, labels(neighbour) AS labels"
    return (
        f"RETURN DISTINCT neighbour.{canonical_prop} AS {canonical_prop}, "
        f"neighbour.name AS name, labels(neighbour) AS labels"
    )


def neighbourhood(
    forge: GraphForge,
    canonical: str,
    hops: int = 2,
    *,
    label: str = "Entity",
    canonical_prop: str = "canonical",
) -> pa.Table:
    """Return the n-hop neighbourhood of a seed node as a pyarrow.Table.

    ``hops=0`` returns a typed empty table with the same schema as a positive-hop
    result (no traversal). Negative hops and non-integers are rejected.
    """
    label = _identifier(label, "label")
    canonical_prop = _identifier(canonical_prop, "canonical_prop")
    if isinstance(hops, bool) or not isinstance(hops, int) or hops < 0:
        raise ValueError(f"hops must be an integer >= 0, got {hops!r}")
    if hops == 0:
        return _empty_neighbourhood_table(canonical_prop)
    query = (
        f"MATCH (seed:{label} {{{canonical_prop}: $canonical}})"
        f"-[*1..{hops}]-(neighbour:{label}) "
        f"WHERE neighbour.{canonical_prop} <> $canonical "
        f"{_neighbourhood_return_clause(canonical_prop)}"
    )
    return forge.execute(query, {"canonical": canonical})
