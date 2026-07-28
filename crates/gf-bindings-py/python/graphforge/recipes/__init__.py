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


def neighbourhood(
    forge: GraphForge,
    canonical: str,
    hops: int = 2,
    *,
    label: str = "Entity",
    canonical_prop: str = "canonical",
) -> pa.Table:
    """Return the n-hop neighbourhood of a seed node as a pyarrow.Table."""
    label = _identifier(label, "label")
    canonical_prop = _identifier(canonical_prop, "canonical_prop")
    if isinstance(hops, bool) or not isinstance(hops, int) or hops < 1:
        raise ValueError(f"hops must be an integer >= 1, got {hops!r}")
    query = (
        f"MATCH (seed:{label} {{{canonical_prop}: $canonical}})"
        f"-[*1..{hops}]-(neighbour:{label}) "
        f"WHERE neighbour.{canonical_prop} <> $canonical "
        f"RETURN DISTINCT neighbour.{canonical_prop} AS {canonical_prop}, "
        f"neighbour.name AS name, labels(neighbour) AS labels"
    )
    return forge.execute(query, {"canonical": canonical})
