"""Canonical JSON shared by Pulumi validation and deployment projections."""

from __future__ import annotations

import json
from typing import Any

_LEGACY_ESCAPES = {
    "<": r"\u003c",
    ">": r"\u003e",
    "&": r"\u0026",
    "\u2028": r"\u2028",
    "\u2029": r"\u2029",
}


def canonical_json_text(value: Any) -> str:
    """Match Terraform ``jsonencode`` key ordering and legacy escaping."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    for character, escape in _LEGACY_ESCAPES.items():
        encoded = encoded.replace(character, escape)
    return encoded


def canonical_json_bytes(value: Any) -> bytes:
    """Return canonical UTF-8 bytes without a trailing newline."""
    return canonical_json_text(value).encode()
