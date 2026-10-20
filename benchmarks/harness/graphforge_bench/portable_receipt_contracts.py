"""Portable receipt contract IDs, bound to the binary that produced the evidence.

Live runs require the current IDs. The recorded host-ladder evidence below was
produced by binaries that emitted the earlier IDs and is hash-chained, so it is
never edited; it is validated against the IDs of its own recorded source commit.
An unknown commit gets the current IDs, so neither a recorded bundle relabelled
with current IDs nor a new run using retired IDs validates.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PortableContracts:
    export: str
    import_: str
    verify: str


CURRENT = PortableContracts(
    export="graphforge-portable-export/1",
    import_="graphforge-portable-import/1",
    verify="graphforge-portable-verify/1",
)

_RETIRED_ERA = PortableContracts(
    export="graphforge-portable-export/2",
    import_="graphforge-portable-import/2",
    verify="graphforge-portable-verify/2",
)

# Source commit recorded in a result's identities -> the IDs that binary emitted.
RECORDED_EVIDENCE_CONTRACTS: Mapping[str, PortableContracts] = {
    # S18-S26 native host ladder (parity bundle).
    "114b1964874419b31d13820bfbdf72fe57286544": _RETIRED_ERA,
    # Preserved diagnostic rung fixtures under tests/fixtures/rungs.
    "6cbfde81494f864a3a61e4d887138aaaa256c92e": _RETIRED_ERA,
    "710c6c64f4718c0664d08bdd3aafe21de4a29eba": _RETIRED_ERA,
}

_FAMILY = re.compile(r"graphforge-portable-(export|import|verify)/[0-9]+")


def contracts_for_commit(commit: str) -> PortableContracts:
    return RECORDED_EVIDENCE_CONTRACTS.get(commit, CURRENT)


def stray_contracts(document: Any, expected: PortableContracts) -> list[str]:
    """Portable receipt contract values in `document` that differ from `expected`."""
    wanted = {
        "export": expected.export,
        "import": expected.import_,
        "verify": expected.verify,
    }
    found: list[str] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            contract = value.get("contract")
            if isinstance(contract, str):
                match = _FAMILY.fullmatch(contract)
                if match and contract != wanted[match.group(1)]:
                    found.append(contract)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    return found
