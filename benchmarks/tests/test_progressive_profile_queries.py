"""Graph500 progressive profiles must use canonical ordered-LIMIT query shapes."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from graphforge_bench.progressive_queries import (
    CANONICAL_QUERY_PHASES,
    ONE_HOP_ORDERED_LIMIT,
    TWO_HOP_ORDERED_LIMIT,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "profiles" / "graph500"


def _phase_cyphers(phase: dict) -> list[str]:
    values: list[str] = []
    for command in phase["action"]["commands"]:
        if "query" not in command:
            continue
        index = command.index("--cypher")
        values.append(command[index + 1])
    return values


def _canonical_hop_cyphers(phase_name: str, phase: dict) -> list[str]:
    cyphers = _phase_cyphers(phase)
    if phase_name == "reopen_proof":
        return cyphers[-2:]
    return cyphers


class ProgressiveProfileQueryTests(unittest.TestCase):
    def test_graph500_profiles_use_canonical_ordered_limit_queries(self) -> None:
        for path in sorted(PROFILES.glob("*.json")):
            with self.subTest(profile=path.name):
                profile = json.loads(path.read_text(encoding="utf-8"))
                phases = {entry["phase"]: entry for entry in profile["phases"]}
                for phase_name, one_hop, two_hop in CANONICAL_QUERY_PHASES:
                    cyphers = _canonical_hop_cyphers(phase_name, phases[phase_name])
                    self.assertEqual(cyphers, [one_hop, two_hop])

    def test_tiny_executable_fixture_matches_canonical_queries(self) -> None:
        profile = json.loads(
            (ROOT / "fixtures/progressive/tiny-executable.json").read_text(encoding="utf-8")
        )
        query = next(entry for entry in profile["phases"] if entry["phase"] == "query")
        cyphers = _phase_cyphers(query)
        self.assertEqual(cyphers, [ONE_HOP_ORDERED_LIMIT, TWO_HOP_ORDERED_LIMIT])


if __name__ == "__main__":
    unittest.main()
