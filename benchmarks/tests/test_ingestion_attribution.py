from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from graphforge_bench.ingestion_attribution import (
    analyze,
    expected_commands,
    interval_union_ns,
    merge_summary,
)
from graphforge_bench.native_rung import read_native_rung

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT.parent / "docs/development/evidence/query-rss-1278-repair"


class IngestionAttributionTests(unittest.TestCase):
    def test_completed_integrated_prefix_has_exact_fraction_and_residual(self):
        report = analyze(ROOT, EVIDENCE)
        self.assertEqual(list(report["rungs"]), ["S18", "S19", "S20", "S22"])
        for rung in report["rungs"].values():
            ingestion_ns = rung["phase_wall_ms"]["ingest"] * 1_000_000
            self.assertEqual(
                ingestion_ns,
                rung["construction_call_wall_ns"] + rung["ingest_outside_construction_calls_ns"],
            )
            self.assertAlmostEqual(
                rung["ingestion_fraction"],
                ingestion_ns / 1e9 / rung["whole_lifecycle_benchexec"]["wall_seconds"],
            )
        self.assertEqual(report["rungs"]["S20"]["phase_wall_ms"]["ingest"], 197749)
        self.assertNotIn("admission", report)

    def test_individually_valid_mixed_prefix_is_rejected(self):
        docs = [read_native_rung(ROOT, EVIDENCE, scale) for scale in (18, 19, 20, 22)]
        for field in ("commit", "gf_sha256", "host_profile_sha256", "generator"):
            modified = copy.deepcopy(docs)
            modified[0]["result"]["identities"][field] = "different"
            with (
                patch(
                    "graphforge_bench.ingestion_attribution.read_native_rung", side_effect=modified
                ),
                self.assertRaisesRegex(ValueError, "different"),
            ):
                analyze(ROOT, EVIDENCE)

    def test_unfinished_receipt_is_not_treated_as_zero(self):
        with (
            patch(
                "graphforge_bench.ingestion_attribution.read_native_rung",
                side_effect=ValueError("unfinished S22"),
            ),
            self.assertRaises(ValueError),
        ):
            analyze(ROOT, EVIDENCE)

    def test_preselection_requires_every_lifecycle_command(self):
        profile = json.loads((ROOT / "profiles/graph500/s18-local.json").read_text())
        selection = [{"name": "s16", "repetition": 0}, {"name": "s16", "repetition": 1}]
        commands = expected_commands(selection, profile, False)
        self.assertEqual(len(commands), 42)
        self.assertIn("s16-r0-ingest-3", commands)
        self.assertIn("s16-r1-reopen_proof-4", commands)
        self.assertEqual(len(commands), len(set(commands)))
        boundary = expected_commands(selection, profile, True)
        self.assertIn("s16-r0-ingest", boundary)
        self.assertNotIn("s16-r0-ingest-3", boundary)

    def test_input_census_without_merge_work_is_incomplete(self):
        line = 'INGEST_DIAGNOSTIC {"event":"inputs","family":"merge-identities","runs":1025}'
        with self.assertRaisesRegex(ValueError, "missing merge groups"):
            merge_summary([line])

    def test_nested_wall_intervals_are_not_added_twice(self):
        self.assertEqual(interval_union_ns([(10, 80), (30, 60), (70, 95)]), 85)
        self.assertEqual(interval_union_ns([(0, 10), (20, 30)]), 20)
        with self.assertRaises(ValueError):
            interval_union_ns([(20, 10)])

    def test_merge_aggregator_rejects_incomplete_and_private_events(self):
        for event in (
            {"event": "inputs", "family": "/private/path", "runs": 1},
            {"event": "group", "family": "merge-identities", "success": False},
        ):
            with self.assertRaises(ValueError):
                merge_summary(["INGEST_DIAGNOSTIC " + json.dumps(event)])
        with self.assertRaises(ValueError):
            merge_summary([])


if __name__ == "__main__":
    unittest.main()
