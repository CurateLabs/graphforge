from __future__ import annotations

import json
import unittest

from graphforge_bench.parity_gate import (
    assert_tiny_parity_ready,
    ladder_bundle_root,
    parity_gate_status,
)
from graphforge_bench.scale_parity import compare_ladder_bundle


class ParityGateTests(unittest.TestCase):
    def test_tiny_parity_ready(self) -> None:
        assert_tiny_parity_ready()

    def test_gate_status_reports_retirement_ready(self) -> None:
        status = parity_gate_status()
        self.assertTrue(status["ready_for_retirement"])
        harness = next(
            row
            for row in status["criteria"]
            if row["name"] == "harness_authoritative_after_ladder_comparison"
        )
        self.assertTrue(harness["met"])
        self.assertIsNone(harness["blocked_by"])
        legacy = next(
            row
            for row in status["criteria"]
            if row["name"] == "legacy_orchestration_retired_with_coverage"
        )
        self.assertTrue(legacy["met"])
        self.assertIn("legacy_present=False", legacy["evidence"])

    def test_historical_evidence_criterion_met(self) -> None:
        status = parity_gate_status()
        historical = next(
            row for row in status["criteria"] if row["name"] == "historical_evidence_readable"
        )
        self.assertTrue(historical["met"])

    def test_ingested_ladder_bundle_runs_comparisons(self) -> None:
        comparisons = compare_ladder_bundle(ladder_bundle_root())
        self.assertEqual(len(comparisons), 2)
        self.assertTrue(all(matrix.get("overall") for matrix in comparisons))

    def test_gate_status_json_serializable(self) -> None:
        payload = parity_gate_status()
        json.dumps(payload)


if __name__ == "__main__":
    unittest.main()
