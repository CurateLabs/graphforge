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

    def test_gate_status_reports_retirement_blocked(self) -> None:
        status = parity_gate_status()
        self.assertFalse(status["ready_for_retirement"])
        blocked = {
            row["name"]: row["blocked_by"] for row in status["criteria"] if row.get("blocked_by")
        }
        self.assertIn("harness_authoritative_after_ladder_comparison", blocked)
        self.assertEqual(blocked["harness_authoritative_after_ladder_comparison"], "#900")

    def test_historical_evidence_criterion_met(self) -> None:
        status = parity_gate_status()
        historical = next(
            row for row in status["criteria"] if row["name"] == "historical_evidence_readable"
        )
        self.assertTrue(historical["met"])

    def test_empty_ladder_bundle_returns_no_comparisons(self) -> None:
        self.assertEqual(compare_ladder_bundle(ladder_bundle_root()), [])

    def test_gate_status_json_serializable(self) -> None:
        payload = parity_gate_status()
        json.dumps(payload)


if __name__ == "__main__":
    unittest.main()
