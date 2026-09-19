from __future__ import annotations

import unittest

from graphforge_bench.gdc_measurement_policy import (
    GdcMeasurementBoundaryError,
    assert_live_diagnostic_boundary,
)


class GdcMeasurementPolicyTests(unittest.TestCase):
    def test_valid_live_evidence_passes(self) -> None:
        assert_live_diagnostic_boundary(
            {
                "certification": False,
                "resources": {"correctness_authority": False, "load": {"wall_ms": 1}},
                "execution_authority": {"caller_supplied_result": False},
            }
        )

    def test_certification_true_fails_closed(self) -> None:
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_live_diagnostic_boundary({"certification": True})
        self.assertEqual(error.exception.cause, "certification_claim")

    def test_missing_certification_marker_fails_closed(self) -> None:
        with self.assertRaises(GdcMeasurementBoundaryError):
            assert_live_diagnostic_boundary({})

    def test_benchexec_authority_on_in_process_runner_fails_closed(self) -> None:
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_live_diagnostic_boundary(
                {
                    "certification": False,
                    "benchexec": {"authority": {"wall_seconds": 1.0}},
                }
            )
        self.assertEqual(error.exception.cause, "misattributed_authority")

    def test_resource_correctness_authority_fails_closed(self) -> None:
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_live_diagnostic_boundary(
                {
                    "certification": False,
                    "resources": {"correctness_authority": True},
                }
            )
        self.assertEqual(error.exception.cause, "resource_gate_authority")

    def test_caller_supplied_result_fails_closed(self) -> None:
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_live_diagnostic_boundary(
                {
                    "certification": False,
                    "identities": {"execution_authority": {"caller_supplied_result": True}},
                }
            )
        self.assertEqual(error.exception.cause, "caller_supplied_result")


if __name__ == "__main__":
    unittest.main()
