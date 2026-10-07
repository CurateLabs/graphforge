from __future__ import annotations

import copy
from typing import Any
import unittest

from graphforge_bench.gdc_measurement_policy import (
    QUERY_DRIVER,
    QUERY_LATENCY_CLOCK,
    GdcMeasurementBoundaryError,
    assert_live_diagnostic_boundary,
    assert_query_latency_authority,
    nearest_rank,
)

SHA = "0" * 64


def query_evidence() -> dict[str, Any]:
    """A minimal driver document; the end-to-end test checks a real one."""
    pair = {"expected": 2, "observed": 2}
    return {
        "schema": "graphforge-gdc-query-evidence/1",
        "certification": False,
        "suite": "fixture",
        "status": "passed",
        "failures": [],
        "driver": {"name": QUERY_DRIVER, "version": "0.0.0", "executable_sha256": SHA},
        "inputs": {"workload_sha256": SHA, "expected_counts_sha256": SHA},
        "project": {
            "path": "/work/project",
            "opened_with": "graphforge_api::GraphForge::new(Some(path))",
        },
        "reconciliation": {
            "status": "reconciled",
            "source": "fixture",
            "method": "count probes",
            "nodes": pair,
            "edges": pair,
            "labels": {"Person": pair},
            "types": {"KNOWS": pair},
        },
        "latency_clock": {
            "id": QUERY_LATENCY_CLOCK,
            "producer": QUERY_DRIVER,
            "source": "std::time::Instant",
            "unit": "ns",
            "interval": "one public API call returning a materialized Arrow result",
            "warmup_passes_per_variant": 1,
            "percentile_method": "nearest_rank",
        },
        "result_digest": "graphforge-gdc-result-digest/1",
        "variants": [
            {
                "query_id": "q1",
                "interface": "graphforge_api::GraphForge::execute_with_params",
                "ordered": True,
                "status": "measured",
                "warmup": {"binding_id": "b1", "excluded": True, "completed": True},
                "samples": [
                    {
                        "binding_id": f"b{index}",
                        "status": "measured",
                        "latency_ns": latency,
                        "rows": 1,
                        "result_sha256": SHA,
                    }
                    for index, latency in enumerate((300, 100, 200), start=1)
                ],
                "summary": {"count": 3, "p50_ns": 200, "p95_ns": 300},
            }
        ],
    }


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


def failed_sample(binding_id: str) -> dict[str, Any]:
    return {
        "binding_id": binding_id,
        "status": "failed",
        "cause": "query_failed",
        "error_code": "GF_PARSE",
        "error": "parse error",
    }


def failed_evidence() -> dict[str, Any]:
    """Binding b2 failed: it has no latency and the summary covers b1 and b3."""
    evidence = query_evidence()
    variant = evidence["variants"][0]
    variant["samples"][1] = failed_sample("b2")
    variant["status"] = "failed"
    variant["summary"] = {"count": 2, "p50_ns": 200, "p95_ns": 300}
    evidence["status"] = "failed"
    evidence["failures"] = [
        {"query_id": "q1", "binding_id": "b2", "cause": "query_failed", "error_code": "GF_PARSE"}
    ]
    return evidence


class QueryLatencyAuthorityTests(unittest.TestCase):
    def refused(self, evidence: dict[str, Any]) -> str:
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_query_latency_authority(evidence)
        return error.exception.cause

    def test_driver_clock_evidence_passes(self) -> None:
        assert_query_latency_authority(query_evidence())

    def test_nearest_rank(self) -> None:
        self.assertEqual(nearest_rank(list(range(1, 21)), 50), 10)
        self.assertEqual(nearest_rank(list(range(1, 21)), 95), 19)
        self.assertEqual(nearest_rank([7], 95), 7)
        self.assertEqual(nearest_rank([3, 1, 2], 50), 2)
        with self.assertRaises(ValueError):
            nearest_rank([], 50)

    def test_foreign_clock_or_producer_is_refused(self) -> None:
        for path, value in (
            (("latency_clock", "id"), "benchexec/wall-time"),
            (("latency_clock", "producer"), "graphforge-benchmark-certify"),
            (("driver", "name"), "gf query"),
        ):
            evidence = query_evidence()
            evidence[path[0]][path[1]] = value
            self.assertEqual(self.refused(evidence), "foreign_latency_clock", path)
        evidence = query_evidence()
        del evidence["latency_clock"]
        self.assertEqual(self.refused(evidence), "foreign_latency_clock")

    def test_percentiles_not_derived_from_driver_samples_are_refused(self) -> None:
        # A BenchExec wall time or any hand-entered number in a summary field.
        for field, value in (("p95_ns", 1_500_000_000), ("p50_ns", 100), ("count", 2)):
            evidence = query_evidence()
            evidence["variants"][0]["summary"][field] = value
            self.assertEqual(self.refused(evidence), "latency_not_from_samples", field)
        evidence = query_evidence()
        evidence["variants"][0]["samples"][0]["latency_ns"] = 1.5
        self.assertEqual(self.refused(evidence), "latency_not_from_samples")
        evidence = query_evidence()
        evidence["variants"][0]["samples"] = []
        self.assertEqual(self.refused(evidence), "latency_not_from_samples")

    def test_failed_run_with_failed_samples_listed_passes(self) -> None:
        assert_query_latency_authority(failed_evidence())

    def test_failed_sample_latency_is_refused(self) -> None:
        evidence = failed_evidence()
        evidence["variants"][0]["samples"][1]["latency_ns"] = 150
        self.assertEqual(self.refused(evidence), "failed_sample_latency")

    def test_summary_must_exclude_failed_samples(self) -> None:
        # The summary of all three bindings, as if the failed one had been timed.
        evidence = failed_evidence()
        evidence["variants"][0]["summary"] = {"count": 3, "p50_ns": 200, "p95_ns": 300}
        self.assertEqual(self.refused(evidence), "latency_not_from_samples")
        evidence = failed_evidence()
        evidence["variants"][0]["samples"] = [failed_sample(f"b{index}") for index in (1, 2, 3)]
        evidence["failures"] = [
            {"query_id": "q1", "binding_id": f"b{index}", "cause": "query_failed",
             "error_code": "GF_PARSE"}
            for index in (1, 2, 3)
        ]  # fmt: skip
        self.assertEqual(self.refused(evidence), "latency_not_from_samples")
        evidence["variants"][0]["summary"] = None
        assert_query_latency_authority(evidence)

    def test_failures_must_list_exactly_the_failed_samples(self) -> None:
        evidence = failed_evidence()
        evidence["failures"] = []
        self.assertEqual(self.refused(evidence), "failure_record_mismatch")
        evidence = failed_evidence()
        evidence["status"] = "passed"
        self.assertEqual(self.refused(evidence), "failure_record_mismatch")
        evidence = failed_evidence()
        evidence["variants"][0]["status"] = "measured"
        self.assertEqual(self.refused(evidence), "failure_record_mismatch")
        evidence = query_evidence()
        evidence["status"] = "failed"
        self.assertEqual(self.refused(evidence), "failure_record_mismatch")

    def test_warmup_latency_is_refused(self) -> None:
        evidence = query_evidence()
        evidence["variants"][0]["warmup"]["latency_ns"] = 5
        self.assertEqual(self.refused(evidence), "warmup_latency_included")
        evidence = query_evidence()
        evidence["variants"][0]["warmup"]["excluded"] = False
        self.assertEqual(self.refused(evidence), "warmup_latency_included")

    def test_unreconciled_project_latency_is_refused(self) -> None:
        evidence = query_evidence()
        evidence["reconciliation"]["status"] = "count_mismatch"
        self.assertEqual(self.refused(evidence), "unreconciled_counts")

    def test_other_timings_inside_the_document_are_refused(self) -> None:
        evidence = query_evidence()
        evidence["variants"][0]["samples"][1]["wall_seconds"] = 0.2
        self.assertEqual(self.refused(evidence), "invalid_document")
        evidence = query_evidence()
        evidence["variants"][0]["phase_timings"] = {"execute_ms": 3}
        self.assertEqual(self.refused(evidence), "invalid_document")
        evidence = query_evidence()
        evidence["benchexec"] = {"wall_seconds": 1.0}
        self.assertEqual(self.refused(evidence), "misattributed_authority")
        evidence = copy.deepcopy(query_evidence())
        evidence["certification"] = True
        self.assertEqual(self.refused(evidence), "certification_claim")


if __name__ == "__main__":
    unittest.main()
