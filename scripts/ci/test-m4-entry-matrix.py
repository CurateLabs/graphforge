#!/usr/bin/env python3
"""Mutation-sensitive tests for the M4 entry matrix contract (#334)."""

from __future__ import annotations

import copy
import importlib.util
import json
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("m4-entry-matrix.py")
SPEC = importlib.util.spec_from_file_location("m4_entry_matrix", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class M4EntryMatrixTests(unittest.TestCase):
    def test_checked_in_contract_is_valid(self) -> None:
        self.assertEqual(GATE.contract_errors(), [])
        self.assertEqual(GATE.validate(), 0)

    def test_deferred_thread_matrix_is_owned_by_337(self) -> None:
        data = GATE.load()
        deferred = {item["id"]: item for item in data["deferred_runtime_configurations"]}
        self.assertEqual(
            set(deferred),
            {"threads-1", "threads-2", "threads-4", "threads-8", "threads-automatic"},
        )
        for item in deferred.values():
            self.assertEqual(item["status"], "deferred")
            self.assertEqual(item["owner_issue"], 337)
        self.assertFalse(data["current_runtime"]["public_resource_policy"])
        self.assertEqual(data["current_runtime"]["tokio_worker_threads"], 2)

    def test_required_workloads_and_parity_assertions(self) -> None:
        data = GATE.load()
        self.assertEqual(
            {item["id"] for item in data["workloads"]},
            GATE.REQUIRED_WORKLOAD_IDS,
        )
        self.assertEqual(set(data["parity_assertions"]), GATE.REQUIRED_PARITY)

    def test_mutations_fail_closed(self) -> None:
        data = copy.deepcopy(GATE.load())
        data["current_runtime"]["tokio_worker_threads"] = 8
        self.assertTrue(
            any("tokio_worker_threads" in error for error in self._errors_from_dict(data))
        )

        data = copy.deepcopy(GATE.load())
        data["deferred_runtime_configurations"][0]["status"] = "supported"
        self.assertTrue(any("deferred" in error for error in self._errors_from_dict(data)))

        data = copy.deepcopy(GATE.load())
        data["workloads"] = [item for item in data["workloads"] if item["id"] != "pagerank"]
        self.assertTrue(any("workloads" in error for error in self._errors_from_dict(data)))

        data = copy.deepcopy(GATE.load())
        data["discovery_evidence"][0]["classification"] = "public_baseline"
        self.assertTrue(
            any(
                "discovery_not_public_facade_baseline" in error
                for error in self._errors_from_dict(data)
            )
        )

    def test_evidence_schema_rejects_fabricated_deferred_execution(self) -> None:
        contract = GATE.load()
        payload = {
            "schema": GATE.EVIDENCE_SCHEMA,
            "contract_schema": GATE.SCHEMA,
            "source_sha": "abc",
            "build_profile": "release",
            "runtime_configuration": {
                "status": "supported",
                "tokio_worker_threads": 2,
            },
            "hardware": {"os": "linux", "logical_cpus": 2},
            "workloads": [
                {
                    "id": "scan-count",
                    "structural_gates": {"output_rows": 5},
                    "timing_observation": {"wall_time_ms": 1.2},
                    "timing_is_pass_fail": False,
                }
            ],
            "deferred_configurations": [
                {"id": "threads-8", "status": "deferred", "executed": True, "owner_issue": 337}
            ],
            "discovery_evidence": [
                {
                    "id": "lower-level-8m-128m",
                    "classification": "discovery_not_public_facade_baseline",
                }
            ],
        }
        errors = GATE.evidence_errors(payload, contract)
        self.assertTrue(any("must not claim deferred configs executed" in e for e in errors))

        payload["deferred_configurations"][0]["executed"] = False
        payload["runtime_configuration"] = {
            "status": "supported",
            "tokio_worker_threads": 2,
            "requested_workers": 8,
        }
        errors = GATE.evidence_errors(payload, contract)
        self.assertTrue(any("must not claim unsupported thread requests" in e for e in errors))

    def test_evidence_accepts_honest_supported_runtime(self) -> None:
        contract = GATE.load()
        payload = {
            "schema": GATE.EVIDENCE_SCHEMA,
            "contract_schema": GATE.SCHEMA,
            "source_sha": "abc",
            "build_profile": "debug",
            "runtime_configuration": {
                "status": "supported",
                "id": "fixed-two-worker",
                "tokio_worker_threads": 2,
            },
            "hardware": {"os": "linux", "logical_cpus": 8, "memory_bytes": 16 << 30},
            "workloads": [
                {
                    "id": "fixed-hop-limit",
                    "structural_gates": {"output_rows": 1000},
                    "timing_observation": {"wall_time_ms": 12.5},
                    "timing_is_pass_fail": False,
                }
            ],
            "deferred_configurations": [
                {"id": item["id"], "status": "deferred", "executed": False, "owner_issue": 337}
                for item in contract["deferred_runtime_configurations"]
            ],
            "discovery_evidence": contract["discovery_evidence"],
        }
        self.assertEqual(GATE.evidence_errors(payload, contract), [])

    def _errors_from_dict(self, data: dict) -> list[str]:
        path = Path(self.id().replace(".", "_") + ".json")
        # Write beside the module into a temp-like unique name under /tmp via unittest isolation.
        target = Path("/tmp") / path.name
        target.write_text(json.dumps(data), encoding="utf-8")
        try:
            return GATE.contract_errors(target)
        finally:
            target.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
