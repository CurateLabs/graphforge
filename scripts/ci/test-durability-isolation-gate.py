#!/usr/bin/env python3
"""Mutation-sensitive tests for the durability/isolation contract gate (#748)."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("durability-isolation-gate.py")
SPEC = importlib.util.spec_from_file_location("durability_isolation_gate", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class DurabilityIsolationGateTests(unittest.TestCase):
    def test_checked_in_contract_is_valid(self) -> None:
        matrix = GATE.validate_matrix()
        self.assertEqual(matrix["contract"], GATE.CONTRACT)
        self.assertEqual(GATE.main(["validate"]), 0)

    def test_bdd_scenarios_and_write_skew_honesty(self) -> None:
        matrix = GATE.load_matrix()
        self.assertEqual(
            {item["id"] for item in matrix["bdd_scenarios"]},
            GATE.REQUIRED_BDD,
        )
        write_skew = next(item for item in matrix["anomalies"] if item["id"] == "write_skew")
        self.assertEqual(
            write_skew["modes"]["optimistic_multi_writer"],
            "allowed_documented_not_ssi",
        )
        self.assertFalse(matrix["write_modes"]["optimistic_multi_writer"]["ssi_claimed"])

    def test_deferred_cells_map_to_later_m6_issues(self) -> None:
        matrix = GATE.load_matrix()
        owners = set()
        for section in ("crash_phases", "anomalies", "lifecycle"):
            for cell in matrix[section]:
                if cell["coverage"] in {"deferred", "partial"} or (
                    cell["coverage"] == "documented" and "owner_issue" in cell
                ):
                    owners.add(cell["owner_issue"])
        self.assertTrue(owners)
        self.assertTrue(owners <= set(range(749, 757)))

    def test_mutations_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            data = copy.deepcopy(GATE.load_matrix())
            data["write_modes"]["optimistic_multi_writer"]["ssi_claimed"] = True
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(GATE.GateError):
                GATE.validate_matrix(path)

            data = copy.deepcopy(GATE.load_matrix())
            data["anomalies"] = [item for item in data["anomalies"] if item["id"] != "write_skew"]
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(GATE.GateError):
                GATE.validate_matrix(path)

            data = copy.deepcopy(GATE.load_matrix())
            data["acknowledgement"]["requires"] = [
                item
                for item in data["acknowledgement"]["requires"]
                if item != "project_root_directory_flush"
            ]
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(GATE.GateError):
                GATE.validate_matrix(path)

            data = copy.deepcopy(GATE.load_matrix())
            data["filesystem_scope"]["best_effort_allowed"] = True
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(GATE.GateError):
                GATE.validate_matrix(path)

            data = copy.deepcopy(GATE.load_matrix())
            covered = next(item for item in data["crash_phases"] if item["coverage"] == "covered")
            covered["evidence"][0]["symbol"] = "missing_symbol_for_gate_test"
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(GATE.GateError):
                GATE.validate_matrix(path)

    def test_report_emission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            self.assertEqual(GATE.main(["report", "--output", str(output)]), 0)
            report = json.loads((output / "durability-isolation-report.json").read_text())
            self.assertEqual(report["contract"], GATE.CONTRACT)
            self.assertEqual(report["issue"], 748)
            self.assertTrue((output / "durability-isolation-report.sha256").is_file())


if __name__ == "__main__":
    unittest.main()
