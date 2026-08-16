#!/usr/bin/env python3
"""Mutation-sensitive tests for the durability certification gate (#756)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("durability-certification-gate.py")
SPEC = importlib.util.spec_from_file_location("durability_certification_gate", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class DurabilityCertificationGateTests(unittest.TestCase):
    def test_checked_in_contract_is_valid(self) -> None:
        self.assertEqual(GATE.main(["validate"]), 0)
        contract = GATE.load_cert_contract()
        self.assertEqual(contract["contract"], GATE.CONTRACT)
        self.assertEqual(contract["seed"], GATE.CERT_SEED)

    def test_seed_and_budget_mutations_fail_closed(self) -> None:
        with self.assertRaises(GATE.GateError):
            GATE.validate_config(GATE.CERT_SEED + 1, GATE.DEFAULT_CI_HISTORIES, GATE.DEFAULT_CI_OPS)
        with self.assertRaises(GATE.GateError):
            GATE.validate_config(GATE.CERT_SEED, 0, GATE.DEFAULT_CI_OPS)
        with self.assertRaises(GATE.GateError):
            GATE.validate_config(GATE.CERT_SEED, GATE.DEFAULT_CI_HISTORIES, 0)

    def test_report_emission_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            payload = {
                "contract": GATE.CONTRACT,
                "issue": 756,
                "seed": GATE.CERT_SEED,
                "history_count": 1,
                "ops_per_history": 1,
                "untriaged_failures": 0,
                "commands": ["cargo test -p graphforge-storage project_certification --lib"],
                "claims": {"ssi": False},
            }
            GATE.write_evidence(output, payload)
            report = json.loads((output / "durability-certification-report.json").read_text())
            self.assertEqual(report["contract"], GATE.CONTRACT)
            self.assertTrue((output / "durability-certification-report.sha256").is_file())
            self.assertTrue((output / "reproduction.txt").is_file())


if __name__ == "__main__":
    unittest.main()
