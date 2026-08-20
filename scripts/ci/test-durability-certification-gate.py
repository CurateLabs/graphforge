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
        self.assertEqual(
            contract["production_observation"]["driver"],
            "graphforge_api::GraphForge",
        )
        self.assertEqual(contract["versions"]["m6_benchmark_inventory"], "m6-storage-v1")

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

    def test_native_aggregate_is_exact_sha_and_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = {
                "contract": "graphforge-native-durability-oracle/v1",
                "filesystem_class": "native-test",
                "profile": "ordered",
                "seed": 7490,
                "observations": [{"phase": "before_ack"}],
                "modeled_faults": [{"safe": True}],
                "minimized_failure": {"minimized_op_ids": [1]},
            }
            paths = {}
            for platform in ("windows", "macos"):
                path = root / f"native-oracle-{platform}.json"
                path.write_text(json.dumps({**template, "platform": platform}))
                paths[platform] = path
            output = root / "aggregate.json"
            args = type(
                "Args",
                (),
                {
                    "expected_sha": "a" * 40,
                    "windows": str(paths["windows"]),
                    "macos": str(paths["macos"]),
                    "output": str(output),
                },
            )()
            self.assertEqual(GATE.cmd_aggregate_native(args), 0)
            aggregate = json.loads(output.read_text())
            self.assertEqual(aggregate["commit"], "a" * 40)
            self.assertEqual(
                [row["platform"] for row in aggregate["platforms"]],
                ["windows", "macos"],
            )
            bad = json.loads(paths["windows"].read_text())
            bad["observations"] = []
            paths["windows"].write_text(json.dumps(bad))
            with self.assertRaises(GATE.GateError):
                GATE.cmd_aggregate_native(args)


if __name__ == "__main__":
    unittest.main()
