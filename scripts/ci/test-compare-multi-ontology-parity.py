#!/usr/bin/env python3
"""Mutation tests for the real four-surface semantic report comparator."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("compare-multi-ontology-parity.py")
SPEC = importlib.util.spec_from_file_location("compare_multi_ontology_parity", SCRIPT)
assert SPEC and SPEC.loader
COMPARATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPARATOR)


class ComparatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        cases = {"case_a": {"ok": True}, "case_b": {"code": "stable.code"}}
        self.ledger = self.root / "ledger.json"
        self.ledger.write_text(json.dumps({"case_evidence": {key: {} for key in cases}}))
        self.paths: dict[str, Path] = {}
        for surface in COMPARATOR.SURFACES:
            path = self.root / f"{surface}.json"
            path.write_text(json.dumps({"contract": COMPARATOR.CONTRACT, "cases": cases}))
            self.paths[surface] = path

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_equal_complete_reports_pass(self) -> None:
        self.assertEqual(COMPARATOR.compare(self.paths, self.ledger), [])

    def test_semantic_divergence_fails(self) -> None:
        node = json.loads(self.paths["node"].read_text())
        node["cases"]["case_a"] = {"ok": False}
        self.paths["node"].write_text(json.dumps(node))
        errors = COMPARATOR.compare(self.paths, self.ledger)
        self.assertTrue(any("differ" in error for error in errors))

    def test_missing_case_and_runtime_identity_fail(self) -> None:
        python = json.loads(self.paths["python"].read_text())
        del python["cases"]["case_b"]
        self.paths["python"].write_text(json.dumps(python))
        cli = json.loads(self.paths["cli"].read_text())
        cli["cases"]["case_a"] = {"id": "00000000-0000-0000-0000-000000000842"}
        self.paths["cli"].write_text(json.dumps(cli))
        errors = COMPARATOR.compare(self.paths, self.ledger)
        self.assertTrue(any("case set" in error for error in errors))
        self.assertTrue(any("runtime-specific" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
