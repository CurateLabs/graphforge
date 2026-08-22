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
        cases = {
            "positive_crud_import_export": {
                "module_ids": [],
                "bridge_id": "b",
                "module_export_match": True,
                "bridge_export_match": True,
            },
            "exact_identity_and_ambiguity": {
                "exact_match": True,
                "diagnostic_code": "resolution.ambiguous",
            },
            "dependency_blocked_deletion": {"safe": False, "diagnostic_code": "dependency.in_use"},
            "unsupported_future_portability": {
                "error_code": "GF_UNSUPPORTED",
                "diagnostic_code": "unsupported",
            },
            "cancellation": {
                "error_code": "GF_CANCELLED",
                "before_modules": [],
                "after_modules": [],
            },
            "idempotent_replay": {
                "first_receipt": {"id": "x"},
                "replay_receipt": {"id": "x"},
                "conflict_code": "GF_IDEMPOTENCY_CONFLICT",
            },
            "no_partial_import_or_authority_change": {
                "before_entries": [],
                "after_entries": [],
                "authority_before": {"generation": 1},
                "authority_after": {"generation": 1},
            },
            "bounded_structured_diagnostics": {
                "outer_code": "GF_ERROR",
                "diagnostic_code": "bounded",
                "bounded": True,
                "path_free": True,
            },
            "deterministic_path_free_cli_json": {
                "first_serialized": "[]",
                "second_serialized": "[]",
                "forbidden_path": "/private/runtime",
            },
            "packaged_clean_install": {
                "package_origin": "/installed/package",
                "operation": "ontology_modules",
                "module_count": 0,
            },
        }
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
        node["cases"]["positive_crud_import_export"]["module_export_match"] = False
        self.paths["node"].write_text(json.dumps(node))
        errors = COMPARATOR.compare(self.paths, self.ledger)
        self.assertTrue(any("differ" in error for error in errors))

    def test_literal_markers_and_missing_case_fail(self) -> None:
        python = json.loads(self.paths["python"].read_text())
        del python["cases"]["cancellation"]
        self.paths["python"].write_text(json.dumps(python))
        cli = json.loads(self.paths["cli"].read_text())
        cli["cases"]["packaged_clean_install"] = {"semantic_smoke": True}
        self.paths["cli"].write_text(json.dumps(cli))
        errors = COMPARATOR.compare(self.paths, self.ledger)
        self.assertTrue(any("case set" in error for error in errors))
        self.assertTrue(any("exact observed fields" in error for error in errors))

    def test_distinct_replay_and_determinism_observations_are_required(self) -> None:
        node = json.loads(self.paths["node"].read_text())
        node["cases"]["idempotent_replay"]["replay_receipt"] = {"id": "different"}
        node["cases"]["deterministic_path_free_cli_json"]["second_serialized"] = "[different]"
        self.paths["node"].write_text(json.dumps(node))
        errors = COMPARATOR.compare(self.paths, self.ledger)
        self.assertTrue(any("equal observed receipts" in error for error in errors))
        self.assertTrue(any("two equal" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
