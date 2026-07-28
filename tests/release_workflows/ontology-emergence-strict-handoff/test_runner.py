"""Dependency-free structural tests for the #2469 bundle."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

BUNDLE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("ontology_handoff_runner", BUNDLE / "run.py")
assert SPEC and SPEC.loader
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class BundleContractTests(unittest.TestCase):
    def test_bundle_is_complete(self) -> None:
        scenario = RUNNER.validate_bundle(BUNDLE)
        self.assertEqual(scenario["owning_issue"], 2469)
        self.assertEqual(
            scenario["load_path_classification"]["rust_bulk"],
            "supported-publish_bulk_nodes-publish_bulk_edges",
        )

    def test_no_mode_shortcut_or_internal_migration_is_claimed(self) -> None:
        manifest = RUNNER.load_object(BUNDLE / "manifests/state-projects.json")
        states = manifest["states"]
        self.assertEqual(states[1]["authority"].split(";")[0], "live session only")
        self.assertEqual(states[2]["project_directory"], "target")
        self.assertIn("pre-v1 migration", manifest["forbidden_shortcuts"])

    def test_expected_errors_are_prepublication_contracts(self) -> None:
        errors = RUNNER.load_object(BUNDLE / "expected/errors.json")
        self.assertTrue(
            all(not contract["partial_mutation"] for contract in errors["required"].values())
        )


if __name__ == "__main__":
    unittest.main()
