"""Light contract validation for knowledge-evolution."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import unittest

BUNDLE = Path(__file__).resolve().parent
ROOT = BUNDLE.parents[2]


class Contract(unittest.TestCase):
    def test_bundle_and_steps_are_exact(self) -> None:
        required = [
            "workflow.feature",
            "scenario.yaml",
            "generator.yaml",
            "README.md",
            "run.py",
            "binding_workflow.py",
            "ontologies/strict-v1.yaml",
            "expected/errors.json",
            "expected/evidence-schema.json",
            "expected/arrow-fingerprints.json",
            "expected/phases/final-view.json",
        ]
        self.assertEqual([], [name for name in required if not (BUNDLE / name).is_file()])
        self.assertTrue(
            (ROOT / "crates/graphforge-api/examples/knowledge_evolution_workflow.rs").is_file()
        )
        scenario = json.loads((BUNDLE / "scenario.yaml").read_text())
        generator = json.loads((BUNDLE / "generator.yaml").read_text())
        self.assertEqual(2472, scenario["owning_issue"])
        self.assertEqual(generator["seed"], scenario["generator"]["seed"])
        digest = "sha256:" + hashlib.sha256((BUNDLE / "generator.yaml").read_bytes()).hexdigest()
        self.assertEqual(digest, scenario["generator"]["fixture_fingerprint"])
        feature = re.findall(r"\[(KE-\d{2})\]", (BUNDLE / "workflow.feature").read_text())
        manifest = [step["id"] for step in scenario["steps"]]
        self.assertEqual([f"KE-{n:02d}" for n in range(1, 13)], feature)
        self.assertEqual(feature, manifest)

    def test_neutrality_and_explicit_selection_are_frozen(self) -> None:
        scenario = json.loads((BUNDLE / "scenario.yaml").read_text())
        correction = scenario["correction"]
        self.assertFalse(correction["graph_mutation"])
        self.assertFalse(correction["autonomous_adjudication"])
        self.assertIn(
            "unresolved-select-change-clear", scenario["coverage_signature"]["state_transitions"]
        )
        self.assertEqual("authoritative-executable", scenario["bindings"]["rust"])


if __name__ == "__main__":
    unittest.main()
