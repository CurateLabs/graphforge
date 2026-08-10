"""Lightweight contract tests for the atomic-recovery bundle."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest

BUNDLE = Path(__file__).resolve().parent

RUNNER_SPEC = importlib.util.spec_from_file_location("atomic_recovery_runner", BUNDLE / "run.py")
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(RUNNER)


def load(name: str) -> dict[str, object]:
    return json.loads((BUNDLE / name).read_text())


class BundleContract(unittest.TestCase):
    def test_same_sha_git_probes_use_the_bounded_runner_timeout(self) -> None:
        tree = ast.parse((BUNDLE / "run.py").read_text())
        probes = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "check_output"
        ]
        self.assertEqual(2, len(probes))
        for probe in probes:
            timeout = next(keyword.value for keyword in probe.keywords if keyword.arg == "timeout")
            self.assertIsInstance(timeout, ast.Name)
            self.assertEqual("TIMEOUT", timeout.id)

    def test_runner_prepares_bounded_persistent_project_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            project = RUNNER.prepare_project_directory(root)
            self.assertEqual(root / "project", project)
            self.assertTrue(project.is_dir())

    def test_bundle_is_complete_and_step_mapping_is_exact(self) -> None:
        required = [
            "workflow.feature",
            "scenario.yaml",
            "generator.yaml",
            "README.md",
            "binding_workflow.py",
            "run.py",
            "ontologies/strict-v1.yaml",
            "ontologies/phase-manifest.json",
            "expected/evidence-schema.json",
            "expected/arrow-fingerprints.json",
            "expected/errors.json",
            "expected/phases/seed.json",
            "expected/phases/graph-knowledge.json",
            "expected/phases/graph-knowledge-epistemic.json",
            "expected/phases/rejected.json",
            "expected/phases/recovered-previous.json",
            "expected/phases/recovered-new.json",
            "expected/phases/final-view.json",
        ]
        self.assertEqual([], [name for name in required if not (BUNDLE / name).is_file()])
        scenario = load("scenario.yaml")
        generator = load("generator.yaml")
        self.assertEqual(2473, scenario["owning_issue"])
        self.assertEqual(generator["seed"], scenario["generator"]["seed"])
        actual = "sha256:" + hashlib.sha256((BUNDLE / "generator.yaml").read_bytes()).hexdigest()
        self.assertEqual(actual, scenario["generator"]["fixture_fingerprint"])
        self.assertEqual("018f0f4e-7b8c-7000-8000-000000050000", generator["fixed_uuid_namespace"])
        workflow = (BUNDLE / "workflow.feature").read_text()
        feature = re.findall(
            r"^\s*(?:Given|When|Then|And|But)\s+\[(AR-\d{2})\]", workflow, flags=re.MULTILINE
        )
        workflow_mapping = re.findall(r"# (AR-\d{2}) implementation=([^\s]+)", workflow)
        manifest_mapping = [(step["id"], step["implementation"]) for step in scenario["steps"]]
        manifest = [step_id for step_id, _ in manifest_mapping]
        self.assertEqual([f"AR-{index:02d}" for index in range(1, 13)], feature)
        self.assertEqual(feature, manifest)
        self.assertEqual(len(feature), len(set(feature)))
        self.assertEqual(manifest_mapping, workflow_mapping)

        evidence_schema = load("expected/evidence-schema.json")
        self.assertEqual(1, evidence_schema["schema_version"])
        self.assertEqual(
            {
                "schema_version": 1,
                "scenario_id": "atomic-recovery",
                "graph_knowledge_committed": True,
                "graph_knowledge_epistemic_committed": True,
                "orphan_free": True,
                "reopen_equal": True,
            },
            evidence_schema["fixed"],
        )

        fingerprints = load("expected/arrow-fingerprints.json")
        self.assertEqual("canonical-json-from-arrow-values/1", fingerprints["contract"])
        self.assertIn("current_findings", fingerprints["schemas"])
        self.assertIn("composite_receipt", fingerprints["schemas"])
        self.assertIn("epistemic_snapshot", fingerprints["schemas"])

        errors = load("expected/errors.json")["structured_failures"]
        self.assertEqual(
            [("AR-04", "GF_ONTOLOGY"), ("AR-09", "GF_IDEMPOTENCY_CONFLICT")],
            [(error["step_id"], error["code"]) for error in errors],
        )
        self.assertTrue(all(error["publication"] == "none" for error in errors))

        seed = load("expected/phases/seed.json")
        final = load("expected/phases/final-view.json")
        self.assertEqual((8, 10, "strict"), (seed["nodes"], seed["edges"], seed["ontology_mode"]))
        self.assertEqual(
            (1, True, "identical"),
            (final["complete_generations"], final["idempotent_retry"], final["reopen"]),
        )

    def test_contract_requires_failpoint_process_control(self) -> None:
        generator = load("generator.yaml")
        self.assertGreaterEqual(len(generator["failpoints"]["pre_current"]), 3)
        self.assertGreaterEqual(len(generator["failpoints"]["post_current"]), 2)
        scenario = load("scenario.yaml")
        self.assertEqual("authoritative-executable", scenario["bindings"]["rust"])
        self.assertEqual("representative-executable", scenario["bindings"]["python"])
        self.assertIn("publish_composite_transaction", scenario["public_surfaces"])


if __name__ == "__main__":
    unittest.main()
