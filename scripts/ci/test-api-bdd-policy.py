#!/usr/bin/env python3
"""Mutation tests for the public API BDD repository policy."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "api_bdd_policy", ROOT / "scripts/ci/api-bdd-policy.py"
)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = POLICY
SPEC.loader.exec_module(POLICY)


class ApiBddPolicyMutationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for relative in (
            "tests/features/api",
            "tests/contracts/api-bdd-exclusions.json",
            "tests/features/conftest.py",
            "tests/features/steps/api_steps.py",
            "tests/features/node/step_definitions/api_steps.ts",
            "crates/graphforge-api/tests/bdd/api_steps.rs",
        ):
            source = ROOT / relative
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                shutil.copytree(source, destination)
            else:
                shutil.copy2(source, destination)
        _, errors = POLICY.validate(self.root)
        self.assertEqual(errors, [])

    def tearDown(self) -> None:
        self.temp.cleanup()

    def assert_rejected(self, expected: str) -> None:
        _, errors = POLICY.validate(self.root)
        self.assertTrue(any(expected in error for error in errors), errors)

    def test_inventory_entry_cannot_be_removed(self) -> None:
        path = self.root / "tests/contracts/api-bdd-exclusions.json"
        inventory = json.loads(path.read_text())
        inventory["exclusions"].pop()
        path.write_text(json.dumps(inventory))
        self.assert_rejected("missing from inventory")

    def test_inventory_must_be_an_object(self) -> None:
        path = self.root / "tests/contracts/api-bdd-exclusions.json"
        path.write_text("[]")
        self.assert_rejected("inventory must be an object")

    def test_inventory_entries_must_be_objects(self) -> None:
        path = self.root / "tests/contracts/api-bdd-exclusions.json"
        inventory = json.loads(path.read_text())
        inventory["exclusions"].append("not-an-object")
        path.write_text(json.dumps(inventory))
        self.assert_rejected("must be an object")

    def test_null_inventory_languages_are_rejected_cleanly(self) -> None:
        path = self.root / "tests/contracts/api-bdd-exclusions.json"
        inventory = json.loads(path.read_text())
        inventory["exclusions"][0]["languages"] = None
        path.write_text(json.dumps(inventory))
        self.assert_rejected("invalid languages []")

    def test_issue_tag_must_match_inventory(self) -> None:
        path = self.root / "tests/features/api/errors.feature"
        path.write_text(path.read_text().replace("@issue-353", "@issue-999", 1))
        self.assert_rejected("missing @issue-353")

    def test_stale_skip_tag_is_rejected(self) -> None:
        path = self.root / "tests/features/api/analyze.feature"
        path.write_text(path.read_text().replace("@api", "@api @skip-node", 1))
        self.assert_rejected("stale language skip tag")

    def test_untracked_binding_only_tag_is_rejected(self) -> None:
        path = self.root / "tests/features/api/analyze.feature"
        path.write_text(path.read_text().replace("@api", "@api @binding-only", 1))
        self.assert_rejected("unapproved binding-only classification")

    def test_python_not_implemented_conversion_is_rejected(self) -> None:
        path = self.root / "tests/features/conftest.py"
        path.write_text(path.read_text() + "\npytest.xfail('NotImplementedError')\n")
        self.assert_rejected("forbidden fail-open pattern")

    def test_node_pending_result_is_rejected(self) -> None:
        path = self.root / "tests/features/node/step_definitions/api_steps.ts"
        path.write_text(path.read_text() + '\nreturn "pending";\n')
        self.assert_rejected("forbidden fail-open pattern")

    def test_rust_manufactured_error_is_rejected(self) -> None:
        path = self.root / "crates/graphforge-api/tests/bdd/api_steps.rs"
        path.write_text(path.read_text() + '\nworld.last_error = Some("not implemented".into());\n')
        self.assert_rejected("forbidden fail-open pattern")

    def test_required_step_source_cannot_be_removed(self) -> None:
        path = self.root / "tests/features/steps/api_steps.py"
        path.unlink()
        self.assert_rejected("required step source is missing")


if __name__ == "__main__":
    unittest.main()
