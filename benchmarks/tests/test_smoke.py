from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.smoke import FIXTURE_DIRECTORIES, discover_fixtures, workspace_root
from jsonschema import Draft202012Validator


class WorkspaceSmokeTests(unittest.TestCase):
    def test_checked_in_fixtures_are_discoverable(self) -> None:
        discovered = discover_fixtures()
        self.assertEqual(set(discovered), set(FIXTURE_DIRECTORIES))
        self.assertTrue(all(discovered.values()))

    def test_smoke_evidence_schema_accepts_its_minimal_document(self) -> None:
        schema = json.loads(
            (workspace_root() / "schemas" / "smoke-evidence.json").read_text(encoding="utf-8")
        )
        Draft202012Validator(schema).validate(
            {
                "schema": "graphforge-benchmark-evidence-schema/1",
                "result": "passed",
            }
        )

    def test_product_manifests_do_not_reference_benchmark_dependencies(self) -> None:
        repository = workspace_root().parent
        manifests = sorted(repository.rglob("Cargo.toml")) + sorted(
            repository.rglob("pyproject.toml")
        )
        product_manifests = [
            manifest
            for manifest in manifests
            if workspace_root() not in manifest.parents
            and not any(part in {"target", ".venv"} for part in manifest.parts)
        ]
        self.assertTrue(product_manifests)
        for manifest in product_manifests:
            text = manifest.read_text(encoding="utf-8").lower()
            self.assertNotIn("reframe-hpc", text)
            self.assertNotIn("benchexec", text)

    def test_missing_fixture_category_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in FIXTURE_DIRECTORIES:
                (root / name).mkdir()
            with self.assertRaisesRegex(RuntimeError, "no fixtures"):
                discover_fixtures(root)


if __name__ == "__main__":
    unittest.main()
