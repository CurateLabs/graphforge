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

    def test_public_certification_evidence_schema_accepts_sanitized_outcome(self) -> None:
        schema = json.loads(
            (workspace_root() / "schemas" / "certification-evidence.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator(schema).validate(
            {
                "schema": "graphforge-public-certification/1",
                "profile_id": "tiny-public-certification",
                "status": "passed",
                "phases": [
                    {
                        "phase": "admission",
                        "status": "passed",
                        "duration_ms": 1,
                        "peak_rss_bytes": 1024,
                        "exit_code": 0,
                    }
                ],
                "failed_phase": None,
            }
        )

    def test_lifecycle_storage_receipt_schema_requires_closed_numeric_contract(self) -> None:
        schema = json.loads(
            (workspace_root() / "schemas" / "certification-evidence.json").read_text(
                encoding="utf-8"
            )
        )
        validator = Draft202012Validator(schema)
        receipt = {
            "contract": "graphforge-lifecycle-storage/1",
            "source_project_current_allocated_bytes": 1024,
            "retained_storage_bytes": 2048,
            "transient_peak_storage_bytes": 4096,
        }

        def evidence(candidate: dict[str, object]) -> dict[str, object]:
            return {
                "schema": "graphforge-public-certification/1",
                "profile_id": "tiny-public-certification",
                "status": "passed",
                "phases": [
                    {
                        "phase": "reopen_proof",
                        "status": "passed",
                        "duration_ms": 1,
                        "peak_rss_bytes": 1024,
                        "exit_code": 0,
                        "receipts": [candidate],
                    }
                ],
                "failed_phase": None,
            }

        validator.validate(evidence(receipt))
        for missing in (
            "source_project_current_allocated_bytes",
            "retained_storage_bytes",
            "transient_peak_storage_bytes",
        ):
            with self.subTest(missing=missing):
                invalid = dict(receipt)
                del invalid[missing]
                self.assertFalse(validator.is_valid(evidence(invalid)))
        for malformed in (True, -1, "1024"):
            with self.subTest(malformed=malformed):
                invalid = dict(receipt)
                invalid["source_project_current_allocated_bytes"] = malformed
                self.assertFalse(validator.is_valid(evidence(invalid)))

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
