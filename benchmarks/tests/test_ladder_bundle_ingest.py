from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest

from graphforge_bench.ladder_bundle_ingest import (
    LadderBundleIngestError,
    ingest_ladder_bundle,
    validate_ladder_bundle,
)
from graphforge_bench.parity_gate import parity_gate_status
from graphforge_bench.scale_parity import workspace_root

COMMIT = "a" * 40
DIGEST = "b" * 64


def manifest() -> dict[str, object]:
    return {
        "commit": COMMIT,
        "image_digest": f"registry.fly.io/gf-progressive-test@sha256:{'c' * 64}",
        "generator_identity": "graphforge-benchmark-graph500-generator",
        "benchexec_version": "2.7.1",
        "maximum_authorized_scale": 26,
    }


def teardown_inventory() -> dict[str, object]:
    return {
        "schema": "graphforge-progressive-provider-teardown-inventory/1",
        "status": "not_required",
        "failure": None,
        "commit": COMMIT,
        "authorized_maximum_scale": 26,
        "completed_scales": [18, 19],
        "authorization_sha256": DIGEST,
        "admitted_plan_sha256": DIGEST,
        "checked_at": None,
        "observed": None,
        "claim": "control_plane_evidence_only",
    }


class LadderBundleIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        rung = json.loads(
            (workspace_root() / "fixtures" / "parity" / "rung" / "s18-pass.json").read_text(
                encoding="utf-8"
            )
        )
        (self.source / "manifest.json").write_text(
            json.dumps(manifest(), indent=2) + "\n", encoding="utf-8"
        )
        (self.source / "teardown-inventory.json").write_text(
            json.dumps(teardown_inventory(), indent=2) + "\n", encoding="utf-8"
        )
        (self.source / "s18-rung.json").write_text(
            json.dumps(rung, indent=2) + "\n", encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_validate_accepts_complete_bundle(self) -> None:
        report = validate_ladder_bundle(self.source)
        self.assertEqual(report["manifest_commit"], COMMIT)
        self.assertEqual(report["rung_files"], ["s18-rung.json"])

    def test_historical_controller_result_names_do_not_select_native_schema(self) -> None:
        for schema in (
            "graphforge-progressive-run-result/1",
            "graphforge-progressive-provider-run-result/1",
        ):
            with self.subTest(schema=schema):
                (self.source / "s18-result.json").write_text(json.dumps({"schema": schema}))
                self.assertEqual(
                    validate_ladder_bundle(self.source)["rung_files"], ["s18-rung.json"]
                )

    def test_validate_refuses_missing_manifest(self) -> None:
        (self.source / "manifest.json").unlink()
        with self.assertRaises(LadderBundleIngestError):
            validate_ladder_bundle(self.source)

    def test_ingest_copies_bundle_and_runs_parity(self) -> None:
        destination = self.root / "dest"
        report = ingest_ladder_bundle(self.source, destination)
        self.assertTrue((destination / "s18-rung.json").is_file())
        self.assertEqual(report["parity_comparisons"], 1)


class ParityGateHarnessAuthorityTests(unittest.TestCase):
    def test_harness_authority_met_after_ingested_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            base = Path(temp_name)
            fixtures = base / "fixtures" / "parity"
            shutil.copytree(workspace_root() / "fixtures" / "parity", fixtures)
            source = base / "bundle-source"
            source.mkdir()
            rung = json.loads(
                (workspace_root() / "fixtures" / "parity" / "rung" / "s18-pass.json").read_text(
                    encoding="utf-8"
                )
            )
            bundle = fixtures / "ladder-bundle"
            (bundle / "manifest.json").write_text(json.dumps(manifest()) + "\n", encoding="utf-8")
            (bundle / "teardown-inventory.json").write_text(
                json.dumps(teardown_inventory()) + "\n", encoding="utf-8"
            )
            (bundle / "s18-rung.json").write_text(json.dumps(rung) + "\n", encoding="utf-8")

            status = parity_gate_status(base)
            harness = next(
                row
                for row in status["criteria"]
                if row["name"] == "harness_authoritative_after_ladder_comparison"
            )
            self.assertFalse(harness["met"])
            self.assertEqual(harness["blocked_by"], "#900")
            self.assertTrue(status["prefix_parity_ready"])
            self.assertFalse(status["full_ladder_evidence_complete"])
            self.assertNotIn("ready_for_retirement", status)


if __name__ == "__main__":
    unittest.main()
