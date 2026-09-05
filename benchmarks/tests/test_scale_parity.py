from __future__ import annotations

import json
import unittest

from graphforge_bench.scale_parity import (
    Outcome,
    ParityError,
    assert_no_unexplained_gaps,
    compare_evidence,
    compare_fixture_pair,
    coverage_map,
    load_accepted_differences,
    normalize_legacy_cert_lifecycle,
    normalize_legacy_evidence,
    normalize_new_evidence,
    normalize_rung_evidence,
    validate_historical_legacy_cert,
    workspace_root,
)
from jsonschema import Draft202012Validator

ROOT = workspace_root()
FIXTURES = ROOT / "fixtures" / "parity"


class ScaleParityTests(unittest.TestCase):
    def test_accepted_differences_fixture_loads(self) -> None:
        document = load_accepted_differences()
        self.assertEqual(
            document["schema"], "graphforge-scale-orchestration-accepted-differences/1"
        )
        self.assertGreaterEqual(len(document["differences"]), 3)
        self.assertEqual(len(document["phase_mapping"]), 10)

    def test_tiny_shadow_fixtures_have_no_unexplained_gaps(self) -> None:
        matrix = compare_fixture_pair(
            FIXTURES / "legacy" / "tiny-pass.json",
            FIXTURES / "new" / "tiny-pass.json",
        )
        self.assertIn(matrix["overall"], {Outcome.MATCH.value, Outcome.ACCEPTED_DIFFERENCE.value})
        assert_no_unexplained_gaps(matrix)

    def test_matrix_schema_is_valid(self) -> None:
        matrix = compare_fixture_pair(
            FIXTURES / "legacy" / "tiny-pass.json",
            FIXTURES / "new" / "tiny-pass.json",
        )
        schema = json.loads(
            (ROOT / "schemas" / "scale-orchestration-parity-matrix.json").read_text()
        )
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(matrix)

    def test_status_mismatch_is_unexplained_gap(self) -> None:
        legacy = normalize_legacy_evidence(
            {
                "profile": "legacy",
                "phases": [{"name": "admission", "ok": False, "duration_secs": 0.1}],
            }
        )
        new = normalize_new_evidence(
            {
                "schema": "graphforge-public-certification/1",
                "profile_id": "new",
                "status": "passed",
                "failed_phase": None,
                "phases": [
                    {
                        "phase": "admission",
                        "status": "passed",
                        "duration_ms": 100,
                        "peak_rss_bytes": 1024,
                        "exit_code": 0,
                        "receipts": [],
                    }
                ],
            }
        )
        matrix = compare_evidence(legacy, new)
        self.assertEqual(matrix["overall"], Outcome.UNEXPLAINED_GAP.value)
        with self.assertRaisesRegex(Exception, "unexplained parity gaps"):
            assert_no_unexplained_gaps(matrix)

    def test_legacy_only_phase_can_be_accepted_difference(self) -> None:
        legacy = normalize_legacy_evidence(
            {
                "profile": "legacy",
                "phases": [
                    {"name": "admission", "ok": True, "duration_secs": 0.1, "max_rss_kib": 64},
                    {"name": "negative_drill", "ok": True, "duration_secs": 0.1, "max_rss_kib": 64},
                ],
            }
        )
        new = normalize_new_evidence(json.loads((FIXTURES / "new" / "tiny-pass.json").read_text()))
        matrix = compare_evidence(legacy, new)
        drill_rows = [
            row for row in matrix["phase_rows"] if row["legacy_phase"] == "negative_drill"
        ]
        self.assertEqual(len(drill_rows), 1)
        self.assertEqual(drill_rows[0]["outcome"], Outcome.ACCEPTED_DIFFERENCE.value)

    def test_rung_and_legacy_cert_lifecycle_align(self) -> None:
        rung = normalize_rung_evidence(
            json.loads((FIXTURES / "rung" / "s18-pass.json").read_text(encoding="utf-8"))
        )
        legacy = normalize_legacy_cert_lifecycle(
            json.loads((FIXTURES / "legacy" / "cert-s20-minimal.json").read_text(encoding="utf-8"))
        )
        matrix = compare_evidence(legacy, rung)
        self.assertIn(matrix["overall"], {Outcome.MATCH.value, Outcome.ACCEPTED_DIFFERENCE.value})
        assert_no_unexplained_gaps(matrix)

    def test_historical_legacy_cert_fixture_fails_closed_without_provider_anchor(self) -> None:
        fixture = FIXTURES / "legacy" / "cert-s20-minimal.json"
        with self.assertRaisesRegex(ParityError, "external provider-result anchor"):
            validate_historical_legacy_cert(fixture, expected_sha="a" * 40)

    def test_coverage_map_lists_legacy_entrypoints(self) -> None:
        mapping = coverage_map()
        self.assertIn("make bench-g500-ladder (retired)", mapping)
        self.assertIn("scripts/ci/validate-g500-certification.py (historical)", mapping)


if __name__ == "__main__":
    unittest.main()
