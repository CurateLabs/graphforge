from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from graphforge_bench.ingestion_attribution import validate_boundary_families

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "diagnostics"))
SPEC = importlib.util.spec_from_file_location(
    "report_parquet_root_1286", ROOT / "diagnostics/report_parquet_root_1286.py"
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
EVIDENCE = ROOT.parent / "docs/development/evidence"


class ParquetRootTests(unittest.TestCase):
    def test_historical_and_retained_root_oracles_are_distinct(self):
        report = json.loads(
            (EVIDENCE / "ingestion-attribution-1282-boundary-diagnostic.json").read_text()
        )
        for command in report["commands"]:
            if "merge_families" not in command:
                continue
            n, e = command["boundary"]["nodes"], command["boundary"]["edges"]
            original = command["merge_families"]
            validate_boundary_families(n, e, original)
            candidate = copy.deepcopy(original)
            changed = False
            for kind, count in (("node-rows-", n), ("edge-rows-", e)):
                if count in (32, 1024):
                    changed = True
                    entry = next(v for k, v in candidate.items() if k.startswith(kind))
                    entry["rows_read"] -= count
                    entry["rows_written"] -= count
            validate_boundary_families(n, e, candidate, retain_completed_roots=True)
            if changed:
                with self.assertRaises(ValueError):
                    validate_boundary_families(n, e, original, retain_completed_roots=True)
                with self.assertRaises(ValueError):
                    validate_boundary_families(n, e, candidate)

    def test_comparison_requires_frozen_comparable_complete_evidence(self):
        baseline = json.loads((EVIDENCE / "ingestion-attribution-1282-scaling.json").read_text())
        for key in REPORT.COMPARABLE:
            baseline.setdefault(key, "fixture-only")
        end = max(c["started_unix"] + c["wall_seconds"] for c in baseline["commands"])
        with patch.object(REPORT.time, "time", return_value=end + 1):
            frozen = REPORT.freeze(baseline)
        candidate = copy.deepcopy(baseline)
        candidate["gf_sha256"] = "candidate"
        for command in candidate["commands"]:
            command["started_unix"] = end + 2
        candidate["baseline_envelopes"]["s17"]["ingestion_wall_seconds"]["median"] -= 1
        self.assertTrue(REPORT.compare(baseline, candidate, frozen)["s17_benefit_threshold_passed"])
        bad = copy.deepcopy(frozen)
        bad["thresholds"]["s17"]["minimum_saving_seconds"] = 0
        with self.assertRaisesRegex(ValueError, "thresholds differ"):
            REPORT.compare(baseline, candidate, bad)
        bad = {**frozen, "frozen_unix": end + 3}
        with self.assertRaisesRegex(ValueError, "not frozen"):
            REPORT.compare(baseline, candidate, bad)
        for key, value in (
            ("instrumented", True),
            ("selection", []),
            ("kernel_release", "changed"),
            ("gf_sha256", baseline["gf_sha256"]),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                REPORT.compare(baseline, {**candidate, key: value}, frozen)
        candidate["baseline_envelopes"]["s17"]["ingestion_wall_seconds"]["median"] += 1
        self.assertFalse(
            REPORT.compare(baseline, candidate, frozen)["s17_benefit_threshold_passed"]
        )
