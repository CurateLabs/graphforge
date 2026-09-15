from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from graphforge_bench.lifecycle_runtime import analyze, summarize
from graphforge_bench.native_rung import read_native_rung

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT.parent / "docs/development/evidence/integrated-storage-1194"


class RuntimeDiagnosisTests(unittest.TestCase):
    def test_preserved_receipts_reproduce_warning_and_only_s24_admission(self) -> None:
        report = analyze(ROOT, EVIDENCE)
        for scale, seconds in ((24, 6975), (25, 14031), (26, 28143)):
            projection = report["diagnostic_extrapolations"][f"S{scale}"]
            self.assertEqual(projection["wall_seconds"], seconds)
            self.assertNotIn("decision", projection)
        admission = report["historical_s24_admission"]
        self.assertEqual(admission["decision"], "refused")
        self.assertEqual(
            [name for name, passed in admission["checks"].items() if not passed],
            ["rss_bounded_or_plateaued"],
        )
        s22 = report["rungs"]["S22"]
        self.assertEqual(s22["construction_calls"]["seal"]["calls"], 1)
        self.assertEqual(s22["construction_calls"]["append"]["calls"], 1088)
        self.assertGreater(s22["ingest_outside_construction_calls_ns"], 0)
        self.assertNotEqual(
            s22["process_peak_rss_bytes"], s22["whole_lifecycle_benchexec"]["peak_rss_bytes"]
        )
        saved = json.loads((EVIDENCE.parent / "lifecycle-runtime-1279-baseline.json").read_text())
        self.assertEqual(report, saved)

    def test_individually_valid_but_different_sources_are_not_comparable(self) -> None:
        low = read_native_rung(ROOT, EVIDENCE, 20)
        high = read_native_rung(ROOT, EVIDENCE, 22)
        for identity in ("commit", "gf_sha256", "host_profile_sha256", "generator"):
            changed = copy.deepcopy(high)
            changed["result"]["identities"][identity] = "different"
            with (
                self.subTest(identity=identity),
                patch(
                    "graphforge_bench.lifecycle_runtime.read_native_rung",
                    side_effect=[low, changed],
                ),
                self.assertRaisesRegex(ValueError, "different"),
            ):
                analyze(ROOT, EVIDENCE)

    def test_missing_call_observations_are_not_reconstructed(self) -> None:
        document = read_native_rung(ROOT, EVIDENCE, 20)
        for phase in document["graphforge"]["phases"]:
            for receipt in phase.get("receipts", []):
                receipt.pop("operation_timings", None)
        with self.assertRaisesRegex(ValueError, "requires operation timing"):
            summarize(document)
