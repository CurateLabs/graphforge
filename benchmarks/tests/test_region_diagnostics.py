from __future__ import annotations

import json
from pathlib import Path
import unittest

from graphforge_bench.region_diagnostics import (
    compare_region_receipts,
    matched_worker_speedup,
    summarize_regions,
)
from jsonschema import Draft202012Validator


class RegionDiagnosticsTest(unittest.TestCase):
    def test_both_receipt_schemas_accept_only_unsigned_known_successful_byte_units(self) -> None:
        schema = json.loads(
            (Path(__file__).parents[1] / "schemas" / "certification-evidence.json").read_text()
        )
        for contract in ("regionDiagnosticsV1", "regionDiagnosticsV2"):
            work_schema = schema["$defs"][contract]["properties"]["regions"][
                "additionalProperties"
            ]["properties"]["work"]
            validator = Draft202012Validator(work_schema)
            validator.validate({"hashed_bytes": 12, "written_bytes": 6})
            validator.validate({})
            for unit in ("hashed_bytes", "written_bytes"):
                for invalid in (-1, 1.5, True, None, "12"):
                    self.assertFalse(validator.is_valid({unit: invalid}), (contract, unit, invalid))
            self.assertFalse(validator.is_valid({"attempted_hashed_bytes": 12}), contract)

    def test_inclusive_children_are_not_added_to_command_totals(self) -> None:
        row = {
            "calls": 1,
            "work": {"rows": 100},
            "inclusive": {
                "wall_ns": 100,
                "process_cpu_ns": 80,
                "thread_running_ns": 60,
            },
            "residual": {"wall_ns": 40},
        }
        report = {
            "complete": True,
            "regions": {"import_command": row, "import_command/validate": row},
        }
        receipts = [
            {"region_diagnostics": report},
            {
                "contract": "graphforge-workflow-timing/1",
                "wall_ns": 150,
                "runner_cpu_ns": 10,
                "children_cpu_ns": 90,
            },
        ]
        result = summarize_regions(receipts)["region_attribution"]
        self.assertEqual(result["command_wall_ns"], 100)
        self.assertEqual(result["complete_ingest"]["outside_command_wall_ns"], 50)
        self.assertEqual(result["complete_ingest"]["outside_command_cpu_ns"], 20)
        self.assertAlmostEqual(result["complete_ingest"]["effective_cores"], 100 / 150)
        self.assertIsNone(result["stages_inclusive_do_not_sum"][0]["matched_worker_speedup"])

    def test_successful_byte_columns_stay_distinct_from_attempted_measurements(self) -> None:
        child = {
            "calls": 1,
            "work": {"hashed_bytes": 12, "written_bytes": 6},
            "inclusive": {
                "wall_ns": 10,
                "process_cpu_ns": 8,
                "thread_running_ns": 6,
                "hashed_bytes": 25,
                "written_bytes": 30,
            },
            "residual": {"wall_ns": 10},
        }
        parent = {**child, "work": {"rows": 1}}
        report = {
            "complete": True,
            "regions": {
                "import_command": parent,
                "import_command/stage+seal": child,
            },
        }
        rows = summarize_regions([{"region_diagnostics": report}])["region_attribution"][
            "stages_inclusive_do_not_sum"
        ]
        self.assertIsNone(rows[0]["successful_hashed_bytes"])
        self.assertIsNone(rows[0]["successful_written_bytes"])
        self.assertEqual(rows[1]["successful_hashed_bytes"], 12)
        self.assertEqual(rows[1]["successful_written_bytes"], 6)
        self.assertEqual(rows[1]["inclusive"]["hashed_bytes"], 25)
        self.assertEqual(rows[1]["inclusive"]["written_bytes"], 30)
        self.assertEqual(rows[1]["useful_work_per_second"]["hashed_bytes"], 1_200_000_000)

    def test_speedup_requires_matched_work_and_single_process(self) -> None:
        baseline = dict.fromkeys(
            ("build", "input", "host", "cache", "resource_policy", "scope", "unit"), "same"
        )
        baseline.update(work=100, workers=1, processes=1, wall_ns=100)
        candidate = {**baseline, "workers": 4, "wall_ns": 40}
        self.assertEqual(matched_worker_speedup(baseline, candidate), 2.5)
        for identity in ("build", "input", "host", "cache", "resource_policy", "scope", "unit"):
            for unknown in (None, "", " "):
                with self.assertRaises(ValueError):
                    matched_worker_speedup(
                        {**baseline, identity: unknown}, {**candidate, identity: unknown}
                    )
        for changes in ({"processes": 4}, {"work": 200}, {"cache": "different"}, {"wall_ns": 0}):
            with self.assertRaises(ValueError):
                matched_worker_speedup(baseline, {**candidate, **changes})

    def test_historical_receipts_are_unchanged(self) -> None:
        self.assertEqual(summarize_regions([]), {})

    def test_comparison_reads_work_and_time_from_the_selected_receipt_stage(self) -> None:
        provenance = dict.fromkeys(("build", "input", "host", "cache", "resource_policy"), "same")

        def observation(workers: int, wall: int) -> dict:
            return {
                "provenance": {**provenance, "workers": workers, "processes": 1},
                "receipt": {
                    "region_diagnostics": {
                        "complete": True,
                        "regions": {
                            "shape": {
                                "work": {"edges": 100},
                                "inclusive": {"wall_ns": wall, "process_cpu_ns": 100},
                            }
                        },
                    }
                },
            }

        result = compare_region_receipts(observation(1, 100), observation(4, 40), "shape", "edges")
        self.assertEqual(result["matched_worker_speedup"], 2.5)
        self.assertEqual(result["process_effective_cores"], [1.0, 2.5])
        with self.assertRaises(ValueError):
            compare_region_receipts(observation(1, 100), observation(1, 40), "shape", "edges")
