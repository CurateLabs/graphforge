from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.ingestion_attribution import (
    analyze,
    cpu_summary,
    expected_commands,
    interval_union_ns,
    merge_summary,
    sync_summary,
)
from graphforge_bench.native_rung import read_native_rung

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT.parent / "docs/development/evidence/query-rss-1278-repair"
SPEC = importlib.util.spec_from_file_location(
    "report_ingestion_1282", ROOT / "diagnostics/report_ingestion_1282.py"
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class IngestionAttributionTests(unittest.TestCase):
    def test_cpu_samples_disclose_missing_callers_and_keep_leaves_disjoint(self):
        leaves = [
            "gf-ordinary 10/10 1.000: cpu-clock: ab sha2::compress (/private/gf-ordinary)",
            "worker 10/11 1.100: cpu-clock: cd [unknown] ([unknown])",
            "runuser 9/9 1.200: cpu-clock: ef wrapper (/private/runuser)",
        ]
        stacks = [
            "gf-ordinary 10/10 1.000: cpu-clock:",
            " ab sha2::compress (/private/gf-ordinary)",
            " cd graphforge_storage::shaping_merge::merge_fixed_group (/private/gf-ordinary)",
            "worker 10/11 1.100: cpu-clock:",
        ]
        result = cpu_summary(leaves, stacks)
        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["excluded_wrapper_samples"], 1)
        self.assertEqual(result["unknown_leaf_samples"], 1)
        self.assertEqual(result["samples_without_unwound_frames"], 1)
        self.assertEqual(result["samples_with_symbolized_application_caller"], 1)
        self.assertEqual(result["inclusive_anchor_samples"]["fixed_merge"], 1)
        self.assertEqual(sum(item["percent"] for item in result["leaf_samples"]), 100)
        self.assertNotIn("/private", json.dumps(result))
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            cpu_summary(leaves, stacks[:-1])
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            cpu_summary(leaves, [*stacks, "gf-ordinary 10/10 1.300: cpu-clock:"])
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            cpu_summary(leaves[2:], stacks)
        with self.assertRaisesRegex(ValueError, "duplicate CPU leaf"):
            cpu_summary(leaves + leaves[:1], stacks)

    def test_sync_latency_keeps_overlapping_threads_separate_from_wall(self):
        report = sync_summary(
            [
                "11 100.000000 fsync(3 <unfinished ...>",
                "22 100.001000 fdatasync(4) = 0 <0.002000>",
                "11 100.004000 <... fsync resumed>) = 0 <0.004000>",
                "11 100.010000 fsync(3) = -1 EIO (Input/output error) <0.001000>",
            ]
        )
        self.assertEqual(report["calls"], 3)
        self.assertEqual(report["failed_calls"], 1)
        self.assertEqual(report["summed_thread_latency_ns"], 7_000_000)
        self.assertEqual(report["elapsed_interval_union_ns"], 5_000_000)
        for lines in (
            ["11 100.000000 fsync(3 <unfinished ...>"],
            ["11 100.004000 <... fsync resumed>) = 0 <0.004000>"],
        ):
            with self.assertRaises(ValueError):
                sync_summary(lines)

    def test_measurement_report_rejects_missing_reordered_and_tampered_commands(self):
        profile_path = ROOT / "profiles/graph500/s18-local.json"
        profile = json.loads(profile_path.read_text())
        selection = [{"name": "s16", "repetition": 0, "nodes": 65536, "edges": 1048576}]
        labels = expected_commands(selection, profile, False)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "selection.json").write_text(
                json.dumps({"cases": selection, "commands": labels})
            )
            (directory / "source-manifest.json").write_text("{}")
            observations = []
            for label in labels:
                observation = {
                    "label": label,
                    "case": "s16",
                    "repetition": 0,
                    "phase": label.split("-")[2],
                    "exit_code": 0,
                    "failure": None,
                    "host_activity_before": [],
                    "wall_seconds": 1,
                    "user_seconds": 0.5,
                    "system_seconds": 0.1,
                    "wait4_input_bytes": 512,
                    "wait4_output_bytes": 512,
                    "sampled_process_peak_bytes": 1024,
                }
                for suffix, contents in (
                    ("stdout", ""),
                    ("stderr", ""),
                    ("proc.json", "[]"),
                    ("command.json", "[]"),
                ):
                    path = directory / f"{label}.{suffix}"
                    path.write_text(contents)
                    key = "command_sha256" if suffix == "command.json" else suffix + "_sha256"
                    observation[key] = REPORT.digest(path)
                observations.append(observation)
            summary = {
                "status": "passed",
                "suite": "scaling",
                "instrumented": False,
                "selection": selection,
                "completed_cases": [
                    {
                        "case": "s16-r0",
                        "nodes": 65536,
                        "edges": 1048576,
                        "independent_oracles": 8,
                        "full_lifecycle": True,
                    }
                ],
                "selection_sha256": REPORT.digest(directory / "selection.json"),
                "source_manifest_sha256": REPORT.digest(directory / "source-manifest.json"),
                "profile_sha256": REPORT.digest(profile_path),
                "rustc": "fixture",
                "observations": observations,
            }

            def check(value):
                (directory / "summary.json").write_text(json.dumps(value))
                return REPORT.summarize(directory)

            self.assertEqual(len(check(summary)["commands"]), 15)
            complete = summary["completed_cases"][0]
            invalid_completions = [[], [complete, complete]]
            for field in complete:
                invalid_completions.append([{k: v for k, v in complete.items() if k != field}])
            for field, value in (
                ("full_lifecycle", False),
                ("full_lifecycle", 1),
                ("independent_oracles", 0),
                ("nodes", -1),
                ("edges", -1),
            ):
                invalid_completions.append([{**complete, field: value}])
            for completed_cases in invalid_completions:
                with (
                    self.subTest(completed_cases=completed_cases),
                    self.assertRaisesRegex(ValueError, "completion evidence"),
                ):
                    check({**summary, "completed_cases": completed_cases})
            for changed in (observations[:-1], list(reversed(observations))):
                with self.assertRaisesRegex(ValueError, "planned commands"):
                    check({**summary, "observations": changed})
            with self.assertRaisesRegex(ValueError, "unfinished"):
                check({**summary, "status": "running"})
            with self.assertRaisesRegex(ValueError, "missing required merge"):
                check({**summary, "custom_counters_enabled": True})
            for suite in ("perf", "sync"):
                with self.assertRaisesRegex(ValueError, "missing required profile"):
                    check({**summary, "suite": suite})
            (directory / f"{labels[0]}.stdout").write_text("tampered")
            with self.assertRaisesRegex(ValueError, "raw artifact digest mismatch"):
                check(summary)

    def test_completed_integrated_prefix_has_exact_fraction_and_residual(self):
        report = analyze(ROOT, EVIDENCE)
        self.assertEqual(list(report["rungs"]), ["S18", "S19", "S20", "S22"])
        for rung in report["rungs"].values():
            ingestion_ns = rung["phase_wall_ms"]["ingest"] * 1_000_000
            self.assertEqual(
                ingestion_ns,
                rung["construction_call_wall_ns"] + rung["ingest_outside_construction_calls_ns"],
            )
            self.assertAlmostEqual(
                rung["ingestion_fraction"],
                ingestion_ns / 1e9 / rung["whole_lifecycle_benchexec"]["wall_seconds"],
            )
        self.assertEqual(report["rungs"]["S20"]["phase_wall_ms"]["ingest"], 197749)
        self.assertNotIn("admission", report)

    def test_individually_valid_mixed_prefix_is_rejected(self):
        docs = [read_native_rung(ROOT, EVIDENCE, scale) for scale in (18, 19, 20, 22)]
        for field in ("commit", "gf_sha256", "host_profile_sha256", "generator"):
            modified = copy.deepcopy(docs)
            modified[0]["result"]["identities"][field] = "different"
            with (
                patch(
                    "graphforge_bench.ingestion_attribution.read_native_rung", side_effect=modified
                ),
                self.assertRaisesRegex(ValueError, "different"),
            ):
                analyze(ROOT, EVIDENCE)

    def test_unfinished_receipt_is_not_treated_as_zero(self):
        with (
            patch(
                "graphforge_bench.ingestion_attribution.read_native_rung",
                side_effect=ValueError("unfinished S22"),
            ),
            self.assertRaises(ValueError),
        ):
            analyze(ROOT, EVIDENCE)

    def test_preselection_requires_every_lifecycle_command(self):
        profile = json.loads((ROOT / "profiles/graph500/s18-local.json").read_text())
        selection = [{"name": "s16", "repetition": 0}, {"name": "s16", "repetition": 1}]
        commands = expected_commands(selection, profile, False)
        # 15 processes per lifecycle: one each for admission, generate, reopen,
        # recount, query, export, verify, and clean_import; five for ingest; two
        # for the reopen proof.
        self.assertEqual(len(commands), 30)
        self.assertIn("s16-r0-ingest-3", commands)
        self.assertIn("s16-r1-reopen_proof-1", commands)
        self.assertNotIn("s16-r1-reopen_proof-2", commands)
        self.assertEqual(len(commands), len(set(commands)))
        boundary = expected_commands(selection, profile, True)
        self.assertIn("s16-r0-ingest", boundary)
        self.assertNotIn("s16-r0-ingest-3", boundary)

    def test_input_census_without_merge_work_is_incomplete(self):
        line = 'INGEST_DIAGNOSTIC {"event":"inputs","family":"merge-identities","runs":1025}'
        with self.assertRaisesRegex(ValueError, "missing merge groups"):
            merge_summary([line])

    def test_nested_wall_intervals_are_not_added_twice(self):
        self.assertEqual(interval_union_ns([(10, 80), (30, 60), (70, 95)]), 85)
        self.assertEqual(interval_union_ns([(0, 10), (20, 30)]), 20)
        with self.assertRaises(ValueError):
            interval_union_ns([(20, 10)])

    def test_merge_aggregator_rejects_incomplete_and_private_events(self):
        for event in (
            {"event": "inputs", "family": "/private/path", "runs": 1},
            {"event": "group", "family": "merge-identities", "success": False},
        ):
            with self.assertRaises(ValueError):
                merge_summary(["INGEST_DIAGNOSTIC " + json.dumps(event)])
        with self.assertRaises(ValueError):
            merge_summary([])


if __name__ == "__main__":
    unittest.main()
