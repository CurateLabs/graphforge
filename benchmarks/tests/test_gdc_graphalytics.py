from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_graphalytics import (
    ALGORITHMS,
    EVIDENCE_SCHEMA,
    GraphalyticsSuiteError,
    assert_separate_from_graph500,
    list_algorithm_rules,
    load_ladder,
    map_job_file,
    ordered_dataset_ids,
    run_tiny_suite,
)
from jsonschema import Draft202012Validator


def _ensure_runner_built(root: Path) -> Path:
    binary = root / "target" / "debug" / "graphforge-benchmark-gdc-graphalytics"
    if binary.is_file():
        return binary
    target_dir = root / "target"
    completed = subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(root / "Cargo.toml"),
            "-p",
            "graphforge-benchmark-gdc-graphalytics",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed to build graphalytics runner\n{completed.stdout}\n{completed.stderr}"
        )
    assert binary.is_file(), binary
    return binary


class GdcGraphalyticsSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.binary = _ensure_runner_built(cls.root)
        os.environ["GRAPHFORGE_GDC_GRAPHALYTICS_BIN"] = str(cls.binary)

    def test_suite_declaration_uses_graphalytics_runner(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        suite = suites["graphalytics"]
        self.assertEqual(suite["runner"], "gdc-graphalytics")
        self.assertEqual(suite["disposition"], "executable")
        self.assertEqual(suite["datasets"][0], "ga-tiny")
        self.assertNotIn("graph500", json.dumps(suite))

    def test_ordered_ladder_begins_with_bounded_fixture(self) -> None:
        self.assertEqual(load_ladder()["datasets"][0]["id"], "ga-tiny")
        self.assertEqual(ordered_dataset_ids()[0], "ga-tiny")
        self.assertIn("wiki-Talk", ordered_dataset_ids())
        assert_separate_from_graph500()

    def test_all_six_algorithms_declare_mapping_and_tolerance_rules(self) -> None:
        rules = list_algorithm_rules()
        self.assertEqual(set(rules), set(ALGORITHMS))
        self.assertEqual(rules["bfs"]["validation"], "exact")
        self.assertEqual(rules["cdlp"]["validation"], "exact")
        self.assertEqual(rules["wcc"]["validation"], "equivalence")
        self.assertEqual(rules["pr"]["validation"], "epsilon")
        self.assertEqual(rules["lcc"]["validation"], "epsilon")
        self.assertEqual(rules["sssp"]["validation"], "epsilon")
        for algorithm in ALGORITHMS:
            self.assertTrue(rules[algorithm]["determinism"])

    def test_compatible_fixture_passes_or_fails_closed_per_algorithm(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "graphalytics")
        self.assertEqual(evidence["dataset_id"], "ga-tiny")
        self.assertEqual(evidence["status"], "passed")
        self.assertIn("spec", evidence["identities"])
        self.assertIn("driver", evidence["identities"])
        by_key = {item["workload_key"]: item for item in evidence["algorithms"]}
        self.assertEqual(set(by_key), set(ALGORITHMS))
        for key in ("bfs", "wcc", "lcc", "sssp"):
            self.assertEqual(by_key[key]["status"], "passed", key)
            self.assertIsNotNone(by_key[key].get("public_api"))
        for key in ("pr", "cdlp"):
            self.assertEqual(by_key[key]["status"], "semantic_incompatibility", key)
            self.assertIsNotNone(by_key[key].get("cause"))
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-graphalytics-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_unsupported_semantics_fail_visibly(self) -> None:
        pr_job = (
            self.root / "fixtures" / "gdc" / "graphalytics-tiny" / "compatible" / "jobs" / "pr.json"
        )
        with self.assertRaises(GraphalyticsSuiteError) as raised:
            map_job_file(pr_job)
        self.assertEqual(raised.exception.cause, "semantic_incompatibility")
        self.assertIn("fixed_iteration_pagerank_not_exposed", str(raised.exception))

        cdlp_job = pr_job.with_name("cdlp.json")
        with self.assertRaises(GraphalyticsSuiteError) as raised_cdlp:
            map_job_file(cdlp_job)
        self.assertEqual(raised_cdlp.exception.cause, "semantic_incompatibility")
        self.assertIn("synchronous_cdlp_not_exposed", str(raised_cdlp.exception))

    def test_reference_mismatch_is_visible(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_key = {item["workload_key"]: item for item in evidence["algorithms"]}
        self.assertEqual(by_key["bfs"]["status"], "failed")
        self.assertIn("reference_mismatch", by_key["bfs"]["cause"])
        self.assertEqual(evidence["status"], "failed")

    def test_index_and_readme_point_at_graphalytics_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`graphalytics`", index)
        self.assertIn("gdc-graphalytics", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_graphalytics", readme)


if __name__ == "__main__":
    unittest.main()
