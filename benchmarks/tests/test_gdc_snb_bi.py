from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_snb_bi import (
    ANALYTICAL_READS,
    BATCH_DELETES,
    BATCH_INSERTS,
    BATCH_UPDATE_CAUSE,
    EVIDENCE_SCHEMA,
    OPERATIONS,
    RESOURCE_SCHEMA,
    WEIGHTED_PATH_CAUSE,
    WEIGHTED_PATH_READS,
    SnbBiSuiteError,
    assert_large_scale_factors_are_opt_in,
    assert_separate_from_other_suites,
    list_operation_rules,
    map_operation_file,
    run_tiny_suite,
)
from jsonschema import Draft202012Validator


def _ensure_runner_built(root: Path) -> Path:
    binary = root / "target" / "debug" / "graphforge-benchmark-gdc-snb-bi"
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
            "graphforge-benchmark-gdc-snb-bi",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed to build snb-bi runner\n{completed.stdout}\n{completed.stderr}"
        )
    assert binary.is_file(), binary
    return binary


class GdcSnbBiSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.binary = _ensure_runner_built(cls.root)
        os.environ["GRAPHFORGE_GDC_SNB_BI_BIN"] = str(cls.binary)

    def test_suite_declaration_uses_snb_bi_runner(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        suite = suites["snb-bi"]
        self.assertEqual(suite["runner"], "gdc-snb-bi")
        self.assertEqual(suite["disposition"], "executable")
        self.assertEqual(suite["datasets"][0], "snb-bi-sf0.003")
        assert_separate_from_other_suites()

    def test_all_operations_declare_mapping_and_validation_rules(self) -> None:
        rules = list_operation_rules()
        self.assertEqual(set(rules), set(OPERATIONS))
        self.assertEqual(len(rules), 36)
        for read in ANALYTICAL_READS:
            self.assertEqual(rules[read]["category"], "analytical_read", read)
            if read in WEIGHTED_PATH_READS:
                self.assertTrue(rules[read]["mapping"].startswith("semantic_incompatibility"), read)
                self.assertIn(WEIGHTED_PATH_CAUSE, rules[read]["mapping"])
                self.assertEqual(rules[read]["validation"], "none", read)
            else:
                self.assertEqual(rules[read]["mapping"], "compatible", read)
        self.assertEqual(rules["BI2"]["validation"], "normalized")
        self.assertEqual(rules["BI12"]["validation"], "normalized")
        self.assertEqual(rules["BI16"]["validation"], "normalized")
        self.assertEqual(rules["BI1"]["validation"], "exact")
        for update in BATCH_INSERTS:
            self.assertEqual(rules[update]["category"], "batch_insert", update)
            self.assertTrue(rules[update]["mapping"].startswith("semantic_incompatibility"), update)
            self.assertEqual(rules[update]["validation"], "none", update)
        for delete in BATCH_DELETES:
            self.assertEqual(rules[delete]["category"], "batch_delete", delete)
            self.assertTrue(rules[delete]["mapping"].startswith("semantic_incompatibility"), delete)
            self.assertEqual(rules[delete]["validation"], "none", delete)

    def test_compatible_fixture_passes_with_per_op_statuses(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "snb-bi")
        self.assertEqual(evidence["dataset_id"], "snb-bi-sf0.003")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(evidence["phases"], ["load", "updates", "query", "validation"])
        self.assertIn("spec", evidence["identities"])
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(set(by_op), set(OPERATIONS))
        for read in ANALYTICAL_READS:
            if read in WEIGHTED_PATH_READS:
                continue
            self.assertEqual(by_op[read]["status"], "passed", read)
            self.assertIsNotNone(by_op[read].get("public_api"))
        for incompatible in (*WEIGHTED_PATH_READS, *BATCH_INSERTS, *BATCH_DELETES):
            self.assertEqual(
                by_op[incompatible]["status"], "semantic_incompatibility", incompatible
            )
            self.assertIsNotNone(by_op[incompatible].get("cause"))
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-snb-bi-evidence.json").read_text(encoding="utf-8")
            )
        ).validate(evidence)

    def test_resources_recorded_separately_from_correctness(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        # Resource evidence lives in a distinct section, never inside correctness.
        resources = evidence["resources"]
        self.assertEqual(resources["schema"], RESOURCE_SCHEMA)
        self.assertEqual(resources["dataset_id"], "snb-bi-sf0.003")
        for field in ("load", "query", "spill", "rss", "io"):
            self.assertIn(field, resources, field)
        self.assertIn("wall_ms", resources["load"])
        self.assertIn("wall_ms", resources["query"])
        self.assertIn("bytes", resources["spill"])
        self.assertIn("peak_bytes", resources["rss"])
        self.assertIn("read_bytes", resources["io"])
        self.assertIn("write_bytes", resources["io"])
        for outcome in evidence["operations"]:
            self.assertNotIn("resources", outcome)
            self.assertNotIn("rss", outcome)
            self.assertNotIn("load", outcome)

    def test_unsupported_semantics_fail_visibly(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "snb-bi-tiny" / "compatible" / "jobs"
        with self.assertRaises(SnbBiSuiteError) as raised_insert:
            map_operation_file(fixture / "INS1.json")
        self.assertEqual(raised_insert.exception.cause, "semantic_incompatibility")
        self.assertIn(BATCH_UPDATE_CAUSE, str(raised_insert.exception))

        with self.assertRaises(SnbBiSuiteError) as raised_delete:
            map_operation_file(fixture / "DEL1.json")
        self.assertEqual(raised_delete.exception.cause, "semantic_incompatibility")
        self.assertIn(BATCH_UPDATE_CAUSE, str(raised_delete.exception))

        with self.assertRaises(SnbBiSuiteError) as raised_weighted:
            map_operation_file(fixture / "BI15.json")
        self.assertEqual(raised_weighted.exception.cause, "semantic_incompatibility")
        self.assertIn(WEIGHTED_PATH_CAUSE, str(raised_weighted.exception))

    def test_reference_mismatch_is_visible(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(by_op["BI1"]["status"], "failed")
        self.assertIn("reference_mismatch", by_op["BI1"]["cause"])
        self.assertEqual(evidence["status"], "failed")

    def test_semantic_incompat_fixture_fails_closed(self) -> None:
        evidence = run_tiny_suite(fixture_name="semantic-incompat")
        by_op = {item["operation"]: item for item in evidence["operations"]}
        for incompatible in (*WEIGHTED_PATH_READS, *BATCH_INSERTS, *BATCH_DELETES):
            self.assertEqual(
                by_op[incompatible]["status"], "semantic_incompatibility", incompatible
            )

    def test_large_scale_factors_are_opt_in(self) -> None:
        assert_large_scale_factors_are_opt_in()

    def test_index_and_readme_point_at_snb_bi_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`snb-bi`", index)
        self.assertIn("gdc-snb-bi", index)
        self.assertIn("## SNB BI", index)
        self.assertNotIn("blocked on suite issue #963", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_snb_bi", readme)


if __name__ == "__main__":
    unittest.main()
