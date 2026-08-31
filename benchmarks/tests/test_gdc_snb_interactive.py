from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_snb_interactive import (
    EVIDENCE_SCHEMA,
    OPERATIONS,
    PHASES,
    SUPPORTED_OPERATIONS,
    TINY_DATASET,
    SnbInteractiveSuiteError,
    assert_not_audited_certification,
    list_operation_rules,
    load_ladder,
    map_job_file,
    ordered_dataset_ids,
    run_tiny_suite,
)
from jsonschema import Draft202012Validator


def _ensure_runner_built(root: Path) -> Path:
    binary = root / "target" / "debug" / "graphforge-benchmark-gdc-snb-interactive"
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
            "graphforge-benchmark-gdc-snb-interactive",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed to build snb-interactive runner\n{completed.stdout}\n{completed.stderr}"
        )
    assert binary.is_file(), binary
    return binary


class GdcSnbInteractiveSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.binary = _ensure_runner_built(cls.root)
        os.environ["GRAPHFORGE_GDC_SNB_INTERACTIVE_BIN"] = str(cls.binary)

    def test_suite_declaration_uses_snb_interactive_runner(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        suite = suites["snb-interactive"]
        self.assertEqual(suite["runner"], "gdc-snb-interactive")
        self.assertEqual(suite["disposition"], "executable")
        self.assertEqual(suite["datasets"][0], TINY_DATASET)

    def test_ordered_ladder_begins_with_tiny_engineering_scale(self) -> None:
        self.assertEqual(load_ladder()["datasets"][0]["id"], TINY_DATASET)
        self.assertEqual(ordered_dataset_ids()[0], TINY_DATASET)
        self.assertIn("snb-sf1", ordered_dataset_ids())

    def test_completeness_and_unsupported_policy_are_explicit(self) -> None:
        rules = list_operation_rules()
        self.assertEqual(set(rules), set(OPERATIONS))
        self.assertEqual(len(OPERATIONS), 29)
        for key in SUPPORTED_OPERATIONS:
            self.assertEqual(rules[key]["support"], "supported", key)
            self.assertEqual(rules[key]["validation"], "exact", key)
        self.assertEqual(rules["ic1"]["support"], "complex_read_requires_interactive_driver")
        self.assertEqual(rules["iu1"]["support"], "update_stream_protocol_not_exposed")
        self.assertEqual(
            rules["is2"]["support"],
            "short_read_requires_interactive_result_contract",
        )

    def test_compatible_fixture_validates_mechanics_and_correctness(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "snb-interactive")
        self.assertEqual(evidence["dataset_id"], TINY_DATASET)
        self.assertEqual(evidence["status"], "passed")
        assert_not_audited_certification(evidence)
        self.assertEqual(
            [phase["phase"] for phase in evidence["phases"]],
            list(PHASES),
        )
        self.assertEqual(evidence["completeness"]["policy"], "full_catalog_declare_gaps")
        self.assertEqual(evidence["completeness"]["catalog_size"], 29)
        self.assertEqual(evidence["completeness"]["supported"], 3)
        self.assertEqual(evidence["completeness"]["unsupported"], 26)
        self.assertEqual(evidence["completeness"]["failed"], 0)
        self.assertIn("spec", evidence["identities"])
        self.assertIn("driver", evidence["identities"])
        by_key = {item["workload_key"]: item for item in evidence["operations"]}
        self.assertEqual(set(by_key), set(OPERATIONS))
        for key in SUPPORTED_OPERATIONS:
            self.assertEqual(by_key[key]["status"], "passed", key)
            self.assertEqual(by_key[key]["public_api"]["interface"], "cypher")
        self.assertEqual(by_key["ic1"]["status"], "semantic_incompatibility")
        self.assertIn("complex_read_requires_interactive_driver", by_key["ic1"]["cause"])
        self.assertEqual(by_key["iu1"]["status"], "semantic_incompatibility")
        self.assertIn("update_stream_protocol_not_exposed", by_key["iu1"]["cause"])
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-snb-interactive-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_unsupported_semantics_fail_visibly_not_skipped(self) -> None:
        ic1 = (
            self.root
            / "fixtures"
            / "gdc"
            / "snb-interactive-tiny"
            / "compatible"
            / "jobs"
            / "ic1.json"
        )
        with self.assertRaises(SnbInteractiveSuiteError) as raised:
            map_job_file(ic1)
        self.assertEqual(raised.exception.cause, "semantic_incompatibility")
        self.assertIn("complex_read_requires_interactive_driver", str(raised.exception))

        iu1 = ic1.with_name("iu1.json")
        with self.assertRaises(SnbInteractiveSuiteError) as raised_iu:
            map_job_file(iu1)
        self.assertEqual(raised_iu.exception.cause, "semantic_incompatibility")
        self.assertIn("update_stream_protocol_not_exposed", str(raised_iu.exception))

    def test_reference_mismatch_is_visible(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_key = {item["workload_key"]: item for item in evidence["operations"]}
        self.assertEqual(by_key["is1"]["status"], "failed")
        self.assertIn("reference_mismatch", by_key["is1"]["cause"])
        self.assertEqual(evidence["status"], "failed")
        assert_not_audited_certification(evidence)

    def test_index_and_readme_point_at_snb_interactive_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`snb-interactive`", index)
        self.assertIn("gdc-snb-interactive", index)
        self.assertNotIn("blocked on suite issue #962", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_snb_interactive", readme)


if __name__ == "__main__":
    unittest.main()
