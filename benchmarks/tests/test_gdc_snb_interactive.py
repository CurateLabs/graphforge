from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_snb_interactive import (
    COMPLEX_READS,
    EVIDENCE_SCHEMA,
    IC14_CAUSE,
    OPERATIONS,
    SHORT_READS,
    UPDATES,
    UPDATE_CAUSE,
    SnbInteractiveSuiteError,
    assert_separate_from_other_suites,
    list_operation_rules,
    map_operation_file,
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
        self.assertEqual(suite["datasets"][0], "snb-sf0.003")
        assert_separate_from_other_suites()

    def test_all_operations_declare_mapping_and_validation_rules(self) -> None:
        rules = list_operation_rules()
        self.assertEqual(set(rules), set(OPERATIONS))
        self.assertEqual(len(rules), 29)
        for read in COMPLEX_READS:
            if read == "IC14":
                continue
            self.assertEqual(rules[read]["mapping"], "compatible", read)
        for read in SHORT_READS:
            self.assertEqual(rules[read]["mapping"], "compatible", read)
            self.assertEqual(rules[read]["category"], "short_read", read)
        self.assertEqual(rules["IC4"]["validation"], "normalized")
        self.assertEqual(rules["IC6"]["validation"], "normalized")
        self.assertEqual(rules["IC10"]["validation"], "normalized")
        self.assertEqual(rules["IC1"]["validation"], "exact")
        self.assertTrue(rules["IC14"]["mapping"].startswith("semantic_incompatibility"))
        for update in UPDATES:
            self.assertEqual(rules[update]["category"], "update", update)
            self.assertTrue(
                rules[update]["mapping"].startswith("semantic_incompatibility"), update
            )
            self.assertEqual(rules[update]["validation"], "none", update)

    def test_compatible_fixture_passes_with_per_op_statuses(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "snb-interactive")
        self.assertEqual(evidence["dataset_id"], "snb-sf0.003")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(evidence["phases"], ["load", "warmup", "execution", "validation"])
        self.assertIn("spec", evidence["identities"])
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(set(by_op), set(OPERATIONS))
        for read in list(COMPLEX_READS) + list(SHORT_READS):
            if read == "IC14":
                continue
            self.assertEqual(by_op[read]["status"], "passed", read)
            self.assertIsNotNone(by_op[read].get("public_api"))
        for incompatible in ("IC14", *UPDATES):
            self.assertEqual(
                by_op[incompatible]["status"], "semantic_incompatibility", incompatible
            )
            self.assertIsNotNone(by_op[incompatible].get("cause"))
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-snb-interactive-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_unsupported_semantics_fail_visibly(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "snb-interactive-tiny" / "compatible" / "jobs"
        with self.assertRaises(SnbInteractiveSuiteError) as raised_update:
            map_operation_file(fixture / "IU1.json")
        self.assertEqual(raised_update.exception.cause, "semantic_incompatibility")
        self.assertIn(UPDATE_CAUSE, str(raised_update.exception))

        with self.assertRaises(SnbInteractiveSuiteError) as raised_ic14:
            map_operation_file(fixture / "IC14.json")
        self.assertEqual(raised_ic14.exception.cause, "semantic_incompatibility")
        self.assertIn(IC14_CAUSE, str(raised_ic14.exception))

    def test_reference_mismatch_is_visible(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(by_op["IC1"]["status"], "failed")
        self.assertIn("reference_mismatch", by_op["IC1"]["cause"])
        self.assertEqual(evidence["status"], "failed")

    def test_index_and_readme_point_at_snb_interactive_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`snb-interactive`", index)
        self.assertIn("gdc-snb-interactive", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_snb_interactive", readme)


if __name__ == "__main__":
    unittest.main()
