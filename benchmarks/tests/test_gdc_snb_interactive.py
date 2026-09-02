from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import GdcContractError, list_gdc_suites, workspace_root
from graphforge_bench.gdc_snb_interactive import (
    COMPLEX_READS,
    EVIDENCE_SCHEMA,
    IC14_CAUSE,
    LIVE_DATASET_ID,
    OPERATIONS,
    SHORT_READS,
    UPDATE_CAUSE,
    UPDATES,
    SnbInteractiveSuiteError,
    assert_separate_from_other_suites,
    list_operation_rules,
    map_operation_file,
    run_live_is1,
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
        self.assertEqual(suite["datasets"][0], "snb-interactive-static-synthetic-v1")
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
            self.assertTrue(rules[update]["mapping"].startswith("semantic_incompatibility"), update)
            self.assertEqual(rules[update]["validation"], "none", update)

    def test_compatible_fixture_passes_with_per_op_statuses(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "snb-interactive")
        self.assertEqual(evidence["dataset_id"], "snb-interactive-static-synthetic-v1")
        self.assertEqual(evidence["lane"], "static_replay")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(evidence["phases"], ["load", "warmup", "execution", "validation"])
        self.assertTrue(
            all(phase["status"] == "not_executed" for phase in evidence["phase_evidence"])
        )
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

    def test_live_is1_executes_real_in_memory_engine_and_validates_arrow_rows(self) -> None:
        evidence = run_live_is1()
        self.assertEqual(evidence["dataset_id"], LIVE_DATASET_ID)
        self.assertEqual(evidence["lane"], "live_in_memory")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(
            [phase["phase"] for phase in evidence["phase_evidence"]],
            ["load", "warmup", "execution", "validation"],
        )
        self.assertTrue(all(phase["status"] == "passed" for phase in evidence["phase_evidence"]))
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(by_op["IS1"]["status"], "passed")
        self.assertEqual(by_op["IC14"]["status"], "semantic_incompatibility")
        for update in UPDATES:
            self.assertEqual(by_op[update]["status"], "semantic_incompatibility")
        self.assertEqual(
            evidence["identities"]["fixture"]["classification"], "synthetic_engineering_fixture"
        )
        self.assertIsNone(evidence["identities"]["runner"]["commit"])
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-snb-interactive-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_live_parameter_and_reference_mutations_fail(self) -> None:
        source = self.root / "fixtures" / "gdc" / "snb-interactive-live-is1"
        with tempfile.TemporaryDirectory() as tmp:
            copied = Path(tmp) / "fixture"
            shutil.copytree(source, copied)
            job = json.loads((copied / "IS1.json").read_text(encoding="utf-8"))
            job["parameters"]["personId"] = 9999
            (copied / "IS1.json").write_text(json.dumps(job), encoding="utf-8")
            with self.assertRaises(SnbInteractiveSuiteError) as raised:
                run_live_is1(fixture_path=copied / "graph.json")
            self.assertEqual(raised.exception.cause, "reference_mismatch")

        with tempfile.NamedTemporaryFile("w", suffix=".ref") as mutated:
            mutated.write(
                "Grace Hopper 1906-12-09 192.0.2.11 Safari 2001 female 2026-01-03T04:05:06Z\n"
            )
            mutated.flush()
            with self.assertRaises(SnbInteractiveSuiteError) as raised:
                run_live_is1(reference_path=Path(mutated.name))
            self.assertEqual(raised.exception.cause, "reference_mismatch")

    def test_live_lane_rejects_static_fixture_and_acquisition_drift(self) -> None:
        static_graph = (
            self.root
            / "fixtures"
            / "gdc"
            / "snb-interactive-tiny"
            / "compatible"
            / "snb-interactive-static-synthetic-v1.graph"
        )
        with self.assertRaises((json.JSONDecodeError, GdcContractError, SnbInteractiveSuiteError)):
            run_live_is1(fixture_path=static_graph)

        source = self.root / "fixtures" / "gdc" / "snb-interactive-live-is1"
        with tempfile.TemporaryDirectory() as tmp:
            copied = Path(tmp) / "fixture"
            shutil.copytree(source, copied)
            acquisition = json.loads((copied / "acquisition.json").read_text(encoding="utf-8"))
            acquisition["recorded_driver"]["commit"] = "0" * 40
            (copied / "acquisition.json").write_text(json.dumps(acquisition), encoding="utf-8")
            with self.assertRaises(GdcContractError) as raised:
                run_live_is1(fixture_path=copied / "graph.json")
            self.assertEqual(raised.exception.cause, "identity_drift")

    def test_index_and_readme_point_at_snb_interactive_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`snb-interactive`", index)
        self.assertIn("gdc-snb-interactive", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_snb_interactive", readme)


if __name__ == "__main__":
    unittest.main()
