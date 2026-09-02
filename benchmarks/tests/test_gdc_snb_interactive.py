from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
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
        context = evidence["live_context"]
        self.assertEqual(context["operation"], "IS1")
        self.assertEqual(
            context["parameter"], {"name": "personId", "data_type": "int64", "value": 1001}
        )
        self.assertEqual(
            context["fixture_sha256"],
            "58e3c52a4ac2d74456439a322211adf1e8f560a7762e3fd2d376bbe96d243d6f",
        )
        self.assertEqual(
            context["job_sha256"],
            "0143a649da769c093d5e235e66c6036a4aa38ab05a5c68f5744ad4025a503831",
        )
        self.assertEqual(
            context["reference_sha256"],
            "71465ea5b672abd79693590e316cb4cc023cd25737c57d8daa13467542972385",
        )
        self.assertEqual(
            context["acquisition_sha256"],
            "fe8167c8b9cb939306495a937b45c375ea09b08725772a439c487099363f25e2",
        )
        self.assertEqual(
            context["identity_sha256"],
            "a7b31720ac9ba61a5968f752d4e8eb8d709353226f36a2dcd065016657d4f030",
        )
        self.assertEqual(context["public_api"], "graphforge_api::GraphForge")
        self.assertEqual(context["mode"], "in_memory")
        self.assertIn("person.id = $personId", context["query"])
        self.assertEqual(len(context["row_schema"]), 8)
        self.assertEqual(
            context["row_order"],
            [field["name"] for field in context["row_schema"]],
        )
        self.assertEqual(
            evidence["identities"]["spec"]["commit"],
            "5f7956e07a214373c363b371a3b88bc83ddcd118",
        )
        self.assertEqual(
            evidence["identities"]["generator"]["commit"],
            "2459f4e45834c78902a50511fc64a05c48dd4029",
        )
        self.assertEqual(
            evidence["identities"]["driver"]["commit"],
            "f9c394a92cd55e535893f6c9907b141d6533c817",
        )
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-snb-interactive-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_python_wrapper_cannot_supply_rows_fixture_reference_or_identity(self) -> None:
        for replacement in (
            {"fixture_path": Path("graph.json")},
            {"job_path": Path("IS1.json")},
            {"reference_path": Path("IS1.ref")},
            {"rows": [["forged"]]},
            {"identities": {}},
        ):
            with self.assertRaises(TypeError):
                run_live_is1(**replacement)

    def test_adversarial_producer_envelope_cannot_emit_live_success(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "snb-interactive-live-is1"
        forged = {
            "schema": "graphforge-arrow-row-receipt/1",
            "source": "graphforge_in_memory_execute",
            "columns": [
                "firstName",
                "lastName",
                "birthday",
                "locationIP",
                "browserUsed",
                "cityId",
                "gender",
                "creationDate",
            ],
            "rows": [(fixture / "IS1.ref").read_text(encoding="utf-8").splitlines()[-1].split()],
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            envelope = tmp_path / "forged-envelope.json"
            identities = tmp_path / "empty-identities.json"
            evidence = tmp_path / "evidence.json"
            envelope.write_text(json.dumps(forged), encoding="utf-8")
            identities.write_text("{}", encoding="utf-8")
            completed = subprocess.run(
                [
                    str(self.binary),
                    "validate-live-is1",
                    str(fixture / "IS1.json"),
                    str(fixture / "IS1.ref"),
                    str(envelope),
                    str(identities),
                    str(evidence),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 2)
            self.assertIn("unknown command", completed.stderr)
            self.assertFalse(evidence.exists())

            extra_arg = subprocess.run(
                [str(self.binary), "run-live-is1", str(evidence), str(envelope)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(extra_arg.returncode, 2)
            self.assertIn("accepts only EVIDENCE.json", extra_arg.stderr)
            self.assertFalse(evidence.exists())

    def test_index_and_readme_point_at_snb_interactive_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`snb-interactive`", index)
        self.assertIn("gdc-snb-interactive", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_snb_interactive", readme)


if __name__ == "__main__":
    unittest.main()
