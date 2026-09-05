from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_snb_bi import (
    ANALYTICAL_READS,
    BATCH_DELETES,
    BATCH_INSERTS,
    BATCH_UPDATE_CAUSE,
    EVIDENCE_SCHEMA,
    LIVE_EVIDENCE_SCHEMA,
    OPERATIONS,
    RESOURCE_SCHEMA,
    WEIGHTED_PATH_CAUSE,
    WEIGHTED_PATH_READS,
    SnbBiSuiteError,
    assert_large_scale_factors_are_opt_in,
    assert_separate_from_other_suites,
    list_operation_rules,
    map_operation_file,
    run_live_bi2,
    run_tiny_suite,
    validate_live_fixture,
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

    def test_internal_driver_identity_matches_actual_source_bytes(self) -> None:
        runner = self.root / "runners" / "gdc-snb-bi"
        digest = hashlib.sha256()
        for relative in ("Cargo.toml", "src/lib.rs", "src/main.rs"):
            digest.update(relative.encode())
            digest.update(b"\0")
            digest.update((runner / relative).read_bytes())
            digest.update(b"\0")
        identity = json.loads(
            (self.root / "profiles" / "gdc" / "snb-bi-identity.json").read_text(encoding="utf-8")
        )
        self.assertEqual(identity["driver"]["content_sha256"], digest.hexdigest())

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
        self.assertEqual(rules["BI2"]["validation"], "exact")
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

    def test_live_bi2_executes_real_in_memory_graphforge(self) -> None:
        evidence = run_live_bi2()
        self.assertEqual(evidence["schema"], LIVE_EVIDENCE_SCHEMA)
        self.assertEqual(evidence["lane"], "live_in_memory")
        self.assertEqual(evidence["operation"], "BI2")
        self.assertEqual(evidence["source_mode"], "runner_owned_rust_api")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(
            evidence["correctness"]["rows"],
            ["Beta 1 3 2", "Alpha 2 1 1", "Gamma 2 1 1"],
        )
        self.assertEqual(
            evidence["correctness"]["validation_mode"],
            "exact",
        )
        self.assertFalse(evidence["execution_authority"]["caller_supplied_result"])
        self.assertRegex(
            evidence["execution_authority"]["runner_executable_sha256"],
            r"^[a-f0-9]{64}$",
        )

    def test_live_lane_rejects_parameter_mutation_and_static_output(self) -> None:
        with self.assertRaises(SnbBiSuiteError) as raised:
            run_live_bi2(parameters_override={"tagClass": "SportsTeam"})
        self.assertEqual(raised.exception.cause, "parameter_identity_mismatch")

        fixture = self.root / "fixtures" / "gdc" / "snb-bi-live"
        identity = validate_live_fixture(fixture)
        forged = {
            "schema": LIVE_EVIDENCE_SCHEMA,
            "source_mode": "runner_owned_rust_api",
            "parameters": identity["parameters"],
            "rows": ["Beta 1 3 2", "Alpha 2 1 1", "Gamma 2 1 1"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            forged_path = Path(tmp) / "forged.json"
            forged_path.write_text(json.dumps(forged) + "\n", encoding="utf-8")
            evidence_path = Path(tmp) / "evidence.json"
            completed = subprocess.run(
                [str(self.binary), "run-live", str(forged_path), str(evidence_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertFalse(evidence_path.exists())

    def test_python_wrapper_cannot_supply_rows_or_identity(self) -> None:
        for replacement in (
            {"rows": ["Beta 1 3 2", "Alpha 2 1 1", "Gamma 2 1 1"]},
            {"identities": {}},
            {"source": "graphforge_public_python_api"},
            {"result_path": Path("forged.json")},
        ):
            with self.assertRaises(TypeError):
                run_live_bi2(**replacement)

    def test_adversarial_caller_envelope_cannot_emit_live_success(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "snb-bi-live"
        forged = {
            "schema": "graphforge-gdc-snb-bi-live-result/1",
            "operation": "BI2",
            "source": "graphforge_public_python_api",
            "parameters_sha256": "0" * 64,
            "columns": ["tagName", "countWindow1", "countWindow2", "diff"],
            "rows": ["Beta 1 3 2", "Alpha 2 1 1", "Gamma 2 1 1"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            envelope = tmp_path / "forged-envelope.json"
            evidence = tmp_path / "evidence.json"
            envelope.write_text(json.dumps(forged) + "\n", encoding="utf-8")
            retired = subprocess.run(
                [
                    str(self.binary),
                    "validate-live",
                    str(envelope),
                    str(fixture / "expected-bi2.ref"),
                    "0" * 64,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(retired.returncode, 2)
            self.assertIn("unknown command", retired.stderr)
            self.assertFalse(evidence.exists())

            extra = subprocess.run(
                [str(self.binary), "run-live", str(fixture), str(evidence), str(envelope)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(extra.returncode, 2)
            self.assertIn("accepts only FIXTURE_DIR EVIDENCE.json", extra.stderr)
            self.assertFalse(evidence.exists())

    def test_live_seed_parameter_and_reference_mutations_fail_closed(self) -> None:
        source = self.root / "fixtures" / "gdc" / "snb-bi-live"
        cases = (
            ("seed.json", "checksum_mismatch"),
            ("parameters.json", "parameter_identity_mismatch"),
            ("expected-bi2.ref", "checksum_mismatch"),
        )
        with tempfile.TemporaryDirectory() as tmp:
            for filename, cause in cases:
                fixture = Path(tmp) / filename
                shutil.copytree(source, fixture)
                path = fixture / filename
                path.write_bytes(path.read_bytes() + b"\nmutated\n")
                with self.subTest(filename=filename), self.assertRaises(SnbBiSuiteError) as raised:
                    validate_live_fixture(fixture)
                self.assertEqual(raised.exception.cause, cause)
                shutil.rmtree(fixture)

    def test_static_replay_uses_an_explicit_non_live_command(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertNotIn("execution_authority", evidence)
        self.assertNotEqual(evidence.get("lane"), "live_in_memory")

    def test_every_live_identity_field_and_member_is_closed(self) -> None:
        source = self.root / "fixtures" / "gdc" / "snb-bi-live"
        original = json.loads((source / "identity.json").read_text(encoding="utf-8"))

        def leaves(value: object, path: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
            if isinstance(value, dict):
                return [
                    leaf for key, child in value.items() for leaf in leaves(child, (*path, key))
                ]
            if isinstance(value, list):
                return [
                    leaf
                    for index, child in enumerate(value)
                    for leaf in leaves(child, (*path, index))
                ]
            return [path]

        def mutate(value: object) -> object:
            if value is None:
                return "mutated"
            if isinstance(value, bool):
                return not value
            if isinstance(value, int):
                return value + 1
            if isinstance(value, str):
                return f"{value}-mutated"
            raise AssertionError(type(value))

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for index, path in enumerate(leaves(original)):
                fixture = base / f"fixture-{index}"
                shutil.copytree(source, fixture)
                changed = json.loads(json.dumps(original))
                parent = changed
                for member in path[:-1]:
                    parent = parent[member]
                parent[path[-1]] = mutate(parent[path[-1]])
                (fixture / "identity.json").write_text(
                    json.dumps(changed) + "\n",
                    encoding="utf-8",
                )
                with self.subTest(path=path), self.assertRaises(SnbBiSuiteError) as raised:
                    validate_live_fixture(fixture)
                self.assertEqual(raised.exception.cause, "identity_drift")
                shutil.rmtree(fixture)

            unknown = base / "fixture-unknown"
            shutil.copytree(source, unknown)
            changed = json.loads(json.dumps(original))
            changed["unexpected"] = "forbidden"
            (unknown / "identity.json").write_text(
                json.dumps(changed) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(SnbBiSuiteError):
                validate_live_fixture(unknown)

    def test_live_phase_and_resource_evidence_stays_out_of_correctness(self) -> None:
        evidence = run_live_bi2()
        self.assertEqual(evidence["phases"], ["load", "query", "validation"])
        self.assertEqual(evidence["resources"]["load"]["rows_loaded"], 29)
        self.assertEqual(evidence["resources"]["query"]["rows_returned"], 3)
        self.assertIs(evidence["resources"]["correctness_authority"], False)
        self.assertNotIn("resources", evidence["correctness"])
        self.assertNotIn("wall_ms", evidence["correctness"])
        self.assertEqual(
            evidence["resources"]["unobserved"],
            ["spill_bytes", "peak_rss_bytes", "io_bytes"],
        )

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
