from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import list_gdc_suites, workspace_root
from graphforge_bench.gdc_finbench_transaction import (
    COMPATIBLE_READS,
    DEFAULT_TRUNCATION_LIMIT,
    EVIDENCE_SCHEMA,
    LIVE_DATASET_ID,
    OPERATIONS,
    QUERY_DATASET_ID,
    READ_WRITES,
    SIMPLE_READS,
    TRUNCATED_READS,
    TRUNCATION_ORDER,
    WRITE_CAUSE,
    WRITES,
    FinBenchTransactionSuiteError,
    assert_separate_from_other_suites,
    list_operation_rules,
    list_query_catalog,
    map_operation_file,
    query_fixture_path,
    run_live_suite,
    run_query_fixture,
    run_tiny_suite,
    validate_live_fixture,
)
from graphforge_bench.gdc_finbench_transaction_reference import (
    COLUMNS as REFERENCE_COLUMNS,
)
from graphforge_bench.gdc_finbench_transaction_reference import (
    LDBC_PARAMETERS,
    derive_expected,
    render,
)
from jsonschema import Draft202012Validator


def _ensure_runner_built(root: Path) -> Path:
    # Always invoke cargo: it is a no-op when the binary is current, and an
    # existing binary may be stale (CI mounts a persistent benchmarks/target).
    binary = root / "target" / "debug" / "graphforge-benchmark-gdc-finbench-transaction"
    target_dir = root / "target"
    completed = subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(root / "Cargo.toml"),
            "-p",
            "graphforge-benchmark-gdc-finbench-transaction",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed to build finbench-transaction runner\n{completed.stdout}\n{completed.stderr}"
        )
    assert binary.is_file(), binary
    return binary


class GdcFinBenchTransactionSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.binary = _ensure_runner_built(cls.root)
        os.environ["GRAPHFORGE_GDC_FINBENCH_TRANSACTION_BIN"] = str(cls.binary)

    def _evidence_validator(self) -> Draft202012Validator:
        return Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-finbench-transaction-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        )

    def test_suite_declaration_uses_finbench_transaction_runner(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        suite = suites["finbench-transaction"]
        self.assertEqual(suite["runner"], "gdc-finbench-transaction")
        self.assertEqual(suite["disposition"], "executable")
        self.assertEqual(suite["datasets"][0], "finbench-engineering-tiny-v1")
        assert_separate_from_other_suites()

    def test_all_operations_declare_mapping_and_validation_rules(self) -> None:
        rules = list_operation_rules()
        self.assertEqual(set(rules), set(OPERATIONS))
        self.assertEqual(len(rules), 40)
        for read in COMPATIBLE_READS:
            self.assertEqual(rules[read]["mapping"], "compatible", read)
        for read in SIMPLE_READS:
            self.assertEqual(rules[read]["category"], "simple_read", read)
        # Normalized aggregations vs exact ordered reads.
        self.assertEqual(rules["TCR7"]["validation"], "normalized")
        self.assertEqual(rules["TCR9"]["validation"], "normalized")
        self.assertEqual(rules["TCR10"]["validation"], "normalized")
        self.assertEqual(rules["TCR6"]["validation"], "exact")
        self.assertEqual(rules["TSR1"]["validation"], "exact")
        # Every read, TCR1-TCR5 included, maps to exact Cypher.
        self.assertEqual(len(COMPATIBLE_READS), 18)
        for read in ("TCR1", "TCR2", "TCR3", "TCR4", "TCR5"):
            self.assertEqual(rules[read]["validation"], "exact", read)
        # Writes and read-writes fail closed with the write cause.
        for write in list(WRITES) + list(READ_WRITES):
            self.assertTrue(rules[write]["mapping"].startswith("semantic_incompatibility"), write)
            self.assertTrue(rules[write]["mapping"].endswith(WRITE_CAUSE), write)
            self.assertEqual(rules[write]["validation"], "none", write)

    def test_compatible_fixture_passes_with_per_op_statuses(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "finbench-transaction")
        self.assertEqual(evidence["dataset_id"], "finbench-engineering-tiny-v1")
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(evidence["execution_mode"], "static_replay")
        self.assertEqual(evidence["phases"], ["load", "warmup", "execution", "validation"])
        self.assertIn("spec", evidence["identities"])
        # The compatible fixture is clean: no resource or harness failures.
        self.assertEqual(evidence["resource_events"], [])
        self.assertEqual(evidence["harness_failures"], [])
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(set(by_op), set(OPERATIONS))
        for read in COMPATIBLE_READS:
            self.assertEqual(by_op[read]["status"], "passed", read)
            self.assertIsNotNone(by_op[read].get("public_api"))
        self.assertEqual(by_op["TCR10"]["validation_mode"], "normalized")
        self.assertIn("jaccardSimilarity", by_op["TCR10"]["public_api"]["cypher_shape"])
        tcr10_ref = (
            self.root
            / "fixtures"
            / "gdc"
            / "finbench-transaction-tiny"
            / "compatible"
            / "references"
            / "finbench-engineering-tiny-v1-TCR10.ref"
        ).read_text(encoding="utf-8")
        tcr10_out = (
            self.root
            / "fixtures"
            / "gdc"
            / "finbench-transaction-tiny"
            / "compatible"
            / "system-outputs"
            / "finbench-engineering-tiny-v1-TCR10.out"
        ).read_text(encoding="utf-8")
        self.assertIn("0.667", tcr10_ref)
        self.assertEqual(tcr10_out.strip(), "0.667")
        self.assertNotIn("company-", tcr10_ref)
        self.assertNotIn("company-", tcr10_out)
        incompatible = list(WRITES) + list(READ_WRITES)
        for op in incompatible:
            self.assertEqual(by_op[op]["status"], "semantic_incompatibility", op)
            self.assertIsNotNone(by_op[op].get("cause"))
        self._evidence_validator().validate(evidence)

    def test_live_tcr10_loads_graphforge_and_validates_normalized_rows(self) -> None:
        evidence = run_live_suite()
        self.assertEqual(evidence["dataset_id"], LIVE_DATASET_ID)
        self.assertEqual(evidence["execution_mode"], "live_graphforge")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(
            evidence["validator"]["interface"],
            "graphforge-finbench-rust-reference-validator/1",
        )
        self.assertFalse(evidence["identities"]["execution_authority"]["caller_supplied_result"])
        self.assertEqual(
            evidence["identities"]["live"]["normalization"]["result_schema"],
            ["jaccardSimilarity"],
        )
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(by_op["TCR10"]["status"], "passed")
        self.assertEqual(by_op["TCR10"]["validation_mode"], "normalized")
        self.assertIn("$startTime", by_op["TCR10"]["public_api"]["cypher_shape"])
        self.assertIn("$endTime", by_op["TCR10"]["public_api"]["cypher_shape"])
        self.assertIn("jaccardSimilarity", by_op["TCR10"]["public_api"]["cypher_shape"])
        self.assertEqual(set(by_op), {"TCR10", "TW1"})
        self.assertEqual(by_op["TW1"]["status"], "semantic_incompatibility")
        self.assertIn(WRITE_CAUSE, by_op["TW1"]["cause"])
        self.assertEqual(evidence["resource_events"], [])
        self.assertEqual(evidence["harness_failures"], [])
        self._evidence_validator().validate(evidence)

    def test_live_parameter_and_unknown_mutations_fail_before_execution(self) -> None:
        with self.assertRaises(FinBenchTransactionSuiteError) as window:
            run_live_suite(params_override={"startTime": 99})
        self.assertEqual(window.exception.cause, "parameter_identity_mismatch")

        with self.assertRaises(FinBenchTransactionSuiteError) as person:
            run_live_suite(params_override={"pid2": 3})
        self.assertEqual(person.exception.cause, "parameter_identity_mismatch")

        with self.assertRaises(FinBenchTransactionSuiteError) as unknown:
            run_live_suite(params_override={"start": 100})
        self.assertEqual(unknown.exception.cause, "harness_error")

    def test_live_lane_rejects_static_output_documents(self) -> None:
        static_output = (
            self.root
            / "fixtures"
            / "gdc"
            / "finbench-transaction-tiny"
            / "compatible"
            / "system-outputs"
            / "finbench-engineering-tiny-v1-TCR10.out"
        )
        envelope = {
            "schema": "graphforge-gdc-finbench-live-produced/1",
            "source": "python_public_api_in_memory",
            "rows": ["0.667"],
        }
        with tempfile.TemporaryDirectory(prefix="finbench-no-static-") as tmp:
            work = Path(tmp)
            produced = work / "produced.json"
            produced.write_text(json.dumps(envelope) + "\n", encoding="utf-8")
            evidence_path = work / "evidence.json"
            for extra in (static_output, produced):
                completed = subprocess.run(
                    [
                        str(self.binary),
                        "run-live",
                        str(extra),
                        str(evidence_path),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(completed.returncode, 0, extra)
                self.assertIn("static output rejected", completed.stderr)
                self.assertFalse(evidence_path.exists())

            retired = subprocess.run(
                [
                    str(self.binary),
                    "validate-live",
                    str(produced),
                    str(evidence_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(retired.returncode, 0)
        self.assertIn("static output rejected", retired.stderr)

    def test_every_live_identity_field_and_member_is_closed(self) -> None:
        source = self.root / "fixtures" / "gdc" / "finbench-transaction-live"
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
                with (
                    self.subTest(path=path),
                    self.assertRaises(FinBenchTransactionSuiteError) as raised,
                ):
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
            with self.assertRaises(FinBenchTransactionSuiteError):
                validate_live_fixture(unknown)

    def test_semantic_seed_mutation_fails_closed(self) -> None:
        source = self.root / "fixtures" / "gdc" / "finbench-transaction-live"
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "fixture"
            shutil.copytree(source, fixture)
            seed = json.loads((fixture / "seed.json").read_text(encoding="utf-8"))
            seed["invests"].append({"person": 1, "company": 12, "timestamp": 160})
            (fixture / "seed.json").write_text(json.dumps(seed, indent=2) + "\n", encoding="utf-8")
            with self.assertRaises(FinBenchTransactionSuiteError) as raised:
                validate_live_fixture(fixture)
            self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_unsupported_and_write_ops_fail_visibly(self) -> None:
        jobs = self.root / "fixtures" / "gdc" / "finbench-transaction-tiny" / "compatible" / "jobs"
        # Write query fails closed with the write cause.
        with self.assertRaises(FinBenchTransactionSuiteError) as raised_write:
            map_operation_file(jobs / "TW1.json")
        self.assertEqual(raised_write.exception.cause, "semantic_incompatibility")
        self.assertIn(WRITE_CAUSE, str(raised_write.exception))
        # Read-write transaction fails closed with the write cause.
        with self.assertRaises(FinBenchTransactionSuiteError) as raised_rw:
            map_operation_file(jobs / "TRW1.json")
        self.assertIn(WRITE_CAUSE, str(raised_rw.exception))
        # The reads this suite used to refuse now map to their Cypher.
        for read in ("TCR1", "TCR2", "TCR3", "TCR4", "TCR5"):
            mapping = map_operation_file(jobs / f"{read}.json")
            self.assertEqual(mapping["interface"], "cypher", read)
            self.assertIn("MATCH", mapping["cypher_shape"], read)

    def test_query_catalog_exposes_every_read_as_data(self) -> None:
        catalog = list_query_catalog()
        self.assertEqual(catalog["suite_id"], "finbench-transaction")
        queries = {query["operation"]: query for query in catalog["queries"]}
        self.assertEqual(list(queries), list(COMPATIBLE_READS))
        for operation, query in queries.items():
            self.assertEqual(
                map_operation_file(self._job(operation))["cypher_shape"], query["cypher"]
            )
            names = [parameter["name"] for parameter in query["parameters"]]
            self.assertTrue(query["columns"], operation)
            truncation = query["truncation"]
            if operation in TRUNCATED_READS:
                self.assertEqual(truncation["default_limit"], DEFAULT_TRUNCATION_LIMIT, operation)
                self.assertEqual(truncation["order"], TRUNCATION_ORDER, operation)
                self.assertTrue(truncation["truncated_steps"], operation)
                self.assertIn("truncationLimit", names, operation)
                self.assertIn("truncationOrder", names, operation)
                self.assertIn("$truncationLimit", query["cypher"], operation)
            else:
                self.assertIsNone(truncation, operation)
                self.assertNotIn("truncationLimit", names, operation)

    def test_reference_columns_and_ldbc_parameters_match_the_query_catalog(self) -> None:
        catalog = {query["operation"]: query for query in list_query_catalog()["queries"]}
        self.assertEqual(list(REFERENCE_COLUMNS), list(catalog))
        for operation, query in catalog.items():
            self.assertEqual(
                [(column["name"], column["kind"]) for column in query["columns"]],
                list(REFERENCE_COLUMNS[operation]),
                operation,
            )
        # The published read parameters exist for the complex reads only.
        self.assertEqual(list(LDBC_PARAMETERS), [op for op in catalog if op.startswith("TCR")])
        for operation, names in LDBC_PARAMETERS.items():
            self.assertEqual(
                [parameter["name"] for parameter in catalog[operation]["parameters"]],
                list(names),
                operation,
            )

    def _job(self, operation: str) -> Path:
        return (
            self.root
            / "fixtures"
            / "gdc"
            / "finbench-transaction-tiny"
            / "compatible"
            / "jobs"
            / f"{operation}.json"
        )

    def test_expected_rows_are_derived_independently_and_are_current(self) -> None:
        fixture = query_fixture_path()
        derived = derive_expected(fixture)
        committed = (fixture / "expected.json").read_text(encoding="utf-8")
        self.assertEqual(committed, render(derived))
        self.assertEqual(derived["dataset_id"], QUERY_DATASET_ID)
        self.assertEqual(set(derived["results"]), set(COMPATIBLE_READS))
        # Truncation must change the answer somewhere for every truncated read.
        for operation in TRUNCATED_READS:
            results = derived["results"][operation]

            def rest(result: dict[str, object]) -> dict[str, object]:
                return {k: v for k, v in result["binding"].items() if k != "truncationLimit"}

            self.assertTrue(
                any(
                    truncated["binding"]["truncationLimit"] < DEFAULT_TRUNCATION_LIMIT
                    and any(
                        full["binding"]["truncationLimit"] == DEFAULT_TRUNCATION_LIMIT
                        and rest(full) == rest(truncated)
                        and full["rows"] != truncated["rows"]
                        for full in results
                    )
                    for truncated in results
                ),
                operation,
            )

    def test_every_read_runs_live_against_independent_rows(self) -> None:
        evidence = run_query_fixture()
        self.assertEqual(evidence["status"], "passed")
        self.assertIs(evidence["certification"], False)
        self.assertEqual(evidence["execution_mode"], "live_graphforge")
        self.assertEqual(
            evidence["reference_derivation"],
            "graphforge_bench.gdc_finbench_transaction_reference",
        )
        expected = json.loads((query_fixture_path() / "expected.json").read_text(encoding="utf-8"))[
            "results"
        ]
        self.assertEqual(
            len(evidence["outcomes"]), sum(len(results) for results in expected.values())
        )
        for outcome in evidence["outcomes"]:
            operation, index = outcome["operation"], outcome["binding_index"]
            self.assertEqual(outcome["status"], "passed", (operation, index, outcome.get("cause")))
            self.assertEqual(outcome["rows"], expected[operation][index]["rows"])
        self.assertEqual({o["operation"] for o in evidence["outcomes"]}, set(COMPATIBLE_READS))

    def test_live_query_run_fails_on_a_wrong_expected_row(self) -> None:
        with tempfile.TemporaryDirectory(prefix="finbench-query-mismatch-") as tmp:
            fixture = Path(tmp) / "fixture"
            shutil.copytree(query_fixture_path(), fixture)
            expected = json.loads((fixture / "expected.json").read_text(encoding="utf-8"))
            expected["results"]["TCR12"][1]["rows"][0][1] = "300.500"
            (fixture / "expected.json").write_text(json.dumps(expected), encoding="utf-8")
            with self.assertRaises(FinBenchTransactionSuiteError) as raised:
                run_query_fixture(fixture=fixture)
        self.assertEqual(raised.exception.cause, "correctness_failed")
        self.assertIn("TCR12", str(raised.exception))

    def test_reference_mismatch_is_visible_in_correctness_lane_only(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_op = {item["operation"]: item for item in evidence["operations"]}
        self.assertEqual(evidence["execution_mode"], "static_replay")
        self.assertEqual(by_op["TCR6"]["status"], "correctness_failed")
        self.assertIn("reference_mismatch", by_op["TCR6"]["cause"])
        self.assertEqual(by_op["TCR10"]["status"], "correctness_failed")
        self.assertIn("reference_mismatch", by_op["TCR10"]["cause"])
        self.assertEqual(by_op["TCR10"]["validation_mode"], "normalized")
        self.assertEqual(evidence["status"], "correctness_failed")
        # A correctness mismatch never leaks into the resource or harness lanes.
        self.assertEqual(evidence["resource_events"], [])
        self.assertEqual(evidence["harness_failures"], [])
        mismatch_out = (
            self.root
            / "fixtures"
            / "gdc"
            / "finbench-transaction-tiny"
            / "reference-mismatch"
            / "system-outputs"
            / "finbench-engineering-tiny-v1-TCR10.out"
        ).read_text(encoding="utf-8")
        self.assertEqual(mismatch_out.strip(), "0.500")
        self.assertNotIn("company-", mismatch_out)
        self._evidence_validator().validate(evidence)

    def test_correctness_resource_and_harness_failures_are_distinguished(self) -> None:
        """A single run separates correctness, resource, and harness failures."""
        src = self.root / "fixtures" / "gdc" / "finbench-transaction-tiny" / "compatible"
        with tempfile.TemporaryDirectory(prefix="finbench-lanes-") as tmp:
            work = Path(tmp)
            shutil.copytree(src / "jobs", work / "jobs")
            shutil.copytree(src / "references", work / "references")
            shutil.copytree(src / "system-outputs", work / "system-outputs")
            outputs = work / "system-outputs"

            # Correctness lane: corrupt TCR6's produced output.
            (outputs / "finbench-engineering-tiny-v1-TCR6.out").write_text(
                "acct-10 500\nacct-11 000\n", encoding="utf-8"
            )
            # Resource lane: TCR8 hits a resource limit (sidecar replaces output).
            (outputs / "finbench-engineering-tiny-v1-TCR8.out").unlink()
            (outputs / "finbench-engineering-tiny-v1-TCR8.limit").write_text(
                "rss_limit_exceeded\n", encoding="utf-8"
            )
            # Harness lane: TSR1's runner crashed (sidecar replaces output).
            (outputs / "finbench-engineering-tiny-v1-TSR1.out").unlink()
            (outputs / "finbench-engineering-tiny-v1-TSR1.harness").write_text(
                "driver_process_crashed\n", encoding="utf-8"
            )

            identities = work / "identities.json"
            identities.write_text("{}\n", encoding="utf-8")
            evidence_path = work / "evidence.json"
            completed = subprocess.run(
                [
                    str(self.binary),
                    "run-suite",
                    str(work / "jobs"),
                    str(work / "references"),
                    str(outputs),
                    str(identities),
                    str(evidence_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0, completed.stderr)
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))

        by_op = {item["operation"]: item for item in evidence["operations"]}
        # Each failure lands in its own distinct per-operation status.
        self.assertEqual(by_op["TCR6"]["status"], "correctness_failed")
        self.assertEqual(by_op["TCR8"]["status"], "resource_exceeded")
        self.assertEqual(by_op["TSR1"]["status"], "harness_error")

        # Each failure also lands only in its own dedicated evidence section.
        resource_ops = {item["operation"] for item in evidence["resource_events"]}
        harness_ops = {item["operation"] for item in evidence["harness_failures"]}
        self.assertEqual(resource_ops, {"TCR8"})
        self.assertEqual(harness_ops, {"TSR1"})
        # Correctness failures are never conflated into resource or harness lanes.
        self.assertNotIn("TCR6", resource_ops)
        self.assertNotIn("TCR6", harness_ops)
        # Resource and harness lanes stay mutually exclusive.
        self.assertFalse(resource_ops & harness_ops)
        # Harness error is the worst class and drives the overall status.
        self.assertEqual(evidence["status"], "harness_error")
        self._evidence_validator().validate(evidence)

    def test_index_and_readme_point_at_finbench_transaction_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`finbench-transaction`", index)
        self.assertIn("gdc-finbench-transaction", index)
        self.assertIn("## FinBench Transaction", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_finbench_transaction", readme)


if __name__ == "__main__":
    unittest.main()
