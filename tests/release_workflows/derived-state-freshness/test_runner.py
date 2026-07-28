import ast
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

BUNDLE = Path(__file__).resolve().parent
ROOT = BUNDLE.parents[2]
SPEC = importlib.util.spec_from_file_location("derived_state_runner", BUNDLE / "run.py")
assert SPEC and SPEC.loader
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
SHA = "1" * 40


def rust_evidence() -> dict[str, object]:
    arrow = json.loads((BUNDLE / "expected/arrow-fingerprints.json").read_text())
    return {
        "scenario_id": "derived-state-freshness",
        "slice": "authoritative-rust",
        "text": RUNNER.TRANSITIONS["text"],
        "text_results": {
            name: {"schema": arrow["schemas"][name], "rows": rows}
            for name, rows in arrow["results"].items()
        },
        "adjacency": RUNNER.TRANSITIONS["adjacency"],
        "cancellation_code": "GF_CANCELLED",
        "prior_authority_preserved": True,
        "analysis": {
            "authoritative_vector_uuids": ["per-run"],
            "property_correction_reanalyzed": True,
        },
        "embeddings": {
            "compatibility_ids": ["a" * 64, "b" * 64],
            "states": RUNNER.TRANSITIONS["embedding"],
            "exact_replay": True,
            "incompatible_code": "GF_VALIDATION",
            "prior_authority_preserved": True,
        },
        "hypothesis": {"exact_snapshot_equal": True},
        "transaction_time_view": {"cutoff": 9223372036854775807, "exact_snapshot_equal": True},
        "ontology_constant": True,
        "reopen_equal": True,
    }


def binding_evidence(root: Path, binding: str) -> dict[str, object]:
    name = "_graphforge_rs.abi3.so" if binding == "python" else "graphforge.darwin-arm64.node"
    native = root / name
    native.write_bytes(binding.encode())
    return {
        "schema_version": 1,
        "scenario_id": "derived-state-freshness",
        "binding": binding,
        "commit_sha": SHA,
        "text_states": RUNNER.TRANSITIONS["text"],
        "adjacency_states": RUNNER.TRANSITIONS["adjacency"],
        "compatibility_ids": ["a" * 64, "b" * 64],
        "generation_ids": ["c" * 64, "d" * 64],
        "embedding_states": RUNNER.TRANSITIONS["embedding"],
        "reopen_equal": True,
        "package_version": RUNNER.PACKAGE_VERSIONS[binding],
        "native_version": "0.5.0-dev",
        "native_module_path": str(native),
        "native_module_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
    }


class RunnerContract(unittest.TestCase):
    def test_parent_bundle_maps_registry_feature_and_scenario_one_to_one(self) -> None:
        required = [
            "README.md",
            "workflow.feature",
            "scenario.yaml",
            "generator.yaml",
            "ontologies/strict-v1.yaml",
            "ontologies/phase-manifest.json",
            "expected/arrow-fingerprints.json",
            "expected/errors.json",
            "expected/evidence-schema.json",
            "expected/phases.json",
        ]
        self.assertEqual([], [name for name in required if not (BUNDLE / name).is_file()])
        scenario = json.loads((BUNDLE / "scenario.yaml").read_text())
        generator = json.loads((BUNDLE / "generator.yaml").read_text())
        registry = json.loads((ROOT / "tests/release_workflows/registry-v1.json").read_text())
        row = next(item for item in registry["scenarios"] if item["id"] == scenario["scenario_id"])
        feature_steps = re.findall(
            r"^\s*(?:Given|When|Then|And|But)\s+\[(DSF-\d{2})\]",
            (BUNDLE / "workflow.feature").read_text(),
            re.MULTILINE,
        )
        manifest_steps = [step["id"] for step in scenario["steps"]]
        self.assertEqual([f"DSF-{number:02d}" for number in range(1, 12)], feature_steps)
        self.assertEqual(feature_steps, manifest_steps)
        self.assertEqual(feature_steps, scenario["registry"]["steps"])
        self.assertEqual(feature_steps, row["steps"])
        generator_hash = hashlib.sha256((BUNDLE / "generator.yaml").read_bytes()).hexdigest()
        self.assertEqual(generator["seed"], scenario["generator"]["seed"])
        self.assertEqual(generator_hash, row["generator"]["sha256"])
        self.assertEqual(f"sha256:{generator_hash}", scenario["generator"]["fixture_fingerprint"])
        self.assertEqual(2470, row["owning_issue"])
        self.assertIn(
            "--output target/release-workflow-evidence/derived-state-freshness.json",
            scenario["local_command"],
        )

    def test_expected_evidence_and_constant_ontology_are_closed(self) -> None:
        schema = json.loads((BUNDLE / "expected/evidence-schema.json").read_text())
        errors = json.loads((BUNDLE / "expected/errors.json").read_text())
        phases = json.loads((BUNDLE / "expected/phases.json").read_text())
        manifest = json.loads((BUNDLE / "ontologies/phase-manifest.json").read_text())
        ontology_hash = hashlib.sha256(
            (BUNDLE / "ontologies/strict-v1.yaml").read_bytes()
        ).hexdigest()
        self.assertEqual(ontology_hash, manifest["sha256"])
        self.assertEqual(phases["phases"], manifest["phases"])
        self.assertEqual(
            [
                {
                    "step_id": "DSF-04",
                    "code": "GF_CANCELLED",
                    "publication": "none",
                    "prior_authority": "unchanged",
                },
                {
                    "step_id": "DSF-08",
                    "code": "GF_VALIDATION",
                    "publication": "none",
                    "prior_authority": "unchanged",
                },
            ],
            errors["structured_failures"],
        )
        self.assertEqual(
            {"schema_version": 1, "scenario_id": "derived-state-freshness"},
            schema["fixed"],
        )
        self.assertEqual(
            {
                "schema_version",
                "scenario_id",
                "commit_sha",
                "barrier_test",
                "generator_sha256",
                "ontology_sha256",
                "transitions",
                "rust",
                "bindings",
            },
            set(schema["canonical_required"]),
        )
        self.assertEqual(
            {
                "schema_version",
                "scenario_id",
                "commit_sha",
                "rust",
                "python",
                "node",
                "wheel_sha256",
            },
            set(schema["observation_required"]),
        )
        self.assertEqual(
            {
                "seed": ["ontology_constant"],
                "baseline-derived-state": [
                    "text",
                    "text_results.baseline",
                    "adjacency",
                    "embeddings",
                    "analysis",
                    "hypothesis",
                ],
                "topology-mutation": ["adjacency.stale"],
                "adjacency-refresh": [
                    "cancellation_code",
                    "prior_authority_preserved",
                    "adjacency.current",
                ],
                "indexed-text-mutation": ["text.stale"],
                "text-refresh": ["text.current", "text_results.refreshed"],
                "property-correction": [
                    "analysis.authoritative_vector_uuids",
                    "transaction_time_view.exact_snapshot_equal",
                ],
                "vector-correction": ["embeddings.compatibility_ids", "embeddings.exact_replay"],
                "incompatible-vector-rejection": [
                    "embeddings.incompatible_code",
                    "embeddings.prior_authority_preserved",
                ],
                "reopen": ["reopen_equal"],
                "binding-parity": ["bindings.python", "bindings.node"],
            },
            phases["evidence"],
        )

    def test_bundle_and_finite_step_ledger_are_complete(self) -> None:
        required = [
            "binding_workflow.py",
            "binding_workflow.mjs",
            "run.py",
            "test_runner.py",
            "ontologies/strict-v1.yaml",
        ]
        self.assertEqual([], [name for name in required if not (BUNDLE / name).is_file()])
        source = (BUNDLE / "run.py").read_text()
        steps = [
            "test_runner.py",
            RUNNER.BARRIER,
            "derived_state_freshness_workflow",
            "binding_workflow.py",
            "binding_workflow.mjs",
        ]
        self.assertEqual(steps, [step for step in steps if source.count(step) == 1])

    def test_contract_declares_all_bindings_and_expected_failures(self) -> None:
        source = (BUNDLE / "run.py").read_text()
        self.assertIn('"cancellation_code": "GF_CANCELLED"', source)
        self.assertIn('"incompatible_code": "GF_VALIDATION"', source)
        self.assertEqual(
            {
                "text": ["current", "stale", "current"],
                "adjacency": ["current", "stale", "current"],
                "embedding": ["fresh", "fresh"],
            },
            RUNNER.TRANSITIONS,
        )

    def test_every_subprocess_is_bounded(self) -> None:
        tree = ast.parse((BUNDLE / "run.py").read_text())
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        direct = [
            node
            for node in calls
            if isinstance(node.func, ast.Attribute) and node.func.attr in {"run", "check_output"}
        ]
        self.assertEqual(3, len(direct))
        for call in direct:
            timeout = next(
                (keyword.value for keyword in call.keywords if keyword.arg == "timeout"), None
            )
            self.assertIsInstance(timeout, ast.Name)
            self.assertEqual("TIMEOUT", timeout.id)

    def test_cargo_timeouts_are_distinct_and_attributable(self) -> None:
        commands = [
            ["cargo", "test", "-p", "gf-api", "--lib", RUNNER.BARRIER, "--", "--exact"],
            ["cargo", "run", "-p", "gf-api", "--example", RUNNER.RUST_EXAMPLE.stem],
        ]
        messages = []
        for command in commands:
            with (
                self.subTest(command=command),
                mock.patch.object(
                    RUNNER.subprocess,
                    "run",
                    side_effect=subprocess.TimeoutExpired(command, RUNNER.TIMEOUT),
                ),
            ):
                with self.assertRaises(SystemExit) as raised:
                    RUNNER.execute(command, {})
                message = str(raised.exception)
                self.assertIn(f"after {RUNNER.TIMEOUT}s", message)
                self.assertIn(" ".join(command), message)
                messages.append(message)
        self.assertNotEqual(messages[0], messages[1])

    def test_barrier_invocation_is_exact_and_private(self) -> None:
        self.assertEqual(
            "search_index::tests::adjacency_barrier_cancellation_and_follow_on_rebuild_are_deterministic",
            RUNNER.BARRIER,
        )
        source = (BUNDLE / "run.py").read_text()
        self.assertIn(
            '["cargo", "test", "-p", "gf-api", "--lib", BARRIER, "--", "--exact"]',
            source,
        )

    def test_evidence_binds_generator_and_ontology_fingerprints(self) -> None:
        source = (BUNDLE / "run.py").read_text()
        self.assertIn('"generator_sha256": digest(GENERATOR)', source)
        self.assertIn('"ontology_sha256": digest(BUNDLE / "ontologies/strict-v1.yaml")', source)

    def test_binding_schema_is_closed_and_rejects_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            valid = binding_evidence(root, "python")
            summary = RUNNER.validate_binding(valid, "python", SHA, root)
            self.assertEqual("_graphforge_rs.abi3.so", summary["native_module"])
            mutations = []
            missing = copy.deepcopy(valid)
            missing.pop("schema_version")
            mutations.append(missing)
            extra = copy.deepcopy(valid)
            extra["unexpected"] = True
            mutations.append(extra)
            for key, value in [
                ("schema_version", True),
                ("scenario_id", "other"),
                ("package_version", "0.0.0"),
                ("native_version", "0.0.0"),
                ("compatibility_ids", ["bad", "bad"]),
                ("generation_ids", ["c" * 64, "c" * 64]),
                ("embedding_states", ["fresh", "stale"]),
                ("native_module_sha256", "0" * 64),
            ]:
                changed = copy.deepcopy(valid)
                changed[key] = value
                mutations.append(changed)
            for record in mutations:
                with self.subTest(record=record), self.assertRaises(ValueError):
                    RUNNER.validate_binding(record, "python", SHA, root)

    def test_rust_embedding_states_are_required_and_fixed(self) -> None:
        valid = rust_evidence()
        self.assertEqual(RUNNER.TRANSITIONS, RUNNER.validate_rust(valid)["transitions"])
        missing = copy.deepcopy(valid)
        missing["embeddings"].pop("states")
        drift = copy.deepcopy(valid)
        drift["embeddings"]["states"] = ["fresh", "stale"]
        for record in [missing, drift]:
            with self.assertRaises(ValueError):
                RUNNER.validate_rust(record)

    def test_rust_text_arrow_and_transaction_time_evidence_fail_closed(self) -> None:
        valid = rust_evidence()
        summary = RUNNER.validate_rust(valid)
        expected = json.loads((BUNDLE / "expected/arrow-fingerprints.json").read_text())
        self.assertEqual(expected["result_sha256"], summary["text_results"]["result_sha256"])
        wrong_value = copy.deepcopy(valid)
        wrong_value["text_results"]["refreshed"]["rows"][0]["name"] = "stale"
        wrong_schema = copy.deepcopy(valid)
        wrong_schema["text_results"]["baseline"]["schema"] = []
        wrong_cutoff = copy.deepcopy(valid)
        wrong_cutoff["transaction_time_view"]["cutoff"] = 0
        empty_analysis = copy.deepcopy(valid)
        empty_analysis["analysis"]["authoritative_vector_uuids"] = []
        false_reanalysis = copy.deepcopy(valid)
        false_reanalysis["analysis"]["property_correction_reanalyzed"] = False
        false_hypothesis = copy.deepcopy(valid)
        false_hypothesis["hypothesis"]["exact_snapshot_equal"] = False
        extra_nested = copy.deepcopy(valid)
        extra_nested["text_results"]["baseline"]["unexpected"] = True
        for record in [
            wrong_value,
            wrong_schema,
            wrong_cutoff,
            empty_analysis,
            false_reanalysis,
            false_hypothesis,
            extra_nested,
        ]:
            with self.assertRaises(ValueError):
                RUNNER.validate_rust(record)

    def test_rust_stdout_failures_are_attributable(self) -> None:
        scenario = RUNNER.RUST_EXAMPLE.stem
        for stdout, message in [
            ("", f"{scenario} produced no evidence on stdout"),
            ("build output\nnot-json\n", f"{scenario} emitted non-JSON evidence"),
            ("[]\n", f"{scenario} evidence must be a JSON object"),
        ]:
            with self.subTest(stdout=stdout), self.assertRaisesRegex(SystemExit, message):
                RUNNER.parse_rust_evidence(stdout)

    def test_canonical_bytes_repeat_while_observations_change(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rust = RUNNER.validate_rust(rust_evidence())
            binding_root = root / "artifacts"
            binding_root.mkdir()
            summaries = [
                RUNNER.validate_binding(
                    binding_evidence(binding_root, name), name, SHA, binding_root
                )
                for name in ["python", "node"]
            ]
            first, second = root / "first.json", root / "second.json"
            first_observations = {
                "rust": {"run_id": "a" * 64},
                "python": {"run_id": "a" * 64},
                "node": {"run_id": "a" * 64},
                "wheel_sha256": "a" * 64,
            }
            second_observations = {
                "rust": {"run_id": "b" * 64},
                "python": {"run_id": "b" * 64},
                "node": {"run_id": "b" * 64},
                "wheel_sha256": "b" * 64,
            }
            RUNNER.write_evidence(first, SHA, rust, summaries, first_observations)
            RUNNER.write_evidence(second, SHA, rust, summaries, second_observations)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            first_observed = first.with_name("first-observations.json").read_bytes()
            second_observed = second.with_name("second-observations.json").read_bytes()
            schema = json.loads((BUNDLE / "expected/evidence-schema.json").read_text())
            self.assertEqual(set(schema["canonical_required"]), set(json.loads(first.read_text())))
            self.assertEqual(set(schema["observation_required"]), set(json.loads(first_observed)))
            self.assertNotEqual(first_observed, second_observed)
            self.assertEqual(SHA, json.loads(first.read_text())["commit_sha"])


if __name__ == "__main__":
    unittest.main()
