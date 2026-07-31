#!/usr/bin/env python3
"""Mutation-sensitive tests for the final M22 non-Cypher surface gate."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/m22-non-cypher-surface-gate.py"
LOAD_TEST = ROOT / "scripts/ci/test-release-load-matrix.py"
WORKFLOW = ROOT / ".github/workflows/m22-non-cypher-surface-gate.yml"
SHA = "a" * 40


def import_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GATE = import_file("m22_surface_gate", SCRIPT)
LOAD_TESTS = import_file("m22_load_test_helpers", LOAD_TEST)


class M22SurfaceGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rust_run = {
            "id": 101,
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "success",
            "head_sha": SHA,
            "event": "workflow_dispatch",
            "path": ".github/workflows/non-cypher-surface-gate.yml",
            "repository": {"full_name": "CurateLabs/graphforge"},
            "html_url": "https://github.com/CurateLabs/graphforge/actions/runs/101",
        }
        self.binding_run = {
            "id": 102,
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "success",
            "head_sha": SHA,
            "event": "workflow_dispatch",
            "path": ".github/workflows/binding-release-candidate.yml",
            "repository": {"full_name": "CurateLabs/graphforge"},
            "html_url": "https://github.com/CurateLabs/graphforge/actions/runs/102",
        }

    def rejected_runs(self, expected: str, **changes) -> None:
        values = {
            "rust_run": copy.deepcopy(self.rust_run),
            "binding_run": copy.deepcopy(self.binding_run),
        }
        values.update(changes)
        with self.assertRaisesRegex(ValueError, expected):
            GATE.validate_component_runs(expected_sha=SHA, **values)

    def test_component_runs_require_exact_successful_manual_runs(self) -> None:
        result = GATE.validate_component_runs(
            self.rust_run,
            self.binding_run,
            SHA,
        )
        self.assertEqual(result["components"]["rust"]["cache_key"], "rust-non-cypher-" + SHA)
        for key, value, message in (
            ("status", "in_progress", "not completed"),
            ("conclusion", "failure", "not completed"),
            ("head_sha", "b" * 40, "SHA drift"),
            ("event", "push", "not manually"),
            ("path", ".github/workflows/test.yml", "workflow path"),
        ):
            run = copy.deepcopy(self.rust_run)
            run[key] = value
            self.rejected_runs(message, rust_run=run)
        boolean_attempt = copy.deepcopy(self.binding_run)
        boolean_attempt["run_attempt"] = True
        self.rejected_runs("run attempt", binding_run=boolean_attempt)

    def test_workflow_is_manual_exact_main_and_validates_before_building(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        header, jobs = text.split("jobs:\n", 1)
        self.assertIn("workflow_dispatch:", header)
        for forbidden in ("push:", "pull_request:", "schedule:"):
            self.assertNotIn(forbidden, header)
        self.assertIn("group: m22-non-cypher-${{ inputs.commit_sha }}", header)
        self.assertIn("cancel-in-progress: true", header)
        self.assertNotIn("gh workflow run", text)
        validate = jobs.index("  validate_source:")
        load = jobs.index("  load:")
        validation_failure = jobs.index("  validation_failure:")
        aggregate = jobs.index("  aggregate:")
        self.assertLess(validate, validation_failure)
        self.assertLess(validation_failure, load)
        self.assertLess(load, aggregate)
        validation_job = jobs[validate:load]
        self.assertIn("ref: main", validation_job)
        self.assertIn('test "$REQUESTED_SHA" = "$main_sha"', validation_job)
        self.assertIn("validate-runs", validation_job)
        self.assertNotIn("cargo build", validation_job)
        self.assertNotIn("maturin-action", validation_job)
        failure_job = jobs[validation_failure:load]
        self.assertIn("if: always() && needs.validate_source.result == 'failure'", failure_job)
        self.assertIn('else "invalid-sha"', failure_job)
        self.assertNotIn("upload-artifact", failure_job)
        load_job = jobs[load:aggregate]
        self.assertIn("needs: validate_source", load_job)
        self.assertIn("release-load-matrix.py run", load_job)
        self.assertIn("useblacksmith/stickydisk@v1", load_job)
        self.assertIn(
            "${{ github.repository }}-m22-load-${{ inputs.commit_sha }}-target-v3",
            load_job,
        )
        self.assertIn("useblacksmith/stickydisk-delete@v1", load_job)
        self.assertIn("needs: load", load_job)
        self.assertIn("CARGO_TARGET_DIR: ${{ github.workspace }}/target", load_job)
        self.assertIn("Reclaim sticky-disk ownership after maturin", load_job)
        self.assertIn(
            'sudo chown -R "$(id -u):$(id -g)" "$CARGO_TARGET_DIR"',
            load_job,
        )
        self.assertIn("Reclaim root-disk headroom before the matrix", load_job)
        self.assertIn("TMPDIR: ${{ runner.temp }}/graphforge-m22-load-tmp", load_job)
        self.assertNotIn("graphforge-m22-load-target", load_job)
        maturin_step = load_job.index("- name: Build one exact-SHA Python wheel")
        reclaim_step = load_job.index("- name: Reclaim sticky-disk ownership after maturin")
        wrapper_step = load_job.index(
            "- name: Prepare Rust compiler wrapper for native load contracts"
        )
        artifact_step = load_job.index("- name: Prepare native load artifacts")
        self.assertLess(maturin_step, reclaim_step)
        self.assertLess(reclaim_step, wrapper_step)
        self.assertLess(wrapper_step, artifact_step)
        self.assertIn("prepare-rustc-wrapper.py", load_job[wrapper_step:artifact_step])
        self.assertNotIn("cargo build", load_job[wrapper_step:artifact_step])
        load_matrix_step = load_job.index("- name: Execute fail-closed release load matrix")
        artifact_build = load_job[artifact_step:load_matrix_step]
        self.assertIn('test "$CARGO_TARGET_DIR" = "$GITHUB_WORKSPACE/target"', artifact_build)
        rust_build = artifact_build.index("cargo build")
        node_build = artifact_build.index("napi build")
        self.assertLess(artifact_build.index("mkdir -p"), rust_build)
        self.assertLess(rust_build, node_build)
        final_job = jobs[aggregate:]
        self.assertIn("Revalidate current main and component artifacts", final_job)
        self.assertIn("actions/cache/restore@v6", final_job)

    def rust_report(self) -> dict:
        inventory = GATE.load_json(GATE.SURFACE)
        evidence = []
        for group_name, group in inventory["method_evidence_groups"].items():
            test_ids = [ref["symbol"] for ref in group["test_refs"]]
            evidence.extend(
                {
                    "kind": "public_method",
                    "identity": identity,
                    "evidence_group": group_name,
                    "test_ids": test_ids,
                    "outcome": "passed",
                    "error_code": None,
                }
                for identity in group["ids"]
            )
        m18 = inventory["m18_registry"]["release-tested"]
        evidence.extend(
            {
                "kind": "m18_registry",
                "identity": identity,
                "test_ids": [ref["symbol"] for ref in m18["test_refs"]],
                "outcome": "passed",
                "error_code": None,
            }
            for identity in m18["ids"]
        )
        for group_name, group in inventory["m19_evidence_groups"].items():
            test_ids = [ref["symbol"] for ref in group["test_refs"]]
            evidence.extend(
                {
                    "kind": "m19_contracts",
                    "identity": identity,
                    "evidence_group": group_name,
                    "test_ids": test_ids,
                    "outcome": "passed",
                    "error_code": None,
                }
                for identity in group["ids"]
            )
        names = {
            "knowledge_isolation",
            "public_lifecycle_conformance",
            "m22_m18_public_surface",
            "m22_m19_public_surface",
            "m22_provider_public_surface",
            "provider_session",
            "public_facade_remaining_conformance",
        }
        return {
            "schema": GATE.RUST_SCHEMA,
            "source_sha": SHA,
            "status": "passed",
            "inventory_sha256": GATE.digest(GATE.SURFACE),
            "evidence": evidence,
            "test_binary_sha256": dict.fromkeys(names, "b" * 64),
            "commands": [
                "cargo test -p gf-api --lib --no-fail-fast",
                "cargo test -p gf-api --test knowledge_isolation --test "
                "public_lifecycle_conformance --test public_facade_remaining_conformance "
                "--test m22_m18_public_surface --test m22_m19_public_surface --test "
                "m22_provider_public_surface --test provider_session --no-fail-fast",
            ],
        }

    def binding_report(self) -> dict:
        validator = GATE.import_script(
            "m22_test_binding", ROOT / "scripts/ci/validate-binding-release-candidate.py"
        )
        contract = GATE.load_json(GATE.BINDING_TARGETS)
        reports = []
        for target, settings in contract["targets"].items():
            mode = settings["execution_mode"]
            reports.append(
                {
                    "schema": "graphforge-binding-rc-target/1",
                    "source_sha": SHA,
                    "language": settings["language"],
                    "target": target,
                    "package_version": "0.5.0",
                    "artifact": {"name": "artifact", "sha256": "c" * 64},
                    "classification": {
                        "name": "policy",
                        "sha256": "d" * 64,
                        "schema": 1
                        if settings["language"] == "node"
                        else "graphforge-python-non-cypher-parity/1",
                    },
                    "execution": {
                        "mode": mode,
                        "rationale": "incompatible host" if mode == "package-validation" else None,
                    },
                    "fallback_execution": False,
                    "cases": [
                        {"identity": "contract", "outcome": "passed", "sanitized_error": None}
                    ],
                    "sanitized_parity_diff": [],
                }
            )
        return validator.validate(reports, contract, SHA)

    def load_report(self, root: Path) -> dict:
        matrix = LOAD_TESTS.GATE.load(LOAD_TESTS.GATE.MATRIX)
        _source, selectors = LOAD_TESTS.GATE.inventory(matrix)
        workloads = {item["id"]: item for item in matrix["workloads"]}
        fixtures, reports = root / "fixtures", root / "reports"
        manifests = LOAD_TESTS.GATE.generate(fixtures)
        reports.mkdir()
        helper = LOAD_TESTS.ReleaseLoadMatrixTests()
        for identity in sorted(LOAD_TESTS.GATE.expected_cases(matrix, manifests)):
            _language, workload, dataset = identity.split("/", 2)
            covered = sorted(
                set().union(
                    *(selectors[name] for name in workloads[workload]["inventory_selectors"])
                )
            )
            manifest = next(item for item in manifests if item["dataset_id"] == dataset)
            report = helper.report(identity, SHA, manifest["content_sha256"], covered)
            (reports / (identity.replace("/", "--") + ".json")).write_text(
                json.dumps(report), encoding="utf-8"
            )
        return LOAD_TESTS.GATE.aggregate(reports, fixtures, SHA, root / "load.json")

    def test_composite_rejects_component_and_cross_component_drift(self) -> None:
        rust = self.rust_report()
        binding = self.binding_report()
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            load = self.load_report(root)
            paths = {}
            for name, value in (("rust", rust), ("binding", binding), ("load", load)):
                path = root / f"{name}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                paths[name] = path
            run_validation = GATE.validate_component_runs(
                self.rust_run,
                self.binding_run,
                SHA,
            )
            run_path = root / "runs.json"
            run_path.write_text(json.dumps(run_validation), encoding="utf-8")
            result = GATE.aggregate(
                paths["rust"], paths["binding"], paths["load"], run_path, SHA, 303
            )
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["components"]["load"]["summary"]["case_count"], 144)

            mutations = []
            bad_rust = copy.deepcopy(rust)
            bad_rust["evidence"].pop()
            mutations.append(("rust", bad_rust, "identity ledger"))
            bad_rust_tests = copy.deepcopy(rust)
            bad_rust_tests["evidence"][0]["test_ids"] = ["wrong_test"]
            mutations.append(("rust", bad_rust_tests, "did not pass"))
            malformed_rust = copy.deepcopy(rust)
            malformed_rust["evidence"][0] = "not-an-object"
            mutations.append(("rust", malformed_rust, "evidence is missing"))
            bad_rust_commands = copy.deepcopy(rust)
            bad_rust_commands["commands"][0] = "cargo test --workspace"
            mutations.append(("rust", bad_rust_commands, "command ledger"))
            bad_binding = copy.deepcopy(binding)
            bad_binding["targets"][0]["fallback_execution"] = True
            mutations.append(("binding", bad_binding, "fallback"))
            bad_load = copy.deepcopy(load)
            bad_load["cases"].pop()
            mutations.append(("load", bad_load, "case ledger"))
            retried_load = copy.deepcopy(load)
            retried_load["cases"][0]["attempt"] = 2
            mutations.append(("load", retried_load, "retried"))
            parity_load = copy.deepcopy(load)
            parity_load["cases"][0]["parity_diff"] = ["rows"]
            mutations.append(("load", parity_load, "parity drift"))
            malformed_load = copy.deepcopy(load)
            malformed_load["cases"][0] = "not-an-object"
            mutations.append(("load", malformed_load, "case ledger is missing"))
            for component, mutated, message in mutations:
                paths[component].write_text(json.dumps(mutated), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, message):
                    GATE.aggregate(
                        paths["rust"], paths["binding"], paths["load"], run_path, SHA, 303
                    )
                paths[component].write_text(
                    json.dumps({"rust": rust, "binding": binding, "load": load}[component]),
                    encoding="utf-8",
                )
            tampered_runs = copy.deepcopy(run_validation)
            tampered_runs["components"]["rust"]["run_url"] = "https://example.invalid/run"
            run_path.write_text(json.dumps(tampered_runs), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "provenance drift"):
                GATE.aggregate(paths["rust"], paths["binding"], paths["load"], run_path, SHA, 303)


if __name__ == "__main__":
    unittest.main()
