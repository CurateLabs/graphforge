#!/usr/bin/env python3
"""Mutation-sensitive tests for the authoritative gate registry (#1009)."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ci" / "gate-registry.py"
SPEC = importlib.util.spec_from_file_location("gate_registry", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class GateRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = GATE.load_registry()

    def rejected(self, expected: str, registry: dict[str, object]) -> None:
        with self.assertRaisesRegex(GATE.RegistryError, expected):
            GATE.validate_registry(registry)

    def test_checked_in_registry_is_complete(self) -> None:
        GATE.validate_registry(self.registry)
        paths = {item["path"] for item in self.registry["workflows"]}
        actual = {
            path.relative_to(ROOT).as_posix()
            for suffix in ("*.yml", "*.yaml")
            for path in (ROOT / ".github" / "workflows").glob(suffix)
        }
        self.assertEqual(paths, actual)

    def test_new_or_stale_workflow_fails_closed(self) -> None:
        mutated = copy.deepcopy(self.registry)
        mutated["workflows"].pop()
        self.rejected("workflow inventory mismatch", mutated)

    def test_missing_registered_workflow_is_a_registry_error(self) -> None:
        mutated = copy.deepcopy(self.registry)
        mutated["workflows"][0]["path"] = ".github/workflows/does-not-exist.yml"
        self.rejected("workflow file is unavailable", mutated)

    def test_required_check_is_exact_head_ci_gate(self) -> None:
        mutated = copy.deepcopy(self.registry)
        test_suite = next(item for item in mutated["workflows"] if item["id"] == "test-suite")
        test_suite["sha_rule"] = "event_sha"
        self.rejected("required PR checks must bind", mutated)

    def test_costly_qualification_cannot_bypass_esc_operator(self) -> None:
        mutated = copy.deepcopy(self.registry)
        fly = next(
            item for item in mutated["operator_gates"] if item["id"] == "fly-tiny-qualification"
        )
        fly["command"] = "native-admission"
        self.rejected("bypasses the Python operator", mutated)

    def test_matrix_variants_share_one_command_definition(self) -> None:
        mutated = copy.deepcopy(self.registry)
        concurrency = next(
            item for item in mutated["workflows"] if item["id"] == "concurrency-stress"
        )
        concurrency["command"] = "repository-policy"
        self.rejected("must share matrix-gate", mutated)

    def test_matrix_dispatch_map_is_registry_owned(self) -> None:
        mutated = copy.deepcopy(self.registry)
        mutated["matrix_variants"]["concurrency/stress"] = "scripts/ci/repo-checks.sh"
        self.rejected("matrix variants", mutated)

    def test_publication_verification_has_one_owner(self) -> None:
        mutated = copy.deepcopy(self.registry)
        clean = next(item for item in mutated["workflows"] if item["id"] == "clean-environment")
        clean["owner"] = "ci"
        self.rejected("one release owner", mutated)

    def test_command_rendering_uses_registry_argv(self) -> None:
        self.assertEqual(
            GATE.command_argv(self.registry, "progressive-ladder"),
            [
                sys.executable,
                "-m",
                "graphforge_bench.qualification_operator",
                "run",
                "--gate",
                "progressive-ladder",
            ],
        )

    def test_matrix_dispatch_uses_current_python_and_shared_definition(self) -> None:
        completed = subprocess.CompletedProcess((), 0)
        with patch.object(GATE.subprocess, "run", return_value=completed) as run:
            self.assertEqual(
                GATE.main(
                    [
                        "matrix",
                        "--family",
                        "concurrency",
                        "--variant",
                        "stress",
                        "--",
                        "--output",
                        "/tmp/evidence",
                    ]
                ),
                0,
            )
        run.assert_called_once_with(
            [
                sys.executable,
                "scripts/ci/concurrency-stress-gate.py",
                "run",
                "--output",
                "/tmp/evidence",
            ],
            cwd=ROOT,
            check=False,
        )

    def test_operator_run_uses_invoking_python_and_explicit_harness_path(self) -> None:
        completed = subprocess.CompletedProcess((), 0)
        with patch.object(GATE.subprocess, "run", return_value=completed) as run:
            self.assertEqual(
                GATE.main(
                    [
                        "run",
                        "fly-tiny-qualification",
                        "--",
                        "--environment",
                        "curatelabs/graphforge/qualification",
                        "--expected-sha",
                        "a" * 40,
                        "--execute",
                        "--confirm-disposable",
                    ]
                ),
                0,
            )
        argv = run.call_args.args[0]
        environment = run.call_args.kwargs["env"]
        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(
            environment["PYTHONPATH"].split(GATE.os.pathsep)[0],
            str(ROOT / "benchmarks" / "harness"),
        )

    def test_run_rejects_registry_owned_gate_override(self) -> None:
        with patch.object(GATE.subprocess, "run") as run:
            self.assertEqual(
                GATE.main(
                    [
                        "run",
                        "fly-tiny-qualification",
                        "--",
                        "--gate=progressive-ladder",
                    ]
                ),
                2,
            )
        run.assert_not_called()

    def test_matrix_rejects_registry_owned_variant_override(self) -> None:
        with patch.object(GATE.subprocess, "run") as run:
            self.assertEqual(
                GATE.main(
                    [
                        "matrix",
                        "--family",
                        "concurrency",
                        "--variant",
                        "stress",
                        "--",
                        "--variant=certification",
                    ]
                ),
                2,
            )
        run.assert_not_called()

    def test_run_executes_python_commands_under_the_invoking_interpreter(self) -> None:
        """A literal `python3` drops the lane's virtualenv and its installed wheel (#1671)."""
        self.assertEqual(GATE.command_argv(self.registry, "concurrency-stress")[0], "python3")
        completed = subprocess.CompletedProcess((), 0)
        with patch.object(GATE.subprocess, "run", return_value=completed) as run:
            self.assertEqual(GATE.main(["run", "concurrency-stress", "--", "--seed", "1"]), 0)
        argv = run.call_args.args[0]
        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(argv[1:3], ["scripts/ci/gate-registry.py", "matrix"])
        self.assertEqual(argv[-2:], ["--seed", "1"])

    def test_operator_gates_have_no_actions_wrapper(self) -> None:
        """Operator gates run through the registry command, not an Actions handoff (#1671)."""
        operators = self.registry["operator_gates"]
        self.assertEqual(
            {item["id"] for item in operators},
            {
                "fly-tiny-qualification",
                "fly-tiny-recovery",
                "native-local-admission",
                "progressive-ladder",
            },
        )
        for item in operators:
            self.assertNotIn("path", item)
            self.assertTrue(GATE.command_argv(self.registry, item["id"]))

    def test_documented_fly_gate_id_renders(self) -> None:
        rendered = GATE.command_argv(self.registry, "fly-tiny-qualification")
        self.assertEqual(rendered[-2:], ["--gate", "fly-tiny"])


if __name__ == "__main__":
    unittest.main()
