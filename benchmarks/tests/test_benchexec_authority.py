from __future__ import annotations

import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from graphforge_bench.benchexec_authority import (
    EvidenceError,
    Limits,
    Outcome,
    adapt_run_result,
    normalize_run,
    require_local_admission,
)
from graphforge_bench.tools.graphforge_certify import Tool
from jsonschema import Draft202012Validator
import tomllib

LIMITS = Limits(10.0, 8.0, 1024, (0, 1))
ADMISSION_MEASUREMENTS = {
    "walltime": 1.0,
    "cputime": 0.1,
    "memory": 1024,
    "blkio-read": 10,
    "blkio-write": 20,
    "pressure-cpu-some": 0.0,
    "pressure-io-some": 0.0,
    "pressure-memory-some": 0.0,
    "terminationreason": "walltime",
    "descendant_stopped": True,
}


def result(**changes):
    value = {
        "wall_seconds": 2.0,
        "cpu_seconds": 3.0,
        "peak_rss_bytes": 400,
        "read_bytes": 500,
        "write_bytes": 600,
        "pressure_cpu_seconds": 0.1,
        "pressure_io_seconds": 0.2,
        "pressure_memory_seconds": 0.3,
        "exit_code": 0,
        "correctness": True,
    }
    value.update(changes)
    return value


class BenchExecAuthorityTests(unittest.TestCase):
    def test_consumes_native_child_tree_admission_interface(self):
        measurements = dict(ADMISSION_MEASUREMENTS)
        admitted = require_local_admission(
            {
                "schema": "graphforge-local-admission-evidence/1",
                "result": "passed",
                "cause": None,
                "measurements": measurements,
            }
        )
        self.assertIs(admitted, measurements)
        with self.assertRaisesRegex(EvidenceError, "child-tree"):
            require_local_admission(
                {
                    "schema": "graphforge-local-admission-evidence/1",
                    "result": "passed",
                    "cause": None,
                    "measurements": ADMISSION_MEASUREMENTS | {"descendant_stopped": False},
                }
            )

    def test_passed_admission_requires_complete_finite_measurements(self):
        for key in ADMISSION_MEASUREMENTS:
            malformed = dict(ADMISSION_MEASUREMENTS)
            del malformed[key]
            with self.subTest(missing=key), self.assertRaises(EvidenceError):
                require_local_admission(
                    {
                        "schema": "graphforge-local-admission-evidence/1",
                        "result": "passed",
                        "cause": None,
                        "measurements": malformed,
                    }
                )
        for invalid in (math.nan, math.inf):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(EvidenceError, "walltime"):
                require_local_admission(
                    {
                        "schema": "graphforge-local-admission-evidence/1",
                        "result": "passed",
                        "cause": None,
                        "measurements": ADMISSION_MEASUREMENTS | {"walltime": invalid},
                    }
                )

    def test_definition_and_dependency_match_benchexec_api(self):
        benchmark_root = Path(__file__).resolve().parents[1]
        definition = ET.parse(benchmark_root / "definitions/graphforge-certification-v1.xml")
        self.assertEqual(int(definition.getroot().attrib["cpuCores"]), 16)
        project = tomllib.loads((benchmark_root / "pyproject.toml").read_text())
        self.assertIn("benchexec>=3.3,<4", project["project"]["dependencies"])

    def test_adapts_native_benchexec_process_tree_keys(self):
        class Exit:
            value = 0
            signal = None

        adapted = adapt_run_result(
            {
                "walltime": 1.0,
                "cputime": 1.5,
                "memory": 99,
                "blkio-read": 10,
                "blkio-write": 20,
                "pressure-cpu-some": 0.1,
                "pressure-io-some": 0.2,
                "pressure-memory-some": 0.3,
                "exitcode": Exit(),
            },
            correctness=True,
        )
        self.assertEqual(
            adapted,
            result(
                wall_seconds=1.0,
                cpu_seconds=1.5,
                peak_rss_bytes=99,
                read_bytes=10,
                write_bytes=20,
            )
            | {"termination_reason": None, "signal": None},
        )

    def test_tool_info_invokes_only_versioned_public_runner_shape(self):
        class Task:
            input_files_or_identifier = ("profile.json",)

        self.assertEqual(
            Tool().cmdline("graphforge-benchmark-certify", [], Task(), None),
            [
                "graphforge-benchmark-certify",
                "run",
                "profile.json",
                "evidence.json",
            ],
        )

    def test_tool_info_writes_certify_evidence_on_provider_work_volume(self):
        class Task:
            input_files_or_identifier = ("profile.json",)

        with (
            patch("graphforge_bench.tools.graphforge_certify.os.path.ismount", return_value=True),
            patch.object(Path, "is_dir", return_value=True),
            patch.object(Path, "mkdir"),
        ):
            self.assertEqual(
                Tool().cmdline("graphforge-benchmark-certify", [], Task(), None)[-1],
                "/work/tmp/graphforge-certify-evidence.json",
            )

    def test_tool_info_writes_certify_evidence_under_host_work_root(self):
        class Task:
            input_files_or_identifier = ("profile.json",)

        with (
            patch.dict(
                "graphforge_bench.tools.graphforge_certify.os.environ",
                {"GRAPHFORGE_HOST_WORK_ROOT": "/host/work"},
                clear=False,
            ),
            patch("graphforge_bench.tools.graphforge_certify.os.path.ismount", return_value=False),
            patch.object(Path, "mkdir"),
        ):
            self.assertEqual(
                Tool().cmdline("graphforge-benchmark-certify", [], Task(), None)[-1],
                "/host/work/tmp/graphforge-certify-evidence.json",
            )

    def test_preserves_tree_authority_and_graphforge_telemetry(self):
        evidence = normalize_run(
            benchexec=result(),
            graphforge={"status": "passed", "phases": [{"phase": "ingest", "duration_ms": 2000}]},
            limits=LIMITS,
        )
        self.assertEqual(evidence["outcome"], Outcome.PASSED)
        self.assertEqual(evidence["authority"]["cpu_seconds"], 3.0)
        self.assertEqual(evidence["limits"]["cores"], [0, 1])
        self.assertEqual(evidence["disagreements"], [])
        schema_path = Path(__file__).resolve().parents[1] / "schemas/benchexec-run-evidence.json"
        schema = json.loads(schema_path.read_text())
        Draft202012Validator(schema).validate(evidence)

    def test_typed_termination_outcomes_remain_distinct(self):
        cases = [
            ({"termination_reason": "walltime"}, Outcome.TIMEOUT),
            ({"termination_reason": "memory"}, Outcome.OOM),
            ({"exit_code": 7, "correctness": True}, Outcome.EXIT),
            ({"signal": 9, "exit_code": None, "correctness": True}, Outcome.SIGNAL),
            ({"termination_reason": "failed"}, Outcome.HARNESS),
            ({"correctness": False}, Outcome.CORRECTNESS),
        ]
        for changes, expected in cases:
            with self.subTest(expected=expected):
                evidence = normalize_run(
                    benchexec=result(**changes),
                    graphforge={
                        "status": "failed",
                        "phases": [{"phase": "query", "duration_ms": 2000}],
                    },
                    limits=LIMITS,
                )
                self.assertEqual(evidence["outcome"], expected)
                schema_path = (
                    Path(__file__).resolve().parents[1] / "schemas/benchexec-run-evidence.json"
                )
                schema = json.loads(schema_path.read_text())
                Draft202012Validator(schema).validate(evidence)

    def test_missing_authoritative_process_tree_field_fails_closed(self):
        malformed = result()
        del malformed["read_bytes"]
        with self.assertRaisesRegex(EvidenceError, "read_bytes"):
            normalize_run(
                benchexec=malformed,
                graphforge={
                    "status": "passed",
                    "phases": [{"phase": "export", "duration_ms": 2000}],
                },
                limits=LIMITS,
            )

        with self.assertRaisesRegex(EvidenceError, "limits"):
            normalize_run(
                benchexec=result(),
                graphforge={
                    "status": "passed",
                    "phases": [{"phase": "export", "duration_ms": 2000}],
                },
                limits=Limits(0, 8, 1024, (0,)),
            )

        for invalid in (math.nan, math.inf):
            with (
                self.subTest(invalid_limit=invalid),
                self.assertRaisesRegex(EvidenceError, "limits"),
            ):
                normalize_run(
                    benchexec=result(),
                    graphforge={
                        "status": "passed",
                        "phases": [{"phase": "export", "duration_ms": 2000}],
                    },
                    limits=Limits(invalid, 8, 1024, (0,)),
                )
            with (
                self.subTest(invalid_measurement=invalid),
                self.assertRaisesRegex(EvidenceError, "wall_seconds"),
            ):
                normalize_run(
                    benchexec=result(wall_seconds=invalid),
                    graphforge={
                        "status": "passed",
                        "phases": [{"phase": "export", "duration_ms": 2000}],
                    },
                    limits=LIMITS,
                )

    def test_schema_rejects_contradictory_outcome_process_status(self):
        schema_path = Path(__file__).resolve().parents[1] / "schemas/benchexec-run-evidence.json"
        validator = Draft202012Validator(json.loads(schema_path.read_text()))
        base = normalize_run(
            benchexec=result(),
            graphforge={"status": "passed", "phases": [{"phase": "ingest", "duration_ms": 2000}]},
            limits=LIMITS,
        )
        contradictions = [
            base | {"outcome": "passed", "exit_code": 7},
            base | {"outcome": "correctness", "exit_code": 7},
            base | {"outcome": "exit", "exit_code": 0},
            base | {"outcome": "signal", "exit_code": 0, "signal": 9},
            base | {"outcome": "timeout", "exit_code": 7},
            base | {"outcome": "oom", "signal": 9},
            base | {"outcome": "harness", "exit_code": 1},
        ]
        for evidence in contradictions:
            with self.subTest(evidence=evidence):
                self.assertTrue(list(validator.iter_errors(evidence)))

    def test_passed_local_admission_schema_requires_measurements(self):
        schema_path = Path(__file__).resolve().parents[1] / "schemas/local-admission-evidence.json"
        validator = Draft202012Validator(json.loads(schema_path.read_text()))
        evidence = {
            "schema": "graphforge-local-admission-evidence/1",
            "result": "passed",
            "cause": None,
            "facts": {
                "operating_system": "linux",
                "cgroups_version": 2,
                "required_controllers": True,
                "kernel_memory_accounting": True,
                "privileged_execution": False,
                "benchexec_cgroup_delegation": True,
                "namespace_isolation": True,
                "overlay_isolation": True,
            },
        }
        self.assertTrue(list(validator.iter_errors(evidence)))
        passed = evidence | {"measurements": ADMISSION_MEASUREMENTS}
        validator.validate(passed)
        invalid_facts = {
            "operating_system": "darwin",
            "cgroups_version": 1,
            "required_controllers": False,
            "kernel_memory_accounting": False,
            "privileged_execution": True,
            "benchexec_cgroup_delegation": False,
            "namespace_isolation": False,
            "overlay_isolation": False,
        }
        for name, value in invalid_facts.items():
            with self.subTest(name=name):
                invalid = passed | {"facts": passed["facts"] | {name: value}}
                self.assertTrue(list(validator.iter_errors(invalid)))

        for result in ("failed", "disqualified"):
            with self.subTest(result=result):
                invalid = evidence | {"result": result, "cause": None}
                self.assertTrue(list(validator.iter_errors(invalid)))

    def test_reports_status_and_wall_time_disagreement(self):
        evidence = normalize_run(
            benchexec=result(),
            graphforge={"status": "failed", "phases": [{"phase": "verify", "duration_ms": 9000}]},
            limits=LIMITS,
        )
        self.assertEqual(evidence["disagreements"], ["status", "wall_time"])


if __name__ == "__main__":
    unittest.main()
