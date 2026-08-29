from __future__ import annotations

import json
from pathlib import Path
import unittest

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

LIMITS = Limits(10.0, 8.0, 1024, (0, 1))


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
        measurements = result() | {
            "descendant_stopped": True,
            "walltime": 1.0,
        }
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
                    "measurements": {"descendant_stopped": False},
                }
            )

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

    def test_reports_status_and_wall_time_disagreement(self):
        evidence = normalize_run(
            benchexec=result(),
            graphforge={"status": "failed", "phases": [{"phase": "verify", "duration_ms": 9000}]},
            limits=LIMITS,
        )
        self.assertEqual(evidence["disagreements"], ["status", "wall_time"])


if __name__ == "__main__":
    unittest.main()
