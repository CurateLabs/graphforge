from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.local_admission import CommandResult, exit_code, qualify_local_host
from graphforge_bench.local_admission_fixture import _parse_runexec_value
from graphforge_bench.validate_local_admission import validate


class LocalAdmissionTests(unittest.TestCase):
    def test_delegated_preflight_uses_system_benchexec_interpreter(self) -> None:
        wrapper = (
            Path(__file__).parents[1] / "scripts/run-delegated-local-admission.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("/usr/bin/python3 -m benchexec.check_cgroups", wrapper)

    def test_runexec_cli_measurements_are_typed(self) -> None:
        self.assertEqual(_parse_runexec_value("1.25s"), 1.25)
        self.assertEqual(_parse_runexec_value("4096B"), 4096)
        self.assertEqual(_parse_runexec_value("0.003s"), 0.003)
        self.assertEqual(_parse_runexec_value("walltime"), "walltime")

    @staticmethod
    def _linux_roots(directory: str) -> tuple[Path, Path]:
        root = Path(directory)
        cgroup = root / "cgroup"
        proc = root / "proc"
        cgroup.mkdir()
        (cgroup / "cgroup.controllers").write_text("cpu io memory", encoding="utf-8")
        namespace = proc / "self/ns"
        namespace.mkdir(parents=True)
        for name in ("mnt", "pid", "user"):
            (namespace / name).touch()
        return cgroup, proc

    def test_macos_is_typed_disqualification_without_importing_benchexec(self) -> None:
        result = qualify_local_host(system="Darwin")
        self.assertEqual(result["result"], "disqualified")
        self.assertEqual(result["cause"], "unsupported_operating_system")
        self.assertEqual(exit_code(result), 2)

    def test_linux_without_cgroups_v2_is_typed_disqualification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = qualify_local_host(system="Linux", cgroup_root=Path(directory))
        self.assertEqual(result["cause"], "cgroups_v2_unavailable")

    def test_package_preflight_reports_missing_cpuset_delegation(self) -> None:
        def runner(_command):
            return CommandResult(1, "", "Cannot limit CPU cores without cpuset cgroup.")

        with tempfile.TemporaryDirectory() as directory:
            cgroup, _proc = self._linux_roots(directory)
            result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
        self.assertEqual(result["result"], "disqualified")
        self.assertEqual(result["cause"], "benchexec_cpuset_delegation_unavailable")

    def test_admitted_linux_requires_metrics_and_tree_termination(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command):
            commands.append(tuple(command))
            if "benchexec.check_cgroups" in command:
                return CommandResult(0, "", "")
            measurements = {
                "walltime": 1.0,
                "cputime": 0.1,
                "memory": 1048576,
                "blkio-read": 4096,
                "blkio-write": 65536,
                "pressure-cpu-some": 0.0,
                "pressure-io-some": 0.0,
                "pressure-memory-some": 0.0,
                "terminationreason": "walltime",
                "descendant_stopped": True,
                "namespace_isolation": True,
                "overlay_isolation": True,
            }
            return CommandResult(0, json.dumps(measurements), "")

        with tempfile.TemporaryDirectory() as directory:
            cgroup, _proc = self._linux_roots(directory)
            result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
        self.assertEqual(result["result"], "passed")
        self.assertIsNone(result["cause"])
        self.assertEqual(len(commands), 2)

    def test_missing_io_metric_fails_closed(self) -> None:
        def runner(command):
            if "benchexec.check_cgroups" in command:
                return CommandResult(0, "", "")
            return CommandResult(
                0,
                json.dumps(
                    {
                        "walltime": 1.0,
                        "cputime": 0.1,
                        "memory": 1,
                        "blkio-read": 0,
                        "terminationreason": "walltime",
                        "descendant_stopped": True,
                        "namespace_isolation": True,
                        "overlay_isolation": True,
                    }
                ),
                "",
            )

        with tempfile.TemporaryDirectory() as directory:
            cgroup, _proc = self._linux_roots(directory)
            result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
        self.assertEqual(result["result"], "failed")
        self.assertEqual(result["cause"], "mandatory_metric_missing")

    def test_non_object_measurements_fail_as_malformed(self) -> None:
        def runner(command):
            if "benchexec.check_cgroups" in command:
                return CommandResult(0, "", "")
            return CommandResult(0, json.dumps(list(range(5))), "")

        with tempfile.TemporaryDirectory() as directory:
            cgroup, _proc = self._linux_roots(directory)
            result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
        self.assertEqual(result["cause"], "malformed_benchexec_evidence")

    def test_non_numeric_and_non_finite_metrics_fail_as_malformed(self) -> None:
        for invalid in (None, True, "1", float("nan"), float("inf"), -1):
            with self.subTest(invalid=invalid):

                def runner(command, invalid_metric=invalid):
                    if "benchexec.check_cgroups" in command:
                        return CommandResult(0, "", "")
                    measurements = {
                        "walltime": 1.0,
                        "cputime": 0.1,
                        "memory": 1048576,
                        "blkio-read": 4096,
                        "blkio-write": invalid_metric,
                        "pressure-cpu-some": 0.0,
                        "pressure-io-some": 0.0,
                        "pressure-memory-some": 0.0,
                        "terminationreason": "walltime",
                        "descendant_stopped": True,
                        "namespace_isolation": True,
                        "overlay_isolation": True,
                    }
                    return CommandResult(0, json.dumps(measurements), "")

                with tempfile.TemporaryDirectory() as directory:
                    cgroup, _proc = self._linux_roots(directory)
                    result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
                self.assertEqual(result["cause"], "malformed_benchexec_evidence")

    def test_live_descendant_fails_closed(self) -> None:
        def runner(command):
            if "benchexec.check_cgroups" in command:
                return CommandResult(0, "", "")
            return CommandResult(
                0,
                json.dumps(
                    {
                        "walltime": 1.0,
                        "cputime": 0.1,
                        "memory": 1,
                        "blkio-read": 0,
                        "blkio-write": 1,
                        "pressure-cpu-some": 0.0,
                        "pressure-io-some": 0.0,
                        "pressure-memory-some": 0.0,
                        "terminationreason": "walltime",
                        "descendant_stopped": False,
                        "namespace_isolation": True,
                        "overlay_isolation": True,
                    }
                ),
                "",
            )

        with tempfile.TemporaryDirectory() as directory:
            cgroup, _proc = self._linux_roots(directory)
            result = qualify_local_host(system="Linux", cgroup_root=cgroup, runner=runner)
        self.assertEqual(result["cause"], "descendant_survived_termination")

    def test_strict_validator_rejects_typed_disqualification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / "evidence.json"
            evidence.write_text(json.dumps(qualify_local_host(system="Darwin")), encoding="utf-8")
            schema = Path(__file__).parents[1] / "schemas/local-admission-evidence.json"
            with self.assertRaisesRegex(ValueError, "was disqualified"):
                validate(evidence, schema)


if __name__ == "__main__":
    unittest.main()
