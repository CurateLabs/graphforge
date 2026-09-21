from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_host_run import (
    HOST_PROFILE_ID,
    MAXIMUM_WALL_SECONDS,
    HostRunError,
    PhaseFailure,
    RungWall,
    _benchexec_hit_wall,
    _certify_phase_failure,
    _result,
    _validate,
    build_plan,
    completed_prefix,
    inventory_work_root,
    load_host_capacity,
    reclaim_rung_workspace,
    reference_wall_seconds,
    require_order,
    require_work_root,
    resolve_host_benchexec_python,
)
from graphforge_bench.progressive_host_run import (
    run as host_run,
)
from graphforge_bench.progressive_run import Executables
from tests.host_run_fixture import executables as fixture_executables
from tests.host_run_fixture import write_host_bundle
from tests.test_progressive_run import passed_rung as local_passed_rung

ROOT = Path(__file__).resolve().parents[1]
WORK_PARENT = Path("/home/ubuntu/graphforge-ladder")
COMMIT = "f013587f0123456789abcdef0123456789abcdef"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def host_capacity() -> dict:
    return {
        "schema": "graphforge-host-capacity/1",
        "host_profile_id": HOST_PROFILE_ID,
        "volume_bytes": 500 * 1024**3,
        "reserved_headroom_bytes": 75 * 1024**3,
        "physical_read_bytes_per_second": 10**9,
        "physical_write_bytes_per_second": 10**9,
        "reader_calls_per_second": 10**6,
        "publication_work_per_second": 10**6,
    }


def passed_rung(scale: int) -> dict:
    document = dict(local_passed_rung(18))
    suffix = "local" if scale in (18, 19) else "provider"
    source = "progressive_profile" if scale in (18, 19) else "canonical_ladder"
    document["profile_id"] = f"graph500-s{scale}-{suffix}"
    document["source"] = source
    document["scale"] = scale
    document["live_edges"] = (1 << scale) * 16
    counts = dict(document["storage_attribution"]["counts"])
    counts.update(
        {
            "source_nodes": 1 << scale,
            "source_edges": 16 * (1 << scale),
            "imported_nodes": 1 << scale,
            "imported_edges": 16 * (1 << scale),
        }
    )
    attribution = dict(document["storage_attribution"])
    attribution["counts"] = counts
    document["storage_attribution"] = attribution
    return document


def host_result(scale: int) -> dict:
    suffix = "local" if scale in (18, 19) else "provider"
    identities = {
        "commit": COMMIT,
        "host_profile_id": HOST_PROFILE_ID,
        "host_profile_sha256": "a" * 64,
        "profile_id": f"graph500-s{scale}-{suffix}",
        "profile_sha256": "b" * 64,
        "generator": "sha256:" + ("c" * 64),
        "generator_executable_sha256": "d" * 64,
        "gf_sha256": "e" * 64,
        "certify_sha256": "f" * 64,
        "benchexec_python_sha256": "1" * 64,
        "benchexec_version": "3.35",
    }
    if scale >= 20:
        identities["admitted_projection_sha256"] = "2" * 64
    return {
        "schema": "graphforge-progressive-host-run-result/1",
        "rung": f"S{scale}",
        "status": "passed",
        "failure": None,
        "identities": identities,
        "artifacts": {
            "plan_sha256": "3" * 64,
            "benchexec_sha256": "4" * 64,
            "graphforge_sha256": "5" * 64,
            "rung_sha256": "6" * 64,
        },
        "claim": "engineering_evidence_only",
    }


class ProgressiveHostRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        WORK_PARENT.mkdir(parents=True, exist_ok=True)

    def test_work_root_must_share_process_root_device(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            try:
                require_work_root(work)
            except HostRunError as error:
                self.assertEqual(str(error), "work_root_invalid")
            else:
                self.assertEqual(work.stat().st_dev, Path("/").stat().st_dev)

    def test_ordering_gates_and_s20_requires_capacity(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary) / "evidence"
            output.mkdir()
            require_order(ROOT, output, 18)
            with self.assertRaisesRegex(HostRunError, "requires completed prefix"):
                require_order(ROOT, output, 19)
            write_host_bundle(output, 18)
            require_order(ROOT, output, 19)
            write_host_bundle(output, 19)
            require_order(ROOT, output, 20)

            python = ROOT / ".venv/bin/python"
            payload = (ROOT / "runners/graph500-generator/src/main.rs").read_bytes()
            gf = Path(temporary) / "gf"
            certify = Path(temporary) / "certify"
            generator = Path(temporary) / "generator"
            for path in (gf, certify, generator):
                path.write_bytes(payload)
                path.chmod(0o755)
            executables = Executables(
                gf=gf,
                certify=certify,
                generator=generator,
                benchexec_python=python,
            )
            with self.assertRaisesRegex(HostRunError, "host capacity is required"):
                build_plan(
                    root=ROOT,
                    output_dir=output,
                    scale=20,
                    commit=COMMIT,
                    executables=executables,
                    capacity=None,
                )

    def test_host_capacity_schema_and_reclaim_inventory(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            root = Path(temporary)
            capacity_path = root / "capacity.json"
            capacity_path.write_text(json.dumps(host_capacity()), encoding="utf-8")
            loaded = load_host_capacity(ROOT, capacity_path)
            self.assertEqual(loaded["host_profile_id"], HOST_PROFILE_ID)
            work = root / "work"
            workspace = work / "workspace" / "s18"
            workspace.mkdir(parents=True)
            (workspace / "nodes.parquet").write_bytes(b"x")
            reclaim_rung_workspace(work, 18)
            inventory = inventory_work_root(work)
            self.assertTrue(inventory["empty"])
            self.assertEqual(inventory["host_profile_id"], HOST_PROFILE_ID)

    def test_completed_prefix_rejects_gaps(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary)
            write_host_bundle(output, 18)
            (output / "s20-rung.json").write_text(json.dumps(passed_rung(20)), encoding="utf-8")
            (output / "s20-result.json").write_text(json.dumps(host_result(20)), encoding="utf-8")
            with self.assertRaisesRegex(HostRunError, "out of order"):
                completed_prefix(ROOT, output)

    def test_build_plan_s18_binds_host_profile(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary)
            python = ROOT / ".venv/bin/python"
            self.assertTrue(python.is_file())
            payload = (ROOT / "runners/graph500-generator/src/main.rs").read_bytes()
            gf = Path(temporary) / "gf"
            certify = Path(temporary) / "certify"
            generator = Path(temporary) / "generator"
            for path in (gf, certify, generator):
                path.write_bytes(payload)
                path.chmod(0o755)
            executables = Executables(
                gf=gf,
                certify=certify,
                generator=generator,
                benchexec_python=python,
            )
            with patch("graphforge_bench.progressive_host_run.version", return_value="3.35"):
                plan = build_plan(
                    root=ROOT,
                    output_dir=output,
                    scale=18,
                    commit=COMMIT,
                    executables=executables,
                    capacity=None,
                )
            self.assertEqual(plan["schema"], "graphforge-progressive-host-run-plan/1")
            self.assertEqual(plan["rung"], "S18")
            self.assertEqual(plan["execution"], "native_linux_benchexec_host")
            self.assertEqual(plan["identities"]["host_profile_id"], HOST_PROFILE_ID)
            self.assertEqual(plan["identities"]["profile_id"], "graph500-s18-local")
            self.assertEqual(
                plan["identities"]["host_profile_sha256"],
                sha256(ROOT / "profiles" / f"{HOST_PROFILE_ID}.json"),
            )
            self.assertNotIn("admitted_projection_sha256", plan["identities"])

    def test_resolve_host_benchexec_python_requires_pystemd(self) -> None:
        venv_python = ROOT / ".venv/bin/python"
        self.assertTrue(venv_python.is_file())
        with self.assertRaises(HostRunError):
            resolve_host_benchexec_python(venv_python)
        system = Path("/usr/bin/python3")
        if system.is_file():
            try:
                resolved = resolve_host_benchexec_python(system)
            except HostRunError:
                self.skipTest("system BenchExec+pystemd unavailable")
            else:
                self.assertEqual(resolved, system.resolve())


if __name__ == "__main__":
    unittest.main()


class RungWallTests(unittest.TestCase):
    """A rung stops a margin above its last accepted wall, never above 4 h."""

    def test_reference_plus_margin_rounds_up_and_caps_at_envelope(self) -> None:
        self.assertEqual(RungWall.from_reference(92, 0.10).wall_seconds, 102)
        self.assertEqual(RungWall.from_reference(1783, 0.10).wall_seconds, 1962)
        self.assertEqual(RungWall.from_reference(100, 0.0).wall_seconds, 100)
        self.assertEqual(RungWall.from_reference(14_000, 0.10).wall_seconds, MAXIMUM_WALL_SECONDS)
        self.assertEqual(RungWall.from_reference(None, 0.10).wall_seconds, MAXIMUM_WALL_SECONDS)
        self.assertEqual(RungWall.envelope().wall_seconds, MAXIMUM_WALL_SECONDS)
        self.assertEqual(
            RungWall.from_reference(92, 0.10).policy(),
            {"maximum_wall_seconds": 14_400, "reference_wall_seconds": 92, "margin": 0.10},
        )
        self.assertEqual(
            RungWall.envelope().policy(),
            {"maximum_wall_seconds": 14_400, "reference_wall_seconds": None, "margin": 0.10},
        )

    def test_refuses_malformed_reference_or_margin(self) -> None:
        for reference in (0, -5, True, 12.5):
            with self.assertRaises(HostRunError):
                RungWall.from_reference(reference, 0.10)  # type: ignore[arg-type]
        for margin in (-0.1, float("nan"), float("inf"), True, "10%"):
            with self.assertRaises(HostRunError):
                RungWall.from_reference(92, margin)  # type: ignore[arg-type]

    def test_reference_wall_seconds_reads_only_passed_rung_evidence(self) -> None:
        self.assertIsNone(reference_wall_seconds(None, 18))
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary) / "evidence"
            self.assertIsNone(reference_wall_seconds(output, 18))
            write_host_bundle(output, 18)
            expected = json.loads((output / "s18-rung.json").read_text())["metrics"]["wall_seconds"]
            self.assertEqual(reference_wall_seconds(output, 18), expected)
            self.assertIsNone(reference_wall_seconds(output, 19))
            rung = json.loads((output / "s18-rung.json").read_text())
            rung["status"] = "failed"
            (output / "s18-rung.json").write_text(json.dumps(rung))
            with self.assertRaisesRegex(HostRunError, "not a passed rung"):
                reference_wall_seconds(output, 18)
            rung["status"] = "passed"
            rung["metrics"]["wall_seconds"] = 0
            (output / "s18-rung.json").write_text(json.dumps(rung))
            with self.assertRaisesRegex(HostRunError, "positive integer wall_seconds"):
                reference_wall_seconds(output, 18)

    def test_build_plan_binds_rung_wall_and_policy(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary)
            python = ROOT / ".venv/bin/python"
            payload = (ROOT / "runners/graph500-generator/src/main.rs").read_bytes()
            gf = Path(temporary) / "gf"
            certify = Path(temporary) / "certify"
            generator = Path(temporary) / "generator"
            for path in (gf, certify, generator):
                path.write_bytes(payload)
                path.chmod(0o755)
            executables = Executables(
                gf=gf, certify=certify, generator=generator, benchexec_python=python
            )
            with patch("graphforge_bench.progressive_host_run.version", return_value="3.35"):
                plan = build_plan(
                    root=ROOT,
                    output_dir=output,
                    scale=18,
                    commit=COMMIT,
                    executables=executables,
                    capacity=None,
                    wall=RungWall.from_reference(92, 0.10),
                )
                envelope = build_plan(
                    root=ROOT,
                    output_dir=output,
                    scale=18,
                    commit=COMMIT,
                    executables=executables,
                    capacity=None,
                )
            self.assertEqual(plan["limits"]["wall_seconds"], 102)
            self.assertEqual(plan["limits"]["memory_bytes"], 4_294_967_296)
            self.assertEqual(plan["wall_policy"]["reference_wall_seconds"], 92)
            self.assertEqual(envelope["limits"]["wall_seconds"], MAXIMUM_WALL_SECONDS)
            self.assertIsNone(envelope["wall_policy"]["reference_wall_seconds"])

    def test_benchexec_hit_wall_reads_the_staged_result(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            stage = Path(temporary)
            self.assertFalse(_benchexec_hit_wall(stage))
            raw = stage / "raw"
            raw.mkdir()
            document = raw / "results.xml"
            document.write_text(
                '<result><run name="profile"><column title="status" value="TIMEOUT"/>'
                '<column title="walltime" value="102.4s"/></run></result>'
            )
            self.assertTrue(_benchexec_hit_wall(stage))
            document.write_text(
                '<result><run name="profile"><column title="status" value="DONE"/>'
                '<column title="walltime" value="91.0s"/></run></result>'
            )
            self.assertFalse(_benchexec_hit_wall(stage))


class RungPhaseFailureTests(unittest.TestCase):
    """A rung that ran and died inside a phase names the phase, not staging."""

    FIXTURE = ROOT / "tests/fixtures/s22-phase-failure-raw"
    TAIL = "GF_IO: storage error: graph construction session: control record exceeds bound"

    def stage(self, parent: Path) -> Path:
        stage = parent / "stage"
        shutil.copytree(self.FIXTURE, stage / "raw")
        return stage

    def plan(self, scale: int) -> dict:
        identities = host_result(scale)["identities"]
        return {
            "schema": "graphforge-progressive-host-run-plan/1",
            "rung": f"S{scale}",
            "execution": "native_linux_benchexec_host",
            "identities": identities,
            "limits": {"wall_seconds": 1963, "memory_bytes": 96_000_000_000, "cores": 16},
            "outputs": [
                f"s{scale}-{name}.json"
                for name in ("plan", "benchexec", "graphforge", "rung", "result")
            ],
            "claim": "engineering_evidence_only",
        }

    def test_certify_phase_failure_reads_the_staged_stream(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            parent = Path(temporary)
            self.assertIsNone(_certify_phase_failure(parent / "absent"))
            stage = self.stage(parent)
            failure = _certify_phase_failure(stage)
            self.assertIsNotNone(failure)
            assert failure is not None
            self.assertEqual(failure.phase, "ingest")
            self.assertEqual(failure.error_tail, self.TAIL)
            documents = [
                json.loads(line)
                for log in (stage / "raw").rglob("*.log")
                for line in log.read_text().splitlines()
                if line.startswith("{")
            ]
            evidence = [
                document
                for document in documents
                if document["schema"] == "graphforge-public-certification/1"
            ]
            self.assertEqual(len(evidence), 1)
            _validate(ROOT, "certification-evidence.json", evidence[0])

    def test_certify_phase_failure_ignores_a_run_that_reached_no_failed_phase(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            raw = Path(temporary) / "raw"
            raw.mkdir()
            (raw / "run.log").write_text(
                json.dumps(
                    {
                        "schema": "graphforge-public-certification-phase-event/1",
                        "profile_id": "graph500-s22-provider",
                        "outcome": {
                            "phase": "admission",
                            "status": "passed",
                            "duration_ms": 10,
                            "peak_rss_bytes": 126976,
                            "exit_code": 0,
                        },
                    }
                )
                + "\nBenchExec could not start the tool\n"
            )
            self.assertIsNone(_certify_phase_failure(Path(temporary)))

    def test_result_carries_the_failed_phase_and_error_text(self) -> None:
        plan = self.plan(22)
        failed = _result(
            plan,
            "failed",
            "rung_phase_failed",
            phase_failure=PhaseFailure("ingest", self.TAIL),
        )
        _validate(ROOT, "progressive-host-run-result.json", failed)
        self.assertEqual(failed["failure"], "rung_phase_failed")
        self.assertEqual(failed["failed_phase"], "ingest")
        self.assertEqual(failed["error_tail"], self.TAIL)
        staging = _result(plan, "failed", "staging_failed")
        _validate(ROOT, "progressive-host-run-result.json", staging)
        self.assertNotIn("failed_phase", staging)
        with self.assertRaises(HostRunError):
            _validate(
                ROOT,
                "progressive-host-run-result.json",
                {**staging, "failed_phase": "ingest"},
            )

    def test_run_reports_the_failed_phase_instead_of_staging_failed(self) -> None:
        plan = self.plan(22)
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            parent = Path(temporary)
            work_root = parent / "work"
            work_root.mkdir()
            output = parent / "evidence"
            stage = self.stage(parent)
            with (
                patch("graphforge_bench.progressive_host_run._native_authority"),
                patch("graphforge_bench.progressive_host_run._safe_stage_host", return_value=stage),
                patch("graphforge_bench.progressive_host_run._run_benchexec", return_value=0),
                self.assertRaisesRegex(HostRunError, "rung_phase_failed"),
            ):
                host_run(
                    root=ROOT,
                    output_dir=output,
                    work_root=work_root,
                    scale=22,
                    plan=plan,
                    executables=fixture_executables(parent),
                )
            result = json.loads((output / "s22-result.json").read_text())
            _validate(ROOT, "progressive-host-run-result.json", result)
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["failure"], "rung_phase_failed")
            self.assertEqual(result["failed_phase"], "ingest")
            self.assertEqual(result["error_tail"], self.TAIL)
            self.assertTrue((output / "s22-failure-raw").is_dir())
