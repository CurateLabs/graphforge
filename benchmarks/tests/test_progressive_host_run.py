from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_host_run import (
    HOST_PROFILE_ID,
    HostRunError,
    build_plan,
    completed_prefix,
    inventory_work_root,
    load_host_capacity,
    reclaim_rung_workspace,
    require_order,
    require_work_root,
    resolve_host_benchexec_python,
)
from graphforge_bench.progressive_run import Executables
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
