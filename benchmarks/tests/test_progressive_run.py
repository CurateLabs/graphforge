from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.progressive_run import (
    ControllerError,
    Executables,
    build_plan,
    require_bulk_ingest_capability,
    require_order,
    validate_fixture_bundle,
    write_plan,
    write_s20_projection,
)

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "78b75aed8fef71cfa3e4700b80a05d6b71e64f22"
PHASES = [
    "admission",
    "generate",
    "ingest",
    "reopen",
    "recount",
    "query",
    "export",
    "verify",
    "clean_import",
    "reopen_proof",
]


def passed_rung(scale: int) -> dict:
    return {
        "profile_id": f"graph500-s{scale}-local",
        "source": "progressive_profile",
        "scale": scale,
        "live_edges": (1 << scale) * 16,
        "status": "passed",
        "correctness": True,
        "phases": PHASES,
        "metrics": {
            "wall_seconds": 10,
            "peak_rss_bytes": 100,
            "retained_storage_bytes": 200,
            "transient_peak_storage_bytes": 300,
            "logical_read_bytes": 400,
            "logical_write_bytes": 500,
            "physical_read_bytes": 600,
            "physical_write_bytes": 700,
            "reader_calls": 8,
            "publication_work_units": 9,
        },
        "failure": None,
    }


def graphforge(scale: int) -> dict:
    phases = [
        {
            "phase": phase,
            "status": "passed",
            "duration_ms": 1,
            "peak_rss_bytes": 100,
            "exit_code": 0,
        }
        for phase in PHASES
    ]
    return {
        "schema": "graphforge-public-certification/1",
        "profile_id": f"graph500-s{scale}-local",
        "status": "passed",
        "phases": phases,
        "failed_phase": None,
    }


def benchexec(gf: dict) -> dict:
    return {
        "schema": "graphforge-benchexec-run/1",
        "outcome": "passed",
        "exit_code": 0,
        "signal": None,
        "authority": {
            "wall_seconds": 0.01,
            "cpu_seconds": 0.01,
            "peak_rss_bytes": 100,
            "read_bytes": 0,
            "write_bytes": 0,
            "pressure_cpu_seconds": 0.0,
            "pressure_io_seconds": 0.0,
            "pressure_memory_seconds": 0.0,
        },
        "limits": {
            "wall_seconds": 14400.0,
            "cpu_seconds": 14400.0,
            "memory_bytes": 4294967296,
            "cores": list(range(16)),
        },
        "graphforge": gf,
        "disagreements": [],
    }


class ProgressiveRunControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.output = self.base / "evidence"
        self.output.mkdir()
        generator = self.base / "graphforge-benchmark-graph500-generator"
        generator.write_bytes((ROOT / "runners/graph500-generator/src/main.rs").read_bytes())
        gf = self.base / "gf"
        certify = self.base / "graphforge-benchmark-certify"
        python = self.base / "python"
        for path in (gf, certify, python):
            path.write_bytes(b"fixture")
        for path in (generator, gf, certify, python):
            path.chmod(path.stat().st_mode | 0o111)
        self.executables = Executables(gf, certify, generator, python)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_dry_plan_binds_exact_immutable_identities_without_paths(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        path = write_plan(self.output, plan)
        profile = json.loads((ROOT / "profiles/graph500/s18-local.json").read_text())
        self.assertEqual(plan["rung"], "S18")
        self.assertEqual(plan["identities"]["commit"], COMMIT)
        self.assertEqual(plan["identities"]["generator"], profile["generator"]["identity"])
        encoded = path.read_text()
        self.assertNotIn(str(self.base), encoded)
        self.assertNotIn("token", encoded.lower())

    def test_s19_requires_passed_s18_and_s18_cannot_repeat(self) -> None:
        with self.assertRaisesRegex(ControllerError, "requires exactly one"):
            require_order(ROOT, self.output, 19)
        (self.output / "s18-rung.json").write_text(json.dumps(passed_rung(18)))
        require_order(ROOT, self.output, 19)
        with self.assertRaisesRegex(ControllerError, "first incomplete"):
            require_order(ROOT, self.output, 18)

    def test_failed_prior_evidence_refuses_progression(self) -> None:
        failed = passed_rung(18)
        failed.update(status="failed", correctness=False, failure="ingest")
        failed["phases"] = failed["phases"][:3]
        failed["metrics"] = {"wall_seconds": 1, "peak_rss_bytes": 2}
        (self.output / "s18-rung.json").write_text(json.dumps(failed))
        with self.assertRaisesRegex(ControllerError, "not a passed"):
            require_order(ROOT, self.output, 19)

    def test_fixture_bundle_validates_all_closed_evidence_contracts(self) -> None:
        bundle = self.base / "bundle"
        bundle.mkdir()
        gf = graphforge(18)
        (bundle / "graphforge.json").write_text(json.dumps(gf))
        (bundle / "benchexec.json").write_text(json.dumps(benchexec(gf)))
        (bundle / "rung.json").write_text(json.dumps(passed_rung(18)))
        validate_fixture_bundle(ROOT, bundle, 18)
        changed = benchexec(gf)
        changed["graphforge"]["profile_id"] = "graph500-s19-local"
        (bundle / "benchexec.json").write_text(json.dumps(changed))
        with self.assertRaisesRegex(ControllerError, "disagree"):
            validate_fixture_bundle(ROOT, bundle, 18)

    def test_generator_executable_digest_and_commit_are_fail_closed(self) -> None:
        original = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )["identities"]["generator_executable_sha256"]
        self.executables.generator.write_bytes(b"wrong")
        changed = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )["identities"]["generator_executable_sha256"]
        self.assertNotEqual(original, changed)
        self.executables.generator.write_bytes(
            (ROOT / "runners/graph500-generator/src/main.rs").read_bytes()
        )
        with self.assertRaisesRegex(ControllerError, "full Git object ID"):
            build_plan(
                root=ROOT,
                output_dir=self.output,
                scale=18,
                commit="HEAD",
                executables=self.executables,
            )

    def test_real_run_requires_closed_bulk_ingest_capability(self) -> None:
        with self.assertRaisesRegex(ControllerError, "bulk_ingest_capability_unproven"):
            require_bulk_ingest_capability(ROOT, self.output)
        invalid = {
            "schema": "graphforge-ordinary-ingest-capability/1",
            "commit": COMMIT,
            "interface": "gf_import_session",
            "bulk_construction": True,
            "minimum_batch_rows": 8192,
            "scalar_durable_loop_absent": False,
        }
        (self.output / "ordinary-ingest-capability.json").write_text(json.dumps(invalid))
        with self.assertRaisesRegex(ControllerError, "validation failed"):
            require_bulk_ingest_capability(ROOT, self.output)

    def test_adjacent_passed_rungs_produce_schema_valid_s20_projection(self) -> None:
        for scale in (18, 19):
            (self.output / f"s{scale}-rung.json").write_text(json.dumps(passed_rung(scale)))
        capacity = self.base / "capacity.json"
        capacity.write_text(
            json.dumps(
                {
                    "physical_read_bytes_per_second": 1_000_000,
                    "physical_write_bytes_per_second": 1_000_000,
                    "reader_calls_per_second": 1_000_000,
                    "publication_work_per_second": 1_000_000,
                    "secret": "discarded",
                }
            )
        )
        path = write_s20_projection(ROOT, self.output, capacity)
        evidence = json.loads(path.read_text())
        self.assertEqual(evidence["source_scales"], [18, 19])
        self.assertNotIn("secret", path.read_text())


if __name__ == "__main__":
    unittest.main()
