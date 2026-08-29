from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.progressive_run import (
    ControllerError,
    Executables,
    _run_benchexec,
    _safe_stage,
    _validate,
    assemble_rung_evidence,
    build_plan,
    ingest_benchexec_result,
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
        "metric_sources": {
            "benchexec": [
                "wall_seconds",
                "peak_rss_bytes",
                "physical_read_bytes",
                "physical_write_bytes",
            ],
            "storage_attribution": [
                "retained_storage_bytes",
                "transient_peak_storage_bytes",
                "logical_read_bytes",
                "logical_write_bytes",
                "reader_calls",
                "publication_work_units",
            ],
            "query_qualification": ["live_edges", "correctness"],
        },
        "storage_components": {
            "source_allocated_physical_bytes": 100,
            "source_retained_logical_eof_bytes": 110,
            "imported_allocated_physical_bytes": 120,
            "imported_retained_logical_eof_bytes": 130,
            "transient_peak_allocated_bytes": 300,
            "logical_read_bytes": 400,
            "logical_write_bytes": 500,
            "reader_calls": 8,
            "publication_work_units": 9,
        },
        "failure": None,
    }


def graphforge(scale: int, receipts: dict[str, list[dict]] | list[dict] | None = None) -> dict:
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
    if isinstance(receipts, dict):
        by_name = {phase["phase"]: phase for phase in phases}
        for name, values in receipts.items():
            by_name[name]["receipts"] = values
    elif receipts is not None:
        phases[2]["receipts"] = receipts
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


def sink(digest: str, *, rows: int, scalar: int | None = None) -> dict:
    receipt = {
        "contract": "graphforge-result-sink/2",
        "format": "ArrowIpc",
        "rows": rows,
        "batches": 1,
        "bytes": 64,
        "complete": True,
        "result_sha256": digest,
        "query_evidence": {"contract": "graphforge-query-evidence/1"},
    }
    if scalar is not None:
        receipt["scalar_u64"] = scalar
    return receipt


def authoritative_receipts(scale: int) -> dict[str, list[dict]]:
    construction = [
        {
            "contract": "graphforge-import-session/1",
            "outcome": "committed",
            "construction": {
                "configured_batch_rows": 65_536,
                "accepted_chunks": 64,
                "publication_committed": True,
                "input_rows": 65_536 * 64,
                "input_batches": 64,
                "transient_peak_allocated_bytes": 300,
                "application_io": {
                    "totals": {"read_bytes": 400, "write_bytes": 500, "read_calls": 8}
                },
                "publication_work": {
                    "contract": "graphforge-publication-work/1",
                    "semantic_total_operations": 9,
                },
            },
        },
        {
            "contract": "graphforge-lifecycle-storage/1",
            "retained_storage_bytes": 200,
            "transient_peak_storage_bytes": 300,
        },
    ]
    source_storage = storage_receipt(100, 110)
    imported_storage = storage_receipt(120, 130)
    nodes, edges = 1 << scale, 16 * (1 << scale)
    node_count = sink("a" * 64, rows=1, scalar=nodes)
    edge_count = sink("b" * 64, rows=1, scalar=edges)
    one_hop = sink("c" * 64, rows=1024)
    two_hop = sink("d" * 64, rows=1024)
    return {
        "ingest": construction,
        "reopen": [source_storage],
        "recount": [node_count, edge_count],
        "query": [one_hop, two_hop],
        "reopen_proof": [node_count, edge_count, one_hop, two_hop, imported_storage],
    }


def storage_receipt(allocated: int, logical_eof: int) -> dict:
    categories = {
        name: {
            "logical_references": 0,
            "logical_bytes": 0,
            "physical_objects": 0,
            "physical_logical_bytes": 0,
            "allocated_bytes": 0,
        }
        for name in (
            "topology_nodes",
            "topology_edges",
            "properties",
            "uuid_and_surrogates",
            "adjacency",
            "catalog_and_manifests",
            "construction_staging",
            "portable_package",
            "clean_imported_project",
            "other",
        )
    }
    return {
        "contract": "graphforge-storage-attribution-command/1",
        "storage": {
            "contract": "graphforge-storage-attribution/1",
            "categories": categories,
            "logical_references": 0,
            "logical_bytes": 0,
            "retained_logical_eof_bytes": logical_eof,
            "allocated_physical_bytes": allocated,
            "physical_objects": 0,
        },
        "reopen_agrees": True,
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

    def test_bulk_capability_is_derived_from_the_ordinary_commit_receipt(self) -> None:
        receipt = {
            "contract": "graphforge-import-session/1",
            "outcome": "committed",
            "construction": {
                "configured_batch_rows": 65_536,
                "accepted_chunks": 2,
                "publication_committed": True,
                "input_rows": 131_072,
                "input_batches": 2,
            },
        }
        self.assertIs(require_bulk_ingest_capability(receipt), receipt)
        invalid = json.loads(json.dumps(receipt))
        invalid["construction"]["configured_batch_rows"] = 8192
        with self.assertRaisesRegex(ControllerError, "bulk_ingest_capability_unproven"):
            require_bulk_ingest_capability(invalid)

    def test_named_authorities_assemble_true_passed_evidence_and_refuse_gaps(self) -> None:
        receipts = authoritative_receipts(18)
        gf = graphforge(18, receipts)
        rung = assemble_rung_evidence(root=ROOT, scale=18, graphforge=gf, benchexec=benchexec(gf))
        self.assertEqual(rung["status"], "passed")
        self.assertEqual(rung["metrics"]["physical_read_bytes"], 0)
        for omitted in receipts:
            with self.subTest(omitted=omitted), self.assertRaises(ControllerError):
                changed = {name: values for name, values in receipts.items() if name != omitted}
                changed_gf = graphforge(18, changed)
                assemble_rung_evidence(
                    root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
                )
        contradictory = authoritative_receipts(18)
        contradictory["reopen_proof"][2] = contradictory["reopen_proof"][2] | {
            "result_sha256": "e" * 64
        }
        changed_gf = graphforge(18, contradictory)
        with self.assertRaisesRegex(ControllerError, "contradicts"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        missing_lifecycle = authoritative_receipts(18)
        missing_lifecycle["ingest"] = [missing_lifecycle["ingest"][0]]
        changed_gf = graphforge(18, missing_lifecycle)
        with self.assertRaisesRegex(ControllerError, "graphforge-lifecycle-storage/1"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        contradictory_storage = authoritative_receipts(18)
        contradictory_storage["reopen"][0]["reopen_agrees"] = False
        changed_gf = graphforge(18, contradictory_storage)
        with self.assertRaisesRegex(ControllerError, "ordinary storage receipt"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        missing_publication = authoritative_receipts(18)
        del missing_publication["ingest"][0]["construction"]["publication_work"]
        changed_gf = graphforge(18, missing_publication)
        with self.assertRaisesRegex(ControllerError, "construction metrics"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )

    def test_exact_benchexec_xml_and_log_are_normalized_into_passed_bundle(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        stage = self.base / "stage-result"
        raw = stage / "raw"
        raw.mkdir(parents=True)
        gf = graphforge(18, authoritative_receipts(18))
        (raw / "run.log").write_text(json.dumps(gf) + "\n")
        columns = {
            "status": "DONE",
            "walltime": "1.25s",
            "cputime": "1.0s",
            "memory": "4096B",
            "blkio-read": "1024B",
            "blkio-write": "2048B",
            "pressure-cpu-some": "0.1s",
            "pressure-io-some": "0.2s",
            "pressure-memory-some": "0.3s",
        }
        xml = (
            "<result><run>"
            + "".join(
                f'<column title="{name}" value="{value}" />' for name, value in columns.items()
            )
            + "</run></result>"
        )
        (raw / "result.xml").write_text(xml)
        normalized, observed, rung = ingest_benchexec_result(
            root=ROOT, stage=stage, scale=18, plan=plan
        )
        self.assertEqual(observed, gf)
        self.assertEqual(normalized["authority"]["read_bytes"], 1024)
        self.assertEqual(rung["metrics"]["wall_seconds"], 2)

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

    def test_staged_executables_are_verified_private_copies(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        profile = ROOT / "profiles/graph500/s18-local.json"
        stage = _safe_stage(ROOT, profile, self.executables, plan["identities"], self.base)
        staged_gf = stage / "bin/gf"
        self.assertFalse(staged_gf.is_symlink())
        original = staged_gf.read_bytes()
        self.executables.gf.write_bytes(b"changed-after-planning")
        self.assertEqual(staged_gf.read_bytes(), original)
        with self.assertRaisesRegex(ControllerError, "staged executable identity mismatch"):
            _safe_stage(ROOT, profile, self.executables, plan["identities"], self.base)

    def test_benchexec_python_is_rechecked_immediately_before_invocation(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        stage = self.base / "stage"
        stage.mkdir()
        (stage / "bin").mkdir()
        (stage / "benchmark.xml").write_text("fixture")
        self.executables.benchexec_python.write_bytes(b"changed-after-planning")
        with self.assertRaisesRegex(ControllerError, "identity changed after planning"):
            _run_benchexec(stage, self.executables, plan["identities"])

    def test_failed_result_schema_requires_closed_exact_identities(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        result = {
            "schema": "graphforge-progressive-run-result/1",
            "rung": "S18",
            "status": "failed",
            "failure": "ordinary_receipt_missing",
            "identities": plan["identities"],
            "claim": "engineering_evidence_only",
        }
        _validate(ROOT, "progressive-run-result.json", result)
        result["identities"] = {}
        with self.assertRaisesRegex(ControllerError, "validation failed"):
            _validate(ROOT, "progressive-run-result.json", result)


if __name__ == "__main__":
    unittest.main()
