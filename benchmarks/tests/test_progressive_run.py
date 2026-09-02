from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_run import (
    ControllerError,
    Executables,
    _authority_staging_parent,
    _bench_home,
    _benchexec_container_flags,
    _benchexec_tool_directory,
    _rewrite_profile_for_provider_volume,
    _run_benchexec,
    _safe_stage,
    _validate,
    _write_json,
    assemble_rung_evidence,
    build_plan,
    ingest_benchexec_result,
    publish_json_no_clobber,
    repository_commit,
    require_bulk_ingest_capability,
    require_order,
    resolve_executables,
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
        "assembly_contract": "graphforge-progressive-rung-assembly/2",
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
            "source_project_current_allocated_bytes": 105,
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
        "storage_attribution": rung_storage_attribution(scale),
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
                "construction_staging": {
                    "logical_references": 3,
                    "logical_bytes": 250,
                    "physical_objects": 3,
                    "physical_logical_bytes": 250,
                    "allocated_bytes": 275,
                },
                "construction_staging_transient_peak_allocated_bytes": 290,
                "application_io": application_io(),
                "publication_work": {
                    "contract": "graphforge-publication-work/1",
                    "semantic_total_operations": 9,
                },
            },
        },
    ]
    source_storage = storage_receipt(100, 110)
    imported_storage = storage_receipt(120, 130)
    nodes, edges = 1 << scale, 16 * (1 << scale)
    node_count = sink("a" * 64, rows=1, scalar=nodes)
    edge_count = sink("b" * 64, rows=1, scalar=edges)
    one_hop = sink("c" * 64, rows=1000)
    two_hop = sink("d" * 64, rows=1000)
    return {
        "ingest": construction,
        "reopen": [source_storage],
        "recount": [node_count, edge_count],
        "query": [one_hop, two_hop],
        "export": [
            {
                "contract": "graphforge-portable-export/2",
                "allocation_logical_bytes": 140,
                "allocation_allocated_bytes": 150,
                "allocation_physical_objects": 1,
            }
        ],
        "reopen_proof": [
            node_count,
            edge_count,
            one_hop,
            two_hop,
            imported_storage,
            {
                "contract": "graphforge-lifecycle-storage/1",
                "source_project_current_allocated_bytes": 105,
                "retained_storage_bytes": 200,
                "transient_peak_storage_bytes": 300,
            },
        ],
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
    categories["topology_nodes"] = {
        "logical_references": 1,
        "logical_bytes": logical_eof,
        "physical_objects": 1,
        "physical_logical_bytes": logical_eof,
        "allocated_bytes": allocated,
    }
    return {
        "contract": "graphforge-storage-attribution-command/1",
        "storage": {
            "contract": "graphforge-storage-attribution/1",
            "categories": categories,
            "logical_references": 1,
            "logical_bytes": logical_eof,
            "retained_logical_eof_bytes": logical_eof,
            "allocated_physical_bytes": allocated,
            "physical_objects": 1,
        },
        "reopen_agrees": True,
    }


def application_io() -> dict:
    fields = (
        "read_bytes",
        "write_bytes",
        "read_calls",
        "write_calls",
        "object_count",
        "block_count",
        "fsync_calls",
    )
    names = (
        "append_merge",
        "seal_authentication",
        "shape_consume_reauthentication",
        "encode_write_postwrite_authentication",
        "publication_preauthentication",
        "cas_install_read_write",
        "hydration_verification",
        "fsync_synchronization",
        "recovery_reauthentication",
    )
    phases = {name: dict.fromkeys(fields, 0) for name in names}
    phases["append_merge"].update(
        read_bytes=400,
        write_bytes=500,
        read_calls=8,
        write_calls=1,
    )
    return {
        "phases": phases,
        "totals": {field: sum(phase[field] for phase in phases.values()) for field in fields},
    }


def rung_storage_attribution(scale: int) -> dict:
    receipts = authoritative_receipts(scale)
    return {
        "source": receipts["reopen"][0]["storage"],
        "imported": receipts["reopen_proof"][4]["storage"],
        "construction": {
            "application_io": receipts["ingest"][0]["construction"]["application_io"],
            "staging": receipts["ingest"][0]["construction"]["construction_staging"],
            "staging_transient_peak_allocated_bytes": receipts["ingest"][0]["construction"][
                "construction_staging_transient_peak_allocated_bytes"
            ],
            "transient_peak_allocated_bytes": 300,
        },
        "portable_package": receipts["export"][0],
        "lifecycle": receipts["reopen_proof"][-1],
        "counts": {
            "source_nodes": 1 << scale,
            "source_edges": 16 * (1 << scale),
            "imported_nodes": 1 << scale,
            "imported_edges": 16 * (1 << scale),
        },
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
        benchexec = self.base / "benchexec"
        for path in (gf, certify, python, benchexec):
            path.write_bytes(b"fixture")
        for path in (generator, gf, certify, python, benchexec):
            path.chmod(path.stat().st_mode | 0o111)
        self.executables = Executables(gf, certify, generator, python)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_evidence_writes_are_atomic_and_never_replace_existing_files(self) -> None:
        path = self.base / "immutable.json"
        _write_json(path, {"value": 1})
        with self.assertRaises(FileExistsError):
            _write_json(path, {"value": 2})
        self.assertEqual(json.loads(path.read_text()), {"value": 1})
        self.assertEqual([item.name for item in self.base.glob(".immutable.json.*")], [])

    def test_nested_publication_syncs_each_fresh_directory_parent(self) -> None:
        path = self.base / "fresh" / "nested" / "evidence.json"
        synced_inodes: list[int] = []
        real_fsync = os.fsync

        def record_fsync(descriptor: int) -> None:
            synced_inodes.append(os.fstat(descriptor).st_ino)
            real_fsync(descriptor)

        with (
            patch(
                "graphforge_bench.progressive_run.os.fsync",
                side_effect=record_fsync,
            ),
            patch(
                "graphforge_bench.progressive_run.os.mkdir",
                wraps=os.mkdir,
            ) as mkdir,
        ):
            publish_json_no_clobber(path, {"value": 1})
        expected_directories = (
            self.base,
            self.base / "fresh",
            self.base / "fresh" / "nested",
        )
        self.assertTrue(
            {item.stat().st_ino for item in expected_directories}.issubset(synced_inodes)
        )
        self.assertEqual([call.args[0] for call in mkdir.call_args_list], ["fresh", "nested"])
        self.assertEqual(json.loads(path.read_text()), {"value": 1})

    def test_interrupted_nested_publication_leaves_no_partial_file(self) -> None:
        path = self.base / "fresh" / "nested" / "evidence.json"
        with (
            patch(
                "graphforge_bench.progressive_run.os.link",
                side_effect=OSError("injected interruption"),
            ),
            self.assertRaisesRegex(OSError, "injected interruption"),
        ):
            publish_json_no_clobber(path, {"value": 1})
        self.assertFalse(path.exists())
        self.assertEqual(list(path.parent.glob(f".{path.name}.*")), [])

    def test_post_link_directory_fsync_failure_keeps_complete_no_clobber_target(
        self,
    ) -> None:
        path = self.base / "evidence.json"
        value = {"status": "complete", "value": 1}
        real_fsync = os.fsync

        def fail_after_link(descriptor: int) -> None:
            temporary = list(path.parent.glob(f".{path.name}.*"))
            if path.exists() and not temporary:
                raise OSError("injected post-link directory fsync failure")
            real_fsync(descriptor)

        with (
            patch(
                "graphforge_bench.progressive_run.os.fsync",
                side_effect=fail_after_link,
            ),
            self.assertRaisesRegex(OSError, "post-link directory fsync failure"),
        ):
            publish_json_no_clobber(path, value)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), value)
        self.assertEqual(list(path.parent.glob(f".{path.name}.*")), [])
        with self.assertRaises(FileExistsError):
            publish_json_no_clobber(path, {"status": "replacement"})
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), value)
        self.assertEqual(list(path.parent.glob(f".{path.name}.*")), [])

    def test_publication_refuses_symlinked_directory_components(self) -> None:
        outside = self.base / "outside"
        outside.mkdir()
        linked = self.base / "linked"
        linked.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            publish_json_no_clobber(linked / "evidence.json", {"value": 1})
        self.assertFalse((outside / "evidence.json").exists())

    def test_venv_python_path_is_not_dereferenced_out_of_its_environment(self) -> None:
        venv = self.base / "venv/bin"
        venv.mkdir(parents=True)
        python = venv / "python"
        python.symlink_to(self.executables.benchexec_python)
        resolved = resolve_executables(
            gf=str(self.executables.gf),
            certify=str(self.executables.certify),
            generator=str(self.executables.generator),
            benchexec_python=str(python),
        )
        self.assertEqual(resolved.benchexec_python, python.absolute())
        self.assertTrue(resolved.benchexec_python.is_symlink())

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
        for field in (
            "configured_batch_rows",
            "accepted_chunks",
            "input_rows",
            "input_batches",
        ):
            for invalid_value in (True, None, "2"):
                with self.subTest(field=field, invalid_value=invalid_value):
                    invalid = json.loads(json.dumps(receipt))
                    invalid["construction"][field] = invalid_value
                    with self.assertRaisesRegex(ControllerError, "bulk_ingest_capability_unproven"):
                        require_bulk_ingest_capability(invalid)

    def test_named_authorities_assemble_true_passed_evidence_and_refuse_gaps(self) -> None:
        receipts = authoritative_receipts(18)
        gf = graphforge(18, receipts)
        rung = assemble_rung_evidence(root=ROOT, scale=18, graphforge=gf, benchexec=benchexec(gf))
        self.assertEqual(rung["status"], "passed")
        self.assertEqual(rung["metrics"]["physical_read_bytes"], 0)
        self.assertEqual(rung["storage_components"]["source_project_current_allocated_bytes"], 105)
        self.assertEqual(
            rung["storage_attribution"]["portable_package"]["allocation_allocated_bytes"], 150
        )
        self.assertEqual(
            rung["storage_attribution"]["construction"]["staging"]["allocated_bytes"], 275
        )
        self.assertEqual(
            rung["storage_attribution"]["construction"]["staging_transient_peak_allocated_bytes"],
            290,
        )
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
        missing_lifecycle["reopen_proof"] = missing_lifecycle["reopen_proof"][:-1]
        changed_gf = graphforge(18, missing_lifecycle)
        with self.assertRaisesRegex(ControllerError, "graphforge-lifecycle-storage/1"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        for invalid in (None, True, -1, "105"):
            with self.subTest(source_project_current_allocated_bytes=invalid):
                malformed_lifecycle = authoritative_receipts(18)
                malformed_lifecycle["reopen_proof"][-1][
                    "source_project_current_allocated_bytes"
                ] = invalid
                changed_gf = graphforge(18, malformed_lifecycle)
                with self.assertRaisesRegex(
                    ControllerError,
                    "lifecycle storage receipt omitted source_project_current_allocated_bytes",
                ):
                    assemble_rung_evidence(
                        root=ROOT,
                        scale=18,
                        graphforge=changed_gf,
                        benchexec=benchexec(changed_gf),
                    )
        incomplete_lifecycle = authoritative_receipts(18)
        del incomplete_lifecycle["reopen_proof"][-1]["source_project_current_allocated_bytes"]
        changed_gf = graphforge(18, incomplete_lifecycle)
        with self.assertRaisesRegex(
            ControllerError,
            "lifecycle storage receipt omitted source_project_current_allocated_bytes",
        ):
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
        missing_portable = authoritative_receipts(18)
        del missing_portable["export"]
        changed_gf = graphforge(18, missing_portable)
        with self.assertRaisesRegex(ControllerError, "graphforge-portable-export/2"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        malformed_categories = authoritative_receipts(18)
        del malformed_categories["reopen"][0]["storage"]["categories"]["other"]
        changed_gf = graphforge(18, malformed_categories)
        with self.assertRaisesRegex(ControllerError, "categories are incomplete"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        malformed_phases = authoritative_receipts(18)
        del malformed_phases["ingest"][0]["construction"]["application_io"]["phases"][
            "recovery_reauthentication"
        ]
        changed_gf = graphforge(18, malformed_phases)
        with self.assertRaisesRegex(ControllerError, "inventory is incomplete"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )
        missing_staging = authoritative_receipts(18)
        del missing_staging["ingest"][0]["construction"]["construction_staging"]
        changed_gf = graphforge(18, missing_staging)
        with self.assertRaisesRegex(ControllerError, "staging authority"):
            assemble_rung_evidence(
                root=ROOT, scale=18, graphforge=changed_gf, benchexec=benchexec(changed_gf)
            )

    def test_receipt_authorities_are_bound_to_their_ordinary_phases(self) -> None:
        cases = (
            ("ingest", "query", 0),
            ("export", "query", 0),
            ("reopen_proof", "query", -1),
        )
        for source_phase, destination_phase, index in cases:
            with self.subTest(source_phase=source_phase):
                receipts = authoritative_receipts(18)
                moved = receipts[source_phase].pop(index)
                receipts[destination_phase].append(moved)
                gf = graphforge(18, receipts)
                with self.assertRaisesRegex(ControllerError, "missing, moved, or ambiguous"):
                    assemble_rung_evidence(
                        root=ROOT, scale=18, graphforge=gf, benchexec=benchexec(gf)
                    )
        for phase, index in (("ingest", 0), ("export", 0), ("reopen_proof", -1)):
            with self.subTest(duplicate=phase):
                receipts = authoritative_receipts(18)
                receipts[phase].append(copy.deepcopy(receipts[phase][index]))
                gf = graphforge(18, receipts)
                with self.assertRaisesRegex(ControllerError, "missing, moved, or ambiguous"):
                    assemble_rung_evidence(
                        root=ROOT, scale=18, graphforge=gf, benchexec=benchexec(gf)
                    )

    def test_storage_and_query_receipts_reject_global_moves_and_duplicates(self) -> None:
        cases = (
            ("reopen", "query", "graphforge-storage-attribution-command/1", "moved"),
            ("reopen", "query", "graphforge-storage-attribution-command/1", "duplicated"),
            ("recount", "admission", "graphforge-result-sink/2", "moved"),
            ("query", "export", "graphforge-result-sink/2", "duplicated"),
        )
        for source, destination, contract, mutation in cases:
            with self.subTest(contract=contract, mutation=mutation):
                receipts = authoritative_receipts(18)
                selected = next(
                    receipt for receipt in receipts[source] if receipt.get("contract") == contract
                )
                if mutation == "moved":
                    receipts[source].remove(selected)
                receipts.setdefault(destination, []).append(copy.deepcopy(selected))
                gf = graphforge(18, receipts)
                with self.assertRaisesRegex(ControllerError, "inventory"):
                    assemble_rung_evidence(
                        root=ROOT, scale=18, graphforge=gf, benchexec=benchexec(gf)
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

        changed = {**gf, "profile_id": "graph500-s19-local"}
        (raw / "run.log").write_text(json.dumps(changed) + "\n")
        with self.assertRaisesRegex(ControllerError, "contradicts the run plan"):
            ingest_benchexec_result(root=ROOT, stage=stage, scale=18, plan=plan)

        (raw / "run.log").write_text(json.dumps(gf) + "\n")
        with self.assertRaisesRegex(ControllerError, "contradicts the run plan"):
            ingest_benchexec_result(
                root=ROOT,
                stage=stage,
                scale=18,
                plan=plan,
                profile_id="graph500-s19-local",
            )

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

    def test_fly_s18_s19_ladder_bundle_refuses_s20_until_rss_plateaus(self) -> None:
        """Real #900 Fly evidence must not authorize S20 while RSS grows across rungs."""
        bundle = ROOT / "fixtures" / "parity" / "ladder-bundle"
        for scale in (18, 19):
            source = bundle / f"s{scale}-rung.json"
            (self.output / source.name).write_text(source.read_text(encoding="utf-8"))
        capacity = self.base / "capacity.json"
        capacity.write_text(
            json.dumps(
                {
                    "physical_read_bytes_per_second": 1_000_000_000,
                    "physical_write_bytes_per_second": 500_000_000,
                    "reader_calls_per_second": 1_000_000,
                    "publication_work_per_second": 500_000,
                }
            )
        )
        path = write_s20_projection(ROOT, self.output, capacity)
        evidence = json.loads(path.read_text())
        self.assertEqual(evidence["decision"], "refused")
        self.assertFalse(evidence["checks"]["rss_bounded_or_plateaued"])
        self.assertFalse(evidence["checks"]["rss_headroom"])

    def test_staged_executables_are_verified_private_copies(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        profile = ROOT / "profiles/graph500/s18-local.json"
        stage = _safe_stage(
            ROOT, profile, self.executables, plan["identities"], self.base, scale=18
        )
        staged_gf = stage / "bin/gf"
        self.assertFalse(staged_gf.is_symlink())
        original = staged_gf.read_bytes()
        self.executables.gf.write_bytes(b"changed-after-planning")
        self.assertEqual(staged_gf.read_bytes(), original)
        with self.assertRaisesRegex(ControllerError, "staged executable identity mismatch"):
            _safe_stage(ROOT, profile, self.executables, plan["identities"], self.base, scale=18)

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

    def test_benchexec_sanitized_environment_keeps_only_the_tool_module_path(self) -> None:
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        stage = self.base / "benchexec-stage"
        stage.mkdir()
        (stage / "bin").mkdir()
        (stage / "benchmark.xml").write_text("fixture", encoding="utf-8")
        with patch("graphforge_bench.progressive_run.subprocess.run") as execute:
            execute.return_value.returncode = 0
            self.assertEqual(_run_benchexec(stage, self.executables, plan["identities"]), 0)
        command = execute.call_args.args[0]
        self.assertEqual(command[0], str(self.base / "benchexec"))
        self.assertEqual(command[1:3], ["--tool-directory", str(stage / "bin")])
        self.assertEqual(command[3:5], ["--full-access-dir", str(stage.resolve())])
        environment = execute.call_args.kwargs["env"]
        self.assertEqual(environment["PYTHONPATH"], str(ROOT / "harness"))
        self.assertEqual(set(environment), {"HOME", "LANG", "LC_ALL", "PATH", "PYTHONPATH"})
        self.assertEqual(environment["HOME"], str(stage / "home"))
        self.assertIn("/usr/local/bin", environment["PATH"])

    def test_bench_home_uses_provider_volume_when_mounted(self) -> None:
        stage = self.base / "stage"
        stage.mkdir()
        with patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True):
            self.assertEqual(_bench_home(stage), Path("/work"))

    def test_authority_staging_parent_uses_output_dir_on_mounted_work(self) -> None:
        with patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True):
            self.assertEqual(_authority_staging_parent(self.output), self.output)
        self.assertIsNone(_authority_staging_parent(self.output))

    def test_benchexec_container_flags_expose_mounted_work(self) -> None:
        stage = self.base / "stage"
        stage.mkdir()
        with (
            patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True),
        ):
            self.assertEqual(
                _benchexec_container_flags(stage),
                [
                    "--read-only-dir",
                    "/",
                    "--hidden-dir",
                    "/run",
                    "--hidden-dir",
                    "/tmp",
                    "--full-access-dir",
                    "/work",
                ],
            )
        self.assertEqual(
            _benchexec_container_flags(stage),
            ["--full-access-dir", str(stage.resolve())],
        )

    def test_rewrite_profile_for_provider_volume_uses_absolute_workspace(self) -> None:
        profile = (ROOT / "profiles/graph500/s18-local.json").read_text(encoding="utf-8")
        rewritten = _rewrite_profile_for_provider_volume(profile, 18)
        self.assertIn('"/work/workspace/s18/nodes.parquet"', rewritten)
        self.assertNotIn('"workspace/s18/nodes.parquet"', rewritten)

    def test_provider_volume_wraps_staged_executables_with_work_tmpdir(self) -> None:
        profile_path = ROOT / "profiles/graph500/s18-local.json"
        plan = build_plan(
            root=ROOT,
            output_dir=self.output,
            scale=18,
            commit=COMMIT,
            executables=self.executables,
        )
        with (
            patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True),
            patch("graphforge_bench.progressive_run._stage_benchmark_xml"),
        ):
            stage = _safe_stage(
                ROOT,
                profile_path,
                self.executables,
                plan["identities"],
                self.base,
                scale=18,
            )
        wrapper = (stage / "bin" / "gf").read_text(encoding="utf-8")
        self.assertIn('export TMPDIR="/work/tmp"', wrapper)
        self.assertTrue((stage / "bin" / "gf.real").is_file())
        self.assertEqual(oct(stage.stat().st_mode & 0o777), oct(0o777))
        with patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True):
            self.assertEqual(_benchexec_tool_directory(stage), stage / "bin")

    def test_provider_volume_keeps_four_gib_benchexec_memory(self) -> None:
        stage = self.base / "stage"
        stage.mkdir()
        with patch("graphforge_bench.progressive_run._provider_volume_mounted", return_value=True):
            from graphforge_bench.progressive_run import _stage_benchmark_xml

            _stage_benchmark_xml(ROOT, stage)
            xml = (stage / "benchmark.xml").read_text(encoding="utf-8")
            self.assertIn('memlimit="4 GB"', xml)
            self.assertNotIn('memlimit="16 GB"', xml)
            plan = build_plan(
                root=ROOT,
                output_dir=self.output,
                scale=18,
                commit=COMMIT,
                executables=self.executables,
            )
            (stage / "bin").mkdir()
            (stage / "benchmark.xml").write_text("fixture", encoding="utf-8")
            with (
                patch("graphforge_bench.progressive_run.subprocess.run") as execute,
                patch.object(Path, "mkdir"),
            ):
                execute.return_value.returncode = 0
                _run_benchexec(stage, self.executables, plan["identities"])
            command = execute.call_args.args[0]
            self.assertNotIn("--memorylimit", command)

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

    def test_repository_commit_reads_image_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            benchmarks = base / "benchmarks"
            benchmarks.mkdir()
            (base / "commit").write_text(COMMIT + "\n", encoding="ascii")
            self.assertEqual(repository_commit(benchmarks), COMMIT)


if __name__ == "__main__":
    unittest.main()
