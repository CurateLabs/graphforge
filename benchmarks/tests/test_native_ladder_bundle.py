from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from graphforge_bench.ladder_bundle_ingest import ingest_ladder_bundle
from graphforge_bench.native_ladder_bundle import (
    NativeBundleError,
    digest,
    native_receipts,
    validate_native_bundle,
)
from graphforge_bench.progressive_host_run import _result, inventory_work_root
from graphforge_bench.progressive_provider_attempt import CANONICAL_RUNGS
from graphforge_bench.progressive_qualification import load_profiles, project
from graphforge_bench.progressive_run import assemble_rung_evidence
from tests.test_progressive_host_run import host_capacity, host_result
from tests.test_progressive_run import authoritative_receipts, benchexec, graphforge

ROOT = Path(__file__).resolve().parents[1]


def write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def write_native_bundle(work: Path, scales: tuple[int, ...] = CANONICAL_RUNGS) -> Path:
    """Use native result/inventory producers around bounded synthetic phase evidence."""
    work.mkdir(parents=True, exist_ok=True)
    source = work / "evidence"
    source.mkdir()
    profiles = {item.scale: item for item in load_profiles()}
    for i, scale in enumerate(scales):
        identities = host_result(scale)["identities"]
        if scale >= 20:
            capacity = host_capacity()
            rates = {key: capacity[key] for key in capacity if key.endswith("_per_second")}
            preceding = [json.loads((source / f"s{s}-rung.json").read_bytes()) for s in scales[:i]]
            projection = project(profiles[scale], preceding, rates)
            path = source / f"s{scale}-projection.json"
            write(path, projection)
            identities["admitted_projection_sha256"] = digest(path)
        plan = {
            "schema": "graphforge-progressive-host-run-plan/1",
            "rung": f"S{scale}",
            "execution": "native_linux_benchexec_host",
            "identities": identities,
            "limits": {"wall_seconds": 14400, "memory_bytes": 4294967296, "cores": 16},
            "outputs": [
                f"s{scale}-{kind}.json"
                for kind in ("plan", "benchexec", "graphforge", "rung", "result")
            ],
            "claim": "engineering_evidence_only",
        }
        gf = graphforge(scale, authoritative_receipts(scale))
        gf["profile_id"] = identities["profile_id"]
        bench = benchexec(gf)
        rung = assemble_rung_evidence(
            root=ROOT,
            scale=scale,
            graphforge=gf,
            benchexec=bench,
            profile_id=identities["profile_id"],
            source="progressive_profile" if scale < 20 else "canonical_ladder",
        )
        artifacts = {}
        for kind, value in (
            ("plan", plan),
            ("benchexec", bench),
            ("graphforge", gf),
            ("rung", rung),
        ):
            path = source / f"s{scale}-{kind}.json"
            write(path, value)
            artifacts[f"{kind}_sha256"] = digest(path)
        write(source / f"s{scale}-result.json", _result(plan, "passed", None, artifacts))
    write(source / "work-root-inventory.json", inventory_work_root(work, source))
    return source


class NativeBundleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name) / "work"

    def test_real_producer_inventory_round_trip_and_ingestion(self) -> None:
        source = write_native_bundle(self.work)
        receipt = validate_native_bundle(source)
        self.assertTrue(receipt["complete"])
        self.assertEqual(receipt["scales"], list(CANONICAL_RUNGS))
        target = Path(self.temp.name) / "ingested"
        report = ingest_ladder_bundle(source, target)
        self.assertEqual(report["parity_comparisons"], 7)
        self.assertTrue(validate_native_bundle(target)["complete"])
        self.assertFalse((target / "manifest.json").exists())
        self.assertFalse((target / "teardown-inventory.json").exists())

    def test_prefix_is_valid_evidence_without_full_completion(self) -> None:
        source = write_native_bundle(self.work, (18, 19))
        self.assertFalse(validate_native_bundle(source)["complete"])

    def test_inventory_does_not_hide_tmp_failure_trees_links_or_special_files(self) -> None:
        source = write_native_bundle(self.work, (18,))
        (self.work / "workspace").mkdir()
        (self.work / "tmp").mkdir()
        self.assertTrue(inventory_work_root(self.work, source)["empty"])
        for name in ("tmp/payload", ".gf-host-authority-failed/payload", "unexpected"):
            path = self.work / name
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"payload")
        (self.work / "dangling").symlink_to("absent")
        outside = Path(self.temp.name) / "outside"
        outside.mkdir()
        (outside / "untouched").write_bytes(b"outside")
        (self.work / "linked").symlink_to(outside, target_is_directory=True)
        if os.name == "posix":
            os.mkfifo(self.work / "fifo")
        inventory = inventory_work_root(self.work, source)
        self.assertFalse(inventory["empty"])
        for name in (
            "tmp/payload",
            ".gf-host-authority-failed",
            "unexpected",
            "dangling",
            "linked",
        ):
            self.assertIn(name, inventory["entries"])
        self.assertNotIn("linked/untouched", inventory["entries"])
        if os.name == "posix":
            self.assertIn("fifo", inventory["entries"])
        write(source / "work-root-inventory.json", inventory)
        self.assertFalse(validate_native_bundle(source)["complete"])

    def test_inventory_cannot_exempt_entire_work_root(self) -> None:
        self.work.mkdir()
        with self.assertRaises(NativeBundleError):
            inventory_work_root(self.work, self.work)

    def test_identity_and_digest_mutations_fail_closed(self) -> None:
        source = write_native_bundle(self.work, (18, 19))
        for name, mutate in {
            "s18-rung.json": lambda d: d.update(live_edges=10),
            "s18-result.json": lambda d: d["identities"].update(commit="b" * 40),
            "s19-result.json": lambda d: d.update(
                status="failed", failure="benchexec_failed", artifacts=None
            ),
            "work-root-inventory.json": lambda d: d["result_sha256"].update(
                {"s18-result.json": "0" * 64}
            ),
        }.items():
            with self.subTest(name=name):
                path = source / name
                original = path.read_bytes()
                value = json.loads(original)
                mutate(value)
                write(path, value)
                with self.assertRaises(NativeBundleError):
                    validate_native_bundle(source)
                path.write_bytes(original)

    def test_missing_noncanonical_and_malformed_receipts_fail_closed(self) -> None:
        source = write_native_bundle(self.work, (18, 19))
        extra = source / "s018-rung.json"
        shutil.copy2(source / "s18-rung.json", extra)
        with self.assertRaises(NativeBundleError):
            native_receipts(source)
        extra.unlink()
        (source / "s19-result.json").unlink()
        with self.assertRaises(NativeBundleError):
            native_receipts(source)
        (source / "s19-result.json").write_text("[]")
        with self.assertRaises(NativeBundleError):
            native_receipts(source)

    def test_old_unbound_inventory_cannot_claim_complete_native_evidence(self) -> None:
        source = write_native_bundle(self.work)
        write(
            source / "work-root-inventory.json",
            {
                "schema": "graphforge-host-work-root-inventory/1",
                "host_profile_id": "local-linux-cgroups-v2",
                "workspace_entries": [],
                "empty": True,
            },
        )
        with self.assertRaises(NativeBundleError):
            validate_native_bundle(source)

    def test_rehashed_but_contradictory_receipts_still_fail(self) -> None:
        source = write_native_bundle(self.work)
        mutations = {
            "rung": lambda d: d["storage_attribution"]["counts"].update(imported_edges=7),
            "plan": lambda d: d["identities"].update(commit="b" * 40),
            "graphforge": lambda d: d["phases"][0].update(status="failed"),
            "projection": lambda d: d.update(decision="refused"),
        }
        for kind, mutate in mutations.items():
            with self.subTest(kind=kind):
                path = source / f"s26-{kind}.json"
                result_path = source / "s26-result.json"
                inventory_path = source / "work-root-inventory.json"
                backups = {p: p.read_bytes() for p in (path, result_path, inventory_path)}
                value = json.loads(path.read_bytes())
                mutate(value)
                write(path, value)
                result = json.loads(result_path.read_bytes())
                if kind == "projection":
                    result["identities"]["admitted_projection_sha256"] = digest(path)
                else:
                    result["artifacts"][f"{kind}_sha256"] = digest(path)
                write(result_path, result)
                inventory = json.loads(inventory_path.read_bytes())
                inventory["result_sha256"][result_path.name] = digest(result_path)
                write(inventory_path, inventory)
                with self.assertRaises(NativeBundleError):
                    validate_native_bundle(source)
                for p, original in backups.items():
                    p.write_bytes(original)

    def test_missing_and_mutated_ordinary_receipts_fail_after_rehashing(self) -> None:
        source = write_native_bundle(self.work, (18,))
        paths = [source / f"s18-{kind}.json" for kind in ("graphforge", "benchexec", "result")]
        paths.append(source / "work-root-inventory.json")
        backups = {path: path.read_bytes() for path in paths}
        cases = [
            "all_missing",
            "ingest",
            "recount",
            "query",
            "reopen",
            "reopen_proof",
            "imported_fingerprint",
        ]
        for case in cases:
            with self.subTest(case=case):
                for path, original in backups.items():
                    path.write_bytes(original)
                gf_path, bench_path, result_path, inventory_path = paths
                gf = json.loads(gf_path.read_bytes())
                phases = {phase["phase"]: phase for phase in gf["phases"]}
                if case == "all_missing":
                    for phase in phases.values():
                        phase.pop("receipts", None)
                elif case == "imported_fingerprint":
                    phases["reopen_proof"]["receipts"][2]["result_sha256"] = "e" * 64
                else:
                    phases[case].pop("receipts", None)
                write(gf_path, gf)
                bench = json.loads(bench_path.read_bytes())
                bench["graphforge"] = gf
                write(bench_path, bench)
                result = json.loads(result_path.read_bytes())
                result["artifacts"]["graphforge_sha256"] = digest(gf_path)
                result["artifacts"]["benchexec_sha256"] = digest(bench_path)
                write(result_path, result)
                inventory = json.loads(inventory_path.read_bytes())
                inventory["result_sha256"][result_path.name] = digest(result_path)
                write(inventory_path, inventory)
                with self.assertRaisesRegex(NativeBundleError, "ordinary lifecycle receipts"):
                    validate_native_bundle(source)


if __name__ == "__main__":
    unittest.main()
