#!/usr/bin/env python3
"""Mutation-sensitive tests for the release load matrix."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("release-load-matrix.py")
SPEC = importlib.util.spec_from_file_location("release_load_matrix", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class ReleaseLoadMatrixTests(unittest.TestCase):
    def write(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_checked_in_contracts_are_total_and_generated_content_repeats(self) -> None:
        self.assertEqual(GATE.contract_errors(), [])
        with (
            tempfile.TemporaryDirectory() as first_dir,
            tempfile.TemporaryDirectory() as second_dir,
        ):
            first = GATE.generate(Path(first_dir))
            second = GATE.generate(Path(second_dir))
            self.assertEqual(len(first), 16)
            self.assertEqual(
                [(item["dataset_id"], item["content_sha256"]) for item in first],
                [(item["dataset_id"], item["content_sha256"]) for item in second],
            )
            for manifest in first:
                self.assertIn(manifest["size_class"], {"XS", "S", "M", "L", "XL"})
                self.assertGreater(manifest["persisted_bytes"], 0)
            self.assertEqual(first, second)

    def test_contract_mutations_fail_closed(self) -> None:
        taxonomy = GATE.load(GATE.TAXONOMY)
        matrix = GATE.load(GATE.MATRIX)
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            bad = copy.deepcopy(taxonomy)
            bad["density"]["formula"] = "edges/nodes"
            self.assertTrue(
                any(
                    "formula drift" in error
                    for error in GATE.contract_errors(
                        self.write(directory, "taxonomy.json", bad), GATE.MATRIX
                    )
                )
            )
            bad = copy.deepcopy(taxonomy)
            bad["datasets"] = [item for item in bad["datasets"] if item["id"] != "xs-dense-cyclic"]
            self.assertTrue(
                any(
                    "multiple materially different dense" in error
                    for error in GATE.contract_errors(
                        self.write(directory, "taxonomy.json", bad), GATE.MATRIX
                    )
                )
            )
            bad_matrix = copy.deepcopy(matrix)
            bad_matrix["workloads"].pop()
            self.assertTrue(
                any(
                    "unmapped public inventory" in error
                    for error in GATE.contract_errors(
                        GATE.TAXONOMY, self.write(directory, "matrix.json", bad_matrix)
                    )
                )
            )
            bad_matrix = copy.deepcopy(matrix)
            bad_matrix["workloads"][0]["dataset_classes"].pop()
            self.assertTrue(
                any(
                    "must cover XS" in error
                    for error in GATE.contract_errors(
                        GATE.TAXONOMY, self.write(directory, "matrix.json", bad_matrix)
                    )
                )
            )

    def report(self, identity: str, sha: str, dataset_sha: str, covered: list[str]) -> dict:
        digest = "a" * 64
        language = identity.split("/", 1)[0]
        matrix = GATE.load(GATE.MATRIX)
        _source, selectors = GATE.inventory(matrix)
        complete = sorted(set().union(*selectors.values()))
        probe_paths = {
            "rust": GATE.ROOT / "crates/gf-api/examples/release_load_probe.rs",
            "python": GATE.ROOT / "scripts/ci/release-load-python-probe.py",
            "node": GATE.ROOT / "crates/gf-bindings-node/tests/release-load-probe.mjs",
        }
        commands = {
            "rust": ["cargo", "test", "-p", "gf-api"],
            "python": [
                sys.executable,
                str(GATE.ROOT / "scripts/ci/run-python-binding-contract.py"),
            ],
            "node": ["pnpm", "--filter", "@graphforge/node", "test"],
        }
        return {
            "schema": GATE.REPORT_SCHEMA,
            "identity": identity,
            "source_sha": sha,
            "dataset_sha256": dataset_sha,
            "outcome": "passed",
            "attempt": 1,
            "sanitized_error": None,
            "parity_diff": [],
            "package": {
                "name": identity.split("/", 1)[0],
                "version": "0.5.0",
                "artifact_sha256": "b" * 64,
            },
            "platform": {"os": "test", "arch": "test"},
            "toolchain": {"name": "test", "version": "test"},
            "covered_inventory": covered,
            "provenance": {
                "schema": "graphforge-load-preflight/1",
                "language": language,
                "source_sha": sha,
                "artifact_sha256": "b" * 64,
                "inventory_sha256": GATE.sha256(GATE.canonical(complete)),
                "surface_manifest_sha256": GATE.sha256(
                    (GATE.ROOT / "tests/contracts/non-cypher-rust-surface.json").read_bytes()
                ),
                "adapter_sha256": GATE.sha256(
                    (GATE.ROOT / "scripts/ci/release-load-executor.py").read_bytes()
                ),
                "probe_sha256": GATE.sha256(probe_paths[language].read_bytes()),
                "command_sha256": GATE.sha256(GATE.canonical(commands[language])),
                "outcome": "passed",
                "elapsed_ns": 1,
                "output_sha256": "c" * 64,
            },
            "observations": {
                "elapsed_ns": 1,
                "peak_rss_bytes": 1,
                "output_rows": 1,
                "output_bytes": 1,
                "open_files": 1,
                "threads": 1,
                "tasks": 1,
                "persisted_bytes": 1,
                "temporary_bytes": 0,
                "cleanup": "complete",
                "reopen_equivalent": True,
            },
            "runner_observations": {"elapsed_ns": 1},
            "result": {
                "schema_sha256": digest,
                "rows_sha256": digest,
                "ordering_sha256": digest,
                "fingerprint": digest,
            },
        }

    def test_aggregate_requires_every_case_once_and_exact_parity(self) -> None:
        sha = "1" * 40
        matrix = GATE.load(GATE.MATRIX)
        _source, selectors = GATE.inventory(matrix)
        workloads = {item["id"]: item for item in matrix["workloads"]}
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            fixtures, reports = root / "fixtures", root / "reports"
            manifests = GATE.generate(fixtures)
            reports.mkdir()
            for identity in sorted(GATE.expected_cases(matrix, manifests)):
                _language, workload, dataset = identity.split("/", 2)
                covered = sorted(
                    set().union(
                        *(selectors[name] for name in workloads[workload]["inventory_selectors"])
                    )
                )
                report = self.report(
                    identity,
                    sha,
                    next(
                        item["content_sha256"]
                        for item in manifests
                        if item["dataset_id"] == dataset
                    ),
                    covered,
                )
                self.write(reports, identity.replace("/", "--") + ".json", report)
            bundle = GATE.aggregate(reports, fixtures, sha, root / "bundle.json")
            self.assertEqual(bundle["status"], "passed")
            self.assertEqual(len(bundle["cases"]), 144)

            fixture = fixtures / "xs-sparse-path.json"
            fixture_bytes = fixture.read_bytes()
            fixture.unlink()
            with self.assertRaisesRegex(ValueError, "fixture ledger mismatch"):
                GATE.aggregate(reports, fixtures, sha, root / "rejected.json")
            fixture.write_bytes(fixture_bytes)

            path = sorted(reports.glob("*.json"))[0]
            mutated = GATE.load(path)
            mutated["attempt"] = 2
            path.write_text(json.dumps(mutated))
            with self.assertRaisesRegex(ValueError, "retried"):
                GATE.aggregate(reports, fixtures, sha, root / "rejected.json")

    def test_report_rejects_sha_fixture_inventory_resources_and_parity_drift(self) -> None:
        sha = "1" * 40
        matrix = GATE.load(GATE.MATRIX)
        _source, selectors = GATE.inventory(matrix)
        workloads = {item["id"]: item for item in matrix["workloads"]}
        with tempfile.TemporaryDirectory() as raw:
            fixtures = Path(raw) / "fixtures"
            manifests = GATE.generate(fixtures)
            identity = "rust/m18-closed-algorithm-registry/xs-sparse-path"
            manifest = next(item for item in manifests if item["dataset_id"] == "xs-sparse-path")
            covered = sorted(selectors["m18-registry"])
            base = self.report(identity, sha, manifest["content_sha256"], covered)
            for key, value, message in (
                ("source_sha", "2" * 40, "SHA drift"),
                ("dataset_sha256", "2" * 64, "fingerprint drift"),
                ("covered_inventory", covered[:-1], "inventory coverage"),
            ):
                mutated = copy.deepcopy(base)
                mutated[key] = value
                with self.assertRaisesRegex(ValueError, message):
                    GATE.validate_report(
                        mutated, sha, {manifest["dataset_id"]: manifest}, workloads, selectors
                    )
            mutated = copy.deepcopy(base)
            mutated["observations"].pop("threads")
            with self.assertRaisesRegex(ValueError, "resource observations"):
                GATE.validate_report(
                    mutated, sha, {manifest["dataset_id"]: manifest}, workloads, selectors
                )
            mutated = copy.deepcopy(base)
            mutated["parity_diff"] = ["rows"]
            with self.assertRaisesRegex(ValueError, "parity drift"):
                GATE.validate_report(
                    mutated, sha, {manifest["dataset_id"]: manifest}, workloads, selectors
                )
            mutated = copy.deepcopy(base)
            mutated["observations"]["peak_rss_bytes"] = 1 << 63
            with self.assertRaisesRegex(ValueError, "bound exceeded"):
                GATE.validate_report(
                    mutated, sha, {manifest["dataset_id"]: manifest}, workloads, selectors
                )
            mutated = copy.deepcopy(base)
            mutated["provenance"]["probe_sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "native provenance drift"):
                GATE.validate_report(
                    mutated, sha, {manifest["dataset_id"]: manifest}, workloads, selectors
                )

    def test_case_disk_headroom_and_tmpdir_reclaim_are_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            free = GATE.ensure_case_disk_headroom(
                root, "node/m18-closed-algorithm-registry/l-dense-cyclic"
            )
            self.assertGreater(free, 0)
            with self.assertRaisesRegex(ValueError, "insufficient free disk"):
                GATE.ensure_case_disk_headroom(
                    root,
                    "node/m18-closed-algorithm-registry/l-dense-cyclic",
                    minimum=free + 1,
                )
            case_tmp = root / "tmp"
            case_tmp.mkdir()
            leftover = case_tmp / "gf-load-node-xyz"
            leftover.mkdir()
            (leftover / "marker").write_text("x", encoding="utf-8")
            (case_tmp / "stale.txt").write_text("y", encoding="utf-8")
            GATE.reclaim_case_tmpdir(case_tmp)
            self.assertEqual(list(case_tmp.iterdir()), [])

    def test_python_probe_bulk_schema_matches_required_nullability(self) -> None:
        # Alphabetically first Python matrix case is L-dense; nullable label
        # fails GF_BULK_VALIDATION(schema_mismatch) before any XS Python case.
        source = (GATE.ROOT / "scripts/ci/release-load-python-probe.py").read_text(encoding="utf-8")
        self.assertIn('pa.field("label", pa.utf8(), nullable=False)', source)
        self.assertIn('pa.field("rel_type", pa.utf8(), nullable=False)', source)
        self.assertIn('pa.field("source_uuid", pa.binary(16), nullable=False)', source)
        self.assertIn('pa.field("target_uuid", pa.binary(16), nullable=False)', source)
        self.assertNotIn('("label", pa.utf8())', source)
        self.assertNotIn('("rel_type", pa.utf8())', source)


if __name__ == "__main__":
    unittest.main()
