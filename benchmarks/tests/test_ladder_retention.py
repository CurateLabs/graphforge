from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from graphforge_bench.ladder_retention import (
    MANIFEST_NAME,
    SUMMARY_NAME,
    LadderEvidenceRetentionError,
    main,
    retain_ladder_evidence,
)

COMMIT = "b" * 40
OTHER_COMMIT = "c" * 40


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_rung(source: Path, scale: int, *, projection: bool = False) -> None:
    payloads = {
        "plan": {"identities": {"commit": COMMIT}, "claim": "engineering_evidence_only"},
        "graphforge": {"failed_phase": None, "phases": [{"phase": "ingest", "status": "passed"}]},
        "result": {
            "claim": "engineering_evidence_only",
            "failure": None,
            "artifacts": {
                "benchexec_sha256": "d" * 64,
                "graphforge_sha256": "",
                "plan_sha256": "",
                "rung_sha256": "",
            },
        },
        "rung": {
            "assembly_contract": "graphforge-progressive-rung-assembly/3",
            "correctness": True,
        },
    }
    for suffix, payload in payloads.items():
        path = source / f"s{scale}-{suffix}.json"
        path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    (source / f"s{scale}-benchexec.json").write_text(
        json.dumps({"rung": scale, "runsets": []}) + "\n", encoding="utf-8"
    )
    if projection:
        (source / f"s{scale}-projection.json").write_text("{}\n", encoding="utf-8")
        plan_path = source / f"s{scale}-plan.json"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        plan["identities"]["admitted_projection_sha256"] = sha256(
            source / f"s{scale}-projection.json"
        )
        plan_path.write_text(json.dumps(plan) + "\n", encoding="utf-8")
    # Record the real digests once every receipt exists.
    result_path = source / f"s{scale}-result.json"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result["artifacts"] = {
        "benchexec_sha256": sha256(source / f"s{scale}-benchexec.json"),
        "graphforge_sha256": sha256(source / f"s{scale}-graphforge.json"),
        "plan_sha256": sha256(source / f"s{scale}-plan.json"),
        "rung_sha256": sha256(source / f"s{scale}-rung.json"),
    }
    result_path.write_text(json.dumps(result) + "\n", encoding="utf-8")


def write_evidence(source: Path, *, projection: bool = False) -> Path:
    source.mkdir(parents=True)
    for scale in (18, 20):
        write_rung(source, scale, projection=projection and scale == 20)
    return source


def write_summary(root: Path, name: str = "gf-clean-ladder.log") -> Path:
    path = root / name
    path.write_text(
        f"=== clean ladder {COMMIT}  2026-09-22T00:00:00Z\n"
        "S18: 4,194,304 edges  ingest  32.3s  129,975 edges/s  cores 0.82\n",
        encoding="utf-8",
    )
    return path


class LadderRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        self.addCleanup(self._temporary.cleanup)

    def test_retains_rung_result_receipts_summary_and_manifest(self) -> None:
        source = write_evidence(self.root / "clean-ab12-evidence", projection=True)
        summary = write_summary(self.root)
        destination = self.root / "retained" / COMMIT

        report = retain_ladder_evidence(destination, evidence_dir=source, summary_log=summary)

        self.assertEqual(report["commit"], COMMIT)
        self.assertEqual(report["schema"], "graphforge-ladder-evidence-retention/1")
        expected = {
            "s18-plan.json",
            "s18-graphforge.json",
            "s18-result.json",
            "s18-rung.json",
            "s20-plan.json",
            "s20-graphforge.json",
            "s20-projection.json",
            "s20-result.json",
            "s20-rung.json",
            SUMMARY_NAME,
        }
        self.assertEqual(set(report["retained_files"]), expected)
        for name in expected:
            self.assertTrue((destination / name).is_file(), name)
        self.assertFalse((destination / "s18-benchexec.json").exists())
        manifest = (destination / MANIFEST_NAME).read_text(encoding="utf-8")
        listed = {line.split("  ", 1)[1] for line in manifest.splitlines()}
        self.assertEqual(listed, expected)
        self.assertEqual(report["archive_digest"], sha256(destination / MANIFEST_NAME))

    def test_include_benchexec_retains_and_verifies_raw_output(self) -> None:
        source = write_evidence(self.root / "clean-ab12-evidence")
        destination = self.root / "retained" / COMMIT

        report = retain_ladder_evidence(destination, evidence_dir=source, include_benchexec=True)

        for scale in (18, 20):
            self.assertTrue((destination / f"s{scale}-benchexec.json").is_file())
        self.assertIn("s18-benchexec.json", report["retained_files"])

    def test_digest_mismatch_refuses_before_any_copy(self) -> None:
        source = write_evidence(self.root / "clean-ab12-evidence")
        rung = json.loads((source / "s18-rung.json").read_text(encoding="utf-8"))
        rung["correctness"] = False
        (source / "s18-rung.json").write_text(json.dumps(rung) + "\n", encoding="utf-8")
        destination = self.root / "retained"

        with self.assertRaisesRegex(LadderEvidenceRetentionError, "s18-rung.json does not match"):
            retain_ladder_evidence(destination, evidence_dir=source)
        self.assertEqual(list(destination.glob("*")), [])

    def test_retention_is_append_only(self) -> None:
        source = write_evidence(self.root / "clean-ab12-evidence")
        summary = write_summary(self.root)
        destination = self.root / "retained"
        retain_ladder_evidence(destination, evidence_dir=source, summary_log=summary)
        before = (destination / MANIFEST_NAME).read_bytes()

        with self.assertRaisesRegex(LadderEvidenceRetentionError, "append-only"):
            retain_ladder_evidence(destination, evidence_dir=source, summary_log=summary)
        self.assertEqual((destination / MANIFEST_NAME).read_bytes(), before)

    def test_missing_or_corrupt_projection_is_refused_before_copy(self) -> None:
        for missing in (False, True):
            with self.subTest(missing=missing):
                source = write_evidence(self.root / f"evidence-{missing}", projection=True)
                projection = source / "s20-projection.json"
                if missing:
                    projection.unlink()
                else:
                    projection.write_text('{"corrupted":true}\n', encoding="utf-8")
                destination = self.root / f"retained-{missing}"
                with self.assertRaisesRegex(LadderEvidenceRetentionError, "projection"):
                    retain_ladder_evidence(destination, evidence_dir=source)
                self.assertFalse(destination.exists())

    def test_rungs_must_agree_on_the_ladder_commit(self) -> None:
        source = write_evidence(self.root / "clean-ab12-evidence")
        plan = json.loads((source / "s20-plan.json").read_text(encoding="utf-8"))
        plan["identities"]["commit"] = OTHER_COMMIT
        (source / "s20-plan.json").write_text(json.dumps(plan) + "\n", encoding="utf-8")
        # Keep artifact digests truthful after the edit.
        result_path = source / "s20-result.json"
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["artifacts"]["plan_sha256"] = sha256(source / "s20-plan.json")
        result_path.write_text(json.dumps(result) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(LadderEvidenceRetentionError, "rungs disagree"):
            retain_ladder_evidence(self.root / "retained", evidence_dir=source)

    def test_summary_only_retention_resolves_commit_from_header(self) -> None:
        summary = write_summary(self.root)
        destination = self.root / "retained" / COMMIT

        report = retain_ladder_evidence(destination, summary_log=summary)

        self.assertEqual(report["commit"], COMMIT)
        self.assertEqual(report["retained_files"], [SUMMARY_NAME])
        self.assertEqual(report["archive_digest"], sha256(destination / MANIFEST_NAME))

    def test_conflicting_summary_or_explicit_commit_is_refused_before_copy(self) -> None:
        source = write_evidence(self.root / "evidence")
        summary = write_summary(self.root)
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                summary.write_text(f"=== clean ladder {OTHER_COMMIT}\n", encoding="utf-8")
                destination = self.root / f"retained-{explicit}"
                with self.assertRaisesRegex(LadderEvidenceRetentionError, "disagrees"):
                    retain_ladder_evidence(
                        destination,
                        evidence_dir=source,
                        summary_log=None if explicit else summary,
                        sha=OTHER_COMMIT if explicit else None,
                    )
                self.assertFalse(destination.exists())

    def test_commit_must_resolve(self) -> None:
        summary = self.root / "headerless.log"
        summary.write_text("S18: 4,194,304 edges  ingest  32.3s\n", encoding="utf-8")

        with self.assertRaisesRegex(
            LadderEvidenceRetentionError, "cannot resolve the ladder commit"
        ):
            retain_ladder_evidence(self.root / "retained", summary_log=summary)

    def test_explicit_short_sha_is_refused(self) -> None:
        summary = write_summary(self.root)
        with self.assertRaisesRegex(LadderEvidenceRetentionError, "40-hex"):
            retain_ladder_evidence(self.root / "retained", summary_log=summary, sha="abc")

    def test_nothing_to_retain_is_refused(self) -> None:
        with self.assertRaisesRegex(LadderEvidenceRetentionError, "nothing to retain"):
            retain_ladder_evidence(self.root / "retained")

    def test_missing_summary_log_is_refused(self) -> None:
        with self.assertRaisesRegex(LadderEvidenceRetentionError, "missing"):
            retain_ladder_evidence(
                self.root / "retained", summary_log=self.root / "absent.log", sha=COMMIT
            )

    def test_manifest_is_deterministic_across_destinations(self) -> None:
        first = write_evidence(self.root / "first")
        second = write_evidence(self.root / "second")
        # Identical bytes must produce identical manifests; cross-check via a
        # second source tree rather than re-running against one destination.
        shutil.copytree(first, second, dirs_exist_ok=True)
        one = retain_ladder_evidence(self.root / "one", evidence_dir=first)
        two = retain_ladder_evidence(self.root / "two", evidence_dir=second)
        self.assertEqual(one["archive_digest"], two["archive_digest"])

    def test_cli_refusal_exits_two(self) -> None:
        with mock.patch("sys.stdout", new=io.StringIO()) as stdout:
            self.assertEqual(main(["--destination", str(self.root / "retained")]), 2)
        self.assertIn("ladder evidence retention refused", stdout.getvalue())

    def test_repository_archives_contain_every_manifest_payload(self) -> None:
        archives = Path(__file__).resolve().parents[2] / "docs/development/evidence/ladder"
        manifests = sorted(archives.glob("*/MANIFEST.sha256"))
        self.assertTrue(manifests)
        for manifest in manifests:
            for line in manifest.read_text(encoding="utf-8").splitlines():
                digest, name = line.split("  ", 1)
                with self.subTest(archive=manifest.parent.name, payload=name):
                    payload = manifest.parent / name
                    self.assertTrue(payload.is_file(), f"missing retained payload: {payload}")
                    self.assertEqual(sha256(payload), digest)


if __name__ == "__main__":
    unittest.main()
