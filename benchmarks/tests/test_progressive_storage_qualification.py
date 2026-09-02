from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from graphforge_bench.progressive_storage_qualification import (
    StorageQualificationError,
    _build_qualification,
    build,
    main,
    validate,
    validate_source_rung,
)
from tests.test_progressive_qualification import rung

VOLUME_BYTES = 500 * 1024**3
RESERVED_BYTES = 75 * 1024**3
COMMIT = subprocess.run(
    ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
    capture_output=True,
    check=True,
    text=True,
).stdout.strip()
IMAGE = "registry.fly.io/graphforge-bench@sha256:" + "1" * 64


class ProgressiveStorageQualificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def provider_rung(scale: int) -> dict:
        value = rung(scale)
        value["source"] = "canonical_ladder"
        value["profile_id"] = f"graph500-s{scale}-provider"
        return value

    def source_pair(self) -> list[dict]:
        return [self.provider_rung(20), self.provider_rung(22)]

    def write_bundle(self, value: dict) -> Path:
        scale = value["scale"]
        prefix = self.base / f"s{scale}"
        rung_path = prefix.with_name(f"s{scale}-rung.json")
        plan_path = prefix.with_name(f"s{scale}-plan.json")
        benchexec_path = prefix.with_name(f"s{scale}-benchexec.json")
        graphforge_path = prefix.with_name(f"s{scale}-graphforge.json")
        result_path = prefix.with_name(f"s{scale}-result.json")
        profile = Path(__file__).resolve().parents[1] / (
            f"profiles/graph500/s{scale}-provider.json"
        )
        identities = {
            "commit": COMMIT,
            "profile_id": f"graph500-s{scale}-provider",
            "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
            "image_digest": IMAGE,
            "generator": "sha256:" + "2" * 64,
            "generator_executable_sha256": "3" * 64,
            "gf_sha256": "4" * 64,
            "certify_sha256": "5" * 64,
            "benchexec_python_sha256": "6" * 64,
            "benchexec_version": "3.30",
            "admitted_plan_sha256": "7" * 64,
            "source_tree_sha256": "8" * 64,
        }
        plan = {
            "schema": "graphforge-progressive-provider-execution-plan/1",
            "rung": f"S{scale}",
            "execution": "provider_native_linux_benchexec",
            "identities": identities,
            "limits": {"wall_seconds": 14_400, "memory_bytes": 4_294_967_296, "cores": 16},
            "outputs": [
                f"s{scale}-plan.json",
                f"s{scale}-benchexec.json",
                f"s{scale}-graphforge.json",
                f"s{scale}-rung.json",
                f"s{scale}-result.json",
            ],
            "claim": "engineering_evidence_only",
        }
        rung_path.write_text(json.dumps(value), encoding="utf-8")
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        benchexec_path.write_text("{}\n", encoding="utf-8")
        graphforge_path.write_text("{}\n", encoding="utf-8")
        artifacts = {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in {
                "plan_sha256": plan_path,
                "benchexec_sha256": benchexec_path,
                "graphforge_sha256": graphforge_path,
                "rung_sha256": rung_path,
            }.items()
        }
        result = {
            "schema": "graphforge-progressive-provider-run-result/1",
            "rung": f"S{scale}",
            "status": "passed",
            "failure": None,
            "identities": identities,
            "artifacts": artifacts,
            "claim": "engineering_evidence_only",
        }
        result_path.write_text(json.dumps(result), encoding="utf-8")
        return rung_path

    def bound_pair(self, scales: tuple[int, int] = (20, 22)) -> list[Path]:
        return [self.write_bundle(self.provider_rung(scale)) for scale in scales]

    def result_anchors(self, paths: list[Path]) -> list[str]:
        result = []
        for path in paths:
            scale = json.loads(path.read_text(encoding="utf-8"))["scale"]
            result.append(
                hashlib.sha256((path.parent / f"s{scale}-result.json").read_bytes()).hexdigest()
            )
        return result

    def build_pair(self, scales: tuple[int, int] = (20, 22), **overrides: int) -> dict:
        paths = self.bound_pair(scales)
        return build(
            paths,
            provider_result_sha256=self.result_anchors(paths),
            expected_commit=COMMIT,
            expected_image_digest=IMAGE,
            volume_bytes=overrides.get("volume_bytes", VOLUME_BYTES),
            reserved_headroom_bytes=overrides.get("reserved_headroom_bytes", RESERVED_BYTES),
        )

    def test_two_complete_adjacent_rungs_produce_valid_exact_v3_evidence(self) -> None:
        low, high = self.source_pair()
        evidence = self.build_pair()
        validate(evidence)
        self.assertEqual(evidence["schema"], "graphforge-g500-ladder-qualification/3")
        self.assertEqual(evidence["projection"]["source_rungs"], ["S20", "S22"])
        self.assertEqual(
            evidence["projection"]["rate"],
            {
                "numerator_bytes": high["storage_attribution"]["lifecycle"][
                    "transient_peak_storage_bytes"
                ],
                "denominator_count": high["storage_attribution"]["counts"]["source_edges"],
            },
        )
        self.assertEqual(len(evidence["rungs"][0]["artifacts"]), 9)
        self.assertEqual(len(evidence["rungs"][0]["phases"]), 9)
        staging = next(
            item
            for item in evidence["rungs"][0]["artifacts"]
            if item["category"] == "construction_staging_spill"
        )
        self.assertEqual(
            staging["allocated_bytes"],
            low["storage_attribution"]["construction"]["staging"]["allocated_bytes"],
        )
        self.assertEqual(
            staging["transient_peak_allocated_bytes"],
            low["storage_attribution"]["construction"]["staging_transient_peak_allocated_bytes"],
        )

    def test_missing_portable_authority_and_historical_v1_are_rejected(self) -> None:
        missing = self.source_pair()[0]
        del missing["storage_attribution"]["portable_package"]
        with self.assertRaisesRegex(StorageQualificationError, "portable_package"):
            validate_source_rung(missing)
        historical = self.source_pair()[0]
        del historical["assembly_contract"]
        del historical["storage_attribution"]
        with self.assertRaisesRegex(StorageQualificationError, "historical rung"):
            validate_source_rung(historical)

    def test_malformed_category_and_application_phase_inventories_are_rejected(self) -> None:
        missing_category = self.source_pair()[0]
        del missing_category["storage_attribution"]["source"]["categories"]["other"]
        with self.assertRaisesRegex(StorageQualificationError, "categories"):
            validate_source_rung(missing_category)
        missing_phase = self.source_pair()[0]
        del missing_phase["storage_attribution"]["construction"]["application_io"]["phases"][
            "recovery_reauthentication"
        ]
        with self.assertRaisesRegex(StorageQualificationError, "application_io"):
            validate_source_rung(missing_phase)
        for malformed in (True, -1, "1"):
            with self.subTest(portable_allocation=malformed):
                invalid = self.source_pair()[0]
                invalid["storage_attribution"]["portable_package"]["allocation_allocated_bytes"] = (
                    malformed
                )
                with self.assertRaises(StorageQualificationError):
                    validate_source_rung(invalid)
        for field in ("staging", "staging_transient_peak_allocated_bytes"):
            with self.subTest(missing_construction_authority=field):
                missing_staging = self.source_pair()[0]
                del missing_staging["storage_attribution"]["construction"][field]
                with self.assertRaisesRegex(StorageQualificationError, field):
                    validate_source_rung(missing_staging)

    def test_one_rung_and_non_adjacent_rungs_are_rejected(self) -> None:
        single = [self.write_bundle(self.provider_rung(20))]
        with self.assertRaisesRegex(StorageQualificationError, "exactly two"):
            build(
                single,
                provider_result_sha256=self.result_anchors(single),
                expected_commit=COMMIT,
                expected_image_digest=IMAGE,
                volume_bytes=VOLUME_BYTES,
                reserved_headroom_bytes=RESERVED_BYTES,
            )
        nonadjacent = self.bound_pair((20, 24))
        with self.assertRaisesRegex(StorageQualificationError, "ordered adjacent"):
            build(
                nonadjacent,
                provider_result_sha256=self.result_anchors(nonadjacent),
                expected_commit=COMMIT,
                expected_image_digest=IMAGE,
                volume_bytes=VOLUME_BYTES,
                reserved_headroom_bytes=RESERVED_BYTES,
            )

    def test_reconciliation_contradictions_and_sensitive_content_are_rejected(self) -> None:
        contradictory = self.source_pair()[0]
        contradictory["storage_attribution"]["source"]["logical_bytes"] += 1
        with self.assertRaisesRegex(StorageQualificationError, "do not reconcile"):
            validate_source_rung(contradictory)
        unsafe = self.source_pair()[0]
        unsafe["storage_attribution"]["source"]["token"] = "must-not-escape"
        with self.assertRaisesRegex(StorageQualificationError, "sensitive evidence key"):
            validate_source_rung(unsafe)

    def test_every_other_category_numeric_must_be_zero(self) -> None:
        totals = {
            "logical_references": "logical_references",
            "logical_bytes": "logical_bytes",
            "physical_objects": "physical_objects",
            "physical_logical_bytes": "retained_logical_eof_bytes",
            "allocated_bytes": "allocated_physical_bytes",
        }
        for field, total in totals.items():
            with self.subTest(field=field):
                invalid = self.source_pair()[0]
                source = invalid["storage_attribution"]["source"]
                source["categories"]["other"][field] = 1
                source[total] += 1
                with self.assertRaises(StorageQualificationError):
                    validate_source_rung(invalid)

    def test_provider_result_binds_source_profile_commit_image_and_rung_bytes(self) -> None:
        for field, value in (
            ("source", "progressive_profile"),
            ("profile_id", "graph500-s20-local"),
        ):
            with self.subTest(field=field):
                invalid = self.provider_rung(20)
                invalid[field] = value
                with self.assertRaisesRegex(
                    StorageQualificationError, "canonical provider source/profile"
                ):
                    validate_source_rung(invalid)

        paths = self.bound_pair()
        wrong_commit = "9" * 40
        result_path = self.base / "s20-result.json"
        plan_path = self.base / "s20-plan.json"
        result = json.loads(result_path.read_text(encoding="utf-8"))
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        result["identities"]["commit"] = wrong_commit
        plan["identities"]["commit"] = wrong_commit
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        result["artifacts"]["plan_sha256"] = hashlib.sha256(plan_path.read_bytes()).hexdigest()
        result_path.write_text(json.dumps(result), encoding="utf-8")
        with self.assertRaisesRegex(StorageQualificationError, "expected commit"):
            build(
                paths,
                provider_result_sha256=self.result_anchors(paths),
                expected_commit=COMMIT,
                expected_image_digest=IMAGE,
                volume_bytes=VOLUME_BYTES,
                reserved_headroom_bytes=RESERVED_BYTES,
            )

        paths = self.bound_pair()
        anchors = self.result_anchors(paths)
        paths[0].write_text(paths[0].read_text(encoding="utf-8") + "\n", encoding="utf-8")
        result_path = self.base / "s20-result.json"
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["artifacts"]["rung_sha256"] = hashlib.sha256(paths[0].read_bytes()).hexdigest()
        result_path.write_text(json.dumps(result), encoding="utf-8")
        with self.assertRaisesRegex(StorageQualificationError, "external anchor"):
            build(
                paths,
                provider_result_sha256=anchors,
                expected_commit=COMMIT,
                expected_image_digest=IMAGE,
                volume_bytes=VOLUME_BYTES,
                reserved_headroom_bytes=RESERVED_BYTES,
            )

    def test_provider_result_anchors_are_complete_ordered_and_well_formed(self) -> None:
        paths = self.bound_pair()
        anchors = self.result_anchors(paths)
        for invalid in (anchors[:1], ["g" * 64, anchors[1]], list(reversed(anchors))):
            with self.subTest(anchors=invalid), self.assertRaises(StorageQualificationError):
                build(
                    paths,
                    provider_result_sha256=invalid,
                    expected_commit=COMMIT,
                    expected_image_digest=IMAGE,
                    volume_bytes=VOLUME_BYTES,
                    reserved_headroom_bytes=RESERVED_BYTES,
                )

    def test_v3_schema_rejects_three_or_four_rungs_directly(self) -> None:
        evidence = _build_qualification(
            self.source_pair(),
            volume_bytes=VOLUME_BYTES,
            reserved_headroom_bytes=RESERVED_BYTES,
        )
        for count in (3, 4):
            with self.subTest(count=count):
                invalid = copy.deepcopy(evidence)
                invalid["rungs"].extend(copy.deepcopy(evidence["rungs"][: count - 2]))
                with self.assertRaisesRegex(StorageQualificationError, "schema violation"):
                    validate(invalid)

    def test_s26_storage_headroom_refuses_and_cannot_be_relabelled_admit(self) -> None:
        baseline = self.build_pair(reserved_headroom_bytes=0)
        projected = baseline["projection"]["projected_lifecycle_peak_bytes"]
        refused = self.build_pair(volume_bytes=projected - 1, reserved_headroom_bytes=0)
        self.assertEqual(refused["projection"]["decision"], "refuse")
        self.assertEqual(refused["projection"]["headroom_bytes"], 0)
        contradiction = copy.deepcopy(refused)
        contradiction["projection"]["decision"] = "admit"
        with self.assertRaisesRegex(StorageQualificationError, "decision contradicts"):
            validate(contradiction)

    def test_cli_writes_only_the_validated_closed_contract(self) -> None:
        low_path, high_path = self.bound_pair()
        low_anchor, high_anchor = self.result_anchors([low_path, high_path])
        output = self.base / "qualification.json"
        self.assertEqual(
            main(
                [
                    str(low_path),
                    str(high_path),
                    str(output),
                    "--commit",
                    COMMIT,
                    "--image-digest",
                    IMAGE,
                    "--low-result-sha256",
                    low_anchor,
                    "--high-result-sha256",
                    high_anchor,
                    "--volume-bytes",
                    str(VOLUME_BYTES),
                    "--reserved-headroom-bytes",
                    str(RESERVED_BYTES),
                ]
            ),
            0,
        )
        evidence = json.loads(output.read_text(encoding="utf-8"))
        validate(evidence)
        self.assertNotIn(str(self.base), output.read_text(encoding="utf-8"))

    def test_cli_never_replaces_an_existing_qualification(self) -> None:
        low_path, high_path = self.bound_pair()
        low_anchor, high_anchor = self.result_anchors([low_path, high_path])
        output = self.base / "qualification.json"
        output.write_text("preserve-existing\n", encoding="utf-8")
        with self.assertRaises(SystemExit):
            main(
                [
                    str(low_path),
                    str(high_path),
                    str(output),
                    "--commit",
                    COMMIT,
                    "--image-digest",
                    IMAGE,
                    "--low-result-sha256",
                    low_anchor,
                    "--high-result-sha256",
                    high_anchor,
                    "--volume-bytes",
                    str(VOLUME_BYTES),
                    "--reserved-headroom-bytes",
                    str(RESERVED_BYTES),
                ]
            )
        self.assertEqual(output.read_text(encoding="utf-8"), "preserve-existing\n")
        self.assertEqual(list(self.base.glob(".qualification.json.*")), [])


if __name__ == "__main__":
    unittest.main()
