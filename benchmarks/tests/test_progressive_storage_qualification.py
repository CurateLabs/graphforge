from __future__ import annotations

import copy
import unittest

from graphforge_bench.progressive_storage_qualification import (
    StorageQualificationError,
    build,
    validate,
    validate_source_rung,
)
from tests.test_progressive_qualification import rung

VOLUME_BYTES = 500 * 1024**3
RESERVED_BYTES = 75 * 1024**3


class ProgressiveStorageQualificationTests(unittest.TestCase):
    def source_pair(self) -> list[dict]:
        return [rung(20), rung(22)]

    def test_two_complete_adjacent_rungs_produce_valid_exact_v3_evidence(self) -> None:
        low, high = self.source_pair()
        evidence = build(
            [low, high],
            volume_bytes=VOLUME_BYTES,
            reserved_headroom_bytes=RESERVED_BYTES,
        )
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

    def test_one_rung_and_non_adjacent_rungs_are_rejected(self) -> None:
        with self.assertRaisesRegex(StorageQualificationError, "exactly two"):
            build(
                [rung(20)],
                volume_bytes=VOLUME_BYTES,
                reserved_headroom_bytes=RESERVED_BYTES,
            )
        with self.assertRaisesRegex(StorageQualificationError, "ordered adjacent"):
            build(
                [rung(20), rung(24)],
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

    def test_s26_storage_headroom_refuses_and_cannot_be_relabelled_admit(self) -> None:
        baseline = build(
            self.source_pair(),
            volume_bytes=VOLUME_BYTES,
            reserved_headroom_bytes=0,
        )
        projected = baseline["projection"]["projected_lifecycle_peak_bytes"]
        refused = build(
            self.source_pair(),
            volume_bytes=projected - 1,
            reserved_headroom_bytes=0,
        )
        self.assertEqual(refused["projection"]["decision"], "refuse")
        self.assertEqual(refused["projection"]["headroom_bytes"], 0)
        contradiction = copy.deepcopy(refused)
        contradiction["projection"]["decision"] = "admit"
        with self.assertRaisesRegex(StorageQualificationError, "decision contradicts"):
            validate(contradiction)


if __name__ == "__main__":
    unittest.main()
