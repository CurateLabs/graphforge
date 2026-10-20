"""Recorded evidence is validated against the contracts of the binary that produced it."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from graphforge_bench import portable_receipt_contracts as contracts
from graphforge_bench.native_ladder_bundle import NativeBundleError, validate_native_bundle
from graphforge_bench.native_rung import NativeRungError, read_native_rung

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "fixtures/parity/ladder-bundle"
RECORDED_COMMIT = "114b1964874419b31d13820bfbdf72fe57286544"
OTHER_COMMIT = "0123456789abcdef0123456789abcdef01234567"
KINDS = ("plan", "benchexec", "graphforge", "rung", "result")


def synthetic_s18(target: Path, *, commit: str, retired_ids: bool) -> Path:
    """Copy the recorded S18 files, then re-bind digests so only the contract rule can fail."""
    for kind in KINDS:
        shutil.copy(BUNDLE / f"s18-{kind}.json", target / f"s18-{kind}.json")
    for kind in KINDS:
        path = target / f"s18-{kind}.json"
        text = path.read_text(encoding="utf-8").replace(RECORDED_COMMIT, commit)
        if not retired_ids:
            for family in ("export", "import", "verify"):
                text = text.replace(f"graphforge-portable-{family}/2", f"graphforge-portable-{family}/1")
        path.write_text(text, encoding="utf-8")
    result_path = target / "s18-result.json"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result["artifacts"] = {
        f"{kind}_sha256": hashlib.sha256((target / f"s18-{kind}.json").read_bytes()).hexdigest()
        for kind in ("plan", "benchexec", "graphforge", "rung")
    }
    result_path.write_text(json.dumps(result), encoding="utf-8")
    return target


class RecordedEvidenceContractTests(unittest.TestCase):
    def read(self, source: Path) -> None:
        read_native_rung(ROOT, source, 18)

    def synthetic(self, **kwargs: object) -> Path:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory, True)
        return synthetic_s18(directory, **kwargs)  # type: ignore[arg-type]

    def test_recorded_bundle_validates_against_its_own_era(self) -> None:
        self.assertEqual(contracts.contracts_for_commit(RECORDED_COMMIT).export, "graphforge-portable-export/2")
        self.assertTrue(validate_native_bundle(BUNDLE)["complete"])

    def test_recorded_commit_with_current_ids_is_refused(self) -> None:
        with self.assertRaisesRegex(NativeRungError, "not those emitted"):
            self.read(self.synthetic(commit=RECORDED_COMMIT, retired_ids=False))

    def test_new_commit_with_retired_ids_is_refused(self) -> None:
        with self.assertRaisesRegex(NativeRungError, "not those emitted"):
            self.read(self.synthetic(commit=OTHER_COMMIT, retired_ids=True))

    def test_new_commit_with_current_ids_validates(self) -> None:
        self.read(self.synthetic(commit=OTHER_COMMIT, retired_ids=False))

    def test_unmodified_synthetic_copy_of_the_recorded_rung_validates(self) -> None:
        self.read(self.synthetic(commit=RECORDED_COMMIT, retired_ids=True))

    def test_without_the_mapping_the_recorded_bundle_is_refused(self) -> None:
        with mock.patch.object(contracts, "RECORDED_EVIDENCE_CONTRACTS", {}):
            with self.assertRaisesRegex(NativeBundleError, "not those emitted"):
                validate_native_bundle(BUNDLE)


if __name__ == "__main__":
    unittest.main()
