from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    list_gdc_suites,
    load_acquisition,
    load_pinned_identity,
    validate_acquisition,
    workspace_root,
)
from jsonschema import Draft202012Validator


class GdcContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.pin = load_pinned_identity(cls.root / "profiles" / "gdc" / "graphalytics-identity.json")
        cls.fixtures = cls.root / "fixtures" / "gdc"

    def test_schemas_are_draft2020_valid(self) -> None:
        for name in (
            "gdc-pinned-identity.json",
            "gdc-acquisition.json",
            "gdc-suite-evidence.json",
            "gdc-suite-declaration.json",
        ):
            schema = json.loads((self.root / "schemas" / name).read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)

    def test_suites_are_independently_selectable(self) -> None:
        suites = list_gdc_suites()
        self.assertEqual(
            [suite["suite_id"] for suite in suites],
            [
                "graphalytics",
                "snb-interactive",
                "snb-bi",
                "finbench-transaction",
                "spb",
            ],
        )
        for suite in suites:
            pin = load_pinned_identity(self.root / suite["pinned_identity"])
            self.assertEqual(pin["suite_id"], suite["suite_id"])
            self.assertEqual(pin["disposition"], suite["disposition"])

    def test_complete_acquisition_records_immutable_identities(self) -> None:
        acquisition = load_acquisition(self.fixtures / "complete" / "acquisition.json")
        evidence = validate_acquisition(self.pin, acquisition, self.fixtures / "complete")
        self.assertEqual(evidence["status"], "passed")
        self.assertIsNone(evidence["cause"])
        self.assertEqual(evidence["identities"]["spec"]["release"], "1.0.0-engineering-pin")
        self.assertEqual(
            evidence["identities"]["driver"]["commit"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        self.assertEqual(
            evidence["datasets"][0]["checksum_sha256"],
            "c0fd57903065bb4950244cc77023e9ce0803f723596922b114141f190ea521d8",
        )
        self.assertEqual(
            evidence["references"][0]["checksum_sha256"],
            "20368ad845455de99ef737732930bf7d07d9d44adde19a31ec7ba6ec332b25db",
        )

    def test_incomplete_provenance_is_rejected(self) -> None:
        incomplete = json.loads(
            (self.fixtures / "incomplete-provenance" / "pinned-identity.json").read_text(
                encoding="utf-8"
            )
        )
        acquisition = load_acquisition(self.fixtures / "incomplete-provenance" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(incomplete, acquisition, self.fixtures / "incomplete-provenance")
        self.assertEqual(raised.exception.cause, "incomplete_provenance")

        stripped = copy.deepcopy(self.pin)
        stripped["datasets"][0]["license"] = ""
        with self.assertRaises(GdcContractError) as raised_license:
            validate_acquisition(
                stripped,
                load_acquisition(self.fixtures / "complete" / "acquisition.json"),
                self.fixtures / "complete",
            )
        self.assertEqual(raised_license.exception.cause, "incomplete_provenance")

    def test_checksum_mismatch_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "checksum-mismatch" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, acquisition, self.fixtures / "checksum-mismatch")
        self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_missing_assets_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "missing-assets" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, acquisition, self.fixtures / "missing-assets")
        self.assertEqual(raised.exception.cause, "missing_assets")

    def test_reference_mismatch_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "reference-mismatch" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, acquisition, self.fixtures / "reference-mismatch")
        self.assertEqual(raised.exception.cause, "reference_mismatch")

    def test_identity_drift_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "identity-drift" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.pin, acquisition, self.fixtures / "identity-drift")
        self.assertEqual(raised.exception.cause, "identity_drift")

    def test_inventory_only_spb_shares_contracts_without_workload_assets(self) -> None:
        pin = load_pinned_identity(self.root / "profiles" / "gdc" / "spb-identity.json")
        acquisition = {
            "schema": "graphforge-gdc-acquisition/1",
            "suite_id": "spb",
            "recorded_spec": pin["spec"],
            "recorded_generator": None,
            "recorded_driver": None,
            "assets": [],
            "references": [],
        }
        evidence = validate_acquisition(pin, acquisition, Path())
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["disposition"], "inventory_only")
        self.assertEqual(evidence["datasets"], [])
        self.assertEqual(evidence["references"], [])

    def test_adapters_do_not_embed_shared_workload_semantics(self) -> None:
        module = (self.root / "harness" / "graphforge_bench" / "gdc_contracts.py").read_text(
            encoding="utf-8"
        )
        for forbidden in ("PageRank", "bfs_params", "interactive_query", "finbench_tcr"):
            self.assertNotIn(forbidden, module)


if __name__ == "__main__":
    unittest.main()
