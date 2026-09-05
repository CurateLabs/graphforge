from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    list_gdc_suites,
    load_acquisition,
    load_pinned_identity,
    load_suite_declaration,
    resolve_pinned_identity,
    validate_acquisition,
    validate_suite_acquisition,
    workspace_root,
)
from jsonschema import Draft202012Validator


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class GdcContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.suite = load_suite_declaration(cls.root / "suites" / "gdc-graphalytics.json")
        cls.static_pin = load_pinned_identity(
            cls.root / "profiles" / "gdc" / "graphalytics-static-identity.json"
        )
        cls.live_pin = load_pinned_identity(
            cls.root / "profiles" / "gdc" / "graphalytics-live-identity.json"
        )
        cls.fixtures = cls.root / "fixtures" / "gdc"
        cls.live_fixture = cls.fixtures / "graphalytics-tiny" / "compatible"

    def test_schemas_are_draft2020_valid(self) -> None:
        for name in (
            "gdc-pinned-identity.json",
            "gdc-acquisition.json",
            "gdc-suite-evidence.json",
            "gdc-suite-declaration.json",
            "gdc-suite-status.json",
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
            for profile_path in suite.get("identity_profiles", {}).values():
                profile_pin = load_pinned_identity(self.root / profile_path)
                self.assertEqual(profile_pin["suite_id"], suite["suite_id"])
                self.assertEqual(profile_pin["disposition"], suite["disposition"])

    def test_graphalytics_identity_profiles_are_distinct_and_truthful(self) -> None:
        profiles = self.suite["identity_profiles"]
        self.assertEqual(
            profiles,
            {
                "static": "profiles/gdc/graphalytics-static-identity.json",
                "live": "profiles/gdc/graphalytics-live-identity.json",
            },
        )
        self.assertEqual(self.suite["pinned_identity"], profiles["live"])
        self.assertEqual({item["id"] for item in self.static_pin["datasets"]}, {"wiki-Talk"})
        self.assertEqual({item["id"] for item in self.live_pin["datasets"]}, {"ga-tiny"})
        self.assertEqual(self.static_pin["spec"]["release"], "historical-wiki-Talk-marker-v1")
        self.assertEqual(self.live_pin["spec"]["release"], "v1.0.5")
        self.assertEqual(
            self.live_pin["spec"]["commit"],
            "5cf6ae65d26c809f2e3e0dac4716f153c71dc639",
        )
        self.assertIsNone(self.static_pin["spec"]["commit"])
        self.assertIsNone(self.static_pin["driver"]["commit"])
        self.assertIsNone(self.live_pin["driver"]["commit"])
        self.assertNotEqual(self.static_pin["spec"], self.live_pin["spec"])
        self.assertNotEqual(self.static_pin["generator"], self.live_pin["generator"])
        self.assertNotEqual(self.static_pin["driver"], self.live_pin["driver"])

    def test_graphalytics_pins_and_acquisitions_have_no_fabricated_commits(self) -> None:
        forbidden = (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "1.0.0-engineering-pin",
        )
        paths = [
            self.root / "profiles" / "gdc" / "graphalytics-static-identity.json",
            self.root / "profiles" / "gdc" / "graphalytics-live-identity.json",
            *self.fixtures.glob("*/acquisition.json"),
            *self.fixtures.glob("graphalytics-tiny/*/acquisition.json"),
            self.fixtures / "incomplete-provenance" / "pinned-identity.json",
        ]
        for path in paths:
            text = path.read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, text, path)

    def test_complete_static_acquisition_records_immutable_identities(self) -> None:
        acquisition = load_acquisition(self.fixtures / "complete" / "acquisition.json")
        self.assertEqual(acquisition["identity_profile"], "static")
        evidence = validate_acquisition(self.static_pin, acquisition, self.fixtures / "complete")
        self.assertEqual(evidence["status"], "passed")
        self.assertIsNone(evidence["cause"])
        self.assertEqual(
            evidence["identities"]["spec"]["release"],
            "historical-wiki-Talk-marker-v1",
        )
        self.assertEqual(
            evidence["identities"]["driver"]["name"],
            "graphforge-gdc-graphalytics-static-engineering-driver",
        )
        self.assertIsNone(evidence["identities"]["driver"]["commit"])
        self.assertEqual(evidence["datasets"][0]["id"], "wiki-Talk")
        self.assertEqual(
            evidence["datasets"][0]["checksum_sha256"],
            "c0fd57903065bb4950244cc77023e9ce0803f723596922b114141f190ea521d8",
        )
        self.assertEqual(
            evidence["references"][0]["checksum_sha256"],
            "20368ad845455de99ef737732930bf7d07d9d44adde19a31ec7ba6ec332b25db",
        )

    def test_complete_live_acquisition_records_ga_tiny_identities(self) -> None:
        acquisition = load_acquisition(self.live_fixture / "acquisition.json")
        self.assertEqual(acquisition["identity_profile"], "live")
        evidence = validate_acquisition(self.live_pin, acquisition, self.live_fixture)
        self.assertEqual(evidence["status"], "passed")
        self.assertIsNone(evidence["cause"])
        self.assertEqual(evidence["identities"]["spec"]["release"], "v1.0.5")
        self.assertEqual(
            evidence["identities"]["spec"]["commit"],
            "5cf6ae65d26c809f2e3e0dac4716f153c71dc639",
        )
        self.assertEqual(evidence["datasets"][0]["id"], "ga-tiny")
        self.assertEqual(
            evidence["datasets"][0]["checksum_sha256"],
            "101e94eed764c6b0a4bbedf9c90a53aec9864fd7ef4942265fb0fb9dce21d5a3",
        )

    def test_suite_profile_selection_binds_each_correct_acquisition(self) -> None:
        static_acquisition = load_acquisition(self.fixtures / "complete" / "acquisition.json")
        live_acquisition = load_acquisition(self.live_fixture / "acquisition.json")
        static_evidence = validate_suite_acquisition(
            self.suite, static_acquisition, self.fixtures / "complete", self.root
        )
        live_evidence = validate_suite_acquisition(
            self.suite, live_acquisition, self.live_fixture, self.root
        )
        self.assertEqual(
            static_evidence["identities"]["spec"]["release"],
            "historical-wiki-Talk-marker-v1",
        )
        self.assertEqual(live_evidence["identities"]["spec"]["release"], "v1.0.5")
        self.assertEqual(
            resolve_pinned_identity(self.suite, static_acquisition)["datasets"][0]["id"],
            "wiki-Talk",
        )
        self.assertEqual(
            resolve_pinned_identity(self.suite, live_acquisition)["datasets"][0]["id"],
            "ga-tiny",
        )

    def test_cross_use_of_live_and_static_pins_is_identity_drift(self) -> None:
        static_acquisition = load_acquisition(self.fixtures / "complete" / "acquisition.json")
        live_acquisition = load_acquisition(self.live_fixture / "acquisition.json")
        with self.assertRaises(GdcContractError) as live_pin_static_assets:
            validate_acquisition(self.live_pin, static_acquisition, self.fixtures / "complete")
        self.assertEqual(live_pin_static_assets.exception.cause, "identity_drift")
        with self.assertRaises(GdcContractError) as static_pin_live_assets:
            validate_acquisition(self.static_pin, live_acquisition, self.live_fixture)
        self.assertEqual(static_pin_live_assets.exception.cause, "identity_drift")

        swapped_static = copy.deepcopy(static_acquisition)
        swapped_static["identity_profile"] = "live"
        with self.assertRaises(GdcContractError) as swapped_static_raised:
            validate_suite_acquisition(
                self.suite, swapped_static, self.fixtures / "complete", self.root
            )
        self.assertEqual(swapped_static_raised.exception.cause, "identity_drift")

        swapped_live = copy.deepcopy(live_acquisition)
        swapped_live["identity_profile"] = "static"
        with self.assertRaises(GdcContractError) as swapped_live_raised:
            validate_suite_acquisition(self.suite, swapped_live, self.live_fixture, self.root)
        self.assertEqual(swapped_live_raised.exception.cause, "identity_drift")

    def test_multi_profile_omitted_identity_profile_is_incomplete_provenance(self) -> None:
        live_omitted = copy.deepcopy(load_acquisition(self.live_fixture / "acquisition.json"))
        del live_omitted["identity_profile"]
        with self.assertRaises(GdcContractError) as live_raised:
            validate_suite_acquisition(self.suite, live_omitted, self.live_fixture, self.root)
        self.assertEqual(live_raised.exception.cause, "incomplete_provenance")
        self.assertIn("identity_profile", str(live_raised.exception))

        static_omitted = copy.deepcopy(
            load_acquisition(self.fixtures / "complete" / "acquisition.json")
        )
        del static_omitted["identity_profile"]
        with self.assertRaises(GdcContractError) as static_raised:
            validate_suite_acquisition(
                self.suite, static_omitted, self.fixtures / "complete", self.root
            )
        self.assertEqual(static_raised.exception.cause, "incomplete_provenance")
        self.assertIn("identity_profile", str(static_raised.exception))

        single_pin_cases = (
            ("gdc-snb-interactive.json", "snb-interactive-tiny/compatible"),
            ("gdc-snb-bi.json", "snb-bi-tiny/compatible"),
            ("gdc-finbench-transaction.json", "finbench-transaction-tiny/compatible"),
        )
        for suite_name, fixture_rel in single_pin_cases:
            suite = load_suite_declaration(self.root / "suites" / suite_name)
            self.assertNotIn("identity_profiles", suite)
            fixture = self.fixtures / fixture_rel
            acquisition = load_acquisition(fixture / "acquisition.json")
            self.assertNotIn("identity_profile", acquisition)
            evidence = validate_suite_acquisition(suite, acquisition, fixture, self.root)
            self.assertEqual(evidence["status"], "passed", suite_name)

        spb = load_suite_declaration(self.root / "suites" / "gdc-spb.json")
        self.assertNotIn("identity_profiles", spb)
        pin = load_pinned_identity(self.root / spb["pinned_identity"])
        spb_acquisition = {
            "schema": "graphforge-gdc-acquisition/1",
            "suite_id": "spb",
            "recorded_spec": pin["spec"],
            "recorded_generator": None,
            "recorded_driver": None,
            "assets": [],
            "references": [],
        }
        self.assertNotIn("identity_profile", spb_acquisition)
        spb_evidence = validate_suite_acquisition(spb, spb_acquisition, Path(), self.root)
        self.assertEqual(spb_evidence["status"], "passed")

    def test_checksums_bind_actual_static_and_live_assets(self) -> None:
        static_dataset = self.fixtures / "complete" / "wiki-Talk.txt"
        static_reference = self.fixtures / "complete" / "wiki-Talk-bfs.ref"
        live_dataset = self.live_fixture / "ga-tiny.edges"
        self.assertEqual(
            _sha256(static_dataset),
            self.static_pin["datasets"][0]["checksum_sha256"],
        )
        self.assertEqual(
            _sha256(static_reference),
            self.static_pin["references"][0]["checksum_sha256"],
        )
        self.assertEqual(
            _sha256(live_dataset),
            self.live_pin["datasets"][0]["checksum_sha256"],
        )
        for item in self.live_pin["references"]:
            path = self.live_fixture / "references" / f"ga-tiny-{item['workload_key']}.ref"
            self.assertEqual(_sha256(path), item["checksum_sha256"], item["workload_key"])

        static_acquisition = load_acquisition(self.fixtures / "complete" / "acquisition.json")
        live_acquisition = load_acquisition(self.live_fixture / "acquisition.json")
        with tempfile.TemporaryDirectory() as tmp:
            mutated_static = Path(tmp) / "static"
            shutil.copytree(self.fixtures / "complete", mutated_static)
            (mutated_static / "wiki-Talk.txt").write_text(
                "mutated-static-bytes\n",
                encoding="utf-8",
            )
            with self.assertRaises(GdcContractError) as static_raised:
                validate_acquisition(self.static_pin, static_acquisition, mutated_static)
            self.assertEqual(static_raised.exception.cause, "checksum_mismatch")

            mutated_live = Path(tmp) / "live"
            shutil.copytree(self.live_fixture, mutated_live)
            (mutated_live / "ga-tiny.edges").write_text("1 2\n", encoding="utf-8")
            with self.assertRaises(GdcContractError) as live_raised:
                validate_acquisition(self.live_pin, live_acquisition, mutated_live)
            self.assertEqual(live_raised.exception.cause, "checksum_mismatch")

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

        stripped = copy.deepcopy(self.static_pin)
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
            validate_acquisition(self.static_pin, acquisition, self.fixtures / "checksum-mismatch")
        self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_missing_assets_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "missing-assets" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.static_pin, acquisition, self.fixtures / "missing-assets")
        self.assertEqual(raised.exception.cause, "missing_assets")

    def test_reference_mismatch_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "reference-mismatch" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.static_pin, acquisition, self.fixtures / "reference-mismatch")
        self.assertEqual(raised.exception.cause, "reference_mismatch")

    def test_identity_drift_fixture(self) -> None:
        acquisition = load_acquisition(self.fixtures / "identity-drift" / "acquisition.json")
        with self.assertRaises(GdcContractError) as raised:
            validate_acquisition(self.static_pin, acquisition, self.fixtures / "identity-drift")
        self.assertEqual(raised.exception.cause, "identity_drift")

    def test_content_addressed_tool_identity_is_narrowly_snb_bi_only(self) -> None:
        pin_path = self.root / "profiles" / "gdc" / "snb-bi-identity.json"
        pin = load_pinned_identity(pin_path)
        self.assertEqual(
            pin["generator"]["provenance_kind"],
            "content_addressed_synthetic",
        )
        self.assertNotIn("release", pin["generator"])
        self.assertNotIn("commit", pin["generator"])
        self.assertEqual(pin["driver"]["provenance_kind"], "repository_source")
        self.assertNotIn("release", pin["driver"])
        self.assertNotIn("commit", pin["driver"])

        acquisition_path = self.fixtures / "snb-bi-tiny" / "compatible" / "acquisition.json"
        acquisition = load_acquisition(acquisition_path)
        evidence = validate_acquisition(
            pin,
            acquisition,
            self.fixtures / "snb-bi-tiny" / "compatible",
        )
        self.assertEqual(evidence["status"], "passed")

        pin_schema = Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-pinned-identity.json").read_text(encoding="utf-8")
            )
        )
        foreign_pin = copy.deepcopy(pin)
        foreign_pin["suite_id"] = "graphalytics"
        self.assertTrue(list(pin_schema.iter_errors(foreign_pin)))

        acquisition_schema = Draft202012Validator(
            json.loads((self.root / "schemas" / "gdc-acquisition.json").read_text(encoding="utf-8"))
        )
        foreign_acquisition = copy.deepcopy(acquisition)
        foreign_acquisition["suite_id"] = "graphalytics"
        self.assertTrue(list(acquisition_schema.iter_errors(foreign_acquisition)))

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
