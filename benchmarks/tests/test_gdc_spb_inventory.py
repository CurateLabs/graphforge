from __future__ import annotations

import json
import unittest

from graphforge_bench.gdc_contracts import (
    SPB_ACTIVATION_CRITERIA,
    SPB_INVENTORY_REASON,
    GdcContractError,
    assert_no_executable_spb_profile,
    list_gdc_suites,
    load_pinned_identity,
    suite_status,
    validate_acquisition,
    workspace_root,
)
from jsonschema import Draft202012Validator


class GdcSpbInventoryTests(unittest.TestCase):
    def test_benchmark_index_includes_spb_inventory_disposition(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        self.assertIn("spb", suites)
        self.assertEqual(suites["spb"]["disposition"], "inventory_only")
        index = (workspace_root() / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`spb`", index)
        self.assertIn("inventory_only", index)
        self.assertIn(SPB_INVENTORY_REASON, index)
        for criterion in SPB_ACTIVATION_CRITERIA:
            self.assertIn(criterion, index)

    def test_operator_status_reports_inventory_only_with_semantic_reason(self) -> None:
        status = suite_status("spb")
        self.assertEqual(status["schema"], "graphforge-gdc-suite-status/1")
        self.assertEqual(status["disposition"], "inventory_only")
        self.assertFalse(status["executable"])
        self.assertEqual(status["reason"], SPB_INVENTORY_REASON)
        self.assertEqual(status["activation_criteria"], list(SPB_ACTIVATION_CRITERIA))
        Draft202012Validator(
            json.loads(
                (workspace_root() / "schemas" / "gdc-suite-status.json").read_text(encoding="utf-8")
            )
        ).validate(status)

    def test_no_executable_spb_profile_is_advertised(self) -> None:
        assert_no_executable_spb_profile()
        for suite in list_gdc_suites():
            if suite["suite_id"] != "spb":
                continue
            self.assertEqual(suite["datasets"], [])
            self.assertEqual(suite["disposition"], "inventory_only")

    def test_activation_criteria_are_objective_and_testable(self) -> None:
        self.assertGreaterEqual(len(SPB_ACTIVATION_CRITERIA), 3)
        for criterion in SPB_ACTIVATION_CRITERIA:
            self.assertRegex(criterion, r"^[a-z][a-z0-9_]{0,80}$")
        status = suite_status("spb")
        # Inventory-only means activation criteria are disclosed but not claimed met.
        self.assertFalse(status["executable"])
        self.assertEqual(status["activation_criteria"], list(SPB_ACTIVATION_CRITERIA))

    def test_does_not_run_sparql_approximation(self) -> None:
        module = (workspace_root() / "harness" / "graphforge_bench" / "gdc_contracts.py").read_text(
            encoding="utf-8"
        )
        for forbidden in (
            "sparql_to_cypher",
            "approximate_spb",
            "rdf_literal_as_cypher",
            "run_spb(",
        ):
            self.assertNotIn(forbidden, module)
        pin = load_pinned_identity(workspace_root() / "profiles" / "gdc" / "spb-identity.json")
        fake_checksum = "c0fd57903065bb4950244cc77023e9ce0803f723596922b114141f190ea521d8"
        with self.assertRaises(GdcContractError):
            validate_acquisition(
                pin,
                {
                    "schema": "graphforge-gdc-acquisition/1",
                    "suite_id": "spb",
                    "recorded_spec": pin["spec"],
                    "recorded_generator": None,
                    "recorded_driver": None,
                    "assets": [
                        {
                            "id": "fake",
                            "path": "fake.txt",
                            "checksum_sha256": fake_checksum,
                            "license": "none",
                            "acquisition": "fixture",
                        }
                    ],
                    "references": [],
                },
                workspace_root(),
            )


if __name__ == "__main__":
    unittest.main()
