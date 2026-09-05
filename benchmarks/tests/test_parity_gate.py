from __future__ import annotations

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.parity_gate import (
    assert_tiny_parity_ready,
    ladder_bundle_root,
    parity_gate_status,
)
from graphforge_bench.parity_gate_cli import main as parity_gate_main
from graphforge_bench.scale_parity import compare_ladder_bundle, workspace_root
from jsonschema import Draft202012Validator

EMPTY_OBSERVED = {
    "app_exists": False,
    "machines": 0,
    "volumes": 0,
    "secrets": 0,
}


def _write_complete_ladder(base: Path) -> Path:
    from tests.test_native_ladder_bundle import write_native_bundle

    source = write_native_bundle(base / "native-work")
    bundle = base / "fixtures" / "parity" / "ladder-bundle"
    shutil.rmtree(bundle)
    shutil.copytree(source, bundle)
    return bundle


def _temporary_fixture_root(temp_name: str) -> Path:
    base = Path(temp_name) / "benchmarks"
    shutil.copytree(workspace_root() / "fixtures" / "parity", base / "fixtures" / "parity")
    (base.parent / "Makefile").write_text("safe-target:\n\t@true\n", encoding="utf-8")
    return base


class ParityGateTests(unittest.TestCase):
    def test_tiny_parity_ready(self) -> None:
        assert_tiny_parity_ready()

    def test_gate_status_reports_retirement_ready(self) -> None:
        status = parity_gate_status()
        self.assertNotIn("ready_for_retirement", status)
        self.assertTrue(status["structural_retirement_ready"])
        self.assertTrue(status["prefix_parity_ready"])
        self.assertFalse(status["full_ladder_evidence_complete"])
        harness = next(
            row
            for row in status["criteria"]
            if row["name"] == "harness_authoritative_after_ladder_comparison"
        )
        self.assertFalse(harness["met"])
        self.assertEqual(harness["blocked_by"], "#900")
        legacy = next(
            row
            for row in status["criteria"]
            if row["name"] == "legacy_orchestration_retired_with_coverage"
        )
        self.assertTrue(legacy["met"])
        self.assertIn("legacy_present=False", legacy["evidence"])

    def test_historical_evidence_criterion_met(self) -> None:
        status = parity_gate_status()
        historical = next(
            row for row in status["criteria"] if row["name"] == "historical_evidence_readable"
        )
        self.assertTrue(historical["met"])

    def test_ingested_ladder_bundle_runs_comparisons(self) -> None:
        comparisons = compare_ladder_bundle(ladder_bundle_root())
        self.assertEqual(len(comparisons), 2)
        self.assertTrue(all(matrix.get("overall") for matrix in comparisons))

    def test_gate_status_json_serializable(self) -> None:
        payload = parity_gate_status()
        json.dumps(payload)

    def test_gate_status_matches_closed_schema(self) -> None:
        schema = json.loads(
            (
                workspace_root() / "schemas" / "scale-orchestration-parity-gate-status.json"
            ).read_text(encoding="utf-8")
        )
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(parity_gate_status())

    def test_exact_complete_ladder_reports_all_states_true(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            base = _temporary_fixture_root(temp_name)
            _write_complete_ladder(base)
            status = parity_gate_status(base)
        self.assertTrue(status["structural_retirement_ready"])
        self.assertTrue(status["prefix_parity_ready"])
        self.assertTrue(status["full_ladder_evidence_complete"])

    def test_certification_defects_do_not_conflate_independent_states(self) -> None:
        mutations = {
            "missing_terminal_rung": lambda bundle: (bundle / "s26-rung.json").unlink(),
            "result_identity_mismatch": lambda bundle: self._update_json(
                bundle / "s26-result.json", rung="S25"
            ),
            "incomplete_teardown": lambda bundle: self._update_json(
                bundle / "work-root-inventory.json",
                empty=False,
                entries=["tmp/payload"],
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp_name:
                base = _temporary_fixture_root(temp_name)
                bundle = _write_complete_ladder(base)
                mutate(bundle)
                status = parity_gate_status(base)
                self.assertTrue(status["structural_retirement_ready"])
                self.assertEqual(status["prefix_parity_ready"], name == "incomplete_teardown")
                self.assertFalse(status["full_ladder_evidence_complete"])

    def test_noncanonical_duplicate_and_out_of_order_rungs_fail_prefix(self) -> None:
        def noncanonical(bundle: Path) -> None:
            source = json.loads((bundle / "s20-rung.json").read_text(encoding="utf-8"))
            source["scale"] = 21
            (bundle / "s21-rung.json").write_text(json.dumps(source) + "\n", encoding="utf-8")

        def duplicate(bundle: Path) -> None:
            shutil.copy2(bundle / "s18-rung.json", bundle / "s018-rung.json")

        def out_of_order(bundle: Path) -> None:
            s18 = json.loads((bundle / "s18-rung.json").read_text(encoding="utf-8"))
            s19 = json.loads((bundle / "s19-rung.json").read_text(encoding="utf-8"))
            s18["scale"], s19["scale"] = s19["scale"], s18["scale"]
            (bundle / "s18-rung.json").write_text(json.dumps(s18) + "\n", encoding="utf-8")
            (bundle / "s19-rung.json").write_text(json.dumps(s19) + "\n", encoding="utf-8")

        for name, mutate in {
            "noncanonical": noncanonical,
            "duplicate": duplicate,
            "out_of_order": out_of_order,
        }.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp_name:
                base = _temporary_fixture_root(temp_name)
                bundle = _write_complete_ladder(base)
                mutate(bundle)
                status = parity_gate_status(base)
                self.assertTrue(status["structural_retirement_ready"])
                self.assertFalse(status["prefix_parity_ready"])
                self.assertFalse(status["full_ladder_evidence_complete"])

    @staticmethod
    def _update_json(path: Path, **updates: object) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        document.update(updates)
        path.write_text(json.dumps(document) + "\n", encoding="utf-8")

    def test_cli_ignores_incomplete_full_ladder_evidence(self) -> None:
        status = parity_gate_status()
        self.assertFalse(status["full_ladder_evidence_complete"])
        with (
            patch("graphforge_bench.parity_gate_cli.parity_gate_status", return_value=status),
            redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(parity_gate_main(), 0)
        self.assertNotIn("ready_for_retirement", output.getvalue())

    def test_cli_fails_only_for_structural_or_prefix_invariant(self) -> None:
        baseline = parity_gate_status()
        for field in ("structural_retirement_ready", "prefix_parity_ready"):
            with self.subTest(field=field):
                status = dict(baseline)
                status[field] = False
                with (
                    patch(
                        "graphforge_bench.parity_gate_cli.parity_gate_status",
                        return_value=status,
                    ),
                    redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(parity_gate_main(), 1)


if __name__ == "__main__":
    unittest.main()
