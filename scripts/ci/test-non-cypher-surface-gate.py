#!/usr/bin/env python3
"""Regression tests for the non-Cypher release-surface omission gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("non-cypher-surface-gate.py")
SPEC = importlib.util.spec_from_file_location("non_cypher_surface_gate", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class SurfaceGateTests(unittest.TestCase):
    def manifest(self) -> dict:
        return json.loads(GATE.MANIFEST.read_text())

    def validate(self, manifest: dict) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps(manifest))
            return GATE.validate(path)

    def test_checked_in_inventory_is_complete(self) -> None:
        self.assertEqual(GATE.validate(), [])
        self.assertEqual(len(GATE.public_methods()), 300)
        self.assertEqual(len(GATE.algorithm_registry()), 94)

    def test_new_or_removed_public_method_fails_frozen_digest(self) -> None:
        manifest = self.manifest()
        manifest["public_method_digest"] = "0" * 64
        self.assertTrue(
            any("public method inventory changed" in error for error in self.validate(manifest))
        )

    def test_stale_method_override_fails(self) -> None:
        manifest = self.manifest()
        manifest["method_policy"]["overrides"]["GraphForge.removed"] = "introspection"
        self.assertTrue(
            any("stale method policy overrides" in error for error in self.validate(manifest))
        )

    def test_missing_registry_entries_fail(self) -> None:
        manifest = self.manifest()
        manifest["algorithm_registry"]["release-tested"]["ids"].pop()
        manifest["search_contracts"]["release-tested"]["ids"].pop()
        errors = self.validate(manifest)
        self.assertTrue(any("unclassified algorithm entries" in error for error in errors))
        self.assertTrue(any("missing required search contracts" in error for error in errors))

    def test_missing_and_duplicate_search_evidence_fail(self) -> None:
        manifest = self.manifest()
        moved = manifest["search_evidence_groups"]["find-modes"]["ids"].pop()
        errors = self.validate(manifest)
        self.assertTrue(any("search contracts without evidence group" in error for error in errors))
        manifest["search_evidence_groups"]["find-modes"]["ids"].append(moved)
        manifest["search_evidence_groups"]["freshness"]["ids"].append(moved)
        errors = self.validate(manifest)
        self.assertTrue(any("multiple evidence groups" in error for error in errors))

    def test_stale_test_reference_fails(self) -> None:
        manifest = self.manifest()
        manifest["algorithm_registry"]["release-tested"]["test_refs"] = [
            {"path": "missing.rs", "symbol": "never"}
        ]
        self.assertTrue(any("stale test path" in error for error in self.validate(manifest)))

    def test_skipped_test_reference_fails(self) -> None:
        manifest = self.manifest()
        manifest["search_contracts"]["release-tested"]["test_refs"] = [
            {
                "path": "crates/graphforge-api/tests/fixed_hop_limit.rs",
                "symbol": "release_livejournal_fixed_hop_limits",
            }
        ]
        self.assertTrue(
            any("referenced test is skipped" in error for error in self.validate(manifest))
        )

    def test_unassigned_and_duplicate_method_evidence_fail(self) -> None:
        manifest = self.manifest()
        moved = manifest["method_evidence_groups"]["algorithm"]["ids"].pop()
        errors = self.validate(manifest)
        self.assertTrue(any("without evidence group" in error for error in errors))

        manifest["method_evidence_groups"]["algorithm"]["ids"].append(moved)
        manifest["method_evidence_groups"]["knowledge"]["ids"].append(moved)
        errors = self.validate(manifest)
        self.assertTrue(any("multiple evidence groups" in error for error in errors))

    def test_broad_non_symbol_reference_fails(self) -> None:
        manifest = self.manifest()
        manifest["method_evidence_groups"]["algorithm"]["test_refs"] = [
            {"path": "crates/graphforge-api/src/lib.rs", "pattern": "#\\[cfg\\(test\\)\\]"}
        ]
        self.assertTrue(any("malformed or broad" in error for error in self.validate(manifest)))

    def test_exact_non_test_symbol_fails(self) -> None:
        manifest = self.manifest()
        manifest["method_evidence_groups"]["search-provider-rerank"]["test_refs"] = [
            {
                "path": "crates/graphforge-api/tests/search_public_surface.rs",
                "symbol": "add_paper",
            }
        ]
        self.assertTrue(any("not a test" in error for error in self.validate(manifest)))

    def test_checkpoint_receiver_cannot_borrow_graphforge_call_evidence(self) -> None:
        manifest = self.manifest()
        manifest["method_evidence_groups"]["checkpoint-view"]["test_refs"] = [
            {
                "path": "crates/graphforge-api/tests/algorithm_public_surface.rs",
                "symbol": (
                    "persisted_public_rank_is_exact_after_repeat_and_reopen_"
                    "and_unavailable_is_stable"
                ),
            }
        ]
        errors = self.validate(manifest)
        self.assertTrue(any("CheckpointView.rank is not called" in error for error in errors))

    def test_every_adjacency_read_has_exactly_one_visibility_guard(self) -> None:
        source = (GATE.ROOT / "crates/graphforge-api/src/lib.rs").read_text()
        calls = list(re.finditer(r"self\.adjacency_provider\.revalidate\(\);", source))
        self.assertEqual(len(calls), 12)
        for call in calls:
            prefix = source[max(0, call.start() - 240) : call.start()]
            self.assertEqual(prefix.count(".adjacency_visibility"), 1)
            self.assertRegex(
                prefix,
                r"let _adjacency_visibility = self\s*\.adjacency_visibility\s*"
                r"\.read\(\)\s*\.expect\(\"adjacency visibility lock poisoned\"\);\s*$",
            )


if __name__ == "__main__":
    unittest.main()
