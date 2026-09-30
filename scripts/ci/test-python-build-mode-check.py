#!/usr/bin/env python3
"""Regression coverage for the Cargo-test versus Python-extension boundary."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name("python-build-mode-check.py")
SPEC = importlib.util.spec_from_file_location("python_build_mode_check", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def metadata(*, extension: bool = False) -> dict:
    return {
        "packages": [{"id": name, "name": name} for name in CHECK.PYO3_CRATES]
        + [
            {
                "id": "binding",
                "name": "graphforge-bindings-py",
                "targets": [{"kind": ["cdylib"], "test": True}],
            }
        ],
        "resolve": {
            "nodes": [
                {
                    "id": name,
                    "features": ["abi3-py310"] + (["extension-module"] if extension else []),
                }
                for name in CHECK.PYO3_CRATES
            ]
        },
    }


class BuildModeTests(unittest.TestCase):
    def test_distinct_test_and_distribution_modes(self) -> None:
        CHECK.validate_metadata(metadata(), extension=False)
        CHECK.validate_metadata(metadata(extension=True), extension=True)

    def test_unconditional_or_indirect_extension_feature_fails_tests(self) -> None:
        # Resolved nodes expose both direct dependency features and default/alias
        # feature activation; looking only at manifest spelling misses the latter.
        for name in CHECK.PYO3_CRATES:
            with self.subTest(crate=name):
                graph = metadata()
                next(node for node in graph["resolve"]["nodes"] if node["id"] == name)[
                    "features"
                ].append("extension-module")
                with self.assertRaisesRegex(ValueError, "ordinary Rust tests"):
                    CHECK.validate_metadata(graph, extension=False)

    def test_packaging_must_enable_extension_and_keep_abi3(self) -> None:
        with self.assertRaisesRegex(ValueError, "extension packaging"):
            CHECK.validate_metadata(metadata(), extension=True)
        graph = metadata(extension=True)
        graph["resolve"]["nodes"][0]["features"].remove("abi3-py310")
        with self.assertRaisesRegex(ValueError, "ABI3"):
            CHECK.validate_metadata(graph, extension=True)

    def test_missing_resolved_crates_fail_closed(self) -> None:
        graph = metadata()
        graph["resolve"]["nodes"].pop()
        with self.assertRaisesRegex(ValueError, "missing resolved"):
            CHECK.validate_metadata(graph, extension=False)

    def test_disabling_binding_rust_tests_is_not_a_linkage_fix(self) -> None:
        graph = metadata()
        graph["packages"][-1]["targets"][0]["test"] = False
        with self.assertRaisesRegex(ValueError, "Rust lib tests must remain enabled"):
            CHECK.validate_metadata(graph, extension=False)


if __name__ == "__main__":
    unittest.main()
