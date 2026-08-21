#!/usr/bin/env python3
"""Deterministic self-tests for the #842 closed surface gate."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("multi-ontology-surface-gate.py")
SPEC = importlib.util.spec_from_file_location("multi_ontology_surface_gate", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def _method(member: str) -> str:
    base, _ = GATE._adapter_base(member)
    return base.split(".", 1)[1]


class SurfaceGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = GATE.load_manifest(GATE.MANIFEST)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self._materialize_complete_synthetic_surface()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def _materialize_complete_synthetic_surface(self) -> None:
        operations = self.manifest["operations"]
        rust_methods = sorted({_method(item["rust"]) for item in operations})
        python_methods = sorted({_method(item["python"]) for item in operations})
        node_methods = sorted({GATE._snake(_method(item["node"])) for item in operations})
        cli_segments = sorted(
            {
                segment.replace("-", "_")
                for item in operations
                for segment in GATE._adapter_base(item["cli"])[0].split("/")
            }
        )
        rust_source = "\n".join(f"pub fn {name}() {{}}" for name in rust_methods)
        evidence_sources = {surface: [] for surface in GATE.SURFACES}
        for refs in self.manifest["case_evidence"].values():
            for surface, ref in refs.items():
                marker_lines = "\n".join(f'let _ = "{marker}";' for marker in ref["markers"])
                if ref["kind"] == "rust_test":
                    body = f"#[test]\nfn {ref['symbol']}() {{ {marker_lines} assert!(true); }}\n"
                elif ref["kind"] == "python_test":
                    body = (
                        f"def {ref['symbol']}():\n"
                        f"    markers = {ref['markers']!r}\n"
                        "    assert len(markers) == 2\n"
                    )
                else:
                    body = (
                        f'test("{ref["symbol"]}", () => {{ '
                        f"const markers = {ref['markers']!r}; "
                        "assert.equal(markers.length, 2); });\n"
                    )
                evidence_sources[surface].append(body)
        rust_source += "\n" + "\n".join(evidence_sources["rust"])
        self._write("crates/graphforge-api/src/multi_ontology.rs", rust_source)
        self._write(
            "crates/graphforge-bindings-py/src/lib.rs",
            "\n".join(f"fn {name}() {{}}" for name in python_methods),
        )
        self._write(
            "crates/graphforge-bindings-py/tests/multi_ontology.py",
            "\n".join(evidence_sources["python"]),
        )
        self._write(
            "crates/graphforge-bindings-node/src/lib.rs",
            "\n".join(f"pub fn {name}() {{}}" for name in node_methods),
        )
        self._write(
            "crates/graphforge-bindings-node/tests/multi-ontology.test.mjs",
            "\n".join(evidence_sources["node"]),
        )
        self._write("crates/graphforge-cli/src/ontology_cli.rs", " ".join(cli_segments))
        self._write(
            "crates/graphforge-cli/tests/multi_ontology.rs",
            "\n".join(evidence_sources["cli"]),
        )
        package_files: dict[str, list[str]] = {}
        for ref in self.manifest["packaged_artifacts"].values():
            package_files.setdefault(ref["workflow"], []).extend(ref["workflow_markers"])
            package_files.setdefault(ref["oracle"], []).extend(ref["oracle_markers"])
        for path, markers in package_files.items():
            existing_path = self.root / path
            existing = existing_path.read_text(encoding="utf-8") if existing_path.is_file() else ""
            self._write(path, existing + "\n" + "\n".join(markers))

    def test_complete_closed_inventory_passes(self) -> None:
        self.assertEqual([], GATE.validate(self.manifest, self.root))

    def test_missing_operation_and_surface_mapping_fail(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["operations"] = manifest["operations"][1:]
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("missing canonical operations" in error for error in errors))
        manifest = copy.deepcopy(self.manifest)
        del manifest["operations"][0]["cli"]
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("four surface mappings" in error for error in errors))

    def test_duplicate_and_default_unexposed_mappings_fail(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["operations"][1]["rust"] = manifest["operations"][0]["rust"]
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("duplicate member mapping" in error for error in errors))
        manifest = copy.deepcopy(self.manifest)
        manifest["operations"][0]["node"] = "GraphForge.default_unexposed"
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("default-unexposed" in error for error in errors))

    def test_stale_member_and_exact_evidence_fail(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["operations"][0]["python"] = "GraphForge.method_that_does_not_exist"
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("stale member" in error for error in errors))
        manifest = copy.deepcopy(self.manifest)
        manifest["case_evidence"]["cancellation"]["cli"]["symbol"] = "not_a_test"
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("stale Rust test symbol" in error for error in errors))

    def test_inventory_only_evidence_cannot_self_certify(self) -> None:
        ref = self.manifest["case_evidence"]["cancellation"]["python"]
        path = self.root / ref["path"]
        body = (
            f"def {ref['symbol']}():\n"
            "    manifest = 'multi-ontology-surface-v1.json'\n"
            "    assert manifest\n"
        )
        path.write_text(
            body,
            encoding="utf-8",
        )
        errors = GATE.validate(self.manifest, self.root)
        self.assertTrue(any("missing case markers" in error for error in errors))

    def test_empty_assertion_free_and_reused_test_evidence_fail(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["case_evidence"]["idempotent_replay"]["cli"] = copy.deepcopy(
            manifest["case_evidence"]["cancellation"]["cli"]
        )
        errors = GATE.validate(manifest, self.root)
        self.assertTrue(any("reused across conformance cases" in error for error in errors))
        ref = self.manifest["case_evidence"]["cancellation"]["rust"]
        path = self.root / ref["path"]
        source = path.read_text(encoding="utf-8")
        source = source.replace("assert!(true);", "let _ = 1;", 1)
        path.write_text(source, encoding="utf-8")
        errors = GATE.validate(self.manifest, self.root)
        self.assertTrue(any("assertion-free" in error for error in errors))

    def test_packaged_artifact_requires_workflow_and_oracle_markers(self) -> None:
        ref = self.manifest["packaged_artifacts"]["node_package"]
        (self.root / ref["workflow"]).write_text("npm pack", encoding="utf-8")
        errors = GATE.validate(self.manifest, self.root)
        self.assertTrue(any("package verification missing" in error for error in errors))

    def test_required_case_drift_fails(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_conformance_cases"].pop()
        errors = GATE.validate(manifest, self.root)
        self.assertIn("required conformance case inventory drifted", errors)

    def test_duplicate_json_member_is_rejected(self) -> None:
        path = self.root / "duplicate.json"
        path.write_text('{"contract":"a","contract":"b"}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate JSON member"):
            GATE.load_manifest(path)


if __name__ == "__main__":
    unittest.main()
