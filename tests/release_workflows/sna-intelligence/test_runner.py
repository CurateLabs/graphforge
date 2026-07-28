"""Mutation-sensitive tests for release-workflow binding provenance."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest

RUNNER = Path(__file__).with_name("run.py")
SPEC = importlib.util.spec_from_file_location("sna_intelligence_runner", RUNNER)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class BindingProvenanceTests(unittest.TestCase):
    def evidence(self, root: Path, native_name: str) -> dict[str, object]:
        native = root / "graphforge" / native_name
        native.parent.mkdir(parents=True)
        native.write_bytes(b"artifact")
        package = native.parent / "__init__.py"
        package.write_text("", encoding="utf-8")
        return {
            "commit_sha": "1" * 40,
            "wheel_sha256": "a" * 64,
            "package_version": "0.5.0.dev0",
            "package_module_path": str(package),
            "native_module_path": str(native),
            "native_module_sha256": hashlib.sha256(b"artifact").hexdigest(),
        }

    def test_stale_commit_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = self.evidence(root, "_graphforge_rs.so")
            with self.assertRaisesRegex(ValueError, "stale"):
                runner.validate_binding_provenance(evidence, "2" * 40, "a" * 64, root)

    def test_pure_python_fallback_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = self.evidence(root, "_graphforge_rs.py")
            with self.assertRaisesRegex(ValueError, "native extension"):
                runner.validate_binding_provenance(evidence, "1" * 40, "a" * 64, root)

    def test_extension_outside_isolated_environment_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            evidence = self.evidence(base / "stale", "_graphforge_rs.so")
            isolated = base / "isolated"
            isolated.mkdir()
            with self.assertRaisesRegex(ValueError, "outside"):
                runner.validate_binding_provenance(evidence, "1" * 40, "a" * 64, isolated)


if __name__ == "__main__":
    unittest.main()
