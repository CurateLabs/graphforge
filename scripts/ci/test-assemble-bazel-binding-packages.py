#!/usr/bin/env python3
"""Unit tests for Bazel binding packaging handoff (#7)."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from assemble_bazel_binding_packages import (
    FORBIDDEN_RECOMPILE,
    assemble_node,
    assemble_python,
    main,
)

ROOT = Path(__file__).resolve().parents[2]


class AssembleBazelBindingPackagesTests(unittest.TestCase):
    def test_forbidden_recompile_pattern_catches_tool_invocations(self) -> None:
        for token in (
            "maturin build --release",
            "maturin develop -m x",
            "napi build --platform",
            "cargo build -p graphforge-bindings-py",
            "cargo rustc -p graphforge-bindings-node",
        ):
            self.assertIsNotNone(FORBIDDEN_RECOMPILE.search(token), token)

    def test_main_refuses_recompile_looking_argv(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            main(
                [
                    "--language",
                    "python",
                    "--native",
                    "x.so",
                    "--package-root",
                    ".",
                    "--out",
                    "out.whl",
                    "maturin",
                    "build",
                ]
            )
        self.assertEqual(raised.exception.code, 2)

    def test_assemble_python_wheel_embeds_native_bytes(self) -> None:
        package_root = ROOT / "crates" / "graphforge-bindings-py"
        with tempfile.TemporaryDirectory() as tmp:
            native = Path(tmp) / "libgraphforge_bindings_py.dylib"
            native.write_bytes(b"FAKE_NATIVE_PY_CDYLIB")
            out = Path(tmp) / "graphforge-smoke.whl"
            evidence = assemble_python(
                native=native,
                package_root=package_root,
                out=out,
            )
            self.assertEqual(evidence["recompiled"], "false")
            self.assertTrue(out.is_file())
            with zipfile.ZipFile(out) as wheel:
                names = set(wheel.namelist())
                self.assertTrue(any(n.endswith("_graphforge_rs.abi3.so") for n in names))
                self.assertIn("graphforge/__init__.py", names)
                module = next(n for n in names if n.endswith("_graphforge_rs.abi3.so"))
                self.assertEqual(wheel.read(module), b"FAKE_NATIVE_PY_CDYLIB")

    def test_assemble_node_zip_embeds_native_bytes(self) -> None:
        package_root = ROOT / "crates" / "graphforge-bindings-node"
        with tempfile.TemporaryDirectory() as tmp:
            native = Path(tmp) / "libgraphforge_bindings_node.dylib"
            native.write_bytes(b"FAKE_NATIVE_NODE_CDYLIB")
            out = Path(tmp) / "node-smoke.zip"
            evidence = assemble_node(
                native=native,
                package_root=package_root,
                out=out,
            )
            self.assertEqual(evidence["recompiled"], "false")
            self.assertTrue(out.is_file())
            with zipfile.ZipFile(out) as archive:
                names = set(archive.namelist())
                self.assertIn("package.json", names)
                self.assertIn("index.js", names)
                self.assertIn("bazel-native-evidence.json", names)
                addon = evidence["addon"]
                self.assertIn(addon, names)
                self.assertEqual(archive.read(addon), b"FAKE_NATIVE_NODE_CDYLIB")
                body = json.loads(archive.read("bazel-native-evidence.json"))
                self.assertEqual(body["recompiled"], "false")

    def test_explicit_cross_platform_tags(self) -> None:
        py_root = ROOT / "crates" / "graphforge-bindings-py"
        node_root = ROOT / "crates" / "graphforge-bindings-node"
        with tempfile.TemporaryDirectory() as tmp:
            py_native = Path(tmp) / "libgraphforge_bindings_py.so"
            py_native.write_bytes(b"FAKE_PY")
            py_out = Path(tmp) / "win.whl"
            py_evidence = assemble_python(
                native=py_native,
                package_root=py_root,
                out=py_out,
                wheel_tag="cp310-abi3-win_amd64",
            )
            self.assertEqual(py_evidence["wheel_tag"], "cp310-abi3-win_amd64")

            node_native = Path(tmp) / "libgraphforge_bindings_node.so"
            node_native.write_bytes(b"FAKE_NODE")
            node_out = Path(tmp) / "linux-arm.zip"
            node_evidence = assemble_node(
                native=node_native,
                package_root=node_root,
                out=node_out,
                platform_tag="linux-arm64-gnu",
            )
            self.assertEqual(node_evidence["platform_tag"], "linux-arm64-gnu")
            self.assertEqual(node_evidence["addon"], "graphforge.linux-arm64-gnu.node")


if __name__ == "__main__":
    unittest.main()
