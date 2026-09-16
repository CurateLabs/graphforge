#!/usr/bin/env python3
"""Unit tests for Bazel binding packaging handoff (#7 / #720)."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

from assemble_bazel_binding_packages import (
    EXPORT_SURFACE_FILENAME,
    FORBIDDEN_RECOMPILE,
    assemble_node,
    assemble_python,
    main,
    pep427_wheel_filename,
    read_node_export_surface,
    resolve_python_wheel_out,
    synthesize_node_index_dts,
    synthesize_node_index_js,
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

    def test_resolve_python_wheel_out_directory_and_untagged_file(self) -> None:
        version = "0.5.2"
        tag = "cp310-abi3-manylinux_2_17_x86_64"
        expected = pep427_wheel_filename(version, tag)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(
                resolve_python_wheel_out(root, version, tag),
                root / expected,
            )
            untagged = root / "graphforge-bazel.whl"
            self.assertEqual(
                resolve_python_wheel_out(untagged, version, tag),
                root / expected,
            )
            tagged = root / expected
            self.assertEqual(resolve_python_wheel_out(tagged, version, tag), tagged)

    def test_assemble_python_emits_pep427_wheel_filename(self) -> None:
        package_root = ROOT / "crates" / "graphforge-bindings-py"
        version = (
            (package_root / "pyproject.toml")
            .read_text(encoding="utf-8")
            .split('version = "', 1)[1]
            .split('"', 1)[0]
        )
        tag = "cp310-abi3-manylinux_2_17_x86_64"
        with tempfile.TemporaryDirectory() as tmp:
            native = Path(tmp) / "libgraphforge_bindings_py.so"
            native.write_bytes(b"FAKE_NATIVE_PY_CDYLIB")
            out_dir = Path(tmp) / "dist"
            out_dir.mkdir()
            evidence = assemble_python(
                native=native,
                package_root=package_root,
                out=out_dir,
                wheel_tag=tag,
            )
            wheel = Path(evidence["wheel"])
            self.assertEqual(wheel.name, pep427_wheel_filename(version, tag))
            self.assertTrue(wheel.is_file())
            self.assertEqual(evidence["wheel_tag"], tag)
            self.assertEqual(evidence["recompiled"], "false")

            # Untagged --out basename must be rewritten to the tagged sibling.
            untagged = Path(tmp) / "graphforge-bazel.whl"
            evidence2 = assemble_python(
                native=native,
                package_root=package_root,
                out=untagged,
                wheel_tag=tag,
            )
            wheel2 = Path(evidence2["wheel"])
            self.assertEqual(wheel2.name, pep427_wheel_filename(version, tag))
            self.assertFalse(untagged.exists())
            self.assertTrue(wheel2.is_file())

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
            wheel = Path(evidence["wheel"])
            self.assertTrue(wheel.is_file())
            self.assertIn(evidence["wheel_tag"], wheel.name)
            with zipfile.ZipFile(wheel) as archive:
                names = set(archive.namelist())
                self.assertTrue(any(n.endswith("_graphforge_rs.abi3.so") for n in names))
                self.assertIn("graphforge/__init__.py", names)
                module = next(n for n in names if n.endswith("_graphforge_rs.abi3.so"))
                self.assertEqual(archive.read(module), b"FAKE_NATIVE_PY_CDYLIB")

    def test_export_surface_manifest_is_declared_and_well_formed(self) -> None:
        package_root = ROOT / "crates" / "graphforge-bindings-node"
        surface = read_node_export_surface(package_root)
        self.assertIn("GraphForge", surface)
        self.assertEqual(surface["GraphForge"], "class")
        self.assertEqual(surface["version"], "function")
        self.assertEqual(list(surface), sorted(surface))

    def test_export_surface_manifest_rejects_bad_declarations(self) -> None:
        for payload in (
            {},
            {"exports": {}},
            {"exports": {"not an identifier": "class"}},
            {"exports": {"GraphForge": "widget"}},
        ):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / EXPORT_SURFACE_FILENAME).write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )
                with self.assertRaises(SystemExit):
                    read_node_export_surface(root)

    def test_synthesized_loader_resolves_named_esm_and_cjs_imports(self) -> None:
        # #1360 — a dynamic mirror loop is invisible to cjs-module-lexer, so
        # `import { GraphForge } from '../index.js'` used to throw
        # `SyntaxError: Named export 'GraphForge' not found`.
        surface = {"GraphForge": "class", "runCli": "function", "version": "function"}
        body = synthesize_node_index_js("graphforge.linux-x64-gnu.node", surface)
        self.assertIn("const nativeBinding = require('./graphforge.linux-x64-gnu.node');", body)
        self.assertIn("module.exports = nativeBinding;", body)
        for name in surface:
            self.assertIn(f"module.exports.{name} = nativeBinding.{name};", body)
        self.assertNotIn("Object.keys(nativeBinding)", body)

        declarations = synthesize_node_index_dts(surface)
        self.assertIn("export declare class GraphForge {", declarations)
        self.assertIn("export declare function version(...args: any[]): any;", declarations)

        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp)
            stub = pkg / "native-stub.js"
            stub.write_text(
                "module.exports = {\n"
                "  GraphForge: class GraphForge {},\n"
                "  runCli() { return 0; },\n"
                "  version() { return '0.0.0-test'; },\n"
                "};\n",
                encoding="utf-8",
            )
            index = pkg / "index.js"
            index.write_text(
                synthesize_node_index_js("native-stub.js", surface),
                encoding="utf-8",
            )
            # Static ESM named import — the form the Binding RC smoke contract uses.
            esm = pkg / "named-import.mjs"
            esm.write_text(
                "import { GraphForge, runCli, version } from './index.js';\n"
                "if (typeof GraphForge !== 'function') process.exit(2);\n"
                "if (typeof runCli !== 'function') process.exit(3);\n"
                "new GraphForge();\n"
                "process.stdout.write(version());\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["node", str(esm)],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.stdout, "0.0.0-test")

            # CommonJS require of the same file.
            cjs = pkg / "named-require.cjs"
            cjs.write_text(
                "const { GraphForge, runCli, version } = require('./index.js');\n"
                "if (typeof GraphForge !== 'function') process.exit(2);\n"
                "if (typeof runCli !== 'function') process.exit(3);\n"
                "new GraphForge();\n"
                "process.stdout.write(version());\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["node", str(cjs)],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.stdout, "0.0.0-test")

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
                index_js = archive.read("index.js").decode("utf-8")
                self.assertIn("module.exports.version = nativeBinding.version;", index_js)
                self.assertIn("module.exports.GraphForge = nativeBinding.GraphForge;", index_js)
                index_dts = archive.read("index.d.ts").decode("utf-8")
                self.assertIn("export declare class GraphForge {", index_dts)
                self.assertIn(
                    "export declare function version(...args: any[]): any;",
                    index_dts,
                )

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
            self.assertTrue(Path(py_evidence["wheel"]).name.endswith("-cp310-abi3-win_amd64.whl"))

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
