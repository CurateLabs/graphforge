#!/usr/bin/env python3
"""Unit tests for Bazel binding packaging handoff (#7 / #720)."""

from __future__ import annotations

from email.parser import Parser
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

from assemble_bazel_binding_packages import (
    _NAPI_PLATFORM_TAGS,
    EXPORT_SURFACE_FILENAME,
    FORBIDDEN_RECOMPILE,
    PYTHON_LEGAL_FILES,
    _napi_host_tag_map,
    _napi_platform_tag,
    assemble_node,
    assemble_python,
    main,
    pep427_wheel_filename,
    read_node_export_surface,
    read_node_package_identity,
    read_python_project_metadata,
    resolve_python_wheel_out,
    synthesize_node_index_dts,
    synthesize_node_index_js,
)
import release_candidate_manifest
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PY_PACKAGE_ROOT = ROOT / "crates" / "graphforge-bindings-py"
LINUX_WHEEL_TAG = "cp310-abi3-manylinux_2_17_x86_64"


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
        body = synthesize_node_index_js(
            "graphforge.linux-x64-gnu.node",
            surface,
            package_name="@curatelabs/graphforge",
            binary_name="graphforge",
        )
        self.assertIn("addCandidate('./graphforge.linux-x64-gnu.node');", body)
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
                synthesize_node_index_js(
                    "native-stub.js",
                    surface,
                    package_name="@curatelabs/graphforge",
                    binary_name="graphforge",
                ),
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

    def test_synthesized_loader_resolves_the_optional_platform_package(self) -> None:
        # #1369 — the published main package ships no addon: `files` excludes
        # *.node and `napi artifacts` moves every addon into
        # npm/<platformArchABI>/. A loader whose only candidate is the sibling
        # file threw `Cannot find module './graphforge.<tag>.node'` on a clean
        # consumer install.
        surface = {"GraphForge": "class", "runCli": "function", "version": "function"}
        host_tag = _napi_platform_tag()
        addon_name = f"graphforge.{host_tag}.node"

        def stub_body(marker: str) -> str:
            return (
                "module.exports = {\n"
                "  GraphForge: class GraphForge {},\n"
                "  runCli() { return 0; },\n"
                f"  version() {{ return '{marker}'; }},\n"
                "};\n"
            )

        def run(pkg: Path, source: str, body: str) -> subprocess.CompletedProcess[str]:
            script = pkg / source
            script.write_text(body, encoding="utf-8")
            return subprocess.run(
                ["node", str(script)],
                cwd=pkg,
                capture_output=True,
                text=True,
                check=False,
            )

        esm = (
            "import { GraphForge, runCli, version } from './index.js';\n"
            "if (typeof GraphForge !== 'function') process.exit(2);\n"
            "if (typeof runCli !== 'function') process.exit(3);\n"
            "new GraphForge();\n"
            "process.stdout.write(version());\n"
        )
        cjs = (
            "const { GraphForge, runCli, version } = require('./index.js');\n"
            "if (typeof GraphForge !== 'function') process.exit(2);\n"
            "if (typeof runCli !== 'function') process.exit(3);\n"
            "new GraphForge();\n"
            "process.stdout.write(version());\n"
        )

        def make_platform_package(pkg: Path) -> None:
            scoped = pkg / "node_modules" / "@curatelabs" / f"graphforge-{host_tag}"
            scoped.mkdir(parents=True)
            (scoped / "stub.cjs").write_text(stub_body("0.0.0-platform-package"), "utf-8")
            (scoped / "package.json").write_text(
                json.dumps(
                    {
                        "name": f"@curatelabs/graphforge-{host_tag}",
                        "version": "0.0.0",
                        "main": "stub.cjs",
                    }
                ),
                encoding="utf-8",
            )

        # Published install: no sibling addon, only the optional platform package.
        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp)
            (pkg / "index.js").write_text(
                synthesize_node_index_js(
                    addon_name,
                    surface,
                    package_name="@curatelabs/graphforge",
                    binary_name="graphforge",
                ),
                encoding="utf-8",
            )
            make_platform_package(pkg)
            for source, body in (("named-import.mjs", esm), ("named-require.cjs", cjs)):
                done = run(pkg, source, body)
                self.assertEqual(done.returncode, 0, done.stderr)
                self.assertEqual(done.stdout, "0.0.0-platform-package", source)

        # Bazel lane: the addon staged beside index.js still wins, even when a
        # platform package is also resolvable.
        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp)
            (pkg / "native-stub.js").write_text(stub_body("0.0.0-sibling"), encoding="utf-8")
            (pkg / "index.js").write_text(
                synthesize_node_index_js(
                    "native-stub.js",
                    surface,
                    package_name="@curatelabs/graphforge",
                    binary_name="graphforge",
                ),
                encoding="utf-8",
            )
            make_platform_package(pkg)
            for source, body in (("named-import.mjs", esm), ("named-require.cjs", cjs)):
                done = run(pkg, source, body)
                self.assertEqual(done.returncode, 0, done.stderr)
                self.assertEqual(done.stdout, "0.0.0-sibling", source)

        # Neither present: the failure names every candidate it tried.
        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp)
            (pkg / "index.js").write_text(
                synthesize_node_index_js(
                    addon_name,
                    surface,
                    package_name="@curatelabs/graphforge",
                    binary_name="graphforge",
                ),
                encoding="utf-8",
            )
            done = run(pkg, "named-require.cjs", cjs)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn(f"@curatelabs/graphforge-{host_tag}", done.stderr)
            self.assertIn(f"./{addon_name}", done.stderr)

    def test_host_tag_map_covers_every_declared_platform(self) -> None:
        # The loader dispatches on `${process.platform}-${process.arch}`, so
        # every declared napi tag needs exactly one host key (#1369).
        table = _napi_host_tag_map()
        self.assertEqual(sorted(table.values()), sorted(_NAPI_PLATFORM_TAGS))
        self.assertEqual(table["linux-x64"], "linux-x64-gnu")
        self.assertEqual(table["win32-x64"], "win32-x64-msvc")
        self.assertEqual(table["darwin-arm64"], "darwin-arm64")

    def test_node_package_identity_matches_the_shipped_manifest(self) -> None:
        # The loader names the optional platform packages after these (#1369).
        package_name, binary_name = read_node_package_identity(
            ROOT / "crates" / "graphforge-bindings-node"
        )
        self.assertEqual(package_name, "@curatelabs/graphforge")
        self.assertEqual(binary_name, "graphforge")

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


class BazelWheelReleaseCandidateContractTests(unittest.TestCase):
    """The Bazel Linux wheel must satisfy the real candidate validator (#1379).

    The macOS and Windows lanes run maturin, which reads the license, summary
    and legal files from ``pyproject.toml``. Only Linux hand-assembles its
    wheel, and it used to emit no license field and no legal files at all, so
    ``release_candidate_manifest._validate_wheel`` rejected every candidate.
    These tests run that validator unchanged, and prove it still rejects a
    wheel that is missing any one of the four things it requires.
    """

    @staticmethod
    def _assemble(directory: Path) -> tuple[Path, str]:
        native = directory / "libgraphforge_bindings_py.so"
        native.write_bytes(b"FAKE_NATIVE_PY_CDYLIB")
        evidence = assemble_python(
            native=native,
            package_root=PY_PACKAGE_ROOT,
            out=directory / "dist",
            wheel_tag=LINUX_WHEEL_TAG,
        )
        return Path(evidence["wheel"]), evidence["version"]

    @staticmethod
    def _rewrite(wheel: Path, out: Path, *, drop: str = "", duplicate: str = "") -> Path:
        """Copy a wheel while dropping or duplicating one member."""
        with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(out, "w") as target:
            for name in source.namelist():
                if drop and name.endswith(drop):
                    continue
                target.writestr(name, source.read(name))
            if duplicate:
                member = next(n for n in source.namelist() if n.endswith(duplicate))
                target.writestr(f"graphforge/{duplicate}", source.read(member))
        return out

    def test_assembled_wheel_passes_release_candidate_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wheel, version = self._assemble(Path(tmp))
            view = release_candidate_manifest.ArchiveView(wheel)
            result = release_candidate_manifest._validate_wheel(view, version)
            self.assertEqual(result["name"], "graphforge")
            self.assertEqual(result["version"], version)
            for legal in PYTHON_LEGAL_FILES:
                self.assertIn(
                    f"graphforge-{version}.dist-info/licenses/{legal}",
                    result["required_files"],
                )

    def test_assembled_wheel_metadata_matches_pyproject(self) -> None:
        project = read_python_project_metadata(PY_PACKAGE_ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            wheel, version = self._assemble(Path(tmp))
            with zipfile.ZipFile(wheel) as archive:
                raw = archive.read(f"graphforge-{version}.dist-info/METADATA").decode("utf-8")
                entry_points = archive.read(
                    f"graphforge-{version}.dist-info/entry_points.txt"
                ).decode("utf-8")
            metadata = Parser().parsestr(raw)
            # Every value below is read from pyproject.toml, never restated in
            # the assembler, so this lane cannot drift from maturin again.
            self.assertEqual(metadata.get("Metadata-Version"), "2.4")
            self.assertEqual(metadata.get("Name"), project["name"])
            self.assertEqual(metadata.get("Version"), version)
            self.assertEqual(metadata.get("License-Expression"), project["license"])
            self.assertEqual(metadata.get("Summary"), project["description"])
            self.assertEqual(metadata.get("Requires-Python"), project["requires-python"])
            self.assertEqual(metadata.get_all("License-File"), list(project["license-files"]))
            self.assertEqual(
                metadata.get_all("Classifier"),
                list(project["classifiers"]),
            )
            self.assertIn("pyarrow>=14", metadata.get_all("Requires-Dist"))
            self.assertEqual(metadata.get_all("Provides-Extra"), ["polars"])
            # A release artifact must not describe itself as a smoke wheel.
            self.assertNotIn("smoke wheel", raw)
            self.assertIn("[console_scripts]", entry_points)
            for name, target in project["scripts"].items():
                self.assertIn(f"{name}={target}", entry_points)

    def test_wheel_version_and_license_come_from_one_pyproject(self) -> None:
        document = tomllib.loads((PY_PACKAGE_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        project = read_python_project_metadata(PY_PACKAGE_ROOT)
        self.assertEqual(project, document["project"])
        self.assertEqual(project["license"], "Apache-2.0")
        self.assertEqual(sorted(project["license-files"]), sorted(PYTHON_LEGAL_FILES))

    def test_validator_still_rejects_wheels_missing_each_requirement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wheel, version = self._assemble(root)
            dist_info = f"graphforge-{version}.dist-info"

            # 1. Apache-2.0 declaration stripped from METADATA.
            stripped = root / "no-license-metadata.whl"
            with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(stripped, "w") as target:
                for name in source.namelist():
                    data = source.read(name)
                    if name == f"{dist_info}/METADATA":
                        data = "\n".join(
                            line
                            for line in data.decode("utf-8").splitlines()
                            if not line.startswith("License-Expression:")
                        ).encode("utf-8")
                    target.writestr(name, data)
            self._expect_rejection(stripped, version, "lacks Apache-2.0 metadata")

            # 2-4. Each legal file missing, and one present twice.
            for legal in PYTHON_LEGAL_FILES:
                missing = self._rewrite(
                    wheel,
                    root / f"no-{legal}.whl",
                    drop=f"/licenses/{legal}",
                )
                self._expect_rejection(missing, version, f"must contain exactly one {legal}")
                doubled = self._rewrite(
                    wheel,
                    root / f"two-{legal}.whl",
                    duplicate=legal,
                )
                self._expect_rejection(doubled, version, f"must contain exactly one {legal}")

    def _expect_rejection(self, wheel: Path, version: str, message: str) -> None:
        view = release_candidate_manifest.ArchiveView(wheel)
        with self.assertRaises(release_candidate_manifest.CandidateError) as raised:
            release_candidate_manifest._validate_wheel(view, version)
        self.assertIn(message, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
