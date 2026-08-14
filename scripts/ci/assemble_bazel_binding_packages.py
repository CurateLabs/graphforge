#!/usr/bin/env python3
"""Assemble Python/Node packages from Bazel-built native cdylibs (#7).

Packaging handoff for RT-maturin-assemble / RT-napi-assemble: maturin and napi
may still sign/publish later, but this step never shells out to
`maturin build`, `napi build`, or `cargo` compile. It only copies the
Bazel-produced shared library into a package layout (wheel or npm-style tree).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import sys
import tempfile
import zipfile

FORBIDDEN_RECOMPILE = re.compile(
    r"""(?ix)
    \bmaturin\s+build\b
    | \bmaturin\s+develop\b
    | \bnapi\s+build\b
    | \bcargo\s+build\b
    | \bcargo\s+rustc\b
    """
)


def _die(message: str, code: int = 2) -> None:
    print(f"assemble_bazel_binding_packages: {message}", file=sys.stderr)
    raise SystemExit(code)


def _assert_no_recompile_args(argv: list[str]) -> None:
    joined = " ".join(argv)
    if FORBIDDEN_RECOMPILE.search(joined):
        _die("refusing argv that looks like a silent native recompile")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _native_python_module_name(native: Path) -> str:
    suffix = native.suffix.lower()
    if suffix == ".pyd" or (suffix == ".dll" and platform.system() == "Windows"):
        return "_graphforge_rs.abi3.pyd"
    # macOS/Linux abi3 extension module uses .abi3.so (including Darwin).
    return "_graphforge_rs.abi3.so"


_NAPI_PLATFORM_TAGS = frozenset(
    {
        "darwin-arm64",
        "darwin-x64",
        "linux-arm64-gnu",
        "linux-x64-gnu",
        "win32-x64-msvc",
    }
)

_WHEEL_TAGS = frozenset(
    {
        "cp310-abi3-manylinux_2_17_x86_64",
        "cp310-abi3-manylinux_2_17_aarch64",
        "cp310-abi3-macosx_11_0_arm64",
        "cp310-abi3-macosx_10_12_x86_64",
        "cp310-abi3-win_amd64",
    }
)


def _napi_platform_tag(explicit: str | None = None) -> str:
    if explicit is not None:
        if explicit not in _NAPI_PLATFORM_TAGS:
            _die(f"unsupported napi platform tag: {explicit}")
        return explicit
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Darwin":
        if machine in ("arm64", "aarch64"):
            return "darwin-arm64"
        if machine in ("x86_64", "amd64"):
            return "darwin-x64"
    if system == "Linux":
        if machine in ("aarch64", "arm64"):
            return "linux-arm64-gnu"
        if machine in ("x86_64", "amd64"):
            return "linux-x64-gnu"
    if system == "Windows":
        if machine in ("amd64", "x86_64"):
            return "win32-x64-msvc"
    _die(f"unsupported host platform for napi addon naming: {system}/{machine}")
    return ""  # unreachable; _die raises SystemExit


def _python_wheel_tag(explicit: str | None = None) -> str:
    if explicit is not None:
        if explicit not in _WHEEL_TAGS:
            _die(f"unsupported abi3 wheel tag: {explicit}")
        return explicit
    sys_name = platform.system()
    mach = platform.machine().lower()
    if sys_name == "Linux" and mach in ("x86_64", "amd64"):
        return "cp310-abi3-manylinux_2_17_x86_64"
    if sys_name == "Linux" and mach in ("aarch64", "arm64"):
        return "cp310-abi3-manylinux_2_17_aarch64"
    if sys_name == "Darwin" and mach in ("arm64", "aarch64"):
        return "cp310-abi3-macosx_11_0_arm64"
    if sys_name == "Darwin" and mach in ("x86_64", "amd64"):
        return "cp310-abi3-macosx_10_12_x86_64"
    if sys_name == "Windows" and mach in ("amd64", "x86_64"):
        return "cp310-abi3-win_amd64"
    _die(f"unsupported host platform for abi3 wheel tagging: {sys_name}/{mach}")
    return ""


def pep427_wheel_filename(version: str, wheel_tag: str) -> str:
    """Return a PEP 427 wheel basename for graphforge + abi3 tags."""
    return f"graphforge-{version}-{wheel_tag}.whl"


def resolve_python_wheel_out(out: Path, version: str, wheel_tag: str) -> Path:
    """Resolve ``--out`` to a PEP 427 wheel path (#720).

    - Directory (existing, or non-``.whl`` path): write ``graphforge-{ver}-{tag}.whl`` inside.
    - ``.whl`` basename missing tags: rewrite to the tagged sibling in the same parent.
    - Already-tagged ``.whl`` path: keep as-is.
    """
    tagged_name = pep427_wheel_filename(version, wheel_tag)
    if out.exists() and out.is_dir():
        return out / tagged_name
    if out.suffix != ".whl":
        return out / tagged_name
    # File path: rewrite when the basename is not the standards-compliant name
    # (e.g. Binding RC historically passed dist/graphforge-bazel.whl).
    if out.name != tagged_name:
        return out.parent / tagged_name
    return out


def synthesize_node_index_js(addon_name: str) -> str:
    """CJS loader that exposes napi-compatible named exports for ESM import (#720).

    ``module.exports = require(addon)`` alone is insufficient: Node's ESM-CJS
    interop does not synthesize named exports from a replaced ``module.exports``
    object. napi's generated index.js assigns ``module.exports.version = ...``
    explicitly; Binding RC smoke uses ``import(...).then(m => m.version())``.
    """
    return "\n".join(
        [
            "// Generated by assemble_bazel_binding_packages.py (#7 / #720).",
            "// Loads the Bazel-built napi cdylib for CI smoke only.",
            f"const nativeBinding = require('./{addon_name}');",
            "module.exports = nativeBinding;",
            # Explicit named export for ESM interop (required for `m.version`).
            "module.exports.version = nativeBinding.version;",
            # Mirror remaining enumerable exports the way napi's index.js does.
            "for (const key of Object.keys(nativeBinding)) {",
            "  module.exports[key] = nativeBinding[key];",
            "}",
            "",
        ]
    )


def synthesize_node_index_dts() -> str:
    return (
        "/* Generated by assemble_bazel_binding_packages.py (#7 / #720). */\n"
        "export declare function version(): string;\n"
    )


def _read_version(package_root: Path, language: str) -> str:
    if language == "python":
        text = (package_root / "pyproject.toml").read_text(encoding="utf-8")
        match = re.search(r'(?m)^version\s*=\s*"([^"]+)"\s*$', text)
        if not match:
            _die("could not parse version from pyproject.toml")
        return match.group(1)
    package_json = json.loads((package_root / "package.json").read_text(encoding="utf-8"))
    version = package_json.get("version")
    if not isinstance(version, str) or not version:
        _die("could not parse version from package.json")
    return version


def assemble_python(
    *,
    native: Path,
    package_root: Path,
    out: Path,
    wheel_tag: str | None = None,
) -> dict[str, str]:
    if not native.is_file():
        _die(f"native library not found: {native}")
    python_pkg = package_root / "python" / "graphforge"
    if not (python_pkg / "__init__.py").is_file():
        _die(f"missing pure-Python package at {python_pkg}")

    version = _read_version(package_root, "python")
    module_name = _native_python_module_name(native)
    native_hash = _sha256(native)
    resolved_wheel_tag = _python_wheel_tag(wheel_tag)
    out = resolve_python_wheel_out(out, version, resolved_wheel_tag)

    with tempfile.TemporaryDirectory(prefix="gf-bazel-py-wheel-") as tmp:
        staging = Path(tmp)
        dist_info = staging / f"graphforge-{version}.dist-info"
        package_dir = staging / "graphforge"
        dist_info.mkdir(parents=True)
        shutil.copytree(
            python_pkg,
            package_dir,
            ignore=shutil.ignore_patterns(
                "*.pyc",
                "__pycache__",
                "_graphforge_rs*.so",
                "_graphforge_rs*.pyd",
                "_graphforge_rs*.dylib",
            ),
        )
        shutil.copy2(native, package_dir / module_name)

        # Explicit --wheel-tag models the #6 cross-platform matrix; host default
        # remains for local/CI smoke when the tag is omitted.
        wheel_tag = resolved_wheel_tag

        (dist_info / "METADATA").write_text(
            "\n".join(
                [
                    "Metadata-Version: 2.1",
                    "Name: graphforge",
                    f"Version: {version}",
                    "Summary: GraphForge native graph engine (Bazel-built smoke wheel)",
                    "Requires-Python: >=3.10",
                    "Requires-Dist: pyarrow>=14",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        (dist_info / "WHEEL").write_text(
            "\n".join(
                [
                    "Wheel-Version: 1.0",
                    "Generator: assemble_bazel_binding_packages.py",
                    "Root-Is-Purelib: false",
                    f"Tag: {wheel_tag}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        record_lines: list[str] = []
        out.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as wheel:
            for path in sorted(staging.rglob("*")):
                if path.is_dir():
                    continue
                arcname = path.relative_to(staging).as_posix()
                data = path.read_bytes()
                digest = (
                    base64.urlsafe_b64encode(hashlib.sha256(data).digest())
                    .rstrip(b"=")
                    .decode("ascii")
                )
                record_lines.append(f"{arcname},sha256={digest},{len(data)}")
                wheel.writestr(arcname, data)
            record_path = f"graphforge-{version}.dist-info/RECORD"
            record_body = "\n".join([*record_lines, f"{record_path},,"]) + "\n"
            wheel.writestr(record_path, record_body)

    return {
        "language": "python",
        "native_sha256": native_hash,
        "native_module": module_name,
        "version": version,
        "wheel": str(out),
        "wheel_tag": resolved_wheel_tag,
        "recompiled": "false",
    }


def assemble_node(
    *,
    native: Path,
    package_root: Path,
    out: Path,
    platform_tag: str | None = None,
) -> dict[str, str]:
    if not native.is_file():
        _die(f"native library not found: {native}")
    if not (package_root / "package.json").is_file():
        _die(f"missing package.json under {package_root}")

    version = _read_version(package_root, "node")
    resolved_platform_tag = _napi_platform_tag(platform_tag)
    addon_name = f"graphforge.{resolved_platform_tag}.node"
    native_hash = _sha256(native)

    write_zip = out.suffix == ".zip"
    with tempfile.TemporaryDirectory(prefix="gf-bazel-node-pkg-") as tmp:
        staging = Path(tmp) / "package"
        staging.mkdir(parents=True)

        for name in (
            "package.json",
            "LICENSE",
            "NOTICE",
            "THIRD_PARTY_NOTICES.md",
            "README.md",
        ):
            src = package_root / name
            if src.is_file():
                shutil.copy2(src, staging / name)

        lib_src = package_root / "lib"
        if lib_src.is_dir():
            shutil.copytree(lib_src, staging / "lib")

        shutil.copy2(native, staging / addon_name)

        # napi's full index.js is generated by `napi build` and gitignored. For
        # the Bazel packaging handoff we synthesize a host-local smoke loader so
        # CI can exercise the Bazel-built addon without recompiling Rust.
        for generated_name in ("index.js", "index.d.ts"):
            checked_in = package_root / generated_name
            if checked_in.is_file():
                shutil.copy2(checked_in, staging / generated_name)
        if not (staging / "index.js").is_file():
            (staging / "index.js").write_text(
                synthesize_node_index_js(addon_name),
                encoding="utf-8",
            )
        if not (staging / "index.d.ts").is_file():
            (staging / "index.d.ts").write_text(
                synthesize_node_index_dts(),
                encoding="utf-8",
            )

        evidence = {
            "language": "node",
            "native_sha256": native_hash,
            "addon": addon_name,
            "platform_tag": resolved_platform_tag,
            "version": version,
            "recompiled": "false",
        }
        (staging / "bazel-native-evidence.json").write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            if out.is_dir():
                shutil.rmtree(out)
            else:
                out.unlink()
        if write_zip:
            with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for path in sorted(staging.rglob("*")):
                    if path.is_dir():
                        continue
                    archive.write(path, path.relative_to(staging).as_posix())
            evidence["package_zip"] = str(out)
        else:
            shutil.copytree(staging, out)
            evidence["package_dir"] = str(out)
    return evidence


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    _assert_no_recompile_args(argv)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--language", choices=("python", "node"), required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--package-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--write-evidence",
        type=Path,
        default=None,
        help="Optional JSON evidence path (native hash, no-recompile assertion).",
    )
    parser.add_argument(
        "--wheel-tag",
        default=None,
        help="Explicit abi3 wheel tag for #6 cross-platform matrix (Python).",
    )
    parser.add_argument(
        "--platform-tag",
        default=None,
        help="Explicit napi platform tag for #6 cross-platform matrix (Node).",
    )
    args = parser.parse_args(argv)

    native = args.native.resolve()
    package_root = args.package_root.resolve()
    out = args.out.resolve()

    if args.language == "python":
        evidence = assemble_python(
            native=native,
            package_root=package_root,
            out=out,
            wheel_tag=args.wheel_tag,
        )
    else:
        evidence = assemble_node(
            native=native,
            package_root=package_root,
            out=out,
            platform_tag=args.platform_tag,
        )

    if args.write_evidence is not None:
        args.write_evidence.parent.mkdir(parents=True, exist_ok=True)
        args.write_evidence.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    # Guard the running process itself: this module must never spawn compile tools.
    if FORBIDDEN_RECOMPILE.search(" ".join(sys.argv)):
        _die("refusing to run with forbidden recompile tokens in argv")
    raise SystemExit(main())
