#!/usr/bin/env python3
"""Assemble Python/Node packages from Bazel-built native cdylibs (#7).

Packaging handoff for RT-maturin-assemble / RT-napi-assemble: maturin and napi
may still sign/publish later, but this step never shells out to
`maturin build`, `napi build`, or `cargo` compile. It only copies the
Bazel-produced shared library into a package layout (wheel or npm-style tree).
"""

from __future__ import annotations

import argparse
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


def _napi_platform_tag() -> str:
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


def assemble_python(*, native: Path, package_root: Path, out: Path) -> dict[str, str]:
    if not native.is_file():
        _die(f"native library not found: {native}")
    python_pkg = package_root / "python" / "graphforge"
    if not (python_pkg / "__init__.py").is_file():
        _die(f"missing pure-Python package at {python_pkg}")

    version = _read_version(package_root, "python")
    module_name = _native_python_module_name(native)
    native_hash = _sha256(native)

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

        wheel_tag = "py3-none-any"
        # Platform-specific abi3 wheel tag keeps clean-install expectations honest
        # for CI smoke; full cross-platform matrix remains #6.
        sys_name = platform.system()
        mach = platform.machine().lower()
        if sys_name == "Linux" and mach in ("x86_64", "amd64"):
            wheel_tag = "cp310-abi3-manylinux_2_17_x86_64"
        elif sys_name == "Linux" and mach in ("aarch64", "arm64"):
            wheel_tag = "cp310-abi3-manylinux_2_17_aarch64"
        elif sys_name == "Darwin" and mach in ("arm64", "aarch64"):
            wheel_tag = "cp310-abi3-macosx_11_0_arm64"
        elif sys_name == "Darwin" and mach in ("x86_64", "amd64"):
            wheel_tag = "cp310-abi3-macosx_10_12_x86_64"
        elif sys_name == "Windows" and mach in ("amd64", "x86_64"):
            wheel_tag = "cp310-abi3-win_amd64"

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
                digest = hashlib.sha256(data).hexdigest()
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
        "recompiled": "false",
    }


def assemble_node(*, native: Path, package_root: Path, out: Path) -> dict[str, str]:
    if not native.is_file():
        _die(f"native library not found: {native}")
    if not (package_root / "package.json").is_file():
        _die(f"missing package.json under {package_root}")

    version = _read_version(package_root, "node")
    platform_tag = _napi_platform_tag()
    addon_name = f"graphforge.{platform_tag}.node"
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
                "\n".join(
                    [
                        "// Generated by assemble_bazel_binding_packages.py (#7).",
                        "// Loads the Bazel-built napi cdylib for CI smoke only.",
                        f"module.exports = require('./{addon_name}');",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
        if not (staging / "index.d.ts").is_file():
            (staging / "index.d.ts").write_text(
                "/* Generated by assemble_bazel_binding_packages.py (#7). */\nexport {};\n",
                encoding="utf-8",
            )

        evidence = {
            "language": "node",
            "native_sha256": native_hash,
            "addon": addon_name,
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
    args = parser.parse_args(argv)

    native = args.native.resolve()
    package_root = args.package_root.resolve()
    out = args.out.resolve()

    if args.language == "python":
        evidence = assemble_python(native=native, package_root=package_root, out=out)
    else:
        evidence = assemble_node(native=native, package_root=package_root, out=out)

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
