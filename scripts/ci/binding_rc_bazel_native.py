#!/usr/bin/env python3
"""Build Binding RC natives with Bazel and assemble packages (no maturin/napi recompile).

Used by binding-release-candidate.yml for hermetic Linux (and later host) lanes.
Never shells out to ``maturin build``, ``napi build``, or ``cargo build``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
ASSEMBLE = ROOT / "scripts" / "ci" / "assemble_bazel_binding_packages.py"

FORBIDDEN = ("maturin build", "napi build", "cargo build", "cargo rustc")


def _die(message: str, code: int = 2) -> None:
    print(f"binding_rc_bazel_native: {message}", file=sys.stderr)
    raise SystemExit(code)


def _run(cmd: list[str]) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def _find_native(bazel_target: str, patterns: tuple[str, ...]) -> Path:
    query = subprocess.check_output(
        ["bazelisk", "cquery", bazel_target, "--output=files"],
        cwd=ROOT,
        text=True,
    )
    candidates: list[Path] = []
    for line in query.splitlines():
        path = Path(line.strip())
        if not path.is_absolute():
            path = ROOT / path
        if path.is_file() and path.name.endswith(patterns):
            candidates.append(path)
    if not candidates:
        # Fallback: walk bazel-bin for the target's package.
        pkg = bazel_target.split(":", maxsplit=1)[0].removeprefix("//")
        base = ROOT / "bazel-bin" / pkg
        if base.is_dir():
            for path in base.rglob("*"):
                if path.is_file() and path.name.endswith(patterns):
                    candidates.append(path)
    if not candidates:
        _die(f"no native library found for {bazel_target} (patterns={patterns})")
    candidates.sort(key=lambda p: (len(p.parts), str(p)))
    return candidates[0]


def build_python(*, out: Path, wheel_tag: str | None) -> dict[str, str]:
    target = "//crates/graphforge-bindings-py:graphforge_bindings_py"
    _run(["bazelisk", "build", target])
    native = _find_native(target, (".so", ".dylib", ".dll", ".pyd"))
    # ``out`` may be a directory (preferred) or a .whl path; the assembler
    # rewrites untagged basenames to PEP 427 ``graphforge-{ver}-{tag}.whl``.
    if out.suffix == ".whl":
        out.parent.mkdir(parents=True, exist_ok=True)
        evidence_scratch = out.parent / ".bazel-python-assemble.evidence.json"
    else:
        out.mkdir(parents=True, exist_ok=True)
        evidence_scratch = out / ".bazel-python-assemble.evidence.json"
    cmd = [
        sys.executable,
        str(ASSEMBLE),
        "--language",
        "python",
        "--native",
        str(native),
        "--package-root",
        str(ROOT / "crates" / "graphforge-bindings-py"),
        "--out",
        str(out),
    ]
    if wheel_tag:
        cmd.extend(["--wheel-tag", wheel_tag])
    cmd.extend(["--write-evidence", str(evidence_scratch)])
    _run(cmd)
    evidence = json.loads(evidence_scratch.read_text(encoding="utf-8"))
    final_wheel = Path(evidence["wheel"])
    adjacent = Path(str(final_wheel) + ".evidence.json")
    if evidence_scratch.resolve() != adjacent.resolve():
        adjacent.write_text(
            evidence_scratch.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        evidence_scratch.unlink(missing_ok=True)
    return evidence


def build_node(*, out_dir: Path, platform_tag: str | None) -> dict[str, str]:
    target = "//crates/graphforge-bindings-node:graphforge_bindings_node"
    _run(["bazelisk", "build", target])
    native = _find_native(target, (".so", ".dylib", ".dll", ".node"))
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gf-binding-rc-node-") as tmp:
        staging = Path(tmp) / "package"
        cmd = [
            sys.executable,
            str(ASSEMBLE),
            "--language",
            "node",
            "--native",
            str(native),
            "--package-root",
            str(ROOT / "crates" / "graphforge-bindings-node"),
            "--out",
            str(staging),
        ]
        if platform_tag:
            cmd.extend(["--platform-tag", platform_tag])
        evidence_path = Path(tmp) / "evidence.json"
        cmd.extend(["--write-evidence", str(evidence_path)])
        _run(cmd)
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        for name in (evidence["addon"], "index.js", "index.d.ts"):
            src = staging / name
            if not src.is_file():
                _die(f"assembler did not produce {name}")
            shutil.copy2(src, out_dir / name)
        shutil.copy2(evidence_path, out_dir / "bazel-native-evidence.json")
    return evidence


def emit_node_loaders(*, addon: Path, out_dir: Path, platform_tag: str | None) -> None:
    """Emit index.js / index.d.ts from a retained addon without recompiling."""
    if not addon.is_file():
        _die(f"addon not found: {addon}")
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gf-emit-loaders-") as tmp:
        staging = Path(tmp) / "package"
        cmd = [
            sys.executable,
            str(ASSEMBLE),
            "--language",
            "node",
            "--native",
            str(addon),
            "--package-root",
            str(ROOT / "crates" / "graphforge-bindings-node"),
            "--out",
            str(staging),
        ]
        if platform_tag:
            cmd.extend(["--platform-tag", platform_tag])
        _run(cmd)
        for name in ("index.js", "index.d.ts"):
            shutil.copy2(staging / name, out_dir / name)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    joined = " ".join(argv).lower()
    for token in FORBIDDEN:
        if token in joined:
            _die(f"refusing argv that looks like a silent native recompile ({token})")

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    py = sub.add_parser("python", help="Bazel-build + assemble an abi3 wheel")
    py.add_argument("--out", type=Path, required=True)
    py.add_argument("--wheel-tag", default=None)

    node = sub.add_parser("node", help="Bazel-build + stage .node + loaders")
    node.add_argument("--out-dir", type=Path, required=True)
    node.add_argument("--platform-tag", default=None)

    emit = sub.add_parser("emit-node-loaders", help="Emit index.js/d.ts from a retained addon")
    emit.add_argument("--addon", type=Path, required=True)
    emit.add_argument("--out-dir", type=Path, required=True)
    emit.add_argument("--platform-tag", default=None)

    args = parser.parse_args(argv)
    if not shutil.which("bazelisk"):
        _die("bazelisk is required on PATH; see docs/development/bazel.md")

    if args.command == "python":
        evidence = build_python(out=args.out.resolve(), wheel_tag=args.wheel_tag)
    elif args.command == "node":
        evidence = build_node(out_dir=args.out_dir.resolve(), platform_tag=args.platform_tag)
    else:
        emit_node_loaders(
            addon=args.addon.resolve(),
            out_dir=args.out_dir.resolve(),
            platform_tag=args.platform_tag,
        )
        evidence = {"language": "node", "recompiled": "false", "loaders": "emitted"}
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
