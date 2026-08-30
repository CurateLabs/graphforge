#!/usr/bin/env python3
"""Prove ``make install`` preserves an already-installed native wheel."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def run(*command: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def native_fingerprint(python: Path) -> dict[str, str]:
    probe = """
import hashlib
import importlib.util
import json
from pathlib import Path

import graphforge

spec = importlib.util.find_spec("graphforge._graphforge_rs")
assert spec is not None and spec.origin is not None
extension = Path(spec.origin).resolve()
print(json.dumps({
    "extension": str(extension),
    "sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
    "version": graphforge.__version__,
}, sort_keys=True))
"""
    return json.loads(run(str(python), "-I", "-c", probe).stdout)


def write_noop(path: Path) -> None:
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path, help="same-checkout graphforge wheel to install")
    args = parser.parse_args()
    wheel = args.wheel.resolve()
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise SystemExit(f"expected one built wheel, got: {wheel}")

    with tempfile.TemporaryDirectory(prefix="graphforge-make-install-") as raw:
        temporary = Path(raw)
        environment = temporary / "venv"
        python = environment / "bin" / "python"
        shims = temporary / "shims"
        shims.mkdir()
        write_noop(shims / "cargo")
        write_noop(shims / "pnpm")

        run("uv", "venv", str(environment), "--python", sys.executable)
        clean_probe = subprocess.run(
            (str(python), "-I", "-c", "import graphforge"),
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if clean_probe.returncode == 0:
            raise SystemExit("clean test environment unexpectedly imports graphforge")

        run("uv", "pip", "install", "--python", str(python), str(wheel))
        before = native_fingerprint(python)

        make_environment = dict(os.environ)
        make_environment["PATH"] = os.pathsep.join((str(shims), make_environment["PATH"]))
        make_environment["UV_PROJECT_ENVIRONMENT"] = str(environment)
        completed = run("make", "--no-print-directory", "install", env=make_environment)
        after = native_fingerprint(python)

        if after != before:
            raise SystemExit(
                "make install replaced or removed the native wheel:\n"
                f"before={json.dumps(before, sort_keys=True)}\n"
                f"after={json.dumps(after, sort_keys=True)}"
            )

        evidence = {
            "installed_native": before,
            "make_install_output_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
            "wheel": wheel.name,
            "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        }
        print(json.dumps(evidence, indent=2, sort_keys=True))
        print("make install preserved the installed native graphforge wheel")


if __name__ == "__main__":
    main()
