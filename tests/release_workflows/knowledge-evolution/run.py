#!/usr/bin/env python3
"""Run bounded same-SHA knowledge-evolution evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
BUNDLE = Path(__file__).resolve().parent
TIMEOUT = 900


def run(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=TIMEOUT)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit_sha):
        raise SystemExit("--commit-sha must be 40 lowercase hexadecimal characters")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if (
        actual != args.commit_sha
        or subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    ):
        raise SystemExit("same-SHA evidence requires the requested clean checkout")
    run([sys.executable, str(BUNDLE / "test_runner.py"), "--quiet"], os.environ.copy())
    output = (
        args.output or ROOT / "target/release-workflows/knowledge-evolution/evidence.json"
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rust_path, python_path = (
        output.with_name("rust-evidence.json"),
        output.with_name("python-evidence.json"),
    )
    env = os.environ.copy()
    env.update(
        {"GF_KNOWLEDGE_EVOLUTION_SHA": actual, "GF_KNOWLEDGE_EVOLUTION_EVIDENCE": str(rust_path)}
    )
    env.setdefault(
        "CARGO_TARGET_DIR", str(ROOT / "target/release-workflows/cargo-knowledge-evolution")
    )
    run(["cargo", "run", "-p", "graphforge-api", "--example", "knowledge_evolution_workflow"], env)
    rust = json.loads(rust_path.read_text())
    schema = json.loads((BUNDLE / "expected/evidence-schema.json").read_text())
    if (
        set(schema["required"]) - set(rust)
        or rust["commit_sha"] != actual
        or not rust["reopen_equal"]
        or rust["neutral"]["identical"] is not True
        or rust["knowledge"]["confidence_selected_implicitly"] is not False
        or rust["knowledge"]["selection_events"] != 3
        or rust["knowledge"]["current_selection"] is not None
    ):
        raise SystemExit("Rust evidence is incomplete or stale")
    with tempfile.TemporaryDirectory(prefix="graphforge-knowledge-evolution-") as temporary:
        temp = Path(temporary)
        wheels = temp / "wheels"
        wheels.mkdir()
        run(
            [
                "uv",
                "run",
                "maturin",
                "build",
                "--manifest-path",
                "crates/graphforge-bindings-py/Cargo.toml",
                "--profile",
                "dev",
                "--out",
                str(wheels),
            ],
            env,
        )
        wheel = list(wheels.glob("*.whl"))
        if len(wheel) != 1:
            raise SystemExit("expected exactly one wheel")
        wheel_sha = digest(wheel[0])
        venv = temp / "venv"
        run(["uv", "venv", str(venv), "--python", "3.13"], env)
        python = venv / ("Scripts/python.exe" if platform.system() == "Windows" else "bin/python")
        run(["uv", "pip", "install", "--python", str(python), str(wheel[0])], env)
        run(
            [
                str(python),
                str(BUNDLE / "binding_workflow.py"),
                "--project",
                str(temp / "project"),
                "--evidence",
                str(python_path),
                "--commit-sha",
                actual,
            ],
            env,
        )
        py = json.loads(python_path.read_text())
        native = Path(py["native_module_path"]).resolve()
        if (
            venv.resolve() not in native.parents
            or py["native_module_sha256"] != digest(native)
            or not py["reopen_equal"]
        ):
            raise SystemExit("Python evidence is not isolated same-SHA native execution")
    output.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scenario_id": "knowledge-evolution",
                "commit_sha": actual,
                "rust": rust,
                "python": py,
                "wheel_sha256": wheel_sha,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
