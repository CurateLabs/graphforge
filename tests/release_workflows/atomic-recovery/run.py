#!/usr/bin/env python3
"""Validate and execute the bounded atomic-recovery release workflow."""

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


def prepare_project_directory(root: Path) -> Path:
    project = root / "project"
    project.mkdir()
    return project


def validate_evidence(record: dict[str, object], sha: str) -> None:
    schema = json.loads((BUNDLE / "expected/evidence-schema.json").read_text())
    missing = sorted(set(schema["required"]) - set(record))
    if missing:
        raise ValueError(f"Rust evidence missing fields: {missing}")
    for key, value in schema["fixed"].items():
        if record.get(key) != value:
            raise ValueError(f"Rust evidence {key} differs from contract")
    if record.get("commit_sha") != sha:
        raise ValueError("Rust evidence SHA differs from checkout")
    validation = record.get("validation_rejection")
    if not isinstance(validation, dict) or validation.get("code") != "GF_ONTOLOGY":
        raise ValueError("validation rejection evidence is absent")
    if validation.get("publication") != "none":
        raise ValueError("validation rejection published participants")
    idempotency = record.get("idempotency")
    if not isinstance(idempotency, dict):
        raise ValueError("idempotency evidence is absent")
    if idempotency.get("exact_retry_identical") is not True:
        raise ValueError("exact retry was not proved identical")
    if idempotency.get("conflict_code") != "GF_IDEMPOTENCY_CONFLICT":
        raise ValueError("structured conflicting replay evidence is absent")
    pre = record.get("pre_current_recoveries")
    post = record.get("post_current_recoveries")
    if (
        not isinstance(pre, list)
        or not pre
        or any(item.get("authority") != "previous" for item in pre)
    ):
        raise ValueError("pre-CURRENT recoveries must preserve previous authority")
    if (
        not isinstance(post, list)
        or not post
        or any(item.get("authority") != "new" for item in post)
    ):
        raise ValueError("post-CURRENT recoveries must select new authority")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit_sha):
        raise SystemExit("--commit-sha must be 40 lowercase hexadecimal characters")
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=TIMEOUT
    ).strip()
    if actual != args.commit_sha:
        raise SystemExit("requested SHA differs from checked-out SHA")
    if subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=ROOT, text=True, timeout=TIMEOUT
    ).strip():
        raise SystemExit("same-SHA evidence requires a clean checkout")

    run([sys.executable, str(BUNDLE / "test_runner.py"), "--quiet"], os.environ.copy())
    output = (
        args.output or ROOT / "target/release-workflows/atomic-recovery/evidence.json"
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rust_evidence = output.with_name("rust-evidence.json")
    python_evidence = output.with_name("python-evidence.json")
    for evidence_path in (rust_evidence, python_evidence):
        evidence_path.unlink(missing_ok=True)
    env = os.environ.copy()
    env["GF_ATOMIC_RECOVERY_SHA"] = actual
    env["GF_ATOMIC_RECOVERY_EVIDENCE"] = str(rust_evidence)
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target/release-workflows/cargo-atomic-recovery"))
    # Deterministic failpoint process-control matrix for composite publication
    # boundaries (library tests; cookie-protected; not a public surface).
    run(
        [
            "cargo",
            "test",
            "-p",
            "graphforge-api",
            "--lib",
            "composite_kill_reopen_matrix_never_exposes_mixed_state",
            "--",
            "--exact",
        ],
        env,
    )
    run(["cargo", "run", "-p", "graphforge-api", "--example", "atomic_recovery_workflow"], env)
    rust_record = json.loads(rust_evidence.read_text())
    validate_evidence(rust_record, actual)

    with tempfile.TemporaryDirectory(prefix="gf-atomic-recovery-") as temporary:
        temp = Path(temporary)
        project_dir = prepare_project_directory(temp)
        wheel_dir = temp / "wheel"
        wheel_dir.mkdir()
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
                str(wheel_dir),
            ],
            env,
        )
        wheels = list(wheel_dir.glob("*.whl"))
        if len(wheels) != 1:
            raise SystemExit(f"expected one native wheel, found {wheels}")
        wheel_sha = digest(wheels[0])
        venv = temp / "venv"
        run(["uv", "venv", str(venv), "--python", "3.13"], env)
        python = venv / ("Scripts/python.exe" if platform.system() == "Windows" else "bin/python")
        run(["uv", "pip", "install", "--python", str(python), str(wheels[0])], env)
        run(
            [
                str(python),
                str(BUNDLE / "binding_workflow.py"),
                "--project",
                str(project_dir),
                "--evidence",
                str(python_evidence),
                "--commit-sha",
                actual,
            ],
            env,
        )
        python_record = json.loads(python_evidence.read_text())
        python_record["wheel_sha256"] = wheel_sha
        python_evidence.write_text(json.dumps(python_record, indent=2, sort_keys=True) + "\n")
        native_path = Path(str(python_record["native_module_path"])).resolve()
        if venv.resolve() not in native_path.parents or python_record[
            "native_module_sha256"
        ] != digest(native_path):
            raise SystemExit("Python evidence did not execute the isolated native wheel")
        if python_record["commit_sha"] != actual or python_record["reopen_equal"] is not True:
            raise SystemExit("Python composite/reopen evidence is stale")
        if python_record.get("orphan_free") is not True:
            raise SystemExit("Python evidence reported orphan residue")

    aggregate = {
        "schema_version": 1,
        "scenario_id": "atomic-recovery",
        "commit_sha": actual,
        "generator_sha256": digest(BUNDLE / "generator.yaml"),
        "ontology_sha256": digest(BUNDLE / "ontologies/strict-v1.yaml"),
        "rust": rust_record,
        "python": python_record,
        "wheel_sha256": wheel_sha,
    }
    output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
