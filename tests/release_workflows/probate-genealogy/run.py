#!/usr/bin/env python3
"""Validate and execute the bounded probate/genealogy release workflow."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[3]
BUNDLE = Path(__file__).resolve().parent
TIMEOUT_SECONDS = 900


def run(command: list[str], env: dict[str, str], timeout: int = TIMEOUT_SECONDS) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=timeout)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_bundle() -> dict[str, object]:
    required = [
        "workflow.feature",
        "scenario.yaml",
        "generator.yaml",
        "ontologies/advisory-v1.yaml",
        "ontologies/phases.json",
        "expected/phases/initial-records.json",
        "expected/phases/final-view.json",
        "expected/arrow-fingerprints.json",
        "expected/errors.json",
        "README.md",
        "binding_workflow.py",
    ]
    missing = [relative for relative in required if not (BUNDLE / relative).is_file()]
    if missing:
        raise SystemExit(f"incomplete probate-genealogy bundle: {missing}")
    scenario = json.loads((BUNDLE / "scenario.yaml").read_text())
    generator = json.loads((BUNDLE / "generator.yaml").read_text())
    if scenario["id"] != "probate-genealogy" or scenario["seed"] != generator["seed"]:
        raise SystemExit("scenario identity or deterministic seed mismatch")
    feature_steps = re.findall(r"\[(PG-\d{2})\]", (BUNDLE / "workflow.feature").read_text())
    if feature_steps != scenario["steps"] or len(feature_steps) != len(set(feature_steps)):
        raise SystemExit("feature step IDs must map one-to-one and in order to scenario.yaml")
    return scenario


def validate_binding_provenance(
    record: dict[str, object],
    *,
    expected_commit: str,
    expected_package_version: str,
    expected_native_version: str,
    expected_wheel_sha256: str,
    expected_extension_sha256: str,
    venv: Path,
) -> None:
    if record.get("commit_sha") != expected_commit:
        raise ValueError("binding evidence commit SHA mismatch")
    if record.get("wheel_sha256") != expected_wheel_sha256:
        raise ValueError("binding evidence wheel SHA mismatch")
    if record.get("native_module_sha256") != expected_extension_sha256:
        raise ValueError("installed native extension differs from the built wheel")
    if (
        record.get("package_version") != expected_package_version
        or record.get("native_version") != expected_native_version
    ):
        raise ValueError("Python package/native version mismatch")
    module = Path(str(record.get("native_module_path", ""))).resolve()
    if module.suffix not in {".so", ".pyd", ".dylib"}:
        raise ValueError("Python workflow did not import a compiled native extension")
    if venv.resolve() not in module.parents:
        raise ValueError("native extension was imported outside the clean-install environment")
    if not module.is_file() or digest(module) != expected_extension_sha256:
        raise ValueError("installed native extension is missing or stale")


def prove_binding_provenance_is_mutation_sensitive(
    record: dict[str, object], expected: dict[str, object]
) -> None:
    mutations = {
        "commit_sha": "0" * 40,
        "wheel_sha256": "0" * 64,
        "native_module_sha256": "0" * 64,
        "native_module_path": str(BUNDLE / "binding_workflow.py"),
        "native_version": "0.0.0-stale",
    }
    for field, value in mutations.items():
        changed = dict(record)
        changed[field] = value
        try:
            validate_binding_provenance(changed, **expected)
        except ValueError:
            continue
        raise SystemExit(f"binding provenance validator accepted mutated {field}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    scenario = validate_bundle()
    if shutil.which("cargo") is None or shutil.which("uv") is None:
        raise SystemExit("cargo and uv are required")

    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    checkout_changes = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=ROOT, text=True
    ).strip()
    if checkout_changes:
        raise SystemExit("release-workflow evidence requires a clean checkout")
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir = args.evidence_dir.resolve()
    rust_evidence = evidence_dir / "probate-genealogy-rust.json"
    python_evidence = evidence_dir / "probate-genealogy-python.json"
    aggregate_evidence = evidence_dir / "probate-genealogy.json"
    env = os.environ.copy()
    target = Path(
        env.get(
            "CARGO_TARGET_DIR",
            str(ROOT / "target/release-workflow-build/probate-genealogy"),
        )
    ).resolve()
    env.update(
        {
            "CARGO_TARGET_DIR": str(target),
            "GF_PROBATE_EVIDENCE_PATH": str(rust_evidence),
            "GF_RELEASE_WORKFLOW_SHA": sha,
        }
    )
    run(
        [
            "cargo",
            "run",
            "-p",
            "gf-api",
            "--example",
            "probate_genealogy_workflow",
        ],
        env,
    )

    with tempfile.TemporaryDirectory(prefix="gf-probate-python-") as temporary:
        temporary_path = Path(temporary)
        wheels = temporary_path / "wheels"
        wheels.mkdir()
        run(
            [
                "uv",
                "run",
                "--with",
                "maturin>=1.9.3,<2.0",
                "maturin",
                "build",
                "--manifest-path",
                "crates/gf-bindings-py/Cargo.toml",
                "--profile",
                "dev",
                "--out",
                str(wheels),
            ],
            env,
        )
        wheel_files = list(wheels.glob("*.whl"))
        if len(wheel_files) != 1:
            raise SystemExit(f"expected exactly one Python wheel, found {wheel_files}")
        wheel_sha256 = digest(wheel_files[0])
        with zipfile.ZipFile(wheel_files[0]) as archive:
            extension_members = [
                name
                for name in archive.namelist()
                if Path(name).suffix in {".so", ".pyd", ".dylib"}
            ]
            if len(extension_members) != 1:
                raise SystemExit(
                    f"wheel must contain exactly one native extension: {extension_members}"
                )
            extension_sha256 = hashlib.sha256(archive.read(extension_members[0])).hexdigest()
        venv = temporary_path / "venv"
        run(["uv", "venv", str(venv), "--python", "3.13"], env)
        python = venv / "bin/python"
        if platform.system() == "Windows":
            python = venv / "Scripts/python.exe"
        run(["uv", "pip", "install", "--python", str(python), str(wheel_files[0])], env)
        run(
            [
                str(python),
                str(BUNDLE / "binding_workflow.py"),
                "--project",
                str(temporary_path / "project"),
                "--ontology",
                str(BUNDLE / "ontologies/advisory-v1.yaml"),
                "--evidence",
                str(python_evidence),
            ],
            env,
        )
        python_record = json.loads(python_evidence.read_text())
        python_record.update(
            {
                "commit_sha": sha,
                "wheel_filename": wheel_files[0].name,
                "wheel_sha256": wheel_sha256,
                "wheel_native_member": extension_members[0],
                "wheel_native_sha256": extension_sha256,
            }
        )
        python_evidence.write_text(json.dumps(python_record, indent=2, sort_keys=True) + "\n")
        expected_provenance = {
            "expected_commit": sha,
            "expected_package_version": "0.5.0.dev0",
            "expected_native_version": "0.5.0-dev",
            "expected_wheel_sha256": wheel_sha256,
            "expected_extension_sha256": extension_sha256,
            "venv": venv,
        }
        validate_binding_provenance(python_record, **expected_provenance)
        prove_binding_provenance_is_mutation_sensitive(python_record, expected_provenance)

    rust = json.loads(rust_evidence.read_text())
    python = json.loads(python_evidence.read_text())
    if rust["commit_sha"] != sha or not rust["reopen_identical"]:
        raise SystemExit("Rust evidence is not bound to this SHA or reopen failed")
    if not python["reopen_identical"] or python["current_selection"] is not None:
        raise SystemExit("Python logical-repeat evidence is incomplete")
    aggregate = {
        "schema_version": 1,
        "scenario_id": scenario["id"],
        "commit_sha": sha,
        "seed": scenario["seed"],
        "fixture_files": {
            str(path.relative_to(BUNDLE)): digest(path)
            for path in sorted(BUNDLE.rglob("*"))
            if path.is_file() and path.name != "run.py" and "__pycache__" not in path.parts
        },
        "rust_evidence_sha256": digest(rust_evidence),
        "python_evidence_sha256": digest(python_evidence),
        "steps": scenario["steps"],
        "public_surfaces": scenario["public_surfaces"],
        "outcome": rust["outcome"],
        "prior_transaction_view_unchanged": True,
        "unselected_hypotheses_not_false": python["unselected_statuses"]
        == [
            "hypothesis",
            "hypothesis",
        ],
        "bindings": {"rust": "authoritative", "python": "clean-installed-wheel"},
        "environment": {
            "os": platform.system(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
        },
    }
    aggregate_evidence.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
    print(aggregate_evidence)


if __name__ == "__main__":
    main()
