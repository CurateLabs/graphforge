#!/usr/bin/env python3
"""Validate and execute the bounded #2467 cyber release workflow."""

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


def run(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=TIMEOUT_SECONDS)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_bundle() -> dict[str, object]:
    required = [
        "workflow.feature",
        "scenario.yaml",
        "generator.yaml",
        "ontologies/strict-v1.yaml",
        "expected/phases/initial.json",
        "expected/phases/corrected.json",
        "expected/arrow-fingerprints.json",
        "expected/errors.json",
        "README.md",
        "binding_workflow.py",
    ]
    missing = [relative for relative in required if not (BUNDLE / relative).is_file()]
    if missing:
        raise SystemExit(f"incomplete cyber-intrusion bundle: {missing}")
    scenario = json.loads((BUNDLE / "scenario.yaml").read_text())
    generator = json.loads((BUNDLE / "generator.yaml").read_text())
    if scenario["id"] != "cyber-intrusion" or scenario["owning_issue"] != 2467:
        raise SystemExit("scenario identity or ownership changed")
    if scenario["seed"] != generator["seed"]:
        raise SystemExit("scenario and generator seeds differ")
    generator_fingerprint = "sha256:" + digest(BUNDLE / "generator.yaml")
    if scenario["generator"]["fixture_fingerprint"] != generator_fingerprint:
        raise SystemExit("generator fixture fingerprint is stale")
    feature_ids = re.findall(r"\[(CY-\d{2})\]", (BUNDLE / "workflow.feature").read_text())
    if feature_ids != scenario["steps"] or len(feature_ids) != len(set(feature_ids)):
        raise SystemExit("feature and scenario step IDs must map one-to-one in order")
    signature = scenario["coverage_signature"]
    if len(signature) != 18 or not all(signature.values()):
        raise SystemExit("coverage signature must contain 18 non-empty axes")
    metrics = scenario["ontology_metrics"]
    if metrics["value_type_mix"] != [
        "utf8",
        "int64",
        "float64",
        "bool",
        "duration",
        "date_time",
        "list",
        "map",
    ]:
        raise SystemExit("ontology value-family coverage changed")
    return scenario


def validate_binding_provenance(
    record: dict[str, object],
    *,
    commit_sha: str,
    wheel_sha256: str,
    native_sha256: str,
    environment: Path,
) -> None:
    if record.get("commit_sha") != commit_sha:
        raise ValueError("binding commit provenance mismatch")
    if record.get("wheel_sha256") != wheel_sha256:
        raise ValueError("binding wheel provenance mismatch")
    if record.get("native_module_sha256") != native_sha256:
        raise ValueError("installed native extension differs from wheel")
    if record.get("package_version") != "0.5.0.dev0":
        raise ValueError("unexpected Python package version")
    if record.get("native_version") != "0.5.0-dev":
        raise ValueError("unexpected native version")
    module = Path(str(record.get("native_module_path", ""))).resolve()
    if module.suffix not in {".so", ".pyd", ".dylib"}:
        raise ValueError("binding did not import a native extension")
    if environment.resolve() not in module.parents:
        raise ValueError("binding imported outside isolated environment")
    if not module.is_file() or digest(module) != native_sha256:
        raise ValueError("installed native extension is absent or stale")


def prove_mutation_sensitivity(record: dict[str, object], expected: dict[str, object]) -> None:
    mutations = {
        "commit_sha": "0" * 40,
        "wheel_sha256": "0" * 64,
        "native_module_sha256": "0" * 64,
        "native_module_path": str(BUNDLE / "binding_workflow.py"),
        "native_version": "stale",
    }
    for field, value in mutations.items():
        changed = dict(record)
        changed[field] = value
        try:
            validate_binding_provenance(changed, **expected)
        except ValueError:
            continue
        raise SystemExit(f"provenance validator accepted mutated {field}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    scenario = validate_bundle()
    if shutil.which("cargo") is None or shutil.which("uv") is None:
        raise SystemExit("cargo and uv are required")

    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise SystemExit("checked-out commit SHA is malformed")
    if subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        text=True,
    ).strip():
        raise SystemExit("same-SHA evidence requires a clean checkout")

    evidence_dir = args.evidence_dir.resolve()
    evidence_dir.mkdir(parents=True, exist_ok=True)
    rust_evidence = evidence_dir / "cyber-intrusion-rust.json"
    python_evidence = evidence_dir / "cyber-intrusion-python.json"
    aggregate = evidence_dir / "cyber-intrusion.json"
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target/release-workflow-build/cyber-intrusion"))
    env["GF_CYBER_EVIDENCE_PATH"] = str(rust_evidence)
    env["GF_RELEASE_WORKFLOW_SHA"] = sha
    run(
        ["cargo", "run", "-p", "gf-api", "--example", "cyber_intrusion_workflow"],
        env,
    )

    with tempfile.TemporaryDirectory(prefix="gf-cyber-python-") as temporary:
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
            raise SystemExit(f"expected one wheel, found {wheel_files}")
        wheel_sha256 = digest(wheel_files[0])
        with zipfile.ZipFile(wheel_files[0]) as archive:
            members = [
                name
                for name in archive.namelist()
                if Path(name).suffix in {".so", ".pyd", ".dylib"}
            ]
            if len(members) != 1:
                raise SystemExit(f"wheel native extension set is invalid: {members}")
            native_sha256 = hashlib.sha256(archive.read(members[0])).hexdigest()
        venv = temporary_path / "venv"
        run(["uv", "venv", str(venv), "--python", "3.13"], env)
        python = venv / ("Scripts/python.exe" if platform.system() == "Windows" else "bin/python")
        run(["uv", "pip", "install", "--python", str(python), str(wheel_files[0])], env)
        run(
            [
                str(python),
                str(BUNDLE / "binding_workflow.py"),
                "--project",
                str(temporary_path / "project"),
                "--ontology",
                str(BUNDLE / "ontologies/strict-v1.yaml"),
                "--evidence",
                str(python_evidence),
            ],
            env,
        )
        record = json.loads(python_evidence.read_text())
        record.update(
            {
                "commit_sha": sha,
                "wheel_filename": wheel_files[0].name,
                "wheel_sha256": wheel_sha256,
                "wheel_native_member": members[0],
                "wheel_native_sha256": native_sha256,
            }
        )
        python_evidence.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        expected = {
            "commit_sha": sha,
            "wheel_sha256": wheel_sha256,
            "native_sha256": native_sha256,
            "environment": venv,
        }
        validate_binding_provenance(record, **expected)
        prove_mutation_sensitivity(record, expected)

    rust = json.loads(rust_evidence.read_text())
    python_record = json.loads(python_evidence.read_text())
    if rust.get("commit_sha") != sha or rust.get("reopen_equal") is not True:
        raise SystemExit("Rust evidence is stale or reopen equality failed")
    if python_record.get("reopen_equal") is not True:
        raise SystemExit("Python reopen equality failed")
    for operation, rows in python_record["operation_rows"].items():
        if rust["operation_rows"].get(operation) != rows:
            raise SystemExit(f"Rust/Python row count differs for {operation}")
    output = {
        "schema_version": 1,
        "scenario_id": scenario["id"],
        "commit_sha": sha,
        "seed": scenario["seed"],
        "steps": scenario["steps"],
        "public_surfaces": scenario["public_surfaces"],
        "fixture_files": {
            str(path.relative_to(BUNDLE)): digest(path)
            for path in sorted(BUNDLE.rglob("*"))
            if path.is_file() and path.name != "run.py" and "__pycache__" not in path.parts
        },
        "rust_evidence_sha256": digest(rust_evidence),
        "python_evidence_sha256": digest(python_evidence),
        "outcome": rust,
        "binding": {
            "python": "clean-installed-native-wheel",
            "wheel_sha256": python_record["wheel_sha256"],
            "native_sha256": python_record["native_module_sha256"],
        },
        "environment": {
            "os": platform.system(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
        },
    }
    aggregate.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(aggregate)


if __name__ == "__main__":
    main()
