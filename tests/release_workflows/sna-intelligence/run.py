"""Validate and execute the deterministic #2465 release-workflow bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any

SCENARIO_ID = "sna-intelligence"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one object")
    return value


def run(command: list[str], *, root: Path, env: dict[str, str]) -> None:
    subprocess.run(command, cwd=root, env=env, check=True)


def validate_bundle(bundle: Path) -> dict[str, Any]:
    required = [
        "workflow.feature",
        "scenario.yaml",
        "generator.yaml",
        "binding_workflow.py",
        "test_runner.py",
        "ontologies/phase-2-advisory.yaml",
        "expected/arrow-fingerprints.json",
        "expected/errors.json",
        "expected/evidence-schema.json",
    ]
    required.extend(
        f"expected/phases/{phase}.json"
        for phase in [
            "01-exploratory",
            "02-advisory",
            "03-corrected",
            "04-reopened",
        ]
    )
    missing = [item for item in required if not (bundle / item).is_file()]
    if missing:
        raise ValueError(f"bundle files missing: {missing}")

    scenario = load_json(bundle / "scenario.yaml")
    generator = load_json(bundle / "generator.yaml")
    if scenario.get("scenario_id") != SCENARIO_ID or scenario.get("owning_issue") != 2465:
        raise ValueError("scenario identity or ownership changed")
    if scenario.get("generator", {}).get("seed") != generator.get("seed"):
        raise ValueError("scenario and generator seeds differ")
    generator_fingerprint = (
        "sha256:" + hashlib.sha256((bundle / "generator.yaml").read_bytes()).hexdigest()
    )
    if scenario.get("generator", {}).get("fixture_fingerprint") != generator_fingerprint:
        raise ValueError("generator fixture fingerprint is stale")
    if scenario.get("bindings", {}).get("rust") != "authoritative-executable":
        raise ValueError("Rust must remain authoritative and executable")
    if scenario.get("bindings", {}).get("python") != "representative-executable":
        raise ValueError("Python representative replay is required")

    feature_ids = re.findall(r"\[(SNA-\d{2})\]", (bundle / "workflow.feature").read_text())
    manifest_ids = [step.get("id") for step in scenario.get("steps", [])]
    if feature_ids != manifest_ids or len(feature_ids) != len(set(feature_ids)):
        raise ValueError("feature and scenario step IDs must map one-to-one in order")
    if feature_ids != [f"SNA-{number:02d}" for number in range(1, 13)]:
        raise ValueError("the frozen 12-step workflow is incomplete")

    signature = scenario.get("coverage_signature", {})
    required_axes = {
        "load_mode",
        "mutation_cadence",
        "graph_shape",
        "topology_transition",
        "ontology_mode",
        "ontology_complexity",
        "runtime_catalog_drift",
        "property_characteristics",
        "operation_chain",
        "result_cardinality",
        "streaming_cancellation",
        "state_transitions",
        "correction_mechanism",
        "temporal_interpretation",
        "epistemic_pattern",
        "binding_lifecycle",
        "unique_outcome",
        "failure_mode",
    }
    if set(signature) != required_axes or not all(signature.values()):
        raise ValueError("coverage signature axes are missing, extra, or empty")
    return scenario


def validate_evidence(
    path: Path, sha: str, schema_path: Path, binding: str | None = None
) -> dict[str, Any]:
    evidence = load_json(path)
    if evidence.get("commit_sha") != sha or evidence.get("reopen_equal") is not True:
        raise ValueError(f"{path} is stale or does not prove reopen equality")
    if binding is None:
        schema = load_json(schema_path)
        missing = sorted(set(schema["required"]) - set(evidence))
        if missing:
            raise ValueError(f"Rust evidence missing fields: {missing}")
        for key, expected in schema["fixed"].items():
            if evidence.get(key) != expected:
                raise ValueError(f"Rust evidence {key} differs from contract")
        if evidence.get("hypotheses", {}).get("confidence_selected_implicitly") is not False:
            raise ValueError("confidence was allowed to select a hypothesis")
        if evidence.get("history", {}).get("supersessions") != 1:
            raise ValueError("correction supersession evidence is absent")
    elif evidence.get("binding") != binding or evidence.get("uuid_composition") is not True:
        raise ValueError(f"{binding} evidence is incomplete")
    return evidence


def validate_binding_provenance(
    evidence: dict[str, Any], sha: str, wheel_sha256: str, environment: Path
) -> None:
    if evidence.get("commit_sha") != sha or evidence.get("wheel_sha256") != wheel_sha256:
        raise ValueError("Python binding SHA or wheel provenance is stale")
    if evidence.get("package_version") != "0.5.0.dev0":
        raise ValueError("Python package version differs from the release-candidate contract")
    native_path = Path(str(evidence.get("native_module_path", ""))).resolve()
    package_path = Path(str(evidence.get("package_module_path", ""))).resolve()
    environment = environment.resolve()
    for path in [native_path, package_path]:
        if environment not in path.parents:
            raise ValueError("Python binding resolved outside the isolated wheel environment")
    if native_path.suffix not in {".so", ".pyd", ".dylib"}:
        raise ValueError("Python binding did not import a native extension")
    if not native_path.is_file():
        raise ValueError("Python native extension is absent")
    actual_native_sha = hashlib.sha256(native_path.read_bytes()).hexdigest()
    if evidence.get("native_module_sha256") != actual_native_sha:
        raise ValueError("Python native extension hash differs from executed artifact")


def build_wheel(root: Path, output: Path, env: dict[str, str]) -> tuple[Path, str]:
    wheel_dir = output.parent / "wheel"
    if wheel_dir.exists():
        shutil.rmtree(wheel_dir)
    wheel_dir.mkdir(parents=True)
    run(
        [
            "uv",
            "run",
            "maturin",
            "build",
            "--release",
            "--manifest-path",
            "crates/gf-bindings-py/Cargo.toml",
            "--out",
            str(wheel_dir),
        ],
        root=root,
        env=env,
    )
    wheels = list(wheel_dir.glob("graphforge-*.whl"))
    if len(wheels) != 1:
        raise ValueError(f"expected exactly one GraphForge wheel, found {len(wheels)}")
    wheel = wheels[0].resolve()
    return wheel, hashlib.sha256(wheel.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--python",
        help="supported Python interpreter for the isolated wheel replay (default: uv >=3.10)",
    )
    args = parser.parse_args()
    if not SHA_RE.fullmatch(args.commit_sha):
        raise SystemExit("--commit-sha must be exactly 40 lowercase hexadecimal characters")

    bundle = Path(__file__).resolve().parent
    root = bundle.parents[2]
    actual_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if actual_sha != args.commit_sha:
        raise SystemExit(f"requested SHA {args.commit_sha} is not checked-out SHA {actual_sha}")
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True
    ).strip()
    if dirty:
        raise SystemExit("same-SHA evidence requires a clean checkout")
    scenario = validate_bundle(bundle)
    if shutil.which("uv") is None:
        raise SystemExit("uv is required to build and isolate the same-SHA Python wheel")

    output = (
        args.output or root / "target/release-workflows/sna-intelligence/evidence.json"
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rust_evidence = output.with_name("rust-evidence.json")
    python_evidence = output.with_name("python-evidence.json")
    for stale in [output, rust_evidence, python_evidence]:
        stale.unlink(missing_ok=True)

    env = os.environ.copy()
    env["GRAPHFORGE_WORKFLOW_SHA"] = args.commit_sha
    env["GRAPHFORGE_WORKFLOW_EVIDENCE"] = str(rust_evidence)
    env.setdefault(
        "CARGO_TARGET_DIR", str(root / "target/release-workflows/cargo-sna-intelligence")
    )
    run(
        [sys.executable, str(bundle / "test_runner.py"), "--quiet"],
        root=root,
        env=env,
    )
    run(
        [
            "cargo",
            "run",
            "-p",
            "gf-api",
            "--release",
            "--example",
            "sna_intelligence_workflow",
        ],
        root=root,
        env=env,
    )

    schema_path = bundle / "expected/evidence-schema.json"
    rust = validate_evidence(rust_evidence, args.commit_sha, schema_path)
    wheel, wheel_sha256 = build_wheel(root, output, env)
    with tempfile.TemporaryDirectory(prefix="graphforge-sna-python-") as temporary:
        environment = Path(temporary) / "venv"
        run(
            ["uv", "venv", "--python", args.python or ">=3.10", str(environment)],
            root=root,
            env=env,
        )
        environment_python = environment / (
            "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
        )
        run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(environment_python),
                str(wheel),
            ],
            root=root,
            env=env,
        )
        run(
            [
                str(environment_python),
                str(bundle / "binding_workflow.py"),
                "--project",
                str(Path(temporary) / "project"),
                "--evidence",
                str(python_evidence),
                "--commit-sha",
                args.commit_sha,
                "--wheel-sha256",
                wheel_sha256,
            ],
            root=root,
            env=env,
        )
        python = validate_evidence(python_evidence, args.commit_sha, schema_path, "python")
        validate_binding_provenance(python, args.commit_sha, wheel_sha256, environment)
    aggregate = {
        "contract_version": 1,
        "scenario_id": SCENARIO_ID,
        "commit_sha": args.commit_sha,
        "fixture_fingerprint": scenario["generator"]["fixture_fingerprint"],
        "environment": {
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "cargo_target_dir": env["CARGO_TARGET_DIR"],
        },
        "rust": rust,
        "python": python,
        "status": "passed",
    }
    output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    print(f"PASS {SCENARIO_ID} sha={args.commit_sha} evidence={output} sha256={digest}")


if __name__ == "__main__":
    main()
