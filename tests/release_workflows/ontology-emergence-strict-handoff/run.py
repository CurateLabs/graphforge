"""Validate and execute the #2469 ontology-emergence release-workflow bundle."""

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

SCENARIO_ID = "ontology-emergence-strict-handoff"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
STEP_RE = re.compile(r"\[(OEH-\d{2})\]")


def load_object(path: Path) -> dict[str, Any]:
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
        "README.md",
        "run.py",
        "binding_workflow.py",
        "binding_workflow.mjs",
        "test_runner.py",
        "ontologies/emergent-advisory-v1.yaml",
        "ontologies/strict-target-v1.yaml",
        "manifests/state-projects.json",
        "expected/arrow-fingerprints.json",
        "expected/errors.json",
        "expected/phases/contract.json",
        "expected/evidence-schema.json",
    ]
    missing = [relative for relative in required if not (bundle / relative).is_file()]
    if missing:
        raise ValueError(f"bundle files missing: {missing}")
    scenario = load_object(bundle / "scenario.yaml")
    generator = load_object(bundle / "generator.yaml")
    if scenario.get("scenario_id") != SCENARIO_ID or scenario.get("owning_issue") != 2469:
        raise ValueError("scenario identity or ownership changed")
    if scenario.get("generator", {}).get("seed") != generator.get("seed"):
        raise ValueError("scenario and generator seeds differ")
    fingerprint = "sha256:" + hashlib.sha256((bundle / "generator.yaml").read_bytes()).hexdigest()
    if scenario.get("generator", {}).get("fixture_fingerprint") != fingerprint:
        raise ValueError("generator fixture fingerprint is stale")
    feature_ids = STEP_RE.findall((bundle / "workflow.feature").read_text(encoding="utf-8"))
    manifest_ids = [step.get("id") for step in scenario.get("steps", [])]
    registry_ids = scenario.get("registry", {}).get("steps")
    expected_ids = [f"OEH-{number:02d}" for number in range(1, 16)]
    if feature_ids != expected_ids or manifest_ids != expected_ids or registry_ids != expected_ids:
        raise ValueError("feature and scenario steps must map one-to-one in frozen order")
    states = scenario.get("state_projects", [])
    expected_states = [
        ("source", "exploratory", "authoritative"),
        ("source", "advisory", "session-scoped-only"),
        ("target", "strict", "authoritative"),
    ]
    actual_states = [
        (state.get("project"), state.get("ontology_mode"), state.get("persistence"))
        for state in states
    ]
    if actual_states != expected_states:
        raise ValueError("the three explicit project/state tuples changed")
    classification = scenario.get("load_path_classification", {})
    for key in ["rust_bulk", "python_bulk", "node_bulk"]:
        if "publish_bulk" not in str(
            classification.get(key, "")
        ).lower() and "publishBulk" not in str(classification.get(key, "")):
            raise ValueError(f"{key} must classify the shipped publish_bulk surfaces")
    if scenario.get("bindings", {}).get("rust") != "authoritative-executable":
        raise ValueError("Rust must remain authoritative and executable")
    return scenario


def validate_rust_evidence(path: Path, sha: str, schema_path: Path) -> dict[str, Any]:
    evidence = load_object(path)
    schema = load_object(schema_path)
    missing = sorted(set(schema["required"]) - set(evidence))
    if missing:
        raise ValueError(f"Rust evidence missing fields: {missing}")
    for key, expected in schema["fixed"].items():
        if evidence.get(key) != expected:
            raise ValueError(f"Rust evidence {key} differs from contract")
    if evidence.get("commit_sha") != sha:
        raise ValueError("Rust evidence SHA is stale")
    if evidence.get("failures", {}).get("partial_mutation") is not False:
        raise ValueError("invalid handoff was allowed to mutate a project")
    modes = [
        (row["project"], row["mode"], row["persistence"]) for row in evidence["ontology_states"]
    ]
    if modes != [
        ("source", "exploratory", "authoritative"),
        ("source", "advisory", "session-scoped-only"),
        ("target", "strict", "authoritative"),
    ]:
        raise ValueError("ontology state tuples drifted")
    return evidence


def validate_binding(path: Path, binding: str, sha: str) -> dict[str, Any]:
    evidence = load_object(path)
    if evidence.get("binding") != binding or evidence.get("commit_sha") != sha:
        raise ValueError(f"{binding} evidence identity is stale")
    if evidence.get("source_reopen_exploratory") is not True:
        raise ValueError(f"{binding} did not prove session-scoped advisory load")
    if evidence.get("strict_reject_before_mutation") is not True:
        raise ValueError(f"{binding} did not prove strict pre-publication rejection")
    if evidence.get("reopen_equal") is not True:
        raise ValueError(f"{binding} reopen equality is missing")
    return evidence


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
            "crates/graphforge-bindings-py/Cargo.toml",
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
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--python", help="Python interpreter for isolated wheel replay")
    args = parser.parse_args()
    if not SHA_RE.fullmatch(args.commit_sha):
        raise SystemExit("--commit-sha must be exactly 40 lowercase hexadecimal characters")

    bundle = Path(__file__).resolve().parent
    root = bundle.parents[2]
    actual_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if actual_sha != args.commit_sha:
        raise SystemExit(f"requested SHA {args.commit_sha} is not checked-out SHA {actual_sha}")
    scenario = validate_bundle(bundle)
    if args.validate_only:
        print(f"VALID {SCENARIO_ID} sha={args.commit_sha}")
        return

    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True
    ).strip()
    if dirty:
        raise SystemExit("same-SHA evidence requires a clean checkout")
    if shutil.which("uv") is None:
        raise SystemExit("uv is required to build and isolate the same-SHA Python wheel")

    output = (
        args.output
        or root / "target/release-workflow-evidence/ontology-emergence-strict-handoff.json"
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rust_evidence = output.with_name("rust-evidence.json")
    python_evidence = output.with_name("python-evidence.json")
    node_evidence = output.with_name("node-evidence.json")
    for stale in [output, rust_evidence, python_evidence, node_evidence]:
        stale.unlink(missing_ok=True)

    env = os.environ.copy()
    env["GRAPHFORGE_WORKFLOW_SHA"] = args.commit_sha
    env["GRAPHFORGE_WORKFLOW_EVIDENCE"] = str(rust_evidence)
    env.setdefault(
        "CARGO_TARGET_DIR",
        str(root / "target/release-workflows/cargo-ontology-emergence"),
    )

    with tempfile.TemporaryDirectory(prefix="graphforge-oeh-") as temporary:
        temporary_path = Path(temporary)
        workflow_root = temporary_path / "workflow-root"
        workflow_root.mkdir()
        env["GRAPHFORGE_WORKFLOW_ROOT"] = str(workflow_root)

        run([sys.executable, str(bundle / "test_runner.py"), "--quiet"], root=root, env=env)
        run(
            [
                "cargo",
                "run",
                "-p",
                "graphforge-api",
                "--release",
                "--example",
                "ontology_emergence_strict_handoff",
            ],
            root=root,
            env=env,
        )
        rust = validate_rust_evidence(
            rust_evidence, args.commit_sha, bundle / "expected/evidence-schema.json"
        )

        wheel, wheel_sha256 = build_wheel(root, output, env)
        environment = temporary_path / "venv"
        run(
            ["uv", "venv", "--python", args.python or ">=3.10", str(environment)],
            root=root,
            env=env,
        )
        environment_python = environment / (
            "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
        )
        run(
            ["uv", "pip", "install", "--python", str(environment_python), str(wheel)],
            root=root,
            env=env,
        )
        run(
            [
                str(environment_python),
                str(bundle / "binding_workflow.py"),
                "--source-project",
                str(workflow_root / "source"),
                "--target-project",
                str(workflow_root / "target"),
                "--ontology",
                str(bundle / "ontologies/emergent-advisory-v1.yaml"),
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
        python = validate_binding(python_evidence, "python", args.commit_sha)

        node_dir = temporary_path / "node"
        node_dir.mkdir()
        run(
            ["pnpm", "install", "--frozen-lockfile", "--filter", "@graphforge/node..."],
            root=root,
            env=env,
        )
        run(
            [
                "pnpm",
                "--filter",
                "@graphforge/node",
                "exec",
                "napi",
                "build",
                "--platform",
                "--release",
                "--output-dir",
                str(node_dir),
            ],
            root=root,
            env=env,
        )
        (node_dir / "package.json").write_text(
            (root / "crates/graphforge-bindings-node/package.json").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        modules = list(node_dir.glob("index.js"))
        if len(modules) != 1:
            raise ValueError(f"expected one Node module, found {modules}")
        run(
            [
                "node",
                str(bundle / "binding_workflow.mjs"),
                "--source-project",
                str(workflow_root / "source"),
                "--target-project",
                str(workflow_root / "target"),
                "--ontology",
                str(bundle / "ontologies/emergent-advisory-v1.yaml"),
                "--evidence",
                str(node_evidence),
                "--commit-sha",
                args.commit_sha,
                "--module",
                str(modules[0]),
            ],
            root=root,
            env=env,
        )
        node = validate_binding(node_evidence, "node", args.commit_sha)

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
        "node": node,
        "status": "passed",
    }
    output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    print(f"PASS {SCENARIO_ID} sha={args.commit_sha} evidence={output} sha256={digest}")


if __name__ == "__main__":
    main()
