#!/usr/bin/env python3
"""Validate and orchestrate versioned release-workflow bundles."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / "tests/release_workflows"
REGISTRY = WORKFLOWS / "registry-v1.json"
TAXONOMY = WORKFLOWS / "ontology-complexity-v1.json"
STEP_RE = re.compile(r"^\s*(?:Given|When|Then|And|But)\s+\[([A-Z]+-[0-9]+)\]", re.MULTILINE)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED = (
    "scenario.yaml",
    "generator.yaml",
    "workflow.feature",
    "README.md",
    "expected/arrow-fingerprints.json",
    "expected/errors.json",
    "run.py",
)


class ContractError(ValueError):
    """A deterministic workflow-contract violation."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"invalid JSON {path.relative_to(ROOT)}: {error}") from error
    if not isinstance(value, dict):
        raise ContractError(f"expected object in {path.relative_to(ROOT)}")
    return value


def safe_path(value: str, *, field: str) -> Path:
    if not isinstance(value, str):
        raise ContractError(f"unsafe {field} path: expected string")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not value:
        raise ContractError(f"unsafe {field} path: {value!r}")
    resolved = (ROOT / path).resolve()
    if not resolved.is_relative_to(ROOT):
        raise ContractError(f"unsafe {field} path: {value!r}")
    return resolved


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_registry(registry_path: Path = REGISTRY) -> list[dict[str, Any]]:
    registry = load_json(registry_path)
    if registry.get("schema") != "workflow-registry-v1":
        raise ContractError("unsupported registry schema")
    if registry.get("evidence_schema") != "evidence-envelope-v1":
        raise ContractError("unsupported evidence schema")
    if not (WORKFLOWS / "registry-v1.schema.json").is_file():
        raise ContractError("registry schema file is missing")
    if not (WORKFLOWS / "evidence-envelope-v1.schema.json").is_file():
        raise ContractError("evidence schema file is missing")
    taxonomy = load_json(TAXONOMY)
    if taxonomy.get("schema") != registry.get("taxonomy"):
        raise ContractError("unsupported ontology taxonomy")
    classes = taxonomy.get("classes")
    if (
        not isinstance(classes, dict)
        or not classes
        or not isinstance(taxonomy.get("formula"), dict)
        or not isinstance(taxonomy.get("states"), list)
    ):
        raise ContractError("ontology taxonomy has no classes")
    scenarios = registry.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ContractError("registry scenarios must be a non-empty array")

    ids: set[str] = set()
    signatures: dict[str, str] = {}
    signature_rows: list[dict[str, Any]] = []
    registered_dirs: set[str] = set()
    required_fields = {
        "id",
        "title",
        "version",
        "domain",
        "owning_issue",
        "bundle",
        "implementation",
        "evidence",
        "generator",
        "public_surfaces",
        "algorithm_search",
        "knowledge_epistemic",
        "axes",
        "coverage_signature",
        "ontology_profile",
        "command",
        "resource",
        "timeout_seconds",
    }
    for index, row in enumerate(scenarios):
        if not isinstance(row, dict):
            raise ContractError(f"scenario row {index} is not an object")
        missing = sorted(required_fields - row.keys())
        if missing:
            raise ContractError(f"scenario row {index} missing fields: {', '.join(missing)}")
        unknown = sorted(row.keys() - required_fields - {"release_risk_rationale", "steps"})
        if unknown:
            raise ContractError(f"scenario row {index} has unknown fields: {', '.join(unknown)}")
        scenario_id = row["id"]
        if not isinstance(scenario_id, str) or scenario_id in ids:
            raise ContractError(f"duplicate or invalid scenario id: {scenario_id!r}")
        ids.add(scenario_id)
        if not all(
            isinstance(row[field], str) and row[field]
            for field in ("title", "version", "domain", "coverage_signature")
        ):
            raise ContractError(f"{scenario_id} has malformed string metadata")
        if not isinstance(row["owning_issue"], int) or row["owning_issue"] < 1:
            raise ContractError(f"{scenario_id} has malformed issue ownership")
        if (
            not isinstance(row["resource"], str)
            or row["resource"] not in {"small", "medium", "large"}
            or not isinstance(row["timeout_seconds"], (int, float))
            or isinstance(row["timeout_seconds"], bool)
            or row["timeout_seconds"] <= 0
            or not all(
                isinstance(row[field], list) and all(isinstance(item, str) for item in row[field])
                for field in ("public_surfaces", "algorithm_search", "knowledge_epistemic")
            )
            or not isinstance(row["axes"], dict)
            or not {"correction", "temporal", "epistemic", "binding"}.issubset(row["axes"])
            or not all(isinstance(value, str) and value for value in row["axes"].values())
        ):
            raise ContractError(f"{scenario_id} has malformed execution or coverage metadata")
        bundle = safe_path(row["bundle"], field=f"{scenario_id}.bundle")
        registered_dirs.add(bundle.name)
        if bundle != WORKFLOWS / scenario_id:
            raise ContractError(f"identity drift for {scenario_id}: bundle is {row['bundle']}")
        if not bundle.is_dir():
            raise ContractError(f"registered bundle missing: {row['bundle']}")
        for component in REQUIRED:
            if not (bundle / component).is_file():
                raise ContractError(f"{scenario_id} missing component: {component}")
        implementation = safe_path(row["implementation"], field=f"{scenario_id}.implementation")
        if not implementation.is_file():
            raise ContractError(f"{scenario_id} implementation is missing")
        safe_path(row["evidence"], field=f"{scenario_id}.evidence")
        command = row["command"]
        if (
            not isinstance(command, list)
            or len(command) < 2
            or command[0] not in {"python", "python3"}
            or safe_path(command[1], field=f"{scenario_id}.command") != bundle / "run.py"
        ):
            raise ContractError(f"{scenario_id} has an unsafe or non-bundle command")
        generator = row["generator"]
        if (
            not isinstance(generator, dict)
            or set(generator) != {"path", "seed", "sha256"}
            or not isinstance(generator.get("seed"), int)
            or not isinstance(generator.get("sha256"), str)
            or not HASH_RE.fullmatch(generator["sha256"])
        ):
            raise ContractError(f"{scenario_id} has malformed generator metadata")
        generator_path = safe_path(generator["path"], field=f"{scenario_id}.generator")
        if not generator_path.is_file():
            raise ContractError(f"{scenario_id} generator missing: {generator['path']}")
        if generator["sha256"] != file_hash(generator_path):
            raise ContractError(f"{scenario_id} stale generator fingerprint")
        if row["ontology_profile"] not in classes:
            raise ContractError(
                f"{scenario_id} unknown ontology profile: {row['ontology_profile']}"
            )
        signature = row["coverage_signature"]
        if signature in signatures:
            raise ContractError(
                f"identical coverage signatures: {signatures[signature]} and {scenario_id}"
            )
        signatures[signature] = scenario_id
        signature_rows.append(row)

        manifest = load_json(bundle / "scenario.yaml")
        metadata = manifest.get("registry")
        if not isinstance(metadata, dict) or metadata.get("schema") != "workflow-scenario-v1":
            raise ContractError(f"{scenario_id} missing workflow-scenario-v1 metadata")
        if metadata.get("id") != scenario_id:
            raise ContractError(
                f"identity drift for {scenario_id}: manifest says {metadata.get('id')!r}"
            )
        feature_steps = STEP_RE.findall((bundle / "workflow.feature").read_text(encoding="utf-8"))
        manifest_steps = metadata.get("steps")
        if not isinstance(manifest_steps, list) or len(manifest_steps) != len(set(manifest_steps)):
            raise ContractError(f"{scenario_id} manifest steps are missing or duplicated")
        if feature_steps != manifest_steps:
            raise ContractError(
                f"{scenario_id} step mapping drift: "
                f"feature={feature_steps!r} manifest={manifest_steps!r}"
            )
        registry_steps = row.get("steps")
        if registry_steps is not None and registry_steps != manifest_steps:
            raise ContractError(
                f"{scenario_id} central registry step mapping drift: "
                f"registry={registry_steps!r} manifest={manifest_steps!r}"
            )
        expected_fingerprint = f"sha256:{generator['sha256']}"
        if metadata.get("generator_fingerprint") != expected_fingerprint:
            raise ContractError(f"{scenario_id} manifest has stale generator fingerprint")

    implemented = {
        path.name
        for path in WORKFLOWS.iterdir()
        if path.is_dir() and (path / "scenario.yaml").is_file()
    }
    if implemented != registered_dirs:
        missing = sorted(registered_dirs - implemented)
        extra = sorted(implemented - registered_dirs)
        raise ContractError(f"registry/bundle omission: missing={missing} unregistered={extra}")
    for left_index, left in enumerate(signature_rows):
        left_axes = left["coverage_signature"].split("|")
        for right in signature_rows[left_index + 1 :]:
            right_axes = right["coverage_signature"].split("|")
            if len(left_axes) != len(right_axes):
                raise ContractError("coverage signatures have inconsistent axis counts")
            differences = sum(a != b for a, b in zip(left_axes, right_axes))
            if differences < 3 and not right.get("release_risk_rationale"):
                raise ContractError(
                    f"near-duplicate coverage requires rationale: {left['id']} and {right['id']}"
                )
    return scenarios


def select(
    rows: list[dict[str, Any]], requested: list[str] | None, run_all: bool
) -> list[dict[str, Any]]:
    if run_all == bool(requested):
        raise ContractError("choose exactly one of --all or --scenario")
    if run_all:
        return rows
    wanted = set(requested or [])
    unknown = sorted(wanted - {row["id"] for row in rows})
    if unknown:
        raise ContractError(f"unknown scenarios: {', '.join(unknown)}")
    return [row for row in rows if row["id"] in wanted]


def sanitize(message: str) -> str:
    cleaned = message.replace(str(ROOT), "<repo>")
    cleaned = re.sub(r"(?i)(token|password|secret|key)=\S+", r"\1=<redacted>", cleaned)
    return cleaned[-2000:]


def validate_evidence(envelope_path: Path, expected_sha: str) -> None:
    envelope = load_json(envelope_path)
    allowed = {
        "schema",
        "commit_sha",
        "selected_scenarios",
        "environment",
        "bounded_command",
        "outcome",
        "sanitized_failure",
        "children",
    }
    if set(envelope) != allowed:
        raise ContractError("evidence envelope fields do not match schema")
    if envelope.get("schema") != "evidence-envelope-v1":
        raise ContractError("unsupported evidence schema")
    if not SHA_RE.fullmatch(expected_sha) or envelope.get("commit_sha") != expected_sha:
        raise ContractError("evidence commit SHA is malformed or stale")
    selected_ids = envelope.get("selected_scenarios")
    if (
        not isinstance(selected_ids, list)
        or not selected_ids
        or len(selected_ids) != len(set(selected_ids))
    ):
        raise ContractError("evidence selected scenarios are malformed")
    outcome = envelope.get("outcome")
    if outcome not in {"passed", "failed"}:
        raise ContractError("evidence outcome is malformed")
    environment = envelope.get("environment")
    bounded_command = envelope.get("bounded_command")
    if (
        not isinstance(environment, dict)
        or not all(isinstance(environment.get(key), str) for key in ("platform", "python"))
        or not isinstance(bounded_command, list)
        or not bounded_command
        or not all(isinstance(item, str) for item in bounded_command)
    ):
        raise ContractError("evidence environment or bounded command is malformed")
    failure = envelope.get("sanitized_failure")
    if failure is not None and (not isinstance(failure, str) or len(failure) > 2000):
        raise ContractError("evidence sanitized failure is malformed")
    children = envelope.get("children")
    if not isinstance(children, list) or not children:
        raise ContractError("evidence has no child artifacts")
    child_ids = [child.get("scenario_id") for child in children if isinstance(child, dict)]
    if child_ids != selected_ids:
        raise ContractError("evidence child order does not match selected scenarios")
    for child in children:
        if not isinstance(child, dict):
            raise ContractError("evidence child is malformed")
        if set(child) != {"scenario_id", "path", "sha256", "outcome", "duration_ms"}:
            raise ContractError("evidence child fields do not match schema")
        if child.get("outcome") not in {"passed", "failed"} or not isinstance(
            child.get("duration_ms"), int
        ):
            raise ContractError(f"malformed child evidence: {child.get('scenario_id')}")
        path = safe_path(child.get("path", ""), field="evidence child")
        digest = child.get("sha256")
        if child["outcome"] == "failed" and digest == "0" * 64 and not path.exists():
            continue
        if (
            not path.is_file()
            or not isinstance(digest, str)
            or not HASH_RE.fullmatch(digest)
            or file_hash(path) != digest
        ):
            raise ContractError(f"stale or malformed child evidence: {child.get('scenario_id')}")


def checked_output(path: Path) -> Path:
    output = path.resolve()
    evidence_root = (ROOT / "target/release-workflow-evidence").resolve()
    if not output.is_relative_to(evidence_root):
        raise ContractError("aggregate output must be under target/release-workflow-evidence")
    return output


def run(rows: list[dict[str, Any]], sha: str, output: Path) -> int:
    if not SHA_RE.fullmatch(sha):
        raise ContractError("--commit-sha must be 40 lowercase hexadecimal characters")
    output = checked_output(output)
    evidence_dir = output.parent
    evidence_dir.mkdir(parents=True, exist_ok=True)
    children: list[dict[str, Any]] = []
    overall = "passed"
    failure: str | None = None
    for row in rows:
        evidence = safe_path(row["evidence"], field=f"{row['id']}.evidence")
        if evidence.is_file():
            evidence.unlink()
        command = [
            part.format(sha=sha, evidence=str(evidence), evidence_dir=str(evidence.parent))
            for part in row["command"]
        ]
        started = time.monotonic()
        try:
            result = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=row["timeout_seconds"],
                check=False,
            )
            returncode = result.returncode
            child_message = result.stderr or result.stdout
        except subprocess.TimeoutExpired as error:
            returncode = -1
            child_message = f"{row['id']} timed out after {row['timeout_seconds']} seconds: {error}"
        duration_ms = int((time.monotonic() - started) * 1000)
        child_sha_valid = False
        if returncode == 0 and evidence.is_file():
            try:
                child_sha_valid = load_json(evidence).get("commit_sha") == sha
            except ContractError:
                child_sha_valid = False
        outcome = "passed" if returncode == 0 and child_sha_valid else "failed"
        if outcome == "failed":
            overall = "failed"
            failure = sanitize(child_message or f"{row['id']} produced no valid SHA-bound evidence")
        children.append(
            {
                "scenario_id": row["id"],
                "path": str(evidence.relative_to(ROOT)),
                "sha256": file_hash(evidence) if evidence.is_file() else "0" * 64,
                "outcome": outcome,
                "duration_ms": duration_ms,
            }
        )
    envelope = {
        "schema": "evidence-envelope-v1",
        "commit_sha": sha,
        "selected_scenarios": [row["id"] for row in rows],
        "environment": {"platform": platform.platform(), "python": platform.python_version()},
        "bounded_command": sys.argv,
        "outcome": overall,
        "sanitized_failure": failure,
        "children": children,
    }
    output.write_text(json.dumps(envelope, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if overall == "passed" else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("validate")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--scenario", action="append")
    run_parser.add_argument("--all", action="store_true")
    run_parser.add_argument("--commit-sha", required=True)
    run_parser.add_argument("--output", type=Path, required=True)
    evidence_parser = sub.add_parser("validate-evidence")
    evidence_parser.add_argument("path", type=Path)
    evidence_parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()
    try:
        rows = validate_registry()
        if args.action == "validate":
            print(f"PASS workflow-registry-v1 scenarios={len(rows)}")
            return 0
        if args.action == "validate-evidence":
            validate_evidence(args.path.resolve(), args.commit_sha)
            print(f"PASS evidence-envelope-v1 path={args.path}")
            return 0
        chosen = select(rows, args.scenario, args.all)
        return run(chosen, args.commit_sha, args.output)
    except ContractError as error:
        print(f"FAIL {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
