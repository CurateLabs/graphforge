#!/usr/bin/env python3
"""Validate and aggregate the exact-SHA M22 non-Cypher release gate."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import re
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SURFACE = ROOT / "tests/contracts/non-cypher-rust-surface.json"
LOAD_TAXONOMY = ROOT / "tests/contracts/load-dataset-taxonomy.json"
LOAD_MATRIX = ROOT / "tests/contracts/load-workload-matrix.json"
BINDING_TARGETS = ROOT / "tests/contracts/binding-release-candidate-targets.json"
REPOSITORY = "CurateLabs/graphforge"

SCHEMA = "graphforge-m22-non-cypher-surface-gate/1"
RUST_SCHEMA = "graphforge-rust-non-cypher-evidence/1"
BINDING_SCHEMA = "graphforge-binding-rc-aggregate/1"
LOAD_SCHEMA = "graphforge-load-evidence/1"
SHA_RE = re.compile(r"[0-9a-f]{40}")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def import_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot import validator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_id(value: Any, owner: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{owner}: invalid Actions run ID")
    return value


def validate_component_runs(
    rust_run: dict[str, Any],
    binding_run: dict[str, Any],
    expected_sha: str,
) -> dict[str, Any]:
    if SHA_RE.fullmatch(expected_sha) is None:
        raise ValueError("expected SHA must be 40 lowercase hexadecimal characters")
    specifications = (
        (
            "rust",
            rust_run,
            ".github/workflows/non-cypher-surface-gate.yml",
            "rust-non-cypher-" + expected_sha,
        ),
        (
            "binding",
            binding_run,
            ".github/workflows/binding-release-candidate.yml",
            "binding-release-candidate-" + expected_sha,
        ),
    )
    components: dict[str, Any] = {}
    for owner, run, workflow_path, cache_key in specifications:
        run_id = _run_id(run.get("id"), owner)
        if run.get("status") != "completed" or run.get("conclusion") != "success":
            raise ValueError(f"{owner}: referenced run is not completed successfully")
        if run.get("head_sha") != expected_sha:
            raise ValueError(f"{owner}: referenced run SHA drift")
        if run.get("event") != "workflow_dispatch":
            raise ValueError(f"{owner}: referenced run was not manually dispatched")
        if run.get("path") != workflow_path:
            raise ValueError(f"{owner}: unexpected workflow path")
        if run.get("repository", {}).get("full_name") != REPOSITORY:
            raise ValueError(f"{owner}: unexpected source repository")
        expected_url = f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"
        if run.get("html_url") != expected_url:
            raise ValueError(f"{owner}: unexpected run URL")
        if (
            isinstance(run.get("run_attempt"), bool)
            or not isinstance(run.get("run_attempt"), int)
            or run["run_attempt"] <= 0
        ):
            raise ValueError(f"{owner}: invalid run attempt")
        components[owner] = {
            "run_id": run_id,
            "run_url": expected_url,
            "run_attempt": run["run_attempt"],
            "workflow_path": workflow_path,
            "cache_key": cache_key,
        }
    return {"source_sha": expected_sha, "components": components}


def validate_rust(report: dict[str, Any], expected_sha: str) -> dict[str, Any]:
    if report.get("schema") != RUST_SCHEMA or report.get("status") != "passed":
        raise ValueError("Rust component did not pass the supported schema")
    if report.get("source_sha") != expected_sha:
        raise ValueError("Rust component SHA drift")
    inventory = load_json(SURFACE)
    inventory_sha = digest(SURFACE)
    if report.get("inventory_sha256") != inventory_sha:
        raise ValueError("Rust component inventory digest drift")
    expected: dict[tuple[str, str], tuple[str | None, list[str]]] = {}
    for group_name, group in inventory["method_evidence_groups"].items():
        test_ids = [ref["symbol"] for ref in group["test_refs"]]
        expected.update(
            {("public_method", identity): (group_name, test_ids) for identity in group["ids"]}
        )
    m18 = inventory["m18_registry"]["release-tested"]
    m18_test_ids = [ref["symbol"] for ref in m18["test_refs"]]
    expected.update({("m18_registry", identity): (None, m18_test_ids) for identity in m18["ids"]})
    for group_name, group in inventory["m19_evidence_groups"].items():
        test_ids = [ref["symbol"] for ref in group["test_refs"]]
        expected.update(
            {("m19_contracts", identity): (group_name, test_ids) for identity in group["ids"]}
        )
    evidence = report.get("evidence")
    if not isinstance(evidence, list) or any(not isinstance(item, dict) for item in evidence):
        raise ValueError("Rust component evidence is missing")
    identities = [(item.get("kind"), item.get("identity")) for item in evidence]
    duplicates = [identity for identity, count in Counter(identities).items() if count > 1]
    if duplicates or set(identities) != set(expected):
        raise ValueError("Rust component identity ledger mismatch")
    for item in evidence:
        key = (item.get("kind"), item.get("identity"))
        expected_group, expected_tests = expected[key]
        if (
            item.get("outcome") != "passed"
            or item.get("error_code") is not None
            or item.get("test_ids") != expected_tests
        ):
            raise ValueError(f"Rust evidence did not pass: {item.get('identity')}")
        if expected_group is None:
            if "evidence_group" in item:
                raise ValueError(f"Rust evidence has an unexpected group: {item.get('identity')}")
        elif item.get("evidence_group") != expected_group:
            raise ValueError(f"Rust evidence group drift: {item.get('identity')}")
    binaries = report.get("test_binary_sha256")
    expected_binaries = {
        "knowledge_isolation",
        "public_lifecycle_conformance",
        "m22_m18_public_surface",
        "m22_m19_public_surface",
        "m22_provider_public_surface",
        "provider_session",
        "public_facade_remaining_conformance",
    }
    if (
        not isinstance(binaries, dict)
        or set(binaries) != expected_binaries
        or any(re.fullmatch(r"[0-9a-f]{64}", str(value)) is None for value in binaries.values())
    ):
        raise ValueError("Rust component test-binary ledger mismatch")
    commands = [
        "cargo test -p graphforge-api --lib --no-fail-fast",
        "cargo test -p graphforge-api --test knowledge_isolation "
        "--test public_lifecycle_conformance --test public_facade_remaining_conformance "
        "--test m22_m18_public_surface "
        "--test m22_m19_public_surface --test m22_provider_public_surface "
        "--test provider_session --no-fail-fast",
    ]
    if report.get("commands") != commands:
        raise ValueError("Rust component command ledger mismatch")
    return {
        "schema": RUST_SCHEMA,
        "inventory_sha256": inventory_sha,
        "evidence_count": len(evidence),
        "commands": commands,
        "test_binary_sha256": dict(sorted(binaries.items())),
    }


def validate_binding(report: dict[str, Any], expected_sha: str) -> dict[str, Any]:
    if report.get("schema") != BINDING_SCHEMA or report.get("status") != "passed":
        raise ValueError("binding component did not pass the supported schema")
    if report.get("source_sha") != expected_sha:
        raise ValueError("binding component SHA drift")
    validator = import_script(
        "m22_binding_validator", ROOT / "scripts/ci/validate-binding-release-candidate.py"
    )
    targets = report.get("targets")
    if not isinstance(targets, list):
        raise ValueError("binding component target ledger is missing")
    rebuilt = validator.validate(targets, load_json(BINDING_TARGETS), expected_sha)
    if canonical(rebuilt) != canonical(report):
        raise ValueError("binding component aggregate is non-canonical")
    return {
        "schema": BINDING_SCHEMA,
        "target_count": len(targets),
        "package_versions": report["package_versions"],
        "targets": [
            {
                "target": target["target"],
                "artifact_sha256": target["artifact"]["sha256"],
                "classification_sha256": target["classification"]["sha256"],
                "case_outcomes": [
                    {"identity": case["identity"], "outcome": case["outcome"]}
                    for case in target["cases"]
                ],
            }
            for target in targets
        ],
    }


def _normalize_version(value: str) -> str:
    return re.sub(r"-dev(?:\.0)?$", "-dev", value.replace(".dev0", "-dev"))


def validate_load(report: dict[str, Any], expected_sha: str) -> dict[str, Any]:
    if report.get("schema") != LOAD_SCHEMA or report.get("status") != "passed":
        raise ValueError("load component did not pass the supported schema")
    if report.get("source_sha") != expected_sha:
        raise ValueError("load component SHA drift")
    if report.get("taxonomy_sha256") != digest(LOAD_TAXONOMY):
        raise ValueError("load taxonomy digest drift")
    if report.get("matrix_sha256") != digest(LOAD_MATRIX):
        raise ValueError("load workload matrix digest drift")
    load_validator = import_script("m22_load_validator", ROOT / "scripts/ci/release-load-matrix.py")
    matrix = load_json(LOAD_MATRIX)
    _surface, selectors = load_validator.inventory(matrix)
    expected_inventory = {name: sorted(values) for name, values in sorted(selectors.items())}
    if report.get("inventory") != expected_inventory:
        raise ValueError("load public inventory drift")
    manifests = report.get("dataset_manifests")
    if not isinstance(manifests, list) or any(not isinstance(item, dict) for item in manifests):
        raise ValueError("load dataset manifest ledger is missing")
    manifest_by_id = {item.get("dataset_id"): item for item in manifests}
    expected_datasets = {item["id"] for item in load_json(LOAD_TAXONOMY)["datasets"]}
    if len(manifest_by_id) != len(manifests) or set(manifest_by_id) != expected_datasets:
        raise ValueError("load dataset manifest ledger mismatch")
    workloads = {item["id"]: item for item in matrix["workloads"]}
    cases = report.get("cases")
    if not isinstance(cases, list) or any(not isinstance(case, dict) for case in cases):
        raise ValueError("load case ledger is missing")
    identities = [
        load_validator.validate_report(case, expected_sha, manifest_by_id, workloads, selectors)
        for case in cases
    ]
    expected_cases = load_validator.expected_cases(matrix, manifests)
    if len(identities) != len(set(identities)) or set(identities) != expected_cases:
        raise ValueError("load case ledger mismatch")
    parity: defaultdict[tuple[str, str], set[tuple[str, ...]]] = defaultdict(set)
    packages: defaultdict[str, set[tuple[str, str]]] = defaultdict(set)
    for case in cases:
        language, workload, dataset = case["identity"].split("/", 2)
        parity[(workload, dataset)].add(
            tuple(
                case["result"][key]
                for key in ("schema_sha256", "rows_sha256", "ordering_sha256", "fingerprint")
            )
        )
        packages[language].add((case["package"]["version"], case["package"]["artifact_sha256"]))
    if any(len(values) != 1 for values in parity.values()):
        raise ValueError("load binding parity drift")
    if any(len(values) != 1 for values in packages.values()):
        raise ValueError("load package identity drift")
    expected_packages = sorted(
        (language, version, artifact)
        for language, values in packages.items()
        for version, artifact in values
    )
    if report.get("packages") != [list(item) for item in expected_packages]:
        raise ValueError("load aggregate package ledger drift")
    normalized = {_normalize_version(version) for _, version, _ in expected_packages}
    if len(normalized) != 1:
        raise ValueError("load package version drift")
    return {
        "schema": LOAD_SCHEMA,
        "case_count": len(cases),
        "dataset_count": len(manifests),
        "taxonomy_sha256": digest(LOAD_TAXONOMY),
        "matrix_sha256": digest(LOAD_MATRIX),
        "packages": [list(item) for item in expected_packages],
        "case_outcomes": [
            {"identity": case["identity"], "outcome": case["outcome"]} for case in cases
        ],
    }


def aggregate(
    rust_path: Path,
    binding_path: Path,
    load_path: Path,
    run_validation_path: Path,
    expected_sha: str,
    load_run_id: int,
) -> dict[str, Any]:
    if SHA_RE.fullmatch(expected_sha) is None:
        raise ValueError("expected SHA must be 40 lowercase hexadecimal characters")
    run_validation = load_json(run_validation_path)
    if run_validation.get("source_sha") != expected_sha:
        raise ValueError("component-run validation SHA drift")
    components = run_validation.get("components")
    if not isinstance(components, dict) or set(components) != {"rust", "binding"}:
        raise ValueError("component-run validation ledger mismatch")
    expected_workflows = {
        "rust": ".github/workflows/non-cypher-surface-gate.yml",
        "binding": ".github/workflows/binding-release-candidate.yml",
    }
    normalized_components: dict[str, dict[str, Any]] = {}
    component_keys = {
        "run_id",
        "run_url",
        "run_attempt",
        "workflow_path",
        "cache_key",
    }
    for name, component in components.items():
        if not isinstance(component, dict) or set(component) != component_keys:
            raise ValueError(f"{name}: component-run field ledger mismatch")
        run_id = _run_id(component["run_id"], f"{name} run")
        run_attempt = _run_id(component["run_attempt"], f"{name} attempt")
        expected_url = f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"
        expected_cache = {
            "rust": "rust-non-cypher-" + expected_sha,
            "binding": "binding-release-candidate-" + expected_sha,
        }[name]
        if (
            component["run_url"] != expected_url
            or component["workflow_path"] != expected_workflows[name]
            or component["cache_key"] != expected_cache
        ):
            raise ValueError(f"{name}: component-run provenance drift")
        normalized_components[name] = {
            "run_id": run_id,
            "run_url": expected_url,
            "run_attempt": run_attempt,
            "workflow_path": expected_workflows[name],
            "cache_key": expected_cache,
        }
    rust_report, binding_report, load_report = (
        load_json(rust_path),
        load_json(binding_path),
        load_json(load_path),
    )
    summaries = {
        "rust": validate_rust(rust_report, expected_sha),
        "binding": validate_binding(binding_report, expected_sha),
        "load": validate_load(load_report, expected_sha),
    }
    binding_versions = summaries["binding"]["package_versions"]
    load_versions = {
        language: version for language, version, _artifact in summaries["load"]["packages"]
    }
    for language in ("python", "node"):
        if _normalize_version(binding_versions[language]) != _normalize_version(
            load_versions[language]
        ):
            raise ValueError(f"{language} version drift between binding and load evidence")
    final_components = {
        name: {
            **normalized_components[name],
            "report_path": f"evidence/{name}/report.json",
            "report_sha256": digest(path),
            "summary": summaries[name],
        }
        for name, path in (("rust", rust_path), ("binding", binding_path))
    }
    final_components["load"] = {
        "run_id": _run_id(load_run_id, "load"),
        "report_path": "evidence/load/report.json",
        "report_sha256": digest(load_path),
        "summary": summaries["load"],
    }
    return {
        "schema": SCHEMA,
        "source_sha": expected_sha,
        "status": "passed",
        "package_versions": load_versions,
        "components": final_components,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    runs = subparsers.add_parser("validate-runs")
    runs.add_argument("--rust-run", type=Path, required=True)
    runs.add_argument("--binding-run", type=Path, required=True)
    runs.add_argument("--expected-sha", required=True)
    runs.add_argument("--output", type=Path, required=True)
    gate = subparsers.add_parser("aggregate")
    gate.add_argument("--rust-report", type=Path, required=True)
    gate.add_argument("--binding-report", type=Path, required=True)
    gate.add_argument("--load-report", type=Path, required=True)
    gate.add_argument("--run-validation", type=Path, required=True)
    gate.add_argument("--expected-sha", required=True)
    gate.add_argument("--load-run-id", type=int, required=True)
    gate.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate-runs":
            result = validate_component_runs(
                load_json(args.rust_run),
                load_json(args.binding_run),
                args.expected_sha,
            )
        else:
            result = aggregate(
                args.rust_report,
                args.binding_report,
                args.load_report,
                args.run_validation,
                args.expected_sha,
                args.load_run_id,
            )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical(result))
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as error:
        if args.command == "aggregate":
            failure = {
                "schema": SCHEMA,
                "source_sha": args.expected_sha,
                "status": "failed",
                "sanitized_failure": str(error)[:500],
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(canonical(failure))
        print(f"M22 non-Cypher surface gate failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
