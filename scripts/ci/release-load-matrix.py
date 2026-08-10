#!/usr/bin/env python3
"""Generate deterministic load fixtures and fail-closed release evidence."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
TAXONOMY = ROOT / "tests/contracts/load-dataset-taxonomy.json"
MATRIX = ROOT / "tests/contracts/load-workload-matrix.json"
REPORT_SCHEMA = "graphforge-load-case/1"
BUNDLE_SCHEMA = "graphforge-load-evidence/1"
# Fail closed before starting another case when the runner cannot host one more
# L/XL publication workspace plus fixture IPC staging (#2765).
MINIMUM_CASE_FREE_BYTES = 3 * 1024 * 1024 * 1024


def ensure_case_disk_headroom(
    path: Path, identity: str, *, minimum: int = MINIMUM_CASE_FREE_BYTES
) -> int:
    free_bytes = shutil.disk_usage(path).free
    if free_bytes < minimum:
        raise ValueError(
            f"{identity}: insufficient free disk ({free_bytes} bytes); retries are forbidden"
        )
    return free_bytes


def reclaim_case_tmpdir(case_tmp: Path) -> None:
    if not case_tmp.is_dir():
        return
    for leftover in case_tmp.iterdir():
        if leftover.is_dir():
            shutil.rmtree(leftover)
        else:
            leftover.unlink()


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected JSON object")
    return value


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def inventory(matrix: dict[str, Any]) -> tuple[dict[str, Any], dict[str, set[str]]]:
    source = load(ROOT / matrix["inventory"])
    release_methods = {
        identity for group in source["method_evidence_groups"].values() for identity in group["ids"]
    }
    selectors = {
        "release-tested-methods": release_methods,
        "algorithm-registry": set(source["algorithm_registry"]["release-tested"]["ids"]),
        "search-contracts": set(source["search_contracts"]["release-tested"]["ids"]),
    }
    return source, selectors


def contract_errors(taxonomy_path: Path = TAXONOMY, matrix_path: Path = MATRIX) -> list[str]:
    errors: list[str] = []
    taxonomy, matrix = load(taxonomy_path), load(matrix_path)
    if taxonomy.get("schema") != "graphforge-load-dataset-taxonomy/1":
        errors.append("unsupported dataset taxonomy schema")
    if matrix.get("schema") != "graphforge-load-workload-matrix/1":
        errors.append("unsupported workload matrix schema")
    formula = taxonomy.get("density", {}).get("formula")
    if formula != "live_edges / (live_nodes * (live_nodes - 1))":
        errors.append("canonical directed density formula drift")
    categories = taxonomy.get("density", {}).get("categories", {})
    if categories.get("sparse", {}).get("maximum", 1) >= categories.get("dense", {}).get(
        "minimum", 0
    ):
        errors.append("sparse and dense thresholds must not overlap")
    datasets = taxonomy.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        return [*errors, "dataset taxonomy is empty"]
    ids = [item.get("id") for item in datasets]
    if len(ids) != len(set(ids)):
        errors.append("dataset identities must be unique")
    classes: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in datasets:
        size = item.get("size")
        bounds = taxonomy.get("size_classes", {}).get(size)
        if (
            not bounds
            or not bounds["minimum_nodes"] <= item.get("nodes", 0) <= bounds["maximum_nodes"]
        ):
            errors.append(f"{item.get('id')}: node count violates {size} threshold")
        if item.get("density") not in {"sparse", "dense"}:
            errors.append(f"{item.get('id')}: invalid density category")
        if item.get("topology") not in {
            "disconnected",
            "hub-heavy",
            "path-heavy",
            "clustered",
            "cyclic",
        }:
            errors.append(f"{item.get('id')}: invalid topology")
        if not isinstance(item.get("seed"), int):
            errors.append(f"{item.get('id')}: deterministic integer seed required")
        classes[str(size)].append(item)
    for size in ("XS", "S", "M"):
        members = classes[size]
        for density in ("sparse", "dense"):
            selected = [item for item in members if item["density"] == density]
            if len(selected) < 2 or len({item["topology"] for item in selected}) < 2:
                errors.append(f"{size}: needs multiple materially different {density} datasets")
    for size in ("L", "XL"):
        members = classes[size]
        missing_density = {"sparse", "dense"} - {item["density"] for item in members}
        if missing_density:
            errors.append(f"{size}: representative bounded sparse and dense datasets required")
    resource_bounds = taxonomy.get("resource_bounds", {})
    required_bounds = {
        "case_timeout_seconds",
        "preflight_timeout_seconds",
        "maximum_peak_rss_bytes",
        "maximum_temporary_bytes",
        "maximum_persisted_bytes",
    }
    for size in ("XS", "S", "M", "L", "XL"):
        bounds = resource_bounds.get(size)
        if (
            not isinstance(bounds, dict)
            or set(bounds) != required_bounds
            or not all(isinstance(value, int) and value > 0 for value in bounds.values())
        ):
            errors.append(f"{size}: complete positive resource bounds required")

    try:
        _source, selectors = inventory(matrix)
    except (KeyError, OSError, json.JSONDecodeError, ValueError) as error:
        return [*errors, f"cannot resolve public inventory: {error}"]
    assigned: defaultdict[str, list[str]] = defaultdict(list)
    workloads = matrix.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        return [*errors, "workload matrix is empty"]
    for workload in workloads:
        identity = workload.get("id", "<missing>")
        if workload.get("dataset_classes") != ["XS", "S", "M", "L", "XL"]:
            errors.append(f"{identity}: must cover XS, S, M, L, and XL")
        if not workload.get("operations") or not workload.get("assertions"):
            errors.append(f"{identity}: operations and assertions are required")
        for selector in workload.get("inventory_selectors", []):
            if selector not in selectors:
                errors.append(f"{identity}: unknown inventory selector {selector}")
                continue
            for entry in selectors[selector]:
                assigned[entry].append(identity)
    expected = set().union(*selectors.values())
    missing = sorted(expected - assigned.keys())
    duplicates = sorted(entry for entry, owners in assigned.items() if len(owners) != 1)
    if missing:
        errors.append(f"unmapped public inventory: {missing}")
    if duplicates:
        errors.append(f"multiply mapped public inventory: {duplicates}")
    if matrix.get("languages") != ["rust", "python", "node"]:
        errors.append("matrix must require Rust, Python, and Node")
    return errors


def edge_set(spec: dict[str, Any]) -> set[tuple[int, int]]:
    n, topology = spec["nodes"], spec["topology"]
    rng = random.Random(spec["seed"])
    edges: set[tuple[int, int]] = set()
    if topology == "path-heavy":
        edges.update((index, index + 1) for index in range(n - 1))
    elif topology == "hub-heavy":
        edges.update((0, index) for index in range(1, n))
        edges.update((index, 0) for index in range(1, min(n, 1 + n // 4)))
    elif topology == "disconnected":
        midpoint = n // 2
        edges.update((index, index + 1) for index in range(midpoint - 1))
        edges.update((index, index + 1) for index in range(midpoint, n - 1))
    elif topology == "clustered":
        groups = 4
        for index in range(n):
            candidate = (index // max(1, n // groups)) * max(1, n // groups)
            target = candidate + ((index + 1) % max(1, n // groups))
            if target < n and target != index:
                edges.add((index, target))
    else:
        edges.update((index, (index + 1) % n) for index in range(n))

    maximum = n * (n - 1)
    density = spec["density"]
    target = (
        max(len(edges), math.ceil(maximum * 0.12))
        if density == "dense"
        else min(max(len(edges), 4 * n), math.floor(maximum * 0.08))
    )
    while len(edges) < target:
        source, target_node = rng.randrange(n), rng.randrange(n)
        if source != target_node:
            edges.add((source, target_node))
    return edges


def graph_metrics(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(nodes)
    adjacency = [set() for _ in nodes]
    indegree = [0] * n
    outdegree = [0] * n
    for edge in edges:
        source, target = edge["source"], edge["target"]
        adjacency[source].add(target)
        adjacency[target].add(source)
        outdegree[source] += 1
        indegree[target] += 1
    seen: set[int] = set()
    components: list[int] = []
    diameter_bound = 0
    for root in range(n):
        if root in seen:
            continue
        queue = deque([(root, 0)])
        seen.add(root)
        count = 0
        while queue:
            node, distance = queue.popleft()
            count += 1
            diameter_bound = max(diameter_bound, distance)
            for neighbor in sorted(adjacency[node]):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append((neighbor, distance + 1))
        components.append(count)
    degrees = sorted(indegree[index] + outdegree[index] for index in range(n))

    def percentile(q: float) -> int:
        return degrees[min(len(degrees) - 1, math.floor((len(degrees) - 1) * q))]

    loops = sum(edge["source"] == edge["target"] for edge in edges)
    pairs = Counter((edge["source"], edge["target"]) for edge in edges)
    return {
        "live_nodes": n,
        "live_edges": len(edges),
        "combined_records": n + len(edges),
        "directed_density": len(edges) / (n * (n - 1)),
        "node_label_cardinality": 2,
        "edge_type_cardinality": 2,
        "property_cardinality": 6,
        "property_profile": {
            "width": 6,
            "types": ["bool", "float64", "int64", "utf8"],
            "null_rate": sum(node["nullable"] is None for node in nodes) / (n * 6),
            "value_distributions": {
                "ordinal": "uniform",
                "salience": "bounded-modular",
                "group": "cyclic",
            },
        },
        "connected_component_count": len(components),
        "largest_component_ratio": max(components) / n,
        "degree": {
            "mean": sum(degrees) / n,
            "p50": percentile(0.50),
            "p90": percentile(0.90),
            "p99": percentile(0.99),
            "maximum": max(degrees),
            "skew": max(degrees) / max(1, sum(degrees) / n),
        },
        "self_loop_count": loops,
        "parallel_edge_count": sum(count - 1 for count in pairs.values()),
        "isolated_node_count": sum(degree == 0 for degree in degrees),
        "hub_count": sum(degree >= max(4, percentile(0.99)) for degree in degrees),
        "approximate_diameter": diameter_bound,
        "diameter_method": "deterministic component-root BFS lower bound",
    }


def generate(output: Path, taxonomy_path: Path = TAXONOMY) -> list[dict[str, Any]]:
    taxonomy = load(taxonomy_path)
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"fixture output must be a fresh empty directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    manifests = []
    for spec in taxonomy["datasets"]:
        nodes = [
            {
                "ordinal": index,
                "label": "Entity" if index % 5 else "Anchor",
                "name": f"n-{index:08d}",
                "group": index % 7,
                "salience": (index * 17 % 1000) / 1000,
                "active": index % 3 != 0,
                "nullable": None if index % 8 == 0 else f"v-{index % 13}",
            }
            for index in range(spec["nodes"])
        ]
        edges = [
            {
                "source": source,
                "target": target,
                "type": "LINK" if (source + target) % 3 else "RELATED",
                "weight": ((source * 31 + target * 17) % 1000 + 1) / 1000,
            }
            for source, target in sorted(edge_set(spec))
        ]
        content = {
            "schema": "graphforge-load-fixture/1",
            "generator_version": taxonomy["generator_version"],
            "dataset_id": spec["id"],
            "seed": spec["seed"],
            "nodes": nodes,
            "edges": edges,
        }
        payload = canonical(content)
        fixture_path = output / f"{spec['id']}.json"
        fixture_path.write_bytes(payload)
        metrics = graph_metrics(nodes, edges)
        threshold = taxonomy["density"]["categories"][spec["density"]]
        valid_density = metrics["directed_density"] <= threshold.get("maximum", 1.0) and metrics[
            "directed_density"
        ] >= threshold.get("minimum", 0.0)
        if not valid_density:
            raise ValueError(f"{spec['id']}: generated density violates declared category")
        manifest = {
            "schema": "graphforge-load-dataset-manifest/1",
            "dataset_id": spec["id"],
            "generator_version": taxonomy["generator_version"],
            "seed": spec["seed"],
            "content_sha256": sha256(payload),
            "size_class": spec["size"],
            "density_category": spec["density"],
            "topology": spec["topology"],
            **metrics,
            "persisted_bytes": len(payload),
        }
        manifest_path = output / f"{spec['id']}.manifest.json"
        manifest_path.write_bytes(canonical(manifest))
        manifests.append(manifest)
    return manifests


def expected_cases(matrix: dict[str, Any], manifests: list[dict[str, Any]]) -> set[str]:
    by_class: defaultdict[str, list[str]] = defaultdict(list)
    for manifest in manifests:
        by_class[manifest["size_class"]].append(manifest["dataset_id"])
    cases = set()
    for workload in matrix["workloads"]:
        for size in workload["dataset_classes"]:
            for dataset in by_class[size]:
                for language in matrix["languages"]:
                    cases.add(f"{language}/{workload['id']}/{dataset}")
    return cases


def validate_fixture_set(fixtures_dir: Path) -> dict[str, dict[str, Any]]:
    taxonomy = load(TAXONOMY)
    specs = {item["id"]: item for item in taxonomy["datasets"]}
    expected_files = {
        *(f"{identity}.json" for identity in specs),
        *(f"{identity}.manifest.json" for identity in specs),
    }
    actual_files = {path.name for path in fixtures_dir.glob("*.json")}
    if actual_files != expected_files:
        raise ValueError(
            "fixture ledger mismatch: "
            f"missing={sorted(expected_files - actual_files)}, "
            f"extra={sorted(actual_files - expected_files)}"
        )
    manifests: dict[str, dict[str, Any]] = {}
    metric_keys = {
        "live_nodes",
        "live_edges",
        "combined_records",
        "directed_density",
        "node_label_cardinality",
        "edge_type_cardinality",
        "property_cardinality",
        "property_profile",
        "connected_component_count",
        "largest_component_ratio",
        "degree",
        "self_loop_count",
        "parallel_edge_count",
        "isolated_node_count",
        "hub_count",
        "approximate_diameter",
        "diameter_method",
    }
    for identity, spec in specs.items():
        fixture_path = fixtures_dir / f"{identity}.json"
        payload = fixture_path.read_bytes()
        fixture = json.loads(payload)
        manifest = load(fixtures_dir / f"{identity}.manifest.json")
        if set(fixture) != {
            "schema",
            "generator_version",
            "dataset_id",
            "seed",
            "nodes",
            "edges",
        } or (
            fixture["schema"] != "graphforge-load-fixture/1"
            or fixture["generator_version"] != taxonomy["generator_version"]
            or fixture["dataset_id"] != identity
            or fixture["seed"] != spec["seed"]
        ):
            raise ValueError(f"{identity}: fixture identity or schema drift")
        expected_manifest_keys = {
            "schema",
            "dataset_id",
            "generator_version",
            "seed",
            "content_sha256",
            "size_class",
            "density_category",
            "topology",
            "persisted_bytes",
            *metric_keys,
        }
        if set(manifest) != expected_manifest_keys:
            raise ValueError(f"{identity}: incomplete or unsafe manifest fields")
        if (
            manifest["schema"] != "graphforge-load-dataset-manifest/1"
            or manifest["dataset_id"] != identity
            or manifest["generator_version"] != taxonomy["generator_version"]
            or manifest["seed"] != spec["seed"]
            or manifest["size_class"] != spec["size"]
            or manifest["density_category"] != spec["density"]
            or manifest["topology"] != spec["topology"]
            or manifest["content_sha256"] != sha256(payload)
            or manifest["persisted_bytes"] != len(payload)
        ):
            raise ValueError(f"{identity}: fixture manifest drift")
        recomputed = graph_metrics(fixture["nodes"], fixture["edges"])
        if any(manifest[key] != recomputed[key] for key in metric_keys):
            raise ValueError(f"{identity}: fixture metrics do not match content")
        threshold = taxonomy["density"]["categories"][spec["density"]]
        density = manifest["directed_density"]
        if density > threshold.get("maximum", 1.0) or density < threshold.get("minimum", 0.0):
            raise ValueError(f"{identity}: density classification violates threshold")
        manifests[identity] = manifest
    return manifests


def validate_report(
    report: dict[str, Any],
    expected_sha: str,
    manifests: dict[str, dict[str, Any]],
    workloads: dict[str, dict[str, Any]],
    selectors: dict[str, set[str]],
) -> str:
    required_report_keys = {
        "schema",
        "identity",
        "source_sha",
        "dataset_sha256",
        "outcome",
        "attempt",
        "sanitized_error",
        "parity_diff",
        "package",
        "platform",
        "toolchain",
        "covered_inventory",
        "provenance",
        "observations",
        "runner_observations",
        "result",
    }
    if set(report) != required_report_keys:
        raise ValueError("case report contains missing or unsafe extra fields")
    if report.get("schema") != REPORT_SCHEMA:
        raise ValueError("unsupported case report schema")
    identity = report.get("identity")
    if not isinstance(identity, str):
        raise ValueError("case identity missing")
    if report.get("source_sha") != expected_sha:
        raise ValueError(f"{identity}: source SHA drift")
    language, workload_id, dataset = identity.split("/", 2)
    workload = workloads.get(workload_id)
    if workload is None or language not in {"rust", "python", "node"}:
        raise ValueError(f"{identity}: unknown language or workload")
    manifest = manifests.get(dataset)
    if manifest is None or report.get("dataset_sha256") != manifest["content_sha256"]:
        raise ValueError(f"{identity}: dataset fingerprint drift")
    if report.get("outcome") != "passed" or report.get("attempt") != 1:
        raise ValueError(f"{identity}: failed, skipped, or retried case")
    if report.get("sanitized_error") is not None or report.get("parity_diff") != []:
        raise ValueError(f"{identity}: error or parity drift")
    package = report.get("package")
    if (
        not isinstance(package, dict)
        or set(package) != {"name", "version", "artifact_sha256"}
        or not re.fullmatch(r"[A-Za-z0-9@_./-]{1,128}", str(package.get("name", "")))
        or not isinstance(package.get("version"), str)
        or len(package["version"]) > 64
        or not re.fullmatch(r"[0-9a-f]{64}", str(package.get("artifact_sha256", "")))
    ):
        raise ValueError(f"{identity}: package identity missing")
    platform = report.get("platform")
    toolchain = report.get("toolchain")
    safe_identity = re.compile(r"[A-Za-z0-9_.+/-]{1,128}")
    if (
        not isinstance(platform, dict)
        or set(platform) != {"os", "arch"}
        or not all(safe_identity.fullmatch(str(value)) for value in platform.values())
        or not isinstance(toolchain, dict)
        or set(toolchain) != {"name", "version"}
        or not all(safe_identity.fullmatch(str(value)) for value in toolchain.values())
    ):
        raise ValueError(f"{identity}: platform and toolchain identity required")
    requested_selectors = workload.get("inventory_selectors")
    if not isinstance(requested_selectors, list) or any(
        name not in selectors for name in requested_selectors
    ):
        raise ValueError(f"{identity}: unknown inventory selector")
    expected_inventory = sorted(set().union(*(selectors[name] for name in requested_selectors)))
    if report.get("covered_inventory") != expected_inventory:
        raise ValueError(f"{identity}: incomplete or stale public inventory coverage")
    provenance = report.get("provenance")
    provenance_keys = {
        "schema",
        "language",
        "source_sha",
        "artifact_sha256",
        "inventory_sha256",
        "surface_manifest_sha256",
        "adapter_sha256",
        "probe_sha256",
        "command_sha256",
        "outcome",
        "elapsed_ns",
        "output_sha256",
    }
    if not isinstance(provenance, dict) or set(provenance) != provenance_keys:
        raise ValueError(f"{identity}: exhaustive native provenance missing")
    complete_inventory = sorted(set().union(*selectors.values()))
    adapter = ROOT / "scripts/ci/release-load-executor.py"
    probe_paths = {
        "rust": ROOT / "crates/graphforge-api/examples/release_load_probe.rs",
        "python": ROOT / "scripts/ci/release-load-python-probe.py",
        "node": ROOT / "crates/graphforge-bindings-node/tests/release-load-probe.mjs",
    }
    commands = {
        "rust": ["cargo", "test", "-p", "graphforge-api"],
        "python": [sys.executable, str(ROOT / "scripts/ci/run-python-binding-contract.py")],
        "node": ["pnpm", "--filter", "@curatelabs/graphforge", "test"],
    }
    expected_provenance = {
        "schema": "graphforge-load-preflight/1",
        "language": language,
        "source_sha": expected_sha,
        "artifact_sha256": package["artifact_sha256"],
        "inventory_sha256": sha256(canonical(complete_inventory)),
        "surface_manifest_sha256": sha256(
            (ROOT / "tests/contracts/non-cypher-rust-surface.json").read_bytes()
        ),
        "adapter_sha256": sha256(adapter.read_bytes()),
        "probe_sha256": sha256(probe_paths[language].read_bytes()),
        "command_sha256": sha256(canonical(commands[language])),
        "outcome": "passed",
    }
    if any(provenance.get(key) != value for key, value in expected_provenance.items()):
        raise ValueError(f"{identity}: exhaustive native provenance drift")
    if (
        not isinstance(provenance["elapsed_ns"], int)
        or provenance["elapsed_ns"] < 0
        or not re.fullmatch(r"[0-9a-f]{64}", str(provenance["output_sha256"]))
    ):
        raise ValueError(f"{identity}: invalid exhaustive native observations")
    observations = report.get("observations")
    required = {
        "elapsed_ns",
        "peak_rss_bytes",
        "output_rows",
        "output_bytes",
        "open_files",
        "threads",
        "tasks",
        "persisted_bytes",
        "temporary_bytes",
        "cleanup",
        "reopen_equivalent",
    }
    if not isinstance(observations, dict) or set(observations) != required:
        raise ValueError(f"{identity}: incomplete resource observations")
    runner_observations = report.get("runner_observations")
    if (
        not isinstance(runner_observations, dict)
        or set(runner_observations) != {"elapsed_ns"}
        or not all(isinstance(value, int) and value >= 0 for value in runner_observations.values())
    ):
        raise ValueError(f"{identity}: incomplete runner observations")
    if observations["cleanup"] != "complete" or observations["reopen_equivalent"] is not True:
        raise ValueError(f"{identity}: cleanup or reopen equivalence failed")
    optional_resources = {"open_files", "threads", "tasks"}
    for key in required - {"cleanup", "reopen_equivalent"}:
        if observations[key] is None and key in optional_resources:
            continue
        if not isinstance(observations[key], int) or observations[key] < 0:
            raise ValueError(f"{identity}: invalid {key} observation")
    result = report.get("result")
    result_keys = {"schema_sha256", "rows_sha256", "ordering_sha256", "fingerprint"}
    if (
        not isinstance(result, dict)
        or set(result) != result_keys
        or not all(re.fullmatch(r"[0-9a-f]{64}", str(result.get(key, ""))) for key in result_keys)
    ):
        raise ValueError(f"{identity}: incomplete deterministic result identity")
    bounds = load(TAXONOMY)["resource_bounds"][manifest["size_class"]]
    for observation, bound in (
        ("peak_rss_bytes", "maximum_peak_rss_bytes"),
        ("temporary_bytes", "maximum_temporary_bytes"),
        ("persisted_bytes", "maximum_persisted_bytes"),
    ):
        if observations[observation] > bounds[bound]:
            raise ValueError(f"{identity}: declared {observation} bound exceeded")
    return identity


def aggregate(
    reports_dir: Path, fixtures_dir: Path, expected_sha: str, output: Path
) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ValueError("expected SHA must be 40 lowercase hexadecimal characters")
    matrix = load(MATRIX)
    manifests = validate_fixture_set(fixtures_dir)
    _source, selectors = inventory(matrix)
    workloads = {item["id"]: item for item in matrix["workloads"]}
    expected = expected_cases(matrix, list(manifests.values()))
    reports = [load(path) for path in sorted(reports_dir.rglob("*.json"))]
    identities = [
        validate_report(report, expected_sha, manifests, workloads, selectors) for report in reports
    ]
    duplicates = sorted(identity for identity, count in Counter(identities).items() if count > 1)
    missing, extra = sorted(expected - set(identities)), sorted(set(identities) - expected)
    if duplicates or missing or extra:
        raise ValueError(
            f"case ledger mismatch: missing={missing}, extra={extra}, duplicates={duplicates}"
        )
    parity: defaultdict[tuple[str, str], dict[str, tuple[str, ...]]] = defaultdict(dict)
    for report in reports:
        language, workload, dataset = report["identity"].split("/", 2)
        parity[(workload, dataset)][language] = tuple(
            report["result"][key]
            for key in ("schema_sha256", "rows_sha256", "ordering_sha256", "fingerprint")
        )
    drift = [
        f"{workload}/{dataset}"
        for (workload, dataset), values in parity.items()
        if len(set(values.values())) != 1
    ]
    if drift:
        raise ValueError(f"binding parity drift: {sorted(drift)}")
    package_versions: defaultdict[str, set[str]] = defaultdict(set)
    for report in reports:
        package_versions[report["identity"].split("/", 1)[0]].add(report["package"]["version"])
    version_drift = {
        language: sorted(versions)
        for language, versions in package_versions.items()
        if len(versions) != 1
    }
    normalized_versions = {
        re.sub(r"-dev(?:\.0)?$", "-dev", next(iter(values))) for values in package_versions.values()
    }
    if version_drift or len(normalized_versions) != 1:
        raise ValueError(f"package version drift: {version_drift or package_versions}")
    bundle = {
        "schema": BUNDLE_SCHEMA,
        "source_sha": expected_sha,
        "status": "passed",
        "taxonomy_sha256": sha256(TAXONOMY.read_bytes()),
        "matrix_sha256": sha256(MATRIX.read_bytes()),
        "inventory": {name: sorted(values) for name, values in sorted(selectors.items())},
        "packages": sorted(
            {
                (
                    report["identity"].split("/", 1)[0],
                    report["package"]["version"],
                    report["package"]["artifact_sha256"],
                )
                for report in reports
            }
        ),
        "dataset_manifests": sorted(manifests.values(), key=lambda item: item["dataset_id"]),
        "cases": sorted(reports, key=lambda item: item["identity"]),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(canonical(bundle))
    return bundle


def run(args: argparse.Namespace) -> None:
    errors = contract_errors()
    if errors:
        raise ValueError("; ".join(errors))
    if not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        raise ValueError("source SHA must be 40 lowercase hexadecimal characters")
    checkout_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    if checkout_sha != args.sha:
        raise ValueError(f"source SHA {args.sha} does not match checkout {checkout_sha}")
    if subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=ROOT
    ).strip():
        raise ValueError("load evidence requires a clean checkout")
    if args.work.exists() and any(args.work.iterdir()):
        raise ValueError("load-matrix work directory must be fresh; stale reports cannot be reused")
    manifests = generate(args.work / "fixtures")
    matrix = load(MATRIX)
    _source, selectors = inventory(matrix)
    adapter = ROOT / "scripts/ci/release-load-executor.py"
    executors = {
        language: [sys.executable, str(adapter), "--language", language]
        for language in matrix["languages"]
    }
    reports = args.work / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    requests = args.work / "requests"
    requests.mkdir(parents=True, exist_ok=True)
    case_tmp = args.work / "tmp"
    case_tmp.mkdir(parents=True, exist_ok=True)
    child_env = os.environ.copy()
    for key in ("TMPDIR", "TMP", "TEMP"):
        child_env[key] = str(case_tmp)
    manifest_by_id = {item["dataset_id"]: item for item in manifests}
    resource_bounds = load(TAXONOMY)["resource_bounds"]
    for identity in sorted(expected_cases(matrix, manifests)):
        ensure_case_disk_headroom(args.work, identity)
        reclaim_case_tmpdir(case_tmp)
        language, workload, dataset = identity.split("/", 2)
        request = requests / f"{language}--{workload}--{dataset}.json"
        workload_contract = next(item for item in matrix["workloads"] if item["id"] == workload)
        covered_inventory = sorted(
            set().union(*(selectors[name] for name in workload_contract["inventory_selectors"]))
        )
        request.write_bytes(
            canonical(
                {
                    "schema": "graphforge-load-request/1",
                    "identity": identity,
                    "source_sha": args.sha,
                    "fixture": str((args.work / "fixtures" / f"{dataset}.json").resolve()),
                    "manifest": manifest_by_id[dataset],
                    "workload": workload_contract,
                    "required_inventory": covered_inventory,
                    "case_timeout_seconds": resource_bounds[manifest_by_id[dataset]["size_class"]][
                        "case_timeout_seconds"
                    ],
                    "preflight_timeout_seconds": resource_bounds[
                        manifest_by_id[dataset]["size_class"]
                    ]["preflight_timeout_seconds"],
                }
            )
        )
        output = reports / f"{language}--{workload}--{dataset}.json"
        started = time.monotonic_ns()
        try:
            completed = subprocess.run(
                [
                    *executors[language],
                    "--request",
                    str(request),
                    "--output",
                    str(output),
                ],
                cwd=ROOT,
                check=False,
                env=child_env,
                timeout=sum(
                    resource_bounds[manifest_by_id[dataset]["size_class"]][key]
                    for key in ("case_timeout_seconds", "preflight_timeout_seconds")
                ),
            )
        except subprocess.TimeoutExpired as error:
            raise ValueError(f"{identity}: executor hung beyond its declared bound") from error
        elapsed = time.monotonic_ns() - started
        if completed.returncode != 0:
            raise ValueError(
                f"{identity}: executor exited {completed.returncode}; retries are forbidden"
            )
        report = load(output)
        report["runner_observations"] = {
            "elapsed_ns": elapsed,
        }
        output.write_bytes(canonical(report))
    aggregate(reports, args.work / "fixtures", args.sha, args.output)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    generate_parser = sub.add_parser("generate")
    generate_parser.add_argument("--output", type=Path, required=True)
    aggregate_parser = sub.add_parser("aggregate")
    aggregate_parser.add_argument("--reports", type=Path, required=True)
    aggregate_parser.add_argument("--fixtures", type=Path, required=True)
    aggregate_parser.add_argument("--sha", required=True)
    aggregate_parser.add_argument("--output", type=Path, required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--sha", required=True)
    run_parser.add_argument("--work", type=Path, required=True)
    run_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate":
            errors = contract_errors()
            if errors:
                raise ValueError("; ".join(errors))
            print("release load matrix contracts passed")
        elif args.command == "generate":
            errors = contract_errors()
            if errors:
                raise ValueError("; ".join(errors))
            print(f"generated {len(generate(args.output))} deterministic datasets")
        elif args.command == "aggregate":
            bundle = aggregate(args.reports, args.fixtures, args.sha, args.output)
            print(f"accepted {len(bundle['cases'])} cases")
        else:
            run(args)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        output = getattr(args, "output", None)
        source_sha = getattr(args, "sha", "local")
        if args.command in {"aggregate", "run"} and isinstance(output, Path):
            sanitized = str(error).replace(str(ROOT), "<repo>")
            work = getattr(args, "work", None)
            if isinstance(work, Path):
                sanitized = sanitized.replace(str(work), "<work>")
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(
                canonical(
                    {
                        "schema": BUNDLE_SCHEMA,
                        "source_sha": source_sha,
                        "status": "failed",
                        "sanitized_failure": sanitized[:500],
                    }
                )
            )
        print(f"release load matrix failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
