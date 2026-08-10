#!/usr/bin/env python3
"""Run the audited downstream consumers against one installed native wheel."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any

import graphforge

RESULT_PREFIX = "GRAPHFORGE_CONSUMER_RESULT="
ALGORITHM_VERBS = {"rank", "cluster", "similar", "paths", "analyze"}
SEARCH_MODES = {"text", "vector", "hybrid"}
MIGRATED = (
    "benchmarks/algorithms/bench_gds.py",
    "scripts/build_feature_graph.py",
    "examples/basic_usage.py",
)
DELETED = (
    "scripts/benchmark_neighbourhood.py",
    "scripts/benchmark_tool_registry.py",
    "scripts/validate_agent_snippets.py",
    "scripts/validate_llm_snippets.py",
    "examples/01_social_network.py",
    "examples/02_knowledge_graph.py",
    "examples/03_data_lineage.py",
    "examples/04_citation_network.py",
    "examples/05_migration_from_networkx.py",
    "examples/basic_graph.py",
)
FORBIDDEN = (
    "graphforge.algorithms",
    "db.gds.",
    "db.search.",
    "create_node(",
    "create_relationship(",
    "CypherValue",
    "PYTHONPATH=src",
)


def _run_consumer(root: Path, path: str, *arguments: str) -> dict[str, Any]:
    command = [sys.executable, str(root / path), *arguments, "--json"]
    completed = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode != 0:
        print(completed.stdout, file=sys.stderr, end="")
        print(completed.stderr, file=sys.stderr, end="")
        raise RuntimeError(f"{path} failed with exit code {completed.returncode}")
    evidence = [
        line.removeprefix(RESULT_PREFIX)
        for line in completed.stdout.splitlines()
        if line.startswith(RESULT_PREFIX)
    ]
    if len(evidence) != 1:
        raise AssertionError(f"{path} emitted {len(evidence)} evidence records")
    print(f"{RESULT_PREFIX}{evidence[0]}")
    result = json.loads(evidence[0])
    if result.get("consumer") != path:
        raise AssertionError(f"{path} reported the wrong consumer identity")
    return result


def main() -> None:
    """Validate the audit, execute survivors, and print closure evidence."""
    root = Path(__file__).resolve().parents[2]
    installed = Path(graphforge.__file__).resolve()
    source_package = root / "crates/graphforge-bindings-py/python/graphforge"
    if installed.is_relative_to(source_package):
        raise AssertionError(f"repository source shadowed the installed wheel: {installed}")

    for path in MIGRATED:
        source = (root / path).read_text(encoding="utf-8")
        for forbidden in FORBIDDEN:
            if forbidden in source:
                raise AssertionError(f"{path} retains forbidden API {forbidden!r}")
        if re.search(r"\]\.value\b", source):
            raise AssertionError(f"{path} retains retired row-wrapper access")
    survivors = [path for path in MIGRATED if (root / path).is_file()]
    if tuple(survivors) != MIGRATED:
        raise AssertionError(f"missing migrated consumers: {set(MIGRATED) - set(survivors)}")
    stale = [path for path in DELETED if (root / path).exists()]
    if stale:
        raise AssertionError(f"obsolete consumers still exist: {stale}")

    # Prevent accidental external HTTP(S). The provider workflow explicitly
    # bypasses these dead proxies only for its deterministic loopback origin.
    os.environ["HTTP_PROXY"] = "http://127.0.0.1:9"
    os.environ["HTTPS_PROXY"] = "http://127.0.0.1:9"
    os.environ["ALL_PROXY"] = "http://127.0.0.1:9"
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"

    with tempfile.TemporaryDirectory() as temporary:
        benchmark = _run_consumer(root, MIGRATED[0])
        feature_graph = _run_consumer(
            root, MIGRATED[1], "--output", str(Path(temporary) / "feature-graph")
        )
        example = _run_consumer(root, MIGRATED[2])

    algorithm = set(benchmark["algorithm_verbs"]) | set(example["algorithm_verbs"])
    search = set(example["search_modes"])
    if algorithm != ALGORITHM_VERBS:
        raise AssertionError(f"incomplete algorithm consumer coverage: {sorted(algorithm)}")
    if search != SEARCH_MODES:
        raise AssertionError(f"incomplete search consumer coverage: {sorted(search)}")
    for key in (
        "explicit_index",
        "lazy_text_index",
        "atomic_embedding_publication",
        "multiple_embedding_spaces",
        "freshness_inspection",
        "provider_plan",
        "semantic_query",
        "rerank",
        "rerank_advisory",
        "installed_wheel",
        "uuid_only",
        "arrow_results",
    ):
        if example.get(key) is not True:
            raise AssertionError(f"missing native consumer evidence: {key}")
    if feature_graph["categories"] <= 0:
        raise AssertionError("feature graph did not build its category inventory")

    sha = os.environ.get("GRAPHFORGE_WHEEL_SHA", "local")
    print("Native downstream consumer audit")
    for path in MIGRATED:
        print(f"  migrate: {path}")
    for path in DELETED:
        print(f"  delete:  {path}")
    print(f"  wheel:   graphforge {graphforge.__version__} ({sha})")
    print(f"  algorithm:     {', '.join(sorted(algorithm))}")
    print(f"  search:     {', '.join(sorted(search))}")
    print("  indexing: explicit + lazy text")
    print("  embeddings: atomic multi-space + freshness inspection")
    print("  provider: tokenizer plan + semantic query + explicit rerank/advisory")


if __name__ == "__main__":
    main()
