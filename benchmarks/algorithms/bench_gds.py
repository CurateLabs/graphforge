"""Small deterministic benchmarks for the native algorithm analyst verbs.

The benchmark intentionally uses a synthetic graph and the installed native
``graphforge`` wheel. It is an executability benchmark, not a performance
threshold. Run it directly or through ``make native-consumers``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
import json
import time
from typing import Any

import pyarrow as pa

from graphforge import GraphForge

RESULT_PREFIX = "GRAPHFORGE_CONSUMER_RESULT="
ALGORITHM_VERBS = {"rank", "cluster", "similar", "paths", "analyze"}


def _timed(operation: Callable[[], pa.Table]) -> tuple[float, pa.Table]:
    started = time.perf_counter()
    result = operation()
    elapsed = time.perf_counter() - started
    assert isinstance(result, pa.Table)
    return elapsed, result


def run() -> dict[str, Any]:
    """Run every native algorithm verb against one bounded local fixture."""
    forge = GraphForge()
    try:
        return _run_benchmark(forge)
    finally:
        forge.close()


def _run_benchmark(forge: GraphForge) -> dict[str, Any]:
    """Execute the bounded fixture while the caller owns the native handle."""
    nodes = [
        forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve", "Frank")
    ]
    for source, target in (
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Bob", "Carol"),
        ("Carol", "Dan"),
        ("Dan", "Eve"),
        ("Eve", "Frank"),
    ):
        forge.execute(
            "MATCH (source:Person {name: $source}), (target:Person {name: $target}) "
            "CREATE (source)-[:KNOWS]->(target)",
            {"source": source, "target": target},
        )

    operations: dict[str, Callable[[], pa.Table]] = {
        "rank": lambda: forge.rank("Person", by="degree", via="KNOWS", directed=False),
        "cluster": lambda: forge.cluster("Person", by="components", via="KNOWS", directed=False),
        "similar": lambda: forge.similar("Person", by="node_similarity", via="KNOWS", k=2),
        "paths": lambda: forge.paths(nodes[0], by="bfs", via="KNOWS"),
        "analyze": lambda: forge.analyze("Person", by="is_dag", via="KNOWS", directed=True),
    }
    measurements: dict[str, dict[str, int | float]] = {}
    for verb, operation in operations.items():
        elapsed, table = _timed(operation)
        measurements[verb] = {
            "seconds": round(elapsed, 6),
            "rows": table.num_rows,
        }

    assert set(measurements) == ALGORITHM_VERBS
    assert all(measurement["rows"] >= 1 for measurement in measurements.values())
    return {
        "consumer": "benchmarks/algorithms/bench_gds.py",
        "algorithm_verbs": sorted(measurements),
        "measurements": measurements,
        "nodes": len(nodes),
        "edges": 6,
    }


def main() -> None:
    """Run the benchmark and optionally emit machine-readable CI evidence."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="emit CI evidence")
    args = parser.parse_args()
    result = run()
    if args.json:
        print(f"{RESULT_PREFIX}{json.dumps(result, sort_keys=True)}")
        return
    print("Native algorithm microbenchmark")
    for verb, measurement in result["measurements"].items():
        print(f"  {verb:<8} {measurement['seconds']:.6f}s ({measurement['rows']} rows)")


if __name__ == "__main__":
    main()
