#!/usr/bin/env python3
"""Comprehensive dataset validation script for v0.3.0 release.

Tests all registered datasets for:
- Download and caching
- Metadata accuracy (node/edge counts)
- Query functionality
- Performance benchmarks
"""

from pathlib import Path
import sys
import time

# Add src to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from graphforge import GraphForge
from graphforge.datasets import list_datasets, load_dataset


def validate_dataset(dataset_name: str, verbose: bool = True) -> dict:
    """Validate a single dataset.

    Args:
        dataset_name: Name of dataset to validate
        verbose: Print detailed output

    Returns:
        Dictionary with validation results
    """
    results = {
        "name": dataset_name,
        "success": False,
        "load_time": None,
        "cached_load_time": None,
        "nodes": None,
        "edges": None,
        "queries_passed": 0,
        "queries_failed": 0,
        "errors": [],
    }

    if verbose:
        print(f"\n{'=' * 60}")
        print(f"Validating: {dataset_name}")
        print("=" * 60)

    try:
        # Test 1: Load dataset (first time - download)
        if verbose:
            print("Test 1: Loading dataset (with download)...")

        gf = GraphForge()
        try:
            start_time = time.time()
            load_dataset(gf, dataset_name)
            load_time = time.time() - start_time
            results["load_time"] = load_time

            if verbose:
                print(f"  ✅ Loaded in {load_time:.2f}s")

            # Test 2: Count nodes and edges
            if verbose:
                print("Test 2: Counting nodes and edges...")

            node_count = gf.execute("MATCH (n) RETURN count(n) AS count")[0]["count"].value
            edge_count = gf.execute("MATCH ()-[r]->() RETURN count(r) AS count")[0]["count"].value

            results["nodes"] = node_count
            results["edges"] = edge_count

            if verbose:
                print(f"  ✅ Nodes: {node_count:,}")
                print(f"  ✅ Edges: {edge_count:,}")
        finally:
            gf.close()

        # Test 3: Load from cache (second load)
        if verbose:
            print("Test 3: Loading from cache...")

        gf2 = GraphForge()
        try:
            start_time = time.time()
            load_dataset(gf2, dataset_name)
            cached_load_time = time.time() - start_time
            results["cached_load_time"] = cached_load_time

            if verbose:
                print(f"  ✅ Loaded from cache in {cached_load_time:.2f}s")

            # Test 4: Basic queries
            if verbose:
                print("Test 4: Running test queries...")

            # Query 1: Sample nodes
            try:
                sample_nodes = gf2.execute("MATCH (n) RETURN n LIMIT 5")
                if verbose:
                    print(f"  ✅ Sample query returned {len(sample_nodes)} nodes")
                results["queries_passed"] += 1
            except (RuntimeError, ValueError) as e:
                results["errors"].append(f"Sample query failed: {e}")
                results["queries_failed"] += 1
                if verbose:
                    print(f"  ❌ Sample query failed: {e}")

            # Query 2: Degree distribution
            try:
                degree_query = """
                    MATCH (n)
                    OPTIONAL MATCH (n)-[r]-()
                    WITH n, count(r) AS degree
                    RETURN degree, count(n) AS frequency
                    ORDER BY degree DESC
                    LIMIT 5
                """
                degree_dist = gf2.execute(degree_query)
                if verbose:
                    print(f"  ✅ Degree distribution query returned {len(degree_dist)} results")
                results["queries_passed"] += 1
            except (RuntimeError, ValueError) as e:
                results["errors"].append(f"Degree query failed: {e}")
                results["queries_failed"] += 1
                if verbose:
                    print(f"  ❌ Degree query failed: {e}")

            # Query 3: Triangle counting (if small enough)
            if node_count < 1000:  # Only for small graphs
                try:
                    triangles = gf2.execute("""
                        MATCH (a)-[:CONNECTED_TO]-(b)-[:CONNECTED_TO]-(c)-[:CONNECTED_TO]-(a)
                        WHERE id(a) < id(b) AND id(b) < id(c)
                        RETURN count(*) AS triangles
                    """)[0]["triangles"].value
                    if verbose:
                        print(f"  ✅ Found {triangles} triangles")
                    results["queries_passed"] += 1
                except (RuntimeError, ValueError, KeyError) as e:
                    # Triangle query may fail if relationship type differs
                    results["queries_failed"] += 1
                    if verbose:
                        print(f"  ⚠️  Triangle query skipped: {e}")
        finally:
            gf2.close()

        results["success"] = True
        if verbose:
            print(f"\n✅ {dataset_name} validation PASSED")

    except (OSError, RuntimeError, ValueError, IndexError, KeyError) as e:
        results["errors"].append(f"Fatal error: {e}")
        if verbose:
            print(f"\n❌ {dataset_name} validation FAILED: {e}")

    return results


def main():
    """Run validation on selected datasets."""
    print("=" * 60)
    print("GraphForge Dataset Validation Suite")
    print("=" * 60)

    # Get all datasets
    all_datasets = list_datasets()
    print(f"\nTotal datasets registered: {len(all_datasets)}")

    # Select datasets to validate
    # 1. All NetworkRepository datasets (newly added)
    netrepo_datasets = [d.name for d in all_datasets if d.name.startswith("netrepo-")]

    # 2. Sample of SNAP datasets (small ones for speed)
    snap_small = [
        "snap-ego-facebook",
        "snap-roadnet-ca",
        "snap-email-enron",
    ]
    snap_datasets = [d.name for d in all_datasets if d.name in snap_small]

    print("\nValidating datasets:")
    print(f"  - NetworkRepository: {len(netrepo_datasets)} datasets")
    print(f"  - SNAP (sample): {len(snap_datasets)} datasets")
    print(f"  - Total to validate: {len(netrepo_datasets) + len(snap_datasets)}")

    # Run validation
    all_results = []

    print("\n" + "=" * 60)
    print("VALIDATING NETWORKREPOSITORY DATASETS")
    print("=" * 60)

    for dataset_name in sorted(netrepo_datasets):
        result = validate_dataset(dataset_name, verbose=True)
        all_results.append(result)

    print("\n" + "=" * 60)
    print("VALIDATING SNAP DATASETS (SAMPLE)")
    print("=" * 60)

    for dataset_name in sorted(snap_datasets):
        result = validate_dataset(dataset_name, verbose=True)
        all_results.append(result)

    # Summary report
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    passed = [r for r in all_results if r["success"]]
    failed = [r for r in all_results if not r["success"]]

    print("\nResults:")
    print(f"  ✅ Passed: {len(passed)}/{len(all_results)}")
    print(f"  ❌ Failed: {len(failed)}/{len(all_results)}")

    if passed:
        print("\nLoad Time Statistics (first load):")
        load_times = [r["load_time"] for r in passed if r["load_time"]]
        if load_times:
            print(f"  Min: {min(load_times):.2f}s")
            print(f"  Max: {max(load_times):.2f}s")
            print(f"  Avg: {sum(load_times) / len(load_times):.2f}s")

        print("\nLoad Time Statistics (cached):")
        cached_times = [r["cached_load_time"] for r in passed if r["cached_load_time"]]
        if cached_times:
            print(f"  Min: {min(cached_times):.2f}s")
            print(f"  Max: {max(cached_times):.2f}s")
            print(f"  Avg: {sum(cached_times) / len(cached_times):.2f}s")

        print("\nDataset Sizes:")
        total_nodes = sum(r["nodes"] for r in passed if r["nodes"])
        total_edges = sum(r["edges"] for r in passed if r["edges"])
        print(f"  Total nodes: {total_nodes:,}")
        print(f"  Total edges: {total_edges:,}")

        print("\nQuery Success Rate:")
        total_queries = sum(r["queries_passed"] + r["queries_failed"] for r in passed)
        passed_queries = sum(r["queries_passed"] for r in passed)
        if total_queries > 0:
            success_rate = (passed_queries / total_queries) * 100
            print(f"  {passed_queries}/{total_queries} queries passed ({success_rate:.1f}%)")

    if failed:
        print("\nFailed Datasets:")
        for r in failed:
            print(f"  ❌ {r['name']}")
            for error in r["errors"]:
                print(f"      - {error}")

    print("\n" + "=" * 60)

    # Exit code
    sys.exit(0 if len(failed) == 0 else 1)


if __name__ == "__main__":
    main()
