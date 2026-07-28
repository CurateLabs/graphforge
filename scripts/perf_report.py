#!/usr/bin/env python3
"""Print real_dataset_baseline_v0.3.9.json as a Markdown table.

Usage:
    python3 scripts/perf_report.py [baseline.json] [compare.json]

With a second file, prints a delta column (compare - baseline).
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

TIER_ORDER = ["xs-karate", "s-facebook", "m-amazon", "l-livejournal", "xl-orkut"]

TIME_METRICS = [
    "ingest_s",
    "persist_write_s",
    "persist_read_s",
    "count_nodes_cold_s",
    "count_nodes_warm_s",
    "count_edges_cold_s",
    "label_filter_cold_s",
    "label_filter_warm_s",
    "two_hop_cold_s",
    "two_hop_warm_s",
    "aggregation_cold_s",
    "aggregation_warm_s",
    "topn_cold_s",
    "topn_warm_s",
]

COUNT_METRICS = ["nodes", "edges"]


def fmt(v: float | None, delta: float | None = None) -> str:
    if v is None:
        return "-"
    s = f"{v:.3f}s" if v < 100 else f"{v:.1f}s"
    if delta is not None:
        sign = "+" if delta >= 0 else ""
        s += f" ({sign}{delta:.3f}s)"
    return s


def main() -> None:
    args = sys.argv[1:]
    baseline_path = Path(args[0]) if args else Path("benchmarks/real_dataset_baseline_v0.3.9.json")
    compare_path = Path(args[1]) if len(args) > 1 else None

    if not baseline_path.exists():
        print(f"Baseline file not found: {baseline_path}")
        print("Run `make test-perf` to generate it.")
        sys.exit(1)

    baseline = json.loads(baseline_path.read_text())
    compare = json.loads(compare_path.read_text()) if compare_path and compare_path.exists() else {}

    tiers = [t for t in TIER_ORDER if t in baseline]
    if not tiers:
        print("No results in baseline file.")
        sys.exit(0)

    header = "| Metric | " + " | ".join(tiers) + " |"
    sep = "|--------|" + "|".join(["--------"] * len(tiers)) + "|"
    print(header)
    print(sep)

    for metric in COUNT_METRICS + TIME_METRICS:
        row_parts = [f"| `{metric}` |"]
        for tier in tiers:
            v = baseline.get(tier, {}).get(metric)
            c = compare.get(tier, {}).get(metric) if compare else None
            delta = (c - v) if (v is not None and c is not None) else None
            if metric in COUNT_METRICS:
                cell = f"{v:,}" if v is not None else "-"
            else:
                cell = fmt(v, delta)
            row_parts.append(f" {cell} |")
        print("".join(row_parts))


if __name__ == "__main__":
    main()
