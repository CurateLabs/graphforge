#!/usr/bin/env python3
"""Exact per-node degree statistics for a Graph500 edge Parquet file.

Streams row groups, counts endpoint occurrences per node UUID (source,
target, and both), and reduces the partial counts with Arrow group_by, so a
billion-edge input fits in memory. Prints one JSON object.

Usage: degrees.py EDGES.parquet
"""

import json
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def reduce(parts):
    table = pa.concat_tables(parts)
    return table.group_by("node").aggregate([("n", "sum")]).rename_columns(["node", "n"])


def counts(values):
    vc = pc.value_counts(values)
    return pa.table({"node": vc.field("values"), "n": vc.field("counts")})


def main():
    started = time.monotonic()
    path = sys.argv[1]
    parquet = pq.ParquetFile(path)
    acc = {"out": [], "in": [], "total": []}
    reduced = dict.fromkeys(acc)
    edges = 0
    for group in range(parquet.metadata.num_row_groups):
        batch = parquet.read_row_group(group, columns=["source_uuid", "target_uuid"])
        edges += batch.num_rows
        src = batch.column("source_uuid").combine_chunks()
        dst = batch.column("target_uuid").combine_chunks()
        acc["out"].append(counts(src))
        acc["in"].append(counts(dst))
        acc["total"].append(counts(pa.concat_arrays([src, dst])))
        if len(acc["total"]) >= 8:
            for key, pending in acc.items():
                previous = [reduced[key]] if reduced[key] is not None else []
                reduced[key] = reduce(pending + previous)
                acc[key] = []
    result = {"edges": edges, "file": path}
    for key, pending in acc.items():
        previous = [reduced[key]] if reduced[key] is not None else []
        table = reduce(pending + previous)
        n = table.column("n")
        order = pc.array_sort_indices(n, order="descending")
        top = pc.take(n, order.slice(0, 10)).to_pylist()
        result[key] = {
            "nodes_with_edges": table.num_rows,
            "max": top[0],
            "top10": top,
            "sum": pc.sum(n).as_py(),
            "nodes_over_1m": pc.sum(pc.greater(n, 1_000_000)).as_py() or 0,
            "nodes_over_4m": pc.sum(pc.greater(n, 4_000_000)).as_py() or 0,
        }
        if key == "total":
            top_nodes = pc.take(table.column("node"), order.slice(0, 3)).to_pylist()
            result["top_total_node_hex"] = [bytes(v).hex() for v in top_nodes]
    result["seconds"] = round(time.monotonic() - started, 1)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
