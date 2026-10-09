"""Tiny deterministic node/edge Parquet inputs for the legacy-index fixture.

usage: tiny_data.py <dst-dir> <first-node-index> <nodes> <first-edge-index>
Nodes i..i+n-1; edges form a ring over those nodes. UUIDs are v7-shaped and index-derived.
"""

import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

dst, first_node, count, first_edge = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
)


def v7(kind, index):
    value = (kind << 100) | (0x7 << 76) | (0x2 << 62) | index
    return uuid.UUID(int=value).bytes


node_ids = [v7(1, first_node + i) for i in range(count)]
nodes = pa.table(
    {
        "node_uuid": pa.array(node_ids, pa.binary(16)),
        "label": pa.array(["Person"] * count),
    }
)
edges = pa.table(
    {
        "edge_uuid": pa.array([v7(2, first_edge + i) for i in range(count)], pa.binary(16)),
        "rel_type": pa.array(["KNOWS"] * count),
        "source_uuid": pa.array(node_ids, pa.binary(16)),
        "target_uuid": pa.array([node_ids[(i + 1) % count] for i in range(count)], pa.binary(16)),
    }
)
pq.write_table(nodes, f"{dst}/nodes.parquet")
pq.write_table(edges, f"{dst}/edges.parquet")
print("ok", dst)
