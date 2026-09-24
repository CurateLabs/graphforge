#!/usr/bin/env python3
"""Write a star graph: LEAVES leaf nodes, each with one edge to one hub node.

UUID layout matches the Graph500 generator (node prefix 0x1000000000007000,
edge prefix 0x2000000000007000, low 8 bytes = 0x8000000000000000 | index), so
only the degree distribution differs from the ladder inputs.

Usage: star.py LEAVES OUT_DIR
"""

from pathlib import Path
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP = 1 << 20


def uuids(prefix, start, count):
    raw = np.empty((count, 16), dtype=np.uint8)
    raw[:, :8] = np.frombuffer(prefix.to_bytes(8, "big"), dtype=np.uint8)
    low = (np.arange(start, start + count, dtype=np.uint64) | np.uint64(1 << 63)).astype(">u8")
    raw[:, 8:] = low.view(np.uint8).reshape(count, 8)
    return pa.FixedSizeBinaryArray.from_buffers(
        pa.binary(16), count, [None, pa.py_buffer(raw.tobytes())]
    )


def main():
    leaves = int(sys.argv[1])
    out = Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    node_prefix, edge_prefix = 0x1000000000007000, 0x2000000000007000
    node_schema = pa.schema([("node_uuid", pa.binary(16)), ("label", pa.string(), False)])
    edge_schema = pa.schema(
        [
            ("edge_uuid", pa.binary(16)),
            ("rel_type", pa.string(), False),
            ("source_uuid", pa.binary(16), False),
            ("target_uuid", pa.binary(16), False),
        ]
    )
    with pq.ParquetWriter(out / "nodes.parquet", node_schema) as writer:
        for start in range(0, leaves + 1, ROW_GROUP):
            count = min(ROW_GROUP, leaves + 1 - start)
            columns = [uuids(node_prefix, start, count), pa.array(["Node"] * count)]
            writer.write_table(pa.table(columns, schema=node_schema))
    hub = uuids(node_prefix, 0, 1)
    with pq.ParquetWriter(out / "edges.parquet", edge_schema) as writer:
        for start in range(0, leaves, ROW_GROUP):
            count = min(ROW_GROUP, leaves - start)
            target = pa.FixedSizeBinaryArray.from_buffers(
                pa.binary(16), count, [None, pa.py_buffer(hub.buffers()[1].to_pybytes() * count)]
            )
            writer.write_table(
                pa.table(
                    [
                        uuids(edge_prefix, start, count),
                        pa.array(["EDGE"] * count),
                        uuids(node_prefix, start + 1, count),
                        target,
                    ],
                    schema=edge_schema,
                )
            )


if __name__ == "__main__":
    main()
