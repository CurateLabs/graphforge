"""Real PyArrow acceptance for the Rust-owned temporal contract (#809)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
from pyarrow import ipc
from pyarrow import parquet as pq

FIXTURE = json.loads(
    (Path(__file__).parents[3] / "tests/contracts/temporal-interchange-v1.json").read_text()
)


def check_temporal_interchange() -> None:
    import graphforge as gf

    forge = gf.GraphForge()
    forge.add_node("Temporal", **{case["name"]: case["value"] for case in FIXTURE["cases"]})
    forge.add_node("Temporal")
    projection = ", ".join(f"n.{case['name']} AS {case['name']}" for case in FIXTURE["cases"])
    table = forge.execute(f"MATCH (n:Temporal) RETURN {projection}")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    for case in FIXTURE["cases"]:
        values = table.column(case["name"])
        assert values[0].is_valid
        assert not values[1].is_valid
    try:
        forge.add_node(
            "Temporal",
            bad={"type": "offset_time", "nanos": 0, "offset_seconds": 64_801},
        )
    except gf.ValidationError:
        pass
    else:
        raise AssertionError("invalid temporal input must fail")


def check_published_fixtures() -> None:
    fixture_dir = Path(__file__).parents[3] / "tests/fixtures/temporal-v1"
    with (fixture_dir / "canonical.arrow").open("rb") as source:
        ipc_table = ipc.open_stream(source).read_all()
    parquet_table = pq.read_table(fixture_dir / "canonical.parquet")
    assert ipc_table.schema == parquet_table.schema
    assert ipc_table.equals(parquet_table)


if __name__ == "__main__":
    check_temporal_interchange()
    check_published_fixtures()
