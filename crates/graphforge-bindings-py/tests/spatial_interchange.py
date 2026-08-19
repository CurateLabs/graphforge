"""Real PyArrow acceptance for the Rust-owned GeoArrow contract (#801)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

FIXTURE = json.loads(
    (Path(__file__).parents[3] / "tests/contracts/geoarrow-interchange-v1.json").read_text()
)


def spatial_value(case: dict[str, object]) -> dict[str, object]:
    value = {
        "spatial_type": {"geometry": case["geometry"], "crs": case["crs"]},
        "coordinates": case["coordinates"],
    }
    if case.get("preservedOnly"):
        value["extension_name"] = case["extensionName"]
        value["extension_metadata"] = case["extensionMetadata"]
    return value


def flatten_coordinates(value: object) -> list[float]:
    if isinstance(value, (int, float)):
        return [float(value)]
    if isinstance(value, dict):
        if set(value) == {"x", "y"}:
            return [float(value["x"]), float(value["y"])]
        return [item for child in value.values() for item in flatten_coordinates(child)]
    if isinstance(value, list):
        return [item for child in value for item in flatten_coordinates(child)]
    raise AssertionError(f"unexpected Arrow coordinate value {type(value)!r}")


def check_geoarrow_interchange() -> None:
    import graphforge as gf

    forge = gf.GraphForge()
    properties = {case["name"]: spatial_value(case) for case in FIXTURE["cases"]}
    forge.add_node("Geometry", **properties)
    forge.add_node("Geometry")
    projection = ", ".join(f"n.{case['name']} AS {case['name']}" for case in FIXTURE["cases"])
    table = forge.execute(f"MATCH (n:Geometry) RETURN {projection}")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert [batch.num_rows for batch in table.to_batches()] == FIXTURE["rows"]["batchSizes"]
    for case in FIXTURE["cases"]:
        field = table.schema.field(case["name"])
        metadata = field.metadata or {}
        assert metadata[b"ARROW:extension:name"].decode() == case["extensionName"]
        assert metadata[b"ARROW:extension:metadata"].decode() == case["extensionMetadata"]
        values = table.column(case["name"]).to_pylist()
        assert flatten_coordinates(values[FIXTURE["rows"]["populated"]]) == case["flat"]
        assert values[FIXTURE["rows"]["null"]] is None

    try:
        forge.add_node(
            "Geometry",
            bad=FIXTURE["malformed"]["value"],
        )
    except gf.ValidationError as error:
        assert error.code == FIXTURE["malformed"]["code"]
        assert str(error) == FIXTURE["malformed"]["message"]
    else:
        raise AssertionError("malformed spatial input must fail")


def check_published_fixtures() -> None:
    fixture_dir = Path(__file__).parents[3] / "tests/fixtures/geoarrow-v1"
    with (fixture_dir / "canonical.arrow").open("rb") as source:
        ipc_table = ipc.open_stream(source).read_all()
    parquet_table = pq.read_table(fixture_dir / "canonical.parquet")
    for table in (ipc_table, parquet_table):
        assert [batch.num_rows for batch in table.to_batches()] == FIXTURE["rows"]["batchSizes"]
        for case in FIXTURE["cases"]:
            field = table.schema.field(case["name"])
            metadata = field.metadata or {}
            assert metadata[b"ARROW:extension:name"].decode() == case["extensionName"]
            assert metadata[b"ARROW:extension:metadata"].decode() == case["extensionMetadata"]
            values = table.column(case["name"]).to_pylist()
            assert flatten_coordinates(values[FIXTURE["rows"]["populated"]]) == case["flat"]
            assert values[FIXTURE["rows"]["null"]] is None


if __name__ == "__main__":
    check_geoarrow_interchange()
    check_published_fixtures()
