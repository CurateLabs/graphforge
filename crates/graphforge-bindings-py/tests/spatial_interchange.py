"""Real PyArrow acceptance for the Rust-owned GeoArrow contract (#801)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

import graphforge as gf

FIXTURE = json.loads(
    (Path(__file__).parents[3] / "tests/contracts/geoarrow-interchange-v1.json").read_text()
)


def spatial_value(case: dict[str, object]) -> dict[str, object]:
    return {
        "spatial_type": {"geometry": case["geometry"], "crs": case["crs"]},
        "coordinates": case["coordinates"],
    }


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
    forge = gf.GraphForge()
    properties = {case["name"]: spatial_value(case) for case in FIXTURE["cases"]}
    forge.add_node("Geometry", **properties)
    forge.add_node("Geometry")
    projection = ", ".join(f"n.{case['name']} AS {case['name']}" for case in FIXTURE["cases"])
    table = forge.execute(f"MATCH (n:Geometry) RETURN {projection}")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert [batch.num_rows for batch in table.to_batches()] == [2]
    for case in FIXTURE["cases"]:
        field = table.schema.field(case["name"])
        metadata = field.metadata or {}
        assert metadata[b"ARROW:extension:name"].decode() == case["extensionName"]
        assert metadata[b"ARROW:extension:metadata"].decode() == case["extensionMetadata"]
        values = table.column(case["name"]).to_pylist()
        assert flatten_coordinates(values[0]) == case["flat"]
        assert values[1] is None

    try:
        forge.add_node(
            "Geometry",
            bad={
                "spatial_type": {"geometry": "point", "crs": "EPSG:9999"},
                "coordinates": {"Point": [1.0, 2.0]},
            },
        )
    except gf.ValidationError as error:
        assert error.code == "GF_VALIDATION"
        assert "coordinate" not in str(error).lower()
    else:
        raise AssertionError("malformed spatial input must fail")


if __name__ == "__main__":
    check_geoarrow_interchange()
