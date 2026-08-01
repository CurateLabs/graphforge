"""Arrow/IPC round-trip data-contract tests — Python side (#595).

Proves that `execute()` results cross to Python faithfully: column types,
values, nullability, schema metadata, multi-column order, a large batch, and a
zero-row schema. The v0.5 result contract is plain Apache Arrow — there are no
`CypherValue` wrappers, so every assertion uses `.as_py()` / Arrow comparisons
(the old CypherValue accessor is never used). The Node binding mirrors these in
``crates/graphforge-bindings-node/tests/arrow_roundtrip.test.mjs``.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

import pyarrow as pa
import pytest

from graphforge import GraphForge

pytestmark = pytest.mark.integration

_ONTOLOGY = """\
ontology_id: people
version: "2026.06"
entity_types:
  - name: Person
properties:
  - name: name
    owner: Person
    type: utf8
"""


class TestArrowRoundtrip:
    def test_type_fidelity(self) -> None:
        # String / Int64 / Float64 / Boolean / Null survive the round-trip.
        table = GraphForge().execute("RETURN 'hi' AS s, 42 AS i, 3.14 AS f, true AS b, null AS n")
        assert table.num_rows == 1
        assert pa.types.is_string(table.schema.field("s").type)
        assert pa.types.is_int64(table.schema.field("i").type)
        assert pa.types.is_float64(table.schema.field("f").type)
        assert pa.types.is_boolean(table.schema.field("b").type)
        assert pa.types.is_null(table.schema.field("n").type)
        assert table.column("s")[0].as_py() == "hi"
        assert table.column("i")[0].as_py() == 42
        assert table.column("f")[0].as_py() == pytest.approx(3.14)
        assert table.column("b")[0].as_py() is True
        assert table.column("n")[0].as_py() is None

    def test_schema_metadata(self) -> None:
        md = GraphForge().execute("RETURN 1 AS a").schema.metadata or {}
        for key in (
            b"graphforge.query_id",
            b"graphforge.ir_version",
            b"graphforge.ontology_mode",
        ):
            assert key in md, sorted(md)

    def test_ontology_version_metadata_when_loaded(self) -> None:
        forge = GraphForge()
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ontology.yaml"
            path.write_text(_ONTOLOGY)
            forge.load_ontology(str(path))
        md = forge.execute("RETURN 1 AS a").schema.metadata or {}
        assert b"graphforge.ontology_version" in md, sorted(md)

    def test_multi_column_order(self) -> None:
        table = GraphForge().execute("RETURN 1 AS a, 2 AS b, 3 AS c")
        assert table.column_names == ["a", "b", "c"]

    def test_null_value(self) -> None:
        table = GraphForge().execute("RETURN null AS x")
        assert table.num_rows == 1
        assert table.column("x")[0].as_py() is None

    def test_large_batch(self) -> None:
        # 10k-row result materialises correctly (no native range(); UNWIND a list).
        literal = "[" + ",".join(str(i) for i in range(1, 10_001)) + "]"
        table = GraphForge().execute(f"UNWIND {literal} AS i RETURN i AS n")
        assert table.num_rows == 10_000
        assert table.column("n")[0].as_py() == 1
        assert table.column("n")[9999].as_py() == 10_000

    def test_zero_row_keeps_schema(self) -> None:
        table = GraphForge().execute("MATCH (n:Nope) RETURN n.node_uuid AS id")
        assert table.num_rows == 0
        assert table.column_names == ["id"]
        assert b"graphforge.query_id" in (table.schema.metadata or {})
