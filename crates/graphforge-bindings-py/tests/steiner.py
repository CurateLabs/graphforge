"""Fresh-wheel acceptance for the two source-free Steiner path algorithms."""

import uuid

import pyarrow as pa

import graphforge as g

FORBIDDEN = {
    "node_id",
    "edge_id",
    "provenance_id",
    "confidence",
    "assertion_uuid",
    "belief_status",
    "valid_time",
}


def _uuid(value: bytes) -> str:
    return uuid.UUID(bytes=value).hex


def _rows(table: pa.Table) -> list[tuple[str, str, str, float]]:
    return [
        (_uuid(edge), _uuid(source), _uuid(target), weight)
        for edge, source, target, weight in zip(
            table.column("edge_uuid").to_pylist(),
            table.column("source_uuid").to_pylist(),
            table.column("target_uuid").to_pylist(),
            table.column("weight").to_pylist(),
            strict=True,
        )
    ]


def _assert_contract(table: pa.Table, algorithm: str) -> None:
    assert table.schema == pa.schema(
        [
            pa.field("edge_uuid", pa.binary(16), nullable=False),
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("weight", pa.float64(), nullable=False),
        ],
        metadata={
            b"graphforge.algorithm": algorithm.encode(),
            b"graphforge.algorithm_schema_version": b"1",
            b"graphforge.verb": b"paths",
        },
    )
    assert not FORBIDDEN.intersection(table.column_names)
    assert all(column.null_count == 0 for column in table.columns)


def _edge_catalog(forge: g.GraphForge) -> list[dict[str, object]]:
    table = forge.execute(
        "MATCH (s:Person)-[r:ROAD]->(t:Person) "
        "RETURN r.edge_uuid AS edge_uuid, s.node_uuid AS source_uuid, "
        "t.node_uuid AS target_uuid, r.tag AS tag"
    )
    return [
        {
            "edge": _uuid(table.column("edge_uuid")[row].as_py()),
            "source": _uuid(table.column("source_uuid")[row].as_py()),
            "target": _uuid(table.column("target_uuid")[row].as_py()),
            "tag": table.column("tag")[row].as_py(),
        }
        for row in range(table.num_rows)
    ]


def _expect(error_type: type[Exception], text: str, call) -> None:
    try:
        call()
    except error_type as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected {error_type.__name__} containing {text!r}")


def check_minimum_steiner_tree() -> None:
    forge = g.GraphForge()
    nodes = {name: forge.add_node("Person", name=name) for name in ["A", "B", "Center", "Unused"]}
    forge.execute(
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'Center'}), (u:Person {name:'Unused'}) "
        "CREATE (a)-[:ROAD {cost:1.0, tag:'ac-first'}]->(c), "
        "(b)-[:ROAD {cost:1.0, tag:'bc'}]->(c), "
        "(a)-[:ROAD {cost:5.0, tag:'ab'}]->(b), "
        "(a)-[:ROAD {cost:1.0, tag:'ac-second'}]->(c), "
        "(c)-[:ROAD {cost:0.0, tag:'loop'}]->(c), "
        "(u)-[:ROAD {cost:9.0, tag:'unused-loop'}]->(u), "
        "(a)-[:OTHER {cost:0.0}]->(b)"
    )
    options = {
        "by": "min_steiner_tree",
        "via": "ROAD",
        "directed": False,
        "weight": "cost",
        "terminal_uuids": [nodes["B"].uuid, nodes["A"].uuid, nodes["B"].uuid],
    }
    result = forge.paths(**options)
    _assert_contract(result, "min_steiner_tree")
    assert result.equals(forge.paths(**options))
    assert result.column("weight").to_pylist() == [1.0, 1.0]
    assert _rows(result) == sorted(_rows(result))

    catalog = _edge_catalog(forge)
    expected = []
    for endpoints in [
        (nodes["A"].uuid, nodes["Center"].uuid),
        (nodes["B"].uuid, nodes["Center"].uuid),
    ]:
        pair = {uuid.UUID(value).hex for value in endpoints}
        expected.append(
            min(row["edge"] for row in catalog if {row["source"], row["target"]} == pair)
        )
    assert [row[0] for row in _rows(result)] == sorted(expected)
    assert {frozenset((row[1], row[2])) for row in _rows(result)} == {
        frozenset((uuid.UUID(nodes["A"].uuid).hex, uuid.UUID(nodes["Center"].uuid).hex)),
        frozenset((uuid.UUID(nodes["B"].uuid).hex, uuid.UUID(nodes["Center"].uuid).hex)),
    }

    unit = forge.paths(
        **(options | {"weight": None, "terminal_uuids": [nodes["A"].uuid, nodes["B"].uuid]})
    )
    assert unit.column("weight").to_pylist() == [1.0]
    _expect(
        g.ExecutionError, "must be false", lambda: forge.paths(**(options | {"directed": True}))
    )
    _expect(
        g.ExecutionError,
        "at least 2 distinct terminals",
        lambda: forge.paths(**(options | {"terminal_uuids": [nodes["A"].uuid]})),
    )
    _expect(
        g.ExecutionError,
        "disconnected",
        lambda: forge.paths(
            **(options | {"terminal_uuids": [nodes["A"].uuid, nodes["Unused"].uuid]})
        ),
    )
    _expect(
        g.ValidationError,
        "does not accept positional",
        lambda: forge.paths(nodes["A"], None, **options),
    )


def check_prize_collecting_steiner_tree() -> None:
    forge = g.GraphForge()
    terminal = forge.add_node("Person", name="Terminal", prize=0.0, confidence=1.0)
    winner = forge.add_node("Person", name="Winner", prize=10.0, confidence=0.0)
    excluded = forge.add_node("Person", name="Excluded", prize=2.0, confidence=1.0)
    forge.execute(
        "MATCH (t:Person {name:'Terminal'}), (w:Person {name:'Winner'}), "
        "(x:Person {name:'Excluded'}) "
        "CREATE (t)-[:ROAD {cost:3.0, tag:'winner-first'}]->(w), "
        "(t)-[:ROAD {cost:3.0, tag:'winner-second'}]->(w), "
        "(t)-[:ROAD {cost:5.0, tag:'excluded'}]->(x), "
        "(w)-[:ROAD {cost:0.0, tag:'loop'}]->(w), "
        "(t)-[:OTHER {cost:0.0}]->(x)"
    )
    options = {
        "by": "prize_collecting_steiner_tree",
        "via": "ROAD",
        "directed": False,
        "weight": "cost",
        "terminal_uuids": [terminal.uuid],
        "prize_property": "prize",
    }
    result = forge.paths(**options)
    _assert_contract(result, "prize_collecting_steiner_tree")
    assert result.equals(forge.paths(**options))
    assert result.column("weight").to_pylist() == [3.0]
    chosen = _rows(result)[0]
    pair = {uuid.UUID(terminal.uuid).hex, uuid.UUID(winner.uuid).hex}
    expected = min(
        row["edge"] for row in _edge_catalog(forge) if {row["source"], row["target"]} == pair
    )
    assert chosen[0] == expected
    assert {chosen[1], chosen[2]} == pair
    assert uuid.UUID(excluded.uuid).hex not in chosen[1:3]

    unit = forge.paths(
        **(options | {"weight": None, "terminal_uuids": [winner.uuid, terminal.uuid, winner.uuid]})
    )
    assert unit.column("weight").to_pylist() == [1.0, 1.0]
    assert _rows(unit) == sorted(_rows(unit))

    for name, value, error_type, text in [
        ("Missing", {}, g.ValidationError, "missing"),
        ("Null", {"prize": None}, g.ValidationError, "missing"),
        ("Invalid", {"prize": -1.0}, g.ExecutionError, "nonnegative"),
    ]:
        bad = g.GraphForge()
        node = bad.add_node("Person", name=name, **value)
        _expect(
            error_type,
            text,
            lambda bad=bad, node=node: bad.paths(
                by="prize_collecting_steiner_tree",
                directed=False,
                terminal_uuids=[node.uuid],
                prize_property="prize",
            ),
        )
    _expect(
        g.ExecutionError,
        "prize_property",
        lambda: forge.paths(**(options | {"prize_property": None})),
    )
    _expect(
        g.ExecutionError, "must be false", lambda: forge.paths(**(options | {"directed": True}))
    )


if __name__ == "__main__":
    check_minimum_steiner_tree()
    check_prize_collecting_steiner_tree()
