"""Fresh-wheel acceptance for source-free Gomory-Hu forests."""

import uuid

import pyarrow as pa

import graphforge as g

FORBIDDEN = {
    "node_id",
    "edge_id",
    "provenance_id",
    "confidence",
    "assertion_uuid",
    "evidence_uuid",
    "belief_status",
    "hypothesis",
    "valid_time",
    "as_of",
    "run_uuid",
}


def _hex(value: bytes) -> str:
    return uuid.UUID(bytes=value).hex


def _rows(table: pa.Table) -> list[tuple[str, str, float]]:
    return [
        (_hex(source), _hex(target), cut)
        for source, target, cut in zip(
            table.column("source_uuid").to_pylist(),
            table.column("target_uuid").to_pylist(),
            table.column("cut_value").to_pylist(),
            strict=True,
        )
    ]


def _paths(forge: g.GraphForge, **overrides) -> pa.Table:
    options = {
        "by": "gomory_hu_tree",
        "via": "PIPE",
        "directed": False,
        "weight": "capacity",
    }
    return forge.paths(**(options | overrides))


def _assert_contract(table: pa.Table) -> None:
    assert table.schema == pa.schema(
        [
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("cut_value", pa.float64(), nullable=False),
        ],
        metadata={
            b"graphforge.algorithm": b"gomory_hu_tree",
            b"graphforge.algorithm_schema_version": b"1",
            b"graphforge.verb": b"paths",
        },
    )
    assert not FORBIDDEN.intersection(table.column_names)
    assert all(column.null_count == 0 for column in table.columns)
    rows = _rows(table)
    assert rows == sorted(rows)
    assert all(source < target and cut >= 0.0 for source, target, cut in rows)


def _expect(error_type: type[Exception], text: str, call) -> None:
    try:
        call()
    except error_type as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected {error_type.__name__} containing {text!r}")


def check_gomory_hu_weighted_multigraph() -> None:
    forge = g.GraphForge()
    nodes = {name: forge.add_node("Person", name=name) for name in ["A", "B", "C", "D", "Isolated"]}
    forge.execute(
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}) "
        "CREATE (a)-[:PIPE {capacity:3.0}]->(b), "
        "(a)-[:PIPE {capacity:1.0}]->(b), "
        "(b)-[:PIPE {capacity:2.0}]->(a), "
        "(a)-[:PIPE {capacity:2.0}]->(c), "
        "(c)-[:PIPE {capacity:1.0}]->(a), "
        "(b)-[:PIPE {capacity:4.0}]->(c), "
        "(c)-[:PIPE {capacity:5.0}]->(d), "
        "(a)-[:PIPE {capacity:99.0}]->(a), "
        "(a)-[:OTHER {capacity:99.0}]->(d)"
    )

    weighted = _paths(forge)
    _assert_contract(weighted)
    assert weighted.equals(_paths(forge))
    assert weighted.num_rows == 3  # |V| - components: 5 - 2.
    weighted_cuts = sorted(weighted.column("cut_value").to_pylist())
    assert weighted_cuts == [5.0, 7.0, 9.0], weighted_cuts

    endpoints = {value for row in _rows(weighted) for value in row[:2]}
    expected = {uuid.UUID(nodes[name].uuid).hex for name in ["A", "B", "C", "D"]}
    assert endpoints == expected
    assert uuid.UUID(nodes["Isolated"].uuid).hex not in endpoints

    unit = _paths(forge, weight=None)
    _assert_contract(unit)
    assert unit.num_rows == 3
    unit_cuts = sorted(unit.column("cut_value").to_pylist())
    assert unit_cuts == [1.0, 3.0, 4.0], unit_cuts


def check_gomory_hu_degenerate_graphs_and_errors() -> None:
    empty = g.GraphForge()
    empty_result = _paths(empty)
    _assert_contract(empty_result)
    assert empty_result.num_rows == 0

    singleton = g.GraphForge()
    only = singleton.add_node("Person", name="Only")
    singleton_result = _paths(singleton)
    _assert_contract(singleton_result)
    assert singleton_result.num_rows == 0

    invalid = g.GraphForge()
    invalid.add_node("Person", name="Left")
    invalid.add_node("Person", name="Right")
    invalid.execute(
        "MATCH (l:Person {name:'Left'}), (r:Person {name:'Right'}) "
        "CREATE (l)-[:PIPE {capacity:-1.0}]->(r)"
    )
    missing = g.GraphForge()
    missing.add_node("Person", name="Left")
    missing.add_node("Person", name="Right")
    missing.execute(
        "MATCH (l:Person {name:'Left'}), (r:Person {name:'Right'}) "
        "CREATE (l)-[:PIPE {capacity:1.0}]->(r)"
    )

    _expect(
        g.ValidationError,
        "positional source or target",
        lambda: _paths(singleton, source=only),
    )
    _expect(
        g.ValidationError,
        "directed=false",
        lambda: _paths(singleton, directed=True),
    )
    _expect(
        g.ExecutionError,
        "finite nonnegative",
        lambda: _paths(invalid),
    )
    for option, value, text in [
        ("k", 2, "k"),
        ("heuristic", "capacity", "heuristic"),
        ("capacity_property", "capacity", "min-cost"),
        ("cost_property", "capacity", "min-cost"),
        ("walk_length", 2, "walk"),
        ("seed", 7, "random-walk"),
        ("terminal_uuids", [only.uuid], "terminal"),
        ("prize_property", "capacity", "prize"),
    ]:
        _expect(
            g.ValidationError,
            text,
            lambda option=option, value=value: _paths(singleton, **{option: value}),
        )
    _expect(
        g.ValidationError,
        "missing",
        lambda: _paths(missing, weight="missing"),
    )


if __name__ == "__main__":
    check_gomory_hu_weighted_multigraph()
    check_gomory_hu_degenerate_graphs_and_errors()
