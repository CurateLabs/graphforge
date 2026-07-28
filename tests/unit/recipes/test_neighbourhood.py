"""Unit tests for graphforge.recipes.neighbourhood (Python wrapper surface)."""

from __future__ import annotations

import pytest

from graphforge import GraphForge
from graphforge.recipes import neighbourhood


def _person_graph() -> GraphForge:
    """Build a small Person graph keyed by ``canonical`` (recipe default)."""
    forge = GraphForge()
    forge.execute(
        "CREATE "
        "(:Person {canonical: 'Alice', name: 'Alice'}), "
        "(:Person {canonical: 'Bob', name: 'Bob'}), "
        "(:Person {canonical: 'Carol', name: 'Carol'}), "
        "(:Person {canonical: 'Lone', name: 'Lone'})"
    )
    forge.execute(
        "MATCH (a:Person {canonical: 'Alice'}), (b:Person {canonical: 'Bob'}) "
        "CREATE (a)-[:KNOWS]->(b)"
    )
    forge.execute(
        "MATCH (b:Person {canonical: 'Bob'}), (c:Person {canonical: 'Carol'}) "
        "CREATE (b)-[:KNOWS]->(c)"
    )
    return forge


def test_neighbourhood_returns_arrow_table() -> None:
    forge = _person_graph()
    table = neighbourhood(forge, "Alice", hops=1, label="Person")
    assert table.num_rows >= 1
    assert "canonical" in table.column_names
    assert "name" in table.column_names


def test_neighbourhood_hops_1_direct_only() -> None:
    forge = _person_graph()
    table = neighbourhood(forge, "Alice", hops=1, label="Person")
    names = set(table.column("name").to_pylist())
    assert "Bob" in names
    assert "Carol" not in names


def test_neighbourhood_hops_2_reaches_two_hop() -> None:
    forge = _person_graph()
    table = neighbourhood(forge, "Alice", hops=2, label="Person")
    names = set(table.column("name").to_pylist())
    assert "Bob" in names
    assert "Carol" in names


def test_neighbourhood_empty_for_isolated_node() -> None:
    forge = _person_graph()
    table = neighbourhood(forge, "Lone", hops=1, label="Person")
    assert table.num_rows == 0


def test_neighbourhood_rejects_invalid_label() -> None:
    forge = GraphForge()
    with pytest.raises(ValueError, match="label must be a valid identifier"):
        neighbourhood(forge, "Alice", label="Person; DROP")


def test_neighbourhood_rejects_invalid_canonical_prop() -> None:
    forge = GraphForge()
    with pytest.raises(ValueError, match="canonical_prop must be a valid identifier"):
        neighbourhood(forge, "Alice", label="Person", canonical_prop="name`")


def test_neighbourhood_rejects_non_positive_hops() -> None:
    forge = GraphForge()
    with pytest.raises(ValueError, match="hops must be an integer >= 1"):
        neighbourhood(forge, "Alice", hops=0, label="Person")
    with pytest.raises(ValueError, match="hops must be an integer >= 1"):
        neighbourhood(forge, "Alice", hops=True, label="Person")  # type: ignore[arg-type]
