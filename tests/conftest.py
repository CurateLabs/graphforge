"""Shared pytest fixtures for all test categories."""

import pytest


@pytest.fixture
def tmp_db_path(tmp_path):
    """A temporary project directory for a Parquet-backed instance.

    The native engine opens an existing *directory* (not a file), so create it.
    """
    d = tmp_path / "graph"
    d.mkdir()
    return d


@pytest.fixture
def db(tmp_db_path):
    """Provides a fresh GraphForge instance backed by a temporary project dir."""
    from graphforge import GraphForge

    gf = GraphForge(str(tmp_db_path))
    yield gf
    gf.close()


@pytest.fixture
def memory_db():
    """Provides an in-memory GraphForge instance (no persistence)."""
    from graphforge import GraphForge

    gf = GraphForge()
    yield gf
    gf.close()


@pytest.fixture
def sample_graph(db):
    """Provides a database with sample data for testing.

    The sample graph contains:
    - 3 Person nodes with properties (name, age)
    - 2 KNOWS relationships
    """
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
    db.execute("CREATE (:Person {name: 'Carol', age: 35})")
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"
    )
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)"
    )
    return db
