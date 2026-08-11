"""Development-only parity checks for the five algorithm analyst verbs."""

from collections import defaultdict

import igraph
import networkx as nx
import pytest

import graphforge


def _uuid_names(forge: graphforge.GraphForge) -> dict[bytes, str]:
    table = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS uuid, n.name AS name")
    return dict(
        zip(
            table.column("uuid").to_pylist(),
            table.column("name").to_pylist(),
            strict=True,
        )
    )


def _normalized_communities(
    assignments: list[tuple[str, int]],
) -> set[frozenset[str]]:
    communities: defaultdict[int, set[str]] = defaultdict(set)
    for name, community_id in assignments:
        communities[community_id].add(name)
    return {frozenset(names) for names in communities.values()}


def test_degree_rank_matches_networkx() -> None:
    forge = graphforge.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b)"
    )

    table = forge.rank("Person", by="degree", via="KNOWS")
    actual = dict(
        zip(
            table.column("name").to_pylist(),
            table.column("score").to_pylist(),
            strict=True,
        )
    )
    oracle = nx.DiGraph([("Alice", "Bob")])
    oracle.add_node("Carol")
    expected = {name: float(degree) for name, degree in oracle.out_degree()}

    assert actual == pytest.approx(expected)


def test_components_match_igraph_after_label_normalization() -> None:
    forge = graphforge.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'})"
    )

    table = forge.cluster("Person", by="components", via="KNOWS")
    actual = _normalized_communities(
        list(
            zip(
                table.column("name").to_pylist(),
                table.column("community_id").to_pylist(),
                strict=True,
            )
        )
    )
    oracle = igraph.Graph.TupleList([("Alice", "Bob")], directed=False, vertex_name_attr="name")
    oracle.add_vertex(name="Carol")
    expected = {
        frozenset(oracle.vs[index]["name"] for index in component)
        for component in oracle.connected_components()
    }

    assert actual == expected


def test_node_similarity_matches_networkx_with_tolerance() -> None:
    forge = graphforge.GraphForge()
    edges = [
        ("Alice", "Dan"),
        ("Alice", "Eve"),
        ("Bob", "Dan"),
        ("Bob", "Eve"),
        ("Carol", "Dan"),
    ]
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), "
        "(b)-[:KNOWS]->(d), (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d)"
    )

    table = forge.similar("Person", by="node_similarity", k=2, via="KNOWS")
    names = _uuid_names(forge)
    actual = {
        (names[left], names[right]): score
        for left, right, score in zip(
            table.column("node1_uuid").to_pylist(),
            table.column("node2_uuid").to_pylist(),
            table.column("similarity").to_pylist(),
            strict=True,
        )
    }
    oracle = nx.DiGraph(edges)
    expected = {}
    for left, right in actual:
        left_neighbors = set(oracle.successors(left))
        right_neighbors = set(oracle.successors(right))
        expected[left, right] = len(left_neighbors & right_neighbors) / len(
            left_neighbors | right_neighbors
        )

    assert set(actual) == {
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Bob", "Alice"),
        ("Bob", "Carol"),
        ("Carol", "Alice"),
        ("Carol", "Bob"),
    }
    assert actual == pytest.approx(expected, abs=1e-12)


def test_bfs_path_matches_networkx() -> None:
    forge = graphforge.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    edges = [
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Bob", "Dan"),
        ("Carol", "Eve"),
        ("Eve", "Dan"),
    ]
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) CREATE (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(d)"
    )

    table = forge.paths(handles["Alice"], handles["Dan"], by="bfs", via="KNOWS")
    names = _uuid_names(forge)
    actual = [names[node_uuid] for node_uuid in table.column("path")[0].as_py()]
    expected = nx.shortest_path(nx.DiGraph(edges), "Alice", "Dan")

    assert actual == expected
    assert table.column("cost")[0].as_py() == pytest.approx(
        float(nx.shortest_path_length(nx.DiGraph(edges), "Alice", "Dan"))
    )


@pytest.mark.parametrize(
    ("edges", "expected"),
    [
        ([("Alice", "Bob"), ("Bob", "Carol")], True),
        ([("Alice", "Bob"), ("Bob", "Alice")], False),
    ],
)
def test_is_dag_matches_networkx(edges: list[tuple[str, str]], expected: bool) -> None:
    forge = graphforge.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Carol'})"
    )
    for source, target in edges:
        forge.execute(
            f"MATCH (a:Person {{name:'{source}'}}), "
            f"(b:Person {{name:'{target}'}}) CREATE (a)-[:KNOWS]->(b)"
        )

    actual = forge.analyze("Person", by="is_dag", via="KNOWS")["is_dag"][0].as_py()
    oracle = nx.DiGraph(edges)
    oracle.add_nodes_from(["Alice", "Bob", "Carol"])

    assert actual is expected
    assert actual is nx.is_directed_acyclic_graph(oracle)
