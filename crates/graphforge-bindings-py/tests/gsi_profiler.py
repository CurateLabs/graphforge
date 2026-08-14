"""GSI profiler binding parity for #398."""

from __future__ import annotations

import uuid

import graphforge as g


def check_empty_and_configured_grades() -> None:
    forge = g.GraphForge()
    empty = forge.profile_gsi()
    assert empty.gsi == "Gx-00-XS-D00"
    assert empty.directedness == "unknown"
    assert empty.node_count == 0
    assert empty.edge_count == 0
    assert empty.density_integer == 0
    assert forge.graph_directedness() is None

    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)"
    )
    unknown = forge.profile_gsi()
    assert unknown.gsi == "Gx-01-XS-D50"
    assert unknown.directedness == "unknown"
    assert unknown.node_count == 3
    assert unknown.edge_count == 3

    forge.set_graph_directedness(
        "undirected",
        operation_uuid=str(uuid.UUID(int=39801)),
    )
    assert forge.graph_directedness() == "undirected"
    undirected = forge.profile_gsi()
    assert undirected.gsi == "GU-01-XS-D100"
    assert undirected.directedness == "undirected"

    forge.set_graph_directedness(
        "directed",
        operation_uuid=str(uuid.UUID(int=39802)),
    )
    directed = forge.profile_gsi()
    assert directed.gsi == "GD-01-XS-D50"
    assert directed.directedness == "directed"

    forge.set_graph_directedness(
        None,
        operation_uuid=str(uuid.UUID(int=39803)),
    )
    assert forge.graph_directedness() is None
    assert forge.profile_gsi().gsi == "Gx-01-XS-D50"


def check_tiny_graph_and_reject_unknown() -> None:
    forge = g.GraphForge()
    forge.execute("CREATE (a:Person {name:'Alice'})")
    tiny = forge.profile_gsi()
    assert tiny.gsi == "Gx-01-XS-D00"
    assert tiny.density_integer == 0

    try:
        forge.set_graph_directedness(
            "bidirectional",
            operation_uuid=str(uuid.UUID(int=39804)),
        )
    except g.ValidationError:
        pass
    else:
        raise AssertionError("unknown directedness must fail closed")


def main() -> None:
    check_empty_and_configured_grades()
    check_tiny_graph_and_reject_unknown()
    print("gsi_profiler.py: ok")


if __name__ == "__main__":
    main()
