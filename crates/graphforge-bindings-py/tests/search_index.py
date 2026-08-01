"""Fresh-wheel acceptance for typed graph-native search indexing."""

import math
import tempfile

import graphforge as g


def _expect_validation(text: str, call) -> None:
    try:
        call()
    except g.ValidationError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected ValidationError containing {text!r}")


def check_typed_search_indexing() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        alice = forge.add_node("Person", name="Alice", summary="Graph search", age=30)
        animal = forge.add_node("Animal", name="Otter")
        forge.add_edge(alice, "KNOWS", animal)
        forge.add_node("adjacency", name="Label, not capability")

        # Explicit None selects deterministic default string-property discovery.
        forge.index("Person", properties=None)
        forge.index("Person", properties=["summary", "name"])
        forge.index("Person", rebuild=True)
        receipt = forge.index("Person", properties=["name"], rebuild=True)
        assert list(receipt) == [
            "project_generation_uuid",
            "properties",
            "source_generation",
            "source_fingerprint",
            "artifact_generation",
            "artifact_source_generation",
            "artifact_source_fingerprint",
            "state",
            "reason",
        ]
        assert receipt["properties"] == ["name"]
        assert receipt["state"] == "current"
        assert receipt["reason"] is None
        assert receipt["artifact_source_generation"] == receipt["source_generation"]
        assert receipt["artifact_source_fingerprint"] == receipt["source_fingerprint"]

        forge.add_node("Person", name="Bob")
        stale = forge.inspect_text_index("Person", properties=["name"])
        assert stale["state"] == "stale"
        assert stale["reason"] == "source_generation_changed"
        repaired = forge.index("Person", properties=["name"], rebuild=True)
        assert repaired["state"] == "current"
        assert repaired["artifact_generation"] != receipt["artifact_generation"]

        _expect_validation("requires text fields", lambda: forge.index("Person"))
        _expect_validation(
            "cannot be combined",
            lambda: forge.index(
                "Person",
                properties=["name"],
                node=alice,
                vector=[1.0, 0.0],
                space="semantic",
            ),
        )
        _expect_validation("at least one property", lambda: forge.index("Person", properties=[]))
        _expect_validation("duplicate", lambda: forge.index("Person", properties=["name", "name"]))
        _expect_validation(
            "not observed as a string", lambda: forge.index("Person", properties=["age"])
        )
        _expect_validation("unknown", lambda: forge.index("Missing", properties=None))
        _expect_validation(
            "unknown search index keyword",
            lambda: forge.index("Person", confidence=0.9),
        )

        vector = [1.0, 0.0]
        forge.index("Person", node=alice, vector=vector, space="semantic")
        forge.index("Person", node=alice.uuid, vector=vector, space="semantic")
        forge.index(
            "Person",
            node={"label": "Person", "property": "name", "value": "Alice"},
            vector=[0.0, 1.0],
            space="semantic",
        )
        forge.index("Person", node=alice.uuid, vector=[0.0, 1.0], space="semantic")

        _expect_validation(
            "requires space", lambda: forge.index("Person", node=alice, vector=vector)
        )
        _expect_validation(
            "required label",
            lambda: forge.index("Person", node=animal, vector=vector, space="semantic"),
        )
        _expect_validation(
            "non-zero",
            lambda: forge.index("Person", node=alice, vector=[0.0, 0.0], space="other"),
        )
        _expect_validation(
            "finite",
            lambda: forge.index("Person", node=alice, vector=[math.nan, 1.0], space="other"),
        )
        _expect_validation(
            "dimension",
            lambda: forge.index("Person", node=alice, vector=[1.0], space="semantic"),
        )
        _expect_validation(
            "node selector",
            lambda: forge.index("Person", node=object(), vector=vector, space="other"),
        )

        # Adjacency capability selection is explicit; a same-named graph label
        # remains independently searchable when search keywords are present.
        adjacency = forge.index_adjacency()
        assert list(adjacency) == [
            "artifact_effective_generation",
            "artifact_fingerprint",
            "artifact_source_generation",
            "project_generation_uuid",
            "reason",
            "source_topology_fingerprint",
            "source_topology_generation",
            "state",
        ]
        assert adjacency["state"] == "current"
        assert adjacency["artifact_fingerprint"] == adjacency["source_topology_fingerprint"]
        assert forge.inspect_adjacency() == adjacency
        cancellation = g.CancellationToken()
        cancellation.cancel()
        try:
            forge.rebuild_adjacency(cancellation=cancellation)
        except g.ValidationError as error:
            assert error.code == "GF_CANCELLED"
        else:
            raise AssertionError("cancelled adjacency rebuild must fail structurally")
        assert forge.inspect_adjacency() == adjacency
        forge.add_node("Person", name="Topology generation without an edge delta")
        assert forge.inspect_adjacency()["state"] == "current"
        forge.execute("MATCH ()-[r:KNOWS]->() DELETE r")
        stale_adjacency = forge.inspect_adjacency()
        assert stale_adjacency["state"] == "stale"
        assert stale_adjacency["reason"] == "incomplete_delta_chain"
        forge.index("adjacency")
        forge.index("adjacency", properties=None)

        expected_reopen = forge.inspect_text_index("Person", properties=["name"])
        expected_adjacency = forge.inspect_adjacency()
        forge.close()
        reopened = g.GraphForge(project)
        reopened_receipt = reopened.inspect_text_index("Person", properties=["name"])
        assert reopened_receipt == expected_reopen, (reopened_receipt, expected_reopen)
        assert reopened.inspect_adjacency() == expected_adjacency
        reopened.index("Person", node=alice.uuid, vector=[0.0, 1.0], space="semantic")
        reopened.close()


if __name__ == "__main__":
    check_typed_search_indexing()
