"""Fresh-wheel acceptance for complete caller embedding publication."""

import math
import tempfile
import uuid

import graphforge as g


def _expect(text: str, call) -> None:
    try:
        call()
    except g.GraphForgeError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected GraphForgeError containing {text!r}")


def _expect_type(text: str, call) -> None:
    try:
        call()
    except TypeError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected TypeError containing {text!r}")


def _uuids(table) -> list[str]:
    return [uuid.UUID(bytes=value).hex for value in table.column("node_uuid").to_pylist()]


def check_caller_embedding_publication() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        alice = forge.add_node("Person", name="Alice")
        bob = forge.add_node("Person", name="Bob")
        rows = [
            {"node": alice, "vector": [1.0, 0.0]},
            {
                "node": {"label": "Person", "property": "name", "value": "Bob"},
                "vector": [0.0, 1.0],
            },
        ]
        identity = forge.publish_caller_embeddings(
            "semantic",
            rows,
            dimensions=2,
            source_projection={"label": "Person", "recipe": "all_people_v1"},
        )
        assert len(identity) == 64
        assert identity == forge.publish_caller_embeddings(
            "semantic",
            rows,
            dimensions=2,
            source_projection={"recipe": "all_people_v1", "label": "Person"},
        )

        result = forge.find(vector=[1.0, 0.0], label="Person", space="semantic", limit=2)
        assert _uuids(result) == [uuid.UUID(alice.uuid).hex, uuid.UUID(bob.uuid).hex]
        assert result.column("score").to_pylist() == [1.0, 0.0]
        assert not {
            "confidence",
            "provenance_id",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        }.intersection(result.column_names)

        normalized = forge.publish_caller_embeddings(
            "normalized",
            [{"node": alice, "vector": [3.0, 4.0]}],
            dimensions=2,
            normalization="l2",
            source_projection={"label": "Person", "recipe": "normalized_v1"},
        )
        assert len(normalized) == 64
        normalized_result = forge.find(
            vector=[0.6, 0.8], label="Person", space="normalized", limit=1
        )
        assert _uuids(normalized_result) == [uuid.UUID(alice.uuid).hex]
        assert abs(normalized_result.column("score")[0].as_py() - 1.0) < 1e-12

        original = forge.publish_caller_embeddings(
            "replaceable",
            [{"node": alice, "vector": [1.0, 0.0]}],
            dimensions=2,
            contract_version="replacement_a",
            source_projection={"label": "Person"},
        )
        _expect(
            "already targets",
            lambda: forge.publish_caller_embeddings(
                "replaceable",
                [{"node": bob, "vector": [0.0, 1.0]}],
                dimensions=2,
                contract_version="replacement_b",
                source_projection={"label": "Person"},
            ),
        )
        replaced = forge.publish_caller_embeddings(
            "replaceable",
            [{"node": bob, "vector": [0.0, 1.0]}],
            dimensions=2,
            contract_version="replacement_b",
            source_projection={"label": "Person"},
            replace=True,
        )
        assert replaced != original

        _expect(
            "duplicate",
            lambda: forge.publish_caller_embeddings(
                "duplicate",
                [{"node": alice, "vector": [1.0, 0.0]}] * 2,
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        _expect(
            "finite",
            lambda: forge.publish_caller_embeddings(
                "nonfinite",
                [{"node": alice, "vector": [math.nan, 1.0]}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        _expect(
            "dimension",
            lambda: forge.publish_caller_embeddings(
                "width",
                [{"node": alice, "vector": [1.0]}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        _expect(
            "non-zero",
            lambda: forge.publish_caller_embeddings(
                "zero",
                [{"node": alice, "vector": [0.0, 0.0]}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        _expect(
            "unknown caller embedding normalization",
            lambda: forge.publish_caller_embeddings(
                "normalization",
                [{"node": alice, "vector": [1.0, 0.0]}],
                dimensions=2,
                normalization="unitish",
                source_projection={"label": "Person"},
            ),
        )
        _expect_type(
            "exactly node and vector",
            lambda: forge.publish_caller_embeddings(
                "malformed",
                [{"node": alice, "vector": [1.0, 0.0], "extra": True}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        _expect_type(
            "exactly node and vector",
            lambda: forge.publish_caller_embeddings(
                "missing-vector",
                [{"node": alice}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )

        foreign_graph = g.GraphForge()
        foreign = foreign_graph.add_node("Person", name="Foreign")
        _expect(
            "another graph instance",
            lambda: forge.publish_caller_embeddings(
                "foreign",
                [{"node": foreign, "vector": [1.0, 0.0]}],
                dimensions=2,
                source_projection={"label": "Person"},
            ),
        )
        foreign_graph.close()

        empty = forge.publish_caller_embeddings(
            "empty", [], dimensions=3, source_projection={"label": "Nobody"}
        )
        assert len(empty) == 64
        forge.close()

        reopened = g.GraphForge(project)
        result = reopened.find(vector=[0.0, 1.0], label="Person", space="semantic", limit=2)
        assert _uuids(result)[0] == uuid.UUID(bob.uuid).hex
        reopened.close()


if __name__ == "__main__":
    check_caller_embedding_publication()
