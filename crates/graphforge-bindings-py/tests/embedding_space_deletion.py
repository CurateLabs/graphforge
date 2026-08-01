"""Fresh-wheel acceptance for explicit embedding-space deletion."""

import tempfile

import graphforge as g


def _expect_validation(fragment: str, call) -> None:
    try:
        call()
    except g.ValidationError as error:
        assert fragment in str(error), str(error)
    else:
        raise AssertionError(f"expected ValidationError containing {fragment!r}")


def _publish(forge: g.GraphForge, node, name: str, vector: list[float]) -> str:
    return forge.publish_caller_embeddings(
        name,
        [{"node": node, "vector": vector}],
        dimensions=2,
        source_projection={"label": "Person", "recipe": f"{name}_v1"},
    )


def check_embedding_space_deletion() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        node = forge.add_node("Person", name="Alice")
        obsolete = _publish(forge, node, "obsolete", [1.0, 0.0])
        retained = _publish(forge, node, "retained", [0.0, 1.0])
        forge.bind_embedding_space_alias("obsolete-copy", obsolete)
        forge.set_default_embedding_space("obsolete-copy")

        assert forge.delete_embedding_space("obsolete") is True
        assert forge.delete_embedding_space("obsolete") is False
        assert forge.embedding_space("retained")["compatibility_id"] == retained
        _expect_validation("not configured", lambda: forge.embedding_space("obsolete-copy"))
        _expect_validation("display name", lambda: forge.delete_embedding_space("\n"))
        assert [space["compatibility_id"] for space in forge.embedding_spaces()] == [retained]

        forge.set_default_embedding_space("retained")
        assert forge.delete_embedding_space() is True
        assert forge.delete_embedding_space() is False
        assert forge.embedding_spaces() == []
        forge.close()

        reopened = g.GraphForge(project)
        assert reopened.embedding_spaces() == []
        reopened.close()


if __name__ == "__main__":
    check_embedding_space_deletion()
