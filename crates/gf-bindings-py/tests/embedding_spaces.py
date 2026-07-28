"""Fresh-wheel acceptance for embedding-space inspection and alias controls."""

import tempfile

import graphforge as g

FORBIDDEN = {
    "vector",
    "source_text",
    "provider_payload",
    "credential",
    "confidence",
    "provenance_id",
    "assertion_uuid",
    "belief_status",
    "valid_time",
}


def _expect_validation(text: str, call) -> None:
    try:
        call()
    except g.ValidationError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected ValidationError containing {text!r}")


def _publish(forge: g.GraphForge, node, name: str, vector: list[float], recipe: str) -> str:
    return forge.publish_caller_embeddings(
        name,
        [{"node": node, "vector": vector}],
        dimensions=2,
        source_projection={"label": "Person", "recipe": recipe},
    )


def _assert_content_free(space: dict) -> None:
    assert not FORBIDDEN.intersection(space)
    assert not FORBIDDEN.intersection(space["producer"])
    assert space["active"] is not None
    assert not FORBIDDEN.intersection(space["active"])


def check_embedding_space_controls() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        node = forge.add_node("Person", name="Alice")
        alpha = _publish(forge, node, "alpha", [1.0, 0.0], "alpha_v1")
        beta = _publish(forge, node, "beta", [0.0, 1.0], "beta_v1")

        spaces = forge.embedding_spaces()
        assert [space["compatibility_id"] for space in spaces] == sorted([alpha, beta])
        assert {alias for space in spaces for alias in space["aliases"]} == {"alpha", "beta"}
        for space in spaces:
            assert space["dimensions"] == 2
            assert space["producer"] == {
                "kind": "caller_supplied",
                "contract_version": "graphforge_binding_caller_v1",
            }
            assert space["tokenizer"] is None
            assert space["chunking"] is None
            _assert_content_free(space)

        bound = forge.bind_embedding_space_alias("also-alpha", alpha)
        assert bound["aliases"] == ["alpha", "also-alpha"]
        selected = forge.set_default_embedding_space("also-alpha")
        assert selected is not None
        assert forge.embedding_space() == forge.embedding_space("alpha")
        assert forge.embedding_space()["default_alias"] == "also-alpha"

        _expect_validation(
            "already bound",
            lambda: forge.bind_embedding_space_alias("also-alpha", beta),
        )
        rebound = forge.bind_embedding_space_alias("also-alpha", beta, replace=True)
        assert rebound["compatibility_id"] == beta
        forge.set_default_embedding_space("beta")
        assert forge.remove_embedding_space_alias("also-alpha") is True
        assert forge.remove_embedding_space_alias("also-alpha") is False
        assert forge.set_default_embedding_space() is None
        _expect_validation("default embedding space", forge.embedding_space)
        _expect_validation("not configured", lambda: forge.embedding_space("missing"))
        forge.set_default_embedding_space("alpha")
        forge.close()

        reopened = g.GraphForge(project)
        assert reopened.embedding_space()["compatibility_id"] == alpha
        assert [space["compatibility_id"] for space in reopened.embedding_spaces()] == sorted(
            [alpha, beta]
        )
        reopened.close()


if __name__ == "__main__":
    check_embedding_space_controls()
