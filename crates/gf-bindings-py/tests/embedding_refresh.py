"""Fresh-wheel acceptance for embedding freshness and refresh controls."""

import tempfile

import graphforge as g


def _expect(text: str, call) -> None:
    try:
        call()
    except g.GraphForgeError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected GraphForgeError containing {text!r}")


def _assert_content_free(value: dict) -> None:
    rendered = repr(value)
    for forbidden in (
        "source_text",
        "provider_payload",
        "credentials",
        "confidence",
        "provenance_id",
        "assertion_uuid",
        "belief_status",
        "valid_time",
    ):
        assert forbidden not in rendered


def check_embedding_refresh_controls() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        node = forge.add_node("Person", name="Alice")
        forge.publish_caller_embeddings(
            "semantic",
            [{"node": node, "vector": [1.0, 0.0]}],
            dimensions=2,
            source_projection={"label": "Person", "recipe": "all_people_v1"},
        )
        forge.set_default_embedding_space("semantic")

        freshness = forge.inspect_embedding_space_freshness()
        assert freshness["state"] == "fresh"
        assert freshness["reason"] is None
        assert freshness["decision"] == {"kind": "serve_fresh"}
        assert forge.inspect_embedding_space_freshness(force_stale=True) == freshness

        assert forge.embedding_refresh_project_policy() == {
            "proactive": True,
            "debounce_millis": 500,
            "max_concurrent_jobs": 2,
        }
        project_policy = forge.set_embedding_refresh_project_policy(
            proactive=False, debounce_millis=250, max_concurrent_jobs=1
        )
        assert project_policy == {
            "proactive": False,
            "debounce_millis": 250,
            "max_concurrent_jobs": 1,
        }

        inspection = forge.set_embedding_refresh_space_policy(proactive=True, debounce_millis=25)
        assert inspection["space_policy"] == {
            "proactive": True,
            "debounce_millis": 25,
        }
        assert inspection["resolved_policy"] == {
            "proactive": True,
            "debounce_millis": 25,
            "max_concurrent_jobs": 1,
        }
        assert inspection["freshness"] == freshness
        assert inspection["worker"] == {
            "state": "running",
            "queued_lineages": 0,
            "in_flight_lineages": 0,
            "selected_lineage_queued": False,
            "selected_lineage_in_flight": False,
            "coalesced_notices": 0,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
        }
        _assert_content_free(inspection)

        cleared = forge.set_embedding_refresh_space_policy(clear=True)
        assert cleared["space_policy"] is None
        assert cleared["resolved_policy"] == project_policy
        _expect(
            "requires an override",
            forge.set_embedding_refresh_space_policy,
        )
        _expect(
            "cannot include overrides",
            lambda: forge.set_embedding_refresh_space_policy(proactive=True, clear=True),
        )
        _expect(
            "1 hour",
            lambda: forge.set_embedding_refresh_project_policy(
                proactive=True,
                debounce_millis=3_600_001,
                max_concurrent_jobs=1,
            ),
        )
        _expect(
            "non-zero",
            lambda: forge.set_embedding_refresh_project_policy(
                proactive=True, debounce_millis=1, max_concurrent_jobs=0
            ),
        )
        _expect(
            "not configured",
            lambda: forge.inspect_embedding_refresh("missing"),
        )
        forge.close()

        reopened = g.GraphForge(project)
        assert reopened.embedding_refresh_project_policy() == project_policy
        reopened_inspection = reopened.inspect_embedding_refresh()
        assert reopened_inspection["space_policy"] is None
        assert reopened_inspection["worker"]["state"] == "running"
        assert reopened_inspection["worker"]["queued_lineages"] == 0
        assert reopened_inspection["worker"]["in_flight_lineages"] == 0
        _assert_content_free(reopened_inspection)
        reopened.close()


if __name__ == "__main__":
    check_embedding_refresh_controls()
