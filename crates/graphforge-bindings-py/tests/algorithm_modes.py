"""Fresh-wheel execution of optional Graphalytics-compatible algorithm modes."""

import math
import uuid

import graphforge as g


def scores(table):
    return dict(zip(table["name"].to_pylist(), table["score"].to_pylist(), strict=True))


def expect_validation_error(call):
    try:
        call()
    except g.ValidationError:
        pass
    else:
        raise AssertionError("invalid option must raise ValidationError")


def check_rank_modes():
    forge = g.GraphForge()
    forge.execute("CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (a)-[:LINK]->(b)")
    options = {"by": "pagerank", "via": "LINK", "iterations": 1, "damping": 0.5}
    result = forge.rank("Person", **options)
    actual = scores(result)
    assert math.isclose(actual["A"], 0.375, abs_tol=1e-15)
    assert math.isclose(actual["B"], 0.625, abs_tol=1e-15)
    assert scores(forge.rank("Person", by="pagerank", iterations=0)) == {"A": 0.5, "B": 0.5}
    descriptor = forge.prepare_rank_invocation("Person", **options)
    assert forge.invoke_descriptor(descriptor).equals(result)
    assert forge.invoke_descriptor_bytes(descriptor.canonical_bytes).equals(result)
    assert (
        descriptor.fingerprint
        != forge.prepare_rank_invocation("Person", **(options | {"iterations": 2})).fingerprint
    )
    forge.enable_capability(
        operation_uuid=str(uuid.uuid4()), capability_id="provenance", capability_version=1
    )
    forge.enable_capability(
        operation_uuid=str(uuid.uuid4()), capability_id="knowledge", capability_version=1
    )
    recorded = forge.invoke_recorded(
        operation_uuid=str(uuid.uuid4()), run_uuid=str(uuid.uuid4()), descriptor=descriptor
    )
    assert recorded.result.equals(result)

    clustering = g.GraphForge()
    clustering.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(a)-[:LINK]->(b), (b)-[:LINK]->(a), (a)-[:LINK]->(c), "
        "(a)-[:LINK]->(d), (b)-[:LINK]->(c), (c)-[:LINK]->(b)"
    )
    lcc = {"by": "clustering_coefficient", "via": "LINK", "directed": True}
    default = scores(clustering.rank("Person", **lcc))
    assert scores(clustering.rank("Person", **lcc, clustering_normalization="fagiolo")) == default
    explicit = clustering.rank("Person", **lcc, clustering_normalization="neighbor_edges")
    assert math.isclose(scores(explicit)["A"], 1 / 3, abs_tol=1e-15)
    assert math.isclose(default["A"], 0.4, abs_tol=1e-15)
    descriptor = clustering.prepare_rank_invocation(
        "Person", **lcc, clustering_normalization="neighbor_edges"
    )
    assert clustering.invoke_descriptor(descriptor).equals(explicit)


def check_synchronous_labels():
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A', external_id:10}), "
        "(b:Person {name:'B', external_id:20}), (a)-[:LINK]->(b), (b)-[:LINK]->(a)"
    )
    options = {
        "by": "label_propagation",
        "via": "LINK",
        "directed": True,
        "synchronous_iterations": 1,
        "initial_label_property": "external_id",
    }
    result = forge.cluster("Person", **options)
    assert result["community_id"].to_pylist() == [20, 10]
    assert forge.cluster("Person", **(options | {"synchronous_iterations": 2}))[
        "community_id"
    ].to_pylist() == [10, 20]
    descriptor = forge.prepare_cluster_invocation("Person", **options)
    assert forge.invoke_descriptor(descriptor).equals(result)
    assert forge.invoke_descriptor_bytes(descriptor.canonical_bytes).equals(result)

    for call in [
        lambda: forge.rank(
            "Person", by="clustering_coefficient", clustering_normalization="invalid"
        ),
        lambda: forge.prepare_rank_invocation(
            "Person", by="clustering_coefficient", clustering_normalization="invalid"
        ),
        lambda: forge.cluster(
            "Person", by="label_propagation", initial_label_property="external_id"
        ),
        lambda: forge.prepare_cluster_invocation(
            "Person", by="label_propagation", initial_label_property="external_id"
        ),
    ]:
        expect_validation_error(call)


def check_projection_modes():
    forge = g.GraphForge()
    for capability in ["provenance", "knowledge", "epistemic"]:
        forge.enable_capability(
            operation_uuid=str(uuid.uuid4()), capability_id=capability, capability_version=1
        )
    node = forge.add_node("Person", name="A", external_id=42)
    forge.create_assertion_with_status(
        operation_uuid=str(uuid.uuid4()),
        assertion_uuid=str(uuid.uuid4()),
        claim="A participates in the graph",
        graph_refs=[
            {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
        ],
        status_event_uuid=str(uuid.uuid4()),
        status="supported",
    )
    projection = forge.resolve_belief_projection(
        transaction_cutoff=2**63 - 1,
        included_statuses=["supported"],
        statusless="exclude",
        supersession_branches="include_all_leaves",
        hypotheses="exclude_unselected_group",
    )
    descriptors = [
        (
            projection.prepare_rank_invocation("Person", by="pagerank", damping=0.5, iterations=1),
            "score",
            1.0,
        ),
        (
            projection.prepare_rank_invocation(
                "Person", by="clustering_coefficient", clustering_normalization="neighbor_edges"
            ),
            "score",
            0.0,
        ),
        (
            projection.prepare_cluster_invocation(
                "Person",
                by="label_propagation",
                synchronous_iterations=1,
                initial_label_property="external_id",
            ),
            "community_id",
            42,
        ),
    ]
    for descriptor, column, expected in descriptors:
        result = forge.invoke_resolved_recorded(
            projection=projection,
            operation_uuid=str(uuid.uuid4()),
            run_uuid=str(uuid.uuid4()),
            attachment_uuid=str(uuid.uuid4()),
            descriptor=descriptor,
        )
        assert result.result[column].to_pylist() == [expected]
        assert result.attachment_state == "attached"


if __name__ == "__main__":
    check_rank_modes()
    check_synchronous_labels()
    check_projection_modes()
